"""
AstroLake Main Airflow DAG -- astrolake_data_pipeline

Orchestrates the real, end-to-end batch pipeline with no Docker, no Kafka,
no MinIO, and no Postgres. Everything runs as local processes:

  1. check_source_availability   - health-check the 3 public APIs
  2. extract.*                   - parallel extraction from each source
  3. store_raw_data               - write raw records to the local Bronze
                                     Parquet layer (data_lake/bronze/)
  4. run_spark_transformations    - spark-submit each PySpark batch job in
                                     local[*] mode: Bronze -> Silver -> Gold,
                                     including habitability scoring
  5. validate_transformed_data    - confirm Silver Parquet was written
  6. load_serving_layer           - load Gold Parquet into the SQLite
                                     serving database used by the dashboard
  7. run_analytics_quality_checks - data quality assertions against SQLite
  8. raise_habitability_alerts    - log any newly discovered HIGH-tier
                                     habitability candidates (the pipeline's
                                     actual decision output)
  9. record_pipeline_metrics      - persist a pipeline_run_history row
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = os.environ.get("ASTROLAKE_PROJECT_ROOT", "/mnt/c/Users/Admin/Desktop/DEEEEEEEEE/astrolake")
PYTHON_BIN = os.environ.get("ASTROLAKE_PYTHON_BIN", os.path.expanduser("~/astrolake-venv/bin/python"))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)
DAG_ID = "astrolake_data_pipeline"

default_args = {
    "owner": "astrolake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


# -- Task implementations --------------------------------------------------------

def check_source_availability(**ctx):
    import requests
    sources = {
        "exoplanets": "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=SELECT+TOP+1+pl_name+FROM+ps&format=json",
        "apod": "https://science.nasa.gov/wp-json/wp/v2/apod-basic/?per_page=1",
        "spacex": "https://ll.thespacedevs.com/2.3.0/launches/?lsp__id=121&limit=1&mode=list",
    }
    results = {}
    for name, url in sources.items():
        try:
            r = requests.get(url, headers={"User-Agent": "AstroLake/1.0"}, timeout=30)
            results[name] = r.status_code == 200
            logger.info("[health] %s: %s (%s)", name, "OK" if results[name] else "FAIL", r.status_code)
        except Exception as e:
            results[name] = False
            logger.error("[health] %s: UNREACHABLE - %s", name, e)
    ctx["ti"].xcom_push(key="source_health", value=results)
    if not any(results.values()):
        raise RuntimeError("All sources unreachable -- aborting pipeline")


def extract_exoplanet_data(**ctx):
    from ingestion.exoplanets.client import fetch_exoplanets
    records = fetch_exoplanets()
    _stash(ctx, "exoplanets", [r.to_dict() for r in records])
    ctx["ti"].xcom_push(key="exo_count", value=len(records))
    logger.info("Extracted %s exoplanet records", len(records))


def extract_nasa_apod_metadata(**ctx):
    from ingestion.nasa_apod.client import fetch_apod_range, default_date_range
    start, end = default_date_range(days_back=90)
    records = fetch_apod_range(start, end)
    _stash(ctx, "apod", [r.to_dict() for r in records])
    ctx["ti"].xcom_push(key="apod_count", value=len(records))
    logger.info("Extracted %s APOD records", len(records))


def extract_spacex_launch_data(**ctx):
    from ingestion.spacex.client import fetch_launches
    records = fetch_launches()
    _stash(ctx, "launches", [r.to_dict() for r in records])
    ctx["ti"].xcom_push(key="spacex_count", value=len(records))
    logger.info("Extracted %s launch records", len(records))


def _stash(ctx, source: str, records: list[dict]):
    import json
    tmp = f"/tmp/astrolake_{source}_{ctx['run_id'].replace(':', '_').replace('+', '_')}.json"
    with open(tmp, "w") as f:
        json.dump(records, f)
    ctx["ti"].xcom_push(key=f"{source}_tmp_path", value=tmp)


def store_raw_data(**ctx):
    import json
    from storage.bronze.writer import write_bronze
    ti = ctx["ti"]
    batch_id = ctx["run_id"].replace(":", "_").replace("+", "_")

    for source, key in [("exoplanets", "exoplanets_tmp_path"), ("apod", "apod_tmp_path"), ("launches", "launches_tmp_path")]:
        tmp = ti.xcom_pull(key=key)
        if not tmp or not os.path.exists(tmp):
            logger.warning("No tmp file for %s", source)
            continue
        with open(tmp) as f:
            records = json.load(f)
        path = write_bronze(records, source, batch_id)
        ti.xcom_push(key=f"{source}_bronze_path", value=path)
        logger.info("Bronze: %s -> %s", source, path)
    ti.xcom_push(key="batch_id", value=batch_id)


def run_spark_transformations(**ctx):
    """Submit each PySpark batch job as a real local[*] Spark application."""
    batch_id = ctx["ti"].xcom_pull(key="batch_id")
    jobs = [
        os.path.join(PROJECT_ROOT, "spark/batch/exoplanet_transform.py"),
        os.path.join(PROJECT_ROOT, "spark/batch/launch_transform.py"),
        os.path.join(PROJECT_ROOT, "spark/batch/apod_transform.py"),
    ]
    for job in jobs:
        cmd = [PYTHON_BIN, job, batch_id]
        logger.info("Running Spark job: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        logger.info(result.stdout[-4000:])
        if result.returncode != 0:
            logger.error("Spark job failed: %s", result.stderr[-4000:])
            raise RuntimeError(f"Spark job failed: {job}")
        logger.info("Spark job done: %s", job)


def validate_transformed_data(**ctx):
    from pathlib import Path
    data_lake = Path(PROJECT_ROOT) / "data_lake"
    checks = {"exoplanets": 100, "launches": 50, "apod": 10}
    for source, min_count in checks.items():
        silver_dir = data_lake / "silver" / source
        parquet_files = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
        if not parquet_files:
            raise ValueError(f"Validation FAILED: silver/{source}/ has no Parquet output")
        logger.info("Validation OK: silver/%s/ -> %s file(s)", source, len(parquet_files))


def load_serving_layer(**ctx):
    from dashboard.db_local import load_gold_into_sqlite
    run_id = ctx["run_id"]
    loaded = load_gold_into_sqlite(run_id)
    ctx["ti"].xcom_push(key="loaded_summary", value=loaded)
    logger.info("Serving layer loaded: %s", loaded)


def run_analytics_quality_checks(**ctx):
    from monitoring.quality_checks import run_all_checks
    results = run_all_checks(ctx["run_id"])
    failed = [r for r in results if not r["passed"]]
    if failed:
        logger.warning("Quality checks failed: %s", failed)


def raise_habitability_alerts(**ctx):
    """Surface the pipeline's actual decision output: newly promising
    exoplanets. This is the step that makes the pipeline more than a
    visualization -- it is where the DAG acts on what Spark computed.
    """
    from dashboard.db_local import get_conn
    conn = get_conn()
    run_id = ctx["run_id"]
    rows = conn.execute(
        "SELECT pl_name, habitability_score, reason FROM habitability_alerts WHERE run_id=? ORDER BY habitability_score DESC",
        (run_id,),
    ).fetchall()
    if rows:
        logger.warning("HABITABILITY ALERT: %s new high-tier candidate(s) this run:", len(rows))
        for row in rows:
            logger.warning("  -> %s (score=%.4f): %s", row[0], row[1], row[2])
    else:
        logger.info("No new HIGH-tier habitability candidates this run.")
    ctx["ti"].xcom_push(key="new_habitability_alerts", value=len(rows))


def record_pipeline_metrics(**ctx):
    from dashboard.db_local import get_conn
    ti = ctx["ti"]
    run_id = ctx["run_id"]
    conn = get_conn()

    total = 0
    for source, count_key in [("exoplanets", "exo_count"), ("apod", "apod_count"), ("spacex", "spacex_count")]:
        count = ti.xcom_pull(key=count_key) or 0
        total += count

    conn.execute(
        """INSERT OR REPLACE INTO pipeline_run_history
           (run_id, dag_id, execution_date, start_ts, end_ts, status, total_records_processed, notes)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            run_id, DAG_ID, str(ctx["logical_date"]),
            datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(),
            "SUCCESS", total,
            f"new_habitability_alerts={ti.xcom_pull(key='new_habitability_alerts') or 0}",
        ),
    )
    conn.commit()
    logger.info("Pipeline metrics recorded for run %s (total_records=%s)", run_id, total)


# -- DAG definition ---------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="AstroLake end-to-end batch pipeline: NASA/SpaceX APIs -> local Parquet lake -> PySpark -> SQLite, with exoplanet habitability scoring",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["astrolake", "batch", "habitability"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_sources = PythonOperator(
        task_id="check_source_availability",
        python_callable=check_source_availability,
    )

    with TaskGroup("extract", tooltip="Parallel extraction from all 3 sources") as tg_extract:
        PythonOperator(task_id="extract_exoplanet_data", python_callable=extract_exoplanet_data)
        PythonOperator(task_id="extract_nasa_apod_metadata", python_callable=extract_nasa_apod_metadata)
        PythonOperator(task_id="extract_spacex_launch_data", python_callable=extract_spacex_launch_data)

    store_raw = PythonOperator(
        task_id="store_raw_data",
        python_callable=store_raw_data,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    spark_jobs = PythonOperator(
        task_id="run_spark_transformations",
        python_callable=run_spark_transformations,
        execution_timeout=timedelta(minutes=30),
    )

    validate = PythonOperator(
        task_id="validate_transformed_data",
        python_callable=validate_transformed_data,
    )

    load_serving = PythonOperator(
        task_id="load_serving_layer",
        python_callable=load_serving_layer,
    )

    quality_checks = PythonOperator(
        task_id="run_analytics_quality_checks",
        python_callable=run_analytics_quality_checks,
    )

    habitability_alerts = PythonOperator(
        task_id="raise_habitability_alerts",
        python_callable=raise_habitability_alerts,
    )

    record_metrics = PythonOperator(
        task_id="record_pipeline_metrics",
        python_callable=record_pipeline_metrics,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> check_sources >> tg_extract >> store_raw >> spark_jobs >> validate >> load_serving
    load_serving >> quality_checks >> habitability_alerts >> record_metrics >> end
