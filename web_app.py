"""AstroLake localhost web server.

FastAPI serves a custom HTML/CSS/JavaScript dashboard and read-only analytics
endpoints backed by the existing SQLite serving layer. No Streamlit runtime is
used by this application.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from dashboard.db_local import fetch_all_data, query_df, scalar

ROOT = Path(__file__).resolve().parent
WEB_DIR = ROOT / "web"
refresh_lock = threading.Lock()

app = FastAPI(
    title="AstroLake API",
    description="Local analytics API for NASA and SpaceX datasets",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url=None,
)


def rows(frame: pd.DataFrame) -> list[dict]:
    """Return JSON-safe records, converting NaN/NaT to null."""
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "astrolake", "runtime": "fastapi"}


@app.get("/api/overview")
def overview():
    exoplanets = int(scalar("SELECT COUNT(*) FROM fact_exoplanets"))
    launches = int(scalar("SELECT COUNT(*) FROM fact_launches"))
    apod = int(scalar("SELECT COUNT(*) FROM fact_apod_publications"))
    successful = int(scalar("SELECT COUNT(*) FROM fact_launches WHERE outcome='SUCCESS'"))
    decided = int(scalar("SELECT COUNT(*) FROM fact_launches WHERE outcome IN ('SUCCESS','FAILURE')"))
    high_tier = int(scalar("SELECT COUNT(*) FROM gold_habitability_candidates WHERE tier='HIGH'"))

    return {
        "generated_on": date.today().isoformat(),
        "counts": {"exoplanets": exoplanets, "launches": launches, "apod": apod, "habitability_high_tier": high_tier},
        "launch_success_rate": round(successful / decided * 100, 1) if decided else 0,
        "discovery_trend": rows(query_df("""
            SELECT disc_year AS year, planet_count AS count
            FROM gold_discoveries_per_year ORDER BY disc_year
        """)),
        "launch_trend": rows(query_df("""
            SELECT launch_year AS year, outcome, launch_count AS count
            FROM gold_launches_per_year ORDER BY launch_year, outcome
        """)),
        "methods": rows(query_df("""
            SELECT method_name AS method, planet_count AS count,
                   ROUND(median_radius, 2) AS median_radius
            FROM gold_method_stats ORDER BY planet_count DESC LIMIT 8
        """)),
        "top_habitability": rows(query_df("""
            SELECT pl_name, hostname, habitability_score, tier
            FROM gold_habitability_candidates
            ORDER BY habitability_score DESC LIMIT 5
        """)),
        "recent_launches": rows(query_df("""
            SELECT mission_name, launch_date, rocket_name, launchpad_name, outcome
            FROM fact_launches ORDER BY launch_date DESC LIMIT 6
        """)),
        "featured_apod": rows(query_df("""
            SELECT apod_date, title, media_type, url, hdurl, explanation, copyright
            FROM fact_apod_publications
            WHERE media_type='image' AND COALESCE(hdurl, url) IS NOT NULL
            ORDER BY apod_date DESC LIMIT 1
        """)),
        "freshness": rows(query_df("""
            SELECT m.source, m.run_ts, m.records_written, m.duration_seconds, m.status
            FROM fact_ingestion_metrics m
            JOIN (
                SELECT source, MAX(id) AS id FROM fact_ingestion_metrics GROUP BY source
            ) latest ON latest.id=m.id
            ORDER BY m.source
        """)),
    }


@app.get("/api/exoplanets")
def exoplanets(
    q: str = Query("", max_length=100),
    method: str = Query("", max_length=100),
    year_min: int = Query(1990, ge=1900, le=2200),
    year_max: int = Query(2200, ge=1900, le=2200),
    limit: int = Query(200, ge=1, le=1000),
):
    conditions = ["disc_year BETWEEN ? AND ?"]
    params: list = [year_min, year_max]
    if q:
        conditions.append("(LOWER(pl_name) LIKE LOWER(?) OR LOWER(hostname) LIKE LOWER(?))")
        params.extend([f"%{q}%", f"%{q}%"])
    if method:
        conditions.append("discoverymethod=?")
        params.append(method)
    where = " AND ".join(conditions)
    total = int(scalar(f"SELECT COUNT(*) FROM fact_exoplanets WHERE {where}", params))
    frame = query_df(f"""
        SELECT pl_name, hostname, disc_year, discoverymethod, pl_radius_earth,
               pl_mass_earth, pl_orbital_period_days, sy_dist, quality_flag
        FROM fact_exoplanets WHERE {where}
        ORDER BY disc_year DESC, pl_name LIMIT ?
    """, [*params, limit])
    methods = rows(query_df("""
        SELECT discoverymethod AS method, COUNT(*) AS count
        FROM fact_exoplanets WHERE discoverymethod IS NOT NULL
        GROUP BY discoverymethod ORDER BY count DESC
    """))
    scatter = rows(query_df("""
        SELECT pl_name, pl_radius_earth AS radius, pl_mass_earth AS mass,
               discoverymethod AS method
        FROM fact_exoplanets
        WHERE pl_radius_earth > 0 AND pl_mass_earth > 0
        ORDER BY disc_year DESC LIMIT 700
    """))
    return {"total": total, "items": rows(frame), "methods": methods, "scatter": scatter}


@app.get("/api/launches")
def launches(
    q: str = Query("", max_length=100),
    outcome: str = Query("", max_length=20),
    year: int | None = Query(None, ge=1900, le=2200),
    limit: int = Query(300, ge=1, le=1000),
):
    conditions = ["1=1"]
    params: list = []
    if q:
        conditions.append("(LOWER(mission_name) LIKE LOWER(?) OR LOWER(rocket_name) LIKE LOWER(?))")
        params.extend([f"%{q}%", f"%{q}%"])
    if outcome:
        conditions.append("outcome=?")
        params.append(outcome)
    if year:
        conditions.append("CAST(SUBSTR(launch_date,1,4) AS INTEGER)=?")
        params.append(year)
    where = " AND ".join(conditions)
    total = int(scalar(f"SELECT COUNT(*) FROM fact_launches WHERE {where}", params))
    frame = query_df(f"""
        SELECT flight_number, mission_name, launch_date, rocket_name,
               launchpad_name, outcome, details
        FROM fact_launches WHERE {where}
        ORDER BY launch_date DESC LIMIT ?
    """, [*params, limit])
    stats = rows(query_df("""
        SELECT outcome, COUNT(*) AS count FROM fact_launches GROUP BY outcome
    """))
    rockets = rows(query_df("""
        SELECT COALESCE(rocket_name, 'Unknown') AS rocket, COUNT(*) AS count,
               SUM(CASE WHEN outcome='SUCCESS' THEN 1 ELSE 0 END) AS successes
        FROM fact_launches GROUP BY rocket_name ORDER BY count DESC
    """))
    return {"total": total, "items": rows(frame), "stats": stats, "rockets": rockets}


@app.get("/api/apod")
def apod(
    q: str = Query("", max_length=100),
    media_type: str = Query("", max_length=20),
    year: int | None = Query(None, ge=1995, le=2200),
    limit: int = Query(60, ge=1, le=366),
):
    conditions = ["1=1"]
    params: list = []
    if q:
        conditions.append("LOWER(title) LIKE LOWER(?)")
        params.append(f"%{q}%")
    if media_type:
        conditions.append("media_type=?")
        params.append(media_type)
    if year:
        conditions.append("CAST(SUBSTR(apod_date,1,4) AS INTEGER)=?")
        params.append(year)
    where = " AND ".join(conditions)
    total = int(scalar(f"SELECT COUNT(*) FROM fact_apod_publications WHERE {where}", params))
    frame = query_df(f"""
        SELECT apod_date, title, media_type, url, hdurl, explanation, copyright
        FROM fact_apod_publications WHERE {where}
        ORDER BY apod_date DESC LIMIT ?
    """, [*params, limit])
    return {"total": total, "items": rows(frame)}


@app.get("/api/habitability")
def habitability(
    tier: str = Query("", max_length=20),
    q: str = Query("", max_length=100),
    limit: int = Query(100, ge=1, le=500),
):
    """Ranked exoplanet habitability candidates -- the pipeline's actual
    decision output (Earth Similarity Index-style score computed by the
    PySpark gold-layer transform, or by the direct-fetch fallback path)."""
    conditions = ["1=1"]
    params: list = []
    if tier:
        conditions.append("tier=?")
        params.append(tier)
    if q:
        conditions.append("(LOWER(pl_name) LIKE LOWER(?) OR LOWER(hostname) LIKE LOWER(?))")
        params.extend([f"%{q}%", f"%{q}%"])
    where = " AND ".join(conditions)

    total = int(scalar(f"SELECT COUNT(*) FROM gold_habitability_candidates WHERE {where}", params))
    frame = query_df(f"""
        SELECT pl_name, hostname, disc_year, discoverymethod, pl_radius_earth,
               pl_mass_earth, pl_orbital_period_days, sy_dist,
               habitability_score, in_habitable_zone_proxy, tier
        FROM gold_habitability_candidates WHERE {where}
        ORDER BY habitability_score DESC LIMIT ?
    """, [*params, limit])
    tier_counts = rows(query_df("""
        SELECT tier, COUNT(*) AS count FROM gold_habitability_candidates
        GROUP BY tier ORDER BY CASE tier WHEN 'HIGH' THEN 0 WHEN 'MODERATE' THEN 1 ELSE 2 END
    """))
    alerts = rows(query_df("""
        SELECT pl_name, habitability_score, tier, reason, run_id, created_at
        FROM habitability_alerts ORDER BY created_at DESC LIMIT 25
    """))
    return {"total": total, "items": rows(frame), "tier_counts": tier_counts, "alerts": alerts}


@app.get("/api/pipeline")
def pipeline():
    checks = [
        ("Exoplanet volume", int(scalar("SELECT COUNT(*) FROM fact_exoplanets")), 1000),
        ("Launch volume", int(scalar("SELECT COUNT(*) FROM fact_launches")), 100),
        ("APOD volume", int(scalar("SELECT COUNT(*) FROM fact_apod_publications")), 30),
        ("Null planet keys", int(scalar("SELECT COUNT(*) FROM fact_exoplanets WHERE pl_name IS NULL")), 0, True),
        ("Invalid outcomes", int(scalar("SELECT COUNT(*) FROM fact_launches WHERE outcome NOT IN ('SUCCESS','FAILURE','UNKNOWN')")), 0, True),
    ]
    quality = []
    for item in checks:
        name, value, threshold, *zero_expected = item
        passed = value == threshold if zero_expected else value >= threshold
        quality.append({"name": name, "value": value, "threshold": threshold, "passed": passed})
    return {
        "quality": quality,
        "metrics": rows(query_df("""
            SELECT source, run_ts, records_fetched, records_written,
                   ROUND(duration_seconds, 2) AS duration_seconds, status, error_detail
            FROM fact_ingestion_metrics ORDER BY id DESC LIMIT 30
        """)),
        "runs": rows(query_df("""
            SELECT run_id, execution_date, status, total_records_processed, notes
            FROM pipeline_run_history ORDER BY execution_date DESC LIMIT 20
        """)),
    }


DATA_LAKE = ROOT / "data_lake"
SOURCES = ["exoplanets", "launches", "apod"]


def _parquet_row_count(path: Path) -> int | None:
    """Count rows across a Parquet file or directory of part-files, without
    loading the data into memory -- reads only each file's footer metadata."""
    if not path.exists():
        return None
    files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
    if not files:
        return None
    total = 0
    for f in files:
        try:
            total += pq.ParquetFile(f).metadata.num_rows
        except Exception:
            return None
    return total


def _latest_batch_dir(source_dir: Path) -> Path | None:
    if not source_dir.exists():
        return None
    batches = sorted(source_dir.glob("batch_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return batches[0] if batches else None


def _stage_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    files = [path] if path.is_file() else list(path.rglob("*.parquet"))
    if not files:
        return None
    newest = max(f.stat().st_mtime for f in files)
    return datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()


@app.get("/api/pipeline/lineage")
def pipeline_lineage():
    """Live, measured data lineage -- actual row counts read from each
    Parquet stage on disk (Bronze -> Silver -> Gold) plus the SQLite serving
    layer, per source. This is what powers the pipeline flow visualization:
    every number shown is counted from real files, not hardcoded."""
    silver_dirs = {
        "exoplanets": DATA_LAKE / "silver" / "exoplanets",
        "launches": DATA_LAKE / "silver" / "launches",
        "apod": DATA_LAKE / "silver" / "apod",
    }
    bronze_dirs = {s: DATA_LAKE / "bronze" / s for s in SOURCES}

    stages = {}
    for source in SOURCES:
        bronze_batch = _latest_batch_dir(bronze_dirs[source])
        bronze_count = _parquet_row_count(bronze_batch) if bronze_batch else None
        silver_count = _parquet_row_count(silver_dirs[source])
        stages[source] = {
            "bronze": bronze_count,
            "bronze_updated_at": _stage_mtime(bronze_batch) if bronze_batch else None,
            "silver": silver_count,
            "silver_updated_at": _stage_mtime(silver_dirs[source]),
        }

    sqlite_counts = {
        "exoplanets": int(scalar("SELECT COUNT(*) FROM fact_exoplanets")),
        "launches": int(scalar("SELECT COUNT(*) FROM fact_launches")),
        "apod": int(scalar("SELECT COUNT(*) FROM fact_apod_publications")),
    }

    habitability_gold = DATA_LAKE / "gold" / "habitability" / "candidates"
    gold = {
        "habitability_scored": _parquet_row_count(habitability_gold),
        "habitability_updated_at": _stage_mtime(habitability_gold),
        "discoveries_per_year_rows": _parquet_row_count(DATA_LAKE / "gold" / "discovery_trends" / "per_year"),
        "method_stats_rows": _parquet_row_count(DATA_LAKE / "gold" / "discovery_trends" / "method_stats"),
        "launches_per_year_rows": _parquet_row_count(DATA_LAKE / "gold" / "launch_analytics" / "per_year"),
        "apod_per_month_rows": _parquet_row_count(DATA_LAKE / "gold" / "apod_trends" / "per_month"),
    }

    latest_run = query_df("""
        SELECT run_id, execution_date, status, total_records_processed, notes
        FROM pipeline_run_history ORDER BY execution_date DESC LIMIT 1
    """)

    return {
        "stages": stages,
        "sqlite": sqlite_counts,
        "gold": gold,
        "latest_run": rows(latest_run)[0] if not latest_run.empty else None,
        "data_lake_exists": DATA_LAKE.exists(),
    }


@app.post("/api/refresh")
async def refresh():
    """Fast path: fetch all 3 sources directly and upsert into SQLite.
    Used by the dashboard's 'Refresh sources' button for an instant demo."""
    if not refresh_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A data refresh is already running")
    try:
        return await asyncio.to_thread(fetch_all_data, True)
    finally:
        refresh_lock.release()


def _airflow_bin() -> str:
    python_bin = os.environ.get("ASTROLAKE_PYTHON_BIN", os.path.expanduser("~/astrolake-venv/bin/python"))
    return str(Path(python_bin).parent / "airflow")


def _airflow_home() -> dict:
    env = dict(os.environ)
    env.setdefault("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
    return env


DAG_ID = "astrolake_data_pipeline"
# The ordered task list as defined in airflow/dags/astrolake_data_pipeline.py,
# used to render a deterministic step order even before Airflow has created
# task-instance rows for a brand-new run.
PIPELINE_TASKS = [
    {"id": "start", "label": "Start"},
    {"id": "check_source_availability", "label": "Check source availability"},
    {"id": "extract.extract_exoplanet_data", "label": "Extract: NASA Exoplanet Archive"},
    {"id": "extract.extract_nasa_apod_metadata", "label": "Extract: NASA APOD"},
    {"id": "extract.extract_spacex_launch_data", "label": "Extract: Launch Library 2"},
    {"id": "store_raw_data", "label": "Write Bronze Parquet"},
    {"id": "run_spark_transformations", "label": "PySpark: Bronze -> Silver -> Gold + habitability scoring"},
    {"id": "validate_transformed_data", "label": "Validate Silver layer"},
    {"id": "load_serving_layer", "label": "Load Gold -> SQLite serving layer"},
    {"id": "run_analytics_quality_checks", "label": "Run data quality checks"},
    {"id": "raise_habitability_alerts", "label": "Raise habitability alerts"},
    {"id": "record_pipeline_metrics", "label": "Record pipeline metrics"},
    {"id": "end", "label": "End"},
]

pipeline_run_state: dict = {"run_id": None, "logical_date": None, "running": False}


@app.post("/api/pipeline/trigger")
async def trigger_pipeline():
    """Fire-and-forget: launches the real Airflow DAG (`airflow dags test`)
    as a detached background process and returns immediately with a run_id.
    Poll GET /api/pipeline/run-status?run_id=... for live per-task progress."""
    if not refresh_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A pipeline run is already in progress")

    import subprocess

    logical_date = date.today().isoformat()
    run_id = f"manual__{logical_date}T00:00:00+00:00"

    def _launch():
        log_path = Path(os.path.expanduser("~/airflow")) / "astrolake_last_run.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "w") as logf:
            proc = subprocess.Popen(
                [_airflow_bin(), "dags", "test", DAG_ID, logical_date],
                stdout=logf, stderr=subprocess.STDOUT, env=_airflow_home(),
            )
        return proc

    proc = await asyncio.to_thread(_launch)
    pipeline_run_state.update({"run_id": run_id, "logical_date": logical_date, "running": True, "pid": proc.pid})

    def _wait_and_release():
        proc.wait()
        pipeline_run_state["running"] = False
        refresh_lock.release()

    threading.Thread(target=_wait_and_release, daemon=True).start()
    return {"status": "started", "run_id": run_id, "logical_date": logical_date}


@app.get("/api/pipeline/run-status")
async def pipeline_run_status(run_id: str = Query(...)):
    """Live per-task status for a triggered run, read from Airflow's own
    metadata DB via `airflow tasks states-for-dag-run` -- real task state,
    not a simulated progress bar."""
    import subprocess

    def _query():
        return subprocess.run(
            [_airflow_bin(), "tasks", "states-for-dag-run", DAG_ID, run_id],
            capture_output=True, text=True, timeout=30, env=_airflow_home(),
        )

    result = await asyncio.to_thread(_query)
    task_states: dict[str, dict] = {}
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 5 or parts[2] == "task_id" or parts[2].startswith("="):
                continue
            task_id, state, start_date, end_date = parts[2], parts[3], parts[4], parts[5] if len(parts) > 5 else ""
            task_states[task_id] = {"state": state, "start_date": start_date or None, "end_date": end_date or None}

    steps = []
    for task in PIPELINE_TASKS:
        info = task_states.get(task["id"], {})
        steps.append({"id": task["id"], "label": task["label"], "state": info.get("state", "pending"),
                       "start_date": info.get("start_date"), "end_date": info.get("end_date")})

    any_failed = any(s["state"] == "failed" for s in steps)
    all_terminal = all(s["state"] in ("success", "failed", "skipped", "upstream_failed") for s in steps)
    overall = "failed" if any_failed else "success" if all_terminal else "running"
    if overall != "running":
        pipeline_run_state["running"] = False

    log_tail = ""
    log_path = Path(os.path.expanduser("~/airflow")) / "astrolake_last_run.log"
    if log_path.exists():
        log_tail = log_path.read_text(errors="replace")[-3000:]

    return {"run_id": run_id, "overall": overall, "steps": steps, "log_tail": log_tail,
            "process_running": pipeline_run_state.get("running", False) and pipeline_run_state.get("run_id") == run_id}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(WEB_DIR / "index.html")


app.mount("/assets", StaticFiles(directory=WEB_DIR), name="assets")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("web_app:app", host="127.0.0.1", port=8000, reload=False)
