"""
Local SQLite serving layer for AstroLake.

Two ways data lands here:
  1. Direct mode (`fetch_all_data`) -- used by the FastAPI "Refresh sources"
     button for an instant, no-pipeline demo. Fetches live from the 3 APIs
     concurrently and upserts straight into SQLite.
  2. Pipeline mode (`load_gold_into_sqlite`) -- used by the Airflow DAG after
     the real PySpark batch jobs have written cleaned Silver and aggregated
     Gold Parquet to the local data lake. This is the actual engineered path:
     NASA/SpaceX APIs -> ingestion clients -> local Parquet bronze layer ->
     PySpark transforms -> Silver/Gold Parquet -> this loader -> SQLite.

Both paths populate the same tables, so the dashboard works identically
regardless of which one produced the data.
"""
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

from ingestion.nasa_apod.client import fetch_apod_range
from ingestion.spacex.client import fetch_launches
from ingestion.exoplanets.client import fetch_exoplanets

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "astrolake_local.db"
DATA_LAKE = Path(__file__).resolve().parents[1] / "data_lake"

# Habitability scoring constants (kept in sync with spark/batch/exoplanet_transform.py)
EARTH_RADIUS_EARTH = 1.0
EARTH_MASS_EARTH = 1.0
EARTH_ORBITAL_PERIOD_DAYS = 365.25
HZ_PERIOD_MIN_DAYS = 50.0
HZ_PERIOD_MAX_DAYS = 500.0


# -- Connection ---------------------------------------------------------------
@lru_cache(maxsize=1)
def get_conn():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _init_schema(conn)
    return conn


def _init_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS fact_exoplanets (
        pl_name TEXT PRIMARY KEY,
        hostname TEXT,
        disc_year INTEGER,
        discoverymethod TEXT,
        pl_radius_earth REAL,
        pl_mass_earth REAL,
        pl_orbital_period_days REAL,
        sy_dist REAL,
        quality_flag TEXT DEFAULT 'OK',
        ingested_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS fact_launches (
        launch_id TEXT PRIMARY KEY,
        mission_name TEXT,
        launch_date TEXT,
        rocket_name TEXT,
        launchpad_name TEXT,
        outcome TEXT,
        flight_number INTEGER,
        details TEXT,
        ingested_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS fact_apod_publications (
        apod_date TEXT PRIMARY KEY,
        title TEXT,
        media_type TEXT,
        url TEXT,
        hdurl TEXT,
        explanation TEXT,
        copyright TEXT,
        ingested_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS fact_ingestion_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        run_ts TEXT DEFAULT (datetime('now')),
        records_fetched INTEGER DEFAULT 0,
        records_written INTEGER DEFAULT 0,
        records_rejected INTEGER DEFAULT 0,
        duration_seconds REAL,
        status TEXT,
        error_detail TEXT
    );
    CREATE TABLE IF NOT EXISTS gold_discoveries_per_year (
        disc_year INTEGER PRIMARY KEY,
        planet_count INTEGER,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS gold_launches_per_year (
        launch_year INTEGER,
        outcome TEXT,
        launch_count INTEGER,
        PRIMARY KEY (launch_year, outcome)
    );
    CREATE TABLE IF NOT EXISTS gold_method_stats (
        method_name TEXT PRIMARY KEY,
        planet_count INTEGER,
        median_radius REAL,
        avg_orbital_period REAL,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS pipeline_run_history (
        run_id TEXT PRIMARY KEY,
        dag_id TEXT,
        execution_date TEXT,
        start_ts TEXT,
        end_ts TEXT,
        status TEXT,
        total_records_processed INTEGER DEFAULT 0,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS quality_check_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        check_name TEXT,
        value REAL,
        threshold REAL,
        passed INTEGER,
        checked_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS gold_habitability_candidates (
        pl_name TEXT PRIMARY KEY,
        hostname TEXT,
        disc_year INTEGER,
        discoverymethod TEXT,
        pl_radius_earth REAL,
        pl_mass_earth REAL,
        pl_orbital_period_days REAL,
        sy_dist REAL,
        habitability_score REAL,
        in_habitable_zone_proxy INTEGER,
        tier TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS habitability_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pl_name TEXT,
        habitability_score REAL,
        tier TEXT,
        reason TEXT,
        run_id TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_exo_year ON fact_exoplanets(disc_year);
    CREATE INDEX IF NOT EXISTS idx_exo_method ON fact_exoplanets(discoverymethod);
    CREATE INDEX IF NOT EXISTS idx_launch_date ON fact_launches(launch_date);
    CREATE INDEX IF NOT EXISTS idx_habitability_score ON gold_habitability_candidates(habitability_score);
    """)
    conn.commit()


def query_df(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_conn()
        return pd.read_sql_query(sql, conn, params=params or [])
    except Exception as e:
        logger.exception("Database query failed: %s", e)
        return pd.DataFrame()


def scalar(sql: str, params=None, default=0):
    df = query_df(sql, params)
    if df.empty or df.iloc[0, 0] is None:
        return default
    return df.iloc[0, 0]


# -- Data freshness check -------------------------------------------------------
def needs_refresh(source: str, hours: int = 24) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(run_ts) FROM fact_ingestion_metrics WHERE source=? AND status='SUCCESS'",
        (source,)
    ).fetchone()
    if not row or not row[0]:
        return True
    last = datetime.fromisoformat(row[0])
    return (datetime.now() - last).total_seconds() > hours * 3600


def _record_metric(source: str, fetched: int, written: int, duration: float, status: str, error: str = None):
    conn = get_conn()
    conn.execute(
        """INSERT INTO fact_ingestion_metrics
           (source, records_fetched, records_written, duration_seconds, status, error_detail)
           VALUES (?,?,?,?,?,?)""",
        (source, fetched, written, duration, status, error),
    )
    conn.commit()


# -- NASA APOD Fetch -------------------------------------------------------------
def fetch_and_store_apod(days_back: int = 365, force: bool = False) -> dict:
    if not force and not needs_refresh("apod"):
        return {"status": "cached", "records": scalar("SELECT COUNT(*) FROM fact_apod_publications")}

    start = (date.today() - timedelta(days=days_back)).isoformat()
    end = date.today().isoformat()
    t0 = time.time()

    try:
        rows = fetch_apod_range(start, end)

        conn = get_conn()
        written = 0
        for r in rows:
            conn.execute("""
                INSERT OR REPLACE INTO fact_apod_publications
                (apod_date, title, media_type, url, hdurl, explanation, copyright)
                VALUES (?,?,?,?,?,?,?)
            """, (
                r.apod_date, r.title, r.media_type or "other", r.url, r.hdurl,
                (r.explanation or "")[:2000], r.copyright,
            ))
            written += 1
        conn.commit()

        duration = time.time() - t0
        _record_metric("apod", len(rows), written, duration, "SUCCESS")
        return {"status": "ok", "records": written}
    except Exception as e:
        _record_metric("apod", 0, 0, time.time() - t0, "FAILED", str(e))
        return {"status": "error", "error": str(e)}


# -- SpaceX Launch Fetch ----------------------------------------------------------
def fetch_and_store_launches(force: bool = False) -> dict:
    if not force and not needs_refresh("spacex"):
        return {"status": "cached", "records": scalar("SELECT COUNT(*) FROM fact_launches")}

    t0 = time.time()
    try:
        launches = fetch_launches()

        conn = get_conn()
        written = 0
        for r in launches:
            lid = r.launch_id
            if not lid:
                continue
            conn.execute("""
                INSERT OR REPLACE INTO fact_launches
                (launch_id, mission_name, launch_date, rocket_name, launchpad_name, outcome, flight_number, details)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                lid, r.mission_name, r.launch_date, r.rocket_name, r.launchpad_name,
                r.outcome, r.flight_number, (r.details or "")[:500],
            ))
            written += 1
        conn.commit()

        conn.execute("DELETE FROM gold_launches_per_year")
        conn.execute("""
            INSERT INTO gold_launches_per_year (launch_year, outcome, launch_count)
            SELECT CAST(SUBSTR(launch_date,1,4) AS INTEGER), outcome, COUNT(*)
            FROM fact_launches
            WHERE launch_date IS NOT NULL AND LENGTH(launch_date) >= 4
            GROUP BY 1, 2
        """)
        conn.commit()

        duration = time.time() - t0
        _record_metric("spacex", len(launches), written, duration, "SUCCESS")
        return {"status": "ok", "records": written}
    except Exception as e:
        _record_metric("spacex", 0, 0, time.time() - t0, "FAILED", str(e))
        return {"status": "error", "error": str(e)}


# -- NASA Exoplanet Fetch ----------------------------------------------------------
def _esi_component(value: float, reference: float, weight: float) -> float:
    denom = value + reference
    if denom <= 0:
        return 0.0
    ratio = abs(value - reference) / denom
    return max(0.0, 1.0 - ratio) ** weight


def _esi_score(pl_rade, pl_bmasse, pl_orbper):
    if not (pl_rade and pl_bmasse and pl_orbper and pl_rade > 0 and pl_bmasse > 0 and pl_orbper > 0):
        return None
    esi_radius = _esi_component(pl_rade, EARTH_RADIUS_EARTH, 0.57)
    esi_mass = _esi_component(pl_bmasse, EARTH_MASS_EARTH, 0.70)
    esi_period = _esi_component(pl_orbper, EARTH_ORBITAL_PERIOD_DAYS, 0.70)
    score = round(esi_radius * esi_mass * esi_period, 4)
    in_hz = HZ_PERIOD_MIN_DAYS <= pl_orbper <= HZ_PERIOD_MAX_DAYS
    return score, in_hz


def _recompute_habitability_and_alert(conn, run_id: str = "direct-fetch") -> int:
    """Recompute habitability scores in SQLite directly (used by the fast
    'Refresh sources' path). The Airflow + PySpark pipeline path writes the
    same table from Spark's gold output instead, via load_gold_into_sqlite().
    """
    df = pd.read_sql_query(
        "SELECT pl_name, hostname, disc_year, discoverymethod, pl_radius_earth, "
        "pl_mass_earth, pl_orbital_period_days, sy_dist FROM fact_exoplanets",
        conn,
    )
    if df.empty:
        return 0

    previous_high = {
        row[0] for row in conn.execute(
            "SELECT pl_name FROM gold_habitability_candidates WHERE tier='HIGH'"
        ).fetchall()
    }

    conn.execute("DELETE FROM gold_habitability_candidates")

    scored_rows = []
    for row in df.itertuples(index=False):
        result = _esi_score(row.pl_radius_earth, row.pl_mass_earth, row.pl_orbital_period_days)
        if result is None:
            continue
        score, in_hz = result
        scored_rows.append((row, score, in_hz))

    # Tier against the observed score distribution rather than fixed
    # absolute cutoffs -- see spark/batch/exoplanet_transform.py for why:
    # strict ESI rarely exceeds ~0.3 for real archive data.
    scores_sorted = sorted(s for _, s, _ in scored_rows)
    if scores_sorted:
        p98 = scores_sorted[min(len(scores_sorted) - 1, int(len(scores_sorted) * 0.98))]
        p85 = scores_sorted[min(len(scores_sorted) - 1, int(len(scores_sorted) * 0.85))]
    else:
        p98 = p85 = 1.0

    new_alerts = []
    scored = 0
    for row, score, in_hz in scored_rows:
        tier = "HIGH" if score >= p98 else "MODERATE" if score >= p85 else "LOW"
        scored += 1
        conn.execute("""
            INSERT INTO gold_habitability_candidates
            (pl_name, hostname, disc_year, discoverymethod, pl_radius_earth, pl_mass_earth,
             pl_orbital_period_days, sy_dist, habitability_score, in_habitable_zone_proxy, tier)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            row.pl_name, row.hostname, row.disc_year, row.discoverymethod,
            row.pl_radius_earth, row.pl_mass_earth, row.pl_orbital_period_days,
            row.sy_dist, score, int(in_hz), tier,
        ))
        if tier == "HIGH" and row.pl_name not in previous_high:
            new_alerts.append((
                row.pl_name, score, tier,
                f"Newly identified HIGH-tier habitability candidate (score {score})",
                run_id,
            ))

    if new_alerts:
        conn.executemany(
            """INSERT INTO habitability_alerts (pl_name, habitability_score, tier, reason, run_id)
               VALUES (?,?,?,?,?)""",
            new_alerts,
        )
    conn.commit()
    logger.info("Habitability: scored %s planets, %s new HIGH-tier alerts", scored, len(new_alerts))
    return scored


def fetch_and_store_exoplanets(force: bool = False) -> dict:
    if not force and not needs_refresh("exoplanets"):
        return {"status": "cached", "records": scalar("SELECT COUNT(*) FROM fact_exoplanets")}

    t0 = time.time()
    try:
        records = fetch_exoplanets()

        conn = get_conn()
        written = 0
        for r in records:
            qf = "INCOMPLETE" if r.pl_rade is None else "OK"
            conn.execute("""
                INSERT OR REPLACE INTO fact_exoplanets
                (pl_name, hostname, disc_year, discoverymethod, pl_radius_earth, pl_mass_earth,
                 pl_orbital_period_days, sy_dist, quality_flag)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                r.pl_name, r.hostname, r.disc_year, r.discoverymethod,
                r.pl_rade, r.pl_bmasse, r.pl_orbper, r.sy_dist, qf,
            ))
            written += 1
        conn.commit()

        conn.execute("DELETE FROM gold_discoveries_per_year")
        conn.execute("""
            INSERT INTO gold_discoveries_per_year (disc_year, planet_count)
            SELECT disc_year, COUNT(*) FROM fact_exoplanets
            WHERE disc_year IS NOT NULL GROUP BY disc_year
        """)

        conn.execute("DELETE FROM gold_method_stats")
        conn.execute("""
            INSERT INTO gold_method_stats (method_name, planet_count, avg_orbital_period)
            SELECT discoverymethod, COUNT(*), AVG(pl_orbital_period_days)
            FROM fact_exoplanets
            WHERE discoverymethod IS NOT NULL
            GROUP BY discoverymethod
        """)
        conn.commit()

        medians_df = pd.read_sql_query(
            "SELECT discoverymethod, pl_radius_earth FROM fact_exoplanets WHERE pl_radius_earth IS NOT NULL",
            conn,
        )
        if not medians_df.empty:
            medians = medians_df.groupby("discoverymethod")["pl_radius_earth"].median().reset_index()
            medians.columns = ["method_name", "median_radius"]
            for _, row in medians.iterrows():
                conn.execute("UPDATE gold_method_stats SET median_radius=? WHERE method_name=?",
                             (row["median_radius"], row["method_name"]))
            conn.commit()

        _recompute_habitability_and_alert(conn)

        duration = time.time() - t0
        _record_metric("exoplanets", len(records), written, duration, "SUCCESS")
        return {"status": "ok", "records": written}
    except Exception as e:
        _record_metric("exoplanets", 0, 0, time.time() - t0, "FAILED", str(e))
        return {"status": "error", "error": str(e)}


def fetch_all_data(force: bool = False) -> dict:
    """Fetch all three sources concurrently (I/O-bound HTTP calls)."""
    fetchers = {
        "apod": lambda: fetch_and_store_apod(force=force),
        "spacex": lambda: fetch_and_store_launches(force=force),
        "exoplanets": lambda: fetch_and_store_exoplanets(force=force),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn): name for name, fn in fetchers.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as e:
                results[name] = {"status": "error", "error": str(e)}
    return results


# -- Pipeline-mode loader: Spark gold Parquet -> SQLite --------------------------
def _read_gold_parquet(relative_path: str) -> pd.DataFrame:
    path = DATA_LAKE / "gold" / relative_path
    if not path.exists():
        logger.warning("Gold path missing: %s", path)
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_gold_into_sqlite(run_id: str) -> dict:
    """Load the PySpark pipeline's Gold-layer Parquet output into SQLite.

    Called by the Airflow DAG's `load_serving_layer` task after
    run_spark_transformations has written data_lake/gold/**.
    """
    conn = get_conn()
    loaded = {}

    # Discovery trend
    df = _read_gold_parquet("discovery_trends/per_year")
    if not df.empty:
        conn.execute("DELETE FROM gold_discoveries_per_year")
        conn.executemany(
            "INSERT INTO gold_discoveries_per_year (disc_year, planet_count) VALUES (?,?)",
            list(df[["disc_year", "planet_count"]].itertuples(index=False, name=None)),
        )
        loaded["discoveries_per_year"] = len(df)

    # Method stats
    df = _read_gold_parquet("discovery_trends/method_stats")
    if not df.empty:
        conn.execute("DELETE FROM gold_method_stats")
        conn.executemany(
            """INSERT INTO gold_method_stats
               (method_name, planet_count, median_radius, avg_orbital_period) VALUES (?,?,?,?)""",
            list(df[["discoverymethod", "planet_count", "median_radius", "avg_orbital_period"]]
                 .itertuples(index=False, name=None)),
        )
        loaded["method_stats"] = len(df)

    # Launch analytics
    df = _read_gold_parquet("launch_analytics/per_year")
    if not df.empty:
        conn.execute("DELETE FROM gold_launches_per_year")
        conn.executemany(
            "INSERT INTO gold_launches_per_year (launch_year, outcome, launch_count) VALUES (?,?,?)",
            list(df[["launch_year", "outcome", "launch_count"]].itertuples(index=False, name=None)),
        )
        loaded["launches_per_year"] = len(df)

    # Habitability candidates -- the actual use-case output
    df = _read_gold_parquet("habitability/candidates")
    if not df.empty:
        previous_high = {
            row[0] for row in conn.execute(
                "SELECT pl_name FROM gold_habitability_candidates WHERE tier='HIGH'"
            ).fetchall()
        }
        conn.execute("DELETE FROM gold_habitability_candidates")
        conn.executemany(
            """INSERT INTO gold_habitability_candidates
               (pl_name, hostname, disc_year, discoverymethod, pl_radius_earth, pl_mass_earth,
                pl_orbital_period_days, sy_dist, habitability_score, in_habitable_zone_proxy, tier)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            list(df[["pl_name", "hostname", "disc_year", "discoverymethod", "pl_rade", "pl_bmasse",
                     "pl_orbper", "sy_dist", "habitability_score", "in_habitable_zone_proxy", "tier"]]
                 .itertuples(index=False, name=None)),
        )
        new_alerts = [
            (row.pl_name, row.habitability_score, row.tier,
             f"Newly identified HIGH-tier habitability candidate (score {row.habitability_score})", run_id)
            for row in df.itertuples(index=False)
            if row.tier == "HIGH" and row.pl_name not in previous_high
        ]
        if new_alerts:
            conn.executemany(
                """INSERT INTO habitability_alerts (pl_name, habitability_score, tier, reason, run_id)
                   VALUES (?,?,?,?,?)""",
                new_alerts,
            )
        loaded["habitability_candidates"] = len(df)
        loaded["new_habitability_alerts"] = len(new_alerts)

    conn.commit()
    logger.info("Loaded Spark gold layer into SQLite: %s", loaded)
    return loaded
