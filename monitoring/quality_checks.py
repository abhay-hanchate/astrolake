"""
Data Quality Checks -- run after Spark transforms.
Records check results to the local SQLite serving database for the
Pipeline view in the dashboard.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CHECKS = [
    ("exoplanet_min_count", "SELECT COUNT(*) FROM fact_exoplanets", 100, ">="),
    ("launch_min_count", "SELECT COUNT(*) FROM fact_launches", 50, ">="),
    ("apod_min_count", "SELECT COUNT(*) FROM fact_apod_publications", 10, ">="),
    ("exo_no_null_name", "SELECT COUNT(*) FROM fact_exoplanets WHERE pl_name IS NULL", 0, "=="),
    ("launch_no_null_id", "SELECT COUNT(*) FROM fact_launches WHERE launch_id IS NULL", 0, "=="),
    ("exo_disc_year_valid", "SELECT COUNT(*) FROM fact_exoplanets WHERE disc_year < 1990 OR disc_year > 2030", 0, "=="),
    ("launch_outcome_valid", "SELECT COUNT(*) FROM fact_launches WHERE outcome NOT IN ('SUCCESS','FAILURE','UNKNOWN')", 0, "=="),
]


def run_all_checks(run_id: str) -> list[dict]:
    """Run all quality checks against the local SQLite database and persist results."""
    from dashboard.db_local import get_conn

    conn = get_conn()
    results = []

    for name, sql, threshold, op in CHECKS:
        value = conn.execute(sql).fetchone()[0] or 0
        passed = (value >= threshold) if op == ">=" else (value == threshold)
        results.append({
            "check_name": name,
            "value": value,
            "threshold": threshold,
            "operator": op,
            "passed": passed,
        })
        logger.info("Quality check [%s]: value=%s threshold=%s passed=%s", name, value, threshold, passed)

    conn.executemany(
        """INSERT INTO quality_check_history
           (run_id, check_name, value, threshold, passed, checked_at)
           VALUES (?,?,?,?,?,?)""",
        [
            (run_id, r["check_name"], r["value"], r["threshold"], int(r["passed"]),
             datetime.now(timezone.utc).isoformat())
            for r in results
        ],
    )
    conn.commit()
    return results
