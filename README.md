# AstroLake 🚀

**A real Apache Airflow + Apache Spark data engineering pipeline that scores confirmed exoplanets for habitability.**

No Docker. No Streamlit. No Kafka. No MinIO/Postgres. Everything runs as local processes — Airflow and PySpark inside WSL (Java is required), the dashboard as a FastAPI + vanilla HTML/JS app.

---

## The use case

AstroLake is not a visualization demo. Its actual output is a **ranked shortlist of exoplanet habitability candidates**, computed by a real PySpark batch job using an Earth Similarity Index (ESI)-style model:

```
ESI = (1 - |radius - 1| / (radius + 1))^0.57
    × (1 - |mass - 1|   / (mass + 1))^0.70
    × (1 - |period - 365.25| / (period + 365.25))^0.70
```

Every confirmed exoplanet with a measured radius, mass, and orbital period is scored against Earth. Because the archive is dominated by hot Jupiters and close-in super-Earths (a detection bias of the transit/radial-velocity methods), raw ESI scores rarely exceed ~0.3 even for genuinely interesting candidates — so tiers are set from the **observed score distribution** each run (top ~2% = `HIGH`, next ~13% = `MODERATE`), not a fixed threshold. The known potentially-habitable-zone planet **Kepler-22 b** consistently ranks in the top 3, which is a useful sanity check that the ranking is scientifically meaningful even though the absolute numbers are small.

The Airflow DAG acts on this: a dedicated `raise_habitability_alerts` task compares each run's `HIGH` tier against the previous run and logs/records any **newly discovered** high-tier candidate — the decision output a real pipeline run is for, not just a chart refresh.

---

## Architecture

```
NASA Exoplanet Archive ─┐
NASA APOD API           ─┼─→ Python ingestion clients
Launch Library 2        ─┘           │
                                      ▼
                        Local Bronze Parquet lake (data_lake/bronze/)
                                      │
                      Apache Airflow DAG (astrolake_data_pipeline)
                                      │
                   Apache Spark batch jobs, local[*] mode (no cluster)
                     Bronze → Silver (clean) → Gold (aggregates +
                     habitability scoring)          │
                                      ▼
                        SQLite serving layer (dashboard/astrolake_local.db)
                                      │
                         FastAPI + custom HTML/JS dashboard
```

Two ways data lands in SQLite:
- **Direct fetch** (`POST /api/refresh`, the "Refresh sources" button) — fetches all 3 sources concurrently and upserts straight into SQLite. Fast, good for a live demo, computes habitability in pure Python.
- **Full pipeline** (`POST /api/pipeline/run`, the "Run full Airflow + Spark pipeline" button on the Pipeline page) — runs the actual Airflow DAG: extraction → Bronze Parquet → real PySpark transforms (Silver/Gold + habitability scoring) → SQLite load → quality checks → alerting.

Both populate the same tables, so the dashboard looks the same either way — only the engineering path differs.

---

## Requirements

- **Dashboard only:** Python 3.10+ (Windows, macOS, or Linux)
- **Full pipeline (Airflow + Spark):** WSL2 (or native Linux/macOS), Python 3.12, a JDK (PySpark needs a JVM). Apache Airflow does not officially support native Windows.

---

## Quick start — dashboard only

```bash
python -m pip install -r dashboard/requirements.txt
python web_app.py
```

Open **http://localhost:8000**. Click **Refresh sources** to pull live data from NASA and Launch Library 2 into the local SQLite cache (`dashboard/astrolake_local.db`) — no Docker, no pipeline setup required.

---

## Full setup — Airflow + Spark pipeline (WSL)

```bash
# 1. Install WSL (if not already) and a Java runtime inside it
wsl --install
# inside WSL:
sudo apt update && sudo apt install -y openjdk-17-jdk-headless python3.12 python3.12-venv

# 2. Create the pipeline virtualenv
python3.12 -m venv ~/astrolake-venv
~/astrolake-venv/bin/pip install -r requirements-pipeline.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt

# 3. Point Airflow at this project's DAG
export AIRFLOW_HOME=~/airflow
mkdir -p ~/airflow/dags
ln -s /mnt/c/path/to/astrolake/airflow/dags/astrolake_data_pipeline.py ~/airflow/dags/
~/astrolake-venv/bin/airflow db migrate

# 4. Run the pipeline once, for today
export ASTROLAKE_PROJECT_ROOT=/mnt/c/path/to/astrolake
export ASTROLAKE_PYTHON_BIN=~/astrolake-venv/bin/python
~/astrolake-venv/bin/airflow dags test astrolake_data_pipeline $(date +%F)
```

Once set up, you can also trigger a real pipeline run from the dashboard itself: open the **Pipeline** page and click **Run full Airflow + Spark pipeline** (calls `POST /api/pipeline/run`, which shells out to `airflow dags test`).

---

## Dashboard pages

| Page | Description |
|------|-------------|
| 📊 Overview | KPIs, discovery trends, launch activity, top habitability candidates |
| 🪐 Exoplanets | Filter/compare 6,000+ confirmed exoplanets |
| 🚀 Launches | SpaceX mission history and success rates |
| 🔭 Observatory | NASA APOD image/video metadata browser |
| ⬡ **Habitability** | **The use case.** Ranked ESI-style habitability shortlist + new-discovery alerts |
| ⌁ Pipeline | Source health, DAG run history, quality gates, "Run full pipeline" trigger |

---

## Data sources

| Source | API | Used for |
|--------|-----|----------|
| NASA Exoplanet Archive | TAP interface | Exoplanet scientific data + habitability scoring |
| NASA APOD | science.nasa.gov APOD WordPress API | Astronomy image metadata |
| Launch Library 2 | ll.thespacedevs.com (SpaceX provider 121) | SpaceX launch records |
| Archived r/SpaceX snapshot | Public v5 JSON snapshot | Historical fallback when LL2's anonymous quota is exhausted |

---

## Project structure

```
astrolake/
├── airflow/dags/            # The real Airflow DAG (astrolake_data_pipeline.py)
├── ingestion/                # NASA/SpaceX API clients
├── spark/batch/              # PySpark batch transforms (local[*] mode) incl. habitability scoring
├── storage/bronze/           # Local Parquet bronze-layer writer
├── data_lake/                # bronze/ silver/ gold/ Parquet (created at runtime)
├── monitoring/                # Data quality checks (against SQLite)
├── dashboard/db_local.py     # SQLite serving layer + direct-fetch + gold-Parquet loader
├── web/                       # FastAPI-served HTML/CSS/JS dashboard
├── web_app.py                 # FastAPI server + analytics API + pipeline trigger
├── requirements-pipeline.txt  # Airflow + PySpark deps (install inside WSL/Linux)
└── dashboard/requirements.txt # Dashboard-only deps (any OS)
```

---

## Notes

- NASA APOD and the NASA Exoplanet Archive need no API key. Launch Library 2's free anonymous tier (15 req/hr) is enough; AstroLake falls back to its development mirror, then an archived static snapshot, if that's exhausted.
- Re-running the pipeline is idempotent: Spark writes use `mode("overwrite")`, SQLite gold tables are replaced wholesale on each load, and the Airflow DAG has `catchup=False` / `max_active_runs=1`.
- Habitability tiers are recalculated from each run's score distribution, not hardcoded thresholds — see "The use case" above.
