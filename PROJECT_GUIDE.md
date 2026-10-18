# AstroLake — Project Guide

**A real Apache Airflow + Apache Spark data engineering pipeline that scores confirmed exoplanets for habitability.**

This document is written for presenting the project to judges: what it is, why it matters, exactly how data flows through it, and how to demo it live.

---

## 1. The one-sentence pitch

AstroLake ingests live data from three public space APIs, runs it through a real Apache Airflow + Apache Spark pipeline (Bronze → Silver → Gold, the industry-standard "medallion" architecture), and the pipeline's actual output is a **ranked, scientifically-grounded shortlist of exoplanet habitability candidates** — not just a dashboard of charts.

If a judge asks "what does this *do*, beyond showing pretty graphs" — this is the answer: **it decides which of 6,000+ confirmed exoplanets are the most Earth-like, and alerts you when a newly discovered one jumps into the top tier.**

---

## 2. Why this use case (not just "a dashboard")

Most student data-engineering projects stop at: fetch data → clean it → chart it. That's valuable, but it's visualization, not a *decision*.

AstroLake's pipeline produces an actual decision output: an **Earth Similarity Index (ESI)-style habitability score** for every confirmed exoplanet with a measured radius, mass, and orbital period. This is a real, published method used in astrobiology (Schulze-Makuch et al., 2011) for comparing exoplanets to Earth.

```
ESI = (1 - |radius - 1| / (radius + 1))^0.57
    × (1 - |mass - 1|   / (mass + 1))^0.70
    × (1 - |period - 365.25| / (period + 365.25))^0.70
```

Each term compares one planetary property to Earth's (radius = 1 Earth radius, mass = 1 Earth mass, orbital period = 365.25 days). A perfect Earth twin would score 1.0.

**A key engineering decision worth highlighting to judges:** real confirmed exoplanets are heavily biased toward "hot Jupiters" and close-in super-Earths, because the transit and radial-velocity detection methods find big, close-in planets far more easily than Earth-sized ones in Earth-like orbits. This means raw ESI scores in the real archive almost never exceed ~0.3, even for the best known candidates. A naive implementation with fixed thresholds (e.g. "HIGH tier = score ≥ 0.80") would leave the HIGH tier **permanently empty** — we actually hit this bug during development.

The fix: tiers are computed from the **score distribution observed in each run** (top ~2% of scored planets = `HIGH`, next ~13% = `MODERATE`), not a hardcoded cutoff. This keeps the shortlist meaningful no matter how the underlying data shifts. As a sanity check, **Kepler-22 b** — a real, well-known potentially-habitable-zone exoplanet discovered by NASA's Kepler mission — consistently ranks in the top 3 of our HIGH tier. That's the kind of detail that shows judges the model is doing something scientifically sound, not just producing numbers.

---

## 3. Architecture overview

```
NASA Exoplanet Archive ─┐
NASA APOD API           ─┼─→ Python ingestion clients (typed, retrying, rate-limit aware)
Launch Library 2        ─┘           │
                                      ▼
                   Local Bronze Parquet lake (data_lake/bronze/<source>/batch_<run_id>/)
                     — raw records, exactly as received from the API, written once per run
                                      │
                      Apache Airflow DAG: astrolake_data_pipeline
                     — orchestrates every step below, in order, with retries
                                      │
                   Apache Spark batch jobs, running in local[*] mode
                     (a real, independent JVM-backed Spark session — no cluster needed)
                     Bronze → Silver (cleaned, deduplicated, typed)
                            → Gold (aggregates + habitability scoring)
                                      │
                        SQLite serving layer (dashboard/astrolake_local.db)
                     — the fast, queryable store the dashboard actually reads from
                                      │
                         FastAPI + custom HTML/JS dashboard (http://localhost:8000)
```

### Why these specific tools

| Tool | Role | Why it's the right fit here |
|---|---|---|
| **Apache Airflow** | Orchestration | Defines the pipeline as a DAG (Directed Acyclic Graph) of 13 tasks with explicit dependencies, retries, and failure handling — the industry-standard way to schedule and monitor data pipelines. |
| **Apache Spark (PySpark)** | Transformation | Runs the actual Bronze→Silver→Gold cleaning, deduplication, and the habitability-scoring computation, using Spark's DataFrame API and distributed-style execution (`local[*]` uses all CPU cores, same engine as a production cluster). |
| **Parquet** | Storage format | Columnar, compressed, schema-aware — the standard format for data lakes. Each stage (bronze/silver/gold) is a real set of Parquet files on disk you can inspect yourself. |
| **SQLite** | Serving layer | Fast, zero-setup, queryable store for the dashboard. Swappable for PostgreSQL in a production deployment without changing any other code. |
| **FastAPI** | API + UI host | Serves both the JSON analytics API and the dashboard's static files from one lightweight Python process. |

### What's deliberately *not* used, and why

- **No Docker.** Every component (Airflow, Spark, the API, the data lake) runs as a plain local process. This was a deliberate simplification: it removes an entire layer of infrastructure complexity (containers, networking, volumes) while keeping the actual data engineering — DAGs, Spark jobs, Parquet layers — completely real.
- **No Kafka.** A real Kafka broker needs a JVM-backed daemon process running continuously; without Docker, that's an awkward manual install for a local demo and doesn't change what the pipeline *computes*. The batch pipeline (Airflow + Spark) already demonstrates the core data-engineering skills.
- **No Streamlit.** The dashboard is a custom FastAPI + HTML/CSS/JS single-page app instead, giving full control over the UI (including the live pipeline-lineage visualization described below) rather than relying on a pre-built component library.

---

## 4. The data flow, stage by stage (exactly what happens on a pipeline run)

This is the Airflow DAG (`airflow/dags/astrolake_data_pipeline.py`), task by task, in the order it actually executes:

1. **`check_source_availability`** — pings all 3 source APIs (NASA Exoplanet Archive, NASA APOD, Launch Library 2) with a lightweight request. Aborts the whole run only if *all three* are unreachable.
2. **`extract.*`** (parallel task group) — three tasks run concurrently, each calling a typed ingestion client:
   - `extract_exoplanet_data` → `ingestion/exoplanets/client.py` (queries NASA's TAP/ADQL interface)
   - `extract_nasa_apod_metadata` → `ingestion/nasa_apod/client.py` (queries NASA's APOD WordPress REST API)
   - `extract_spacex_launch_data` → `ingestion/spacex/client.py` (queries Launch Library 2, with a 3-tier fallback — see §5)
3. **`store_raw_data`** — takes whatever each extraction task fetched and writes it as Parquet into the **Bronze layer**: `data_lake/bronze/<source>/batch_<run_id>/data.parquet`. This is raw, untouched data — the permanent, replayable record of exactly what the API returned.
4. **`run_spark_transformations`** — submits three independent PySpark jobs (`spark/batch/{exoplanet,launch,apod}_transform.py`), each of which:
   - Reads its Bronze Parquet
   - Deduplicates, casts types, normalizes strings, flags incomplete records → writes **Silver** (`data_lake/silver/<source>/`)
   - Computes aggregates (discoveries per year, method stats, launches per year/rocket, APOD per month) → writes **Gold** (`data_lake/gold/...`)
   - *(exoplanets only)* Computes the ESI-style habitability score for every scorable planet, assigns percentile-based tiers, and writes `data_lake/gold/habitability/candidates/`
5. **`validate_transformed_data`** — confirms each source's Silver layer actually has Parquet output on disk before continuing (a real data-quality gate, not a formality).
6. **`load_serving_layer`** — reads every Gold table from Parquet and loads it into the SQLite serving database (`dashboard/astrolake_local.db`), including the habitability candidates.
7. **`run_analytics_quality_checks`** — runs 7 SQL assertions against SQLite (minimum row counts, no null keys, valid year ranges, valid outcome enums) and records pass/fail per check.
8. **`raise_habitability_alerts`** — compares this run's `HIGH`-tier habitability candidates against the previous run's. Any planet that's newly promoted to `HIGH` tier gets logged as an alert and recorded in the `habitability_alerts` table — **this is the pipeline acting on what it computed**, not just displaying it.
9. **`record_pipeline_metrics`** — writes a summary row (run ID, status, total records processed, alert count) to `pipeline_run_history`, visible in the dashboard's Pipeline page.

### How to *see* this flow, live, in the dashboard

The **Pipeline** page (sidebar item 06) has a section called **"Live record lineage, per source"** — three horizontal funnel bars (one per source: Exoplanets / Launches / APOD), each showing four stages: Bronze → Silver → Gold → SQLite, with the **actual row count measured from disk** at each stage, right now. This is backed by a dedicated endpoint (`GET /api/pipeline/lineage`) that reads Parquet file metadata directly — every number shown is real, not mocked or hardcoded. Watching the Exoplanets bar go from 6,445 (Bronze/Silver) down to 1,554 (Gold — only planets with complete radius/mass/period measurements can be scored) is a visual, honest representation of what the Spark job actually filtered.

---

## 5. Data sources

| Source | API | What it provides | Auth needed? |
|---|---|---|---|
| **NASA Exoplanet Archive** | TAP (Table Access Protocol) / ADQL query interface at `exoplanetarchive.ipac.caltech.edu` | ~6,400+ confirmed exoplanets: name, host star, discovery year/method, radius, mass, orbital period, distance | No |
| **NASA APOD** (Astronomy Picture of the Day) | `science.nasa.gov` WordPress REST API | Daily astronomy image/video metadata: title, media URL, explanation, credit | No |
| **Launch Library 2 (The Space Devs)** | `ll.thespacedevs.com`, filtered to SpaceX (agency ID 121) | SpaceX launch history: mission name, date, rocket, launchpad, outcome | No (free tier, rate-limited) |
| *Fallback:* archived r/SpaceX snapshot | Static JSON on GitHub | Historical launch data, used only if Launch Library 2's free quota (15 req/hr) is exhausted | No |

**Resilience detail worth mentioning to judges:** the SpaceX client has a 3-tier fallback chain — production API → official development mirror → archived static snapshot — merged by launch ID so there's never duplicate data. The Airflow DAG's Bronze-write step also uses `NONE_FAILED_MIN_ONE_SUCCESS` trigger logic, meaning the pipeline keeps going even if one source is temporarily down, rather than failing the entire run over one flaky API.

---

## 6. How to demo this to judges (suggested script)

**1. Open the dashboard** (`http://localhost:8000`) — start on **Overview**. Point out the live KPIs (confirmed exoplanets, launches, APOD count, mission success rate) and the "Top habitability candidates" panel — note that these numbers come from a live SQLite database the pipeline populated, not a static file.

**2. Go to Habitability** (sidebar 05) — this is the use case. Show the tier breakdown (HIGH/MODERATE/LOW), point out Kepler-22 b in the ranked table, and explain the ESI formula and why tiers are percentile-based rather than fixed (§2 above — this shows engineering judgment, a good thing to narrate).

**3. Go to Pipeline** (sidebar 06):
   - Point at the 6-stage architecture diagram (Source APIs → Bronze → Airflow → Spark → Serving → FastAPI).
   - Scroll to **"Live record lineage, per source"** — this is the money shot. Explain that these bars are measured live from Parquet files on disk, right now, and walk through why Gold is smaller than Bronze/Silver for Exoplanets specifically (only scorable planets make it through).
   - Click **"Run full Airflow + Spark pipeline"** — this actually shells out to `airflow dags test astrolake_data_pipeline <today>`, which re-runs the entire 13-task DAG end-to-end: live API calls → Bronze → real Spark jobs → Gold → SQLite → quality checks → alerting. Takes 1–3 minutes; while it runs, explain what's happening at each stage using the DAG task list in §4.
   - When it finishes, the lineage bars and DAG Run History table update with the new numbers — proof the whole thing just ran for real, not from a cached recording.
   - Scroll to the **Audit Log** — real per-source ingestion metrics (records fetched/written, duration, status) going back through every run so far.

**4. (Optional, if asked "can we see the raw files")** Open `data_lake/bronze/`, `data_lake/silver/`, `data_lake/gold/` in a file explorer or terminal — these are real Parquet files with timestamps matching what was just shown in the dashboard. This is strong proof the pipeline is real, not simulated.

---

## 7. Anticipated judge questions

**"Why no Docker?"**
A deliberate simplification for local, judge-machine-friendly demoing. Docker would add container/network/volume complexity without changing what's actually being engineered — the DAG structure, the Spark transforms, and the medallion data lake are all real either way. The README documents how to containerize it if that were ever required for production deployment.

**"Is the Spark job really doing anything, or could this be pandas?"**
It's a real PySpark `SparkSession` in `local[*]` mode — a genuine, independently-started Java/Spark process (confirmed by watching `java` show up in the process list during a run), using Spark's DataFrame API (`groupBy`, `percentile_approx`, `approxQuantile`, window-style aggregations) — the same code that would run unmodified against a real multi-node Spark cluster by changing one config line (`.master("local[*]")` → `.master("spark://cluster:7077")`).

**"Why SQLite instead of Postgres?"**
SQLite is the serving layer for a zero-setup local demo. The data-access pattern (`dashboard/db_local.py`) is isolated behind simple functions (`query_df`, `scalar`, `load_gold_into_sqlite`), so swapping in Postgres means changing the connection logic in one file — nothing about the Airflow DAG or Spark jobs would need to change.

**"What happens if a source API is down during the demo?"**
The pipeline is built for exactly this. `check_source_availability` only aborts if *all three* sources fail. `store_raw_data` uses `NONE_FAILED_MIN_ONE_SUCCESS`, so Bronze gets written for whatever sources succeeded. The SpaceX client specifically has a 3-level fallback (see §5). Worst case, the demo falls back to data from a previous successful run, already sitting in `data_lake/` and SQLite.

**"Why does the habitability score look so low (max ~0.3) even for the best candidates — is that a bug?"**
No — explained in detail in §2. It's a real property of the ESI formula applied to the actual confirmed-exoplanet population, which is dominated by planets very unlike Earth due to detection bias. The tiering is deliberately calibrated against the *observed* distribution for exactly this reason, and Kepler-22 b landing in the top 3 is the sanity check that the relative ranking (not the absolute number) is scientifically meaningful.

**"What's the actual business/scientific value here?"**
This mirrors a real workflow at places like NASA's Exoplanet Science Institute or SETI-adjacent research groups: triaging a large, constantly-growing catalog of confirmed exoplanets down to a small, ranked shortlist worth follow-up observation time (which is expensive and limited). The alerting mechanism (`raise_habitability_alerts`) mirrors how a production system would notify researchers only when something new and promising appears, rather than making them re-scan the whole archive by hand.

---

## 8. Where to look in the code

| What | File |
|---|---|
| The Airflow DAG (orchestration) | `airflow/dags/astrolake_data_pipeline.py` |
| Habitability scoring (the use case) | `spark/batch/exoplanet_transform.py` |
| Other Spark transforms | `spark/batch/launch_transform.py`, `spark/batch/apod_transform.py` |
| Ingestion clients | `ingestion/exoplanets/`, `ingestion/nasa_apod/`, `ingestion/spacex/` |
| Bronze-layer writer | `storage/bronze/writer.py` |
| SQLite serving layer + direct-fetch fallback | `dashboard/db_local.py` |
| Data quality checks | `monitoring/quality_checks.py` |
| FastAPI server + all API endpoints | `web_app.py` |
| Dashboard frontend | `web/index.html`, `web/app.js`, `web/styles.css` |

See `README.md` for setup instructions (dashboard-only vs. full Airflow+Spark pipeline).
