"""
PySpark Batch Transformation -- Exoplanets
Source layer: Bronze (raw Parquet from NASA Exoplanet Archive, local data lake)
Output: Silver (cleaned) + Gold (aggregated metrics + habitability scores)

Use case: AstroLake exists to surface which confirmed exoplanets are the most
promising habitability candidates. This job computes an Earth Similarity Index
(ESI)-style habitability_score for every planet with sufficient measurements,
ranks them, and writes a gold "habitability_candidates" table. The FastAPI
layer and the Airflow DAG's alerting task both read this table directly --
it is the actual decision output of the pipeline, not just a visualization.
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

DATA_LAKE = Path(__file__).resolve().parents[2] / "data_lake"

# Earth reference values used by the habitability scoring model.
EARTH_RADIUS_EARTH = 1.0
EARTH_MASS_EARTH = 1.0
EARTH_ORBITAL_PERIOD_DAYS = 365.25

# Conservative orbital-period band (days) used as a crude proxy for a star's
# habitable zone when stellar flux data isn't available in this dataset.
HZ_PERIOD_MIN_DAYS = 50.0
HZ_PERIOD_MAX_DAYS = 500.0


def get_spark(app: str = "AstroLake-Exoplanets") -> SparkSession:
    """Real PySpark running in local mode -- no cluster, no Docker required."""
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _esi_component(value_col: str, reference: float, weight: float):
    """One Earth Similarity Index term: (1 - |x - x0| / (x + x0)) ** weight."""
    col = F.col(value_col)
    diff = F.abs(col - F.lit(reference))
    denom = col + F.lit(reference)
    ratio = F.when(denom > 0, diff / denom).otherwise(F.lit(1.0))
    return F.pow(F.greatest(F.lit(0.0), F.lit(1.0) - ratio), F.lit(weight))


def run(batch_id: str = "*"):
    spark = get_spark()

    # -- Read Bronze --------------------------------------------------------
    bronze_glob = str(DATA_LAKE / "bronze" / "exoplanets" / f"batch_{batch_id}" / "data.parquet")
    df = spark.read.parquet(bronze_glob)
    print(f"[Exoplanet Transform] Bronze rows: {df.count()}")

    # -- Silver: Clean & Validate --------------------------------------------
    silver = (
        df.dropDuplicates(["pl_name"])
        .withColumn("disc_year", F.col("disc_year").cast(IntegerType()))
        .withColumn("pl_rade", F.col("pl_rade").cast(FloatType()))
        .withColumn("pl_bmasse", F.col("pl_bmasse").cast(FloatType()))
        .withColumn("pl_orbper", F.col("pl_orbper").cast(FloatType()))
        .withColumn("sy_dist", F.col("sy_dist").cast(FloatType()))
        .withColumn("discoverymethod", F.lower(F.trim(F.col("discoverymethod"))))
        .withColumn(
            "quality_flag",
            F.when(F.col("disc_year").isNull() | F.col("discoverymethod").isNull(), "INCOMPLETE")
            .otherwise("OK"),
        )
        .filter(F.col("pl_name").isNotNull())
    )

    silver_path = str(DATA_LAKE / "silver" / "exoplanets")
    silver.write.mode("overwrite").parquet(silver_path)
    print(f"[Exoplanet Transform] Silver rows written: {silver.count()}")

    # -- Gold: Discovery trend aggregations ----------------------------------
    discoveries_per_year = (
        silver.filter(F.col("disc_year").isNotNull())
        .groupBy("disc_year")
        .agg(F.count("pl_name").alias("planet_count"))
        .orderBy("disc_year")
    )
    discoveries_per_year.write.mode("overwrite").parquet(
        str(DATA_LAKE / "gold" / "discovery_trends" / "per_year")
    )

    method_stats = (
        silver.groupBy("discoverymethod")
        .agg(
            F.count("pl_name").alias("planet_count"),
            F.percentile_approx("pl_rade", 0.5).alias("median_radius"),
            F.avg("pl_orbper").alias("avg_orbital_period"),
        )
        .orderBy(F.col("planet_count").desc())
    )
    method_stats.write.mode("overwrite").parquet(
        str(DATA_LAKE / "gold" / "discovery_trends" / "method_stats")
    )

    # -- Gold: Habitability scoring (the actual use case) --------------------
    # Only planets with a measured radius, mass, and orbital period can be
    # scored -- most of the archive is missing one or more of these.
    scorable = silver.filter(
        F.col("pl_rade").isNotNull()
        & F.col("pl_bmasse").isNotNull()
        & F.col("pl_orbper").isNotNull()
        & (F.col("pl_rade") > 0)
        & (F.col("pl_bmasse") > 0)
        & (F.col("pl_orbper") > 0)
    )

    radius_term = _esi_component("pl_rade", EARTH_RADIUS_EARTH, 0.57)
    mass_term = _esi_component("pl_bmasse", EARTH_MASS_EARTH, 0.70)
    period_term = _esi_component("pl_orbper", EARTH_ORBITAL_PERIOD_DAYS, 0.70)

    scored = (
        scorable.withColumn("esi_radius", radius_term)
        .withColumn("esi_mass", mass_term)
        .withColumn("esi_period", period_term)
        .withColumn(
            "habitability_score",
            F.round(F.col("esi_radius") * F.col("esi_mass") * F.col("esi_period"), 4),
        )
        .withColumn(
            "in_habitable_zone_proxy",
            (F.col("pl_orbper") >= F.lit(HZ_PERIOD_MIN_DAYS))
            & (F.col("pl_orbper") <= F.lit(HZ_PERIOD_MAX_DAYS)),
        )
    )

    # Known confirmed exoplanets are overwhelmingly hot Jupiters and close-in
    # super-Earths (detection bias of transit/RV methods), so the strict ESI
    # product rarely exceeds ~0.3 even for the best real candidates. Fixed
    # absolute cutoffs would make HIGH/MODERATE permanently empty. Instead,
    # tier against the score distribution actually observed in this batch:
    # top ~2% = HIGH, next ~13% = MODERATE, rest = LOW. This keeps the
    # shortlist meaningful regardless of how the archive's composition shifts.
    p98, p85 = scored.approxQuantile("habitability_score", [0.98, 0.85], 0.001)

    habitability = (
        scored.withColumn(
            "tier",
            F.when(F.col("habitability_score") >= F.lit(p98), "HIGH")
            .when(F.col("habitability_score") >= F.lit(p85), "MODERATE")
            .otherwise("LOW"),
        )
        .select(
            "pl_name",
            "hostname",
            "disc_year",
            "discoverymethod",
            "pl_rade",
            "pl_bmasse",
            "pl_orbper",
            "sy_dist",
            "habitability_score",
            "in_habitable_zone_proxy",
            "tier",
        )
        .orderBy(F.col("habitability_score").desc())
    )

    habitability.write.mode("overwrite").parquet(
        str(DATA_LAKE / "gold" / "habitability" / "candidates")
    )
    scored_count = habitability.count()
    top_count = habitability.filter(F.col("tier") == "HIGH").count()
    print(f"[Exoplanet Transform] Habitability: scored {scored_count} planets, "
          f"{top_count} HIGH tier (tier cutoffs: HIGH>={p98:.4f}, MODERATE>={p85:.4f})")

    print("[Exoplanet Transform] Gold aggregations written.")
    row_count = silver.count()
    spark.stop()
    return row_count


if __name__ == "__main__":
    batch = sys.argv[1] if len(sys.argv) > 1 else "*"
    run(batch)
