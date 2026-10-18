"""
PySpark Batch Transformation -- SpaceX Launches
Bronze -> Silver (cleaned launches) + Gold (launch analytics), all on the
local data lake (no MinIO / S3 required).
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

DATA_LAKE = Path(__file__).resolve().parents[2] / "data_lake"


def get_spark():
    return (
        SparkSession.builder.appName("AstroLake-Launches")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run(batch_id: str = "*"):
    spark = get_spark()

    bronze_glob = str(DATA_LAKE / "bronze" / "launches" / f"batch_{batch_id}" / "data.parquet")
    df = spark.read.parquet(bronze_glob)
    print(f"[Launch Transform] Bronze rows: {df.count()}")

    silver = (
        df.dropDuplicates(["launch_id"])
        .withColumn("launch_date", F.to_timestamp("launch_date"))
        .withColumn("launch_year", F.year("launch_date").cast(IntegerType()))
        .withColumn(
            "outcome",
            F.when(F.col("outcome") == "SUCCESS", "SUCCESS")
            .when(F.col("outcome") == "FAILURE", "FAILURE")
            .otherwise("UNKNOWN"),
        )
        .filter(F.col("launch_id").isNotNull())
    )

    silver.write.mode("overwrite").parquet(str(DATA_LAKE / "silver" / "launches"))
    print(f"[Launch Transform] Silver rows: {silver.count()}")

    per_year = (
        silver.filter(F.col("launch_year").isNotNull())
        .groupBy("launch_year", "outcome")
        .agg(F.count("launch_id").alias("launch_count"))
        .orderBy("launch_year")
    )
    per_year.write.mode("overwrite").parquet(str(DATA_LAKE / "gold" / "launch_analytics" / "per_year"))

    per_rocket = (
        silver.groupBy("rocket_name")
        .agg(F.count("launch_id").alias("launch_count"))
        .orderBy(F.col("launch_count").desc())
    )
    per_rocket.write.mode("overwrite").parquet(str(DATA_LAKE / "gold" / "launch_analytics" / "per_rocket"))

    print("[Launch Transform] Gold written.")
    row_count = silver.count()
    spark.stop()
    return row_count


if __name__ == "__main__":
    batch = sys.argv[1] if len(sys.argv) > 1 else "*"
    run(batch)
