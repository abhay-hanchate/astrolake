"""
PySpark Batch Transformation -- NASA APOD
Bronze -> Silver (cleaned) + Gold (publication trends) on the local data lake.
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_LAKE = Path(__file__).resolve().parents[2] / "data_lake"


def get_spark():
    return (
        SparkSession.builder.appName("AstroLake-APOD")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run(batch_id: str = "*"):
    spark = get_spark()
    bronze_glob = str(DATA_LAKE / "bronze" / "apod" / f"batch_{batch_id}" / "data.parquet")
    df = spark.read.parquet(bronze_glob)
    print(f"[APOD Transform] Bronze rows: {df.count()}")

    silver = (
        df.dropDuplicates(["apod_date"])
        .withColumn("apod_date", F.to_date("apod_date", "yyyy-MM-dd"))
        .withColumn(
            "media_type",
            F.when(F.col("media_type") == "image", "image")
            .when(F.col("media_type") == "video", "video")
            .otherwise("other"),
        )
        .withColumn("explanation", F.substring(F.col("explanation"), 1, 2000))
        .filter(F.col("apod_date").isNotNull())
    )

    silver.write.mode("overwrite").parquet(str(DATA_LAKE / "silver" / "apod"))
    print(f"[APOD Transform] Silver rows: {silver.count()}")

    per_month = (
        silver.withColumn("year_month", F.date_format("apod_date", "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.count("apod_date").alias("pub_count"))
        .orderBy("year_month")
    )
    per_month.write.mode("overwrite").parquet(str(DATA_LAKE / "gold" / "apod_trends" / "per_month"))

    media_dist = silver.groupBy("media_type").agg(F.count("apod_date").alias("count"))
    media_dist.write.mode("overwrite").parquet(str(DATA_LAKE / "gold" / "apod_trends" / "media_dist"))

    print("[APOD Transform] Gold written.")
    row_count = silver.count()
    spark.stop()
    return row_count


if __name__ == "__main__":
    batch = sys.argv[1] if len(sys.argv) > 1 else "*"
    run(batch)
