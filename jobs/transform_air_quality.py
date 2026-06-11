"""
PySpark transformation job (Spark LOCAL mode — no YARN, CPU-only).

Run by Airflow's `run_pyspark_transformation` task via:
    spark-submit --master local[*] jobs/transform_air_quality.py \
        --raw-dir /opt/airflow/data/raw \
        --out-dir /opt/airflow/data/curated \
        --run-date 2026-06-11

What it does:
  1. Reads raw air-quality + weather JSON from the local data lake
     (these files were also uploaded to S3's `raw/` layer by an earlier task).
  2. Flattens & cleans the records.
  3. Joins air-quality with weather on (city, date).
  4. Aggregates to daily city-level metrics:
       city, date, pm25_avg, pm10_avg, no2_avg,
       temperature_avg, humidity_avg, pollution_category
  5. Writes the curated output as Parquet AND CSV to the `curated/` layer.

A separate Airflow task (`load_curated_data_to_postgres`) then loads the CSV
into PostgreSQL, keeping Spark free of JDBC-driver setup for this MVP.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

# Make `src` importable when run via spark-submit from the project root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from src.transform.pollution_logic import categorize_pm25


def build_spark(app_name: str = "air_quality_transform") -> SparkSession:
    """Create a local-mode SparkSession (CPU-only, single JVM)."""
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")  # small data → few partitions
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# Register the pure-Python pollution logic as a Spark UDF so the same rules
# are used everywhere (API, tests, Spark).
categorize_udf = F.udf(lambda v: categorize_pm25(v), StringType())


def transform(spark: SparkSession, raw_dir: str, run_date: str):
    """Core transformation. Returns the curated Spark DataFrame.

    Separated from I/O so tests can call it with tiny in-memory DataFrames.
    """
    aq_path = os.path.join(raw_dir, "air_quality")
    wx_path = os.path.join(raw_dir, "weather")

    air = spark.read.option("multiLine", True).json(aq_path)
    weather = spark.read.option("multiLine", True).json(wx_path)

    return transform_frames(air, weather, run_date)


def transform_frames(air, weather, run_date: str):
    """The join + aggregation logic, taking already-loaded DataFrames.

    This is the unit-testable heart of the job (see test_spark_transform.py).
    """
    # --- Clean & cast air-quality data ---
    air = (
        air.select(
            F.col("city").cast("string").alias("city"),
            F.coalesce(F.col("date").cast("string"), F.lit(run_date)).alias("date"),
            F.col("pm25").cast("double").alias("pm25"),
            F.col("pm10").cast("double").alias("pm10"),
            F.col("no2").cast("double").alias("no2"),
        )
        .where(F.col("city").isNotNull())
    )

    air_agg = air.groupBy("city", "date").agg(
        F.round(F.avg("pm25"), 2).alias("pm25_avg"),
        F.round(F.avg("pm10"), 2).alias("pm10_avg"),
        F.round(F.avg("no2"), 2).alias("no2_avg"),
    )

    # --- Clean & cast weather data ---
    weather = (
        weather.select(
            F.col("city").cast("string").alias("city"),
            F.coalesce(F.col("date").cast("string"), F.lit(run_date)).alias("date"),
            F.col("temperature").cast("double").alias("temperature"),
            F.col("humidity").cast("double").alias("humidity"),
        )
        .where(F.col("city").isNotNull())
    )

    weather_agg = weather.groupBy("city", "date").agg(
        F.round(F.avg("temperature"), 2).alias("temperature_avg"),
        F.round(F.avg("humidity"), 2).alias("humidity_avg"),
    )

    # --- Join air-quality with weather on (city, date) ---
    curated = air_agg.join(weather_agg, on=["city", "date"], how="left")

    # --- Derive pollution category from PM2.5 average ---
    curated = curated.withColumn("pollution_category", categorize_udf(F.col("pm25_avg")))

    return curated.select(
        "city",
        "date",
        "pm25_avg",
        "pm10_avg",
        "no2_avg",
        "temperature_avg",
        "humidity_avg",
        "pollution_category",
    )


def write_outputs(curated, out_dir: str, run_date: str) -> str:
    """Write Parquet + a single CSV. Returns the CSV path for the loader task."""
    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "air_quality_metrics_parquet")
    csv_dir = os.path.join(out_dir, "air_quality_metrics_csv")

    curated.write.mode("overwrite").parquet(parquet_path)
    # coalesce(1) → one CSV file, easy for the downstream pandas/SQL loader.
    curated.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_dir)

    print(f"[transform] Wrote Parquet to {parquet_path}")
    print(f"[transform] Wrote CSV to {csv_dir}")
    return csv_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Air Quality PySpark transformation")
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/airflow/data/raw"))
    parser.add_argument("--out-dir", default=os.getenv("CURATED_DIR", "/opt/airflow/data/curated"))
    parser.add_argument("--run-date", default=date.today().isoformat())
    args = parser.parse_args()

    spark = build_spark()
    try:
        curated = transform(spark, args.raw_dir, args.run_date)
        count = curated.count()
        if count == 0:
            print("[transform] WARNING: produced 0 curated rows.", file=sys.stderr)
        write_outputs(curated, args.out_dir, args.run_date)
        print(f"[transform] Done. Curated rows: {count}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
