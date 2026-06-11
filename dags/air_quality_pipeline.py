"""
Airflow DAG: air_quality_pipeline

Orchestrates the full Air Quality Intelligence Platform pipeline.

Task flow:
    extract_air_quality_data  ┐
                              ├─> upload_raw_data_to_s3 ─> run_pyspark_transformation
    extract_weather_data      ┘                                      │
                                                                     v
                                          load_curated_data_to_postgres ─> data_quality_check

How components communicate here:
  - Python tasks import `src.*` (extract/load) and write raw JSON to the
    shared, Docker-mounted data lake at /opt/airflow/data/raw.
  - upload_raw_data_to_s3 pushes that raw layer to S3 (skips if no creds).
  - run_pyspark_transformation shells out to `spark-submit` in LOCAL mode,
    running jobs/transform_air_quality.py (Spark lives in the same image).
  - load_curated_data_to_postgres reads the curated CSV and upserts into
    PostgreSQL (air_quality_metrics).
  - data_quality_check validates the loaded data and records results.

FastAPI triggers this DAG through the Airflow REST API (POST .../dagRuns).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Project root inside the Airflow container (mounted via docker-compose).
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CURATED_DIR = os.path.join(DATA_DIR, "curated")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# --------------------------------------------------------------------------- #
# Task callables (kept thin; real logic lives in src/*)
# --------------------------------------------------------------------------- #
def _run_id_from_context(context) -> str:
    return context["dag_run"].run_id if context.get("dag_run") else context["run_id"]


def task_extract_air_quality(**context):
    from src.extract.air_quality_api import extract_air_quality

    run_date = context["ds"]
    records = extract_air_quality(run_date=run_date)
    out_dir = os.path.join(RAW_DIR, "air_quality")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{run_date}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh)
    print(f"Wrote {len(records)} air-quality records -> {out_path}")
    return out_path


def task_extract_weather(**context):
    from src.extract.weather_api import extract_weather

    run_date = context["ds"]
    records = extract_weather(run_date=run_date)
    out_dir = os.path.join(RAW_DIR, "weather")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{run_date}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh)
    print(f"Wrote {len(records)} weather records -> {out_path}")
    return out_path


def task_upload_raw_to_s3(**context):
    from src.load.s3_loader import upload_directory
    from src.utils.config import settings

    run_id = _run_id_from_context(context)
    # Mark the run as started in PostgreSQL (best-effort).
    try:
        from src.load.postgres_loader import record_run_start

        record_run_start(run_id=run_id, dag_run_id=run_id)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not record run start: {exc}")

    count = upload_directory(RAW_DIR, settings.s3_raw_prefix)
    print(f"Uploaded {count} raw files to S3 (0 means S3 not configured — that's OK for local runs).")
    return count


def task_load_curated_to_postgres(**context):
    import glob

    import pandas as pd

    from src.load.postgres_loader import record_run_finish, upsert_metrics
    from src.utils.config import settings

    run_id = _run_id_from_context(context)
    csv_dir = os.path.join(CURATED_DIR, "air_quality_metrics_csv")
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No curated CSV found in {csv_dir}. Did the Spark task run?")

    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    written = upsert_metrics(rows, run_id=run_id)

    # Optionally push the curated layer to S3's processed prefix.
    try:
        from src.load.s3_loader import upload_directory

        upload_directory(CURATED_DIR, settings.s3_processed_prefix)
    except Exception as exc:  # noqa: BLE001
        print(f"Processed-layer S3 upload skipped: {exc}")

    record_run_finish(run_id=run_id, status="success", rows_processed=written, notes="curated loaded")
    print(f"Loaded {written} curated rows into PostgreSQL.")
    return written


def task_data_quality_check(**context):
    from src.load.postgres_loader import fetch_latest_metrics, record_quality_result

    run_id = _run_id_from_context(context)
    metrics = fetch_latest_metrics(limit=1000)

    checks = []

    # Check 1: we actually have rows.
    not_empty = len(metrics) > 0
    checks.append(("non_empty_metrics", not_empty, f"row_count={len(metrics)}"))

    # Check 2: PM2.5 averages are within a sane physical range (or null).
    bad_pm25 = [
        m for m in metrics
        if m.get("pm25_avg") is not None and not (0 <= float(m["pm25_avg"]) <= 1000)
    ]
    checks.append(("pm25_in_range", len(bad_pm25) == 0, f"out_of_range={len(bad_pm25)}"))

    # Check 3: every row has a pollution category.
    missing_cat = [m for m in metrics if not m.get("pollution_category")]
    checks.append(("category_present", len(missing_cat) == 0, f"missing={len(missing_cat)}"))

    all_passed = True
    for name, passed, details in checks:
        record_quality_result(run_id=run_id, check_name=name, passed=passed, details=details)
        all_passed = all_passed and passed
        print(f"DQ check '{name}': {'PASS' if passed else 'FAIL'} ({details})")

    if not all_passed:
        raise ValueError("One or more data-quality checks failed. See data_quality_results table.")
    return "all_checks_passed"


with DAG(
    dag_id="air_quality_pipeline",
    description="Fetch air-quality + weather, process with PySpark, load to PostgreSQL.",
    default_args=DEFAULT_ARGS,
    schedule=None,  # triggered manually / via FastAPI; set a cron later if desired
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["air-quality", "spark", "s3", "postgres"],
) as dag:

    extract_air_quality_data = PythonOperator(
        task_id="extract_air_quality_data",
        python_callable=task_extract_air_quality,
    )

    extract_weather_data = PythonOperator(
        task_id="extract_weather_data",
        python_callable=task_extract_weather,
    )

    upload_raw_data_to_s3 = PythonOperator(
        task_id="upload_raw_data_to_s3",
        python_callable=task_upload_raw_to_s3,
    )

    # Spark LOCAL mode — spark-submit runs inside the Airflow image (Java + pyspark).
    run_pyspark_transformation = BashOperator(
        task_id="run_pyspark_transformation",
        bash_command=(
            "spark-submit --master local[*] "
            f"{PROJECT_ROOT}/jobs/transform_air_quality.py "
            f"--raw-dir {RAW_DIR} --out-dir {CURATED_DIR} --run-date {{{{ ds }}}}"
        ),
        env={
            "PYTHONPATH": PROJECT_ROOT,
            "SPARK_MASTER": "local[*]",
        },
        append_env=True,
    )

    load_curated_data_to_postgres = PythonOperator(
        task_id="load_curated_data_to_postgres",
        python_callable=task_load_curated_to_postgres,
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=task_data_quality_check,
    )

    # Wiring (the arrows from the docstring):
    [extract_air_quality_data, extract_weather_data] >> upload_raw_data_to_s3
    upload_raw_data_to_s3 >> run_pyspark_transformation
    run_pyspark_transformation >> load_curated_data_to_postgres >> data_quality_check
