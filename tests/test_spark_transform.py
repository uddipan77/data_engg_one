"""
PySpark transformation tests using tiny in-memory DataFrames.

These tests start a local SparkSession. They require a JVM (Java) to be
available. If Spark/Java is not present (e.g. a minimal environment), the
tests are skipped rather than failing the whole suite.
"""
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from jobs.transform_air_quality import transform_frames  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            SparkSession.builder.appName("test_air_quality")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - no JVM available
        pytest.skip(f"Spark could not start (no JVM?): {exc}")
    yield session
    session.stop()


def test_transform_joins_and_aggregates(spark):
    air = spark.createDataFrame(
        [
            {"city": "Berlin", "date": "2026-06-11", "pm25": 10.0, "pm10": 18.0, "no2": 12.0},
            {"city": "Berlin", "date": "2026-06-11", "pm25": 14.0, "pm10": 22.0, "no2": 16.0},
            {"city": "Delhi", "date": "2026-06-11", "pm25": 180.0, "pm10": 250.0, "no2": 40.0},
        ]
    )
    weather = spark.createDataFrame(
        [
            {"city": "Berlin", "date": "2026-06-11", "temperature": 20.0, "humidity": 60.0},
            {"city": "Delhi", "date": "2026-06-11", "temperature": 35.0, "humidity": 50.0},
        ]
    )

    result = {r["city"]: r.asDict() for r in transform_frames(air, weather, "2026-06-11").collect()}

    # Berlin: pm25 avg = 12.0 -> "Good"
    assert result["Berlin"]["pm25_avg"] == 12.0
    assert result["Berlin"]["temperature_avg"] == 20.0
    assert result["Berlin"]["pollution_category"] == "Good"

    # Delhi: pm25 avg = 180 -> "Very Unhealthy"
    assert result["Delhi"]["pm25_avg"] == 180.0
    assert result["Delhi"]["pollution_category"] == "Very Unhealthy"


def test_transform_handles_missing_weather(spark):
    air = spark.createDataFrame(
        [{"city": "Paris", "date": "2026-06-11", "pm25": 30.0, "pm10": 40.0, "no2": 20.0}]
    )
    # Empty weather frame (same schema) -> left join keeps the air row.
    weather = spark.createDataFrame(
        [],
        "city string, date string, temperature double, humidity double",
    )

    rows = transform_frames(air, weather, "2026-06-11").collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["city"] == "Paris"
    assert row["temperature_avg"] is None  # no weather data -> null after left join
    assert row["pollution_category"] == "Moderate"  # pm25 30 -> Moderate
