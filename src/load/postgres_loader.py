"""
PostgreSQL loader + schema management for the final dashboard database.

Tables (created automatically on first use):
  - pipeline_runs         : one row per DAG run (status/timestamps/row counts)
  - air_quality_metrics   : the curated daily city-level analytics table
  - data_quality_results  : per-run data-quality check outcomes

FastAPI reads these tables; Airflow tasks write to them. Uses SQLAlchemy so
the same code works locally and inside Docker.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterable, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.config import settings

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Lazily create (and cache) a SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(settings.sqlalchemy_url, pool_pre_ping=True)
    return _engine


@contextmanager
def get_connection():
    engine = get_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def check_connection() -> bool:
    """Return True if the database answers a trivial query (used by tests/health)."""
    try:
        with get_connection() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("PostgreSQL connection check failed: %s", exc)
        return False


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id          TEXT PRIMARY KEY,
        dag_run_id      TEXT,
        status          TEXT NOT NULL,
        started_at      TIMESTAMPTZ DEFAULT now(),
        finished_at     TIMESTAMPTZ,
        rows_processed  INTEGER DEFAULT 0,
        notes           TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS air_quality_metrics (
        city               TEXT NOT NULL,
        date               DATE NOT NULL,
        pm25_avg           DOUBLE PRECISION,
        pm10_avg           DOUBLE PRECISION,
        no2_avg            DOUBLE PRECISION,
        temperature_avg    DOUBLE PRECISION,
        humidity_avg       DOUBLE PRECISION,
        pollution_category TEXT,
        run_id             TEXT,
        loaded_at          TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (city, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_quality_results (
        id          SERIAL PRIMARY KEY,
        run_id      TEXT,
        check_name  TEXT NOT NULL,
        passed      BOOLEAN NOT NULL,
        details     TEXT,
        checked_at  TIMESTAMPTZ DEFAULT now()
    )
    """,
]


def init_db() -> None:
    """Create all tables if they do not exist (idempotent)."""
    engine = get_engine()
    with engine.begin() as conn:
        for ddl in DDL_STATEMENTS:
            conn.execute(text(ddl))
    logger.info("PostgreSQL schema ensured (pipeline_runs, air_quality_metrics, data_quality_results).")


# --------------------------------------------------------------------------- #
# pipeline_runs helpers
# --------------------------------------------------------------------------- #
def record_run_start(run_id: str, dag_run_id: str = "") -> None:
    init_db()
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_runs (run_id, dag_run_id, status)
                VALUES (:run_id, :dag_run_id, 'running')
                ON CONFLICT (run_id) DO UPDATE
                    SET status = 'running', started_at = now()
                """
            ),
            {"run_id": run_id, "dag_run_id": dag_run_id},
        )


def record_run_finish(run_id: str, status: str, rows_processed: int = 0, notes: str = "") -> None:
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE pipeline_runs
                   SET status = :status,
                       finished_at = now(),
                       rows_processed = :rows,
                       notes = :notes
                 WHERE run_id = :run_id
                """
            ),
            {"run_id": run_id, "status": status, "rows": rows_processed, "notes": notes},
        )


# --------------------------------------------------------------------------- #
# air_quality_metrics helpers
# --------------------------------------------------------------------------- #
def upsert_metrics(rows: Iterable[dict], run_id: str = "") -> int:
    """Insert/replace curated metric rows. Returns number of rows written."""
    init_db()
    rows = list(rows)
    if not rows:
        return 0
    with get_engine().begin() as conn:
        for row in rows:
            payload = dict(row)
            payload["run_id"] = run_id
            conn.execute(
                text(
                    """
                    INSERT INTO air_quality_metrics
                        (city, date, pm25_avg, pm10_avg, no2_avg,
                         temperature_avg, humidity_avg, pollution_category, run_id)
                    VALUES
                        (:city, :date, :pm25_avg, :pm10_avg, :no2_avg,
                         :temperature_avg, :humidity_avg, :pollution_category, :run_id)
                    ON CONFLICT (city, date) DO UPDATE SET
                        pm25_avg = EXCLUDED.pm25_avg,
                        pm10_avg = EXCLUDED.pm10_avg,
                        no2_avg = EXCLUDED.no2_avg,
                        temperature_avg = EXCLUDED.temperature_avg,
                        humidity_avg = EXCLUDED.humidity_avg,
                        pollution_category = EXCLUDED.pollution_category,
                        run_id = EXCLUDED.run_id,
                        loaded_at = now()
                    """
                ),
                payload,
            )
    logger.info("Upserted %d rows into air_quality_metrics.", len(rows))
    return len(rows)


def record_quality_result(run_id: str, check_name: str, passed: bool, details: str = "") -> None:
    init_db()
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO data_quality_results (run_id, check_name, passed, details)
                VALUES (:run_id, :check_name, :passed, :details)
                """
            ),
            {"run_id": run_id, "check_name": check_name, "passed": passed, "details": details},
        )


# --------------------------------------------------------------------------- #
# Read helpers used by FastAPI
# --------------------------------------------------------------------------- #
def fetch_latest_metrics(limit: int = 100) -> List[dict]:
    init_db()
    with get_connection() as conn:
        result = conn.execute(
            text(
                """
                SELECT city, date, pm25_avg, pm10_avg, no2_avg,
                       temperature_avg, humidity_avg, pollution_category
                  FROM air_quality_metrics
                 WHERE date = (SELECT MAX(date) FROM air_quality_metrics)
                 ORDER BY city
                 LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [dict(r._mapping) for r in result]


def fetch_city_trends(city: str, days: int = 30) -> List[dict]:
    init_db()
    with get_connection() as conn:
        result = conn.execute(
            text(
                """
                SELECT city, date, pm25_avg, pm10_avg, no2_avg,
                       temperature_avg, humidity_avg, pollution_category
                  FROM air_quality_metrics
                 WHERE city = :city
                 ORDER BY date DESC
                 LIMIT :days
                """
            ),
            {"city": city, "days": days},
        )
        return [dict(r._mapping) for r in result]
