"""
Thin database access layer for FastAPI.

Reuses the shared SQLAlchemy engine + read helpers defined in
`src/load/postgres_loader.py`, so the API and the Airflow tasks talk to the
database in exactly the same way (single source of truth for schema/queries).
"""
from __future__ import annotations

from typing import List

from src.load.postgres_loader import (
    check_connection,
    fetch_city_trends,
    fetch_latest_metrics,
    init_db,
)


def db_is_up() -> bool:
    return check_connection()


def ensure_schema() -> None:
    """Create tables on API startup (idempotent)."""
    try:
        init_db()
    except Exception:  # noqa: BLE001 - API should still boot if DB is briefly down
        pass


def get_latest_metrics(limit: int = 100) -> List[dict]:
    return fetch_latest_metrics(limit=limit)


def get_city_trends(city: str, days: int = 30) -> List[dict]:
    return fetch_city_trends(city=city, days=days)
