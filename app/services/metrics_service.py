"""
Business logic for serving metrics to the API/UI.

Wraps the database read helpers and normalizes rows into plain dicts that map
cleanly onto the Pydantic schemas.
"""
from __future__ import annotations

from typing import List

from app import database


def _normalize(row: dict) -> dict:
    """Ensure JSON-serializable types and consistent keys."""
    out = dict(row)
    if out.get("date") is not None and not isinstance(out["date"], str):
        # SQLAlchemy returns datetime.date; let Pydantic handle it, but be safe.
        out["date"] = out["date"]
    return out


def latest_metrics(limit: int = 100) -> List[dict]:
    return [_normalize(r) for r in database.get_latest_metrics(limit=limit)]


def city_trends(city: str, days: int = 30) -> List[dict]:
    rows = database.get_city_trends(city=city, days=days)
    # Return chronological order (oldest -> newest) for nicer charts.
    rows = sorted(rows, key=lambda r: str(r.get("date")))
    return [_normalize(r) for r in rows]
