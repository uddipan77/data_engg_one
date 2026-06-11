"""
Air-quality data extraction.

Primary source : OpenAQ (https://docs.openaq.org/) — public air-quality API.
Fallback       : deterministic sample data generated locally, so the pipeline
                 runs end-to-end with NO API key and NO internet.

Output shape (list of dicts), one record per city/measurement:
    {
        "city": "Berlin",
        "date": "2026-06-11",
        "pm25": 12.3,
        "pm10": 20.1,
        "no2": 15.0,
        "source": "openaq" | "fallback",
    }
"""
from __future__ import annotations

import logging
import random
from datetime import date
from typing import Dict, List, Optional

import requests

from src.utils.config import settings

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15  # seconds


def _fallback_air_quality(cities: List[str], run_date: str) -> List[Dict]:
    """Generate stable-ish synthetic air-quality data (seeded per city/date)."""
    records: List[Dict] = []
    for city in cities:
        rng = random.Random(f"{city}-{run_date}-aq")
        # Make Delhi/Kolkata noticeably more polluted for a realistic demo.
        base = 70 if city in ("Delhi", "Kolkata") else 15
        pm25 = round(base + rng.uniform(-5, 25), 1)
        records.append(
            {
                "city": city,
                "date": run_date,
                "pm25": pm25,
                "pm10": round(pm25 * rng.uniform(1.3, 1.8), 1),
                "no2": round(rng.uniform(5, 45), 1),
                "source": "fallback",
            }
        )
    logger.warning("Using FALLBACK sample air-quality data for %d cities.", len(records))
    return records


def _fetch_openaq_city(city: str, run_date: str) -> Optional[Dict]:
    """Try to fetch the latest measurements for one city from OpenAQ v3.

    Returns None on any failure so the caller can decide to fall back.
    Note: OpenAQ v3 requires an API key (free). Without it we return None.
    """
    if not settings.openaq_api_key:
        return None
    try:
        headers = {"X-API-Key": settings.openaq_api_key}
        # Minimal, defensive query. OpenAQ schemas evolve, so we keep parsing loose.
        resp = requests.get(
            f"{settings.openaq_base_url}/locations",
            params={"limit": 1, "city": city},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            return None
        # Pull whatever parameters we can find; default missing to None.
        params: Dict[str, float] = {}
        for sensor in results[0].get("sensors", []):
            name = (sensor.get("parameter") or {}).get("name")
            value = sensor.get("latest", {}).get("value")
            if name and value is not None:
                params[name] = value
        return {
            "city": city,
            "date": run_date,
            "pm25": params.get("pm25"),
            "pm10": params.get("pm10"),
            "no2": params.get("no2"),
            "source": "openaq",
        }
    except (requests.RequestException, ValueError, KeyError) as exc:
        logger.warning("OpenAQ fetch failed for %s: %s", city, exc)
        return None


def extract_air_quality(cities: Optional[List[str]] = None, run_date: Optional[str] = None) -> List[Dict]:
    """Public entry point used by the Airflow `extract_air_quality_data` task."""
    cities = cities or settings.cities
    run_date = run_date or date.today().isoformat()

    records: List[Dict] = []
    for city in cities:
        record = _fetch_openaq_city(city, run_date)
        if record:
            records.append(record)

    # If we got nothing usable (no key, network down, empty results), fall back.
    if not records and settings.use_fallback_on_error:
        return _fallback_air_quality(cities, run_date)

    # Fill any cities we missed with fallback rows so downstream joins are complete.
    fetched_cities = {r["city"] for r in records}
    missing = [c for c in cities if c not in fetched_cities]
    if missing and settings.use_fallback_on_error:
        records.extend(_fallback_air_quality(missing, run_date))

    return records
