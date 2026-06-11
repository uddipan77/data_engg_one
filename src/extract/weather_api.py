"""
Weather data extraction.

Primary source : Open-Meteo (https://open-meteo.com/) — free, NO API key.
Fallback       : deterministic sample data generated locally.

Output shape (list of dicts), one record per city/date:
    {
        "city": "Berlin",
        "date": "2026-06-11",
        "temperature": 18.4,
        "humidity": 61.0,
        "source": "open-meteo" | "fallback",
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


def _fallback_weather(cities: List[str], run_date: str) -> List[Dict]:
    records: List[Dict] = []
    for city in cities:
        rng = random.Random(f"{city}-{run_date}-wx")
        records.append(
            {
                "city": city,
                "date": run_date,
                "temperature": round(rng.uniform(5, 35), 1),
                "humidity": round(rng.uniform(30, 90), 1),
                "source": "fallback",
            }
        )
    logger.warning("Using FALLBACK sample weather data for %d cities.", len(records))
    return records


def _fetch_open_meteo_city(city: str, run_date: str) -> Optional[Dict]:
    """Fetch current weather for one city using its lat/lon from config."""
    coords = settings.city_coords(city)
    if not coords:
        return None
    lat, lon = coords
    try:
        resp = requests.get(
            f"{settings.weather_base_url}/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m",
                "timezone": "UTC",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        current = resp.json().get("current", {})
        temp = current.get("temperature_2m")
        humidity = current.get("relative_humidity_2m")
        if temp is None and humidity is None:
            return None
        return {
            "city": city,
            "date": run_date,
            "temperature": temp,
            "humidity": humidity,
            "source": "open-meteo",
        }
    except (requests.RequestException, ValueError, KeyError) as exc:
        logger.warning("Open-Meteo fetch failed for %s: %s", city, exc)
        return None


def extract_weather(cities: Optional[List[str]] = None, run_date: Optional[str] = None) -> List[Dict]:
    """Public entry point used by the Airflow `extract_weather_data` task."""
    cities = cities or settings.cities
    run_date = run_date or date.today().isoformat()

    records: List[Dict] = []
    for city in cities:
        record = _fetch_open_meteo_city(city, run_date)
        if record:
            records.append(record)

    if not records and settings.use_fallback_on_error:
        return _fallback_weather(cities, run_date)

    fetched_cities = {r["city"] for r in records}
    missing = [c for c in cities if c not in fetched_cities]
    if missing and settings.use_fallback_on_error:
        records.extend(_fallback_weather(missing, run_date))

    return records
