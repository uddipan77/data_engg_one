"""
Pure, dependency-free pollution-category logic.

Kept separate (and free of Spark/pandas imports) so it can be:
  - unit-tested cheaply (see tests/test_pollution_logic.py),
  - reused by the PySpark job (via a UDF), and
  - reused by FastAPI / any plain-Python caller.

Categories follow a simplified US EPA AQI breakpoint scheme based on the
PM2.5 daily average (µg/m³). This is intentionally easy to read rather than
a full multi-pollutant AQI calculation.
"""
from __future__ import annotations

from typing import Optional

# (inclusive_upper_bound, category_label) ordered from cleanest to worst.
PM25_BREAKPOINTS = [
    (12.0, "Good"),
    (35.4, "Moderate"),
    (55.4, "Unhealthy for Sensitive Groups"),
    (150.4, "Unhealthy"),
    (250.4, "Very Unhealthy"),
    (float("inf"), "Hazardous"),
]

UNKNOWN_CATEGORY = "Unknown"


def categorize_pm25(pm25: Optional[float]) -> str:
    """Map a PM2.5 daily-average concentration (µg/m³) to a pollution category.

    Returns ``"Unknown"`` for ``None`` / negative / non-numeric input so the
    pipeline never crashes on dirty data.
    """
    if pm25 is None:
        return UNKNOWN_CATEGORY
    try:
        value = float(pm25)
    except (TypeError, ValueError):
        return UNKNOWN_CATEGORY
    if value < 0:
        return UNKNOWN_CATEGORY
    for upper, label in PM25_BREAKPOINTS:
        if value <= upper:
            return label
    return "Hazardous"


def category_rank(category: str) -> int:
    """Numeric severity rank (0 = cleanest). Useful for sorting/coloring charts."""
    labels = [label for _, label in PM25_BREAKPOINTS]
    try:
        return labels.index(category)
    except ValueError:
        return -1
