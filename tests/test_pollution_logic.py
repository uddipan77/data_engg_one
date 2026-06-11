"""Unit tests for the pure pollution-category logic."""
import pytest

from src.transform.pollution_logic import categorize_pm25, category_rank


@pytest.mark.parametrize(
    "pm25,expected",
    [
        (0, "Good"),
        (12.0, "Good"),
        (12.1, "Moderate"),
        (35.4, "Moderate"),
        (40, "Unhealthy for Sensitive Groups"),
        (55.5, "Unhealthy"),
        (200, "Very Unhealthy"),
        (300, "Hazardous"),
        (9999, "Hazardous"),
    ],
)
def test_categorize_pm25_breakpoints(pm25, expected):
    assert categorize_pm25(pm25) == expected


@pytest.mark.parametrize("bad", [None, -1, -0.01, "not-a-number", float("nan") if False else "x"])
def test_categorize_pm25_handles_bad_input(bad):
    # None / negative / non-numeric all map to "Unknown".
    assert categorize_pm25(bad) == "Unknown"


def test_categorize_pm25_accepts_numeric_strings():
    assert categorize_pm25("10") == "Good"


def test_category_rank_is_ordered():
    assert category_rank("Good") < category_rank("Moderate")
    assert category_rank("Moderate") < category_rank("Hazardous")
    assert category_rank("Unknown") == -1
