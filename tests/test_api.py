"""
API tests for the FastAPI backend.

These tests use FastAPI's TestClient and DO NOT require a live database or
Airflow — the DB-dependent fields are allowed to report "down" gracefully.
"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_endpoint_returns_ok():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    # database may be "up" or "down" depending on environment; both are valid.
    assert body["database"] in ("up", "down")
    assert "airflow_base_url" in body


def test_city_trends_requires_city():
    # Missing required query param -> 422 from FastAPI validation.
    resp = client.get("/city-trends")
    assert resp.status_code == 422


def test_latest_metrics_validates_limit():
    resp = client.get("/latest-metrics", params={"limit": 0})
    assert resp.status_code == 422
