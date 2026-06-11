"""
FastAPI backend for the Air Quality Intelligence Platform.

Endpoints (as specified):
    GET  /health           -> liveness + DB + Airflow info
    POST /trigger-pipeline -> triggers the Airflow DAG via REST API
    GET  /latest-metrics   -> most recent day's curated metrics from PostgreSQL
    GET  /city-trends?city= -> historical trend for one city

Communication map:
    Streamlit UI  --HTTP-->  FastAPI  --REST-->  Airflow   (trigger DAG)
                                      --SQL-->   PostgreSQL (read metrics)

Run locally:  uvicorn app.main:app --reload
Docs:         http://localhost:8000/docs
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from app import database
from app.schemas import (
    CityTrendsResponse,
    HealthResponse,
    LatestMetricsResponse,
    TriggerResponse,
)
from app.services import airflow_client, metrics_service
from src.utils.config import settings

app = FastAPI(
    title="Air Quality Intelligence Platform API",
    description="Fetch, process, and serve city-level air-quality analytics.",
    version="1.0.0",
)


@app.on_event("startup")
def _startup() -> None:
    # Ensure tables exist so the UI never hits a missing-table error.
    database.ensure_schema()


@app.get("/health", response_model=HealthResponse, tags=["system"])
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        database="up" if database.db_is_up() else "down",
        airflow_base_url=settings.airflow_base_url,
    )


@app.post("/trigger-pipeline", response_model=TriggerResponse, tags=["pipeline"])
def trigger_pipeline() -> TriggerResponse:
    success, dag_run_id, message = airflow_client.trigger_dag()
    return TriggerResponse(
        triggered=success,
        dag_id=settings.dag_id,
        dag_run_id=dag_run_id or None,
        message=message,
    )


@app.get("/latest-metrics", response_model=LatestMetricsResponse, tags=["metrics"])
def latest_metrics(limit: int = Query(100, ge=1, le=1000)) -> LatestMetricsResponse:
    rows = metrics_service.latest_metrics(limit=limit)
    return LatestMetricsResponse(count=len(rows), metrics=rows)


@app.get("/city-trends", response_model=CityTrendsResponse, tags=["metrics"])
def city_trends(
    city: str = Query(..., description="City name, e.g. Berlin"),
    days: int = Query(30, ge=1, le=365),
) -> CityTrendsResponse:
    if not city.strip():
        raise HTTPException(status_code=400, detail="city must not be empty")
    rows = metrics_service.city_trends(city=city, days=days)
    return CityTrendsResponse(city=city, count=len(rows), trends=rows)


@app.get("/pipeline-status", tags=["pipeline"])
def pipeline_status() -> dict:
    """Convenience endpoint for the UI: last DAG-run state."""
    return {"dag_id": settings.dag_id, "state": airflow_client.get_latest_dag_run_state()}
