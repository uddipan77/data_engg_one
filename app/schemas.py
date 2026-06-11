"""Pydantic response/request models for the FastAPI backend."""
from __future__ import annotations

from datetime import date as date_type
from typing import List, Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str  # "up" / "down"
    airflow_base_url: str


class TriggerResponse(BaseModel):
    triggered: bool
    dag_id: str
    dag_run_id: Optional[str] = None
    message: str


class MetricRow(BaseModel):
    city: str
    date: Optional[date_type] = None
    pm25_avg: Optional[float] = None
    pm10_avg: Optional[float] = None
    no2_avg: Optional[float] = None
    temperature_avg: Optional[float] = None
    humidity_avg: Optional[float] = None
    pollution_category: Optional[str] = None


class LatestMetricsResponse(BaseModel):
    count: int
    metrics: List[MetricRow]


class CityTrendsResponse(BaseModel):
    city: str
    count: int
    trends: List[MetricRow]
