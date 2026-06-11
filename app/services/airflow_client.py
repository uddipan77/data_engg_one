"""
Airflow REST API client.

FastAPI's POST /trigger-pipeline uses this to start the `air_quality_pipeline`
DAG by calling the Airflow stable REST API:

    POST {AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns

Auth is HTTP Basic (the default Airflow auth backend). All config comes from
environment variables via `settings` — no secrets hardcoded.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Tuple

import requests

from src.utils.config import settings

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 20


def trigger_dag(dag_id: str | None = None) -> Tuple[bool, str, str]:
    """Trigger a DAG run.

    Returns (success, dag_run_id, message).
    """
    dag_id = dag_id or settings.dag_id
    url = f"{settings.airflow_base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    dag_run_id = f"api__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}__{uuid.uuid4().hex[:6]}"

    try:
        resp = requests.post(
            url,
            json={"dag_run_id": dag_run_id, "conf": {"source": "fastapi"}},
            auth=(settings.airflow_username, settings.airflow_password),
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code in (200, 201):
            return True, dag_run_id, f"DAG '{dag_id}' triggered."
        # Common case: DAG paused or not found yet.
        return False, "", f"Airflow returned {resp.status_code}: {resp.text[:300]}"
    except requests.RequestException as exc:
        logger.warning("Failed to trigger DAG %s: %s", dag_id, exc)
        return False, "", f"Could not reach Airflow at {settings.airflow_base_url}: {exc}"


def get_latest_dag_run_state(dag_id: str | None = None) -> str:
    """Return the state of the most recent DAG run, or 'unknown'."""
    dag_id = dag_id or settings.dag_id
    url = f"{settings.airflow_base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    try:
        resp = requests.get(
            url,
            params={"order_by": "-execution_date", "limit": 1},
            auth=(settings.airflow_username, settings.airflow_password),
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            runs = resp.json().get("dag_runs", [])
            if runs:
                return runs[0].get("state", "unknown")
        return "unknown"
    except requests.RequestException:
        return "unknown"
