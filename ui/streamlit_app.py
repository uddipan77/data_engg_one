"""
Streamlit UI for the Air Quality Intelligence Platform.

The UI is intentionally "thin": it ONLY talks to the FastAPI backend over HTTP.
It never touches PostgreSQL or Airflow directly. This keeps responsibilities
clean and mirrors a real production setup.

    Streamlit  --HTTP-->  FastAPI  -->  PostgreSQL / Airflow

Run locally:  streamlit run ui/streamlit_app.py
Open:         http://localhost:8501
"""
from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 30

st.set_page_config(page_title="Air Quality Intelligence Platform", page_icon="🌍", layout="wide")


def api_get(path: str, params: dict | None = None):
    try:
        resp = requests.get(f"{API_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"API error calling {path}: {exc}")
        return None


def api_post(path: str):
    try:
        resp = requests.post(f"{API_URL}{path}", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"API error calling {path}: {exc}")
        return None


# --------------------------------------------------------------------------- #
# Header / explanation
# --------------------------------------------------------------------------- #
st.title("🌍 Air Quality Intelligence Platform")
st.markdown(
    """
This platform fetches **air-quality** (OpenAQ) and **weather** (Open-Meteo) data
for several cities, stores raw data in **AWS S3**, processes it with **PySpark**,
loads curated daily metrics into **PostgreSQL**, and serves them through a
**FastAPI** backend — all orchestrated by **Apache Airflow** and shipped with
**Docker Compose**.

Use the button below to trigger the pipeline, then explore the latest metrics
and per-city trends.
"""
)

# --------------------------------------------------------------------------- #
# Sidebar: health + trigger
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.header("⚙️ Controls")

    health = api_get("/health")
    if health:
        st.metric("API", health.get("status", "?"))
        st.metric("Database", health.get("database", "?"))

    status = api_get("/pipeline-status")
    if status:
        st.caption(f"Last DAG run state: **{status.get('state', 'unknown')}**")

    if st.button("🚀 Trigger Latest Pipeline", type="primary", use_container_width=True):
        with st.spinner("Asking Airflow to run the pipeline..."):
            result = api_post("/trigger-pipeline")
        if result and result.get("triggered"):
            st.success(f"Triggered! DAG run: {result.get('dag_run_id')}")
        elif result:
            st.warning(result.get("message", "Could not trigger pipeline."))


# --------------------------------------------------------------------------- #
# Latest metrics
# --------------------------------------------------------------------------- #
st.subheader("📊 Latest Processed Metrics")

latest = api_get("/latest-metrics", params={"limit": 100})
if latest and latest.get("metrics"):
    df = pd.DataFrame(latest["metrics"])

    st.dataframe(df, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**PM2.5 by City (µg/m³)**")
        st.bar_chart(df.set_index("city")["pm25_avg"])
        st.markdown("**PM10 by City (µg/m³)**")
        st.bar_chart(df.set_index("city")["pm10_avg"])
    with col2:
        st.markdown("**Average Temperature by City (°C)**")
        st.bar_chart(df.set_index("city")["temperature_avg"])
        st.markdown("**Pollution Category Distribution**")
        cat_counts = df["pollution_category"].value_counts()
        st.bar_chart(cat_counts)
else:
    st.info(
        "No metrics yet. Click **Trigger Latest Pipeline** in the sidebar and wait "
        "for the Airflow DAG to finish, then refresh."
    )


# --------------------------------------------------------------------------- #
# City trends
# --------------------------------------------------------------------------- #
st.subheader("📈 City Trends")

default_cities = ["Berlin", "Munich", "Paris", "London", "Delhi", "Kolkata"]
if latest and latest.get("metrics"):
    options = sorted({m["city"] for m in latest["metrics"]}) or default_cities
else:
    options = default_cities

city = st.selectbox("Select a city", options)
if city:
    trends = api_get("/city-trends", params={"city": city, "days": 30})
    if trends and trends.get("trends"):
        tdf = pd.DataFrame(trends["trends"])
        tdf = tdf.set_index("date")
        st.line_chart(tdf[["pm25_avg", "pm10_avg", "no2_avg"]])
        st.line_chart(tdf[["temperature_avg", "humidity_avg"]])
    else:
        st.info(f"No trend data yet for {city}.")

st.caption("Air Quality Intelligence Platform · Version 1 · CPU-only · Spark local mode")
