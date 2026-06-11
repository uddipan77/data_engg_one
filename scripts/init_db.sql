-- =============================================================================
-- PostgreSQL bootstrap for the Air Quality Intelligence Platform.
--
-- This runs automatically the FIRST time the `postgres` container starts
-- (mounted into /docker-entrypoint-initdb.d). It creates the analytics
-- tables. The application also creates them on demand (init_db()), so this
-- file is a convenience + documentation of the schema.
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    dag_run_id      TEXT,
    status          TEXT NOT NULL,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_processed  INTEGER DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS air_quality_metrics (
    city               TEXT NOT NULL,
    date               DATE NOT NULL,
    pm25_avg           DOUBLE PRECISION,
    pm10_avg           DOUBLE PRECISION,
    no2_avg            DOUBLE PRECISION,
    temperature_avg    DOUBLE PRECISION,
    humidity_avg       DOUBLE PRECISION,
    pollution_category TEXT,
    run_id             TEXT,
    loaded_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (city, date)
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT,
    check_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     TEXT,
    checked_at  TIMESTAMPTZ DEFAULT now()
);
