"""
Central configuration loader for the Air Quality Intelligence Platform.

Everything configurable (AWS, PostgreSQL, Airflow, data paths, target cities)
is read from environment variables here, so NO secret is ever hardcoded.

How the pieces communicate (high level):
  - Airflow tasks import `src.*` modules and call these settings.
  - The PySpark job (`jobs/transform_air_quality.py`) reads DATA_DIR paths.
  - FastAPI (`app/`) reads POSTGRES_* + AIRFLOW_BASE_URL.
  - Streamlit (`ui/`) reads FASTAPI_URL to call the API.

In Docker, env vars come from the `.env` file (see `.env.example`).
Locally, you can `export`/`set` them or rely on the safe defaults below.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _get(name: str, default: str = "") -> str:
    """Read an env var, returning a default when it is missing/empty."""
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _get_bool(name: str, default: bool = False) -> bool:
    return _get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Cities tracked by the pipeline. Override with CITIES="Berlin,Paris" if needed.
# Each city carries lat/lon so the weather API (Open-Meteo) can be queried
# without an API key.
# ---------------------------------------------------------------------------
DEFAULT_CITIES = {
    "Berlin": (52.52, 13.405),
    "Munich": (48.137, 11.575),
    "Paris": (48.857, 2.352),
    "London": (51.507, -0.1278),
    "Delhi": (28.6139, 77.209),
    "Kolkata": (22.5726, 88.3639),
}


@dataclass
class Settings:
    # --- AWS / S3 (data lake: raw + processed layers) ---
    aws_access_key_id: str = field(default_factory=lambda: _get("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: str = field(default_factory=lambda: _get("AWS_SECRET_ACCESS_KEY"))
    aws_region: str = field(default_factory=lambda: _get("AWS_REGION", "eu-central-1"))
    s3_bucket_name: str = field(default_factory=lambda: _get("S3_BUCKET_NAME"))
    s3_raw_prefix: str = field(default_factory=lambda: _get("S3_RAW_PREFIX", "raw"))
    s3_processed_prefix: str = field(default_factory=lambda: _get("S3_PROCESSED_PREFIX", "processed"))

    # --- PostgreSQL (final dashboard database) ---
    postgres_host: str = field(default_factory=lambda: _get("POSTGRES_HOST", "postgres"))
    postgres_port: int = field(default_factory=lambda: int(_get("POSTGRES_PORT", "5432")))
    postgres_db: str = field(default_factory=lambda: _get("POSTGRES_DB", "air_quality"))
    postgres_user: str = field(default_factory=lambda: _get("POSTGRES_USER", "airflow"))
    postgres_password: str = field(default_factory=lambda: _get("POSTGRES_PASSWORD", "airflow"))

    # --- Airflow (orchestration) ---
    airflow_base_url: str = field(default_factory=lambda: _get("AIRFLOW_BASE_URL", "http://airflow-webserver:8080"))
    airflow_username: str = field(default_factory=lambda: _get("AIRFLOW_USERNAME", "airflow"))
    airflow_password: str = field(default_factory=lambda: _get("AIRFLOW_PASSWORD", "airflow"))
    dag_id: str = field(default_factory=lambda: _get("DAG_ID", "air_quality_pipeline"))

    # --- Service URLs ---
    fastapi_url: str = field(default_factory=lambda: _get("FASTAPI_URL", "http://fastapi:8000"))

    # --- API data sources ---
    openaq_api_key: str = field(default_factory=lambda: _get("OPENAQ_API_KEY"))
    openaq_base_url: str = field(default_factory=lambda: _get("OPENAQ_BASE_URL", "https://api.openaq.org/v3"))
    open_meteo_base_url: str = field(
        default_factory=lambda: _get("OPEN_METEO_BASE_URL", "https://air-quality-api.open-meteo.com/v1")
    )
    weather_base_url: str = field(
        default_factory=lambda: _get("WEATHER_BASE_URL", "https://api.open-meteo.com/v1")
    )

    # --- Local data lake paths (mounted into containers) ---
    data_dir: str = field(default_factory=lambda: _get("DATA_DIR", "/opt/airflow/data"))

    # --- Behaviour flags ---
    # When True, network failures silently fall back to bundled sample data so
    # the whole project runs end-to-end even with no credentials / no internet.
    use_fallback_on_error: bool = field(default_factory=lambda: _get_bool("USE_FALLBACK_ON_ERROR", True))

    @property
    def cities(self) -> List[str]:
        override = _get("CITIES")
        if override:
            return [c.strip() for c in override.split(",") if c.strip()]
        return list(DEFAULT_CITIES.keys())

    def city_coords(self, city: str):
        return DEFAULT_CITIES.get(city)

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def has_aws_credentials(self) -> bool:
        return bool(self.aws_access_key_id and self.aws_secret_access_key and self.s3_bucket_name)

    @property
    def raw_dir(self) -> str:
        return os.path.join(self.data_dir, "raw")

    @property
    def curated_dir(self) -> str:
        return os.path.join(self.data_dir, "curated")


# A single shared instance imported across the project.
settings = Settings()
