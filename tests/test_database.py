"""
Database utility tests.

`check_connection()` must always return a bool and never raise, regardless of
whether PostgreSQL is reachable — this guarantees the health endpoint and the
Airflow tasks degrade gracefully.
"""
from src.load.postgres_loader import check_connection
from src.utils.config import settings


def test_check_connection_returns_bool_and_never_raises():
    result = check_connection()
    assert isinstance(result, bool)


def test_sqlalchemy_url_is_well_formed():
    url = settings.sqlalchemy_url
    assert url.startswith("postgresql+psycopg2://")
    assert settings.postgres_db in url
