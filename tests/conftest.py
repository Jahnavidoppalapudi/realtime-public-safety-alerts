import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 5, 12, 20, 0, tzinfo=UTC)


@pytest.fixture
def nws_features():
    return json.loads((FIXTURES / "nws_alerts.json").read_text())["features"]


@pytest.fixture
def usgs_features():
    return json.loads((FIXTURES / "usgs_day.json").read_text())["features"]


# ---------------------------------------------------------------- PostgreSQL
# Database tests run when TEST_DATABASE_URL is set (CI starts a Postgres service for them).
import os  # noqa: E402
import uuid  # noqa: E402

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def db():
    """A connection whose tables live in a fresh, throwaway schema."""
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    psycopg = pytest.importorskip("psycopg")
    from src.db import apply_schema

    schema = f"test_{uuid.uuid4().hex[:12]}"
    conn = psycopg.connect(TEST_DATABASE_URL, autocommit=True)
    conn.execute(f"create schema {schema}")
    conn.execute(f"set search_path to {schema}")
    apply_schema(conn)
    yield conn
    conn.execute(f"drop schema {schema} cascade")
    conn.close()


@pytest.fixture
def alerts(nws_features):
    from src import nws

    return [nws.parse_alert(f, NOW) for f in nws_features[:3]]


@pytest.fixture
def quake_v1(usgs_features):
    from src import usgs

    return usgs.parse_quake(usgs_features[0], NOW)
