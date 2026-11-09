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
