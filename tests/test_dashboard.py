"""Every Grafana panel query runs against a real schema with data in it."""

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = json.loads((ROOT / "grafana/dashboards/public-safety.json").read_text())
PANELS = [p for p in DASHBOARD["panels"] if p["type"] != "row"]


def grafana_to_sql(raw: str) -> str:
    """Expand Grafana's time macro the way the Postgres data source does."""
    return re.sub(
        r"\$__timeFilter\((\w+)\)",
        r"\1 between now() - interval '24 hours' and now()",
        raw,
    )


def test_panels_use_the_provisioned_datasource():
    provisioned = yaml.safe_load(
        (ROOT / "grafana/provisioning/datasources/postgres.yml").read_text()
    )
    uid = provisioned["datasources"][0]["uid"]
    assert all(t["datasource"]["uid"] == uid for p in PANELS for t in p["targets"])
    ids = [p["id"] for p in DASHBOARD["panels"]]
    assert len(ids) == len(set(ids))


@pytest.fixture
def live_db(db, alerts, quake_v1):
    from src.store import store_alert, store_quake

    now = datetime.now(UTC)
    db.execute(
        "insert into county_population values "
        "('48201', 'Harris County', 'Texas', 4731145, '2023', now()),"
        "('48157', 'Fort Bend County', 'Texas', 889146, '2023', now())"
    )
    with db.cursor() as cur:
        for i, alert in enumerate(alerts):
            store_alert(cur, {**alert, "sent": now - timedelta(minutes=30 - i),
                              "ingested_at": now - timedelta(minutes=29 - i),
                              "expires": now + timedelta(hours=2), "ends": None})  # fmt: skip
        recent = {**quake_v1, "origin_time": now - timedelta(hours=1), "ingested_at": now}
        store_quake(cur, {**recent, "updated_at": now - timedelta(minutes=50)})
        store_quake(cur, {**recent, "updated_at": now - timedelta(minutes=10), "magnitude": 4.9})
    db.execute("insert into consumer_lag values (now(), 'gov.nws.alerts.v1', 0, 3)")
    db.execute("insert into processor_batches values (now(), 5, 3, 2, 0, 0, 0.05)")
    return db


@pytest.mark.parametrize("panel", PANELS, ids=[p["title"] for p in PANELS])
def test_panel_query_runs_and_returns_rows(live_db, panel):
    for target in panel["targets"]:
        rows = live_db.execute(grafana_to_sql(target["rawSql"])).fetchall()
        assert rows, f"{panel['title']} returned no rows for a populated database"
        if target["format"] == "time_series":
            assert live_db.execute(grafana_to_sql(target["rawSql"])).description[0].name == "time"


def test_headline_numbers(live_db):
    queries = {p["title"]: grafana_to_sql(p["targets"][0]["rawSql"]) for p in PANELS}
    assert live_db.execute(queries["Active warnings"]).fetchone() == (1,)
    assert live_db.execute(queries["People under an active warning"]).fetchone() == (4731145,)
    assert live_db.execute(queries["Earthquakes M2.5+ (24h)"]).fetchone() == (1,)
