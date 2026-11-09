"""Storage logic against a real PostgreSQL: idempotency, ordering, incident state."""

from datetime import timedelta
from decimal import Decimal

from src.store import store_alert, store_quake


def incident(db, incident_id):
    return db.execute(
        "select state, message_count, county_fips, latest_alert_id from incidents "
        "where incident_id = %s",
        (incident_id,),
    ).fetchone()


def test_duplicate_delivery_is_stored_once(db, alerts):
    original = alerts[0]
    with db.cursor() as cur:
        assert store_alert(cur, original) is True
        assert store_alert(cur, original) is False  # same message delivered again
    assert db.execute("select count(*) from alert_messages").fetchone()[0] == 1
    assert incident(db, original["alert_id"])[1] == 1  # count not inflated by the duplicate


def test_update_joins_the_incident_and_wins(db, alerts):
    original, update = alerts[0], alerts[1]
    with db.cursor() as cur:
        store_alert(cur, original)
        store_alert(cur, update)
    state, count, counties, latest = incident(db, original["alert_id"])
    assert (state, count, latest) == ("active", 2, update["alert_id"])
    assert counties == ["48201"]  # the update narrowed the warning to one county


def test_out_of_order_arrival_gives_the_same_result(db, alerts):
    original, update = alerts[0], alerts[1]
    with db.cursor() as cur:
        store_alert(cur, update)  # the update arrives first
        store_alert(cur, original)
    state, count, counties, latest = incident(db, original["alert_id"])
    assert (count, latest, counties) == (2, update["alert_id"], ["48201"])
    assert db.execute("select count(*) from incidents").fetchone()[0] == 1


def test_cancellation_closes_the_incident_and_late_messages_cannot_reopen_it(db, alerts):
    original, update = alerts[0], alerts[1]
    cancel = {**update, "alert_id": "urn:test.001.3", "message_type": "Cancel",
              "sent": update["sent"] + timedelta(minutes=10)}  # fmt: skip
    with db.cursor() as cur:
        store_alert(cur, original)
        store_alert(cur, cancel)
        store_alert(cur, update)  # older update arrives after the cancellation
    state, count, _, latest = incident(db, original["alert_id"])
    assert (state, count, latest) == ("cancelled", 3, "urn:test.001.3")


def test_long_chain_follows_stored_references(db, alerts):
    """Later NWS messages may drop the earliest reference; the stored chain still links them."""
    original, update = alerts[0], alerts[1]
    second = {**update, "alert_id": "urn:test.001.4", "incident_key": update["alert_id"],
              "references": [{"alert_id": update["alert_id"], "sent": update["sent"]}],
              "sent": update["sent"] + timedelta(minutes=20)}  # fmt: skip
    with db.cursor() as cur:
        for alert in (original, update, second):
            store_alert(cur, alert)
    assert incident(db, original["alert_id"])[1] == 3
    assert db.execute("select count(distinct incident_id) from alert_messages").fetchone()[0] == 1


def test_quake_revisions_track_first_and_latest_magnitude(db, quake_v1):
    revised = {**quake_v1, "magnitude": 4.8, "status": "reviewed",
               "updated_at": quake_v1["updated_at"] + timedelta(minutes=30)}  # fmt: skip
    with db.cursor() as cur:
        assert store_quake(cur, revised) is True  # the revision arrives first
        assert store_quake(cur, quake_v1) is True
        assert store_quake(cur, quake_v1) is False  # duplicate
    row = db.execute(
        "select magnitude, first_magnitude, status, version_count from quakes"
    ).fetchone()
    assert row == (4.8, 4.6, "reviewed", 2)
    change = db.execute("select magnitude_change from v_quake_revisions").fetchone()[0]
    assert change == Decimal("0.20")


def test_active_incident_view_adds_population(db, alerts):
    db.execute(
        "insert into county_population values "
        "('48201', 'Harris County', 'Texas', 4800000, '2023', now()),"
        "('48157', 'Fort Bend County', 'Texas', 850000, '2023', now())"
    )
    future = {**alerts[0], "expires": alerts[0]["sent"] + timedelta(days=3650), "ends": None}
    with db.cursor() as cur:
        store_alert(cur, future)
    row = db.execute(
        "select event, population, counties, unmatched_counties from v_active_incidents"
    ).fetchone()
    assert row == ("Tornado Warning", 5650000, 2, 0)
    by_state = db.execute("select state_name, population from v_people_under_warnings").fetchall()
    assert by_state == [("Texas", 5650000)]


def test_expired_and_test_alerts_are_not_active(db, alerts):
    expired = alerts[0]  # expires in May 2026, which is in the past
    exercise = {**alerts[2], "status": "Exercise",
                "expires": alerts[2]["sent"] + timedelta(days=3650)}  # fmt: skip
    with db.cursor() as cur:
        store_alert(cur, expired)
        store_alert(cur, exercise)
    assert db.execute("select count(*) from v_active_incidents").fetchone()[0] == 0
