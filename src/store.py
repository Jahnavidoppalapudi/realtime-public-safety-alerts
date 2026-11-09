"""Write decoded events to PostgreSQL. Every write is idempotent.

Kafka delivers at least once, so the same message can arrive twice (after a producer restart
or a consumer crash before committing offsets). Each insert is keyed on the version
(alert id, or earthquake id + update time) and does nothing if that version is already stored.
Derived state (incidents, latest earthquake) is updated only when a version is new, and only
if it is newer than what is stored, so messages can arrive in any order.
"""

from __future__ import annotations

INSERT_ALERT = """
insert into alert_messages (
    alert_id, incident_id, message_type, status, event, category, severity, urgency,
    certainty, headline, area_desc, county_fips, sender_name, sent, effective, onset,
    expires, ends, ingested_at, kafka_partition, kafka_offset)
values (
    %(alert_id)s, %(incident_id)s, %(message_type)s, %(status)s, %(event)s, %(category)s,
    %(severity)s, %(urgency)s, %(certainty)s, %(headline)s, %(area_desc)s, %(county_fips)s,
    %(sender_name)s, %(sent)s, %(effective)s, %(onset)s, %(expires)s, %(ends)s,
    %(ingested_at)s, %(partition)s, %(offset)s)
on conflict (alert_id) do nothing
returning alert_id
"""


def latest_wins(alias: str, version_column: str, columns: list[str]) -> str:
    """SET clauses that take the incoming value only if its version is at least as new.

    In ON CONFLICT DO UPDATE, `alias.col` is the stored row and `excluded.col` the incoming
    one, so a late, older message can never overwrite newer state.
    """
    return ",\n    ".join(
        f"{c} = case when excluded.{version_column} >= {alias}.{version_column} "
        f"then excluded.{c} else {alias}.{c} end"
        for c in columns
    )


INCIDENT_FIELDS = ["event", "status", "state", "severity", "headline", "area_desc",
                   "county_fips", "expires", "ends", "latest_alert_id"]  # fmt: skip

# The latest message (by issue time) decides the incident's current details and state
UPSERT_INCIDENT = f"""
insert into incidents as i (
    incident_id, event, status, state, severity, headline, area_desc, county_fips,
    first_sent, last_sent, expires, ends, latest_alert_id, message_count)
values (
    %(incident_id)s, %(event)s, %(status)s, %(state)s, %(severity)s, %(headline)s,
    %(area_desc)s, %(county_fips)s, %(sent)s, %(sent)s, %(expires)s, %(ends)s,
    %(alert_id)s, 1)
on conflict (incident_id) do update set
    {latest_wins("i", "last_sent", INCIDENT_FIELDS)},
    message_count = i.message_count + 1,
    first_sent    = least(i.first_sent, excluded.first_sent),
    last_sent     = greatest(i.last_sent, excluded.last_sent)
"""

INSERT_QUAKE_VERSION = """
insert into quake_versions (
    event_id, updated_at, origin_time, magnitude, mag_type, place, status, event_type,
    latitude, longitude, depth_km, alert_level, tsunami, significance, ingested_at)
values (
    %(event_id)s, %(updated_at)s, %(origin_time)s, %(magnitude)s, %(mag_type)s, %(place)s,
    %(status)s, %(event_type)s, %(latitude)s, %(longitude)s, %(depth_km)s, %(alert_level)s,
    %(tsunami)s, %(significance)s, %(ingested_at)s)
on conflict (event_id, updated_at) do nothing
returning event_id
"""

QUAKE_FIELDS = ["origin_time", "magnitude", "mag_type", "place", "status", "event_type",
                "latitude", "longitude", "depth_km", "alert_level", "tsunami"]  # fmt: skip

UPSERT_QUAKE = f"""
insert into quakes as q (
    event_id, updated_at, origin_time, magnitude, first_magnitude, mag_type, place, status,
    event_type, latitude, longitude, depth_km, alert_level, tsunami, version_count,
    first_version_at, first_seen_at)
values (
    %(event_id)s, %(updated_at)s, %(origin_time)s, %(magnitude)s, %(magnitude)s, %(mag_type)s,
    %(place)s, %(status)s, %(event_type)s, %(latitude)s, %(longitude)s, %(depth_km)s,
    %(alert_level)s, %(tsunami)s, 1, %(updated_at)s, %(ingested_at)s)
on conflict (event_id) do update set
    {latest_wins("q", "updated_at", QUAKE_FIELDS)},
    first_magnitude  = case when excluded.first_version_at < q.first_version_at
                            then excluded.first_magnitude else q.first_magnitude end,
    first_version_at = least(q.first_version_at, excluded.first_version_at),
    version_count    = q.version_count + 1,
    updated_at       = greatest(q.updated_at, excluded.updated_at)
"""


def resolve_incident(cur, alert: dict) -> str:
    """Join this message to the incident of any earlier message it references.

    The producer's incident_key covers most cases. Following stored references also handles
    long chains where NWS has dropped the earliest reference from later updates.
    """
    referenced = [r["alert_id"] for r in alert["references"]]
    if referenced:
        row = cur.execute(
            "select incident_id from alert_messages where alert_id = any(%s) order by sent limit 1",
            (referenced,),
        ).fetchone()
        if row:
            return row[0]
    return alert["incident_key"]


def store_alert(cur, alert: dict, partition: int | None = None, offset: int | None = None) -> bool:
    """Returns False if this exact message was already stored (a duplicate delivery)."""
    incident_id = resolve_incident(cur, alert)
    row = cur.execute(
        INSERT_ALERT,
        {**alert, "incident_id": incident_id, "partition": partition, "offset": offset},
    ).fetchone()
    if row is None:
        return False
    for ref in alert["references"]:
        cur.execute(
            "insert into alert_references values (%s, %s) on conflict do nothing",
            (alert["alert_id"], ref["alert_id"]),
        )
    state = "cancelled" if alert["message_type"] == "Cancel" else "active"
    cur.execute(UPSERT_INCIDENT, {**alert, "incident_id": incident_id, "state": state})
    return True


def store_quake(cur, quake: dict) -> bool:
    """Returns False if this exact version was already stored."""
    if cur.execute(INSERT_QUAKE_VERSION, quake).fetchone() is None:
        return False
    cur.execute(UPSERT_QUAKE, quake)
    return True
