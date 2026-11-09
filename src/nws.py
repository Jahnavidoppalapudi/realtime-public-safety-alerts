"""Normalize National Weather Service alerts (api.weather.gov, CAP in GeoJSON)."""

from __future__ import annotations

import re
from datetime import UTC, datetime

ALERTS_URL = "https://api.weather.gov/alerts/active"
_SAME = re.compile(r"^\d{6}$")


class InvalidRecord(ValueError):
    """The upstream record breaks our contract; it goes to the dead letter queue."""


def parse_time(text: str | None, field: str, required: bool = True) -> datetime | None:
    if not text:
        if required:
            raise InvalidRecord(f"missing required timestamp '{field}'")
        return None
    try:
        value = datetime.fromisoformat(text)
    except ValueError as err:
        raise InvalidRecord(f"bad timestamp in '{field}': {text!r}") from err
    if value.tzinfo is None:
        raise InvalidRecord(f"timestamp '{field}' has no time zone: {text!r}")
    return value.astimezone(UTC)


def same_to_fips(codes: list[str]) -> list[str]:
    """SAME code '048201' = subdivision 0 + state 48 + county 201 -> county FIPS '48201'."""
    fips = []
    for code in codes or []:
        if not _SAME.match(code):
            raise InvalidRecord(f"malformed SAME code {code!r}")
        if code[1:] not in fips:
            fips.append(code[1:])
    return fips


def incident_key(alert_id: str, references: list[dict]) -> str:
    """Updates and cancellations reference earlier messages. Keying every message by the
    earliest one it references keeps a hazard's whole story on one Kafka partition, in order."""
    if not references:
        return alert_id
    return min(references, key=lambda r: r["sent"])["alert_id"]


def parse_alert(feature: dict, ingested_at: datetime) -> dict:
    """GeoJSON feature -> record matching schemas/nws_alert.avsc. Raises InvalidRecord."""
    p = feature.get("properties") or {}
    for field in ("id", "messageType", "status", "event", "severity", "urgency", "certainty"):
        if not p.get(field):
            raise InvalidRecord(f"missing required field '{field}'")

    references = [
        {"alert_id": r["identifier"], "sent": parse_time(r.get("sent"), "references.sent")}
        for r in p.get("references") or []
        if r.get("identifier")
    ]
    geocode = p.get("geocode") or {}
    return {
        "alert_id": p["id"],
        "incident_key": incident_key(p["id"], references),
        "message_type": p["messageType"],
        "status": p["status"],
        "event": p["event"],
        "category": p.get("category"),
        "severity": p["severity"],
        "urgency": p["urgency"],
        "certainty": p["certainty"],
        "headline": p.get("headline"),
        "area_desc": p.get("areaDesc") or "",
        "county_fips": same_to_fips(geocode.get("SAME") or []),
        "ugc_codes": list(geocode.get("UGC") or []),
        "references": references,
        "sender_name": p.get("senderName"),
        "sent": parse_time(p.get("sent"), "sent"),
        "effective": parse_time(p.get("effective"), "effective"),
        "onset": parse_time(p.get("onset"), "onset", required=False),
        "expires": parse_time(p.get("expires"), "expires"),
        "ends": parse_time(p.get("ends"), "ends", required=False),
        "source_url": feature.get("id") or p["id"],
        "ingested_at": ingested_at,
    }


def version_key(record: dict) -> str:
    """Every NWS message has its own id, so the id is the version."""
    return record["alert_id"]
