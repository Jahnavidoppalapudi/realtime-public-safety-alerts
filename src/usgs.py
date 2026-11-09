"""Normalize USGS real-time earthquake feed events (GeoJSON summary format)."""

from __future__ import annotations

from datetime import UTC, datetime

from src.nws import InvalidRecord

# The 24-hour window means a producer outage of up to a day loses nothing
FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def _ms(value, field: str) -> datetime:
    if not isinstance(value, int):
        raise InvalidRecord(f"'{field}' must be epoch milliseconds, got {value!r}")
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _float_or_none(value, field: str) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float):
        raise InvalidRecord(f"'{field}' must be a number, got {value!r}")
    return float(value)


def parse_quake(feature: dict, ingested_at: datetime) -> dict:
    """GeoJSON feature -> record matching schemas/usgs_earthquake.avsc. Raises InvalidRecord."""
    p = feature.get("properties") or {}
    event_id = feature.get("id")
    if not event_id:
        raise InvalidRecord("missing event id")
    coords = (feature.get("geometry") or {}).get("coordinates") or []
    if len(coords) < 2:
        raise InvalidRecord("missing coordinates")
    lon, lat = _float_or_none(coords[0], "longitude"), _float_or_none(coords[1], "latitude")
    if lon is None or lat is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise InvalidRecord(f"coordinates out of range: {coords[:2]}")

    return {
        "event_id": event_id,
        "updated_at": _ms(p.get("updated"), "updated"),
        "origin_time": _ms(p.get("time"), "time"),
        "magnitude": _float_or_none(p.get("mag"), "mag"),
        "mag_type": p.get("magType"),
        "place": p.get("place"),
        "status": p.get("status") or "unknown",
        "event_type": p.get("type") or "unknown",
        "latitude": lat,
        "longitude": lon,
        "depth_km": _float_or_none(coords[2] if len(coords) > 2 else None, "depth"),
        "alert_level": p.get("alert"),
        "tsunami": bool(p.get("tsunami")),
        "significance": p.get("sig"),
        "network": p.get("net") or "unknown",
        "all_ids": [i for i in (p.get("ids") or "").split(",") if i],
        "url": p.get("url") or "",
        "ingested_at": ingested_at,
    }


def version_key(record: dict) -> str:
    """USGS keeps the event id when it revises an event; the update time marks the version."""
    return f"{record['event_id']}@{int(record['updated_at'].timestamp() * 1000)}"
