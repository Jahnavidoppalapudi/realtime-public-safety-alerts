from datetime import UTC, datetime

import pytest
from conftest import NOW

from src import nws, usgs
from src.nws import InvalidRecord


def test_alert_is_normalized_to_utc_with_county_fips(nws_features):
    alert = nws.parse_alert(nws_features[0], NOW)
    assert alert["county_fips"] == ["48201", "48157"]  # SAME 048201 -> FIPS 48201
    assert alert["sent"] == datetime(2026, 5, 12, 19, 5, tzinfo=UTC)
    assert alert["incident_key"] == alert["alert_id"]  # original alert starts its own incident


def test_update_shares_the_incident_key_of_the_original(nws_features):
    original = nws.parse_alert(nws_features[0], NOW)
    update = nws.parse_alert(nws_features[1], NOW)
    assert update["message_type"] == "Update"
    assert update["incident_key"] == original["alert_id"]
    assert update["onset"] is None and update["ends"] is None


def test_marine_alert_without_same_codes_has_no_counties(nws_features):
    assert nws.parse_alert(nws_features[2], NOW)["county_fips"] == []


def test_malformed_same_code_is_rejected(nws_features):
    with pytest.raises(InvalidRecord, match="malformed SAME code"):
        nws.parse_alert(nws_features[3], NOW)


@pytest.mark.parametrize("field", ["messageType", "severity", "event"])
def test_missing_required_alert_field_is_rejected(nws_features, field):
    del nws_features[0]["properties"][field]
    with pytest.raises(InvalidRecord, match=field):
        nws.parse_alert(nws_features[0], NOW)


def test_naive_timestamp_is_rejected(nws_features):
    nws_features[0]["properties"]["sent"] = "2026-05-12T14:05:00"
    with pytest.raises(InvalidRecord, match="no time zone"):
        nws.parse_alert(nws_features[0], NOW)


def test_quake_versions_and_fields(usgs_features):
    quake = usgs.parse_quake(usgs_features[0], NOW)
    assert quake["all_ids"] == ["ak0test001", "us7000test"]
    assert quake["depth_km"] == 35.4 and quake["tsunami"] is False
    assert usgs.version_key(quake) == "ak0test001@1778598600000"


def test_quake_revision_is_a_new_version(usgs_features):
    first = usgs.parse_quake(usgs_features[0], NOW)
    usgs_features[0]["properties"].update(mag=4.8, updated=1778599999000)
    revised = usgs.parse_quake(usgs_features[0], NOW)
    assert revised["event_id"] == first["event_id"]
    assert usgs.version_key(revised) != usgs.version_key(first)


def test_quake_without_magnitude_is_allowed(usgs_features):
    blast = usgs.parse_quake(usgs_features[1], NOW)
    assert blast["magnitude"] is None and blast["event_type"] == "quarry blast"


def test_quake_with_bad_timestamp_is_rejected(usgs_features):
    with pytest.raises(InvalidRecord, match="epoch milliseconds"):
        usgs.parse_quake(usgs_features[2], NOW)
