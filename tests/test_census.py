from datetime import UTC, datetime, timedelta

import pytest

from src.census import ensure_counties, load_counties, needs_refresh, parse_counties

# Shape of https://api.census.gov/data/2023/acs/acs5?get=NAME,B01003_001E&for=county:*
ROWS = [
    ["NAME", "B01003_001E", "state", "county"],
    ["Harris County, Texas", "4731145", "48", "201"],
    ["Doña Ana County, New Mexico", "220175", "35", "013"],
    ["Suppressed County, Nowhere", None, "99", "999"],
]


def test_parse_builds_five_digit_fips_and_splits_names():
    counties = parse_counties(ROWS, "2023")
    assert [
        (c["county_fips"], c["county_name"], c["state_name"], c["population"]) for c in counties
    ] == [
        ("48201", "Harris County", "Texas", 4731145),
        ("35013", "Doña Ana County", "New Mexico", 220175),
    ]  # suppressed estimate skipped


def test_unexpected_response_is_rejected():
    with pytest.raises(ValueError, match="unexpected Census response"):
        parse_counties([["error", "invalid key"]], "2023")


def test_load_is_an_upsert_and_refresh_rules(db):
    counties = parse_counties(ROWS, "2023")
    assert needs_refresh(db) is True  # empty table
    load_counties(db, counties)
    load_counties(db, [{**counties[0], "population": 4800000, "vintage": "2024"}])
    assert db.execute(
        "select population, vintage from county_population where county_fips = '48201'"
    ).fetchone() == (4800000, "2024")
    # Fewer than 3,000 counties means a partial load: refresh again
    assert needs_refresh(db) is True


def test_full_table_refreshes_only_after_30_days(db):
    rows = [{"county_fips": f"{i:05d}", "county_name": "C", "state_name": "S",
             "population": 1, "vintage": "2023"} for i in range(3100)]  # fmt: skip
    load_counties(db, rows)
    assert needs_refresh(db) is False
    assert needs_refresh(db, now=datetime.now(UTC) + timedelta(days=31)) is True


def test_census_outage_never_stops_the_pipeline(db):
    class DownSession:
        def get(self, *args, **kwargs):
            raise ConnectionError("api.census.gov unreachable")

    assert ensure_counties(db, DownSession()).startswith("failed:")
