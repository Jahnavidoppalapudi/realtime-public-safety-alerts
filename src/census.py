"""County population from the Census Bureau API (ACS 5-year estimates, table B01003).

Used to estimate how many people live in the counties an alert covers. Enrichment is
best-effort: if the Census API is unreachable, alerts still flow and population shows 0.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

log = logging.getLogger("census")

URL = "https://api.census.gov/data/{vintage}/acs/acs5"
DEFAULT_VINTAGE = "2023"
REFRESH_AFTER = timedelta(days=30)


def parse_counties(rows: list[list[str]], vintage: str) -> list[dict]:
    """Census API returns a header row then one list per county."""
    if not rows or rows[0][:2] != ["NAME", "B01003_001E"]:
        raise ValueError(f"unexpected Census response header: {rows[:1]}")
    header = rows[0]
    state_i, county_i = header.index("state"), header.index("county")
    counties = []
    for row in rows[1:]:
        name, population = row[0], row[1]
        if population is None or not population.lstrip("-").isdigit() or int(population) < 0:
            continue  # suppressed or missing estimate
        county_name, _, state_name = name.rpartition(", ")
        counties.append(
            {
                "county_fips": row[state_i] + row[county_i],
                "county_name": county_name or name,
                "state_name": state_name,
                "population": int(population),
                "vintage": vintage,
            }
        )
    return counties


def fetch_counties(session, vintage: str = DEFAULT_VINTAGE, api_key: str | None = None):
    params = {"get": "NAME,B01003_001E", "for": "county:*"}
    if api_key:
        params["key"] = api_key
    response = session.get(
        URL.format(vintage=vintage), params=params, timeout=60,
        headers={"Accept": "application/json"},
    )  # fmt: skip
    response.raise_for_status()
    return parse_counties(response.json(), vintage)


def load_counties(conn, counties: list[dict]) -> int:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            insert into county_population
                (county_fips, county_name, state_name, population, vintage, loaded_at)
            values (%(county_fips)s, %(county_name)s, %(state_name)s, %(population)s,
                    %(vintage)s, now())
            on conflict (county_fips) do update set
                county_name = excluded.county_name, state_name = excluded.state_name,
                population = excluded.population, vintage = excluded.vintage,
                loaded_at = excluded.loaded_at
            """,
            counties,
        )
    return len(counties)


def needs_refresh(conn, now: datetime | None = None) -> bool:
    now = now or datetime.now(UTC)
    count, newest = conn.execute(
        "select count(*), max(loaded_at) from county_population"
    ).fetchone()
    return count < 3000 or newest is None or now - newest > REFRESH_AFTER


def ensure_counties(conn, session, vintage=DEFAULT_VINTAGE, api_key=None) -> str:
    if not needs_refresh(conn):
        return "fresh"
    try:
        loaded = load_counties(conn, fetch_counties(session, vintage, api_key))
        return f"loaded {loaded} counties"
    except Exception as err:  # enrichment must never stop the stream
        log.warning("Census population load failed, continuing without it: %s", err)
        return f"failed: {err}"
