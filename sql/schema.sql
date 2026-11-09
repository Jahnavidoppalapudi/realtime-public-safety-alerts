-- Applied by the processor on every start. Every statement is safe to re-run.

-- Reference data: county population from the Census Bureau (ACS 5-year estimates)
create table if not exists county_population (
    county_fips  char(5) primary key,
    county_name  text not null,
    state_name   text not null,
    population   integer not null check (population >= 0),
    vintage      text not null,
    loaded_at    timestamptz not null default now()
);

-- Every NWS alert message exactly once (alert_id is the version key)
create table if not exists alert_messages (
    alert_id        text primary key,
    incident_id     text not null,
    message_type    text not null,
    status          text not null,
    event           text not null,
    category        text,
    severity        text not null,
    urgency         text not null,
    certainty       text not null,
    headline        text,
    area_desc       text not null,
    county_fips     text[] not null,
    sender_name     text,
    sent            timestamptz not null,
    effective       timestamptz not null,
    onset           timestamptz,
    expires         timestamptz not null,
    ends            timestamptz,
    ingested_at     timestamptz not null,
    processed_at    timestamptz not null default now(),
    kafka_partition integer,
    kafka_offset    bigint
);
create index if not exists alert_messages_incident on alert_messages (incident_id);
create index if not exists alert_messages_sent on alert_messages (sent);

-- Which earlier messages each message updates or cancels
create table if not exists alert_references (
    alert_id       text not null,
    referenced_id  text not null,
    primary key (alert_id, referenced_id)
);
create index if not exists alert_references_referenced on alert_references (referenced_id);

-- Current state of each hazard: the latest message wins, whatever order messages arrive in
create table if not exists incidents (
    incident_id      text primary key,
    event            text not null,
    status           text not null,
    state            text not null check (state in ('active', 'cancelled')),
    severity         text not null,
    headline         text,
    area_desc        text not null,
    county_fips      text[] not null,
    first_sent       timestamptz not null,
    last_sent        timestamptz not null,
    expires          timestamptz not null,
    ends             timestamptz,
    latest_alert_id  text not null,
    message_count    integer not null
);
create index if not exists incidents_open on incidents (expires) where state = 'active';

-- Every version of every earthquake, and its latest state
create table if not exists quake_versions (
    event_id      text not null,
    updated_at    timestamptz not null,
    origin_time   timestamptz not null,
    magnitude     double precision,
    mag_type      text,
    place         text,
    status        text not null,
    event_type    text not null,
    latitude      double precision not null,
    longitude     double precision not null,
    depth_km      double precision,
    alert_level   text,
    tsunami       boolean not null,
    significance  integer,
    ingested_at   timestamptz not null,
    processed_at  timestamptz not null default now(),
    primary key (event_id, updated_at)
);

create table if not exists quakes (
    event_id         text primary key,
    updated_at       timestamptz not null,
    origin_time      timestamptz not null,
    magnitude        double precision,
    first_magnitude  double precision,
    mag_type         text,
    place            text,
    status           text not null,
    event_type       text not null,
    latitude         double precision not null,
    longitude        double precision not null,
    depth_km         double precision,
    alert_level      text,
    tsunami          boolean not null,
    version_count    integer not null,
    first_version_at timestamptz not null,
    first_seen_at    timestamptz not null
);
create index if not exists quakes_origin on quakes (origin_time);

-- Pipeline health
create table if not exists consumer_lag (
    measured_at  timestamptz not null,
    topic        text not null,
    partition    integer not null,
    lag          bigint not null,
    primary key (measured_at, topic, partition)
);

create table if not exists processor_batches (
    batch_at            timestamptz primary key,
    messages            integer not null,
    new_alert_messages  integer not null,
    new_quake_versions  integer not null,
    duplicates          integer not null,
    errors              integer not null,
    seconds             numeric not null
);

-- ---------------------------------------------------------------- views for dashboards

-- Hazards in effect right now, with the population of the counties they cover
create or replace view v_active_incidents as
select
    i.incident_id,
    i.event,
    i.severity,
    i.headline,
    i.area_desc,
    i.first_sent,
    i.last_sent,
    i.expires,
    i.message_count,
    cardinality(i.county_fips) as counties,
    coalesce(p.population, 0) as population,
    cardinality(i.county_fips) - coalesce(p.matched, 0) as unmatched_counties
from incidents as i
left join lateral (
    select sum(c.population) as population, count(*) as matched
    from county_population as c
    where c.county_fips = any (i.county_fips)
) as p on true
where i.state = 'active'
  and i.status = 'Actual'
  and i.expires > now()
  and (i.ends is null or i.ends > now());

-- People under at least one active warning, by state (each county counted once)
create or replace view v_people_under_warnings as
select c.state_name, count(*) as counties, sum(c.population) as population
from (
    select distinct unnest(i.county_fips) as county_fips
    from incidents as i
    where i.state = 'active'
      and i.status = 'Actual'
      and i.event like '%Warning'
      and i.expires > now()
      and (i.ends is null or i.ends > now())
) as warned
join county_population as c using (county_fips)
group by c.state_name;

-- How late data reaches us: issued -> ingested, and ingested -> stored
create or replace view v_alert_latency as
select
    date_bin('15 minutes', processed_at, timestamptz '2000-01-01') as bucket,
    count(*) as messages,
    percentile_cont(0.95) within group (order by extract(epoch from ingested_at - sent))
        as p95_source_to_ingest_seconds,
    percentile_cont(0.95) within group (order by extract(epoch from processed_at - ingested_at))
        as p95_ingest_to_store_seconds
from alert_messages
group by 1;

-- Earthquakes whose magnitude changed after first publication
create or replace view v_quake_revisions as
select
    event_id, place, origin_time, first_magnitude, magnitude as latest_magnitude,
    round((magnitude - first_magnitude)::numeric, 2) as magnitude_change,
    version_count, status
from quakes
where version_count > 1
  and magnitude is distinct from first_magnitude;
