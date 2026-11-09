# Real-Time Public Safety Alerts

![CI](https://github.com/Jahnavidoppalapudi/realtime-public-safety-alerts/actions/workflows/ci.yml/badge.svg)

> A streaming pipeline for live U.S. government hazard data: National Weather Service alerts
> and USGS earthquakes. Alerts are updated and cancelled after they are issued, earthquakes are
> revised as more data arrives, and both arrive by polling. The pipeline turns that into a clean,
> ordered, deduplicated event stream.

**Status:** 🚧 Ingestion complete. Stream processing (event-time windows, incident state,
county population impact) and live Grafana dashboards in progress.

## The Problem

Public safety data is not a simple append-only feed:

- A **tornado warning** is followed by updates (area narrowed, time extended) and a
  cancellation. Each is a new message that references earlier ones.
- An **earthquake** is first published automatically, then revised (magnitude 4.6 → 4.8,
  location refined, reviewed by a seismologist) under the same event id.
- Both feeds are **polled**: every minute returns everything still active, so most of each
  response has already been seen.

Downstream consumers need each change exactly once, in order per incident, with bad records
isolated instead of breaking the stream.

## Architecture

```
api.weather.gov (NWS alerts) ──┐   conditional GET          ┌─▶ gov.nws.alerts.v1        (key: incident)
                               ├─▶ Python producer ─────────┼─▶ gov.usgs.earthquakes.v1  (key: event id)
USGS earthquake feed ──────────┘   validate · dedupe · Avro └─▶ gov.ingest.dlq.v1        (rejected + reason)
                                           │                            │
                                    Schema Registry               Redpanda (Kafka API)
                                                                        │
                                              (next) stream processing ─▶ Postgres ─▶ Grafana
```

## Tech Stack

| Layer | Tool |
|---|---|
| Event log | Redpanda (Kafka API) with built-in Schema Registry |
| Serialization | Avro schemas, registered per topic |
| Producer | Python, confluent-kafka (librdkafka), idempotent producer |
| Sources | NWS alerts API (CAP), USGS real-time earthquake GeoJSON feed |
| Testing | pytest with librdkafka's in-process mock broker (no Docker needed in CI) |
| Next | Stream processing, PostgreSQL, Grafana |

## Ingestion: How It Works

Every 60 seconds the producer polls both feeds and, for each record:

1. **Validates and normalizes** it: UTC timestamps, county FIPS codes derived from NWS SAME codes
   (for joining with Census population), references to earlier alerts.
2. **Skips versions already published.** Each message carries a `version_key` header.
3. **Serializes with Avro** against the registered schema.
4. **Publishes with a key** that keeps related messages ordered: an alert and all its updates
   share an *incident key*; earthquake revisions share the event id.
5. **Dead-letters** anything that breaks the contract, with the stage, error and original payload.

**Reliability choices**

- **Conditional requests:** the producer sends the previous `ETag` / `Last-Modified`, so an
  unchanged feed returns `304 Not Modified` instead of the full payload.
- **Idempotent producer** (`acks=all`): retries never duplicate messages in the log.
- **Confirmed before remembered:** a version is only marked published once the broker confirms
  it, so a failed send is retried on the next poll instead of being lost.
- **At-least-once with idempotent consumers:** after a restart, recent versions may be sent again;
  consumers deduplicate on `version_key` ([ADR-002](docs/adr/002-delivery-guarantees-and-keys.md)).
- **Isolation:** one feed being down never stops the other; one bad record never stops a feed.
- **24-hour USGS window:** a producer outage of up to a day loses no earthquake revisions.
- **Topics as code:** partitions and retention are defined in `src/topics.py` and created on start.

## Key Design Decisions

- [ADR-001](docs/adr/001-redpanda-local-broker.md): Redpanda as a lightweight Kafka-compatible broker
- [ADR-002](docs/adr/002-delivery-guarantees-and-keys.md): at-least-once delivery, idempotent consumers, incident keys
- [ADR-003](docs/adr/003-avro-schemas.md): Avro schemas, event vs processing time, safe schema evolution

## How to Run

Requires Docker Desktop and Python 3.12.

```bash
git clone git@github.com:Jahnavidoppalapudi/realtime-public-safety-alerts.git
cd realtime-public-safety-alerts
make env          # creates .env; set CONTACT_USER_AGENT to your app name and email
make setup        # local dev tools
make test         # unit tests + Kafka tests against an in-process mock broker
make up           # start Redpanda, Console and the producer
make logs         # per-cycle stats: fetched, published, already seen, dead-lettered
make topics       # topics with message counts
make peek         # newest alerts, decoded from Avro
make dlq          # rejected records and why
```

- Redpanda Console: http://localhost:8085 (topics, messages, schemas, consumer lag)

## Project Structure

```
├── src/
│   ├── producer.py   # polling loop, graceful shutdown
│   ├── pipeline.py   # validate → dedupe → serialize → publish → DLQ
│   ├── nws.py        # NWS alert parsing, incident keys, SAME → FIPS
│   ├── usgs.py       # USGS event parsing and versioning
│   ├── feeds.py      # conditional HTTP polling with retries
│   ├── topics.py     # topic definitions
│   └── schemas/      # Avro schemas
├── tests/            # parser tests + end-to-end Kafka tests (mock broker)
└── docs/adr/         # architecture decision records
```

## What I'd Do in Production

- Three or more brokers with replication factor 3 and `min.insync.replicas=2`.
- Register schemas from CI with compatibility checks; disable auto-registration.
- Alert on producer cycle failures, dead-letter rate, and feed freshness (no new USGS version
  in N minutes usually means the feed or the producer is stuck).
- Persist producer state (ETags, seen versions) to survive restarts without re-sending.
- Run the producer with two replicas and leader election, or as a scheduled job with a lock.

## Data Sources

- National Weather Service, [api.weather.gov](https://www.weather.gov/documentation/services-web-api) (public domain). Requests identify the app and a contact email, as NWS requires.
- U.S. Geological Survey, real-time earthquake feeds (public domain).
