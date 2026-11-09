# ADR-003: Avro with a schema registry, and how schemas may change

**Status:** Accepted
**Date:** 2026-11-09

## Context
Topics are a contract between this producer and every consumer. Field types, required fields
and timestamp semantics must be explicit, and the contract must be able to evolve without
breaking existing consumers.

## Decisions
- **Avro + Schema Registry** (`src/schemas/*.avsc`), registered under the topic name.
  Messages carry a schema id, not the schema, so they stay small.
- **Two timestamps per record:** *event time* (`sent` for alerts, `origin_time` for earthquakes)
  and *processing time* (`ingested_at`). Windows and lateness are computed on event time.
- **Strings, not Avro enums, for upstream codes** such as `message_type` and `severity`. If NWS
  adds a new value, an enum would fail serialization; a string flows through, and validation
  lives in the stream processor where it can be monitored.
- **Optional fields are `["null", type]` with a default**, so a new optional field is a
  backward-compatible change.
- **Topic names carry a major version** (`.v1`). A breaking change goes to a new `.v2` topic
  while consumers migrate.

## Consequences
- Adding an optional field with a default: safe, no coordination needed.
- Removing or retyping a field: new topic version and a migration window.
- Schemas are registered automatically by the producer; in production I would register them
  from CI and turn off auto-registration so an unreviewed change cannot reach the topic.
