# ADR-002: At-least-once delivery, idempotent consumers, and incident-based keys

**Status:** Accepted
**Date:** 2026-11-09

## Context
Both sources are *polled*, not pushed: every minute the producer receives every active NWS
alert and every USGS event from the last 24 hours, most of which it has already published.
The same hazard also changes over time: NWS issues Update and Cancel messages that reference
earlier alerts, and USGS revises an earthquake's magnitude and location under the same id.

## Decisions

**Keys.** USGS messages are keyed by event id. NWS messages are keyed by an *incident key*:
the earliest alert id the message references (or its own id if it references none). Kafka keeps
each key on one partition in order, so a warning, its updates and its cancellation are consumed
in sequence.

**Versions.** Every message carries a `version_key` header: the alert id for NWS (each message
has its own id) and `event_id@updated` for USGS.

**Delivery.** The producer is idempotent (`enable.idempotence=true`, `acks=all`), so broker-level
retries never duplicate. A bounded in-memory cache skips versions already published, and a
version is only marked published once the broker confirms delivery, so a failed send is retried
on the next poll. After a producer restart, recent versions are sent once more. Consumers
deduplicate on `version_key`. This is at-least-once delivery with idempotent consumers.

**Bad data.** Records that break the contract (missing fields, malformed codes, wrong types)
go to `gov.ingest.dlq.v1` with the stage, error and original payload. One bad record never
stops the feed, and nothing disappears silently.

## Options Considered for delivery
1. **Exactly-once with Kafka transactions:** guarantees no duplicates in the log, but only
   inside Kafka. The source is a polled HTTP feed, so the producer cannot know what it sent
   before a crash without its own durable state.
2. **Persist the seen-cache to disk:** removes most restart duplicates, adds state to manage.
3. **At-least-once + idempotent consumers (chosen):** simplest correct design; duplicates are
   rare (only after restarts) and harmless because every consumer keys on `version_key`.

## Consequences
- Consumers must upsert on `version_key`, never blindly append.
- The incident key depends on NWS including references; when a long chain drops early
  references, a later message may start a new key. The stream processor resolves incidents
  by following references, not by key alone.
