# ADR-004: Stream processing as a Kafka consumer with an idempotent PostgreSQL sink

**Status:** Accepted
**Date:** 2026-11-09

## Context
The processor must turn two topics into live state: which hazards are in effect, how many
people they cover, and how earthquakes were revised. Volume is small (hundreds of messages
per minute at peak), but correctness is subtle: messages arrive late, out of order, and more
than once, and an incident's state depends on its whole message history.

## Options Considered
1. **Apache Flink or Spark Structured Streaming:** event-time windows, watermarks and managed
   state out of the box. Both need a JVM cluster (1.5–3 GB) on an 8 GB laptop, plus a sink
   for the dashboards anyway. Built for orders of magnitude more data than this.
2. **Kafka Streams / ksqlDB:** state in Kafka; ksqlDB is another JVM service, and the
   dashboard still needs a queryable store.
3. **A Python consumer that writes state to PostgreSQL (chosen):** PostgreSQL is both the state
   store and what Grafana queries. Correctness comes from idempotent, order-independent writes.

## Decision
- Consume both topics in one consumer group with manual offset commits.
- Write each batch in **one database transaction**, then commit offsets. A crash between the two
  replays the batch; every write is keyed on the message version and does nothing the second
  time. Result: effectively-once state from at-least-once delivery.
- **Latest version wins, regardless of arrival order:** incident and earthquake state is only
  overwritten by a message with a newer event time (`sent`, `updated_at`), so a delayed update
  can never reopen a cancelled warning or roll back a revised magnitude.
- **Windows are computed in SQL on event time** (`date_bin` over `sent`). Because state is stored,
  a late message simply lands in the correct past bucket; nothing is dropped by a watermark.
- **Per-message savepoints:** an undecodable or invalid message rolls back only itself and goes to
  the dead letter queue; the partition keeps moving.
- **Pipeline health is data:** consumer lag (every 30 s), per-batch counts and latency are stored
  and charted next to the business metrics.

## Consequences
- Simple to run and to test: the full path is tested in CI against a mock broker and a real
  PostgreSQL, including crash-and-replay.
- One consumer instance is plenty here. To scale out, add instances to the consumer group (up to
  the partition count); writes are already safe to run concurrently because they are idempotent.
- If volume grew 100x, or windows had to be emitted continuously with bounded memory, I would move
  the computation to Flink and keep PostgreSQL (or a time-series store) only for serving.
