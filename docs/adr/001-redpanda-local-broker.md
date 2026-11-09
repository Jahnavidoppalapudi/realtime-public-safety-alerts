# ADR-001: Redpanda as the local Kafka-compatible broker

**Status:** Accepted
**Date:** 2026-11-09

## Context
The pipeline needs a Kafka-compatible event log with a schema registry, running on an
8 GB laptop alongside a stream processor, a database and Grafana.

## Options Considered
1. **Apache Kafka (KRaft) + Confluent Schema Registry:** the reference implementation;
   two JVM services and roughly 1.5–2 GB of memory before any data flows.
2. **Redpanda:** one container that speaks the Kafka protocol, with a built-in schema
   registry and admin metrics; runs in about 1 GB.
3. **A cloud service (Confluent Cloud, MSK, Redpanda Cloud):** production-like, but needs
   an account and costs money for a demo.

## Decision
Redpanda v26.1, single node in dev-container mode, capped at 1 GB and one core. All code uses
the standard Kafka client (confluent-kafka / librdkafka) and Schema Registry API, so nothing
is Redpanda-specific.

## Consequences
- Switching to Apache Kafka or a managed service changes connection settings only.
- One broker means replication factor 1: fine locally, never in production (use 3+ brokers,
  `min.insync.replicas=2`, rack awareness).
