"""Kafka topics as code: created (or confirmed) by the producer at start-up."""

from __future__ import annotations

from dataclasses import dataclass

DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_days: int
    description: str

    def config(self) -> dict[str, str]:
        return {"retention.ms": str(self.retention_days * DAY_MS), "cleanup.policy": "delete"}


NWS_ALERTS = TopicSpec("gov.nws.alerts.v1", 3, 30, "Every NWS alert message, keyed by incident")
USGS_QUAKES = TopicSpec(
    "gov.usgs.earthquakes.v1", 3, 30, "Every USGS event revision, keyed by event"
)
DLQ = TopicSpec("gov.ingest.dlq.v1", 1, 30, "Records that broke the contract, with the reason")

ALL_TOPICS = (NWS_ALERTS, USGS_QUAKES, DLQ)


def ensure_topics(admin, specs=ALL_TOPICS, timeout: float = 30) -> dict[str, str]:
    """Create missing topics. Safe to run on every start: existing topics are left alone."""
    from confluent_kafka import KafkaError
    from confluent_kafka.admin import NewTopic

    existing = set(admin.list_topics(timeout=timeout).topics)
    missing = [s for s in specs if s.name not in existing]
    result = {s.name: "exists" for s in specs if s.name in existing}
    if not missing:
        return result
    futures = admin.create_topics(
        [
            NewTopic(s.name, num_partitions=s.partitions, replication_factor=1, config=s.config())
            for s in missing
        ]  # fmt: skip
    )
    for name, future in futures.items():
        try:
            future.result(timeout)
            result[name] = "created"
        except Exception as err:  # another producer may have created it a moment ago
            code = err.args[0].code() if err.args and hasattr(err.args[0], "code") else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            result[name] = "exists"
    return result
