"""Turn a batch of feed records into Kafka messages: validate, deduplicate, serialize, publish.

Delivery guarantees:
- The producer is idempotent (enable.idempotence, acks=all): broker retries never duplicate.
- A bounded in-memory cache skips versions already published, so polling every minute does not
  re-send every active alert. After a restart, recent versions are sent once more; consumers
  deduplicate on the version key carried in every message header. This is at-least-once delivery
  with idempotent consumers, a deliberate choice over exactly-once (see ADR-002).
- Anything that breaks the contract goes to the dead letter queue with the reason, instead of
  stopping the pipeline or silently disappearing.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from src.nws import InvalidRecord

MAX_DLQ_PAYLOAD_BYTES = 100_000


class SeenCache:
    """Remembers the most recent N version keys (least recently seen are forgotten first)."""

    def __init__(self, max_size: int = 50_000):
        self.max_size = max_size
        self._keys: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, key: str) -> bool:
        if key in self._keys:
            self._keys.move_to_end(key)
            return True
        return False

    def add(self, key: str) -> None:
        self._keys[key] = None
        self._keys.move_to_end(key)
        while len(self._keys) > self.max_size:
            self._keys.popitem(last=False)

    def __len__(self) -> int:
        return len(self._keys)


@dataclass
class Source:
    name: str
    topic: str
    parse: Callable[[dict, datetime], dict]
    key: Callable[[dict], str]
    version_key: Callable[[dict], str]
    serialize: Callable[[dict, str], bytes]


@dataclass
class BatchStats:
    source: str
    fetched: int = 0
    published: int = 0
    already_seen: int = 0
    dead_lettered: int = 0
    delivery_failed: int = 0
    reasons: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def dlq_message(source: str, stage: str, error: Exception, raw) -> bytes:
    payload = json.dumps(raw, default=str)
    return json.dumps(
        {
            "source": source,
            "stage": stage,
            "error_type": type(error).__name__,
            "error": str(error),
            "failed_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "raw": payload[:MAX_DLQ_PAYLOAD_BYTES],
            "raw_truncated": len(payload) > MAX_DLQ_PAYLOAD_BYTES,
        }
    ).encode()


def publish_batch(
    producer,
    source: Source,
    features: list[dict],
    seen: SeenCache,
    dlq_topic: str,
    now: datetime | None = None,
    flush_timeout: float = 30,
) -> BatchStats:
    """Publish one fetched batch and wait for the broker to confirm it.

    A version is only marked as seen once the broker confirms delivery, so a failed send is
    retried on the next poll instead of being lost.
    """
    stats = BatchStats(source.name, fetched=len(features))
    now = now or datetime.now(UTC)
    in_flight: set[str] = set()

    def on_delivery(version: str):
        def callback(err, _msg):
            in_flight.discard(version)
            if err is None:
                seen.add(version)
                stats.published += 1
            else:
                stats.delivery_failed += 1

        return callback

    def dead_letter(stage: str, err: Exception, raw, key: str | None) -> None:
        stats.dead_lettered += 1
        stats.reasons[str(err)[:80]] = stats.reasons.get(str(err)[:80], 0) + 1
        producer.produce(
            dlq_topic,
            key=(key or "unknown").encode(),
            value=dlq_message(source.name, stage, err, raw),
            headers={"source": source.name, "stage": stage},
        )

    for feature in features:
        try:
            record = source.parse(feature, now)
        except (InvalidRecord, KeyError, TypeError) as err:
            dead_letter(
                "parse", err, feature, feature.get("id") if isinstance(feature, dict) else None
            )
            continue

        version = source.version_key(record)
        if version in seen or version in in_flight:
            stats.already_seen += 1
            continue
        try:
            value = source.serialize(record, source.topic)
        except Exception as err:  # schema violation: wrong type, missing field
            dead_letter("serialize", err, feature, version)
            continue

        producer.produce(
            source.topic,
            key=source.key(record).encode(),
            value=value,
            headers={"version_key": version, "source": source.name},
            on_delivery=on_delivery(version),
        )
        in_flight.add(version)
        producer.poll(0)  # serve delivery callbacks without blocking

    remaining = producer.flush(flush_timeout)
    if remaining:
        stats.delivery_failed += remaining
    return stats
