"""Consume alerts and earthquakes from Kafka and maintain live state in PostgreSQL.

    python -m src.processor

Each batch is written in one database transaction, and Kafka offsets are committed only
after that transaction succeeds. If the processor crashes in between, the batch is read
again and the idempotent writes in src/store.py turn the replay into a no-op. The result
is effectively-once state from at-least-once delivery.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from src.store import store_alert, store_quake
from src.topics import DLQ, NWS_ALERTS, USGS_QUAKES

log = logging.getLogger("processor")
GROUP_ID = "public-safety-processor"
LAG_EVERY_SECONDS = 30


@dataclass
class BatchResult:
    messages: int = 0
    new_alert_messages: int = 0
    new_quake_versions: int = 0
    duplicates: int = 0
    errors: int = 0


def consumer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,  # we commit only after the database commit
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }


def dead_letter(producer, msg, err: Exception) -> None:
    producer.produce(
        DLQ.name,
        key=msg.key() or b"unknown",
        value=json.dumps(
            {
                "source": msg.topic(),
                "stage": "process",
                "error_type": type(err).__name__,
                "error": str(err)[:1000],
                "partition": msg.partition(),
                "offset": msg.offset(),
                "failed_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
        ).encode(),  # fmt: skip
        headers={"stage": "process"},
    )


def process_batch(conn, messages, decode, dlq_producer) -> BatchResult:
    """Decode and store one batch in a single transaction. Undecodable or invalid messages
    go to the dead letter queue so one bad message never blocks the partition."""
    result = BatchResult(messages=len(messages))
    with conn.transaction(), conn.cursor() as cur:
        for msg in messages:
            if msg.error():
                result.errors += 1
                continue
            try:
                with conn.transaction():  # savepoint: a bad message rolls back only itself
                    record = decode(msg)
                    if msg.topic() == NWS_ALERTS.name:
                        new = store_alert(cur, record, msg.partition(), msg.offset())
                        result.new_alert_messages += new
                    elif msg.topic() == USGS_QUAKES.name:
                        new = store_quake(cur, record)
                        result.new_quake_versions += new
                    else:
                        continue
                    result.duplicates += not new
            except Exception as err:
                result.errors += 1
                log.warning("message %s[%s]@%s failed: %s", msg.topic(), msg.partition(),
                            msg.offset(), err)  # fmt: skip
                dead_letter(dlq_producer, msg, err)
    if dlq_producer is not None:
        dlq_producer.flush(10)
    return result


def record_batch(conn, result: BatchResult, seconds: float) -> None:
    conn.execute(
        "insert into processor_batches values (now(), %s, %s, %s, %s, %s, %s) "
        "on conflict (batch_at) do nothing",
        (result.messages, result.new_alert_messages, result.new_quake_versions,
         result.duplicates, result.errors, round(seconds, 3)),
    )  # fmt: skip


def measure_lag(consumer) -> list[tuple[str, int, int]]:
    """(topic, partition, lag) for every assigned partition: how far behind the log end we are."""
    lags = []
    assignment = consumer.assignment()
    if not assignment:
        return lags
    for tp in consumer.position(assignment):
        _low, high = consumer.get_watermark_offsets(tp, timeout=5)
        position = tp.offset if tp.offset >= 0 else _low
        lags.append((tp.topic, tp.partition, max(0, high - position)))
    return lags


def record_lag(conn, lags) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "insert into consumer_lag values (now(), %s, %s, %s) on conflict do nothing", lags
        )
        cur.execute("delete from consumer_lag where measured_at < now() - interval '7 days'")
        cur.execute("delete from processor_batches where batch_at < now() - interval '7 days'")


def make_decoder(registry):
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    deserializer = AvroDeserializer(registry)  # reads the schema id from each message
    return lambda msg: deserializer(
        msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
    )


def run(consumer, conn, decode, dlq_producer, should_stop, max_batch=500) -> None:
    last_lag = 0.0
    while not should_stop():
        messages = consumer.consume(num_messages=max_batch, timeout=1.0)
        if messages:
            started = time.monotonic()
            result = process_batch(conn, messages, decode, dlq_producer)
            record_batch(conn, result, time.monotonic() - started)
            consumer.commit(asynchronous=False)  # only after the database commit
            if result.new_alert_messages or result.new_quake_versions or result.errors:
                log.info("batch: %s", result.__dict__)
        if time.monotonic() - last_lag > LAG_EVERY_SECONDS:
            record_lag(conn, measure_lag(consumer))
            last_lag = time.monotonic()


def main() -> None:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient

    from src.census import ensure_counties
    from src.db import apply_schema, connect
    from src.feeds import build_session

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    conn = connect(os.environ["DATABASE_URL"])
    conn.autocommit = True  # transactions are explicit
    apply_schema(conn, os.environ.get("GRAFANA_DB_PASSWORD"))
    census = ensure_counties(
        conn, build_session(os.environ.get("CONTACT_USER_AGENT", "")),
        os.environ.get("CENSUS_VINTAGE", "2023"), os.environ.get("CENSUS_API_KEY") or None,
    )  # fmt: skip
    log.info("county population: %s", census)

    registry = SchemaRegistryClient(
        {"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")}
    )
    consumer = Consumer(consumer_config(bootstrap))
    consumer.subscribe([NWS_ALERTS.name, USGS_QUAKES.name])
    dlq_producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    try:
        run(consumer, conn, make_decoder(registry), dlq_producer, lambda: bool(stopping))
    finally:
        consumer.close()  # leaves the group cleanly so partitions are reassigned at once
        dlq_producer.flush(10)
        conn.close()
        log.info("processor stopped cleanly")


if __name__ == "__main__":
    main()
