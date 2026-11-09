"""Producer -> mock Kafka broker -> processor -> real PostgreSQL."""

import json
import uuid

import pytest
from conftest import NOW

pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, Producer  # noqa: E402
from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (  # noqa: E402
    MockSchemaRegistryClient,
)

from src.pipeline import SeenCache, publish_batch  # noqa: E402
from src.processor import consumer_config, make_decoder, measure_lag, run  # noqa: E402
from src.producer import build_sources, kafka_config  # noqa: E402
from src.topics import DLQ, NWS_ALERTS, USGS_QUAKES  # noqa: E402


@pytest.fixture
def stream(nws_features, usgs_features):
    """A mock broker loaded with the fixture feeds plus one corrupt message."""
    producer = Producer({**kafka_config(), "test.mock.num.brokers": 1, "log_level": 0})
    bootstrap = producer.list_topics(timeout=10).orig_broker_name.split("/")[0]
    registry = MockSchemaRegistryClient({"url": f"http://{uuid.uuid4()}"})
    (_, nws_source), (_, usgs_source) = build_sources(registry)
    publish_batch(producer, nws_source, nws_features, SeenCache(), DLQ.name, now=NOW)
    publish_batch(producer, usgs_source, usgs_features, SeenCache(), DLQ.name, now=NOW)
    producer.produce(NWS_ALERTS.name, key=b"garbage", value=b"not avro at all")
    producer.flush(10)
    yield bootstrap, registry, producer


def consume_everything(bootstrap, registry, db, group, expected=6):
    consumer = Consumer({**consumer_config(bootstrap), "group.id": group, "log_level": 0})
    consumer.subscribe([NWS_ALERTS.name, USGS_QUAKES.name])
    dlq = Producer({"bootstrap.servers": bootstrap, "log_level": 0})
    loops = []

    def stop():
        loops.append(1)
        done = db.execute("select coalesce(sum(messages), 0) from processor_batches").fetchone()[0]
        return done >= expected or len(loops) > 30

    run(consumer, db, make_decoder(registry), dlq, stop)
    return consumer


def test_stream_is_processed_into_live_state(stream, db):
    bootstrap, registry, _ = stream
    consumer = consume_everything(bootstrap, registry, db, group="processor")

    # 3 valid alerts, 2 valid quakes, 1 corrupt message
    counts = db.execute(
        "select (select count(*) from alert_messages), (select count(*) from incidents), "
        "(select count(*) from quakes), (select sum(errors) from processor_batches)"
    ).fetchone()
    assert counts == (3, 2, 2, 1)  # alert + its update share one incident

    # Lag is measurable and zero once caught up
    assert all(lag == 0 for _, _, lag in measure_lag(consumer))
    consumer.close()

    # The corrupt message went to the dead letter queue with the reason
    reader = Consumer({"bootstrap.servers": bootstrap, "group.id": "dlq-check",
                       "auto.offset.reset": "earliest", "log_level": 0})  # fmt: skip
    reader.subscribe([DLQ.name])
    stages = []
    for _ in range(40):
        msg = reader.poll(0.5)
        if msg is not None and not msg.error():
            stages.append((msg.key(), json.loads(msg.value())["stage"]))
    reader.close()
    assert (b"garbage", "process") in stages


def test_committed_offsets_mean_a_restart_reads_nothing_twice(stream, db):
    bootstrap, registry, _ = stream
    consume_everything(bootstrap, registry, db, group="restart").close()

    restarted = Consumer({**consumer_config(bootstrap), "group.id": "restart", "log_level": 0})
    restarted.subscribe([NWS_ALERTS.name, USGS_QUAKES.name])
    assert [m for m in restarted.consume(num_messages=50, timeout=5) if not m.error()] == []
    restarted.close()


def test_replaying_the_whole_stream_changes_nothing(stream, db):
    """Simulates a crash before the offset commit: everything is delivered again."""
    bootstrap, registry, _ = stream
    consume_everything(bootstrap, registry, db, group="first").close()
    before = db.execute("select * from incidents order by incident_id").fetchall()

    consume_everything(bootstrap, registry, db, group="replay", expected=12).close()
    after = db.execute("select * from incidents order by incident_id").fetchall()
    assert after == before
    new, duplicates = db.execute(
        "select sum(new_alert_messages + new_quake_versions), sum(duplicates) "
        "from processor_batches"
    ).fetchone()
    assert (new, duplicates) == (5, 5)  # 5 stored the first time, 5 recognised on replay
    assert db.execute("select count(*) from alert_messages").fetchone()[0] == 3
