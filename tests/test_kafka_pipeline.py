"""End to end against librdkafka's built-in mock broker and an in-memory schema registry.

No Docker needed: this runs the real Kafka client and real Avro serialization, so it also
runs in CI. The mock broker creates topics automatically and does not support the admin
API, so topic creation is tested separately with a fake admin client.
"""

import json
import uuid

import pytest
from conftest import NOW

pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer  # noqa: E402
from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (  # noqa: E402
    MockSchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from src.pipeline import SeenCache, publish_batch  # noqa: E402
from src.producer import build_sources, kafka_config  # noqa: E402
from src.topics import ALL_TOPICS, DLQ, NWS_ALERTS, USGS_QUAKES, ensure_topics  # noqa: E402


@pytest.fixture
def kafka():
    producer = Producer({**kafka_config(), "test.mock.num.brokers": 1, "log_level": 0})
    bootstrap = producer.list_topics(timeout=10).orig_broker_name.split("/")[0]
    registry = MockSchemaRegistryClient({"url": f"http://{uuid.uuid4()}"})
    yield producer, bootstrap, registry
    producer.flush(5)


def consume_all(bootstrap, topic, expected, timeout=20):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": str(uuid.uuid4()),
                         "auto.offset.reset": "earliest", "log_level": 0})  # fmt: skip
    consumer.subscribe([topic])
    messages = []
    for _ in range(timeout * 2):
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            messages.append(msg)
        if len(messages) >= expected:
            break
    consumer.close()
    return messages


class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self, timeout=None):
        if self.error:
            raise self.error


class FakeAdmin:
    """Stands in for AdminClient: remembers topics and the configs they were created with."""

    def __init__(self, existing=(), race=()):
        self.topics = {name: {} for name in existing}
        self.race = set(race)  # topics another producer creates at the same moment

    def list_topics(self, timeout=None):
        return type("Metadata", (), {"topics": dict(self.topics)})()

    def create_topics(self, new_topics):
        futures = {}
        for t in new_topics:
            if t.topic in self.race:
                futures[t.topic] = FakeFuture(
                    KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
                )
            else:
                self.topics[t.topic] = {"partitions": t.num_partitions, "config": t.config}
                futures[t.topic] = FakeFuture()
        return futures


def test_topics_are_created_once_with_retention():
    admin = FakeAdmin()
    assert set(ensure_topics(admin).values()) == {"created"}
    assert set(ensure_topics(admin).values()) == {"exists"}  # safe to run on every start
    created = admin.topics[NWS_ALERTS.name]
    assert created["partitions"] == 3
    assert created["config"]["retention.ms"] == str(30 * 24 * 60 * 60 * 1000)
    assert {t.name for t in ALL_TOPICS} == set(admin.topics)


def test_topic_created_by_someone_else_meanwhile_is_fine():
    result = ensure_topics(FakeAdmin(existing=[DLQ.name], race=[USGS_QUAKES.name]))
    assert result == {DLQ.name: "exists", USGS_QUAKES.name: "exists", NWS_ALERTS.name: "created"}


def test_real_topic_errors_are_raised():
    class BrokenAdmin(FakeAdmin):
        def create_topics(self, new_topics):
            err = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
            return {t.topic: FakeFuture(err) for t in new_topics}

    with pytest.raises(KafkaException):
        ensure_topics(BrokenAdmin())


def test_alerts_published_as_avro_keyed_by_incident(kafka, nws_features):
    producer, bootstrap, registry = kafka
    (_, nws_source), _ = build_sources(registry)
    seen = SeenCache()

    stats = publish_batch(producer, nws_source, nws_features, seen, DLQ.name, now=NOW)
    assert (stats.published, stats.dead_lettered, stats.delivery_failed) == (3, 1, 0)

    messages = consume_all(bootstrap, NWS_ALERTS.name, expected=3)
    decode = AvroDeserializer(registry)
    alerts = [
        decode(m.value(), SerializationContext(m.topic(), MessageField.VALUE)) for m in messages
    ]
    by_id = {
        a["alert_id"]: (m.key().decode(), a, m.partition())
        for m, a in zip(messages, alerts, strict=True)
    }

    original_key, original, original_partition = by_id["urn:oid:2.49.0.1.840.0.test.001.1"]
    update_key, update, update_partition = by_id["urn:oid:2.49.0.1.840.0.test.001.2"]
    assert update_key == original_key == original["alert_id"]
    assert update_partition == original_partition  # whole incident stays in order
    assert update["county_fips"] == ["48201"]
    assert dict(messages[0].headers())["version_key"]


def test_repeated_poll_publishes_nothing_new(kafka, nws_features):
    producer, _, registry = kafka
    (_, nws_source), _ = build_sources(registry)
    seen = SeenCache()
    publish_batch(producer, nws_source, nws_features, seen, DLQ.name, now=NOW)

    again = publish_batch(producer, nws_source, nws_features, seen, DLQ.name, now=NOW)
    assert (again.published, again.already_seen) == (0, 3)


def test_bad_records_go_to_dlq_with_reason(kafka, usgs_features):
    producer, bootstrap, registry = kafka
    _, (_, usgs_source) = build_sources(registry)

    stats = publish_batch(producer, usgs_source, usgs_features, SeenCache(), DLQ.name, now=NOW)
    assert (stats.published, stats.dead_lettered) == (2, 1)

    [dead] = consume_all(bootstrap, DLQ.name, expected=1)
    body = json.loads(dead.value())
    assert dead.key() == b"xx0testbad"
    assert (body["source"], body["stage"]) == ("usgs", "parse")
    assert "epoch milliseconds" in body["error"]
    assert json.loads(body["raw"])["id"] == "xx0testbad"


def test_quake_revision_is_published_as_new_version(kafka, usgs_features):
    producer, bootstrap, registry = kafka
    _, (_, usgs_source) = build_sources(registry)
    seen = SeenCache()
    publish_batch(producer, usgs_source, usgs_features[:1], seen, DLQ.name, now=NOW)

    usgs_features[0]["properties"].update(mag=4.8, updated=1778599999000, status="reviewed")
    stats = publish_batch(producer, usgs_source, usgs_features[:1], seen, DLQ.name, now=NOW)
    assert stats.published == 1

    messages = consume_all(bootstrap, USGS_QUAKES.name, expected=2)
    decode = AvroDeserializer(registry)
    mags = [decode(m.value(), SerializationContext(m.topic(), MessageField.VALUE))["magnitude"]
            for m in messages]  # fmt: skip
    assert mags == [4.6, 4.8]
    assert {m.key() for m in messages} == {b"ak0test001"}  # same key: revisions stay ordered


def test_seen_cache_forgets_oldest_first():
    seen = SeenCache(max_size=2)
    for key in ("a", "b", "c"):
        seen.add(key)
    assert "a" not in seen and "b" in seen and "c" in seen


def test_cycle_survives_one_feed_failing(kafka, usgs_features):
    from src.producer import run_cycle

    producer, _, registry = kafka

    class Response:
        status_code, headers = 200, {"ETag": '"e1"'}

        def raise_for_status(self):
            pass

        def json(self):
            return {"features": usgs_features}

    class Session:
        def get(self, url, headers, timeout):
            if "weather.gov" in url:
                raise ConnectionError("api.weather.gov is down")
            return Response()

    nws_result, usgs_result = run_cycle(producer, Session(), build_sources(registry), SeenCache())
    assert nws_result == {"source": "nws", "error": "api.weather.gov is down"}
    assert (usgs_result["published"], usgs_result["dead_lettered"]) == (2, 1)
