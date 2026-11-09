"""Poll the NWS and USGS feeds and publish every new alert or event version to Kafka.

python -m src.producer            # run forever (the Docker service)
python -m src.producer --once     # one polling cycle, then exit
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from src import nws, usgs
from src.feeds import ConditionalFeed, build_session
from src.pipeline import SeenCache, Source, publish_batch
from src.topics import DLQ, NWS_ALERTS, USGS_QUAKES, ensure_topics

log = logging.getLogger("producer")
SCHEMA_DIR = Path(__file__).parent / "schemas"


def kafka_config() -> dict:
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "client.id": "public-safety-producer",
        "enable.idempotence": True,  # broker-side retries never create duplicates
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "zstd",
    }


def avro_serializer(registry, schema_file: str):
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    serializer = AvroSerializer(
        registry,
        (SCHEMA_DIR / schema_file).read_text(),
        conf={"auto.register.schemas": True},
    )
    return lambda record, topic: serializer(record, SerializationContext(topic, MessageField.VALUE))


def build_sources(registry) -> list[tuple[ConditionalFeed, Source]]:
    return [
        (
            ConditionalFeed("nws", nws.ALERTS_URL),
            Source("nws", NWS_ALERTS.name, nws.parse_alert, lambda r: r["incident_key"],
                   nws.version_key, avro_serializer(registry, "nws_alert.avsc")),
        ),
        (
            ConditionalFeed("usgs", usgs.FEED_URL),
            Source("usgs", USGS_QUAKES.name, usgs.parse_quake, lambda r: r["event_id"],
                   usgs.version_key, avro_serializer(registry, "usgs_earthquake.avsc")),
        ),
    ]  # fmt: skip


def run_cycle(producer, session, sources, seen: SeenCache) -> list[dict]:
    results = []
    for feed, source in sources:
        started = time.monotonic()
        try:
            features = feed.fetch(session)
        except Exception as err:  # one feed being down must not stop the other
            log.warning("%s fetch failed: %s", feed.name, err)
            results.append({"source": feed.name, "error": str(err)[:200]})
            continue
        if features is None:
            results.append({"source": feed.name, "not_modified": True})
            continue
        stats = publish_batch(producer, source, features, seen, DLQ.name).as_dict()
        stats["seconds"] = round(time.monotonic() - started, 2)
        results.append(stats)
    return results


def main(argv: list[str]) -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    interval = int(os.environ.get("POLL_SECONDS", "60"))
    config = kafka_config()

    topics = ensure_topics(AdminClient({"bootstrap.servers": config["bootstrap.servers"]}))
    log.info("topics: %s", topics)
    registry = SchemaRegistryClient(
        {"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")}
    )
    producer = Producer(config)
    session = build_session(os.environ.get("CONTACT_USER_AGENT", ""))
    sources = build_sources(registry)
    seen = SeenCache()

    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        log.info("signal %s received, finishing current cycle", signum)
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while not stopping:
        cycle_start = time.monotonic()
        results = run_cycle(producer, session, sources, seen)
        print(json.dumps({"at": datetime.now(UTC).isoformat(timespec="seconds"),
                          "seen_cache": len(seen), "feeds": results}), flush=True)  # fmt: skip
        if "--once" in argv:
            break
        sleep_for = max(0, interval - (time.monotonic() - cycle_start))
        while sleep_for > 0 and not stopping:
            time.sleep(min(1, sleep_for))
            sleep_for -= 1
    producer.flush(30)
    log.info("producer stopped cleanly")


if __name__ == "__main__":
    main(sys.argv[1:])
