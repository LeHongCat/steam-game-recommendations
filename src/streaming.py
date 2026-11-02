"""Publish live API review changes from an outbox; durably consume versioned events."""

import argparse
import base64
import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src import storage
from src.events import validate_event
from src.schemas import STREAM_SCHEMA

TOPIC = "steam.reviews.upserts.v2"
GROUP = "steam-api-bronze-v2"


def initialize(bootstrap):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="steam-api-demo-admin")
    try:
        admin.create_topics(
            [
                NewTopic(
                    TOPIC,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"retention.ms": "86400000"},
                )
            ]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def publish(bootstrap, producer=None):
    with storage.database() as db:
        pending = db.execute(
            "SELECT event_id,payload_json FROM outbox WHERE sent_at IS NULL ORDER BY sequence"
        ).fetchall()
    if not pending:
        result = {"published": 0, "pending": 0, "topic": TOPIC, "source_mode": "api_poll"}
        storage.atomic_json(storage.ROOT / "reports/runtime/api-publisher.json", result)
        return result
    owned = producer is None
    if owned:
        initialize(bootstrap)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            retries=3,
            value_serializer=lambda value: storage.dumps(value).encode("utf-8"),
        )
    published = 0
    try:
        with storage.database() as db:
            for event_id, raw in pending:
                event = validate_event(json.loads(raw))
                producer.send(TOPIC, key=event["review_id"].encode(), value=event).get(timeout=30)
                # A crash after ACK can duplicate the same event ID; commit each delivery separately.
                db.execute(
                    "UPDATE outbox SET sent_at=? WHERE event_id=?",
                    (storage.now().isoformat(), event_id),
                )
                db.commit()
                published += 1
        producer.flush()
    finally:
        if owned:
            producer.close()
    with storage.database() as db:
        remaining = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
    result = {
        "published": published,
        "pending": remaining,
        "topic": TOPIC,
        "source_mode": "api_poll",
    }
    storage.atomic_json(storage.ROOT / "reports/runtime/api-publisher.json", result)
    return result


def export_events():
    events, receipts = {}, 0
    for path in sorted((storage.DATA / "bronze/kafka" / TOPIC).glob("*/*.json")):
        event = validate_event(storage.read_json(path)["payload"])
        receipts += 1
        previous = events.get(event["event_id"])
        if previous and previous != event:
            raise ValueError("Conflicting payload for an existing event ID")
        events[event["event_id"]] = event
    rows, latest = [], {}
    for event in sorted(events.values(), key=lambda value: value["event_id"]):
        row = dict(event["review"])
        for field in ("created_at", "updated_at", "observed_at"):
            row[field] = datetime.fromisoformat(row[field])
        row.update(
            event_id=event["event_id"],
            revision=event["revision"],
            source_run_id=event["source_run_id"],
            source_mode="api_poll",
        )
        rows.append(row)
        previous = latest.get(row["review_id"])
        if previous is None or (row["updated_at"], row["observed_at"]) > (
            previous["updated_at"],
            previous["observed_at"],
        ):
            latest[row["review_id"]] = row
    storage.write_parquet(
        storage.DATA / "silver/stream/review_versions.parquet", rows, STREAM_SCHEMA
    )
    storage.write_parquet(
        storage.DATA / "silver/stream/reviews_latest.parquet",
        sorted(latest.values(), key=lambda row: row["review_id"]),
        STREAM_SCHEMA,
    )
    result = {
        "bronze_receipts": receipts,
        "unique_review_versions": len(events),
        "latest_review_ids": len(latest),
        "duplicate_deliveries_collapsed": receipts - len(events),
        "source_mode": "api_poll",
    }
    storage.atomic_json(storage.ROOT / "reports/runtime/api-stream-export.json", result)
    return result


def consume(bootstrap, count=1000, timeout_seconds=45, continuous=False):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        group_id=GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=500,
    )
    received = accepted = rejected = 0
    started = time.monotonic()
    try:
        while continuous or (received < count and time.monotonic() - started < timeout_seconds):
            batch = consumer.poll(
                timeout_ms=1000, max_records=500 if continuous else min(500, count - received)
            )
            for messages in batch.values():
                for message in messages:
                    coordinates = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                    }
                    try:
                        event = validate_event(json.loads(message.value))
                    except (ValueError, TypeError, UnicodeDecodeError, KeyError) as error:
                        path = (
                            storage.DATA
                            / "quarantine/kafka"
                            / message.topic
                            / str(message.partition)
                            / f"{message.offset}.json"
                        )
                        storage.atomic_json(
                            path,
                            {
                                **coordinates,
                                "raw_base64": base64.b64encode(message.value or b"").decode(),
                                "error": str(error),
                            },
                        )
                        rejected += 1
                    else:
                        path = (
                            storage.DATA
                            / "bronze/kafka"
                            / message.topic
                            / str(message.partition)
                            / f"{message.offset}.json"
                        )
                        if path.exists():
                            if storage.read_json(path)["payload"] != event:
                                raise ValueError("Bronze record differs for the same Kafka offset")
                        else:
                            storage.atomic_json(
                                path,
                                {**coordinates, "ingested_at": storage.now(), "payload": event},
                            )
                        accepted += 1
                    received += 1
            if batch:
                consumer.commit()
                export_events()
                if continuous:
                    print(
                        storage.dumps(
                            {"received": received, "accepted": accepted, "quarantined": rejected}
                        ),
                        flush=True,
                    )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    result = {
        "received": received,
        "accepted": accepted,
        "quarantined": rejected,
        "duration_seconds": round(time.monotonic() - started, 2),
    }
    storage.atomic_json(storage.ROOT / "reports/runtime/api-consumer.json", result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["init", "publish", "consume", "export"])
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    )
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    if args.count <= 0 or args.timeout_seconds <= 0:
        parser.error("count and timeout must be positive")
    if args.action == "init":
        initialize(args.bootstrap)
        result = {"topic": TOPIC}
    elif args.action == "publish":
        result = publish(args.bootstrap)
    elif args.action == "consume":
        result = consume(args.bootstrap, args.count, args.timeout_seconds, args.continuous)
    else:
        result = export_events()
    print(storage.dumps(result))
    if args.action == "consume" and not args.continuous and result["received"] < args.count:
        parser.exit(2, "No more new messages before the requested count/timeout.\n")


if __name__ == "__main__":
    main()
