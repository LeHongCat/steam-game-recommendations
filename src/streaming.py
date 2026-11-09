"""Publish live API review changes from an outbox; durably consume versioned events."""

import argparse
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import batched, chain

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src import storage
from src.events import validate_event
from src.schemas import STREAM_SCHEMA

TOPIC = "steam.reviews.upserts.v2"
GROUP = "steam-api-bronze-v2"
PUBLISH_BATCH_SIZE = 500


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
    started = time.monotonic()
    with storage.database() as db:
        pending = db.execute(
            "SELECT event_id,payload_json FROM outbox WHERE sent_at IS NULL ORDER BY sequence"
        ).fetchall()
    if not pending:
        result = {
            "published": 0,
            "pending_before": 0,
            "pending": 0,
            "duration_seconds": round(time.monotonic() - started, 3),
            "events_per_second": 0.0,
            "topic": TOPIC,
            "source_mode": "api_poll",
        }
        storage.atomic_json(storage.ROOT / "reports/runtime/api-publisher.json", result)
        return result
    owned = producer is None
    if owned:
        initialize(bootstrap)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            retries=3,
            linger_ms=10,
            value_serializer=lambda value: storage.dumps(value).encode("utf-8"),
        )
    published = 0
    try:
        with storage.database() as db:
            for batch in batched(pending, PUBLISH_BATCH_SIZE):
                deliveries = []
                for _, raw in batch:
                    event = validate_event(json.loads(raw))
                    deliveries.append(
                        producer.send(TOPIC, key=event["review_id"].encode(), value=event)
                    )
                for delivery in deliveries:
                    delivery.get(timeout=30)
                # An interrupted batch can replay the same event IDs after an ACK.
                sent_at = storage.now().isoformat()
                db.executemany(
                    "UPDATE outbox SET sent_at=? WHERE event_id=? AND sent_at IS NULL",
                    ((sent_at, event_id) for event_id, _ in batch),
                )
                db.commit()
                published += len(batch)
        producer.flush()
    finally:
        if owned:
            producer.close()
    with storage.database() as db:
        remaining = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
    duration = time.monotonic() - started
    result = {
        "published": published,
        "pending_before": len(pending),
        "pending": remaining,
        "duration_seconds": round(duration, 3),
        "events_per_second": round(published / duration, 2),
        "topic": TOPIC,
        "source_mode": "api_poll",
    }
    storage.atomic_json(storage.ROOT / "reports/runtime/api-publisher.json", result)
    return result


def consumer_lag(bootstrap):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=GROUP, enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            raise RuntimeError(f"Kafka topic missing: {TOPIC}")
        keys = [TopicPartition(TOPIC, partition) for partition in sorted(partitions)]
        first = consumer.beginning_offsets(keys)
        end = consumer.end_offsets(keys)
        offsets = []
        for key in keys:
            committed = consumer.committed(key)
            offsets.append(
                {
                    "partition": key.partition,
                    "beginning_offset": first[key],
                    "end_offset": end[key],
                    "committed_offset": committed,
                    "lag": max(0, end[key] - max(first[key], committed or 0)),
                }
            )
        return {
            "topic": TOPIC,
            "group": GROUP,
            "lag": sum(row["lag"] for row in offsets),
            "partitions": offsets,
        }
    finally:
        consumer.close()


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


def store_receipt(message):
    coordinates = {"topic": message.topic, "partition": message.partition, "offset": message.offset}
    relative = f"{message.topic}/{message.partition}/{message.offset}.json"
    try:
        event = validate_event(json.loads(message.value))
    except (ValueError, TypeError, UnicodeDecodeError, KeyError) as error:
        storage.atomic_json(
            storage.DATA / "quarantine/kafka" / relative,
            {
                **coordinates,
                "raw_base64": base64.b64encode(message.value or b"").decode(),
                "error": str(error),
            },
        )
        return False

    path = storage.DATA / "bronze/kafka" / relative
    if path.exists():
        if storage.read_json(path)["payload"] != event:
            raise ValueError("Bronze record differs for the same Kafka offset")
    else:
        storage.atomic_json(path, {**coordinates, "ingested_at": storage.now(), "payload": event})
    return True


def consume(bootstrap, count=1000, timeout_seconds=45, continuous=False, *, export=True):
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
        with ThreadPoolExecutor(max_workers=8) as writers:
            while continuous or (received < count and time.monotonic() - started < timeout_seconds):
                batch = consumer.poll(
                    timeout_ms=1000, max_records=500 if continuous else min(500, count - received)
                )
                if not batch:
                    continue
                results = list(writers.map(store_receipt, chain.from_iterable(batch.values())))
                batch_accepted = sum(results)
                accepted += batch_accepted
                rejected += len(results) - batch_accepted
                received += len(results)
                consumer.commit()
                if continuous:
                    export_events()
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
    ingest_duration = time.monotonic() - started
    if not continuous and accepted and export:
        export_events()
    result = {
        "received": received,
        "accepted": accepted,
        "quarantined": rejected,
        "duration_seconds": round(time.monotonic() - started, 2),
        "ingest_duration_seconds": round(ingest_duration, 3),
        "events_per_second": round(received / ingest_duration, 2),
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
