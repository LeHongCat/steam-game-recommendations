"""Replay historical Steam review feedback through a real Kafka broker."""
import argparse
import base64
import json
import os
import time
import uuid
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.pipeline import ROOT, atomic_json, now, parquet

TOPIC = "steam.review-feedback.v1"
STREAM_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("review_id", pa.string()), ("user_id", pa.string()),
    ("item_id", pa.string()), ("recommended", pa.bool_()), ("event_time", pa.string()),
    ("produced_at", pa.string()), ("source_mode", pa.string()), ("schema_version", pa.int64()),
])


def validate(event):
    if not isinstance(event, dict) or type(event.get("schema_version")) is not int or event.get("schema_version") != 1 or event.get("source_mode") != "replay":
        raise ValueError("Unknown event schema or source mode")
    expected = {"schema_version", "event_id", "review_id", "user_id", "item_id", "recommended", "event_time", "produced_at", "source_mode"}
    if set(event) != expected:
        raise ValueError("Event fields do not match contract v1")
    for name in ("event_id", "review_id", "user_id", "item_id", "produced_at"):
        if not isinstance(event.get(name), str) or not event[name]:
            raise ValueError(f"Missing or invalid {name}")
    if type(event.get("recommended")) is not bool:
        raise ValueError("recommended must be boolean")
    if event.get("event_time") is not None and not isinstance(event["event_time"], str):
        raise ValueError("event_time must be null or an ISO timestamp")
    for field in ("produced_at", "event_time"):
        if event[field] is not None and datetime.fromisoformat(event[field]).tzinfo is None:
            raise ValueError(f"{field} must include a timezone")
    uuid.UUID(event["event_id"])
    return event


def initialize(bootstrap):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="steam-demo-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                     topic_configs={"retention.ms": "86400000"})])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    return {"topic": TOPIC, "partitions": 1, "replication_factor": 1}


def produce(bootstrap, count):
    latest = json.loads((ROOT / "data/latest.json").read_text())["snapshot_id"]
    rows = pq.read_table(ROOT / "data/silver" / latest / "reviews.parquet").to_pylist()[:count]
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                             value_serializer=lambda value: json.dumps(value).encode("utf-8"))
    try:
        for row in rows:
            event = validate({"schema_version": 1, "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, row["review_id"])),
                              "review_id": row["review_id"], "user_id": row["user_id"], "item_id": row["item_id"],
                              "recommended": row["recommended"], "event_time": row["event_time"],
                              "produced_at": now(), "source_mode": "replay"})
            producer.send(TOPIC, key=row["user_id"].encode(), value=event).get(timeout=30)
        producer.flush()
    finally:
        producer.close()
    return {"produced": len(rows), "topic": TOPIC, "source_mode": "replay"}


def consume(bootstrap, max_messages, timeout_seconds):
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=bootstrap, group_id="steam-bronze-v1",
                             auto_offset_reset="earliest", enable_auto_commit=False,
                             max_poll_records=min(max_messages, 500))
    received = accepted = rejected = 0
    started = time.monotonic()
    try:
        while received < max_messages and time.monotonic() - started < timeout_seconds:
            batch = consumer.poll(timeout_ms=1000, max_records=max_messages - received)
            for messages in batch.values():
                for message in messages:
                    coordinates = {"topic": message.topic, "partition": message.partition, "offset": message.offset}
                    try:
                        event = validate(json.loads(message.value))
                    except (ValueError, TypeError, UnicodeDecodeError) as error:
                        path = ROOT / "data/quarantine/events" / message.topic / str(message.partition) / f"{message.offset}.json"
                        atomic_json(path, {**coordinates, "raw_base64": base64.b64encode(message.value or b"").decode(),
                                           "is_tombstone": message.value is None, "error": str(error)})
                        rejected += 1
                    else:
                        path = ROOT / "data/bronze/events" / message.topic / str(message.partition) / f"{message.offset}.json"
                        if path.exists():
                            existing = json.loads(path.read_text(encoding="utf-8"))
                            if existing["payload"] != event:
                                raise ValueError("Existing Bronze record differs for the same Kafka offset")
                        else:
                            atomic_json(path, {**coordinates, "ingested_at": now(), "payload": event})
                        accepted += 1
                    received += 1
            # Acknowledge only after every polled record is durable in Bronze/quarantine.
            if batch:
                consumer.commit()
    finally:
        consumer.close()
    result = {"received": received, "accepted": accepted, "quarantined": rejected,
              "duration_seconds": round(time.monotonic() - started, 2)}
    atomic_json(ROOT / "reports/runtime/stream-consumer.json", result)
    return result


def export():
    unique = {}
    receipts = 0
    for path in sorted((ROOT / "data/bronze/events" / TOPIC).glob("*/*.json")):
        event = validate(json.loads(path.read_text(encoding="utf-8"))["payload"])
        receipts += 1
        previous = unique.get(event["event_id"])
        if previous and any(previous[key] != event[key] for key in ("review_id", "user_id", "item_id", "recommended", "event_time")):
            raise ValueError("Conflicting payloads for the same logical event_id")
        if previous is None or event["produced_at"] > previous["produced_at"]:
            unique[event["event_id"]] = event
    rows = sorted(unique.values(), key=lambda event: event["event_id"])
    parquet(ROOT / "data/silver/stream/review_feedback.parquet", rows, STREAM_SCHEMA)
    result = {"bronze_receipts": receipts, "silver_unique_events": len(rows),
              "duplicate_events_collapsed": receipts - len(rows), "source_mode": "replay"}
    atomic_json(ROOT / "reports/runtime/stream-export.json", result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["init", "produce", "produce-invalid", "consume", "export"])
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--timeout-seconds", type=int, default=45)
    args = parser.parse_args()
    if args.count <= 0 or args.timeout_seconds <= 0:
        parser.error("count and timeout must be positive")
    if args.action == "init":
        result = initialize(args.bootstrap)
    elif args.action == "produce":
        result = produce(args.bootstrap, args.count)
    elif args.action == "produce-invalid":
        producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all")
        try:
            producer.send(TOPIC, value=b'{"schema_version":1,"recommended":"invalid"}').get(timeout=30)
            result = {"malformed_test_event_produced": 1}
        finally:
            producer.close()
    elif args.action == "consume":
        result = consume(args.bootstrap, args.count, args.timeout_seconds)
    else:
        result = export()
    print(json.dumps(result, indent=2))
    if args.action == "consume" and result["received"] < args.count:
        parser.exit(2, "Consumer timed out before receiving the requested number of messages.\n")


if __name__ == "__main__":
    main()
