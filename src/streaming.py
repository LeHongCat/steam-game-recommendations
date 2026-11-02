"""Publish live API review changes from an outbox; durably consume versioned events."""
import argparse
import base64
import json
import os
import time
import uuid
from datetime import datetime

import pyarrow as pa
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src import common as c
from src.pipeline import parquet, revision
from src.schemas import REVIEW_SCHEMA, STREAM_SCHEMA

TOPIC = "steam.reviews.upserts.v2"
GROUP = "steam-api-bronze-v2"


def validate(event):
    expected = {"schema_version", "event_id", "event_type", "review_id", "item_id", "revision", "source_run_id", "source_mode", "emitted_at", "review"}
    if not isinstance(event, dict) or set(event) != expected:
        raise ValueError("Event fields do not match contract v2")
    if type(event["schema_version"]) is not int or event["schema_version"] != 2 or event["source_mode"] != "api_poll" or event["event_type"] != "review_upsert":
        raise ValueError("Unknown event schema or source mode")
    row = event["review"]
    if not isinstance(row, dict) or set(row) != set(REVIEW_SCHEMA.names) or row.get("source") != "steam_api":
        raise ValueError("Invalid review payload fields")
    if type(row.get("recommended")) is not bool or type(row.get("absa_eligible")) is not bool:
        raise ValueError("Review flags must be boolean")
    if not isinstance(row.get("review_id"), str) or not row["review_id"].isdigit() or row["review_id"] != event["review_id"] or row["item_id"] != event["item_id"]:
        raise ValueError("Review identity does not match event")
    if not isinstance(row.get("user_id"), str) or not row["user_id"].startswith("steam:"):
        raise ValueError("Invalid Steam user namespace")
    for field in ("review_text", "language", "item_id", "source_page"):
        if not isinstance(row[field], str):
            raise ValueError(f"{field} must be a string")
    for field in ("playtime_at_review_minutes", "playtime_forever_minutes", "votes_up", "votes_funny"):
        if row[field] is not None and (type(row[field]) is not int or row[field] < 0):
            raise ValueError(f"{field} must be null or a nonnegative integer")
    for value in (row["created_at"], row["updated_at"], row["observed_at"], event["emitted_at"]):
        if not isinstance(value, str) or datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("Timestamps must include a timezone")
    if datetime.fromisoformat(row["updated_at"]) < datetime.fromisoformat(row["created_at"]):
        raise ValueError("Review updated before creation")
    typed_row = dict(row)
    for field in ("created_at", "updated_at", "observed_at"):
        typed_row[field] = datetime.fromisoformat(row[field])
    pa.Table.from_pylist([typed_row], schema=REVIEW_SCHEMA)
    if revision(row) != event["revision"]:
        raise ValueError("Review revision checksum mismatch")
    expected_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "steam-review:" + row["review_id"] + ":" + event["revision"]))
    if event["event_id"] != expected_id:
        raise ValueError("Event ID does not match review version")
    return event


def initialize(bootstrap):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="steam-api-demo-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                     topic_configs={"retention.ms": "86400000"})])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def publish(bootstrap, producer=None):
    with c.database() as db:
        pending = db.execute("SELECT event_id,payload_json FROM outbox WHERE sent_at IS NULL ORDER BY sequence").fetchall()
    if not pending:
        result = {"published": 0, "pending": 0, "topic": TOPIC, "source_mode": "api_poll"}
        c.atomic_json(c.ROOT / "reports/runtime/api-publisher.json", result)
        return result
    owned = producer is None
    if owned:
        initialize(bootstrap)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                                 value_serializer=lambda value: c.dumps(value).encode("utf-8"))
    published = 0
    try:
        with c.database() as db:
            for event_id, raw in pending:
                event = validate(json.loads(raw))
                producer.send(TOPIC, key=event["review_id"].encode(), value=event).get(timeout=30)
                # A crash after ACK can duplicate the same event ID; commit each delivery separately.
                db.execute("UPDATE outbox SET sent_at=? WHERE event_id=?", (c.now().isoformat(), event_id))
                db.commit()
                published += 1
        producer.flush()
    finally:
        if owned:
            producer.close()
    with c.database() as db:
        remaining = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
    result = {"published": published, "pending": remaining, "topic": TOPIC, "source_mode": "api_poll"}
    c.atomic_json(c.ROOT / "reports/runtime/api-publisher.json", result)
    return result


def export_events():
    events, receipts = {}, 0
    for path in sorted((c.DATA / "bronze/kafka" / TOPIC).glob("*/*.json")):
        event = validate(c.read_json(path)["payload"])
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
        row.update(event_id=event["event_id"], revision=event["revision"], source_run_id=event["source_run_id"], source_mode="api_poll")
        rows.append(row)
        previous = latest.get(row["review_id"])
        if previous is None or (row["updated_at"], row["observed_at"]) > (previous["updated_at"], previous["observed_at"]):
            latest[row["review_id"]] = row
    parquet(c.DATA / "silver/stream/review_versions.parquet", rows, STREAM_SCHEMA)
    parquet(c.DATA / "silver/stream/reviews_latest.parquet", sorted(latest.values(), key=lambda row: row["review_id"]), STREAM_SCHEMA)
    result = {"bronze_receipts": receipts, "unique_review_versions": len(events),
              "latest_review_ids": len(latest), "duplicate_deliveries_collapsed": receipts - len(events), "source_mode": "api_poll"}
    c.atomic_json(c.ROOT / "reports/runtime/api-stream-export.json", result)
    return result


def consume(bootstrap, count=1000, timeout_seconds=45, continuous=False):
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=bootstrap, group_id=GROUP,
                             auto_offset_reset="earliest", enable_auto_commit=False, max_poll_records=500)
    received = accepted = rejected = 0
    started = time.monotonic()
    try:
        while continuous or (received < count and time.monotonic() - started < timeout_seconds):
            batch = consumer.poll(timeout_ms=1000, max_records=500 if continuous else min(500, count - received))
            for messages in batch.values():
                for message in messages:
                    coordinates = {"topic": message.topic, "partition": message.partition, "offset": message.offset}
                    try:
                        event = validate(json.loads(message.value))
                    except (ValueError, TypeError, UnicodeDecodeError, KeyError) as error:
                        path = c.DATA / "quarantine/kafka" / message.topic / str(message.partition) / f"{message.offset}.json"
                        c.atomic_json(path, {**coordinates, "raw_base64": base64.b64encode(message.value or b"").decode(), "error": str(error)})
                        rejected += 1
                    else:
                        path = c.DATA / "bronze/kafka" / message.topic / str(message.partition) / f"{message.offset}.json"
                        if path.exists():
                            if c.read_json(path)["payload"] != event:
                                raise ValueError("Bronze record differs for the same Kafka offset")
                        else:
                            c.atomic_json(path, {**coordinates, "ingested_at": c.now(), "payload": event})
                        accepted += 1
                    received += 1
            if batch:
                consumer.commit()
                export_events()
                if continuous:
                    print(c.dumps({"received": received, "accepted": accepted, "quarantined": rejected}), flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    result = {"received": received, "accepted": accepted, "quarantined": rejected,
              "duration_seconds": round(time.monotonic() - started, 2)}
    c.atomic_json(c.ROOT / "reports/runtime/api-consumer.json", result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["init", "publish", "consume", "export"])
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
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
    print(c.dumps(result))
    if args.action == "consume" and not args.continuous and result["received"] < args.count:
        parser.exit(2, "No more new messages before the requested count/timeout.\n")


if __name__ == "__main__":
    main()
