"""Stable review revisions and validation of the Kafka event contract."""

import re
import uuid
from datetime import datetime

import pyarrow as pa

from src import storage
from src.schemas import REVIEW_SCHEMA


def make_event(review: dict, run_id: str) -> dict:
    version = revision(review)
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"steam-review:{review['review_id']}:{version}"))
    return {
        "schema_version": 2,
        "event_id": event_id,
        "event_type": "review_upsert",
        "review_id": review["review_id"],
        "item_id": review["item_id"],
        "revision": version,
        "source_run_id": run_id,
        "source_mode": "api_poll",
        "emitted_at": storage.now(),
        "review": review,
    }


def revision(row):
    fields = (
        "review_id",
        "user_id",
        "item_id",
        "review_text",
        "recommended",
        "language",
        "created_at",
        "updated_at",
        "playtime_at_review_minutes",
    )
    return storage.digest(storage.dumps({key: row[key] for key in fields}).encode("utf-8"))


def validate_event(event):
    expected = {
        "schema_version",
        "event_id",
        "event_type",
        "review_id",
        "item_id",
        "revision",
        "source_run_id",
        "source_mode",
        "emitted_at",
        "review",
    }
    if not isinstance(event, dict) or set(event) != expected:
        raise ValueError("Event fields do not match contract v2")
    if (
        type(event["schema_version"]) is not int
        or event["schema_version"] != 2
        or event["source_mode"] != "api_poll"
        or event["event_type"] != "review_upsert"
    ):
        raise ValueError("Unknown event schema or source mode")
    row = event["review"]
    if (
        not isinstance(row, dict)
        or set(row) != set(REVIEW_SCHEMA.names)
        or row.get("source") != "steam_api"
    ):
        raise ValueError("Invalid review payload fields")
    if type(row.get("recommended")) is not bool or type(row.get("absa_eligible")) is not bool:
        raise ValueError("Review flags must be boolean")
    if (
        not isinstance(row.get("review_id"), str)
        or not row["review_id"].isdigit()
        or row["review_id"] != event["review_id"]
        or row["item_id"] != event["item_id"]
    ):
        raise ValueError("Review identity does not match event")
    if not isinstance(row.get("user_id"), str) or not re.fullmatch(
        r"steam:[a-f0-9]{64}", row["user_id"]
    ):
        raise ValueError("Invalid Steam user namespace")
    if not isinstance(event["source_run_id"], str) or not event["source_run_id"]:
        raise ValueError("source_run_id must be a nonempty string")
    for field in ("review_text", "language", "item_id", "source_page"):
        if not isinstance(row[field], str):
            raise ValueError(f"{field} must be a string")
    for field in (
        "playtime_at_review_minutes",
        "playtime_forever_minutes",
        "votes_up",
        "votes_funny",
    ):
        if row[field] is not None and (type(row[field]) is not int or row[field] < 0):
            raise ValueError(f"{field} must be null or a nonnegative integer")
    for field in ("steam_purchase", "received_for_free", "refunded"):
        if row[field] is not None and type(row[field]) is not bool:
            raise ValueError(f"{field} must be null or boolean")
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
    expected_id = str(
        uuid.uuid5(uuid.NAMESPACE_URL, "steam-review:" + row["review_id"] + ":" + event["revision"])
    )
    if event["event_id"] != expected_id:
        raise ValueError("Event ID does not match review version")
    return event
