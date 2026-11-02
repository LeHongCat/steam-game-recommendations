"""Local storage and state shared by API ingestion and Kafka delivery."""

import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[1]))
DATA = ROOT / "data/steam_api"


def now():
    return datetime.now(timezone.utc)


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_serialize_datetime)


def _serialize_datetime(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def digest(value):
    return hashlib.sha256(value).hexdigest()


def atomic_bytes(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("wb") as file:
            file.write(value)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json(path, value):
    rendered = json.dumps(
        value, ensure_ascii=False, sort_keys=True, indent=2, default=_serialize_datetime
    )
    atomic_bytes(path, (rendered + "\n").encode("utf-8"))


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def read_manifest(run_id):
    return read_json(DATA / "manifests" / f"{run_id}.json")


def save_manifest(info):
    atomic_json(DATA / "manifests" / f"{info['run_id']}.json", info)


@contextmanager
def database():
    DATA.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DATA / "state.sqlite", timeout=30)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY, revision TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
            run_id TEXT NOT NULL, payload_json TEXT NOT NULL, sent_at TEXT);
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, manifest_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_state (
            key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        with db:
            yield db
    finally:
        db.close()


def write_parquet(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="snappy")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
