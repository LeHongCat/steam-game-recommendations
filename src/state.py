"""Commit accepted snapshots and their Kafka outbox in one SQLite transaction."""

import json
from typing import Any

from src import storage
from src.events import make_event, revision


class StaleSnapshotError(ValueError):
    """Another run committed after this crawl selected its base snapshot."""


def _latest_run_id(db) -> str | None:
    row = db.execute("SELECT value FROM pipeline_state WHERE key='latest_run_id'").fetchone()
    if row:
        return row[0]

    # Adopt a snapshot written before the SQLite latest pointer was introduced.
    marker = storage.DATA / "latest.json"
    if marker.exists():
        run_id = storage.read_json(marker)["run_id"]
        if db.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone():
            return run_id
    return None


def latest_run_id() -> str | None:
    with storage.database() as db:
        return _latest_run_id(db)


def commit_snapshot(info: dict, reviews: list[dict], checks: dict[str, bool]) -> dict[str, Any]:
    run_id = info["run_id"]
    with storage.database() as db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute("SELECT manifest_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if existing:
            return json.loads(existing[0])

        current = _latest_run_id(db)
        if info["base_run_id"] != current:
            raise StaleSnapshotError(
                f"Snapshot base changed from {info['base_run_id']!r} to {current!r}; "
                "start a new crawl instead of overwriting the latest snapshot."
            )

        enqueued = 0
        for review in reviews:
            version = revision(review)
            old = db.execute(
                "SELECT revision, updated_at FROM reviews WHERE review_id=?", (review["review_id"],)
            ).fetchone()
            if old and (old[0] == version or old[1] > review["updated_at"].isoformat()):
                continue

            event = make_event(review, run_id)
            cursor = db.execute(
                "INSERT OR IGNORE INTO outbox(event_id,run_id,payload_json) VALUES(?,?,?)",
                (event["event_id"], run_id, storage.dumps(event)),
            )
            enqueued += cursor.rowcount
            db.execute(
                "INSERT OR REPLACE INTO reviews VALUES(?,?,?)",
                (review["review_id"], version, review["updated_at"].isoformat()),
            )

        completed = {
            **info,
            "status": "completed",
            "checks": checks,
            "new_events_enqueued": enqueued,
            "verified_at": storage.now().isoformat(),
        }
        db.execute("INSERT INTO runs VALUES(?,?)", (run_id, storage.dumps(completed)))
        db.execute(
            "INSERT OR REPLACE INTO pipeline_state(key,value) VALUES('latest_run_id',?)", (run_id,)
        )
        return completed


def publish_latest() -> None:
    """Refresh the file marker from committed state without racing another writer."""
    with storage.database() as db:
        db.execute("BEGIN IMMEDIATE")
        run_id = _latest_run_id(db)
        if run_id is not None:
            storage.atomic_json(storage.DATA / "latest.json", {"run_id": run_id})
