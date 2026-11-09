"""Persist one structured log record per pipeline step attempt."""

import re
import time
import uuid

from src import storage


class StepFailure(RuntimeError):
    def __init__(self, message, metrics):
        super().__init__(message)
        self.metrics = metrics


def run_directory(run_id):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", run_id):
        raise ValueError("Invalid run ID")
    return storage.ROOT / "reports/runtime/runs" / run_id


def step_records(run_id):
    records = [storage.read_json(path) for path in (run_directory(run_id) / "steps").glob("*.json")]
    return sorted(records, key=lambda record: record["started_at"])


def run_step(run_id, step, operation, *args, **kwargs):
    path = run_directory(run_id) / "steps" / f"{step}-{uuid.uuid4().hex}.json"
    started = time.monotonic()
    record = {"step": step, "started_at": storage.now(), "status": "running"}
    storage.atomic_json(path, record)
    try:
        result = operation(*args, **kwargs)
        record["status"] = "success"
        if isinstance(result, dict):
            record["metrics"] = result
        return result
    except (Exception, KeyboardInterrupt) as error:
        record.update(status="failed", error=f"{type(error).__name__}: {error}")
        if isinstance(error, StepFailure):
            record["metrics"] = error.metrics
        raise
    finally:
        record.update(ended_at=storage.now(), duration_seconds=round(time.monotonic() - started, 3))
        storage.atomic_json(path, record)
        print(storage.dumps({"run_id": run_id, **record}), flush=True)
