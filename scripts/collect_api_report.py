"""Collect aggregate verification evidence without publishing raw Steam user data."""
import json
import sys

import airflow
import kafka
import pyarrow
import pyarrow.parquet as pq

from src import common as c
from src.streaming import TOPIC


def main():
    manifests = sorted((c.read_json(path) for path in (c.DATA / "manifests").glob("*.json")), key=lambda value: value["started_at"])
    complete = [value for value in manifests if value["status"] == "completed"]
    first = complete[0]
    latest_id = c.read_json(c.DATA / "latest.json")["run_id"]
    latest = c.read_manifest(latest_id)
    base = c.DATA / "silver" / latest_id
    games = pq.read_table(base / "games.parquet").to_pylist()
    reviews = pq.read_table(base / "reviews.parquet").to_pylist()
    versions = pq.read_table(c.DATA / "silver/stream/review_versions.parquet").to_pylist()
    stream_latest = pq.read_table(c.DATA / "silver/stream/reviews_latest.parquet").to_pylist()
    with c.database() as db:
        pending = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
        sent_ids = {row[0] for row in db.execute("SELECT event_id FROM outbox WHERE sent_at IS NOT NULL")}
        sent = len(sent_ids)
        by_run = dict(db.execute("SELECT run_id, COUNT(*) FROM outbox GROUP BY run_id"))
    receipts = len(list((c.DATA / "bronze/kafka" / TOPIC).glob("*/*.json")))
    by_game = [{"item_id": game["item_id"], "title": game["title"],
                "reviews": sum(row["item_id"] == game["item_id"] for row in reviews),
                "positive": sum(row["item_id"] == game["item_id"] and row["recommended"] for row in reviews),
                "negative": sum(row["item_id"] == game["item_id"] and not row["recommended"] for row in reviews)}
               for game in games]
    checks = {"latest_quality_gate": all(latest["checks"].values()), "no_pending_outbox": pending == 0,
              "all_sent_versions_consumed": sent_ids == {row["event_id"] for row in versions},
              "stream_matches_batch_review_ids": {row["review_id"] for row in reviews} == {row["review_id"] for row in stream_latest},
              "api_source_only": all(row["source"] == "steam_api" for row in reviews + games),
              "api_events_only": all(row["source_mode"] == "api_poll" for row in versions)}
    report = {"recorded_at": c.now(), "source": "steam_api", "coverage": latest["coverage"],
              "first_run_id": first["run_id"], "latest_run_id": latest_id, "games": by_game,
              "reviews_in_latest_snapshot": len(reviews), "empty_review_text": sum(not row["absa_eligible"] for row in reviews),
              "quality_checks_passed": sum(latest["checks"].values()), "quality_checks_total": len(latest["checks"]),
              "polls": [{"run_id": value["run_id"], "reviews_observed": value["counts"]["review_rows_observed"],
                         "new_events_enqueued": value["new_events_enqueued"], "outbox_versions": by_run.get(value["run_id"], 0)} for value in complete],
              "kafka": {"topic": TOPIC, "sent_versions": sent, "pending_versions": pending, "bronze_receipts": receipts,
                        "unique_review_versions": len(versions), "latest_review_ids": len(stream_latest)}, "checks": checks}
    destination = c.ROOT / "reports/steam_api"
    c.atomic_json(destination / "RESULTS.json", report)
    c.atomic_json(destination / "FIRST_CRAWL.json", first)
    c.atomic_json(destination / "LATEST_CRAWL.json", latest)
    c.atomic_json(destination / "ENVIRONMENT.json", {"python": sys.version.split()[0], "airflow": airflow.__version__,
                  "pyarrow": pyarrow.__version__, "kafka_python": kafka.__version__})
    print(c.dumps(report))
    if not all(checks.values()):
        raise SystemExit("API/Kafka verification incomplete; inspect the aggregate checks")


if __name__ == "__main__":
    main()
