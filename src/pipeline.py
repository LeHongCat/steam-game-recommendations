"""Steam API Bronze → cleaned Silver → descriptive Gold and durable Kafka outbox."""

import argparse
import json
from datetime import datetime

import pyarrow.parquet as pq

from src import state, storage
from src.events import revision
from src.normalization import normalize_game, normalize_review
from src.schemas import GAME_SCHEMA, INTERACTION_SCHEMA, REVIEW_SCHEMA, STATS_SCHEMA


def clean(run_id):
    info = storage.read_manifest(run_id)
    if not info["collection_complete"]:
        raise ValueError("Cannot clean an incomplete API crawl")
    games, reviews = {}, {}
    if info["base_run_id"]:
        base = storage.DATA / "silver" / info["base_run_id"]
        games = {row["item_id"]: row for row in pq.read_table(base / "games.parquet").to_pylist()}
        reviews = {
            row["review_id"]: row for row in pq.read_table(base / "reviews.parquet").to_pylist()
        }
    previous = {key: revision(row) for key, row in reviews.items()}
    rejected, polled, repeated = [], {}, 0
    catalog = json.loads((storage.DATA / info["catalog"]["path"]).read_bytes())["response"][
        "store_items"
    ]
    for index, raw in enumerate(catalog):
        try:
            row = normalize_game(
                raw, info["config"], datetime.fromisoformat(info["catalog"]["fetched_at"])
            )
            games[row["item_id"]] = row
        except (ValueError, TypeError, KeyError) as error:
            rejected.append({"entity": "game", "row": index, "reason": str(error)})
    for app, progress in info["apps"].items():
        for page in progress["pages"]:
            raw_rows = json.loads((storage.DATA / page["path"]).read_bytes())["response"]["reviews"]
            for index, raw in enumerate(raw_rows):
                try:
                    if app not in games:
                        raise ValueError("item_missing_from_catalog")
                    row = normalize_review(
                        raw,
                        app,
                        page["path"],
                        datetime.fromisoformat(page["fetched_at"]),
                        info["config"]["language"],
                    )
                    key = row["review_id"]
                    repeated += key in polled
                    if key not in polled or row["updated_at"] >= polled[key]["updated_at"]:
                        polled[key] = row
                except (ValueError, TypeError, KeyError, OverflowError, OSError) as error:
                    rejected.append(
                        {
                            "entity": "review",
                            "page": page["path"],
                            "row": index,
                            "reason": str(error),
                        }
                    )
    for key, row in polled.items():
        if key not in reviews or row["updated_at"] >= reviews[key]["updated_at"]:
            reviews[key] = row
    review_rows = sorted(reviews.values(), key=lambda row: row["review_id"])
    interactions = [
        {
            "event_id": "review:" + row["review_id"],
            "source_review_id": row["review_id"],
            "user_id": row["user_id"],
            "item_id": row["item_id"],
            "event_type": "review_positive" if row["recommended"] else "review_negative",
            "event_time": row["created_at"],
            "source": "steam_api_review",
        }
        for row in review_rows
    ]
    destination = storage.DATA / "silver" / run_id
    storage.write_parquet(
        destination / "games.parquet",
        sorted(games.values(), key=lambda row: row["item_id"]),
        GAME_SCHEMA,
    )
    storage.write_parquet(destination / "reviews.parquet", review_rows, REVIEW_SCHEMA)
    storage.write_parquet(destination / "interactions.parquet", interactions, INTERACTION_SCHEMA)
    storage.atomic_json(storage.DATA / "quarantine" / f"{run_id}.json", rejected)
    counts = {
        "catalog_rows_observed": len(catalog),
        "games_in_snapshot": len(games),
        "review_rows_observed": sum(app["rows"] for app in info["apps"].values()),
        "unique_reviews_in_poll": len(polled),
        "duplicate_review_rows": repeated,
        "rejected_reviews": sum(row["entity"] == "review" for row in rejected),
        "rejected_games": sum(row["entity"] == "game" for row in rejected),
        "reviews_in_snapshot": len(reviews),
        "new_review_ids": sum(key not in previous for key in reviews),
        "changed_review_versions": sum(
            key in previous and revision(row) != previous[key] for key, row in reviews.items()
        ),
        "empty_review_text": sum(not row["absa_eligible"] for row in reviews.values()),
    }
    info.update(status="silver_ready", counts=counts)
    storage.save_manifest(info)
    return run_id


def aggregate(run_id):
    groups = {}
    for row in pq.read_table(storage.DATA / "silver" / run_id / "reviews.parquet").to_pylist():
        value = groups.setdefault(row["item_id"], [0, 0])
        value[0] += 1
        value[1] += int(row["recommended"])
    rows = [
        {
            "item_id": key,
            "review_count": total,
            "positive_count": positive,
            "negative_count": total - positive,
            "positive_ratio": positive / total,
            "run_id": run_id,
        }
        for key, (total, positive) in sorted(groups.items())
    ]
    storage.write_parquet(
        storage.DATA / "gold" / run_id / "game_statistics.parquet", rows, STATS_SCHEMA
    )
    return run_id


def verify(run_id):
    info = storage.read_manifest(run_id)
    pages = [info["catalog"]] + [page for app in info["apps"].values() for page in app["pages"]]
    integrity = all(
        storage.digest((storage.DATA / page["path"]).read_bytes()) == page["sha256"]
        for page in pages
    )
    base = storage.DATA / "silver" / run_id
    games = pq.read_table(base / "games.parquet").to_pylist()
    reviews = pq.read_table(base / "reviews.parquet").to_pylist()
    interactions = pq.read_table(base / "interactions.parquet").to_pylist()
    stats = pq.read_table(storage.DATA / "gold" / run_id / "game_statistics.parquet").to_pylist()
    ids = {row["item_id"] for row in games}
    checks = {
        "bronze_checksums": integrity,
        "collection_complete": info["collection_complete"],
        "requested_games_available": set(map(str, info["config"]["app_ids"])).issubset(ids),
        "nonempty_reviews": bool(reviews),
        "unique_review_ids": len({r["review_id"] for r in reviews}) == len(reviews),
        "valid_item_references": all(row["item_id"] in ids for row in reviews + interactions),
        "review_reconciliation": info["counts"]["review_rows_observed"]
        == info["counts"]["unique_reviews_in_poll"]
        + info["counts"]["duplicate_review_rows"]
        + info["counts"]["rejected_reviews"],
        "one_feedback_per_review": len(interactions) == len(reviews),
        "valid_utc_timestamps": all(
            row["created_at"].tzinfo and row["updated_at"] >= row["created_at"] for row in reviews
        ),
        "gold_reconciliation": sum(row["review_count"] for row in stats) == len(reviews),
        "binary_feedback_preserved": all(
            row["positive_count"] + row["negative_count"] == row["review_count"] for row in stats
        ),
    }
    if not all(checks.values()):
        raise ValueError(f"Quality gate failed: {checks}")
    info = state.commit_snapshot(info, reviews, checks)
    storage.save_manifest(info)
    state.publish_latest()
    storage.atomic_json(storage.ROOT / "reports/runtime" / f"api-quality-{run_id}.json", info)
    return {
        "run_id": run_id,
        "counts": info["counts"],
        "checks": info["checks"],
        "new_events_enqueued": info["new_events_enqueued"],
    }


def main():
    from src.steam_api import crawl

    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["all", "crawl", "clean", "aggregate", "verify"])
    parser.add_argument("--config")
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.stage in ("all", "crawl"):
        run_id = crawl(args.config, args.run_id)
    elif args.run_id:
        run_id = args.run_id
    else:
        parser.error("--run-id is required for this stage")
    if args.stage in ("all", "clean"):
        clean(run_id)
    if args.stage in ("all", "aggregate"):
        aggregate(run_id)
    print(storage.dumps(verify(run_id)) if args.stage in ("all", "verify") else run_id)


if __name__ == "__main__":
    main()
