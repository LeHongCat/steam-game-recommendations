"""Collect a per-game review dataset and verify delivery through the local Kafka pipeline."""

import argparse
import os
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src import game_selection, pipeline, reporting, state, steam_api, storage, streaming, telemetry
from src.schemas import GAME_SCHEMA, REVIEW_SCHEMA


def collect(config, run_id):
    steam_api.crawl(config, run_id)
    manifest = storage.read_manifest(run_id)
    pages = [manifest["catalog"]] + [
        page for app in manifest["apps"].values() for page in app["pages"]
    ]
    return {
        "rows_observed": sum(app["rows"] for app in manifest["apps"].values()),
        "pages_saved": len(pages),
        "retries_for_saved_requests": sum(page["attempts"] - 1 for page in pages),
        "elapsed_since_first_attempt_seconds": round(
            (
                datetime.fromisoformat(manifest["collected_at"])
                - datetime.fromisoformat(manifest["started_at"])
            ).total_seconds(),
            2,
        ),
    }


def export_dataset(run_id, per_game, destination):
    manifest = storage.read_manifest(run_id)
    if manifest["status"] != "completed" or not all(manifest["checks"].values()):
        raise ValueError("The batch snapshot has not passed verification")

    silver = storage.DATA / "silver" / run_id
    reviews = pq.read_table(silver / "reviews.parquet")
    app_ids = list(map(str, manifest["config"]["app_ids"]))
    games = pq.read_table(silver / "games.parquet")
    games = games.filter(pc.is_in(games["item_id"], value_set=pa.array(app_ids)))
    titles = dict(zip(games["item_id"].to_pylist(), games["title"].to_pylist(), strict=True))
    language = manifest["config"]["language"]
    reviews = reviews.filter(pc.starts_with(reviews["source_page"], f"bronze/api/{run_id}/"))
    if language != "all":
        reviews = reviews.filter(pc.equal(reviews["language"], language))

    output, stats = [], []
    for app_id in app_ids:
        app = reviews.filter(pc.equal(reviews["item_id"], app_id))
        if len(app) < per_game:
            raise ValueError(
                f"{titles[app_id]} has {len(app):,} eligible reviews; need {per_game:,}"
            )
        rows = app.sort_by([("updated_at", "descending"), ("review_id", "ascending")])
        rows = rows.slice(0, per_game).to_pylist()
        positive = sum(row["recommended"] for row in rows)
        output.extend(rows)
        stats.append(
            {
                "app_id": int(app_id),
                "title": titles[app_id],
                "observed": manifest["apps"][app_id]["rows"],
                "cleaned_from_run": len(app),
                "exported": len(rows),
                "positive": positive,
                "negative": len(rows) - positive,
                "empty_review_text": sum(not row["absa_eligible"] for row in rows),
            }
        )

    parquet = destination / "reviews.parquet"
    storage.write_parquet(parquet, output, REVIEW_SCHEMA)
    keys = pq.read_table(parquet, columns=["review_id", "item_id", "source", "language"])
    expected_count = per_game * len(stats)
    if (
        len(keys) != expected_count
        or pc.count_distinct(keys["review_id"]).as_py() != expected_count
        or pc.unique(keys["source"]).to_pylist() != ["steam_api"]
        or (language != "all" and pc.unique(keys["language"]).to_pylist() != [language])
        or {row["values"]: row["counts"] for row in pc.value_counts(keys["item_id"]).to_pylist()}
        != {str(row["app_id"]): per_game for row in stats}
    ):
        raise ValueError("Parquet export does not match expected counts, keys, or source")
    storage.write_parquet(destination / "games.parquet", games.to_pylist(), GAME_SCHEMA)

    report = {
        "source": "steam_api",
        "run_id": run_id,
        "collected_at": manifest["collected_at"],
        "language": language,
        "selection": "Most recently updated reviews among valid, unique rows from this crawl",
        "games": stats,
        "per_game": per_game,
        "total_exported": expected_count,
        "batch_counts": manifest["counts"],
        "batch_checks": manifest["checks"],
        "parquet": parquet.name,
        "catalog_parquet": "games.parquet",
        "parquet_sha256": storage.digest(parquet.read_bytes()),
    }
    storage.atomic_json(destination / "summary.json", report)
    return report


def consume_backfill(bootstrap, lag):
    if lag == 0:
        return {"received": 0, "accepted": 0, "quarantined": 0, "events_per_second": 0}
    result = streaming.consume(bootstrap, count=lag, timeout_seconds=1800, export=False)
    if result["received"] != lag or result["quarantined"]:
        raise telemetry.StepFailure("Kafka consumption incomplete or rejected events", result)
    return result


def verify_delivery(run_id, destination, lag_before, lag_after):
    report, _, latest = reporting.collect_report()
    report.update(lag_before_consume=lag_before, lag_after_consume=lag_after)
    attempts = [
        {"status": record["status"], **record["metrics"]}
        for record in telemetry.step_records(run_id)
        if record["step"] == "consume" and "metrics" in record
    ]
    elapsed = sum(attempt.get("ingest_duration_seconds", 0) for attempt in attempts)
    committed = lag_before["lag"] - lag_after["lag"]
    report["consume_attempts"] = attempts
    report["kafka"].update(
        offsets_processed=committed,
        consume_seconds=round(elapsed, 3),
        consume_events_per_second=round(committed / elapsed, 2) if elapsed else 0,
    )
    storage.atomic_json(destination / "kafka.json", report)
    if latest["run_id"] != run_id or lag_after["lag"] != 0 or not all(report["checks"].values()):
        raise RuntimeError(
            f"Kafka reconciliation failed: {report['checks']}; lag={lag_after['lag']}"
        )
    return {"kafka": report["kafka"], "checks": report["checks"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--per-game", type=int, default=20000)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--config")
    selection.add_argument("--games", type=int, help="Select this many games from Steam Charts")
    args = parser.parse_args()
    if args.per_game < 1:
        parser.error("--per-game must be positive")
    if args.games is not None and not 1 <= args.games <= 50:
        parser.error("--games must be between 1 and 50")

    run_id = args.run_id
    directory = telemetry.run_directory(run_id)
    step = telemetry.run_step
    config = args.config
    target = directory / "target.json"
    if target.exists() and storage.read_json(target)["per_game"] != args.per_game:
        parser.error("A resumed run must keep the same per-game target")
    storage.atomic_json(target, {"per_game": args.per_game})
    if config is None:
        config = directory / "crawl-config.json"
        settings = steam_api.load_config(config if config.exists() else None)
        if config.exists():
            if args.games and len(settings["app_ids"]) != args.games:
                parser.error("A resumed run must keep the same game count")
        else:
            settings["max_reviews_per_game"] = args.per_game + min(200, max(1, args.per_game // 10))
            settings["max_attempts"] = max(settings["max_attempts"], 6)
            if args.games:
                step(
                    run_id,
                    "select_games",
                    game_selection.select,
                    run_id,
                    settings,
                    args.games,
                    config,
                )
            else:
                storage.atomic_json(config, settings)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    destination = storage.DATA / "exports" / run_id

    saved = {
        record["step"]: record.get("metrics", {})
        for record in telemetry.step_records(run_id)
        if record["status"] == "success"
    }
    if "crawl" not in saved:
        step(run_id, "crawl", collect, config, run_id)
    if storage.read_manifest(run_id)["status"] != "completed":
        step(run_id, "clean", pipeline.clean, run_id)
        step(run_id, "aggregate", pipeline.aggregate, run_id)
        step(run_id, "verify_batch", pipeline.verify, run_id)
    if state.latest_run_id() != run_id:
        raise RuntimeError(
            "The backfill must be the latest committed batch before Kafka publishing"
        )

    if "export_dataset" not in saved or not all(
        (destination / filename).is_file()
        for filename in ("games.parquet", "reviews.parquet", "summary.json")
    ):
        step(run_id, "export_dataset", export_dataset, run_id, args.per_game, destination)
    if saved.get("publish", {}).get("pending") != 0:
        step(run_id, "publish", streaming.publish, bootstrap)
    if "lag_before" in saved:
        before = saved["lag_before"]
        remaining = step(run_id, "lag_resume", streaming.consumer_lag, bootstrap)
    else:
        before = remaining = step(run_id, "lag_before", streaming.consumer_lag, bootstrap)
    if remaining["lag"] or "consume" not in saved:
        step(run_id, "consume", consume_backfill, bootstrap, remaining["lag"])
    after = step(run_id, "lag_after", streaming.consumer_lag, bootstrap)
    step(run_id, "export_stream", streaming.export_events)
    step(run_id, "verify_stream", verify_delivery, run_id, destination, before, after)


if __name__ == "__main__":
    main()
