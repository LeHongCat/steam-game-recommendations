"""Validate the Phase 1 handoff and build a versioned recommendation snapshot."""

import argparse
import json
import math
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq

from src import storage, telemetry
from src.recommendation import EXPERIMENTS, MODEL_VERSION, fit
from src.recommendation_handoff import attach_evidence
from src.recommendation_query import PRIMARY_TAG_LIMIT

ROOT = storage.ROOT / "data/recommendation"
DEFAULT_RUN = "backfill-50x1000-20261109"
ABSA_SCHEMA = pa.schema(
    [
        ("review_id", pa.string()),
        ("item_id", pa.string()),
        ("user_id", pa.string()),
        ("aspect", pa.string()),
        ("sentiment", pa.string()),
        ("raw_score", pa.float64()),
        ("sentiment_score", pa.float64()),
        ("aspect_term", pa.string()),
        ("opinion_term", pa.string()),
        ("sentence_index", pa.int32()),
        ("model_version", pa.string()),
    ]
)
PROFILE_SCHEMA = pa.schema(
    [
        ("entity_id", pa.string()),
        ("aspect", pa.string()),
        ("score", pa.float64()),
        ("review_support", pa.int64()),
    ]
)
REVIEW_FIELDS = (
    "review_id",
    "item_id",
    "user_id",
    "review_text",
    "created_at",
    "updated_at",
    "recommended",
)


def add_primary_tags(games, run_id):
    try:
        catalog = storage.read_manifest(run_id)["catalog"]
        raw = (storage.DATA / catalog["path"]).read_bytes()
    except FileNotFoundError:
        # Parquet-only handoffs do not preserve the original Steam tag order.
        return
    if storage.digest(raw) != catalog["sha256"]:
        raise ValueError("The Bronze catalog does not match its manifest checksum")
    for item in json.loads(raw)["response"]["store_items"]:
        game = games.get(str(item.get("appid")))
        if game and "tagids" in item:
            game["primary_tag_ids"] = [
                tag for tag in item["tagids"][:PRIMARY_TAG_LIMIT] if tag in game["tag_ids"]
            ]


def read_inputs(run_id, handoff):
    export = storage.DATA / "exports" / run_id
    reviews = pq.read_table(
        export / "reviews.parquet", columns=[*REVIEW_FIELDS, "absa_eligible"]
    ).to_pylist()
    games = {game["item_id"]: game for game in pq.read_table(export / "games.parquet").to_pylist()}
    add_primary_tags(games, run_id)
    indexed = {row["review_id"]: row for row in reviews}
    if len(indexed) != len(reviews) or any(row["item_id"] not in games for row in reviews):
        raise ValueError(
            "The Steam export contains duplicate reviews or references an unknown game"
        )

    config = storage.read_json(handoff / "build_config.json")
    model_id = config.get("hf_model_id") or config.get("model_source")
    if not model_id:
        raise ValueError("Phase 1 build_config.json must identify the ABSA model")
    model_version = f"{model_id}@{config.get('hf_revision') or 'unpinned'}"
    handoff_path = handoff / "absa_database.parquet"
    handoff_rows = pq.read_table(
        handoff_path, columns=[*REVIEW_FIELDS, "n_results", "results_json"]
    )
    mentions, outputs = {}, []
    for row in handoff_rows.to_pylist():
        review_id = row["review_id"]
        review = indexed.get(review_id)
        if (
            review is None
            or not review["absa_eligible"]
            or any(review[key] != row[key] for key in REVIEW_FIELDS)
        ):
            raise ValueError(f"Phase 1 review {review_id} does not match the Steam export")
        if review_id in mentions:
            raise ValueError(f"Duplicate Phase 1 review {review_id}")
        results = json.loads(row["results_json"])
        if not isinstance(results, list) or len(results) != row["n_results"]:
            raise ValueError(f"Invalid ABSA result count for review {review_id}")
        topics = mentions[review_id] = {}
        for result in results:
            match = re.fullmatch(
                r"([A-Z0-9_]+#[A-Z0-9_]+):(POS|NEU|NEG)", result["topic and polarity"]
            )
            raw = float(result["score"])
            if not match or not math.isfinite(raw):
                raise ValueError(f"Invalid ABSA prediction for review {review_id}")
            aspect, sentiment = match.groups()
            score = math.tanh(raw)
            topics.setdefault(aspect, []).append(score)
            outputs.append(
                {
                    "review_id": review_id,
                    "item_id": row["item_id"],
                    "user_id": row["user_id"],
                    "aspect": aspect,
                    "sentiment": sentiment,
                    "raw_score": raw,
                    "sentiment_score": score,
                    "aspect_term": span(result.get("aspect")),
                    "opinion_term": span(result.get("opinion")),
                    "sentence_index": result.get("sentence_idx"),
                    "model_version": model_version,
                }
            )
    eligible = sum(row["absa_eligible"] for row in reviews)
    stats = {
        "games": len(games),
        "reviews": len(reviews),
        "absa_eligible": eligible,
        "absa_processed": len(mentions),
        "reviews_with_aspects": sum(map(bool, mentions.values())),
        "aspect_mentions": len(outputs),
        "distinct_aspects": len({row["aspect"] for row in outputs}),
        "absa_model": model_version,
        "absa_score_mode": config.get("score_mode"),
        "absa_database_sha256": storage.digest(handoff_path.read_bytes()),
        "reviews_sha256": storage.digest((export / "reviews.parquet").read_bytes()),
    }
    return games, reviews, mentions, outputs, stats


def span(value):
    return None if value in (None, "NULL") else value


def metrics(ranker, targets, experiment, k=10):
    recall = ndcg = average_precision = 0.0
    covered = set()
    for user, target in targets.items():
        items = [
            row["item_id"] for row in ranker.rank(user_id=user, top_k=k, experiment=experiment)
        ]
        covered.update(items)
        if target in items:
            rank = items.index(target) + 1
            recall += 1
            ndcg += 1 / math.log2(rank + 1)
            average_precision += 1 / rank
    n = len(targets)
    return {
        "recall": recall / n if n else None,
        "ndcg": ndcg / n if n else None,
        "map": average_precision / n if n else None,
        "coverage": len(covered) / len(ranker.games) if n else None,
    }


def temporal_evaluation(games, reviews, mentions):
    ordered = sorted(reviews, key=lambda row: (row["updated_at"], row["review_id"]))
    train_end = ordered[int(len(ordered) * 0.75)]["updated_at"]
    validation_end = ordered[int(len(ordered) * 0.9)]["updated_at"]
    train = [row for row in ordered if row["updated_at"] < train_end]
    validation = [row for row in ordered if train_end <= row["updated_at"] < validation_end]
    test = [row for row in ordered if row["updated_at"] >= validation_end]

    def targets(ranker, rows):
        result = {}
        for row in rows:
            profile = ranker.users.get(row["user_id"], {})
            if (
                profile.get("liked")
                and row["recommended"]
                and row["item_id"] not in profile["seen"]
            ):
                result.setdefault(row["user_id"], row["item_id"])
        return result

    tuning = fit(games, train, mentions, train_end)
    val_targets = targets(tuning, validation)
    alternatives = [(0.10, 0.50, 0.25, 0.15), (0.20, 0.25, 0.40, 0.15), (0.10, 0.30, 0.30, 0.30)]
    trials = []
    for weights in alternatives:
        tuning.weights = weights
        trials.append({"weights": weights, **metrics(tuning, val_targets, "E4")})
    best = max(trials, key=lambda trial: trial["ndcg"] or 0)["weights"]
    final = fit(games, train + validation, mentions, validation_end)
    final.weights = best
    test_targets = targets(final, test)
    return {
        "target": "First positive Steam review of an unseen game in the window, per user with a prior positive review",
        "timestamp": "updated_at",
        "train_end_exclusive": train_end,
        "validation_end_exclusive": validation_end,
        "train_reviews": len(train),
        "validation_reviews": len(validation),
        "test_reviews": len(test),
        "validation_users": len(val_targets),
        "test_users": len(test_targets),
        "k": 10,
        "weights": best,
        "validation_trials": trials,
        "catalog": "Fixed Steam catalog observed at crawl time. No historical catalog snapshot is available.",
        "results": [
            {"id": experiment, "label": label, **metrics(final, test_targets, experiment)}
            for experiment, label in EXPERIMENTS.items()
        ],
    }


def publish_snapshot(run_id, ranker, outputs, report):
    gold = ROOT / "gold" / run_id
    storage.write_parquet(gold / "absa_outputs.parquet", outputs, ABSA_SCHEMA)
    item_profiles = [
        {
            "entity_id": item,
            "aspect": aspect,
            "score": row["quality"],
            "review_support": row["support"],
        }
        for item, profile in ranker.item_aspects.items()
        for aspect, row in profile.items()
    ]
    user_profiles = [
        {
            "entity_id": user,
            "aspect": aspect,
            "score": row["sentiment"],
            "review_support": row["support"],
        }
        for user, profile in ranker.users.items()
        for aspect, row in profile["aspects"].items()
    ]
    storage.write_parquet(gold / "item_aspect_profiles.parquet", item_profiles, PROFILE_SCHEMA)
    storage.write_parquet(gold / "user_aspect_profiles.parquet", user_profiles, PROFILE_SCHEMA)
    storage.atomic_json(gold / "evaluation.json", report["evaluation"])
    storage.atomic_json(gold / "handoff_audit.json", report["handoff"])

    path = ROOT / "serving.sqlite"
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        db = sqlite3.connect(temporary)
        try:
            with db:
                db.executescript("""
                    CREATE TABLE games (id TEXT PRIMARY KEY, data TEXT NOT NULL);
                    CREATE TABLE user_profiles (id TEXT PRIMARY KEY, data TEXT NOT NULL);
                    CREATE TABLE snapshot (key TEXT PRIMARY KEY, data TEXT NOT NULL);
                """)
                db.executemany(
                    "INSERT INTO games VALUES (?,?)",
                    (
                        (
                            item,
                            storage.dumps(
                                {
                                    **game,
                                    "aspects": ranker.item_aspects.get(item, {}),
                                    "popularity": ranker.popularity[item],
                                    "similarities": ranker.similarities[item],
                                }
                            ),
                        )
                        for item, game in ranker.games.items()
                    ),
                )
                db.executemany(
                    "INSERT INTO user_profiles VALUES (?,?)",
                    ((user, storage.dumps(profile)) for user, profile in ranker.users.items()),
                )
                db.execute("INSERT INTO snapshot VALUES ('report', ?)", (storage.dumps(report),))
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("SQLite snapshot failed its integrity check")
        finally:
            db.close()
        storage._replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "item_profiles": len(item_profiles),
        "user_profiles": len(user_profiles),
        "users": len(ranker.users),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=DEFAULT_RUN)
    parser.add_argument("--handoff", type=Path, default=Path("/handoff"))
    args = parser.parse_args()
    run_id = args.run_id
    log_id = f"recommend-{run_id}"
    telemetry.run_directory(log_id)
    started = perf_counter()
    games, reviews, mentions, outputs, stats = telemetry.run_step(
        log_id, "import_absa", read_inputs, run_id, args.handoff
    )
    stats["handoff"] = telemetry.run_step(
        log_id,
        "verify_absa_handoff",
        attach_evidence,
        games,
        reviews,
        mentions,
        outputs,
        args.handoff,
        stats["absa_model"].rsplit("@", 1)[0],
    )
    evaluation = telemetry.run_step(
        log_id, "evaluate", temporal_evaluation, games, reviews, mentions
    )
    ranker = fit(games, reviews, mentions, max(row["updated_at"] for row in reviews))
    ranker.weights = evaluation["weights"]
    report = {
        **stats,
        "run_id": run_id,
        "model_version": MODEL_VERSION,
        "built_at": datetime.now(timezone.utc),
        "build_seconds_before_publish": perf_counter() - started,
        "users": len(ranker.users),
        "evaluation": evaluation,
    }
    telemetry.run_step(
        log_id, "publish_features", publish_snapshot, run_id, ranker, outputs, report
    )
    print(
        storage.dumps(
            {**stats, "test_users": evaluation["test_users"], "model_version": MODEL_VERSION}
        )
    )


if __name__ == "__main__":
    main()
