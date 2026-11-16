"""Audit the Phase 1 artifacts and attach review evidence to each game."""

import hashlib
import json
import math
import re
from collections import Counter, defaultdict

import pyarrow.parquet as pq

from src import storage

PACE = re.compile(r"\b(?:fast[- ]paced|high[- ]octane)\b", re.I)
PRIVATE_TEXT = re.compile(r"https?://\S+|\S+@\S+\.\S+|\b\d{10,}\b", re.I)


def excerpt(value):
    value = " ".join(value.split())
    return PRIVATE_TEXT.sub("[…]", value) if 24 <= len(value) <= 220 else ""


def attach_evidence(games, reviews, mentions, outputs, handoff, model_id):
    all_counts = Counter(row["item_id"] for row in reviews)
    processed = [row for row in reviews if row["review_id"] in mentions]
    item_counts = Counter(row["item_id"] for row in processed)
    user_counts = Counter(row["user_id"] for row in processed)
    item_topics, user_topics = defaultdict(Counter), defaultdict(Counter)
    for output in outputs:
        item_topics[output["item_id"]][output["aspect"]] += 1
        user_topics[output["user_id"]][output["aspect"]] += 1

    audit = {
        "catalog": check_catalog(handoff / "games.parquet", games),
        "items": check_profiles(handoff / "item_dict.json", item_counts, item_topics, "support"),
        "users": check_profiles(
            handoff / "user_dict.json", user_counts, user_topics, "topic_counts"
        ),
    }
    sentence_path = handoff / "absa_sentences.parquet"
    cache_path = handoff / ("absa_cache__" + re.sub(r"[^\w.-]", "_", model_id) + ".jsonl")
    cache = {}
    if cache_path.is_file():
        with cache_path.open(encoding="utf-8") as source:
            for line in source:
                row = json.loads(line)
                cache[row["h"]] = (set(row["labels"]), float(row["raw"]))

    sentences = {}
    audit.update({"sentences": None, "cache": None})
    if cache_path.is_file():
        audit["cache"] = {"entries": len(cache), "matched_sentences": 0, "consistent_sentences": 0}
    if sentence_path.is_file():
        for row in pq.read_table(sentence_path).to_pylist():
            key = row["review_id"], row["sent_idx"]
            if key in sentences or key[0] not in mentions:
                raise ValueError(f"Invalid or duplicate ABSA sentence key {key}")
            text = row["sentence"]
            labels = set(json.loads(row["labels_json"]))
            sentences[key] = text, labels
            if audit["cache"] is not None:
                digest = hashlib.md5(text.encode(), usedforsecurity=False).hexdigest()
                if digest in cache:
                    cached_labels, cached_score = cache[digest]
                    audit["cache"]["matched_sentences"] += 1
                    audit["cache"]["consistent_sentences"] += (
                        labels == cached_labels
                        and math.isclose(cached_score, row["raw_score"], abs_tol=0.0001)
                    )
        audit["sentences"] = {"rows": len(sentences), "verified_mentions": 0}

    profiles = {item: {} for item in games}
    pace_reviews = defaultdict(set)
    reviews_with_aspects = defaultdict(set)
    examples = {}
    for output in outputs:
        item = output["item_id"]
        topic = output["aspect"]
        polarity = output["sentiment"]
        review_id = output["review_id"]
        profile = profiles[item].setdefault(topic, {"review_ids": set(), "counts": Counter()})
        profile["review_ids"].add(review_id)
        profile["counts"][polarity] += 1
        reviews_with_aspects[item].add(review_id)
        if audit["sentences"] is None:
            continue
        text, labels = sentences.get((review_id, output["sentence_index"]), ("", set()))
        if f"{topic}:{polarity}" not in labels:
            continue
        audit["sentences"]["verified_mentions"] += 1
        if topic == "GAMEPLAY#GENERAL" and polarity == "POS" and PACE.search(text):
            pace_reviews[item].add(review_id)
        quoted = excerpt(text)
        if quoted:
            key = item, topic, polarity
            score = abs(output["sentiment_score"])
            if key not in examples or score > examples[key][0]:
                examples[key] = score, quoted

    for item, game in games.items():
        game["absa_summary"] = {
            "reviews": all_counts[item],
            "processed": item_counts[item],
            "reviews_with_aspects": len(reviews_with_aspects[item]),
            "fast_paced_reviews": len(pace_reviews[item]),
            "aspects": {
                topic: {
                    "support": len(profile["review_ids"]),
                    "counts": dict(profile["counts"]),
                    "examples": [
                        {"sentiment": polarity, "text": examples[item, topic, polarity][1]}
                        for polarity in ("POS", "NEG", "NEU")
                        if (item, topic, polarity) in examples
                    ],
                }
                for topic, profile in profiles[item].items()
            },
        }
    audit["games_with_pace_evidence"] = sum(len(rows) >= 2 for rows in pace_reviews.values())
    return audit


def check_catalog(path, games):
    if not path.is_file():
        return None
    rows = pq.read_table(path, columns=["item_id", "title", "tag_ids", "category_ids"]).to_pylist()
    matched = sum(
        row["item_id"] in games
        and all(
            games[row["item_id"]][key] == row[key] for key in ("title", "tag_ids", "category_ids")
        )
        for row in rows
    )
    return {"rows": len(rows), "matched": matched}


def check_profiles(path, counts, topics, field):
    if not path.is_file():
        return None
    tables = storage.read_json(path)
    return {
        "rows": len(tables),
        "matched": sum(
            key in counts and row["review_count"] == counts[key] and row[field] == topics[key]
            for key, row in tables.items()
        ),
    }
