"""Steam API Bronze → cleaned Silver → descriptive Gold and durable Kafka outbox."""
import argparse
import html
import json
import unicodedata
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser

import pyarrow as pa
import pyarrow.parquet as pq

from src import common as c
from src.schemas import GAME_SCHEMA, REVIEW_SCHEMA, INTERACTION_SCHEMA, STATS_SCHEMA


def parquet(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="snappy")
    temporary.replace(path)


def text(value):
    value = unicodedata.normalize("NFC", str(value or "")).replace("\r\n", "\n").replace("\r", "\n")
    value = "".join(character for character in value if unicodedata.category(character) != "Cc" or character in "\n\t")
    return "\n".join(line.strip() for line in value.splitlines()).strip()


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, value):
        self.parts.append(value)


def description(value):
    parser = PlainText()
    parser.feed(str(value or ""))
    return text(html.unescape(" ".join(parser.parts)))


def timestamp(value):
    if type(value) is not int or value <= 0:
        raise ValueError("invalid_unix_timestamp")
    return datetime.fromtimestamp(value, timezone.utc)


def optional_int(value):
    return value if type(value) is int and value >= 0 else None


def optional_bool(value):
    return value if type(value) is bool else None


def normalize_game(row, config, observed_at):
    app = str(row.get("appid", ""))
    if not app.isdigit() or int(app) not in config["app_ids"] or row.get("success") != 1 or not row.get("name"):
        raise ValueError("unavailable_or_invalid_catalog_item")
    if row.get("type", 0) != 0:
        raise ValueError("not_a_base_game")
    basic = row.get("basic_info", {})
    option = row.get("best_purchase_option") or {}
    free = row.get("is_free", False)
    raw_price = str(option.get("final_price_in_cents", ""))
    price = 0 if free else int(raw_price) if raw_price.isdigit() else None
    categories = row.get("categories", {})
    return {"item_id": app, "title": text(row["name"]), "description": description(basic.get("short_description")),
            "developers": [text(value["name"]) for value in basic.get("developers", []) if value.get("name")],
            "publishers": [text(value["name"]) for value in basic.get("publishers", []) if value.get("name")],
            "tag_ids": sorted(set(int(value) for value in row.get("tagids", []))),
            "category_ids": sorted(set(value for values in categories.values() for value in values)),
            "is_free": free, "price_minor_units": price, "price_currency": config["currency"] if price is not None else None,
            "price_currency_source": "request_context" if price is not None else None,
            "purchase_option_name": option.get("purchase_option_name"),
            "purchase_options_json": c.dumps(row.get("purchase_options", [])), "country_code": config["country_code"],
            "observed_at": observed_at, "source": "steam_api"}


def normalize_review(row, app, page, observed_at, language):
    review_id = str(row.get("recommendationid", ""))
    author = row.get("author") or {}
    steam_id = str(author.get("steamid", ""))
    if not review_id.isdigit() or not steam_id.isdigit() or int(review_id) <= 0 or int(steam_id) <= 0:
        raise ValueError("missing_or_invalid_review_or_user_id")
    if type(row.get("voted_up")) is not bool:
        raise ValueError("recommended_must_be_boolean")
    if language != "all" and row.get("language") != language:
        raise ValueError("unexpected_review_language")
    created, updated = timestamp(row.get("timestamp_created")), timestamp(row.get("timestamp_updated"))
    if updated < created:
        raise ValueError("updated_before_created")
    review_text = text(row.get("review"))
    return {"review_id": review_id, "user_id": "steam:" + c.digest(steam_id.encode()), "item_id": app,
            "review_text": review_text, "recommended": row["voted_up"], "language": row.get("language"),
            "created_at": created, "updated_at": updated, "observed_at": observed_at,
            "playtime_at_review_minutes": optional_int(author.get("playtime_at_review")),
            "playtime_forever_minutes": optional_int(author.get("playtime_forever")),
            "votes_up": optional_int(row.get("votes_up")), "votes_funny": optional_int(row.get("votes_funny")),
            "steam_purchase": optional_bool(row.get("steam_purchase")),
            "received_for_free": optional_bool(row.get("received_for_free")), "refunded": optional_bool(row.get("refunded")),
            "absa_eligible": bool(review_text), "source_page": page, "source": "steam_api"}


def revision(row):
    fields = ("review_id", "user_id", "item_id", "review_text", "recommended", "language", "created_at", "updated_at", "playtime_at_review_minutes")
    return c.digest(c.dumps({key: row[key] for key in fields}).encode("utf-8"))


def clean(run_id):
    info = c.read_manifest(run_id)
    if not info["collection_complete"]:
        raise ValueError("Cannot clean an incomplete API crawl")
    games, reviews = {}, {}
    if info["base_run_id"]:
        base = c.DATA / "silver" / info["base_run_id"]
        games = {row["item_id"]: row for row in pq.read_table(base / "games.parquet").to_pylist()}
        reviews = {row["review_id"]: row for row in pq.read_table(base / "reviews.parquet").to_pylist()}
    previous = {key: revision(row) for key, row in reviews.items()}
    rejected, polled, repeated = [], {}, 0
    catalog = json.loads((c.DATA / info["catalog"]["path"]).read_bytes())["response"]["store_items"]
    for index, raw in enumerate(catalog):
        try:
            row = normalize_game(raw, info["config"], datetime.fromisoformat(info["catalog"]["fetched_at"]))
            games[row["item_id"]] = row
        except (ValueError, TypeError, KeyError) as error:
            rejected.append({"entity": "game", "row": index, "reason": str(error)})
    for app, progress in info["apps"].items():
        for page in progress["pages"]:
            raw_rows = json.loads((c.DATA / page["path"]).read_bytes())["response"]["reviews"]
            for index, raw in enumerate(raw_rows):
                try:
                    if app not in games:
                        raise ValueError("item_missing_from_catalog")
                    row = normalize_review(raw, app, page["path"], datetime.fromisoformat(page["fetched_at"]), info["config"]["language"])
                    key = row["review_id"]
                    repeated += key in polled
                    if key not in polled or row["updated_at"] >= polled[key]["updated_at"]:
                        polled[key] = row
                except (ValueError, TypeError, KeyError, OverflowError, OSError) as error:
                    rejected.append({"entity": "review", "page": page["path"], "row": index, "reason": str(error)})
    for key, row in polled.items():
        if key not in reviews or row["updated_at"] >= reviews[key]["updated_at"]:
            reviews[key] = row
    review_rows = sorted(reviews.values(), key=lambda row: row["review_id"])
    interactions = [{"event_id": "review:" + row["review_id"], "source_review_id": row["review_id"],
                     "user_id": row["user_id"], "item_id": row["item_id"],
                     "event_type": "review_positive" if row["recommended"] else "review_negative",
                     "event_time": row["created_at"], "source": "steam_api_review"} for row in review_rows]
    destination = c.DATA / "silver" / run_id
    parquet(destination / "games.parquet", sorted(games.values(), key=lambda row: row["item_id"]), GAME_SCHEMA)
    parquet(destination / "reviews.parquet", review_rows, REVIEW_SCHEMA)
    parquet(destination / "interactions.parquet", interactions, INTERACTION_SCHEMA)
    c.atomic_json(c.DATA / "quarantine" / f"{run_id}.json", rejected)
    counts = {"catalog_rows_observed": len(catalog), "games_in_snapshot": len(games),
              "review_rows_observed": sum(app["rows"] for app in info["apps"].values()),
              "unique_reviews_in_poll": len(polled), "duplicate_review_rows": repeated,
              "rejected_reviews": sum(row["entity"] == "review" for row in rejected),
              "rejected_games": sum(row["entity"] == "game" for row in rejected),
              "reviews_in_snapshot": len(reviews), "new_review_ids": sum(key not in previous for key in reviews),
              "changed_review_versions": sum(key in previous and revision(row) != previous[key] for key, row in reviews.items()),
              "empty_review_text": sum(not row["absa_eligible"] for row in reviews.values())}
    info.update(status="silver_ready", counts=counts)
    c.save_manifest(info)
    return run_id


def aggregate(run_id):
    groups = {}
    for row in pq.read_table(c.DATA / "silver" / run_id / "reviews.parquet").to_pylist():
        value = groups.setdefault(row["item_id"], [0, 0])
        value[0] += 1
        value[1] += int(row["recommended"])
    rows = [{"item_id": key, "review_count": total, "positive_count": positive,
             "negative_count": total - positive, "positive_ratio": positive / total, "run_id": run_id}
            for key, (total, positive) in sorted(groups.items())]
    parquet(c.DATA / "gold" / run_id / "game_statistics.parquet", rows, STATS_SCHEMA)
    return run_id


def verify(run_id):
    info = c.read_manifest(run_id)
    pages = [info["catalog"]] + [page for app in info["apps"].values() for page in app["pages"]]
    integrity = all(c.digest((c.DATA / page["path"]).read_bytes()) == page["sha256"] for page in pages)
    base = c.DATA / "silver" / run_id
    games = pq.read_table(base / "games.parquet").to_pylist()
    reviews = pq.read_table(base / "reviews.parquet").to_pylist()
    interactions = pq.read_table(base / "interactions.parquet").to_pylist()
    stats = pq.read_table(c.DATA / "gold" / run_id / "game_statistics.parquet").to_pylist()
    ids = {row["item_id"] for row in games}
    checks = {"bronze_checksums": integrity, "collection_complete": info["collection_complete"],
              "requested_games_available": set(map(str, info["config"]["app_ids"])).issubset(ids),
              "nonempty_reviews": bool(reviews), "unique_review_ids": len({r["review_id"] for r in reviews}) == len(reviews),
              "valid_item_references": all(row["item_id"] in ids for row in reviews + interactions),
              "review_reconciliation": info["counts"]["review_rows_observed"] == info["counts"]["unique_reviews_in_poll"] + info["counts"]["duplicate_review_rows"] + info["counts"]["rejected_reviews"],
              "one_feedback_per_review": len(interactions) == len(reviews),
              "valid_utc_timestamps": all(row["created_at"].tzinfo and row["updated_at"] >= row["created_at"] for row in reviews),
              "gold_reconciliation": sum(row["review_count"] for row in stats) == len(reviews),
              "binary_feedback_preserved": all(row["positive_count"] + row["negative_count"] == row["review_count"] for row in stats)}
    if not all(checks.values()):
        raise ValueError(f"Quality gate failed: {checks}")
    # Commit accepted review versions and pending Kafka events in the same transaction.
    with c.database() as db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute("SELECT manifest_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if existing:
            info = json.loads(existing[0])
        else:
            enqueued = 0
            for row in reviews:
                version = revision(row)
                old = db.execute("SELECT revision, updated_at FROM reviews WHERE review_id=?", (row["review_id"],)).fetchone()
                if old and (old[0] == version or old[1] > row["updated_at"].isoformat()):
                    continue
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "steam-review:" + row["review_id"] + ":" + version))
                event = {"schema_version": 2, "event_id": event_id, "event_type": "review_upsert", "review_id": row["review_id"],
                         "item_id": row["item_id"], "revision": version, "source_run_id": run_id,
                         "source_mode": "api_poll", "emitted_at": c.now(), "review": row}
                cursor = db.execute("INSERT OR IGNORE INTO outbox(event_id,run_id,payload_json) VALUES(?,?,?)", (event_id, run_id, c.dumps(event)))
                enqueued += cursor.rowcount
                db.execute("INSERT OR REPLACE INTO reviews VALUES(?,?,?)", (row["review_id"], version, row["updated_at"].isoformat()))
            info.update(status="completed", checks=checks, new_events_enqueued=enqueued, verified_at=c.now().isoformat())
            db.execute("INSERT INTO runs VALUES(?,?)", (run_id, c.dumps(info)))
    c.save_manifest(info)
    c.atomic_json(c.DATA / "latest.json", {"run_id": run_id})
    c.atomic_json(c.ROOT / "reports/runtime" / f"api-quality-{run_id}.json", info)
    return {"run_id": run_id, "counts": info["counts"], "checks": info["checks"], "new_events_enqueued": info["new_events_enqueued"]}


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
    print(c.dumps(verify(run_id)) if args.stage in ("all", "verify") else run_id)


if __name__ == "__main__":
    main()
