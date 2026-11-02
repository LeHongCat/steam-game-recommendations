"""Small, auditable batch pipeline. No model training takes place here."""

import argparse
import ast
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[1]))
SOURCES = {
    "reviews": ("australian_user_reviews.json.gz", "https://mcauleylab.ucsd.edu/public_datasets/data/steam/australian_user_reviews.json.gz"),
    "games": ("steam_games.json.gz", "https://cseweb.ucsd.edu/~wckang/steam_games.json.gz"),
}

GAME_SCHEMA = pa.schema([
    ("item_id", pa.string()), ("title", pa.string()), ("genres", pa.list_(pa.string())),
    ("tags", pa.list_(pa.string())), ("developer", pa.string()), ("publisher", pa.string()),
    ("price_raw", pa.string()), ("source", pa.string()),
])
REVIEW_SCHEMA = pa.schema([
    ("review_id", pa.string()), ("user_id", pa.string()), ("item_id", pa.string()),
    ("review_text", pa.string()), ("recommended", pa.bool_()), ("event_time", pa.string()),
    ("timestamp_precision", pa.string()), ("posted_raw", pa.string()),
    ("absa_eligible", pa.bool_()), ("source", pa.string()),
])
INTERACTION_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("user_id", pa.string()), ("item_id", pa.string()),
    ("event_type", pa.string()), ("event_time", pa.string()), ("source_review_id", pa.string()),
    ("source_mode", pa.string()),
])
STATS_SCHEMA = pa.schema([
    ("item_id", pa.string()), ("review_count", pa.int64()),
    ("positive_count", pa.int64()), ("negative_count", pa.int64()),
    ("positive_ratio", pa.float64()), ("snapshot_id", pa.string()),
])


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)


def records(path):
    # These UCSD files contain Python dict literals, rather than strict JSON.
    # literal_eval parses data without executing the downloaded text.
    with gzip.open(path, "rt", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    yield ast.literal_eval(line)


def parquet(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="snappy")
    os.replace(temporary, path)


def manifest(snapshot_id):
    return json.loads((ROOT / "data/manifests" / f"{snapshot_id}.json").read_text(encoding="utf-8"))


def ingest(limit=5000, source_dir=None):
    files = {}
    for kind, (filename, url) in SOURCES.items():
        cache = Path(source_dir) / filename if source_dir else ROOT / "data/downloads" / filename
        if not cache.exists():
            if source_dir:
                raise FileNotFoundError(cache)
            cache.parent.mkdir(parents=True, exist_ok=True)
            temporary = cache.with_suffix(".part")
            for attempt in range(3):
                try:
                    with urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                        shutil.copyfileobj(response, output)
                    # Read the entire gzip stream to detect truncated/corrupt downloads.
                    with gzip.open(temporary, "rb") as compressed:
                        while compressed.read(1024 * 1024):
                            pass
                    os.replace(temporary, cache)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt)
        checksum = digest(cache.read_bytes())
        destination = ROOT / "data/bronze/files" / checksum / filename
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            shutil.copyfile(cache, destination)
        if digest(destination.read_bytes()) != checksum:
            raise ValueError(f"Bronze checksum mismatch: {destination}")
        files[kind] = {"path": destination.relative_to(ROOT).as_posix(), "sha256": checksum,
                       "url": url, "size_bytes": cache.stat().st_size}
    snapshot_id = digest(json.dumps({"files": files, "limit": limit, "contract": 1}, sort_keys=True).encode())[:16]
    path = ROOT / "data/manifests" / f"{snapshot_id}.json"
    if not path.exists():
        atomic_json(path, {"snapshot_id": snapshot_id, "ingested_at": now(), "limit": limit,
                           "contract_version": 1, "status": "bronze_ready", "files": files})
    return snapshot_id


def parse_posted(value):
    match = re.fullmatch(r"Posted ([A-Za-z]+ \d{1,2}, \d{4})\.", value or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%B %d, %Y").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def clean(snapshot_id):
    info = manifest(snapshot_id)
    games, reviews, rejected = {}, {}, []
    games_scanned = game_duplicates = 0
    for index, row in enumerate(records(ROOT / info["files"]["games"]["path"])):
        games_scanned += 1
        item_id = str(row.get("id", "")).strip()
        title = row.get("title") or row.get("app_name")
        if not item_id.isdigit() or not title:
            rejected.append({"entity": "game", "row": index, "reason": "missing_item_id_or_title"})
            continue
        game_duplicates += item_id in games
        games[item_id] = {"item_id": item_id, "title": str(title), "genres": row.get("genres") or [],
                          "tags": row.get("tags") or [], "developer": row.get("developer"),
                          "publisher": row.get("publisher"), "price_raw": str(row.get("price", "")),
                          "source": "ucsd_steam_catalog_v2"}
    seen, duplicates = 0, 0
    for user_row in records(ROOT / info["files"]["reviews"]["path"]):
        source_user = str(user_row.get("user_id", "")).strip()
        for row in user_row.get("reviews", []):
            if info["limit"] and seen >= info["limit"]:
                break
            seen += 1
            item_id = str(row.get("item_id", "")).strip()
            if not source_user or item_id not in games or type(row.get("recommend")) is not bool:
                rejected.append({"entity": "review", "row": seen, "reason": "invalid_user_item_or_recommend"})
                continue
            user_id = "ucsd_v1:" + digest(source_user.encode())
            posted_raw = str(row.get("posted", ""))
            review_id = "ucsd_v1:" + digest(f"{user_id}|{item_id}|{posted_raw}".encode())
            event_time = parse_posted(posted_raw)
            text = str(row.get("review") or "").strip()
            duplicates += review_id in reviews
            reviews[review_id] = {"review_id": review_id, "user_id": user_id, "item_id": item_id,
                                  "review_text": text, "recommended": row["recommend"],
                                  "event_time": event_time, "timestamp_precision": "day_assumed_utc" if event_time else "unknown",
                                  "posted_raw": posted_raw, "absa_eligible": bool(text), "source": "ucsd_steam_reviews_v1"}
        if info["limit"] and seen >= info["limit"]:
            break
    review_rows = sorted(reviews.values(), key=lambda row: row["review_id"])
    interactions = [{"event_id": row["review_id"], "user_id": row["user_id"], "item_id": row["item_id"],
                     "event_type": "review_positive" if row["recommended"] else "review_negative",
                     "event_time": row["event_time"], "source_review_id": row["review_id"], "source_mode": "historical"}
                    for row in review_rows]
    destination = ROOT / "data/silver" / snapshot_id
    parquet(destination / "games.parquet", sorted(games.values(), key=lambda row: row["item_id"]), GAME_SCHEMA)
    parquet(destination / "reviews.parquet", review_rows, REVIEW_SCHEMA)
    parquet(destination / "interactions.parquet", interactions, INTERACTION_SCHEMA)
    atomic_json(ROOT / "data/quarantine" / f"{snapshot_id}.json", rejected)
    info.update(status="silver_ready", counts={"games": len(games), "games_scanned": games_scanned,
                "game_duplicates_collapsed": game_duplicates, "reviews_scanned": seen,
                "reviews": len(reviews), "interactions": len(interactions), "duplicates_collapsed": duplicates,
                "rejected_reviews": sum(row["entity"] == "review" for row in rejected),
                "rejected_games": sum(row["entity"] == "game" for row in rejected),
                "missing_review_time": sum(row["event_time"] is None for row in review_rows)})
    atomic_json(ROOT / "data/manifests" / f"{snapshot_id}.json", info)
    return snapshot_id


def aggregate(snapshot_id):
    rows = pq.read_table(ROOT / "data/silver" / snapshot_id / "reviews.parquet").to_pylist()
    groups = {}
    for row in rows:
        counts = groups.setdefault(row["item_id"], [0, 0])
        counts[0] += 1
        counts[1] += int(row["recommended"])
    output = [{"item_id": item_id, "review_count": count, "positive_count": positive,
               "negative_count": count - positive, "positive_ratio": positive / count, "snapshot_id": snapshot_id}
              for item_id, (count, positive) in sorted(groups.items())]
    parquet(ROOT / "data/gold" / snapshot_id / "game_statistics.parquet", output, STATS_SCHEMA)
    return snapshot_id


def verify(snapshot_id):
    info = manifest(snapshot_id)
    for source in info["files"].values():
        if digest((ROOT / source["path"]).read_bytes()) != source["sha256"]:
            raise ValueError("Bronze source integrity check failed")
    base = ROOT / "data/silver" / snapshot_id
    games = pq.read_table(base / "games.parquet").to_pylist()
    reviews = pq.read_table(base / "reviews.parquet").to_pylist()
    interactions = pq.read_table(base / "interactions.parquet").to_pylist()
    stats = pq.read_table(ROOT / "data/gold" / snapshot_id / "game_statistics.parquet").to_pylist()
    ids = {row["item_id"] for row in games}
    checks = {
        "nonempty_games": bool(games), "nonempty_reviews": bool(reviews),
        "unique_review_ids": len({r["review_id"] for r in reviews}) == len(reviews),
        "valid_item_references": all(r["item_id"] in ids for r in reviews + interactions),
        "game_reconciliation": info["counts"]["games_scanned"] == len(games) + info["counts"]["game_duplicates_collapsed"] + info["counts"]["rejected_games"],
        "one_feedback_per_review": len(interactions) == len(reviews),
        "review_reconciliation": info["counts"]["reviews_scanned"] == len(reviews) + info["counts"]["duplicates_collapsed"] + info["counts"]["rejected_reviews"],
        "gold_reconciliation": sum(r["review_count"] for r in stats) == len(reviews),
        "binary_feedback_preserved": all(r["positive_count"] + r["negative_count"] == r["review_count"] for r in stats),
    }
    atomic_json(ROOT / "reports/runtime" / f"quality-{snapshot_id}.json", {"snapshot_id": snapshot_id, "checks": checks, "counts": info["counts"]})
    if not all(checks.values()):
        raise ValueError(f"Quality checks failed: {checks}")
    info.update(status="completed", checks=checks, verified_at=now())
    atomic_json(ROOT / "data/manifests" / f"{snapshot_id}.json", info)
    atomic_json(ROOT / "data/latest.json", {"snapshot_id": snapshot_id})
    metadata = ROOT / "data/metadata.sqlite"
    with sqlite3.connect(metadata) as db:
        db.execute("CREATE TABLE IF NOT EXISTS runs (snapshot_id TEXT PRIMARY KEY, verified_at TEXT, manifest_json TEXT)")
        db.execute("INSERT OR REPLACE INTO runs VALUES (?, ?, ?)", (snapshot_id, info["verified_at"], json.dumps(info)))
    return {"snapshot_id": snapshot_id, "checks": checks, "counts": info["counts"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["all", "ingest", "clean", "aggregate", "verify"])
    parser.add_argument("--limit", type=int, default=5000, help="0 reads all reviews; default is a biased engineering demo subset")
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--snapshot-id")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    if args.stage in ("all", "ingest"):
        snapshot_id = ingest(args.limit, args.source_dir)
        if args.stage == "ingest":
            print(snapshot_id)
            return
    else:
        snapshot_id = args.snapshot_id
        if not snapshot_id:
            parser.error("--snapshot-id is required for this stage")
    if args.stage in ("all", "clean"):
        clean(snapshot_id)
    if args.stage in ("all", "aggregate"):
        aggregate(snapshot_id)
    if args.stage in ("all", "verify"):
        print(json.dumps(verify(snapshot_id), indent=2))
    else:
        print(snapshot_id)


if __name__ == "__main__":
    main()
