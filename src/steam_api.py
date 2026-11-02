"""Bounded, resumable polling of live Steam catalog and review APIs."""

import json
import math
import time
import uuid
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src import state, storage

CATALOG_URL = "https://api.steampowered.com/IStoreBrowseService/GetItems/v1/"
REVIEWS_URL = "https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/"


def load_config(path=None):
    config = storage.read_json(path or storage.ROOT / "config/steam.json")
    fields = {
        "app_ids",
        "language",
        "country_code",
        "currency",
        "max_reviews_per_game",
        "page_size",
        "request_interval_seconds",
        "timeout_seconds",
        "max_attempts",
    }
    if not isinstance(config, dict) or set(config) != fields:
        raise ValueError(f"Config must contain exactly these fields: {', '.join(sorted(fields))}")
    ids = config.get("app_ids")
    if (
        not isinstance(ids, list)
        or not ids
        or any(type(value) is not int or value <= 0 for value in ids)
        or len(ids) != len(set(ids))
    ):
        raise ValueError("app_ids must contain unique positive Steam App IDs")
    if any(
        type(config[field]) is not int or config[field] <= 0
        for field in ("page_size", "max_reviews_per_game", "max_attempts")
    ):
        raise ValueError("Page size, review limit and attempt limit must be positive integers")
    if config["page_size"] > 100:
        raise ValueError("Invalid per-game or page size limit")
    if config["country_code"] != "US" or config["currency"] != "USD":
        raise ValueError("This adapter currently supports an explicit US/USD price context only")
    for field, minimum in (("timeout_seconds", 0), ("request_interval_seconds", 0)):
        value = config[field]
        if type(value) not in (int, float) or not math.isfinite(value) or value < minimum:
            raise ValueError(f"{field} must be a finite nonnegative number")
    if config["timeout_seconds"] == 0:
        raise ValueError("timeout_seconds must be positive")
    if not isinstance(config["language"], str) or not config["language"].strip():
        raise ValueError("language must be a nonempty Steam language code or 'all'")
    return config


def retry_delay(value: str, attempt: int) -> float:
    """Accept both Retry-After seconds and the standard HTTP-date form."""
    if value:
        try:
            seconds = float(value)
            if math.isfinite(seconds):
                return max(0, seconds)
        except ValueError:
            try:
                deadline = parsedate_to_datetime(value)
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                return max(0, (deadline - storage.now()).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
    return min(2**attempt, 30)


class SteamClient:
    def __init__(self, config):
        self.config = config
        self.last_request = 0

    def get(self, endpoint, parameters):
        url = (
            endpoint
            + "?"
            + urlencode({"input_json": json.dumps(parameters, separators=(",", ":"))})
        )
        for attempt in range(self.config["max_attempts"]):
            delay = self.config["request_interval_seconds"] - (time.monotonic() - self.last_request)
            if delay > 0:
                time.sleep(delay)
            self.last_request = time.monotonic()
            try:
                request = Request(
                    url,
                    headers={
                        "User-Agent": "SteamRecommendationCourseDemo/2.0",
                        "Accept": "application/json",
                    },
                )
                with urlopen(request, timeout=self.config["timeout_seconds"]) as response:
                    raw = response.read()
                    headers = {
                        name: response.headers[name]
                        for name in ("Content-Type", "Date", "ETag")
                        if response.headers.get(name)
                    }
                    status = response.status
                body = json.loads(raw)
                if not isinstance(body.get("response"), dict):
                    raise ValueError("Steam API response envelope is missing")
                return raw, {
                    "endpoint": endpoint,
                    "parameters": parameters,
                    "http_status": status,
                    "headers": headers,
                    "fetched_at": storage.now().isoformat(),
                    "attempts": attempt + 1,
                }
            except HTTPError as error:
                if error.code != 429 and error.code < 500:
                    raise
                delay = retry_delay(error.headers.get("Retry-After", ""), attempt)
                cause = error
            except (URLError, TimeoutError, ConnectionError) as error:
                cause = error
                delay = min(2**attempt, 30)
            if attempt + 1 == self.config["max_attempts"]:
                raise RuntimeError(f"Steam API request failed after retries: {endpoint}") from cause
            time.sleep(delay)


def store_response(run_id, name, raw, metadata):
    path = storage.DATA / "bronze/api" / run_id / name
    storage.atomic_bytes(path, raw)
    storage.atomic_json(path.with_suffix(".meta.json"), metadata)
    return {
        "path": path.relative_to(storage.DATA).as_posix(),
        "sha256": storage.digest(raw),
        **metadata,
    }


def crawl(config_path=None, run_id=None, client=None):
    config = load_config(config_path)
    run_id = (
        run_id or "poll-" + storage.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    )
    if not run_id or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        for character in run_id
    ):
        raise ValueError("Unsafe run_id")
    path = storage.DATA / "manifests" / f"{run_id}.json"
    if path.exists():
        info = storage.read_manifest(run_id)
        if info["config"] != config:
            raise ValueError("A resumed crawl must keep the original configuration")
        if info["collection_complete"]:
            return run_id
    else:
        info = {
            "run_id": run_id,
            "source": "steam_api",
            "started_at": storage.now().isoformat(),
            "config": config,
            "base_run_id": state.latest_run_id(),
            "collection_complete": False,
            "status": "collecting",
            "catalog": None,
            "apps": {
                str(app): {"pages": [], "next_cursor": "*", "rows": 0, "done": False}
                for app in config["app_ids"]
            },
        }
        storage.save_manifest(info)
    client = client or SteamClient(config)
    if info["catalog"] is None:
        parameters = {
            "ids": [{"appid": app} for app in config["app_ids"]],
            "context": {"language": config["language"], "country_code": config["country_code"]},
            "data_request": {
                "include_basic_info": True,
                "include_release": True,
                "include_all_purchase_options": True,
                "include_tag_count": 20,
            },
        }
        raw, metadata = client.get(CATALOG_URL, parameters)
        if not isinstance(json.loads(raw)["response"].get("store_items"), list):
            raise ValueError("Catalog API did not return store_items")
        info["catalog"] = store_response(run_id, "catalog.json", raw, metadata)
        storage.save_manifest(info)
    for app, progress in info["apps"].items():
        while not progress["done"]:
            parameters = {
                "appid": int(app),
                "filter": 2,
                "languages": [config["language"]],
                "review_type": 0,
                "purchase_type": 1,
                "cursor": progress["next_cursor"],
                "num_per_page": min(
                    config["page_size"], config["max_reviews_per_game"] - progress["rows"]
                ),
            }
            raw, metadata = client.get(REVIEWS_URL, parameters)
            body = json.loads(raw)["response"]
            if not isinstance(body.get("reviews"), list):
                raise ValueError("Review API did not return a reviews list")
            rows = len(body["reviews"])
            next_cursor = body.get("cursor")
            if rows and (not next_cursor or next_cursor == progress["next_cursor"]):
                raise ValueError("Review pagination cursor did not advance")
            record = store_response(
                run_id, f"reviews-{app}-{len(progress['pages']):04d}.json", raw, metadata
            )
            record["row_count"] = rows
            progress["pages"].append(record)
            progress["rows"] += rows
            progress["next_cursor"] = next_cursor
            progress["done"] = rows == 0 or progress["rows"] >= config["max_reviews_per_game"]
            progress["stop_reason"] = (
                "limit" if rows and progress["done"] else "exhausted" if not rows else "paging"
            )
            storage.save_manifest(info)  # Durable page and cursor before continuing/retrying.
    info.update(
        collection_complete=True,
        status="bronze_ready",
        collected_at=storage.now().isoformat(),
        coverage="bounded_latest_updates",
    )
    storage.save_manifest(info)
    return run_id
