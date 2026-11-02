"""Convert Steam API records into the Silver data contract."""

import html
import unicodedata
from datetime import datetime, timezone
from html.parser import HTMLParser

from src import storage


def text(value):
    if value is not None and not isinstance(value, str):
        raise ValueError("text_must_be_string")
    value = unicodedata.normalize("NFC", str(value or "")).replace("\r\n", "\n").replace("\r", "\n")
    value = "".join(
        character
        for character in value
        if unicodedata.category(character) != "Cc" or character in "\n\t"
    )
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
    if not isinstance(row, dict):
        raise ValueError("catalog_item_must_be_object")
    app = str(row.get("appid", ""))
    if (
        not app.isdigit()
        or int(app) not in config["app_ids"]
        or row.get("success") != 1
        or not row.get("name")
    ):
        raise ValueError("unavailable_or_invalid_catalog_item")
    if row.get("type", 0) != 0:
        raise ValueError("not_a_base_game")
    basic = row.get("basic_info") or {}
    option = row.get("best_purchase_option") or {}
    free = row.get("is_free", False)
    if not isinstance(basic, dict) or not isinstance(option, dict) or type(free) is not bool:
        raise ValueError("invalid_catalog_metadata")
    raw_price = str(option.get("final_price_in_cents", ""))
    price = 0 if free else int(raw_price) if raw_price.isdigit() else None
    categories = row.get("categories") or {}
    if not isinstance(categories, dict):
        raise ValueError("categories_must_be_object")
    return {
        "item_id": app,
        "title": text(row["name"]),
        "description": description(basic.get("short_description")),
        "developers": [
            text(value["name"]) for value in basic.get("developers", []) if value.get("name")
        ],
        "publishers": [
            text(value["name"]) for value in basic.get("publishers", []) if value.get("name")
        ],
        "tag_ids": sorted(set(int(value) for value in row.get("tagids", []))),
        "category_ids": sorted(set(value for values in categories.values() for value in values)),
        "is_free": free,
        "price_minor_units": price,
        "price_currency": config["currency"] if price is not None else None,
        "price_currency_source": "request_context" if price is not None else None,
        "purchase_option_name": option.get("purchase_option_name"),
        "purchase_options_json": storage.dumps(row.get("purchase_options", [])),
        "country_code": config["country_code"],
        "observed_at": observed_at,
        "source": "steam_api",
    }


def normalize_review(row, app, page, observed_at, language):
    if not isinstance(row, dict):
        raise ValueError("review_must_be_object")
    review_id = str(row.get("recommendationid", ""))
    author = row.get("author") or {}
    if not isinstance(author, dict):
        raise ValueError("author_must_be_object")
    steam_id = str(author.get("steamid", ""))
    if (
        not review_id.isdigit()
        or not steam_id.isdigit()
        or int(review_id) <= 0
        or int(steam_id) <= 0
    ):
        raise ValueError("missing_or_invalid_review_or_user_id")
    if type(row.get("voted_up")) is not bool:
        raise ValueError("recommended_must_be_boolean")
    if language != "all" and row.get("language") != language:
        raise ValueError("unexpected_review_language")
    created, updated = (
        timestamp(row.get("timestamp_created")),
        timestamp(row.get("timestamp_updated")),
    )
    if updated < created:
        raise ValueError("updated_before_created")
    review_text = text(row.get("review"))
    return {
        "review_id": review_id,
        "user_id": "steam:" + storage.digest(steam_id.encode()),
        "item_id": app,
        "review_text": review_text,
        "recommended": row["voted_up"],
        "language": row.get("language"),
        "created_at": created,
        "updated_at": updated,
        "observed_at": observed_at,
        "playtime_at_review_minutes": optional_int(author.get("playtime_at_review")),
        "playtime_forever_minutes": optional_int(author.get("playtime_forever")),
        "votes_up": optional_int(row.get("votes_up")),
        "votes_funny": optional_int(row.get("votes_funny")),
        "steam_purchase": optional_bool(row.get("steam_purchase")),
        "received_for_free": optional_bool(row.get("received_for_free")),
        "refunded": optional_bool(row.get("refunded")),
        "absa_eligible": bool(review_text),
        "source_page": page,
        "source": "steam_api",
    }
