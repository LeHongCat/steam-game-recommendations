"""Select distinct base games with enough public reviews for a balanced backfill."""

import json
from itertools import batched

from src import steam_api, storage, telemetry

CHARTS = (
    "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/",
    "https://api.steampowered.com/ISteamChartsService/GetGamesByConcurrentPlayers/v1/",
)


def select(run_id, config, game_count, config_path):
    client = steam_api.SteamClient(config)
    candidates, charts = {}, []
    excluded = set(config["app_ids"])
    for index, endpoint in enumerate(CHARTS):
        raw, metadata = client.get(endpoint, {})
        chart = json.loads(raw)["response"]
        charts.append(
            steam_api.store_response(run_id, f"selection/chart-{index}.json", raw, metadata)
        )
        for row in chart["ranks"]:
            if row["appid"] not in excluded:
                candidates.setdefault(row["appid"], {"chart": index, "rank": row["rank"]})

    catalog = {}
    for index, batch in enumerate(batched(candidates, 50)):
        parameters = steam_api.catalog_parameters(config, batch)
        raw, metadata = client.get(steam_api.CATALOG_URL, parameters)
        steam_api.store_response(run_id, f"selection/catalog-{index}.json", raw, metadata)
        for row in json.loads(raw)["response"]["store_items"]:
            if row.get("success") == 1 and row.get("type", 0) == 0 and row.get("name"):
                catalog[row["appid"]] = row["name"]

    checked, selected = [], []
    for app, rank in candidates.items():
        result = {"app_id": app, **rank}
        if app not in catalog:
            checked.append({**result, "outcome": "not_an_available_base_game"})
            continue
        raw, metadata = client.get(
            steam_api.REVIEWS_URL, steam_api.review_parameters(config, app, "*", 1)
        )
        response = json.loads(raw)["response"]
        first_review = next(iter(response.get("reviews", [])), {})
        matching = response.get(
            "total_matching", response.get("query_summary", {}).get("total_reviews", 0)
        )
        enough = matching >= config["max_reviews_per_game"]
        same_language = (
            config["language"] == "all" or first_review.get("language") == config["language"]
        )
        accepted = bool(first_review) and enough and same_language
        checked.append(
            {
                **result,
                "title": catalog[app],
                "matching_reviews": matching,
                "outcome": "selected" if accepted else "too_few_matching_reviews",
                "fetched_at": metadata["fetched_at"],
                "attempts": metadata["attempts"],
            }
        )
        if accepted:
            selected.append(app)
            if len(selected) == game_count:
                break

    report = {
        "charts": charts,
        "excluded_app_ids": sorted(excluded),
        "language": config["language"],
        "minimum_matching_reviews": config["max_reviews_per_game"],
        "selected_app_ids": selected,
        "candidate_checks": checked,
    }
    storage.atomic_json(telemetry.run_directory(run_id) / "selection.json", report)
    if len(selected) != game_count:
        raise ValueError(f"Selected {len(selected)} eligible base games; need {game_count}")
    storage.atomic_json(config_path, {**config, "app_ids": selected})
    return {
        "requested_games": game_count,
        "selected_games": len(selected),
        "candidates_checked": len(checked),
    }
