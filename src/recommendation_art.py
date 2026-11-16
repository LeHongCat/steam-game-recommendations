"""Cache public Steam artwork so the local demo can display it offline."""

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.error import URLError
from urllib.request import urlopen

import pyarrow.parquet as pq

from src import steam_api, storage, telemetry

CDN = "https://shared.fastly.steamstatic.com/store_item_assets/steam/apps"
FALLBACK_CDN = "https://cdn.cloudflare.steamstatic.com/steam/apps"


def cache(run_id):
    telemetry.run_directory(run_id)
    export = storage.DATA / "exports" / run_id
    items = pq.read_table(export / "games.parquet", columns=["item_id"])["item_id"].to_pylist()
    root = storage.ROOT / "data/recommendation/art"
    missing = [item for item in items if not (root / f"{item}.jpg").is_file()]
    assets = {}
    if missing:
        config = steam_api.load_config()
        config.update(max_attempts=1, timeout_seconds=8)
        try:
            parameters = steam_api.catalog_parameters(config, list(map(int, missing)))
            parameters["data_request"] = {"include_assets": True}
            raw, _ = steam_api.SteamClient(config).get(steam_api.CATALOG_URL, parameters)
            for game in json.loads(raw)["response"]["store_items"]:
                header = game.get("assets", {}).get("header", "")
                if re.fullmatch(r"(?:[a-f0-9]{40}/)?header\.jpg", header):
                    assets[str(game["appid"])] = header
        except (OSError, RuntimeError, ValueError):
            print("Steam catalog artwork lookup unavailable. Trying standard CDN paths.")

    def download(job):
        name, item, image = job
        for cdn in (CDN, FALLBACK_CDN):
            try:
                with urlopen(f"{cdn}/{item}/{image}", timeout=10) as response:
                    content = response.read(5_000_001)
                if content.startswith(b"\xff\xd8\xff") and len(content) <= 5_000_000:
                    storage.atomic_bytes(root / name, content)
                    return True
            except (URLError, TimeoutError, ConnectionError):
                continue
        print(f"Steam artwork unavailable for {name}")
        return False

    jobs = [(f"{item}.jpg", item, assets.get(item, "header.jpg")) for item in missing]
    if not (root / "hero.jpg").is_file():
        jobs.append(("hero.jpg", "1086940", "library_hero.jpg"))
    with ThreadPoolExecutor(max_workers=5) as workers:
        saved = sum(workers.map(download, jobs))
    print(f"Cached {saved}/{len(jobs)} missing Steam images")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    cache(parser.parse_args().run_id)
