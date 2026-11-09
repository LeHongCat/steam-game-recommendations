"""Read-only local dashboard for Steam pipeline evidence."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from src import storage, telemetry

ASSETS = {
    "/": ("index.html", "text/html"),
    "/dashboard.css": ("dashboard.css", "text/css"),
    "/dashboard.js": ("dashboard.js", "text/javascript"),
}


def read_optional(path, default=None):
    try:
        return storage.read_json(path)
    except FileNotFoundError:
        return default


def status(run_id=None):
    manifests = storage.DATA / "manifests"
    runs = [
        path.stem
        for path in sorted(
            manifests.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True
        )
    ]
    if not run_id:
        run_id = runs[0] if runs else None
    if run_id is None:
        return {"runs": runs, "run_id": None}

    directory = telemetry.run_directory(run_id)
    info = read_optional(manifests / f"{run_id}.json")
    if info is None:
        return {"runs": runs, "run_id": run_id, "missing": True}

    titles = {}
    if info["catalog"]:
        catalog = read_optional(storage.DATA / info["catalog"]["path"], {})
        titles = {
            str(game["appid"]): game["name"]
            for game in catalog.get("response", {}).get("store_items", [])
            if game.get("appid") and game.get("name")
        }

    destination = storage.DATA / "exports" / run_id
    summary = read_optional(destination / "summary.json", {})
    exported = {str(game["app_id"]): game for game in summary.get("games", [])}
    kafka = read_optional(destination / "kafka.json", {})
    steps = telemetry.step_records(run_id)
    latest = {entry["step"]: entry for entry in steps}
    return {
        "run_id": run_id,
        "runs": runs,
        "status": info["status"],
        "started_at": info["started_at"],
        "language": info["config"]["language"],
        "raw_limit": info["config"]["max_reviews_per_game"],
        "target": read_optional(directory / "target.json", {}).get("per_game"),
        "counts": info.get("counts", {}),
        "batch_checks": info.get("checks", {}),
        "stream_checks": kafka.get("checks", {}),
        "kafka": kafka.get("kafka", {}),
        "exported": summary.get("total_exported"),
        "games": [
            {
                "app_id": app,
                "title": titles.get(app, f"Steam App {app}"),
                "rows": info["apps"][app]["rows"],
                "pages": len(info["apps"][app]["pages"]),
                "done": info["apps"][app]["done"],
                "export": exported.get(app, {}),
            }
            for app in map(str, info["config"]["app_ids"])
        ],
        "steps": latest,
        "attempts": [
            {key: value for key, value in entry.items() if key != "metrics"} for entry in steps
        ],
        "incidents": read_optional(directory / "incidents.json", []),
    }


class DashboardHandler(BaseHTTPRequestHandler):
    def send(self, code, content_type, data):
        self.send_response(code)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/api/status":
            try:
                result = status(parse_qs(url.query).get("run", [None])[0])
            except ValueError:
                self.send(400, "application/json", b'{"error":"Invalid run ID"}')
                return
            self.send(200, "application/json", storage.dumps(result).encode())
        elif url.path in ASSETS:
            name, content_type = ASSETS[url.path]
            self.send(200, content_type, (storage.ROOT / "ui" / name).read_bytes())
        else:
            self.send(404, "text/plain", b"Not found")

    def log_request(self, code="-", size="-"):
        if code != 200:
            super().log_request(code, size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    with ThreadingHTTPServer((args.host, args.port), DashboardHandler) as server:
        print(f"Dashboard listening on {args.host}:{args.port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
