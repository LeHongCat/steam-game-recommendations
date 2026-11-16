"""Local FastAPI serving and feedback capture for the Steam recommendation demo."""

import hmac
import json
import logging
import math
import os
import sqlite3
import uuid
from contextlib import closing, contextmanager
from functools import lru_cache
from pathlib import Path
from time import perf_counter
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src import query_model
from src import recommendation_activity as activity
from src import storage
from src.recommendation import MIN_SUPPORT, QUERY_MODEL_WEIGHT, Ranker
from src.recommendation_labels import display_name, explanation, tag_labels
from src.recommendation_query import PACE_TAG, parse_query

ROOT = storage.ROOT / "data/recommendation"
app = FastAPI(title="Steam Game Recommendations")
logger = logging.getLogger(__name__)

class RecommendRequest(BaseModel):
    user_id: str | None = Field(default=None, max_length=80)
    liked_items: list[str] = Field(default_factory=list, max_length=5)
    aspects: list[str] = Field(default_factory=list, max_length=6)
    query: str = Field(default="", max_length=400)
    top_k: int = Field(default=6, ge=1, le=20)


class FeedbackRequest(BaseModel):
    request_id: uuid.UUID
    event: Literal["impression", "detail", "click", "dismiss"]
    item_ids: list[str] = Field(min_length=1, max_length=20)


class DemoUserLogin(BaseModel):
    user_id: int = Field(ge=1, le=3, strict=True)


class DemoAdminLogin(BaseModel):
    pin: str = Field(min_length=1, max_length=128)


@lru_cache(maxsize=1)
def load_snapshot(path: Path, __version: int):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        report = json.loads(
            db.execute("SELECT data FROM snapshot WHERE key='report'").fetchone()[0]
        )
        games, popularity, similarities, aspects = {}, {}, {}, {}
        for item, payload in db.execute("SELECT id, data FROM games"):
            game = json.loads(payload)
            popularity[item] = game.pop("popularity")
            similarities[item] = game.pop("similarities")
            aspects[item] = game.pop("aspects")
            games[item] = game
        users = {
            user: json.loads(profile)
            for user, profile in db.execute("SELECT id, data FROM user_profiles")
        }
    return Ranker(
        games, users, popularity, similarities, aspects, report["evaluation"]["weights"]
    ), report


def snapshot():
    path = ROOT / "serving.sqlite"
    try:
        return load_snapshot(path, path.stat().st_mtime_ns)
    except (OSError, sqlite3.Error, TypeError, ValueError) as error:
        logger.warning("Recommendation snapshot unavailable: %s", error)
        raise HTTPException(503, "Build the recommendation database first.") from error


@contextmanager
def event_database():
    with closing(sqlite3.connect(ROOT / "events.sqlite", timeout=15)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS recommendation_requests (
                id TEXT PRIMARY KEY, event_time TEXT NOT NULL, model_version TEXT NOT NULL,
                run_id TEXT NOT NULL, latency_ms REAL NOT NULL, candidate_count INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS recommendation_items (
                request_id TEXT REFERENCES recommendation_requests(id), item_id TEXT NOT NULL,
                rank INTEGER NOT NULL, score REAL NOT NULL, PRIMARY KEY(request_id, item_id)
            );
            CREATE TABLE IF NOT EXISTS feedback (
                request_id TEXT NOT NULL, item_id TEXT NOT NULL, event TEXT NOT NULL,
                event_time TEXT NOT NULL, PRIMARY KEY(request_id, item_id, event),
                FOREIGN KEY(request_id, item_id) REFERENCES recommendation_items(request_id, item_id)
            );
        """)
        db.executescript(activity.SCHEMA)
        with db:
            yield db


def public_game(game):
    item = game["item_id"]
    return {
        "item_id": item,
        "title": game["title"],
        "description": game["description"],
        "is_free": game["is_free"],
        "price_minor_units": game["price_minor_units"],
        "price_currency": game["price_currency"],
        "category_ids": game["category_ids"],
        "store_url": f"https://store.steampowered.com/app/{item}/",
        "image_url": (
            f"/art/{item}.jpg"
            if (ROOT / "art" / f"{item}.jpg").is_file()
            else f"https://cdn.cloudflare.steamstatic.com/steam/apps/{item}/header.jpg"
        ),
    }


def demo_profiles(ranker):
    users = (
        (len(profile["seen"]), user, profile)
        for user, profile in ranker.users.items()
        if len(profile["liked"]) >= 2 and profile["aspects"]
    )
    available = sorted(users, key=lambda entry: (-entry[0], entry[1]))[:3]
    return {
        f"demo-{index}": {
            "user_id": user,
            "liked_games": [ranker.games[item]["title"] for item in profile["liked"]],
        }
        for index, (_, user, profile) in enumerate(available, start=1)
    }


def current_session(request, role):
    token = request.cookies.get(activity.COOKIE_NAMES[role])
    if not token:
        return None
    with event_database() as db:
        return activity.session(db, token, role)


def sign_in(request, response, role, user_id=None):
    name = activity.COOKIE_NAMES[role]
    with event_database() as db:
        activity.revoke(db, request.cookies.get(name))
        token = activity.create_session(db, role, user_id)
    response.set_cookie(
        name, token, max_age=activity.SESSION_SECONDS, httponly=True, samesite="strict"
    )


def sign_out(request, response, role):
    name = activity.COOKIE_NAMES[role]
    with event_database() as db:
        activity.revoke(db, request.cookies.get(name))
    response.delete_cookie(name, httponly=True, samesite="strict")
    return {"ok": True}


def user_profile(ranker, user_id):
    profile = demo_profiles(ranker).get(f"demo-{user_id}")
    if profile is None:
        raise HTTPException(404, "This demo profile is missing from the snapshot.")
    return profile


def public_user(user_id, ranker):
    profile = user_profile(ranker, user_id)
    aspects = ranker.users[profile["user_id"]]["aspects"]
    ranked_aspects = sorted(aspects.items(), key=lambda row: (-row[1]["support"], row[0]))
    with event_database() as db:
        clicked = activity.clicked_games(db, user_id, ranker.games)
    return {
        "id": user_id,
        "liked_games": profile["liked_games"],
        "recent_clicks": [ranker.games[item]["title"] for item in clicked],
        "review_aspects": [
            {"label": display_name(aspect), "support": value["support"]}
            for aspect, value in ranked_aspects[:3]
        ],
    }


@app.middleware("http")
async def disable_dynamic_cache(request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/") or request.url.path in {
        "/",
        "/admin",
        "/recommend",
        "/feedback",
    }:
        response.headers["Cache-Control"] = "no-store"
    return response


@app.post("/api/user/login")
def user_login(payload: DemoUserLogin, request: Request, response: Response):
    ranker, _ = snapshot()
    user = public_user(payload.user_id, ranker)
    sign_in(request, response, "user", payload.user_id)
    return {"user": user}


@app.get("/api/user/session")
def user_session(request: Request):
    current = current_session(request, "user")
    if not current:
        return {"user": None}
    ranker, _ = snapshot()
    return {"user": public_user(current["user_id"], ranker)}


@app.get("/api/user/games/{item_id}/aspects")
def game_aspects(
    item_id: str, request: Request, aspect: list[str] = Query(default=[], max_length=3)
):
    if not current_session(request, "user"):
        raise HTTPException(401, "Sign in to view review evidence.")
    ranker, _ = snapshot()
    game = ranker.games.get(item_id)
    if game is None:
        raise HTTPException(404, "Game not found in the catalog.")
    summary = game.get("absa_summary", {})
    supported = summary.get("aspects", {})
    chosen = aspect or sorted(supported, key=lambda name: (-supported[name]["support"], name))[:2]
    return {
        "title": game["title"],
        "reviews_processed": summary.get("processed", 0),
        "aspects": [
            {"id": name, "label": display_name(name), **supported[name]}
            for name in dict.fromkeys(chosen)
            if name in supported
        ],
    }


@app.post("/api/user/logout")
def user_logout(request: Request, response: Response):
    return sign_out(request, response, "user")


@app.post("/api/admin/login")
def admin_login(payload: DemoAdminLogin, request: Request, response: Response):
    pin = os.environ.get("STEAM_DEMO_ADMIN_PIN", "2468")
    if not pin or not hmac.compare_digest(payload.pin.encode(), pin.encode()):
        raise HTTPException(403, "Incorrect Admin PIN.")
    sign_in(request, response, "admin")
    return {"authenticated": True}


@app.get("/api/admin/session")
def admin_session(request: Request):
    return {"authenticated": current_session(request, "admin") is not None}


@app.post("/api/admin/logout")
def admin_logout(request: Request, response: Response):
    return sign_out(request, response, "admin")


@app.get("/health")
def health():
    _, report = snapshot()
    return {"status": "ok", "model_version": report["model_version"], "run_id": report["run_id"]}


@app.get("/api/catalog")
def catalog():
    ranker, report = snapshot()
    topics = {
        topic
        for profile in ranker.item_aspects.values()
        for topic, row in profile.items()
        if row["support"] >= MIN_SUPPORT
    }
    return {
        "games": [
            public_game(game)
            for game in sorted(ranker.games.values(), key=lambda game: game["title"].casefold())
        ],
        "aspects": [
            {"id": topic, "label": display_name(topic)}
            for topic in sorted(topics, key=display_name)
        ],
        "profiles": [
            {"id": name, "liked_games": profile["liked_games"]}
            for name, profile in demo_profiles(ranker).items()
        ],
        "run_id": report["run_id"],
        "model_version": report["model_version"],
    }


@app.post("/api/user/recommend")
def recommend_demo(payload: RecommendRequest, request: Request):
    current = current_session(request, "user")
    if current is None:
        raise HTTPException(401, "Sign in to find games.")
    return rank_request(payload, current["user_id"])


@app.post("/recommend")
def recommend(request: RecommendRequest):
    return rank_request(request)


def rank_request(request, demo_user_id=None):
    started = perf_counter()
    ranker, report = snapshot()
    if set(request.liked_items) - ranker.games.keys():
        raise HTTPException(422, "An App ID is outside the demo catalog.")
    topics = {topic for profile in ranker.item_aspects.values() for topic in profile}
    if set(request.aspects) - topics:
        raise HTTPException(422, "An aspect is outside the ABSA dataset.")
    profiles = (
        demo_profiles(ranker) if request.user_id and request.user_id.startswith("demo-") else {}
    )
    user_id = profiles.get(request.user_id, {}).get("user_id", request.user_id)
    clicked = []
    if demo_user_id is not None:
        user_id = user_profile(ranker, demo_user_id)["user_id"]
        with event_database() as db:
            clicked = activity.clicked_games(db, demo_user_id, ranker.games)
    query = request.query.strip()
    intent = parse_query(query) if query else None
    semantic = query_model.score(query, ranker.games) if query else None
    semantic_weight = QUERY_MODEL_WEIGHT
    semantic_only = bool(intent and not intent.recognized)
    match = "rules" if intent and intent.recognized else None
    if semantic_only:
        # Relevance dominates when the request has no supported tag or aspect. Weak matches
        # still rank the whole catalog by similarity so a free-text request never comes back
        # empty; the response says the match is approximate.
        close = query_model.close_matches(semantic)
        match = "close" if close else "approximate" if semantic else None
        semantic = close or semantic
        semantic_weight = query_model.SEMANTIC_ONLY_WEIGHT
    requested_aspects = intent.aspects if intent else []
    priorities = list(
        dict.fromkeys([*request.aspects, *(name for name in requested_aspects if name in topics)])
    )
    ranked = ranker.rank(
        user_id, request.liked_items, priorities, request.top_k, intent=intent, clicked=clicked,
        semantic=semantic, semantic_weight=semantic_weight,
    )
    profile = ranker.users.get(user_id, {})
    rows = []
    for rank, entry in enumerate(ranked, start=1):
        item = entry["item_id"]
        matches = sorted(entry["matched_aspects"], key=lambda aspect: -aspect["quality"])
        matches = [{**aspect, "label": display_name(aspect["aspect"])} for aspect in matches]
        matched_tags = tag_labels(entry.pop("matched_tag_ids"))
        reason = explanation(matches, matched_tags, entry["pace_review_support"],
                             bool(entry["signals"]["cf"] or entry["signals"]["content"]))
        if semantic is not None:
            reason = "Scored against your request using the query model. " + reason
        rows.append(
            {
                **entry,
                "rank": rank,
                "matched_aspects": matches,
                "matched_tags": matched_tags,
                "explanation": reason,
                "game": public_game(ranker.games[item]),
            }
        )
    request_id = str(uuid.uuid4())
    latency = round(1000 * (perf_counter() - started), 2)
    ranking = {
        "top_k": request.top_k, "returned": len(rows),
        "eligible_candidates": rows[0]["scoring"]["eligible_candidates"] if rows else 0,
    }
    query_intent = None
    if intent:
        shown_tags = {tag["id"] for row in rows for tag in row["matched_tags"]}
        if any(row["pace_review_support"] for row in rows):
            shown_tags.add(PACE_TAG)
        query_intent = {
            "status": {
                "rules": "applied", "close": "semantic", "approximate": "approximate"
            }.get(match, "unrecognized"),
            "query_model": {"id": query_model.MODEL_ID, "weight": semantic_weight} if semantic else None,
            "required_tag_groups": [tag_labels(group) for group in intent.required_groups],
            "excluded_tags": tag_labels(intent.excluded),
            "preferred_tags": tag_labels(intent.preferred),
            "preferred_tags_missing_in_results": tag_labels(intent.preferred - shown_tags),
            "aspects": [
                {"id": name, "label": display_name(name), "available": name in topics}
                for name in requested_aspects
            ],
        }
    with event_database() as db:
        db.execute(
            "INSERT INTO recommendation_requests VALUES (?,?,?,?,?,?)",
            (
                request_id,
                storage.now().isoformat(),
                report["model_version"],
                report["run_id"],
                latency,
                len(rows),
            ),
        )
        db.executemany(
            "INSERT INTO recommendation_items VALUES (?,?,?,?)",
            [(request_id, row["item_id"], row["rank"], row["score"]) for row in rows],
        )
        if demo_user_id is not None:

            def named_games(ids):
                return [{"title": ranker.games[item]["title"], "item_id": item} for item in ids]

            criteria = [display_name(topic) for topic in priorities]
            if intent:
                criteria.extend(
                    tag["label"] for tag in tag_labels(intent.required_tags | intent.preferred)
                )
                criteria.extend("Exclude " + tag["label"] for tag in tag_labels(intent.excluded))
            source = [
                name
                for name, value in (
                    ("query", query),
                    ("games", request.liked_items),
                    ("aspects", request.aspects),
                )
                if value
            ]
            activity.save_search(
                db,
                request_id,
                demo_user_id,
                query,
                {
                    "source": source or ["history"],
                    "selected_games": named_games(dict.fromkeys(request.liked_items)),
                    "steam_history": named_games(profile["liked"]),
                    "click_history": named_games(clicked),
                    "criteria": criteria,
                    "query_intent": query_intent,
                    "top_k": request.top_k,
                    "ranking": ranking,
                },
                rows,
            )
    return {
        "request_id": request_id,
        "recommendations": rows,
        "latency_ms": latency,
        "model_version": report["model_version"],
        "run_id": report["run_id"],
        "mode": "query"
        if intent
        else "hybrid"
        if profile or request.liked_items or priorities
        else "popularity",
        "query_intent": query_intent,
        "ranking": ranking,
    }


@app.post("/feedback")
def feedback(request: FeedbackRequest, http: Request):
    request_id = str(request.request_id)
    items = set(request.item_ids)
    with event_database() as db:
        owner = db.execute(
            "SELECT user_id FROM demo_searches WHERE request_id=?", (request_id,)
        ).fetchone()
        if owner:
            current = activity.session(db, http.cookies.get(activity.COOKIE_NAMES["user"]), "user")
            if not current or current["user_id"] != owner[0]:
                raise HTTPException(403, "This search belongs to another profile.")
        results = dict(
            db.execute(
                "SELECT item_id, rank FROM recommendation_items WHERE request_id=?", (request_id,)
            )
        )
        if not items <= results.keys():
            raise HTTPException(404, "This game was not recommended in the search.")
        if request.event != "impression":
            seen = {
                row[0]
                for row in db.execute(
                    "SELECT item_id FROM feedback WHERE request_id=? AND event='impression'",
                    (request_id,),
                )
            }
            if not items <= seen:
                raise HTTPException(409, "Record an impression before submitting feedback.")
        before = db.total_changes
        db.executemany(
            "INSERT OR IGNORE INTO feedback VALUES (?,?,?,?)",
            [(request_id, item, request.event, storage.now().isoformat()) for item in items],
        )
        return {"recorded": db.total_changes - before}


def require_admin(request):
    if not current_session(request, "admin"):
        raise HTTPException(401, "Sign in as Admin to view activity.")


@app.get("/api/admin/activity")
def admin_activity(
    request: Request,
    user_id: int | None = Query(default=None, ge=1, le=3),
    hours: int = Query(default=0, ge=0, le=720),
    limit: int = Query(default=20, ge=1, le=50),
    offset: int = Query(default=0, ge=0),
):
    require_admin(request)
    with event_database() as db:
        return activity.activity_overview(db, user_id, hours, limit, offset)


@app.get("/api/admin/searches/{request_id}")
def admin_search(request_id: uuid.UUID, request: Request):
    require_admin(request)
    with event_database() as db:
        detail = activity.search_detail(db, str(request_id))
    if detail is None:
        raise HTTPException(404, "Search not found.")
    return detail


@app.get("/api/admin/absa")
def admin_absa(request: Request):
    require_admin(request)
    ranker, report = snapshot()
    games = []
    for game in sorted(ranker.games.values(), key=lambda value: value["title"].casefold()):
        summary = game.get("absa_summary")
        if summary is None:
            continue
        games.append(
            {
                "item_id": game["item_id"],
                "title": game["title"],
                **{key: value for key, value in summary.items() if key != "aspects"},
                "aspects": [
                    {
                        "id": name,
                        "label": display_name(name),
                        "support": data["support"],
                        "counts": data["counts"],
                    }
                    for name, data in sorted(
                        summary["aspects"].items(), key=lambda row: (-row[1]["support"], row[0])
                    )
                ],
            }
        )
    return {
        "run_id": report["run_id"],
        "model": report.get("absa_model"),
        "score_mode": report.get("absa_score_mode"),
        "audit": report.get("handoff"),
        "games": games,
        "reviews": report.get("reviews"),
        "processed": report.get("absa_processed"),
        "reviews_with_aspects": report.get("reviews_with_aspects"),
        "mentions": report.get("aspect_mentions"),
    }


@app.get("/api/metrics")
def metrics():
    _, report = snapshot()
    with event_database() as db:
        values = db.execute(
            "SELECT latency_ms, candidate_count FROM recommendation_requests WHERE run_id=? AND model_version=? ORDER BY event_time DESC LIMIT 1000",
            (report["run_id"], report["model_version"]),
        ).fetchall()
        event_counts = dict(
            db.execute(
                """
            SELECT f.event, count(*) FROM feedback f JOIN recommendation_requests r ON f.request_id=r.id
            WHERE r.run_id=? AND r.model_version=? GROUP BY f.event
        """,
                (report["run_id"], report["model_version"]),
            )
        )
    ordered = sorted(row[0] for row in values)
    latency = (
        {
            "p50": ordered[math.ceil(0.5 * len(ordered)) - 1],
            "p95": ordered[math.ceil(0.95 * len(ordered)) - 1],
        }
        if ordered
        else {}
    )
    return {
        **report,
        "feedback": event_counts,
        "latency_ms": latency,
        "sample_requests": len(values),
        "empty_results": sum(row[1] == 0 for row in values),
    }


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(storage.ROOT / "ui/recommendation/index.html")


@app.get("/admin", include_in_schema=False)
def admin_page():
    return FileResponse(storage.ROOT / "ui/recommendation/admin.html")


app.mount("/static", StaticFiles(directory=storage.ROOT / "ui/recommendation", check_dir=False))


@app.get("/art/{name}", include_in_schema=False)
def art(name: str):
    if name != "hero.jpg" and not (
        name.endswith(".jpg") and name[:-4].isascii() and name[:-4].isdigit()
    ):
        raise HTTPException(404)
    path = ROOT / "art" / name
    if not path.is_file():
        raise HTTPException(404)
    return FileResponse(
        path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"}
    )
