"""Store demo sessions and attribute searches and feedback to a numeric user."""

import json
import secrets
from collections import Counter
from datetime import timedelta

from src import storage
from src.recommendation_labels import criterion, display_name, explanation, tag_labels

SESSION_SECONDS = 8 * 3600
COOKIE_NAMES = {"user": "steam_demo_user", "admin": "steam_demo_admin"}

SCHEMA = """
    CREATE TABLE IF NOT EXISTS demo_sessions (
        token_hash TEXT PRIMARY KEY,
        role TEXT NOT NULL CHECK(role IN ('user', 'admin')),
        user_id INTEGER,
        expires_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS demo_searches (
        request_id TEXT PRIMARY KEY REFERENCES recommendation_requests(id),
        user_id INTEGER NOT NULL,
        query_text TEXT NOT NULL,
        query_key TEXT NOT NULL,
        input_json TEXT NOT NULL,
        results_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS demo_searches_user ON demo_searches(user_id);
"""


def session(db, token, role):
    if not token:
        return None
    row = db.execute(
        "SELECT user_id FROM demo_sessions WHERE token_hash=? AND role=? AND expires_at>?",
        (storage.digest(token.encode()), role, int(storage.now().timestamp())),
    ).fetchone()
    if row is not None:
        return {"role": role, "user_id": row[0]}
    return None


def revoke(db, token):
    if token:
        db.execute(
            "DELETE FROM demo_sessions WHERE token_hash=?", (storage.digest(token.encode()),)
        )


def create_session(db, role, user_id=None):
    token = secrets.token_urlsafe(32)
    now = int(storage.now().timestamp())
    db.execute("DELETE FROM demo_sessions WHERE expires_at<=?", (now,))
    db.execute(
        "INSERT INTO demo_sessions VALUES (?,?,?,?)",
        (storage.digest(token.encode()), role, user_id, now + SESSION_SECONDS),
    )
    return token


def clicked_games(db, user_id, catalog):
    rows = db.execute(
        """
        SELECT f.item_id, max(f.event_time)
        FROM feedback f JOIN demo_searches s ON s.request_id=f.request_id
        WHERE s.user_id=? AND f.event='click'
        GROUP BY f.item_id ORDER BY max(f.event_time) DESC, f.item_id LIMIT 5
    """,
        (user_id,),
    )
    return [item for item, _ in rows if item in catalog]


def save_search(db, request_id, user_id, query, inputs, rows):
    results = [
        {
            "item_id": row["item_id"],
            "title": row["game"]["title"],
            "rank": row["rank"],
            "score": row["score"],
            "explanation": row["explanation"],
            "matched_aspects": row["matched_aspects"],
            "matched_tags": row["matched_tags"],
            "scoring": row["scoring"],
        }
        for row in rows
    ]
    query_key = " ".join(query.casefold().split())
    db.execute(
        "INSERT INTO demo_searches VALUES (?,?,?,?,?,?)",
        (request_id, user_id, query, query_key, storage.dumps(inputs), storage.dumps(results)),
    )


def search_detail(db, request_id):
    row = db.execute(
        """
        SELECT s.user_id, s.query_text, s.input_json, s.results_json, r.event_time,
               r.model_version, r.run_id
        FROM demo_searches s JOIN recommendation_requests r ON r.id=s.request_id
        WHERE s.request_id=?
    """,
        (request_id,),
    ).fetchone()
    if row is None:
        return None
    results = json.loads(row[3])
    inputs = json.loads(row[2])
    inputs["criteria"] = [criterion(value) for value in inputs["criteria"]]
    for game in results:
        if "scoring" not in game:
            for aspect in game["matched_aspects"]:
                aspect["label"] = display_name(aspect["aspect"])
            game["matched_tags"] = tag_labels(tag["id"] for tag in game["matched_tags"])
            game["explanation"] = explanation(
                game["matched_aspects"], game["matched_tags"], similar="Gần với" in game["explanation"]
            )
    titles = {game["item_id"]: game["title"] for game in results}
    events = [
        {"item_id": item, "title": titles[item], "event": event, "at": at}
        for item, event, at in db.execute(
            "SELECT item_id,event,event_time FROM feedback WHERE request_id=? ORDER BY event_time",
            (request_id,),
        )
    ]
    return {
        "request_id": request_id,
        "user_id": row[0],
        "query": row[1],
        "inputs": inputs,
        "results": results,
        "at": row[4],
        "model_version": row[5],
        "run_id": row[6],
        "events": events,
    }


def activity_overview(db, user_id, hours, limit, offset):
    filters, parameters = [], []
    if user_id is not None:
        filters.append("s.user_id=?")
        parameters.append(user_id)
    if hours:
        filters.append("r.event_time>=?")
        parameters.append((storage.now() - timedelta(hours=hours)).isoformat())
    where = "WHERE " + " AND ".join(filters) if filters else ""
    searches = db.execute(
        f"""
        SELECT s.request_id,s.user_id,s.query_text,s.query_key,s.input_json,s.results_json,
               r.event_time,r.candidate_count
        FROM demo_searches s JOIN recommendation_requests r ON r.id=s.request_id
        {where} ORDER BY r.event_time DESC, s.request_id
    """,
        parameters,
    )
    phrases, criteria, selections, viewed_games, totals = (Counter() for _ in range(5))
    events = {}
    for request_id, item_id, event, count in db.execute(
        f"""
        SELECT f.request_id,f.item_id,f.event,count(*)
        FROM feedback f JOIN demo_searches s ON s.request_id=f.request_id
        JOIN recommendation_requests r ON r.id=s.request_id
        {where} GROUP BY f.request_id,f.item_id,f.event
    """,
        parameters,
    ):
        events.setdefault(request_id, {}).setdefault(item_id, {})[event] = count
        totals[event] += count
    users, latest = set(), []
    total_searches = 0
    phrase_labels = {}
    for index, (key, user, query, query_key, inputs, results, at, count) in enumerate(searches):
        total_searches += 1
        users.add(user)
        inputs = json.loads(inputs)
        if query_key:
            phrases[query_key] += 1
            phrase_labels.setdefault(query_key, query)
        criteria.update({criterion(value) for value in inputs["criteria"]})
        selections.update(game["title"] for game in inputs["selected_games"])
        search_events = events.get(key, {})
        if search_events:
            for game in json.loads(results):
                item_events = search_events.get(game["item_id"], {})
                if item_events.keys() & {"detail", "click"}:
                    viewed_games[game["title"]] += 1
        if offset <= index < offset + limit:
            latest.append(
                {
                    "request_id": key,
                    "user_id": user,
                    "query": query,
                    "at": at,
                    "source": inputs["source"],
                    "result_count": count,
                    "view_count": sum(
                        bool(row.keys() & {"detail", "click"}) for row in search_events.values()
                    ),
                    "selected_games": inputs["selected_games"],
                }
            )

    def chart(counter, labels=None):
        return [
            {"label": labels[key] if labels else key, "count": count}
            for key, count in sorted(counter.items(), key=lambda row: (-row[1], row[0]))[:8]
        ]

    return {
        "summary": {
            "searches": total_searches,
            "users": len(users),
            "impressions": totals["impression"],
            "details": totals["detail"],
            "clicks": totals["click"],
            "dismisses": totals["dismiss"],
        },
        "charts": {
            "queries": chart(phrases, phrase_labels),
            "criteria": chart(criteria),
            "selected_games": chart(selections),
            "viewed_games": chart(viewed_games),
        },
        "searches": latest,
        "total": total_searches,
    }
