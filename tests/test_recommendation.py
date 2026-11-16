import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from src import recommendation_api
from src.recommendation import fit
from src.recommendation_handoff import attach_evidence
from src.recommendation_query import parse_query

NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)
GAMES = {
    item: {
        "item_id": item,
        "title": f"Game {item}",
        "description": "Space exploration",
        "tag_ids": [1],
        "category_ids": [2],
        "is_free": True,
        "price_minor_units": 0,
        "price_currency": "USD",
    }
    for item in ("1", "2", "3")
}


def review(review_id, user, item, recommended=True):
    return {
        "review_id": review_id,
        "user_id": user,
        "item_id": item,
        "recommended": recommended,
        "updated_at": NOW,
    }


class RecommendationTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(recommendation_api.query_model, "score", return_value=None)
        self.query_score = patcher.start()
        self.addCleanup(patcher.stop)

    def test_handoff_only_exposes_evidence_linked_to_the_predicted_label(self):
        games = {"1": dict(GAMES["1"])}
        reviews = [review("r", "u", "1")]
        prediction = {
            "review_id": "r",
            "item_id": "1",
            "user_id": "u",
            "aspect": "GAMEPLAY#GENERAL",
            "sentiment": "POS",
            "sentence_index": 0,
            "sentiment_score": 0.8,
        }
        with tempfile.TemporaryDirectory() as directory:
            handoff = Path(directory)
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "review_id": "r",
                            "sent_idx": 0,
                            "sentence": "Fast-paced action and flexible controls make this enjoyable.",
                            "labels_json": json.dumps(["GAMEPLAY#GENERAL:POS"]),
                            "raw_score": 0.8,
                        },
                        {
                            "review_id": "r",
                            "sent_idx": 1,
                            "sentence": "Another fast-paced sentence without the expected label.",
                            "labels_json": "[]",
                            "raw_score": 0.0,
                        },
                    ]
                ),
                handoff / "absa_sentences.parquet",
            )
            audit = attach_evidence(
                games,
                reviews,
                {"r": {}},
                [prediction, {**prediction, "sentence_index": 1}],
                handoff,
                "test-model",
            )
        self.assertEqual(audit["sentences"]["verified_mentions"], 1)
        summary = games["1"]["absa_summary"]
        self.assertEqual(summary["fast_paced_reviews"], 1)
        examples = summary["aspects"]["GAMEPLAY#GENERAL"]["examples"]
        self.assertEqual(len(examples), 1)
        self.assertIn("flexible controls", examples[0]["text"])

    def test_demo_user_search_is_visible_to_admin_with_its_results_and_feedback(self):
        games = {**GAMES, "4": {**GAMES["3"], "item_id": "4", "title": "Game 4", "tag_ids": [1663]}}
        reviews = [
            review("r1", "a", "1"),
            review("r2", "a", "2"),
            review("r3", "b", "2"),
            review("r4", "b", "3"),
        ]
        ranker = fit(
            games, reviews, {row["review_id"]: {"STORY#GENERAL": [0.7]} for row in reviews}, NOW
        )
        report = {"model_version": "test-model", "run_id": "test-run"}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(recommendation_api, "ROOT", Path(directory)),
            patch.object(recommendation_api, "snapshot", return_value=(ranker, report)),
            patch.dict("os.environ", {"STEAM_DEMO_ADMIN_PIN": "test-pin"}),
            TestClient(recommendation_api.app) as web,
        ):
            self.assertEqual(web.get("/api/admin/activity").status_code, 401)
            self.assertEqual(web.post("/api/user/recommend", json={}).status_code, 401)
            self.assertEqual(web.post("/api/admin/login", json={"pin": "wrong"}).status_code, 403)
            self.assertEqual(web.post("/api/user/login", json={"user_id": 4}).status_code, 422)

            user = web.post("/api/user/login", json={"user_id": 1}).json()["user"]
            self.assertEqual(user["liked_games"], ["Game 1", "Game 2"])
            params = {
                "query": "FPS, cốt truyện",
                "liked_items": ["3"],
                "top_k": 2,
                "user_id": "demo-2",
            }
            result = web.post("/api/user/recommend", json=params).json()
            self.assertEqual([row["item_id"] for row in result["recommendations"]], ["4"])
            request_id = result["request_id"]
            event = {"request_id": request_id, "item_ids": ["4"]}
            for kind in ("impression", "detail", "click"):
                self.assertEqual(
                    web.post("/feedback", json={**event, "event": kind}).json(), {"recorded": 1}
                )
            self.assertEqual(web.get("/api/admin/activity").status_code, 401)
            self.assertEqual(
                web.post("/api/admin/login", json={"pin": "test-pin"}).status_code, 200
            )
            self.assertEqual(web.get("/api/user/session").json()["user"]["id"], 1)

            overview = web.get("/api/admin/activity?user_id=1").json()
            self.assertEqual(overview["summary"]["searches"], 1)
            self.assertEqual(
                overview["charts"]["queries"], [{"label": params["query"], "count": 1}]
            )
            self.assertEqual(overview["charts"]["viewed_games"], [{"label": "Game 4", "count": 1}])
            self.assertEqual(web.get("/api/admin/activity?user_id=2").json()["total"], 0)
            detail = web.get(f"/api/admin/searches/{request_id}").json()
            self.assertEqual(detail["inputs"]["source"], ["query", "games"])
            self.assertEqual(detail["inputs"]["selected_games"][0]["title"], "Game 3")
            self.assertEqual(detail["inputs"]["steam_history"][0]["item_id"], "1")
            self.assertEqual(detail["results"][0]["rank"], 1)
            self.assertEqual(detail["results"][0]["scoring"], result["recommendations"][0]["scoring"])
            self.assertEqual(detail["inputs"]["ranking"], result["ranking"])
            self.assertEqual(
                [row["event"] for row in detail["events"]], ["impression", "detail", "click"]
            )

            second = web.post("/api/user/recommend", json=params).json()
            self.assertEqual(second["recommendations"], [])
            next_search = web.get(f"/api/admin/searches/{second['request_id']}").json()
            self.assertEqual(
                next_search["inputs"]["click_history"], [{"title": "Game 4", "item_id": "4"}]
            )

            web.post("/api/user/login", json={"user_id": 2})
            self.assertEqual(
                web.post("/feedback", json={**event, "event": "dismiss"}).status_code, 403
            )
            web.post("/api/user/logout")
            self.assertIsNone(web.get("/api/user/session").json()["user"])
            self.assertEqual(web.get("/api/admin/activity").status_code, 200)
            web.post("/api/admin/logout")
            self.assertEqual(web.get(f"/api/admin/searches/{request_id}").status_code, 401)

    def test_semantic_query_logs_weighted_score_and_abstains_on_weak_matches(self):
        games = {**GAMES, "4": {**GAMES["3"], "item_id": "4"}}
        ranker = fit(games, [review("r1", "a", "1")], {}, NOW)
        report = {"model_version": "test-model", "run_id": "test-run"}
        self.query_score.return_value = {"1": 0.71, "2": 0.85, "3": 0.82, "4": 0.74}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(recommendation_api, "ROOT", Path(directory)),
            patch.object(recommendation_api, "snapshot", return_value=(ranker, report)),
            TestClient(recommendation_api.app) as web,
        ):
            result = web.post("/recommend", json={"query": "Deliver cargo across the continent", "top_k": 1}).json()
            self.assertEqual(result["query_intent"]["status"], "semantic")
            self.assertEqual(result["ranking"], {"top_k": 1, "eligible_candidates": 2, "returned": 1})
            row = result["recommendations"][0]
            self.assertEqual(row["item_id"], "2")
            trace = row["scoring"]
            self.assertAlmostEqual(sum(signal["contribution"] for signal in trace["components"]), trace["base_score"])
            step, = trace["adjustments"]
            self.assertEqual(step["kind"], "semantic_query")
            self.assertAlmostEqual(step["relevance"], (step["similarity"] - 0.70) / 0.25)
            self.assertAlmostEqual(step["after"], (1 - step["weight"]) * step["before"] + step["weight"] * step["relevance"])
            self.assertEqual(step["after"], trace["final_score"])
            self.assertEqual(trace["final_score"], row["score"])
            self.query_score.return_value = {"1": 0.78, "2": 0.76, "3": 0.73, "4": 0.70}
            approximate = web.post("/recommend", json={"query": "xyz"}).json()
            self.assertEqual(approximate["query_intent"]["status"], "approximate")
            self.assertEqual(approximate["recommendations"][0]["item_id"], "1")

    def test_query_parses_vietnamese_conditions_and_priorities(self):
        intent = parse_query(
            "Tôi muốn chơi FPS hoặc RPG, không kinh dị, đồ họa đẹp, gameplay nhanh"
        )
        self.assertEqual(intent.required_groups, [{1663, 122}])
        self.assertEqual(intent.excluded, {1667})
        self.assertEqual(intent.preferred, {1734})
        self.assertEqual(intent.aspects, ["GRAPHICS#GENERAL", "GAMEPLAY#GENERAL"])
        self.assertTrue(intent.allows({122}))
        self.assertFalse(intent.allows({122, 1667}))
        self.assertFalse(intent.allows({9}))
        self.assertEqual(parse_query("Smooth performance at 60 FPS").required_groups, [])
        self.assertEqual(parse_query("don't want horror").excluded, {1667})
        self.assertFalse(parse_query("không cần FPS").recognized)
        self.assertEqual(parse_query("I want to play a nice action game").required_groups, [{19}])
        self.assertEqual(parse_query("Lái xe tải chở hàng xuyên châu Âu").required_groups, [{1644}])
        self.assertEqual(parse_query("game hành động đẹp mắt").aspects, ["GRAPHICS#GENERAL"])

    def test_query_filter_precedes_ranking_and_reports_missing_preferences(self):
        games = {
            "1": {**GAMES["1"], "tag_ids": [1663]},
            "2": {**GAMES["2"], "tag_ids": [1663, 1667]},
            "3": {**GAMES["3"], "tag_ids": [122, 1663, 1734], "primary_tag_ids": [122]},
        }
        topic = "GRAPHICS#GENERAL"
        reviews = [review(str(i), f"user-{i}", "1") for i in range(12)]
        ranker = fit(games, reviews, {row["review_id"]: {topic: [0.6]} for row in reviews}, NOW)
        report = {"model_version": "test-model", "run_id": "test-run"}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(recommendation_api, "ROOT", Path(directory)),
            patch.object(recommendation_api, "snapshot", return_value=(ranker, report)),
            TestClient(recommendation_api.app) as web,
        ):
            response = web.post(
                "/recommend", json={"query": "FPS, không kinh dị, đồ họa đẹp, gameplay nhanh"}
            )
            self.assertEqual(response.status_code, 200)
            result = response.json()
            self.assertEqual([row["item_id"] for row in result["recommendations"]], ["1"])
            row = result["recommendations"][0]
            self.assertEqual(row["matched_tags"], [{"id": 1663, "label": "FPS"}])
            self.assertEqual(row["matched_aspects"][0]["aspect"], topic)
            self.assertEqual(
                result["query_intent"]["preferred_tags_missing_in_results"][0]["id"], 1734
            )
            unavailable = web.post("/recommend", json={"query": "game giải đố"}).json()
            self.assertEqual(unavailable["query_intent"]["status"], "applied")
            self.assertEqual(unavailable["recommendations"], [])
            unknown = web.post("/recommend", json={"query": "xyz"}).json()
            self.assertEqual(unknown["query_intent"]["status"], "unrecognized")
            self.assertTrue(unknown["recommendations"])
            self.assertEqual(web.post("/recommend", json={"query": "x" * 401}).status_code, 422)

    def test_features_only_use_reviews_in_fitted_window(self):
        topic = "STORY#GENERAL"
        ranker = fit(
            GAMES,
            [review("earlier", "same-user", "1")],
            {
                "earlier": {topic: [-0.5]},
                "later": {topic: [1.0]},
            },
            NOW,
        )
        self.assertNotIn("2", ranker.item_aspects)
        self.assertEqual(ranker.users["same-user"]["aspects"][topic]["support"], 1)
        self.assertEqual([row["item_id"] for row in ranker.rank("same-user")], ["2", "3"])

    def test_explicit_aspect_uses_supported_sentiment_and_excludes_seed(self):
        topic = "STORY#GENERAL"
        reviews = [
            review(f"{item}-{index}", f"user-{index}", item, recommended=True)
            for item in ("2", "3")
            for index in range(12)
        ]
        mentions = {
            row["review_id"]: {topic: [0.8 if row["item_id"] == "2" else -0.8]} for row in reviews
        }
        ranker = fit(GAMES, reviews, mentions, NOW)
        result = ranker.rank(seeds=["1"], aspects=[topic])
        self.assertEqual([row["item_id"] for row in result], ["2", "3"])
        self.assertEqual(result[0]["matched_aspects"][0]["support"], 12)
        self.assertIsNone(result[0]["signals"]["absa"])

    def test_feedback_is_bound_to_returned_items_and_requires_impression(self):
        ranker = fit(GAMES, [review("r", "u", "1")], {}, NOW)
        report = {"model_version": "test-model", "run_id": "test-run"}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(recommendation_api, "ROOT", Path(directory)),
            patch.object(recommendation_api, "snapshot", return_value=(ranker, report)),
            TestClient(recommendation_api.app) as web,
        ):
            self.assertEqual(web.post("/recommend", json={"top_k": 0}).status_code, 422)
            result = web.post("/recommend", json={"top_k": 1}).json()
            self.assertEqual(result["mode"], "popularity")
            item = result["recommendations"][0]["item_id"]
            feedback = {"request_id": result["request_id"], "item_ids": [item]}
            self.assertEqual(
                web.post("/feedback", json={**feedback, "event": "click"}).status_code, 409
            )
            self.assertEqual(
                web.post(
                    "/feedback",
                    json={
                        **feedback,
                        "item_ids": ["10001"],
                        "event": "impression",
                    },
                ).status_code,
                404,
            )
            impression = {**feedback, "event": "impression"}
            self.assertEqual(web.post("/feedback", json=impression).json(), {"recorded": 1})
            self.assertEqual(web.post("/feedback", json=impression).json(), {"recorded": 0})
            self.assertEqual(
                web.post("/feedback", json={**feedback, "event": "click"}).json(), {"recorded": 1}
            )
