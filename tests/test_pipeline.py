import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow.parquet as pq

from src import backfill, dashboard, storage, telemetry
from src import pipeline as p
from src import steam_api as api
from src import streaming as stream


class FixtureAPI:
    """Deterministic API-shaped responses; real connectivity is verified separately."""

    def __init__(self, reviews, fail_page=None):
        self.reviews = reviews
        self.fail_page = fail_page
        self.calls = []

    def get(self, endpoint, parameters):
        self.calls.append((endpoint, parameters))
        if endpoint == api.CATALOG_URL:
            body = {
                "store_items": [
                    {
                        "appid": 10,
                        "success": 1,
                        "type": 0,
                        "name": "Test game",
                        "is_free": True,
                        "purchase_options": [
                            {
                                "final_price_in_cents": "1499",
                                "purchase_option_name": "Optional upgrade",
                            }
                        ],
                        "basic_info": {"short_description": "<b>A &amp; B</b>"},
                    }
                ]
            }
        else:
            page = 0 if parameters["cursor"] == "*" else int(parameters["cursor"])
            if page == self.fail_page:
                raise ConnectionError("Injected page failure")
            size = parameters["num_per_page"]
            body = {"reviews": self.reviews[page : page + size], "cursor": str(page + size)}
        raw = json.dumps({"response": body}).encode()
        return raw, {
            "endpoint": endpoint,
            "parameters": parameters,
            "http_status": 200,
            "fetched_at": "2026-11-02T00:00:00+00:00",
            "headers": {},
            "attempts": 1,
        }


def review(
    review_id="100", recommended=False, updated=1700000000, message="  Not good!\r\n  Bugs.\x00  "
):
    return {
        "recommendationid": review_id,
        "author": {"steamid": "76561198000000001", "playtime_at_review": 120},
        "language": "english",
        "review": message,
        "voted_up": recommended,
        "timestamp_created": 1600000000,
        "timestamp_updated": updated,
        "votes_up": 2,
    }


class APIDataBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.root_patch = patch.object(storage, "ROOT", self.root)
        self.data_patch = patch.object(storage, "DATA", self.root / "data/steam_api")
        self.root_patch.start()
        self.data_patch.start()
        self.config = self.root / "config.json"
        self.config.write_text(
            json.dumps(
                {
                    "app_ids": [10],
                    "language": "english",
                    "country_code": "US",
                    "currency": "USD",
                    "max_reviews_per_game": 2,
                    "page_size": 1,
                    "request_interval_seconds": 0,
                    "timeout_seconds": 10,
                    "max_attempts": 2,
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self):
        self.data_patch.stop()
        self.root_patch.stop()
        self.temporary.cleanup()

    def run_poll(self, run_id, rows):
        api.crawl(self.config, run_id, FixtureAPI(rows))
        p.clean(run_id)
        p.aggregate(run_id)
        return p.verify(run_id)

    def test_pagination_cleaning_negative_feedback_and_free_game_price(self):
        result = self.run_poll("first", [review(), review("101", True)])
        self.assertTrue(all(result["checks"].values()))
        self.assertEqual(result["new_events_enqueued"], 2)
        rows = pq.read_table(storage.DATA / "silver/first/reviews.parquet").to_pylist()
        self.assertFalse(rows[0]["recommended"])
        self.assertEqual(rows[0]["review_text"], "Not good!\nBugs.")
        self.assertIsNone(rows[0]["playtime_forever_minutes"])
        self.assertEqual(rows[0]["created_at"].utcoffset().total_seconds(), 0)
        game = pq.read_table(storage.DATA / "silver/first/games.parquet").to_pylist()[0]
        self.assertEqual(
            game["price_minor_units"], 0
        )  # The optional upgrade is not the base game's price.
        self.assertEqual(game["description"], "A & B")

    def test_same_api_state_has_no_new_event_and_updates_create_one_version(self):
        self.run_poll("first", [review(), review("101", True)])
        repeated = self.run_poll("second", [review(), review("101", True)])
        self.assertEqual(repeated["new_events_enqueued"], 0)
        updated = self.run_poll(
            "third", [review(updated=1700001000, message="Updated opinion"), review("101", True)]
        )
        self.assertEqual(updated["new_events_enqueued"], 1)
        self.assertEqual(updated["counts"]["reviews_in_snapshot"], 2)
        with storage.database() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 3)

    def test_resume_fetches_only_unfinished_pages(self):
        client = FixtureAPI([review(), review("101")], fail_page=1)
        with self.assertRaises(ConnectionError):
            api.crawl(self.config, "resumable", client)
        resumed = FixtureAPI([review(), review("101")])
        api.crawl(self.config, "resumable", resumed)
        self.assertEqual(len(resumed.calls), 1)
        self.assertEqual(resumed.calls[0][1]["cursor"], "1")

    def test_invalid_review_quarantine_and_duplicate_reconciliation(self):
        invalid = review("101")
        invalid["timestamp_created"] = "not a unix timestamp"
        result = self.run_poll("invalid", [review(), invalid])
        self.assertEqual(result["counts"]["rejected_reviews"], 1)
        self.assertEqual(result["counts"]["reviews_in_snapshot"], 1)
        duplicate = self.run_poll("duplicate", [review(), review()])
        self.assertEqual(duplicate["counts"]["duplicate_review_rows"], 1)

    def test_tampered_bronze_prevents_outbox_commit(self):
        api.crawl(self.config, "corrupt", FixtureAPI([review(), review("101")]))
        p.clean("corrupt")
        p.aggregate("corrupt")
        path = storage.DATA / storage.read_manifest("corrupt")["catalog"]["path"]
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "Quality gate failed"):
            p.verify("corrupt")
        with storage.database() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)

    def test_publish_failure_keeps_outbox_pending(self):
        self.run_poll("first", [review(), review("101")])

        class FailingProducer:
            def send(self, *args, **kwargs):
                raise ConnectionError("Broker unavailable")

        with self.assertRaises(ConnectionError):
            stream.publish("unused", producer=FailingProducer())
        with storage.database() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0], 2
            )

    def test_publish_ack_failure_replays_the_uncommitted_batch(self):
        self.run_poll("first", [review(), review("101")])
        failed_ack = Mock()
        failed_ack.get.side_effect = TimeoutError("No ACK")
        producer = Mock()
        producer.send.side_effect = [Mock(), failed_ack]

        with self.assertRaises(TimeoutError):
            stream.publish("unused", producer=producer)
        first_attempt = producer.send.call_args_list[:]
        with storage.database() as db:
            pending = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
        self.assertEqual(pending, 2)

        producer.reset_mock(side_effect=True)
        result = stream.publish("unused", producer=producer)
        self.assertEqual((result["published"], result["pending"]), (2, 0))
        self.assertEqual(producer.send.call_args_list, first_attempt)

    def test_atomic_write_retries_a_transient_sharing_violation(self):
        destination = self.root / "manifest.json"
        storage.atomic_json(destination, {"state": "before"})
        replace = storage.os.replace

        def busy_then_replace(temporary, target):
            if busy.call_count == 1:
                raise PermissionError("File temporarily in use")
            return replace(temporary, target)

        with patch.object(storage.os, "replace", side_effect=busy_then_replace) as busy:
            storage.atomic_json(destination, {"state": "after"})
        self.assertEqual(busy.call_count, 2)
        self.assertEqual(storage.read_json(destination), {"state": "after"})

    def test_backfill_export_counts_only_reviews_from_the_selected_crawl(self):
        self.run_poll("earlier", [review("98"), review("99")])
        self.run_poll("backfill", [review(), review("101", True)])
        destination = storage.DATA / "exports/backfill"
        summary = backfill.export_dataset("backfill", 2, destination)
        self.assertEqual(summary["total_exported"], 2)
        table = pq.read_table(destination / "reviews.parquet", columns=["review_id"])
        self.assertEqual(set(table["review_id"].to_pylist()), {"100", "101"})
        with self.assertRaisesRegex(ValueError, "need 3"):
            backfill.export_dataset("backfill", 3, destination)

    def test_dashboard_reads_failed_step_and_preserves_checkpoint_summary(self):
        client = FixtureAPI([review(), review("101")], fail_page=1)
        with (
            patch("builtins.print"),
            self.assertRaises(ConnectionError),
        ):
            telemetry.run_step("demo", "crawl", api.crawl, self.config, "demo", client)

        result = dashboard.status("demo")
        self.assertEqual(result["steps"]["crawl"]["status"], "failed")
        self.assertIn("Injected page failure", result["steps"]["crawl"]["error"])
        self.assertEqual(result["games"][0]["rows"], 1)
        self.assertEqual(result["games"][0]["title"], "Test game")
        self.assertNotIn("next_cursor", result["games"][0])

    def test_consumer_commits_after_persisting_receipt_and_quarantine(self):
        self.run_poll("first", [review(), review("101")])
        with storage.database() as db:
            payload = db.execute("SELECT payload_json FROM outbox LIMIT 1").fetchone()[0].encode()
        messages = [
            Mock(topic=stream.TOPIC, partition=0, offset=0, value=payload),
            Mock(topic=stream.TOPIC, partition=0, offset=1, value=b"broken JSON"),
        ]
        consumer = Mock()
        consumer.poll.return_value = {0: messages}

        def check_commit():
            self.assertTrue((storage.DATA / "bronze/kafka" / stream.TOPIC / "0/0.json").is_file())
            self.assertTrue(
                (storage.DATA / "quarantine/kafka" / stream.TOPIC / "0/1.json").is_file()
            )

        consumer.commit.side_effect = check_commit
        with patch.object(stream, "KafkaConsumer", return_value=consumer):
            result = stream.consume("unused", count=2, export=False)
        consumer.commit.assert_called_once()
        self.assertEqual((result["received"], result["accepted"], result["quarantined"]), (2, 1, 1))

    def test_duplicate_kafka_delivery_dedups_and_preserves_updated_review(self):
        self.run_poll("first", [review(), review("101")])
        self.run_poll("updated", [review(updated=1700001000, message="Changed"), review("101")])
        with storage.database() as db:
            events = [
                json.loads(value[0])
                for value in db.execute("SELECT payload_json FROM outbox ORDER BY sequence")
            ]
        for offset, event in enumerate(events + [events[0]]):
            storage.atomic_json(
                storage.DATA / "bronze/kafka" / stream.TOPIC / "0" / f"{offset}.json",
                {"payload": event},
            )
        result = stream.export_events()
        self.assertEqual(
            (
                result["bronze_receipts"],
                result["unique_review_versions"],
                result["latest_review_ids"],
            ),
            (4, 3, 2),
        )
        rows = pq.read_table(storage.DATA / "silver/stream/reviews_latest.parquet").to_pylist()
        self.assertEqual(rows[0]["review_text"], "Changed")
        bad = dict(events[0], source_mode="invalid_mode")
        with self.assertRaises(ValueError):
            stream.validate_event(bad)

    def test_http_429_is_retried_and_uses_input_json(self):
        from io import BytesIO
        from urllib.error import HTTPError

        response = unittest.mock.MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = b'{"response":{"reviews":[]}}'
        response.headers = {}
        response.status = 200
        config = api.load_config(self.config)
        error = HTTPError(api.REVIEWS_URL, 429, "rate limit", {"Retry-After": "0"}, BytesIO())
        with patch.object(api, "urlopen", side_effect=[error, response]) as opener:
            raw, metadata = api.SteamClient(config).get(api.REVIEWS_URL, {"appid": 10})
        self.assertEqual(metadata["attempts"], 2)
        self.assertIn("input_json=", opener.call_args.args[0].full_url)

    def test_wrong_optional_field_type_cannot_poison_the_stream(self):
        self.run_poll("first", [review(), review("101")])
        with storage.database() as db:
            event = json.loads(db.execute("SELECT payload_json FROM outbox LIMIT 1").fetchone()[0])
        event["review"]["votes_up"] = "not an integer"
        with self.assertRaisesRegex(ValueError, "votes_up"):
            stream.validate_event(event)

    def test_verifying_an_old_run_does_not_rewind_latest_snapshot(self):
        self.run_poll("first", [review(), review("101")])
        self.run_poll("second", [review("101"), review("102")])
        p.verify("first")
        self.assertEqual(storage.read_json(storage.DATA / "latest.json")["run_id"], "second")
        api.crawl(self.config, "third", FixtureAPI([review(), review("102")]))
        self.assertEqual(storage.read_manifest("third")["base_run_id"], "second")

    def test_a_stale_crawl_cannot_replace_a_more_recent_commit(self):
        from src.state import StaleSnapshotError

        api.crawl(self.config, "stale", FixtureAPI([review(), review("101")]))
        self.run_poll("fresh", [review(), review("102")])
        p.clean("stale")
        p.aggregate("stale")
        with self.assertRaises(StaleSnapshotError):
            p.verify("stale")
        self.assertEqual(storage.read_json(storage.DATA / "latest.json")["run_id"], "fresh")
        with storage.database() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 2)

    def test_malformed_review_objects_are_quarantined(self):
        malformed_author = review("101")
        malformed_author["author"] = ["not an object"]
        for index, malformed in enumerate((None, malformed_author)):
            with self.subTest(malformed=malformed):
                result = self.run_poll(f"bad-{index}", [review(), malformed])
                self.assertEqual(result["counts"]["rejected_reviews"], 1)
                self.assertEqual(result["counts"]["reviews_in_snapshot"], 1)

    def test_config_rejects_booleans_and_noninteger_limits(self):
        original = json.loads(self.config.read_text())
        for field, value in (
            ("page_size", True),
            ("max_reviews_per_game", 1.5),
            ("timeout_seconds", False),
        ):
            with self.subTest(field=field, value=value):
                self.config.write_text(json.dumps({**original, field: value}))
                with self.assertRaises(ValueError):
                    api.load_config(self.config)

    def test_retry_after_supports_http_dates_and_long_delays(self):
        from datetime import datetime, timezone

        current = datetime(2026, 11, 2, tzinfo=timezone.utc)
        with patch.object(storage, "now", return_value=current):
            self.assertEqual(api.retry_delay("Mon, 02 Nov 2026 00:05:00 GMT", 0), 300)
        self.assertEqual(api.retry_delay("300", 0), 300)
        self.assertEqual(api.retry_delay("invalid", 1), 2)

    def test_incomplete_measurements_do_not_overwrite_a_verified_report(self):
        from src.reporting import ReportNotReadyError, write_report

        destination = self.root / "reports/VERIFICATION.md"
        destination.parent.mkdir()
        destination.write_text("previous verified report", encoding="utf-8")
        with self.assertRaises(ReportNotReadyError):
            write_report({"checks": {"no_pending_outbox": False}}, {}, {})
        self.assertEqual(destination.read_text(encoding="utf-8"), "previous verified report")


if __name__ == "__main__":
    unittest.main()
