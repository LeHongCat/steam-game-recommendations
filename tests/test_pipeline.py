import gzip
import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from src import pipeline, streaming


class PipelineBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.patch_pipeline = patch.object(pipeline, "ROOT", self.root)
        self.patch_streaming = patch.object(streaming, "ROOT", self.root)
        self.patch_pipeline.start()
        self.patch_streaming.start()
        self.source = self.root / "source"
        self.source.mkdir()
        review = {"item_id": "10", "recommend": True, "review": "Fun gameplay", "posted": "Posted January 1, 2015."}
        negative = {"item_id": "10", "recommend": False, "review": "Bugs", "posted": "Posted January 2."}
        orphan = {"item_id": "999", "recommend": True, "review": "Unknown game", "posted": "Posted January 2, 2015."}
        self.write_source("reviews", [{"user_id": "user-a", "reviews": [review, review, negative, orphan]}])
        self.write_source("games", [{"id": "10", "title": "Game", "tags": ["Action"]}] * 2)

    def tearDown(self):
        self.patch_streaming.stop()
        self.patch_pipeline.stop()
        self.temporary.cleanup()

    def write_source(self, kind, rows):
        with gzip.open(self.source / pipeline.SOURCES[kind][0], "wt", encoding="utf-8") as output:
            output.write("\n".join(repr(row) for row in rows))

    def run_pipeline(self):
        snapshot = pipeline.ingest(limit=0, source_dir=self.source)
        pipeline.clean(snapshot)
        pipeline.aggregate(snapshot)
        return snapshot, pipeline.verify(snapshot)

    def test_reconcile_rejects_duplicates_negative_feedback_and_unknown_date(self):
        snapshot, result = self.run_pipeline()
        self.assertEqual(result["counts"]["reviews"], 2)
        self.assertEqual(result["counts"]["duplicates_collapsed"], 1)
        self.assertEqual(result["counts"]["rejected_reviews"], 1)
        self.assertEqual(result["counts"]["missing_review_time"], 1)
        self.assertEqual(result["counts"]["game_duplicates_collapsed"], 1)
        stats = pq.read_table(self.root / "data/gold" / snapshot / "game_statistics.parquet").to_pylist()[0]
        self.assertEqual((stats["positive_count"], stats["negative_count"]), (1, 1))

    def test_repeated_batch_has_stable_snapshot_and_one_metadata_row(self):
        first, _ = self.run_pipeline()
        second, _ = self.run_pipeline()
        self.assertEqual(first, second)
        with sqlite3.connect(self.root / "data/metadata.sqlite") as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1)

    def test_modified_bronze_is_detected(self):
        snapshot, _ = self.run_pipeline()
        path = self.root / pipeline.manifest(snapshot)["files"]["reviews"]["path"]
        with path.open("ab") as file:
            file.write(b"corruption")
        with self.assertRaisesRegex(ValueError, "integrity"):
            pipeline.verify(snapshot)

    def test_replayed_event_dedup_and_schema_guard(self):
        event = {"schema_version": 1, "event_id": str(uuid.uuid4()), "review_id": "r1", "user_id": "u1",
                 "item_id": "10", "recommended": False, "event_time": None,
                 "produced_at": "2026-11-02T00:00:00+00:00", "source_mode": "replay"}
        for offset in (0, 1):
            pipeline.atomic_json(self.root / "data/bronze/events" / streaming.TOPIC / "0" / f"{offset}.json", {"payload": event})
        result = streaming.export()
        self.assertEqual(result["bronze_receipts"], 2)
        self.assertEqual(result["silver_unique_events"], 1)
        malformed = dict(event, recommended="false")
        with self.assertRaisesRegex(ValueError, "boolean"):
            streaming.validate(malformed)
        malformed = dict(event)
        del malformed["event_time"]
        with self.assertRaisesRegex(ValueError, "fields"):
            streaming.validate(malformed)


if __name__ == "__main__":
    unittest.main()
