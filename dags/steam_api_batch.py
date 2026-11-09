"""Poll live Steam APIs hourly, verify storage, and publish changed review versions."""

import uuid
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="steam_api_batch",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["steam", "sources", "ingestion", "storage"],
)
def steam_api_batch():
    @task
    def crawl_steam():
        from src.steam_api import crawl
        from src.telemetry import run_step

        run_id = "airflow-" + uuid.uuid5(uuid.NAMESPACE_URL, get_current_context()["run_id"]).hex
        return run_step(run_id, "crawl", crawl, None, run_id)

    @task
    def clean_silver(snapshot_run):
        from src.pipeline import clean
        from src.telemetry import run_step

        return run_step(snapshot_run, "clean", clean, snapshot_run)

    @task
    def build_gold(snapshot_run):
        from src.pipeline import aggregate
        from src.telemetry import run_step

        return run_step(snapshot_run, "aggregate", aggregate, snapshot_run)

    @task
    def validate_snapshot(snapshot_run):
        from src.pipeline import verify
        from src.telemetry import run_step

        return run_step(snapshot_run, "verify_batch", verify, snapshot_run)

    @task
    def publish_changes(verification):
        import os

        from src.streaming import publish
        from src.telemetry import run_step

        return run_step(
            verification["run_id"], "publish", publish, os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        )

    publish_changes(validate_snapshot(build_gold(clean_silver(crawl_steam()))))


steam_api_batch()
