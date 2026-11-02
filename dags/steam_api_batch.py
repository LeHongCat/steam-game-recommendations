"""Poll live Steam APIs hourly, verify storage, and publish changed review versions."""
import uuid
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context


@dag(dag_id="steam_api_batch", schedule="@hourly", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     catchup=False, max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
     tags=["steam", "sources", "ingestion", "storage"])
def steam_api_batch():
    @task
    def crawl_steam():
        from src.steam_api import crawl
        run_id = "airflow-" + uuid.uuid5(uuid.NAMESPACE_URL, get_current_context()["run_id"]).hex
        return crawl(run_id=run_id)

    @task
    def silver(snapshot_id):
        from src.pipeline import clean
        return clean(snapshot_id)

    @task
    def gold(snapshot_id):
        from src.pipeline import aggregate
        return aggregate(snapshot_id)

    @task
    def quality_gate(snapshot_id):
        from src.pipeline import verify
        return verify(snapshot_id)

    @task
    def publish_review_changes(result):
        import os
        from src.streaming import publish
        return publish(os.environ["KAFKA_BOOTSTRAP_SERVERS"])

    publish_review_changes(quality_gate(gold(silver(crawl_steam()))))


steam_api_batch()
