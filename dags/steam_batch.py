"""Daily batch ingestion. Only small snapshot identifiers are sent through XCom."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(dag_id="steam_batch", schedule="@daily", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     catchup=False, max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
     tags=["steam", "sources", "ingestion", "storage"])
def steam_batch():
    @task
    def ingest_sources():
        from src.pipeline import ingest
        return ingest(limit=5000)

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

    quality_gate(gold(silver(ingest_sources())))


steam_batch()
