"""
STREAMING TRACK — Step 2/3
dag_stream_silver: Bronze raw_fraud_events → Silver stg_fraud_events

Cleans and deduplicates streaming events from Bronze.
Triggered every 5 minutes to maintain near-real-time Silver freshness.
Dedup key: event_id (merge/upsert — idempotent).

Schedule: */5 * * * *
Source:   s3://bronze/raw_fraud_events  (Delta Lake)
Sink:     s3://silver/stg_fraud_events  (Delta Lake, deduped)
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

bronze_events = Asset("s3://bronze/raw_fraud_events")
silver_events = Asset("s3://silver/stg_fraud_events")


@dag(
    dag_id="stream_silver",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "silver"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
)
def stream_silver_dag():

    @task(inlets=[bronze_events], outlets=[silver_events])
    def clean_events():
        from src.pipelines.silver.clean_events import run
        run()

    clean_events()


stream_silver_dag()
