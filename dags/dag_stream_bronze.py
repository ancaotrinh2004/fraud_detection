"""
STREAMING TRACK — Step 1/3
dag_stream_bronze: Kafka → Delta Lake Bronze

Consumes fraud.events.raw topic and appends to Bronze raw_fraud_events.
Runs every 5 minutes. Commits Kafka offsets only after successful write.

Schedule: */5 * * * *
Source:   Kafka topic fraud.events.raw  (consumer group: bronze-ingest-events)
Sink:     s3://bronze/raw_fraud_events  (Delta Lake, partitioned by event_date)
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

bronze_events = Asset("s3://bronze/raw_fraud_events")


@dag(
    dag_id="stream_bronze",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "bronze", "kafka"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
)
def stream_bronze_dag():

    @task(outlets=[bronze_events])
    def consume_kafka():
        from src.pipelines.bronze.kafka_consumer import run
        from src.pipelines.utils.db import load_pipeline_config
        cfg = load_pipeline_config()
        run(cfg)

    consume_kafka()


stream_bronze_dag()
