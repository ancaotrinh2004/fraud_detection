"""
STREAMING TRACK — Step 1/3  (CDC + Spark architecture, Parts A+B)
dag_stream_bronze: Bronze freshness monitor.

Bronze ingestion is now CONTINUOUS and runs OUTSIDE Airflow:

    product app → oltp.transaction_events (Postgres)
        → Debezium CDC → Kafka fraud.events.raw
        → Spark Structured Streaming (Deployment fraud-spark-bronze)
        → Delta s3://bronze/raw_fraud_events

So Airflow no longer pulls Kafka in a 5-min micro-batch. Instead this DAG just
verifies the streaming engine is alive by checking Bronze freshness: it reads
max(ingest_ts) from the Bronze table and fails (→ alert) if data has gone stale
beyond the SLA, signalling the Spark/Debezium pipeline has stalled.

Schedule: */10 * * * *
Checks:   max(ingest_ts) on s3://bronze/raw_fraud_events vs sla.bronze_freshness_min
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

bronze_events = Asset("s3://bronze/raw_fraud_events")


@dag(
    dag_id="stream_bronze",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "bronze", "monitor", "cdc", "spark"],
    default_args={
        "owner": "fraud-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def stream_bronze_dag():

    @task(inlets=[bronze_events])
    def check_bronze_freshness():
        """Fail if the continuous Spark→Bronze stream has gone stale (alert signal)."""
        from datetime import datetime as dt, timezone
        import pandas as pd
        from src.pipelines.utils.db import load_pipeline_config
        from src.pipelines.utils.delta import get_last_ingest_ts, get_storage_options

        cfg = load_pipeline_config()
        table_path = f"{cfg['bronze_base_path']}/raw_fraud_events"
        sla_min = cfg.get("sla", {}).get("bronze_freshness_min", 10)

        last_ts = get_last_ingest_ts(table_path, storage_options=get_storage_options(cfg))
        if last_ts is None:
            raise ValueError(
                "Bronze raw_fraud_events has no data — is the fraud-spark-bronze "
                "Deployment running and is Debezium producing to fraud.events.raw?"
            )

        last_ts = pd.Timestamp(last_ts).tz_localize(None)
        age_min = (dt.now(timezone.utc).replace(tzinfo=None) - last_ts).total_seconds() / 60
        print(f"[stream_bronze] last ingest_ts={last_ts} (age={age_min:.1f} min, SLA={sla_min})")
        if age_min > sla_min:
            raise ValueError(
                f"Bronze is STALE: last ingest {age_min:.1f} min ago > SLA {sla_min} min. "
                "Check the Spark streaming Deployment and Debezium connector."
            )

    check_bronze_freshness()


stream_bronze_dag()
