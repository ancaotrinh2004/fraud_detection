"""Streaming feature DAG — every 5 min."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_events    = Asset("s3://silver/stg_fraud_events")
feat_stream_30m  = Asset(f"{_PG}/feat_stream_30m")


@dag(
    dag_id="feat_stream_30m",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["features", "streaming", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def feat_stream_30m_dag():

    @task(inlets=[silver_events], outlets=[feat_stream_30m])
    def compute_feat_stream_30m():
        from src.pipelines.features.feat_pipelines import run_feat_stream_30m
        run_feat_stream_30m()

    compute_feat_stream_30m()


feat_stream_30m_dag()
