"""Unified feature DAG — every 15 min.

Reads from feat_customer_90d and feat_stream_30m tables (already populated by
their respective DAGs) and joins them into feat_customer_unified.
No ExternalTaskSensor needed — tables always hold latest data from prior runs.
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

feat_customer_90d  = Asset(f"{_PG}/feat_customer_90d")
feat_stream_30m    = Asset(f"{_PG}/feat_stream_30m")
feat_unified       = Asset(f"{_PG}/feat_customer_unified")


@dag(
    dag_id="feat_unified",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["features", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def feat_unified_dag():

    @task(inlets=[feat_customer_90d, feat_stream_30m], outlets=[feat_unified])
    def feat_customer_unified():
        from src.pipelines.features.feat_pipelines import run_feat_unified
        run_feat_unified()

    feat_customer_unified()


feat_unified_dag()
