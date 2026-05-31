"""Offline customer feature DAG — every 60 min."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_transactions = Asset("s3://silver/stg_transactions")
dim_card            = Asset(f"{_PG}/dim_card")
feat_customer_90d   = Asset(f"{_PG}/feat_customer_90d")


@dag(
    dag_id="feat_customer_90d",
    schedule="0 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["features", "offline", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def feat_customer_90d_dag():

    @task(inlets=[silver_transactions, dim_card], outlets=[feat_customer_90d])
    def compute_feat_customer_90d():
        from src.pipelines.features.feat_pipelines import run_feat_customer_90d
        run_feat_customer_90d()

    compute_feat_customer_90d()


feat_customer_90d_dag()
