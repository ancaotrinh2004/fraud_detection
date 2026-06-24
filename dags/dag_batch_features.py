"""
BATCH TRACK — Step 4/4
dag_batch_features: Silver transactions → feat_customer_90d

Computes 90-day rolling offline features per customer:
  - f_customer_total_txn_90d           (transaction count)
  - f_customer_avg_txn_amount_90d      (average amount)
  - f_customer_distinct_merchants_90d  (merchant diversity)
  - f_customer_decline_rate_90d        (decline signal)
  - f_customer_foreign_txn_ratio_90d   (foreign transaction ratio)
  - f_customer_night_txn_ratio_90d     (night-time behaviour)

These features represent the stable customer behaviour profile used by
the inference service alongside real-time streaming features.

Schedule: 0 * * * *  (hourly — 90-day windows don't need sub-hourly refresh)
Source:   s3://silver/stg_transactions  +  postgres/gold_fraud/dim_card
Sink:     postgres/gold_fraud/feat_customer_90d
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_transactions = Asset("s3://silver/stg_transactions")
dim_card            = Asset(f"{_PG}/dim_card")
feat_customer_90d   = Asset(f"{_PG}/feat_customer_90d")


@dag(
    dag_id="batch_features",
    schedule="0 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "features", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
)
def batch_features_dag():

    @task(inlets=[silver_transactions, dim_card], outlets=[feat_customer_90d])
    def compute_customer_features():
        from src.pipelines.features.feat_pipelines import run_feat_customer_90d
        run_feat_customer_90d()

    compute_customer_features()


batch_features_dag()
