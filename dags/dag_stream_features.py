"""
STREAMING TRACK — Step 3/3
dag_stream_features: Silver stg_fraud_events → feat_stream_30m

Computes near-real-time streaming features over a 30-minute sliding window:
  - f_stream_otp_failed_count_30m   (fraud signal: failed OTP attempts)
  - f_stream_decline_count_30m      (fraud signal: transaction declines)
  - f_stream_txn_velocity_1h        (velocity: transaction count in 1 hour)
  - f_stream_new_merchant_flag      (first time seeing this merchant)
  - f_stream_burst_activity_flag    (activity spike vs baseline)

These features are the primary real-time fraud signals consumed by the
inference service (FraudModel.predict) and the ML training pipeline.

Schedule: */5 * * * *
Source:   s3://silver/stg_fraud_events     (Delta Lake)
Sink:     postgres/gold_fraud/feat_stream_30m
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_events   = Asset("s3://silver/stg_fraud_events")
feat_stream_30m = Asset(f"{_PG}/feat_stream_30m")


@dag(
    dag_id="stream_features",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "features", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
)
def stream_features_dag():

    @task(inlets=[silver_events], outlets=[feat_stream_30m])
    def compute_stream_features():
        from src.pipelines.features.feat_pipelines import run_feat_stream_30m
        run_feat_stream_30m()

    compute_stream_features()


stream_features_dag()
