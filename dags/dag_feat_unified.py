"""
MERGE POINT — Streaming ∪ Batch
dag_feat_unified: feat_customer_90d + feat_stream_30m → feat_customer_unified

Joins the two feature tracks into a single point-in-time correct feature
table used by the inference service and the ML training pipeline.

  Batch track  →  feat_customer_90d   (updated hourly)
  Stream track →  feat_stream_30m     (updated every 5 min)
                        │                       │
                        └───────────┬───────────┘
                                    ▼
                          feat_customer_unified
                          (updated every 15 min)

The join is always-latest: each run upserts the most recent snapshot
for every customer_id. The inference service queries this table at
prediction time to fetch live features.

Schedule: */15 * * * *
Source:   postgres/gold_fraud/feat_customer_90d
          postgres/gold_fraud/feat_stream_30m
Sink:     postgres/gold_fraud/feat_customer_unified
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

feat_customer_90d = Asset(f"{_PG}/feat_customer_90d")
feat_stream_30m   = Asset(f"{_PG}/feat_stream_30m")
feat_unified      = Asset(f"{_PG}/feat_customer_unified")


@dag(
    dag_id="feat_unified",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["features", "merge", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def feat_unified_dag():

    @task(inlets=[feat_customer_90d, feat_stream_30m], outlets=[feat_unified])
    def merge_features():
        from src.pipelines.features.feat_pipelines import run_feat_unified
        run_feat_unified()

    merge_features()


feat_unified_dag()
