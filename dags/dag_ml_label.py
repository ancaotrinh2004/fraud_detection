"""ML label + training table DAG — runs daily after gold_model."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

fact_txn        = Asset(f"{_PG}/fact_transaction")
dim_customer    = Asset(f"{_PG}/dim_customer")
feat_unified    = Asset(f"{_PG}/feat_customer_unified")
dim_card        = Asset(f"{_PG}/dim_card")

ml_label        = Asset(f"{_PG}/ml_fraud_label")
ml_training     = Asset(f"{_PG}/ml_fraud_training")


@dag(
    dag_id="ml_label",
    schedule="0 7 * * *",
    start_date=datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "label"],
    default_args={
        "owner": "fraud-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def ml_label_dag():

    @task(inlets=[fact_txn, dim_customer], outlets=[ml_label])
    def build_label():
        from src.pipelines.features.ml_label import run_ml_label
        run_ml_label()

    @task(inlets=[ml_label, feat_unified, fact_txn, dim_card], outlets=[ml_training])
    def build_training():
        from src.pipelines.features.ml_label import run_ml_training
        run_ml_training()

    build_label() >> build_training()


ml_label_dag()
