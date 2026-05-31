"""Silver transformation DAG — every 30 min."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

# ── Asset definitions ─────────────────────────────────────────────────────────
bronze_customers    = Asset("s3://bronze/raw_customers")
bronze_merchants    = Asset("s3://bronze/raw_merchants")
bronze_cards        = Asset("s3://bronze/raw_cards")
bronze_transactions = Asset("s3://bronze/raw_transactions")
bronze_events       = Asset("s3://bronze/raw_fraud_events")

silver_customers    = Asset("s3://silver/stg_customers")
silver_merchants    = Asset("s3://silver/stg_merchants")
silver_cards        = Asset("s3://silver/stg_cards")
silver_transactions = Asset("s3://silver/stg_transactions")
silver_events       = Asset("s3://silver/stg_fraud_events")


@dag(
    dag_id="silver_transform",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transform"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def silver_transform():

    @task(inlets=[bronze_customers, bronze_merchants, bronze_cards],
          outlets=[silver_customers, silver_merchants, silver_cards])
    def clean_reference():
        from src.pipelines.silver.clean_reference import run
        run()

    @task(inlets=[bronze_transactions], outlets=[silver_transactions])
    def clean_transactions():
        from src.pipelines.silver.clean_transactions import run
        run()

    @task(inlets=[bronze_events], outlets=[silver_events])
    def clean_events():
        from src.pipelines.silver.clean_events import run
        run()

    ref = clean_reference()
    txn = clean_transactions()
    evt = clean_events()
    ref >> txn >> evt


silver_transform()
