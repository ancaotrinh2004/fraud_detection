"""Bronze ingestion DAG — every 30 min."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

# ── Asset definitions ─────────────────────────────────────────────────────────
raw_customers    = Asset("s3://raw/offline/customers.parquet")
raw_merchants    = Asset("s3://raw/offline/merchants.parquet")
raw_cards        = Asset("s3://raw/offline/cards.parquet")
raw_transactions = Asset("s3://raw/offline/transactions")
raw_events       = Asset("s3://raw/streaming/fraud_events.json")

bronze_customers    = Asset("s3://bronze/raw_customers")
bronze_merchants    = Asset("s3://bronze/raw_merchants")
bronze_cards        = Asset("s3://bronze/raw_cards")
bronze_transactions = Asset("s3://bronze/raw_transactions")
bronze_events       = Asset("s3://bronze/raw_fraud_events")


@dag(
    dag_id="bronze_ingest",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=32),
    },
)
def bronze_ingest():

    @task(inlets=[raw_customers, raw_merchants, raw_cards],
          outlets=[bronze_customers, bronze_merchants, bronze_cards])
    def ingest_reference():
        from src.pipelines.bronze.ingest_reference import run
        run()

    @task(inlets=[raw_transactions], outlets=[bronze_transactions])
    def ingest_transactions():
        from src.pipelines.bronze.ingest_transactions import run
        run()

    @task(inlets=[raw_events], outlets=[bronze_events])
    def ingest_events():
        from src.pipelines.bronze.ingest_events import run
        run()

    ref = ingest_reference()
    ref >> [ingest_transactions(), ingest_events()]


bronze_ingest()
