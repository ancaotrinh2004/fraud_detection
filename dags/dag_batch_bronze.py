"""
BATCH TRACK — Step 1/4
dag_batch_bronze: S3 raw files → Delta Lake Bronze

Ingests all offline data sources from S3 raw zone into Bronze Delta Lake.
Incremental: reads only new records since last watermark (created_ts).

  reference:    customers / merchants / cards  (full reload — small tables)
  transactions: partitioned parquet, incremental by created_ts

Schedule: */30 * * * *
Source:   s3://raw/offline/{customers,merchants,cards,transactions}
Sink:     s3://bronze/{raw_customers,raw_merchants,raw_cards,raw_transactions}
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

raw_customers    = Asset("s3://raw/offline/customers.parquet")
raw_merchants    = Asset("s3://raw/offline/merchants.parquet")
raw_cards        = Asset("s3://raw/offline/cards.parquet")
raw_transactions = Asset("s3://raw/offline/transactions")

bronze_customers    = Asset("s3://bronze/raw_customers")
bronze_merchants    = Asset("s3://bronze/raw_merchants")
bronze_cards        = Asset("s3://bronze/raw_cards")
bronze_transactions = Asset("s3://bronze/raw_transactions")


@dag(
    dag_id="batch_bronze",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "bronze", "ingestion"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=32),
    },
)
def batch_bronze_dag():

    @task(inlets=[raw_customers, raw_merchants, raw_cards],
          outlets=[bronze_customers, bronze_merchants, bronze_cards])
    def ingest_reference():
        from src.pipelines.bronze.ingest_reference import run
        run()

    @task(inlets=[raw_transactions], outlets=[bronze_transactions])
    def ingest_transactions():
        from src.pipelines.bronze.ingest_transactions import run
        run()

    ingest_reference() >> ingest_transactions()


batch_bronze_dag()
