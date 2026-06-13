"""
BATCH TRACK — Step 2/4
dag_batch_silver: Bronze → Delta Lake Silver (batch sources)

Cleans, normalises, and deduplicates batch data from Bronze.
Applies schema validation, null checks, and type coercion.
Dedup keys:  customer_id / merchant_id / card_id / transaction_id

Schedule: */30 * * * *
Source:   s3://bronze/{raw_customers,raw_merchants,raw_cards,raw_transactions}
Sink:     s3://silver/{stg_customers,stg_merchants,stg_cards,stg_transactions}
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

bronze_customers    = Asset("s3://bronze/raw_customers")
bronze_merchants    = Asset("s3://bronze/raw_merchants")
bronze_cards        = Asset("s3://bronze/raw_cards")
bronze_transactions = Asset("s3://bronze/raw_transactions")

silver_customers    = Asset("s3://silver/stg_customers")
silver_merchants    = Asset("s3://silver/stg_merchants")
silver_cards        = Asset("s3://silver/stg_cards")
silver_transactions = Asset("s3://silver/stg_transactions")


@dag(
    dag_id="batch_silver",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "silver", "transform"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=32),
    },
)
def batch_silver_dag():

    @task(inlets=[bronze_customers, bronze_merchants, bronze_cards],
          outlets=[silver_customers, silver_merchants, silver_cards])
    def clean_reference():
        from src.pipelines.silver.clean_reference import run
        run()

    @task(inlets=[bronze_transactions], outlets=[silver_transactions])
    def clean_transactions():
        from src.pipelines.silver.clean_transactions import run
        run()

    clean_reference() >> clean_transactions()


batch_silver_dag()
