"""Gold modeling DAG — every 30 min."""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

# ── Asset definitions ─────────────────────────────────────────────────────────
_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_customers    = Asset("s3://silver/stg_customers")
silver_merchants    = Asset("s3://silver/stg_merchants")
silver_cards        = Asset("s3://silver/stg_cards")
silver_transactions = Asset("s3://silver/stg_transactions")
silver_events       = Asset("s3://silver/stg_fraud_events")

dim_customer   = Asset(f"{_PG}/dim_customer")
dim_merchant   = Asset(f"{_PG}/dim_merchant")
dim_card       = Asset(f"{_PG}/dim_card")
dim_date       = Asset(f"{_PG}/dim_date")
dim_status     = Asset(f"{_PG}/dim_transaction_status")

fact_txn       = Asset(f"{_PG}/fact_transaction")
fact_event     = Asset(f"{_PG}/fact_fraud_event")
obt_fraud      = Asset(f"{_PG}/obt_transaction_fraud_summary")


@dag(
    dag_id="gold_model",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "modeling"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def gold_model():

    @task(inlets=[silver_customers, silver_merchants, silver_cards, silver_transactions],
          outlets=[dim_customer, dim_merchant, dim_card, dim_date, dim_status])
    def dim_load():
        from src.pipelines.gold.dim_load import run
        run()

    @task(inlets=[silver_transactions, dim_customer, dim_card, dim_merchant, dim_status],
          outlets=[fact_txn])
    def fact_transaction():
        from src.pipelines.gold.fact_transaction import run
        run()

    @task(inlets=[silver_events, dim_customer, dim_card, dim_merchant],
          outlets=[fact_event])
    def fact_fraud_event():
        from src.pipelines.gold.fact_fraud_event import run
        run()

    @task(inlets=[fact_txn, dim_customer, dim_merchant, dim_card, dim_status],
          outlets=[obt_fraud])
    def obt_fraud_task():
        from src.pipelines.gold.obt_transaction_fraud_summary import run
        run()

    dims = dim_load()
    t_txn = fact_transaction()
    t_evt = fact_fraud_event()
    dims >> [t_txn, t_evt]
    t_txn >> obt_fraud_task()


gold_model()
