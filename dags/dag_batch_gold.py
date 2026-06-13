"""
BATCH TRACK — Step 3/4
dag_batch_gold: Silver → Gold (Dimensional Model + OBT)

Builds the analytical Gold layer from cleaned Silver data:
  dims:     dim_customer, dim_merchant, dim_card, dim_date, dim_transaction_status
  facts:    fact_transaction, fact_fraud_event
  obt:      obt_transaction_fraud_summary  (wide denormalised table for BI/ML)

Gold is the authoritative source for ML label generation and BI dashboards.

Schedule: */30 * * * *
Source:   s3://silver/{stg_customers,stg_merchants,stg_cards,stg_transactions,stg_fraud_events}
Sink:     postgres/gold_fraud/{dim_*,fact_*,obt_*}
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pendulum import datetime

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

silver_customers    = Asset("s3://silver/stg_customers")
silver_merchants    = Asset("s3://silver/stg_merchants")
silver_cards        = Asset("s3://silver/stg_cards")
silver_transactions = Asset("s3://silver/stg_transactions")
silver_events       = Asset("s3://silver/stg_fraud_events")

dim_customer = Asset(f"{_PG}/dim_customer")
dim_merchant = Asset(f"{_PG}/dim_merchant")
dim_card     = Asset(f"{_PG}/dim_card")
dim_date     = Asset(f"{_PG}/dim_date")
dim_status   = Asset(f"{_PG}/dim_transaction_status")
fact_txn     = Asset(f"{_PG}/fact_transaction")
fact_event   = Asset(f"{_PG}/fact_fraud_event")
obt_fraud    = Asset(f"{_PG}/obt_transaction_fraud_summary")


@dag(
    dag_id="batch_gold",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "gold", "modeling"],
    default_args={
        "owner": "fraud-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=32),
    },
)
def batch_gold_dag():

    @task(inlets=[silver_customers, silver_merchants, silver_cards, silver_transactions],
          outlets=[dim_customer, dim_merchant, dim_card, dim_date, dim_status])
    def load_dims():
        from src.pipelines.gold.dim_load import run
        run()

    @task(inlets=[silver_transactions, dim_customer, dim_card, dim_merchant, dim_status],
          outlets=[fact_txn])
    def load_fact_transaction():
        from src.pipelines.gold.fact_transaction import run
        run()

    @task(inlets=[silver_events, dim_customer, dim_card, dim_merchant],
          outlets=[fact_event])
    def load_fact_event():
        from src.pipelines.gold.fact_fraud_event import run
        run()

    @task(inlets=[fact_txn, dim_customer, dim_merchant, dim_card, dim_status],
          outlets=[obt_fraud])
    def load_obt():
        from src.pipelines.gold.obt_transaction_fraud_summary import run
        run()

    @task(inlets=[dim_customer, dim_card, fact_txn, obt_fraud])
    def validate_contracts():
        """GE data-contract quality gate — critical violations fail the DAG."""
        import pandas as pd
        from src.pipelines.utils.db import load_pipeline_config, get_sqlalchemy_engine
        from src.pipelines.governance.ge_validate import validate_table

        cfg    = load_pipeline_config()
        engine = get_sqlalchemy_engine(cfg)
        schema = cfg["postgres"]["schema"]

        # Tables materialised by THIS DAG (feat_/ml_ tables are validated downstream)
        tables = ["dim_customer", "dim_card", "fact_transaction",
                  "obt_transaction_fraud_summary"]
        for t in tables:
            df = pd.read_sql(
                f"SELECT * FROM {schema}.{t} ORDER BY RANDOM() LIMIT 10000", engine
            )
            if df.empty:
                continue
            validate_table(t, df, build_docs=False, raise_on_critical=True)

    dims = load_dims()
    t_txn = load_fact_transaction()
    t_evt = load_fact_event()
    dims >> [t_txn, t_evt]
    obt = load_obt()
    t_txn >> obt
    obt >> validate_contracts()


batch_gold_dag()
