"""
Emit accurate data lineage to DataHub for the fraud detection project.

Architecture:
  MinIO (s3://raw/)
    └─► Bronze Delta Lake (s3://bronze/)
          └─► Silver Delta Lake (s3://silver/)
                └─► Gold PostgreSQL (fraud_detection.gold_fraud)
                      └─► Feature Store PostgreSQL (fraud_detection.gold_fraud)

Prerequisites:
  1. kubectl port-forward svc/datahub-datahub-gms 8080:8080 -n datahub
  2. uv pip install -r requirements.txt

Run:
  python scripts/emit_lineage.py
"""

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

GMS_URL = "http://localhost:8080"
ENV = "DEV"

# PostgreSQL database name (as seen from DataHub)
PG_DB = "fraud_detection"
PG_SCHEMA = "gold_fraud"


# ── URN helpers ──────────────────────────────────────────────────────────────

def raw(path: str) -> str:
    """MinIO raw source file/directory (s3://raw/...)."""
    return make_dataset_urn("s3", f"raw/{path}", ENV)


def bronze(table: str) -> str:
    """Delta Lake Bronze table (s3://bronze/<table>)."""
    return make_dataset_urn("delta-lake", f"bronze/{table}", ENV)


def silver(table: str) -> str:
    """Delta Lake Silver table (s3://silver/<table>)."""
    return make_dataset_urn("delta-lake", f"silver/{table}", ENV)


def gold(table: str) -> str:
    """PostgreSQL Gold / Feature table (fraud_detection.gold_fraud.<table>)."""
    return make_dataset_urn("postgres", f"{PG_DB}.{PG_SCHEMA}.{table}", ENV)


# ── Emit helper ──────────────────────────────────────────────────────────────

def emit(emitter: DatahubRestEmitter, downstream_urn: str, upstream_urns: list[str],
         label: str = ""):
    upstreams = [
        UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
        for u in upstream_urns
    ]
    mcp = MetadataChangeProposalWrapper(
        entityUrn=downstream_urn,
        aspect=UpstreamLineageClass(upstreams=upstreams),
    )
    emitter.emit(mcp)
    tag = f"  [{label}]" if label else ""
    print(f"  ✓{tag}  → {downstream_urn.split(',')[-1].rstrip(')')}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    emitter = DatahubRestEmitter(gms_server=GMS_URL)
    emitter.test_connection()
    print(f"Connected to DataHub GMS at {GMS_URL}\n")

    # ── Bronze: MinIO raw files → Delta Lake ─────────────────────────────────
    print("Bronze (DAG: bronze_ingest):")

    emit(emitter, bronze("raw_transactions"),
         [raw("offline/transactions/")],
         "dag_bronze_ingest")

    emit(emitter, bronze("raw_fraud_events"),
         [raw("streaming/fraud_events.json")],
         "dag_bronze_ingest")

    emit(emitter, bronze("raw_customers"),
         [raw("offline/customers.parquet")],
         "dag_bronze_ingest")

    emit(emitter, bronze("raw_merchants"),
         [raw("offline/merchants.parquet")],
         "dag_bronze_ingest")

    emit(emitter, bronze("raw_cards"),
         [raw("offline/cards.parquet")],
         "dag_bronze_ingest")

    # ── Silver: Bronze Delta Lake → cleaned Delta Lake ────────────────────────
    print("\nSilver (DAG: silver_transform):")

    emit(emitter, silver("stg_transactions"),
         [bronze("raw_transactions")],
         "clean_transactions")

    emit(emitter, silver("stg_fraud_events"),
         [bronze("raw_fraud_events")],
         "clean_events")

    emit(emitter, silver("stg_customers"),
         [bronze("raw_customers")],
         "clean_reference")

    emit(emitter, silver("stg_merchants"),
         [bronze("raw_merchants")],
         "clean_reference")

    emit(emitter, silver("stg_cards"),
         [bronze("raw_cards")],
         "clean_reference")

    # ── Gold — Dimensions: Silver → PostgreSQL ────────────────────────────────
    print("\nGold — Dimensions (DAG: gold_model / dim_load):")

    # dim_date and dim_transaction_status are derived from stg_transactions dates/statuses
    emit(emitter, gold("dim_date"),
         [silver("stg_transactions")],
         "dim_load")

    emit(emitter, gold("dim_transaction_status"),
         [silver("stg_transactions")],
         "dim_load")

    emit(emitter, gold("dim_customer"),
         [silver("stg_customers")],
         "dim_load")

    emit(emitter, gold("dim_merchant"),
         [silver("stg_merchants")],
         "dim_load")

    emit(emitter, gold("dim_card"),
         [silver("stg_cards")],
         "dim_load")

    # ── Gold — Facts: Silver + Dimensions → PostgreSQL ───────────────────────
    print("\nGold — Facts (DAG: gold_model / fact pipelines):")

    emit(emitter, gold("fact_transaction"),
         [
             silver("stg_transactions"),
             gold("dim_customer"),
             gold("dim_card"),
             gold("dim_merchant"),
             gold("dim_transaction_status"),
         ],
         "fact_transaction")

    emit(emitter, gold("fact_fraud_event"),
         [
             silver("stg_fraud_events"),
             gold("dim_customer"),
             gold("dim_card"),
             gold("dim_merchant"),
         ],
         "fact_fraud_event")

    # ── Gold — OBT: Facts + Dimensions → denormalised table ──────────────────
    print("\nGold — OBT (DAG: gold_model / obt):")

    emit(emitter, gold("obt_transaction_fraud_summary"),
         [
             gold("fact_transaction"),
             gold("dim_customer"),
             gold("dim_merchant"),
             gold("dim_card"),
             gold("dim_transaction_status"),
         ],
         "obt_transaction_fraud_summary")

    # ── Feature Store ─────────────────────────────────────────────────────────
    print("\nFeature Store:")

    # feat_customer_90d: reads stg_transactions (Delta) + dim_card (PG) for card_country
    emit(emitter, gold("feat_customer_90d"),
         [
             silver("stg_transactions"),
             gold("dim_card"),
         ],
         "dag_feat_customer_90d")

    # feat_stream_30m: reads stg_fraud_events (Delta) only
    emit(emitter, gold("feat_stream_30m"),
         [silver("stg_fraud_events")],
         "dag_feat_stream_30m")

    # feat_customer_unified: point-in-time join of the two feature tables
    emit(emitter, gold("feat_customer_unified"),
         [
             gold("feat_customer_90d"),
             gold("feat_stream_30m"),
         ],
         "dag_feat_unified")

    print("\nDone! All lineage emitted.")
    print("\nView in DataHub UI: http://localhost:9002")
    print("  → Search 'obt_transaction_fraud_summary' → Lineage tab (upstream graph)")
    print("  → Search 'feat_customer_unified'         → Lineage tab (full end-to-end)")


if __name__ == "__main__":
    main()
