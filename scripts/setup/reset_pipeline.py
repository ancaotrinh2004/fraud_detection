"""
Reset Bronze/Silver/Gold/Feature tables so all DAGs re-process from scratch.

Run from project root: python scripts/reset_pipeline.py
Requires:
  - MinIO port-forward on localhost:9000
  - PostgreSQL port-forward on localhost:5432

Full re-run workflow:
  1. python -m src.generator.generate_data          # regenerate raw data
  2. python scripts/upload_raw_to_minio.py          # upload to MinIO raw bucket
  3. python scripts/reset_pipeline.py               # clear Bronze/Silver/Gold
  4. Trigger DAGs in order (see bottom of this file)
"""

import boto3
import psycopg2
from botocore.client import Config

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "fraud_minio_user"
MINIO_SECRET   = "fraud_minio_pass"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "fraud_detection"
PG_USER = "fraud_user"
PG_PASS = "fraud_pass"

SCHEMA = "gold_fraud"


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def delete_s3_prefix(s3, bucket: str, prefix: str):
    """Delete all objects under a MinIO prefix (one-by-one to avoid MD5 header issue)."""
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    print(f"  Deleted {deleted} objects from s3://{bucket}/{prefix}")


def reset_minio(s3):
    print("\n=== Clearing Bronze Delta Lake tables ===")
    for prefix in [
        "raw_transactions/",
        "raw_customers/",
        "raw_merchants/",
        "raw_cards/",
        "raw_fraud_events/",
    ]:
        delete_s3_prefix(s3, "bronze", prefix)

    print("\n=== Clearing Silver Delta Lake tables ===")
    for prefix in [
        "stg_transactions/",
        "stg_customers/",
        "stg_merchants/",
        "stg_cards/",
        "stg_fraud_events/",
    ]:
        delete_s3_prefix(s3, "silver", prefix)


def reset_postgres():
    print("\n=== Truncating Gold + Feature + ML tables ===")
    # Order matters: truncate dependents first, then dimensions
    tables = [
        # ML (depend on features + gold)
        "ml_fraud_training",
        "ml_fraud_label",
        "ml_fraud_scores",
        "ml_model_registry",
        # Drift monitoring
        "feature_drift_alerts",
        "agg_feature_health_daily",
        # Features (depend on Gold)
        "feat_customer_unified",
        "feat_customer_90d",
        "feat_stream_30m",
        # Gold facts + OBT (depend on dims)
        "obt_transaction_fraud_summary",
        "fact_transaction",
        "fact_fraud_event",
        # Gold dims
        "dim_customer",
        "dim_merchant",
        "dim_card",
        "dim_date",
        "dim_transaction_status",
        # Pipeline log
        "pipeline_run_log",
    ]
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )
    conn.autocommit = True
    cur = conn.cursor()
    for t in tables:
        cur.execute(f"TRUNCATE TABLE {SCHEMA}.{t} CASCADE;")
        print(f"  Truncated {SCHEMA}.{t}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    s3 = get_s3()
    reset_minio(s3)
    reset_postgres()
    print("\n✓ Reset complete. Now trigger DAGs in order:")
    print("  1. dag_bronze_ingest")
    print("  2. dag_silver_transform")
    print("  3. dag_gold_model")
    print("  4. dag_feat_stream_30m  +  dag_feat_customer_90d  (parallel)")
    print("  5. dag_feat_unified")
    print("  6. dag_drift_monitor")
    print("  7. dag_ml_label")
    print("  8. dag_ml_train")
