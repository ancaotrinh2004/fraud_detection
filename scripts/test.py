"""Check MinIO amounts vs local to identify where drift is lost."""
import boto3
import pandas as pd
import io
from botocore.client import Config

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="fraud_minio_user",
    aws_secret_access_key="fraud_minio_pass",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

def read_minio_parquet(bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

# Check raw bucket — Sep 15 partition
sep_key = "offline/transactions/transaction_date=2025-09-15/data.parquet"
jul_key = "offline/transactions/transaction_date=2025-07-15/data.parquet"

try:
    sep_df = read_minio_parquet("raw", sep_key)
    jul_df = read_minio_parquet("raw", jul_key)
    print(f"MinIO raw — Sep 15 mean: {sep_df['amount'].mean():,.0f}")
    print(f"MinIO raw — Jul 15 mean: {jul_df['amount'].mean():,.0f}")
    print(f"Sep/Jul ratio in MinIO: {sep_df['amount'].mean() / jul_df['amount'].mean():.3f}")
except Exception as e:
    print(f"ERROR reading MinIO: {e}")
