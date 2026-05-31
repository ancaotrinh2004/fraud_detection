"""
Upload raw data from host to MinIO.
Run from project root: python scripts/upload_raw_to_minio.py

Requires MinIO port-forward active on localhost:19000.
"""

import boto3
from pathlib import Path
from botocore.client import Config

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY     = "fraud_minio_user"
SECRET_KEY     = "fraud_minio_pass"
RAW_DIR        = Path("data/raw")
BUCKETS        = ["raw", "bronze", "silver"]


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_buckets(client):
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for bucket in BUCKETS:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
            print(f"  Created bucket: {bucket}")
        else:
            print(f"  Bucket exists:  {bucket}")


def upload_dir(client, local_dir: Path, bucket: str, prefix: str):
    files = [f for f in local_dir.rglob("*") if f.is_file()]
    print(f"Uploading {len(files)} files → s3://{bucket}/{prefix}")
    for i, fpath in enumerate(files, 1):
        key = f"{prefix}/{fpath.relative_to(local_dir)}"
        client.upload_file(str(fpath), bucket, key)
        if i % 10 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {key}")


def main():
    client = get_client()

    print("=== Ensuring buckets ===")
    ensure_buckets(client)

    print("\n=== Uploading reference parquets ===")
    for parquet in (RAW_DIR / "offline").glob("*.parquet"):
        key = f"offline/{parquet.name}"
        client.upload_file(str(parquet), "raw", key)
        print(f"  {key}  ({parquet.stat().st_size / 1024 / 1024:.1f} MB)")

    print("\n=== Uploading transaction partitions ===")
    upload_dir(client, RAW_DIR / "offline" / "transactions",
               bucket="raw", prefix="offline/transactions")

    print("\n=== Uploading fraud events (full file) ===")
    src = RAW_DIR / "streaming" / "fraud_events.json"
    size_gb = src.stat().st_size / 1024 / 1024 / 1024
    print(f"  File size: {size_gb:.2f} GB — this may take a while...")
    client.upload_file(str(src), "raw", "streaming/fraud_events.json")
    print(f"  Uploaded: streaming/fraud_events.json  ({size_gb:.2f} GB)")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
