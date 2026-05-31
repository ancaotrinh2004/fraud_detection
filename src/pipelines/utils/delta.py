"""src/pipelines/utils/delta.py — Delta Lake read/write helpers (local + S3/MinIO)."""

import gc
import os
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from deltalake import DeltaTable, write_deltalake

_SILVER_WRITE_CHUNK = 100_000

logger = logging.getLogger(__name__)


# ── Storage option helpers ────────────────────────────────────────────────────

def get_storage_options(cfg: dict) -> Optional[dict]:
    """Return Delta Lake storage_options for S3/MinIO. Returns None for local paths."""
    minio = cfg.get("minio")
    if not minio:
        return None
    access_key = os.environ.get("MINIO_ACCESS_KEY", minio.get("access_key", ""))
    secret_key = os.environ.get("MINIO_SECRET_KEY", minio.get("secret_key", ""))
    return {
        "AWS_ENDPOINT_URL": minio["endpoint_url"],
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": "us-east-1",
    }


def get_pandas_s3_opts(cfg: dict) -> Optional[dict]:
    """Return pandas/fsspec storage_options for S3/MinIO reads. None for local paths."""
    minio = cfg.get("minio")
    if not minio:
        return None
    access_key = os.environ.get("MINIO_ACCESS_KEY", minio.get("access_key", ""))
    secret_key = os.environ.get("MINIO_SECRET_KEY", minio.get("secret_key", ""))
    return {
        "endpoint_url": minio["endpoint_url"],
        "key": access_key,
        "secret": secret_key,
    }


def list_s3_partitions(source_dir: str, cfg: dict) -> list[str]:
    """List s3://bucket/prefix/transaction_date=YYYY-MM-DD paths via s3fs."""
    import s3fs
    minio = cfg.get("minio", {})
    access_key = os.environ.get("MINIO_ACCESS_KEY", minio.get("access_key", ""))
    secret_key = os.environ.get("MINIO_SECRET_KEY", minio.get("secret_key", ""))
    fs = s3fs.S3FileSystem(
        endpoint_url=minio.get("endpoint_url"),
        key=access_key,
        secret=secret_key,
        use_ssl=False,
    )
    path = source_dir.replace("s3://", "", 1)
    try:
        entries = fs.ls(path)
        return sorted(
            f"s3://{e}" for e in entries
            if "transaction_date=" in e.split("/")[-1]
        )
    except Exception:
        return []


# ── Delta Lake write/read ─────────────────────────────────────────────────────

def write_bronze(df: pd.DataFrame, table_path: str,
                 storage_options: Optional[dict] = None,
                 partition_by: list[str] | None = None) -> None:
    """Append-only write to Delta Lake (Bronze layer)."""
    if not table_path.startswith("s3://"):
        Path(table_path).mkdir(parents=True, exist_ok=True)
    write_deltalake(
        table_path,
        df,
        mode="append",
        partition_by=partition_by or [],
        storage_options=storage_options,
    )
    logger.info(f"[bronze] Wrote {len(df):,} rows → {table_path}")


def write_silver(df: pd.DataFrame, table_path: str,
                 merge_keys: list[str],
                 storage_options: Optional[dict] = None,
                 partition_by: list[str] | None = None) -> None:
    """Upsert to Delta Lake (Silver layer). Falls back to overwrite on first write."""
    if not table_path.startswith("s3://"):
        Path(table_path).mkdir(parents=True, exist_ok=True)
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
        (
            dt.merge(source=df, predicate=condition,
                     source_alias="source", target_alias="target")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info(f"[silver] Merged {len(df):,} rows → {table_path}")
    except Exception:
        # First write — chunk to avoid Arrow peak-memory with large DataFrames
        total = len(df)
        for i, start in enumerate(range(0, total, _SILVER_WRITE_CHUNK)):
            chunk = df.iloc[start:start + _SILVER_WRITE_CHUNK]
            mode = "overwrite" if i == 0 else "append"
            write_deltalake(
                table_path, chunk, mode=mode,
                partition_by=partition_by or [],
                storage_options=storage_options,
            )
            del chunk
            gc.collect()
            logger.info(f"[silver] Initial write chunk {i+1}: {min(start+_SILVER_WRITE_CHUNK, total):,}/{total:,} rows → {table_path}")
        logger.info(f"[silver] Initial write complete {total:,} rows → {table_path}")


def read_delta(table_path: str, storage_options: Optional[dict] = None,
               filters: list | None = None,
               partitions: list | None = None) -> pd.DataFrame:
    """Read a Delta Lake table into a DataFrame.

    Use `partitions` for partition-column pruning (skips irrelevant Parquet files).
    Use `filters` for row-level filtering after file scan.
    """
    dt = DeltaTable(table_path, storage_options=storage_options)
    kwargs = {}
    if partitions:
        kwargs["partitions"] = partitions
    if filters:
        kwargs["filters"] = filters
    return dt.to_pandas(**kwargs) if kwargs else dt.to_pandas()


def get_last_ingest_ts(table_path: str, storage_options: Optional[dict] = None,
                       ts_col: str = "ingest_ts"):
    """Return the max value of ts_col from a Delta table (column-projection only — no full scan)."""
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        df = dt.to_pandas(columns=[ts_col])
        if len(df) > 0:
            return df[ts_col].max()
    except Exception:
        pass
    return None


def get_last_created_watermark(table_path: str, storage_options: Optional[dict] = None):
    """Return max(created_ts) from a Bronze table — used as the late-event watermark.
    Tracking created_ts instead of event_timestamp allows late-arriving events
    (old event_timestamp, recent created_ts) to pass through on subsequent runs.
    """
    return get_last_ingest_ts(table_path, storage_options=storage_options, ts_col="created_ts")
