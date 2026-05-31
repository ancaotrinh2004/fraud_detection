"""
src/pipelines/bronze/ingest_transactions.py
Incremental load: transactions Parquet partitions → Delta Lake Bronze.
Scheduled: every 30 minutes.
"""

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, log_run
from src.pipelines.utils.delta import (
    write_bronze, get_last_ingest_ts,
    get_storage_options, get_pandas_s3_opts, list_s3_partitions,
)
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_schema, check_no_nulls,
)

logger = logging.getLogger(__name__)

REQUIRED_COLS = [
    "transaction_id", "customer_id", "card_id", "merchant_id",
    "transaction_timestamp", "created_ts", "amount",
    "currency", "transaction_status", "city", "is_fraud",
]

PIPELINE_NAME = "bronze_ingest_transactions"


def _find_new_partitions(source_dir: str, last_ingest_ts, cfg: dict) -> list[str]:
    """Return partition paths not yet ingested (supports local and S3)."""
    if source_dir.startswith("s3://"):
        all_parts = list_s3_partitions(source_dir, cfg)
    else:
        all_parts = sorted(
            str(p) for p in Path(source_dir).glob("transaction_date=*")
        )

    if last_ingest_ts is None:
        return all_parts

    cutoff_date = (pd.Timestamp(last_ingest_ts) - timedelta(days=1)).date()
    new_parts = []
    for p in all_parts:
        date_str = p.rstrip("/").split("/")[-1].split("=")[1]
        try:
            if datetime.strptime(date_str, "%Y-%m-%d").date() >= cutoff_date:
                new_parts.append(p)
        except ValueError:
            continue
    return new_parts


def run(cfg: dict | None = None, execution_date: str | None = None) -> None:
    """
    execution_date: if set (YYYY-MM-DD), only load that specific partition.
    Otherwise: incremental load of all new partitions since last ingest.
    """
    if cfg is None:
        cfg = load_pipeline_config()

    raw_base    = cfg.get("raw_base_path", "data/raw")
    bronze_base = cfg["bronze_base_path"]
    source_dir  = f"{raw_base}/offline/transactions"
    table_path  = f"{bronze_base}/raw_transactions"

    storage_opts = get_storage_options(cfg)
    pd_s3_opts   = get_pandas_s3_opts(cfg)

    start_ts = datetime.utcnow()
    logger.info(f"[{PIPELINE_NAME}] Starting (execution_date={execution_date})...")

    try:
        if execution_date:
            partitions = [f"{source_dir}/transaction_date={execution_date}"]
        else:
            last_ts = get_last_ingest_ts(table_path, storage_options=storage_opts)
            partitions = _find_new_partitions(source_dir, last_ts, cfg)

        if not partitions:
            logger.info(f"[{PIPELINE_NAME}] No new partitions. Skipping.")
            return

        dfs = []
        for part in partitions:
            dfs.append(pd.read_parquet(part, storage_options=pd_s3_opts))

        df = pd.concat(dfs, ignore_index=True)

        qr = QualityResult(pipeline=PIPELINE_NAME)
        check_not_empty(df, qr)
        check_schema(df, REQUIRED_COLS, qr)
        check_no_nulls(df, ["transaction_id", "customer_id", "amount"], qr)
        if not qr.passed:
            raise ValueError(f"Quality checks failed:\n{qr.summary()}")

        df["ingest_ts"] = datetime.utcnow()
        df["batch_id"]  = str(uuid.uuid4())
        df["transaction_date"] = (
            pd.to_datetime(df["transaction_timestamp"]).dt.date.astype(str)
        )

        write_bronze(df, table_path, storage_options=storage_opts,
                     partition_by=["transaction_date"])

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", len(df), len(df), cfg=cfg)
        logger.info(
            f"[{PIPELINE_NAME}] Done. {len(df):,} rows across {len(partitions)} partitions."
        )

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
