"""
src/pipelines/silver/clean_transactions.py
Bronze raw_transactions → Silver stg_transactions.
Dedup, type casting, schema evolution handling, dead-letter for bad rows.

Processes one transaction_date partition at a time to avoid OOM.
Scheduled: every 30 minutes.
"""

import gc
import glob
import logging
from datetime import datetime, timedelta

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from src.pipelines.utils.db import load_pipeline_config, log_run
from src.pipelines.utils.delta import get_storage_options
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_unique,
    check_no_nulls, check_fraud_rate,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "silver_clean_transactions"

VALID_STATUSES   = {"approved", "declined", "pending", "reversed"}
VALID_CURRENCIES = {"VND", "USD", "SGD", "THB"}
LATE_ARRIVAL_DAYS = 2


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Keep latest created_ts per transaction_id (removes gateway retries)."""
    return (
        df.sort_values("created_ts", ascending=False)
        .drop_duplicates(subset=["transaction_id"], keep="first")
    )


def _coerce_object_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all-null object columns to string so Delta Lake can infer a valid schema."""
    for col in df.select_dtypes(include="object").columns:
        if df[col].isna().all():
            df[col] = df[col].astype("string")
    return df


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce")
    df["created_ts"]            = pd.to_datetime(df["created_ts"],            errors="coerce")
    df["amount"]                = pd.to_numeric(df["amount"], errors="coerce")
    df["is_fraud"]              = df["is_fraud"].astype("Int8")
    return df


def _reject_bad_rows(df: pd.DataFrame, dead_letter_base: str, start_ts: datetime):
    required_notnull = ["transaction_id", "customer_id", "card_id",
                        "merchant_id", "amount", "transaction_ts"]
    mask_bad = df[required_notnull].isna().any(axis=1)
    mask_bad |= ~df["transaction_status"].isin(VALID_STATUSES)
    mask_bad |= df["amount"] <= 0

    bad_df  = df[mask_bad].copy()
    good_df = df[~mask_bad].copy()

    if len(bad_df) > 0:
        import os; os.makedirs(dead_letter_base, exist_ok=True)
        ts_str = start_ts.strftime("%Y%m%d_%H%M%S")
        dl_path = f"{dead_letter_base}/stg_transactions_rejected_{ts_str}.parquet"
        bad_df["rejection_reason"] = "null_required_field_or_invalid_value"
        bad_df["rejected_at"] = datetime.utcnow()
        bad_df.to_parquet(dl_path, index=False)
        logger.warning(f"[{PIPELINE_NAME}] {len(bad_df)} rows → dead-letter: {dl_path}")

    return good_df, len(bad_df)


def _list_bronze_partition_dates(bronze_base: str, table: str, partition_col: str,
                                  storage_opts) -> list[str]:
    """List bronze partition dates via S3 directory listing — skips delta log entirely."""
    key = f"{partition_col}="
    if storage_opts is None:
        dirs = glob.glob(f"{bronze_base}/{table}/{partition_col}=*")
        return sorted(d.rstrip("/").split(f"{partition_col}=")[-1] for d in dirs)

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    path = f"{bronze_base}/{table}".replace("s3://", "")
    try:
        entries = fs.ls(path, detail=False)
        return sorted(
            e.split(key)[-1].rstrip("/")
            for e in entries
            if key in e.split("/")[-1]
        )
    except Exception:
        return []


def _get_silver_partition_dates(table_path: str, storage_options, partition_col: str) -> set[str]:
    """Return set of committed partition values from the silver Delta table."""
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        files = dt.files()
        key = f"{partition_col}="
        return set(
            f.split(key)[1].split("/")[0]
            for f in files if key in f
        )
    except Exception:
        return set()


def _read_partition_direct(bronze_base: str, table: str, partition_col: str,
                            date_val: str, storage_opts) -> pd.DataFrame:
    """Read one partition directly via s3fs + pandas (bypasses delta-rs full-table scan)."""
    if storage_opts is None:
        partition_dir = f"{bronze_base}/{table}/{partition_col}={date_val}"
        files = glob.glob(f"{partition_dir}/*.parquet")
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    partition_path = (
        f"{bronze_base}/{table}/{partition_col}={date_val}".replace("s3://", "")
    )
    try:
        entries = [e for e in fs.ls(partition_path) if e.endswith(".parquet")]
    except FileNotFoundError:
        return pd.DataFrame()

    if not entries:
        return pd.DataFrame()

    s3_opts = {
        "endpoint_url": storage_opts["AWS_ENDPOINT_URL"],
        "key": storage_opts["AWS_ACCESS_KEY_ID"],
        "secret": storage_opts["AWS_SECRET_ACCESS_KEY"],
    }
    return pd.concat(
        [pd.read_parquet(f"s3://{e}", storage_options=s3_opts) for e in entries],
        ignore_index=True,
    )


def _read_silver_partition(silver_path: str, date_val: str, storage_opts) -> pd.DataFrame:
    """Read existing silver partition for late-arrival reprocessing."""
    try:
        dt = DeltaTable(silver_path, storage_options=storage_opts)
        return dt.to_pandas(partitions=[("transaction_date", "=", date_val)])
    except Exception:
        return pd.DataFrame()


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    bronze_base     = cfg["bronze_base_path"]
    silver_base     = cfg["silver_base_path"]
    dead_letter     = cfg.get("dead_letter_base_path", "data/dead_letter")
    qcfg            = cfg.get("quality", {})
    storage_opts    = get_storage_options(cfg)
    silver_txn_path = f"{silver_base}/stg_transactions"
    start_ts        = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    total_input = total_output = total_rejected = 0

    try:
        # Use S3 dir listing for bronze — avoids loading the delta log (can be 1000+ commits)
        bronze_dates = _list_bronze_partition_dates(
            bronze_base, "raw_transactions", "transaction_date", storage_opts
        )
        if not bronze_dates:
            raise ValueError("No partitions found in raw_transactions")

        # Only process dates not yet in silver — never re-merge existing data
        silver_dates = _get_silver_partition_dates(silver_txn_path, storage_opts, "transaction_date")
        new_dates = set(d for d in bronze_dates if d not in silver_dates)
        late_cutoff = (start_ts - timedelta(days=LATE_ARRIVAL_DAYS)).strftime("%Y-%m-%d")
        reprocess_dates = set(d for d in silver_dates if d >= late_cutoff)
        dates_to_process = sorted(new_dates | reprocess_dates)
        silver_is_new = len(silver_dates) == 0

        if not dates_to_process:
            logger.info(
                f"[{PIPELINE_NAME}] Silver is up to date ({len(silver_dates)} partitions). "
                f"Nothing to do."
            )
            log_run(PIPELINE_NAME, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        logger.info(
            f"[{PIPELINE_NAME}] {len(new_dates)} new + {len(reprocess_dates)} reprocess "
            f"(late-arrival {LATE_ARRIVAL_DAYS}d) = {len(dates_to_process)} total "
            f"(silver has {len(silver_dates)}/{len(bronze_dates)})."
        )
        first_write = True

        all_fraud_flags = []

        for date_val in dates_to_process:
            df = _read_partition_direct(
                bronze_base, "raw_transactions", "transaction_date", date_val, storage_opts
            )
            if df.empty:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: empty partition, skipping.")
                continue

            total_input += len(df)

            df = _cast_types(df)
            df, n_rejected = _reject_bad_rows(df, dead_letter, start_ts)
            total_rejected += n_rejected

            # Dedup within partition (gateway retries share the same transaction_date)
            df = _dedup(df)

            if "device_fingerprint" not in df.columns:
                df["device_fingerprint"] = pd.Series(dtype="string", index=df.index)
            else:
                df["device_fingerprint"] = df["device_fingerprint"].astype("string")
            if "ip_country" not in df.columns:
                df["ip_country"] = pd.Series(dtype="string", index=df.index)
            else:
                df["ip_country"] = df["ip_country"].astype("string")
            df["ip_country"] = df["ip_country"].str.strip().str.upper()

            df["stg_processed_ts"] = datetime.utcnow()
            df["transaction_date"] = df["transaction_ts"].dt.date.astype(str)

            qr = QualityResult(pipeline=PIPELINE_NAME)
            check_not_empty(df, qr)
            check_unique(df, ["transaction_id"], qr)
            check_no_nulls(df, ["transaction_id", "customer_id", "amount"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed for {date_val}:\n{qr.summary()}")

            all_fraud_flags.append(df["is_fraud"].dropna())

            is_reprocess = date_val in reprocess_dates and date_val not in new_dates
            if is_reprocess:
                existing = _read_silver_partition(silver_txn_path, date_val, storage_opts)
                if not existing.empty:
                    df = pd.concat([df, existing], ignore_index=True)
                df = _dedup(df)

            df = _coerce_object_nulls(df)
            if is_reprocess:
                write_deltalake(
                    silver_txn_path, df,
                    mode="overwrite",
                    predicate=f"transaction_date = '{date_val}'",
                    partition_by=["transaction_date"],
                    storage_options=storage_opts,
                    schema_mode="merge",
                )
                logger.info(f"[{PIPELINE_NAME}] {date_val}: {len(df):,} rows (overwrite/reprocess).")
            else:
                # Re-check per-partition to guard against concurrent DAG runs writing the same date
                current_silver = _get_silver_partition_dates(silver_txn_path, storage_opts, "transaction_date")
                if date_val in current_silver:
                    logger.warning(f"[{PIPELINE_NAME}] {date_val}: already written by concurrent run, skipping.")
                    del df
                    gc.collect()
                    continue
                if silver_is_new and first_write:
                    mode = "overwrite"
                    first_write = False
                else:
                    mode = "append"
                write_deltalake(
                    silver_txn_path, df, mode=mode,
                    partition_by=["transaction_date"],
                    storage_options=storage_opts,
                    schema_mode="merge",
                )
                logger.info(f"[{PIPELINE_NAME}] {date_val}: {len(df):,} rows ({mode}).")

            total_output += len(df)

            del df
            gc.collect()

        # Fraud rate check across all partitions
        if all_fraud_flags:
            fraud_rate = pd.concat(all_fraud_flags).mean()
            fr_min = qcfg.get("fraud_rate_min", 0.005)
            fr_max = qcfg.get("fraud_rate_max", 0.150)
            if not (fr_min <= fraud_rate <= fr_max):
                logger.warning(
                    f"[{PIPELINE_NAME}] Fraud rate {fraud_rate:.3f} outside [{fr_min}, {fr_max}]"
                )

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", total_input, total_output, cfg=cfg)
        logger.info(
            f"[{PIPELINE_NAME}] Done. in={total_input:,} "
            f"rejected={total_rejected} out={total_output:,}"
        )

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
