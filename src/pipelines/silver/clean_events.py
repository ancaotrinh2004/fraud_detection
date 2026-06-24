"""
src/pipelines/silver/clean_events.py
Bronze raw_fraud_events → Silver stg_fraud_events.
Dedup by (event_id, event_ts).

Memory-safe design:
- Reads one event_date partition at a time via s3fs (bypasses delta-rs full-table scan).
- Compares bronze vs silver partition dates; only appends missing partitions.
- Reprocesses partitions within the last LATE_ARRIVAL_DAYS days to reconcile late-arriving
  events (events with old event_ts but recent created_ts that arrived after their
  partition was first written to silver).
- Never merges against the full target table — avoids O(n^2) memory growth.
Scheduled: every 5 minutes.
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
    QualityResult, check_not_empty, check_unique, check_no_nulls,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "silver_clean_events"

VALID_EVENT_TYPES = {
    "login", "card_check", "transaction_attempt", "transaction_approved",
    "transaction_declined", "otp_request", "otp_failed",
}
# Reprocess silver partitions within this window on every run to capture
# late-arriving events (events with event_ts in the past that arrived
# in bronze after the silver partition was first written).
LATE_ARRIVAL_DAYS = 2


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values("created_ts", ascending=False)
        .drop_duplicates(subset=["event_id", "event_ts"], keep="first")
    )


def _coerce_object_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all-null object columns to string so Delta Lake can infer a valid schema."""
    for col in df.select_dtypes(include="object").columns:
        if df[col].isna().all():
            df[col] = df[col].astype("string")
    return df


def _reject_bad_rows(df: pd.DataFrame, dead_letter_base: str, start_ts: datetime):
    mask_bad = df["event_id"].isna() | df["event_ts"].isna() | df["customer_id"].isna()
    mask_bad |= ~df["event_type"].isin(VALID_EVENT_TYPES)

    bad_df  = df[mask_bad].copy()
    good_df = df[~mask_bad].copy()

    if len(bad_df) > 0:
        import os; os.makedirs(dead_letter_base, exist_ok=True)
        ts_str = start_ts.strftime("%Y%m%d_%H%M%S")
        dl_path = f"{dead_letter_base}/stg_events_rejected_{ts_str}.parquet"
        bad_df["rejection_reason"] = "null_required_field_or_invalid_event_type"
        bad_df["rejected_at"] = datetime.utcnow()
        bad_df.to_parquet(dl_path, index=False)
        logger.warning(f"[{PIPELINE_NAME}] {len(bad_df)} bad rows → {dl_path}")

    return good_df, len(bad_df)


def _get_delta_dates(table_path: str, storage_options, partition_col: str) -> set[str]:
    """Return set of committed partition values from a Delta table (silver only — log is small)."""
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


def _read_partition_direct(bronze_base: str, date_val: str, storage_opts) -> pd.DataFrame:
    """Read one event_date partition directly via s3fs (no delta-rs full-table scan)."""
    if storage_opts is None:
        partition_dir = f"{bronze_base}/raw_fraud_events/event_date={date_val}"
        files = glob.glob(f"{partition_dir}/*.parquet")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    partition_path = (
        f"{bronze_base}/raw_fraud_events/event_date={date_val}".replace("s3://", "")
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
        return dt.to_pandas(partitions=[("event_date", "=", date_val)])
    except Exception:
        return pd.DataFrame()


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    bronze_base        = cfg["bronze_base_path"]
    silver_base        = cfg["silver_base_path"]
    dead_letter        = cfg.get("dead_letter_base_path", "data/dead_letter")
    storage_opts       = get_storage_options(cfg)
    silver_events_path = f"{silver_base}/stg_fraud_events"
    start_ts           = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    total_input = total_output = total_rejected = 0

    try:
        bronze_dates = _list_bronze_partition_dates(
            bronze_base, "raw_fraud_events", "event_date", storage_opts
        )
        if not bronze_dates:
            raise ValueError("No partitions found in raw_fraud_events")

        silver_dates = _get_delta_dates(silver_events_path, storage_opts, "event_date")

        # New dates not yet in silver
        new_dates = set(d for d in bronze_dates if d not in silver_dates)

        # Partitions within late arrival window — reprocess to capture late-arriving events
        late_cutoff = (start_ts - timedelta(days=LATE_ARRIVAL_DAYS)).strftime("%Y-%m-%d")
        reprocess_dates = set(d for d in silver_dates if d >= late_cutoff)

        dates_to_process = sorted(new_dates | reprocess_dates)

        if not dates_to_process:
            logger.info(
                f"[{PIPELINE_NAME}] Silver is up to date ({len(silver_dates)} partitions). "
                f"Nothing to do."
            )
            log_run(PIPELINE_NAME, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        silver_is_new = len(silver_dates) == 0
        first_write   = True

        logger.info(
            f"[{PIPELINE_NAME}] {len(new_dates)} new + {len(reprocess_dates)} reprocess "
            f"(late-arrival window {LATE_ARRIVAL_DAYS}d) = {len(dates_to_process)} total "
            f"(silver has {len(silver_dates)}/{len(bronze_dates)})."
        )

        for date_val in dates_to_process:
            df = _read_partition_direct(bronze_base, date_val, storage_opts)
            if df.empty:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: empty bronze partition, skipping.")
                continue

            # For reprocessing: merge fresh bronze data with existing silver to include
            # all previously-accepted events plus any newly-arrived late events.
            is_reprocess = date_val in reprocess_dates and date_val not in new_dates
            if is_reprocess:
                existing = _read_silver_partition(silver_events_path, date_val, storage_opts)
                if not existing.empty:
                    df = pd.concat([df, existing], ignore_index=True)
                    logger.info(
                        f"[{PIPELINE_NAME}] {date_val}: reprocessing "
                        f"(bronze={len(df) - len(existing):,} + existing={len(existing):,})."
                    )

            df["event_ts"] = pd.to_datetime(df["event_ts"])
            df["created_ts"]      = pd.to_datetime(df["created_ts"])
            total_input += len(df)

            df, n_rejected = _reject_bad_rows(df, dead_letter, start_ts)
            total_rejected += n_rejected

            df = _dedup(df)

            df["is_otp_failed"]          = (df["event_type"] == "otp_failed").astype("Int8")
            df["is_declined"]            = (df["event_type"] == "transaction_declined").astype("Int8")
            df["is_transaction_attempt"] = (df["event_type"] == "transaction_attempt").astype("Int8")
            df["stg_processed_ts"]       = datetime.utcnow()
            df["event_date"]             = df["event_ts"].dt.date.astype(str)

            qr = QualityResult(pipeline=PIPELINE_NAME)
            check_not_empty(df, qr)
            check_unique(df, ["event_id", "event_ts"], qr)
            check_no_nulls(df, ["event_id", "event_type", "customer_id"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed for {date_val}:\n{qr.summary()}")

            df = _coerce_object_nulls(df)
            if is_reprocess:
                # Overwrite this partition only — replaceWhere on event_date
                write_deltalake(
                    silver_events_path, df,
                    mode="overwrite",
                    predicate=f"event_date = '{date_val}'",
                    partition_by=["event_date"],
                    storage_options=storage_opts,
                    schema_mode="merge",
                )
                logger.info(f"[{PIPELINE_NAME}] {date_val}: {len(df):,} rows (overwrite/reprocess).")
            else:
                # Re-check per-partition to guard against concurrent DAG runs writing the same date
                current_silver = _get_delta_dates(silver_events_path, storage_opts, "event_date")
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
                    silver_events_path, df, mode=mode,
                    partition_by=["event_date"],
                    storage_options=storage_opts,
                    schema_mode="merge",
                )
                logger.info(f"[{PIPELINE_NAME}] {date_val}: {len(df):,} rows ({mode}).")

            total_output += len(df)

            del df
            gc.collect()

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
