"""
src/pipelines/gold/fact_fraud_event.py
Silver stg_fraud_events → Gold fact_fraud_event.
Partition-aware incremental load: one event_date at a time to avoid OOM on 34M rows.
"""

import gc
import glob
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.delta import get_storage_options
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_unique, check_no_nulls,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME  = "gold_fact_fraud_event"
SCHEMA         = "gold_fraud"
UPSERT_CHUNK   = 5_000
LATE_ARRIVAL_DAYS = 2

_FACT_COLS = [
    "event_id", "customer_key", "event_date_key", "event_type",
    "event_ts", "created_ts", "session_id", "device_type",
    "ip_country", "card_key", "merchant_key", "amount", "failure_reason",
    "is_otp_failed", "is_declined", "is_transaction_attempt",
]

_UPSERT_SQL = text(f"""
    INSERT INTO gold_fraud.fact_fraud_event
        (event_id, customer_key, event_date_key, event_type,
         event_ts, created_ts, session_id, device_type,
         ip_country, card_key, merchant_key, amount, failure_reason,
         is_otp_failed, is_declined, is_transaction_attempt)
    VALUES
        (:event_id, :customer_key, :event_date_key, :event_type,
         :event_ts, :created_ts, :session_id, :device_type,
         :ip_country, :card_key, :merchant_key, :amount, :failure_reason,
         :is_otp_failed, :is_declined, :is_transaction_attempt)
    ON CONFLICT (event_id, event_ts) DO UPDATE SET
        customer_key           = EXCLUDED.customer_key,
        event_date_key         = EXCLUDED.event_date_key,
        is_otp_failed          = EXCLUDED.is_otp_failed,
        is_declined            = EXCLUDED.is_declined,
        is_transaction_attempt = EXCLUDED.is_transaction_attempt
""")


def _list_silver_partition_dates(silver_base: str, storage_opts) -> list[str]:
    """List silver event_date partitions via S3 directory listing — no delta log read."""
    key = "event_date="
    if storage_opts is None:
        dirs = glob.glob(f"{silver_base}/stg_fraud_events/event_date=*")
        return sorted(d.rstrip("/").split(key)[-1] for d in dirs)

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    path = f"{silver_base}/stg_fraud_events".replace("s3://", "")
    try:
        entries = fs.ls(path, detail=False)
        return sorted(
            e.split(key)[-1].rstrip("/")
            for e in entries
            if key in e.split("/")[-1]
        )
    except Exception:
        return []


def _get_fact_loaded_dates(engine) -> set[str]:
    """Return set of YYYY-MM-DD strings already present in fact_fraud_event (via event_date_key)."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT DISTINCT event_date_key FROM {SCHEMA}.fact_fraud_event"
            )).fetchall()
        return set(
            f"{str(k)[0:4]}-{str(k)[4:6]}-{str(k)[6:]}"
            for (k,) in rows if k is not None
        )
    except Exception:
        return set()


def _read_silver_partition(silver_base: str, date_val: str, storage_opts) -> pd.DataFrame:
    """Read one silver event_date partition directly via s3fs."""
    if storage_opts is None:
        files = glob.glob(f"{silver_base}/stg_fraud_events/event_date={date_val}/*.parquet")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    path = f"{silver_base}/stg_fraud_events/event_date={date_val}".replace("s3://", "")
    try:
        entries = [e for e in fs.ls(path) if e.endswith(".parquet")]
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


def _load_dim_keys(engine) -> dict[str, pd.DataFrame]:
    """Load dim surrogate keys — small tables, loaded once and reused across all partitions."""
    dims = {}
    dims["customer"] = pd.read_sql(
        f"SELECT DISTINCT ON (customer_id) customer_key, customer_id"
        f" FROM {SCHEMA}.dim_customer WHERE is_current = TRUE"
        f" ORDER BY customer_id, customer_key DESC",
        engine,
    )
    dims["card"] = pd.read_sql(
        f"SELECT DISTINCT ON (card_id) card_key, card_id"
        f" FROM {SCHEMA}.dim_card ORDER BY card_id, card_key DESC",
        engine,
    )
    dims["merchant"] = pd.read_sql(
        f"SELECT DISTINCT ON (merchant_id) merchant_key, merchant_id"
        f" FROM {SCHEMA}.dim_merchant WHERE is_current = TRUE"
        f" ORDER BY merchant_id, merchant_key DESC",
        engine,
    )
    dims["valid_date_keys"] = set(
        pd.read_sql(f"SELECT date_key FROM {SCHEMA}.dim_date", engine)["date_key"]
    )
    return dims


def _resolve_keys(df: pd.DataFrame, dims: dict) -> pd.DataFrame:
    df = df.merge(dims["customer"], on="customer_id", how="left")
    if "card_id" in df.columns:
        df = df.merge(dims["card"], on="card_id", how="left")
    else:
        df["card_key"] = None
    if "merchant_id" in df.columns:
        df = df.merge(dims["merchant"], on="merchant_id", how="left")
    else:
        df["merchant_key"] = None
    df["event_date_key"] = (
        pd.to_datetime(df["event_ts"]).dt.strftime("%Y%m%d").astype(int)
    )
    before = len(df)
    df = df[df["event_date_key"].isin(dims["valid_date_keys"])].copy()
    dropped = before - len(df)
    if dropped:
        logger.warning(f"[{PIPELINE_NAME}] Dropped {dropped:,} rows with event_date_key not in dim_date (corrupt timestamps).")
    return df


def _upsert_fact(df: pd.DataFrame, engine) -> int:
    df = df[[c for c in _FACT_COLS if c in df.columns]].copy()
    total = len(df)
    rows_inserted = 0
    for start in range(0, total, UPSERT_CHUNK):
        chunk = df.iloc[start:start + UPSERT_CHUNK]
        records = [
            {k: None if isinstance(v, float) and v != v else v for k, v in r.items()}
            for r in chunk.to_dict("records")
        ]
        with engine.begin() as conn:
            conn.execute(_UPSERT_SQL, records)
        rows_inserted += len(chunk)
        if rows_inserted % 50_000 == 0 or rows_inserted == total:
            logger.info(f"[{PIPELINE_NAME}] Upserted {rows_inserted:,}/{total:,} rows...")
    return rows_inserted


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    silver_base  = cfg["silver_base_path"]
    engine       = get_sqlalchemy_engine(cfg)
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    total_input = total_output = 0

    try:
        silver_dates = _list_silver_partition_dates(silver_base, storage_opts)
        if not silver_dates:
            raise ValueError("No partitions found in stg_fraud_events")

        fact_dates = _get_fact_loaded_dates(engine)

        # Load dims early so we can pre-filter silver partition dates not covered by dim_date
        # (e.g. corrupt 1970-era partitions from pandas datetime64 resolution bug in generator)
        dims = _load_dim_keys(engine)
        valid_date_strs = {
            f"{str(k)[0:4]}-{str(k)[4:6]}-{str(k)[6:]}"
            for k in dims["valid_date_keys"]
        }

        late_cutoff = (start_ts - timedelta(days=LATE_ARRIVAL_DAYS)).strftime("%Y-%m-%d")
        dates_to_process = [
            d for d in silver_dates
            if (d not in fact_dates or d >= late_cutoff) and d in valid_date_strs
        ]

        if not dates_to_process:
            logger.info(
                f"[{PIPELINE_NAME}] Fact is up to date ({len(fact_dates)} dates). Nothing to do."
            )
            log_run(PIPELINE_NAME, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        logger.info(
            f"[{PIPELINE_NAME}] {len(dates_to_process)} dates to load "
            f"(fact has {len(fact_dates)}/{len(silver_dates)})."
        )

        for date_val in dates_to_process:
            df = _read_silver_partition(silver_base, date_val, storage_opts)
            if df.empty:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: empty, skipping.")
                continue

            total_input += len(df)
            df = _resolve_keys(df, dims)

            if df.empty:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: all rows dropped after key resolution, skipping.")
                continue

            before_dedup = len(df)
            df = df.sort_values("created_ts", ascending=False).drop_duplicates(
                subset=["event_id", "event_ts"], keep="first"
            )
            if len(df) < before_dedup:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: deduped {before_dedup - len(df):,} duplicate rows from silver.")

            qr = QualityResult(pipeline=PIPELINE_NAME)
            check_not_empty(df, qr)
            check_unique(df, ["event_id", "event_ts"], qr)
            check_no_nulls(df, ["event_id", "event_type", "event_ts"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed for {date_val}:\n{qr.summary()}")

            n_out = _upsert_fact(df, engine)
            total_output += n_out
            logger.info(f"[{PIPELINE_NAME}] {date_val}: {len(df):,} rows → {n_out:,} upserted.")

            del df
            gc.collect()

        del dims
        gc.collect()

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", total_input, total_output, cfg=cfg)
        logger.info(f"[{PIPELINE_NAME}] Done. in={total_input:,} out={total_output:,}")

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
