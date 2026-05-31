"""
src/pipelines/gold/fact_transaction.py
Silver stg_transactions → Gold fact_transaction.
Partition-aware incremental load: one transaction_date at a time to avoid OOM.
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
    QualityResult, check_not_empty, check_unique,
    check_no_nulls, check_referential,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME  = "gold_fact_transaction"
SCHEMA         = "gold_fraud"
UPSERT_CHUNK   = 5_000
LATE_ARRIVAL_DAYS = 2

_FACT_COLS = [
    "transaction_id", "customer_key", "card_key", "merchant_key",
    "transaction_date_key", "status_key", "transaction_timestamp", "created_ts",
    "amount", "currency", "city", "device_fingerprint", "ip_country",
    "is_fraud", "is_approved", "is_declined",
]

_UPSERT_SQL = text(f"""
    INSERT INTO gold_fraud.fact_transaction
        (transaction_id, customer_key, card_key, merchant_key,
         transaction_date_key, status_key, transaction_timestamp, created_ts,
         amount, currency, city, device_fingerprint, ip_country,
         is_fraud, is_approved, is_declined)
    VALUES
        (:transaction_id, :customer_key, :card_key, :merchant_key,
         :transaction_date_key, :status_key, :transaction_timestamp, :created_ts,
         :amount, :currency, :city, :device_fingerprint, :ip_country,
         :is_fraud, :is_approved, :is_declined)
    ON CONFLICT (transaction_id, transaction_date_key) DO UPDATE SET
        customer_key       = EXCLUDED.customer_key,
        card_key           = EXCLUDED.card_key,
        merchant_key       = EXCLUDED.merchant_key,
        status_key         = EXCLUDED.status_key,
        amount             = EXCLUDED.amount,
        device_fingerprint = EXCLUDED.device_fingerprint,
        ip_country         = EXCLUDED.ip_country,
        is_fraud           = EXCLUDED.is_fraud,
        is_approved        = EXCLUDED.is_approved,
        is_declined        = EXCLUDED.is_declined
""")


def _list_silver_partition_dates(silver_base: str, storage_opts) -> list[str]:
    """List silver transaction_date partitions via S3 directory listing — no delta log read."""
    key = "transaction_date="
    if storage_opts is None:
        dirs = glob.glob(f"{silver_base}/stg_transactions/transaction_date=*")
        return sorted(d.rstrip("/").split(key)[-1] for d in dirs)

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    path = f"{silver_base}/stg_transactions".replace("s3://", "")
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
    """Return set of YYYY-MM-DD strings already present in fact_transaction (via transaction_date_key)."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT DISTINCT transaction_date_key FROM {SCHEMA}.fact_transaction"
            )).fetchall()
        return set(
            f"{str(k)[0:4]}-{str(k)[4:6]}-{str(k)[6:]}"
            for (k,) in rows if k is not None
        )
    except Exception:
        return set()


def _read_silver_partition(silver_base: str, date_val: str, storage_opts) -> pd.DataFrame:
    """Read one silver transaction_date partition directly via s3fs."""
    if storage_opts is None:
        files = glob.glob(f"{silver_base}/stg_transactions/transaction_date={date_val}/*.parquet")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()

    import s3fs
    fs = s3fs.S3FileSystem(
        endpoint_url=storage_opts["AWS_ENDPOINT_URL"],
        key=storage_opts["AWS_ACCESS_KEY_ID"],
        secret=storage_opts["AWS_SECRET_ACCESS_KEY"],
        use_ssl=False,
        client_kwargs={"region_name": "us-east-1"},
    )
    path = f"{silver_base}/stg_transactions/transaction_date={date_val}".replace("s3://", "")
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
    dims["status"] = pd.read_sql(
        f"SELECT status_key, status_name FROM {SCHEMA}.dim_transaction_status",
        engine,
    )
    dims["valid_date_keys"] = set(
        pd.read_sql(f"SELECT date_key FROM {SCHEMA}.dim_date", engine)["date_key"]
    )
    return dims


def _resolve_keys(df: pd.DataFrame, dims: dict, dead_letter: str) -> pd.DataFrame:
    qr = QualityResult(pipeline=PIPELINE_NAME)

    df = df.merge(dims["customer"], on="customer_id", how="left")
    df = df.merge(dims["card"], on="card_id", how="left")
    df = df.merge(dims["merchant"], on="merchant_id", how="left")
    df = df.merge(dims["status"].rename(columns={"status_name": "transaction_status"}),
                  on="transaction_status", how="left")

    df["transaction_date_key"] = (
        pd.to_datetime(df["transaction_timestamp"]).dt.strftime("%Y%m%d").astype(int)
    )

    before = len(df)
    df = df[df["transaction_date_key"].isin(dims["valid_date_keys"])].copy()
    dropped = before - len(df)
    if dropped:
        logger.warning(f"[{PIPELINE_NAME}] Dropped {dropped:,} rows with transaction_date_key not in dim_date (corrupt timestamps).")

    df = check_referential(df, "customer_key", set(dims["customer"]["customer_key"]),
                           qr, dead_letter)
    logger.info(qr.summary())
    return df


def _derive_flags(df: pd.DataFrame) -> pd.DataFrame:
    df["is_approved"] = (df["transaction_status"] == "approved").astype("Int8")
    df["is_declined"] = (df["transaction_status"] == "declined").astype("Int8")
    return df


def _upsert_fact(df: pd.DataFrame, engine) -> int:
    df = df[[c for c in _FACT_COLS if c in df.columns]].copy()

    date_keys = df["transaction_date_key"].dropna().unique()
    with engine.begin() as conn:
        for dk in sorted(int(d) for d in date_keys):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_transaction_{dk}
                PARTITION OF {SCHEMA}.fact_transaction
                FOR VALUES FROM ({dk}) TO ({dk + 1});
            """))

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
    dead_letter  = cfg.get("dead_letter_base_path", "data/dead_letter")
    engine       = get_sqlalchemy_engine(cfg)
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    total_input = total_output = 0

    try:
        silver_dates = _list_silver_partition_dates(silver_base, storage_opts)
        if not silver_dates:
            raise ValueError("No partitions found in stg_transactions")

        fact_dates = _get_fact_loaded_dates(engine)

        # Load dims early so we can pre-filter silver partition dates not covered by dim_date
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
            df = _resolve_keys(df, dims, dead_letter)
            df = _derive_flags(df)

            if df.empty:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: all rows dropped after key resolution, skipping.")
                continue

            before_dedup = len(df)
            df = df.sort_values("created_ts", ascending=False).drop_duplicates(
                subset=["transaction_id"], keep="first"
            )
            if len(df) < before_dedup:
                logger.warning(f"[{PIPELINE_NAME}] {date_val}: deduped {before_dedup - len(df):,} duplicate rows from silver.")

            qr = QualityResult(pipeline=PIPELINE_NAME)
            check_not_empty(df, qr)
            check_unique(df, ["transaction_id"], qr)
            check_no_nulls(df, ["transaction_id", "customer_key", "amount",
                                 "transaction_timestamp"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed for {date_val}:\n{qr.summary()}")
            logger.info(qr.summary())

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
