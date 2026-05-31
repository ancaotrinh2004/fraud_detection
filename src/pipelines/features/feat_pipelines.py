"""
src/pipelines/features/feat_pipelines.py
Compute all three feature tables:
  - feat_customer_90d    (every 60 min)
  - feat_stream_30m      (every 5 min)
  - feat_customer_unified (every 15 min)
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.delta import read_delta, get_storage_options

logger = logging.getLogger(__name__)
SCHEMA = "gold_fraud"


# ── feat_customer_90d ─────────────────────────────────────────────────────────

def run_feat_customer_90d(cfg: dict | None = None, as_of_date: str | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    engine       = get_sqlalchemy_engine(cfg)
    silver_base  = cfg["silver_base_path"]
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()
    pipeline     = "feat_customer_90d_pipeline"

    logger.info(f"[{pipeline}] Starting... as_of_date={as_of_date}")
    try:
        df = read_delta(f"{silver_base}/stg_transactions", storage_options=storage_opts)
        df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"])

        if as_of_date:
            as_of_ts = pd.Timestamp(as_of_date)
            df = df[df["transaction_timestamp"] <= as_of_ts]
            now = as_of_ts
        else:
            now = df["transaction_timestamp"].max()

        cutoff = now - timedelta(days=90)
        df90 = df[df["transaction_timestamp"] >= cutoff].copy()

        # Compute features
        feats = (
            df90.groupby("customer_id")
            .agg(
                f_customer_total_txn_90d        =("transaction_id", "count"),
                f_customer_avg_txn_amount_90d   =("amount", "mean"),
                f_customer_distinct_merchants_90d=("merchant_id", "nunique"),
            )
            .reset_index()
        )

        # Decline rate
        decline_rate = (
            df90.groupby("customer_id")
            .apply(lambda g: (g["transaction_status"] == "declined").mean())
            .reset_index(name="f_customer_decline_rate_90d")
        )
        feats = feats.merge(decline_rate, on="customer_id", how="left")

        # Foreign txn ratio (ip_country != card_country)
        df90_new = df90[df90["ip_country"].notna()].copy()
        if not df90_new.empty:
            # Load card_country
            card_country = pd.read_sql(
                f"SELECT card_id, card_country FROM {SCHEMA}.dim_card", engine
            )
            df90_new = df90_new.merge(card_country, on="card_id", how="left")
            df90_new["is_foreign"] = (
                df90_new["ip_country"].str.upper() != df90_new["card_country"].str.upper()
            ).astype(int)
            foreign_rate = (
                df90_new.groupby("customer_id")["is_foreign"].mean()
                .reset_index(name="f_customer_foreign_txn_ratio_90d")
            )
        else:
            foreign_rate = pd.DataFrame(
                columns=["customer_id", "f_customer_foreign_txn_ratio_90d"]
            )
        feats = feats.merge(foreign_rate, on="customer_id", how="left")
        feats["f_customer_foreign_txn_ratio_90d"] = (
            feats["f_customer_foreign_txn_ratio_90d"].fillna(0)
        )

        # Night txn ratio (01:00–04:00)
        df90["hour"] = df90["transaction_timestamp"].dt.hour
        night_rate = (
            df90.groupby("customer_id")
            .apply(lambda g: g["hour"].between(1, 4).mean())
            .reset_index(name="f_customer_night_txn_ratio_90d")
        )
        feats = feats.merge(night_rate, on="customer_id", how="left")

        feats["event_timestamp"] = now
        feats["created_ts"]      = datetime.utcnow()

        # Upsert to PostgreSQL
        _upsert_features(engine, feats, "feat_customer_90d",
                         ["customer_id", "event_timestamp"])

        log_run(pipeline, start_ts, datetime.utcnow(),
                "success", len(df90), len(feats), cfg=cfg)
        logger.info(f"[{pipeline}] Done. {len(feats):,} customer rows.")

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise


# ── feat_stream_30m ───────────────────────────────────────────────────────────

def run_feat_stream_30m(cfg: dict | None = None, as_of_date: str | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    engine       = get_sqlalchemy_engine(cfg)
    silver_base  = cfg["silver_base_path"]
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()
    pipeline     = "feat_stream_30m_pipeline"

    logger.info(f"[{pipeline}] Starting... as_of_date={as_of_date}")
    try:
        # Load only the 2 relevant event_date partitions (current + previous day).
        # Max lookback window is 1h, so we never need more than 2 partitions.
        # Loading the full table (~33M rows) causes OOMKilled in a 4Gi pod.
        if as_of_date:
            as_of_ts  = pd.Timestamp(as_of_date)
            cur_date  = as_of_ts.strftime("%Y-%m-%d")
            prev_date = (as_of_ts - timedelta(days=1)).strftime("%Y-%m-%d")
            now       = as_of_ts
        else:
            now       = pd.Timestamp.utcnow()
            cur_date  = now.strftime("%Y-%m-%d")
            prev_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

        silver_events = f"{silver_base}/stg_fraud_events"
        parts = []
        for d in [prev_date, cur_date]:
            chunk = read_delta(silver_events, storage_options=storage_opts,
                               partitions=[("event_date", "=", d)])
            if not chunk.empty:
                parts.append(chunk)

        if not parts:
            logger.warning(f"[{pipeline}] No events found for {prev_date}/{cur_date}, skipping.")
            log_run(pipeline, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        df = pd.concat(parts, ignore_index=True)
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        df = df[df["event_timestamp"] <= now]

        w30m   = now - timedelta(minutes=30)
        w1h    = now - timedelta(hours=1)

        df30m  = df[df["event_timestamp"] >= w30m].copy()
        df1h   = df[df["event_timestamp"] >= w1h].copy()

        # OTP failed count (30m)
        otp_counts = (
            df30m[df30m["event_type"] == "otp_failed"]
            .groupby("customer_id")
            .size()
            .reset_index(name="f_stream_otp_failed_count_30m")
        )

        # Decline count (30m)
        decline_counts = (
            df30m[df30m["event_type"] == "transaction_declined"]
            .groupby("customer_id")
            .size()
            .reset_index(name="f_stream_decline_count_30m")
        )

        # Transaction velocity (1h)
        velocity = (
            df1h[df1h["event_type"] == "transaction_attempt"]
            .groupby("customer_id")
            .size()
            .reset_index(name="f_stream_txn_velocity_1h")
        )

        # All active customers in window
        all_customers = df30m[["customer_id"]].drop_duplicates()
        feats = (
            all_customers
            .merge(otp_counts,    on="customer_id", how="left")
            .merge(decline_counts, on="customer_id", how="left")
            .merge(velocity,      on="customer_id", how="left")
            .fillna(0)
        )

        # Burst activity flag (12:00–12:20, 22:00–22:20)
        def _is_burst(ts):
            h, m = ts.hour, ts.minute
            return int((h == 12 and m <= 20) or (h == 22 and m <= 20))

        feats["f_stream_burst_activity_flag"] = int(_is_burst(now))

        # New merchant flag — merchant not seen in customer's 90d history
        # Approximation: flag if customer only has 1 distinct merchant in 30m window
        merch_diversity = (
            df30m[df30m["merchant_id"].notna()]
            .groupby("customer_id")["merchant_id"]
            .nunique()
            .reset_index(name="_merch_count_30m")
        )
        feats = feats.merge(merch_diversity, on="customer_id", how="left")
        feats["f_stream_new_merchant_flag"] = (
            feats["_merch_count_30m"].fillna(0) == 1
        ).astype(int)
        feats = feats.drop(columns=["_merch_count_30m"])

        feats["event_timestamp"] = now
        feats["created_ts"]      = datetime.utcnow()

        # Cast int columns
        for col in ["f_stream_otp_failed_count_30m", "f_stream_decline_count_30m",
                    "f_stream_txn_velocity_1h"]:
            feats[col] = feats[col].astype(int)

        _upsert_features(engine, feats, "feat_stream_30m",
                         ["customer_id", "event_timestamp"])

        log_run(pipeline, start_ts, datetime.utcnow(),
                "success", len(df30m), len(feats), cfg=cfg)
        logger.info(f"[{pipeline}] Done. {len(feats):,} rows.")

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise


# ── feat_customer_unified ─────────────────────────────────────────────────────

def run_feat_unified(cfg: dict | None = None, as_of_date: str | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()
    pipeline = "feat_unified_pipeline"

    logger.info(f"[{pipeline}] Starting... as_of_date={as_of_date}")
    try:
        ts_filter_90d = f"AND event_timestamp <= '{as_of_date}'" if as_of_date else ""
        ts_filter_30m = f"AND event_timestamp <= '{as_of_date}'" if as_of_date else ""

        f90d = pd.read_sql(
            f"""SELECT * FROM {SCHEMA}.feat_customer_90d
                WHERE (customer_id, event_timestamp) IN (
                    SELECT customer_id, MAX(event_timestamp)
                    FROM {SCHEMA}.feat_customer_90d
                    WHERE TRUE {ts_filter_90d}
                    GROUP BY customer_id
                )""",
            engine,
        )
        f30m = pd.read_sql(
            f"""SELECT * FROM {SCHEMA}.feat_stream_30m
                WHERE (customer_id, event_timestamp) IN (
                    SELECT customer_id, MAX(event_timestamp)
                    FROM {SCHEMA}.feat_stream_30m
                    WHERE TRUE {ts_filter_30m}
                    GROUP BY customer_id
                )""",
            engine,
        )

        # Point-in-time join: use earliest of the two event_timestamps
        merged = f90d.merge(
            f30m,
            on="customer_id",
            how="outer",
            suffixes=("_90d", "_30m"),
        )

        # Use the later snapshot timestamp as unified event_timestamp
        merged["event_timestamp"] = merged[["event_timestamp_90d", "event_timestamp_30m"]].max(axis=1)
        merged["created_ts"]      = datetime.utcnow()

        # Select final columns
        feat_cols = [
            "customer_id", "event_timestamp", "created_ts",
            "f_customer_total_txn_90d", "f_customer_avg_txn_amount_90d",
            "f_customer_distinct_merchants_90d", "f_customer_decline_rate_90d",
            "f_customer_foreign_txn_ratio_90d", "f_customer_night_txn_ratio_90d",
            "f_stream_otp_failed_count_30m", "f_stream_decline_count_30m",
            "f_stream_txn_velocity_1h", "f_stream_new_merchant_flag",
            "f_stream_burst_activity_flag",
        ]
        unified = merged[[c for c in feat_cols if c in merged.columns]]
        unified = unified.fillna(0)

        _upsert_features(engine, unified, "feat_customer_unified",
                         ["customer_id", "event_timestamp"])

        log_run(pipeline, start_ts, datetime.utcnow(),
                "success", len(f90d) + len(f30m), len(unified), cfg=cfg)
        logger.info(f"[{pipeline}] Done. {len(unified):,} unified rows.")

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise


# ── Generic upsert helper ─────────────────────────────────────────────────────

_UPSERT_CHUNK = 5_000  # rows per bulk INSERT statement


def _upsert_features(engine, df: pd.DataFrame, table: str, pk_cols: list[str]) -> None:
    """Bulk-upsert DataFrame rows into a Gold feature table.

    Uses a single INSERT ... ON CONFLICT per chunk instead of one SQL call per
    row. For 120K-row feature tables this reduces ~120K roundtrips to ~24 calls.
    """
    if df.empty:
        return

    cols   = df.columns.tolist()
    non_pk = [c for c in cols if c not in pk_cols]
    col_str = ", ".join(cols)
    val_str = ", ".join(f":{c}" for c in cols)
    upd_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
    pk_str  = ", ".join(pk_cols)

    sql = text(f"""
        INSERT INTO {SCHEMA}.{table} ({col_str})
        VALUES ({val_str})
        ON CONFLICT ({pk_str}) DO UPDATE SET {upd_str}
    """)

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        for start in range(0, len(records), _UPSERT_CHUNK):
            conn.execute(sql, records[start : start + _UPSERT_CHUNK])