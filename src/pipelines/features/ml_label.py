"""
src/pipelines/features/ml_label.py

Create ML tables for fraud detection:
  - ml_fraud_label:    one row per transaction, label = is_fraud
  - ml_fraud_training: PIT join of ml_fraud_label + feat_customer_90d
                       + per-transaction streaming features from stg_fraud_events
"""

import logging
from datetime import datetime


import numpy as np
import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.delta import read_delta, get_storage_options
from src.pipelines.utils.metrics import push_metrics

logger = logging.getLogger(__name__)
SCHEMA = "gold_fraud"


def _compute_per_txn_streaming_features(txn_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-transaction streaming features at actual transaction time.

    For each transaction row in txn_df (needs transaction_id, customer_id,
    event_timestamp), count events from stg_fraud_events within sliding windows:
      - f_stream_otp_failed_count_30m  : otp_failed events in [txn_ts - 30min, txn_ts)
      - f_stream_decline_count_30m     : transaction_declined events in [txn_ts - 30min, txn_ts)
      - f_stream_txn_velocity_1h       : transaction_attempt events in [txn_ts - 1h, txn_ts)
      - f_stream_new_merchant_flag     : 1 if ≤1 distinct merchant in 30m window
      - f_stream_burst_activity_flag   : 1 if txn hour is 12:00–12:20 or 22:00–22:20

    Uses numpy searchsorted per customer for O(N log M) performance.
    """
    W30M_NS = int(pd.Timedelta(minutes=30).total_seconds() * 1e9)
    W1H_NS  = int(pd.Timedelta(hours=1).total_seconds() * 1e9)
    _EMPTY  = np.array([], dtype="int64")

    # Pre-group events by customer and event type
    def _build_ts_map(ev_df):
        return {
            cid: g["event_timestamp"].sort_values().values.astype("int64")
            for cid, g in ev_df.groupby("customer_id")
        }

    otp_map = _build_ts_map(events_df[events_df["event_type"] == "otp_failed"])
    dec_map = _build_ts_map(events_df[events_df["event_type"] == "transaction_declined"])
    vel_map = _build_ts_map(events_df[events_df["event_type"] == "transaction_attempt"])

    # For new_merchant_flag: store (sorted_ts_array, merchant_id_array) per customer
    merch_ev = events_df[events_df["merchant_id"].notna()].copy()
    merch_map: dict = {}
    for cid, g in merch_ev.groupby("customer_id"):
        g_sorted = g.sort_values("event_timestamp")
        merch_map[cid] = (
            g_sorted["event_timestamp"].values.astype("int64"),
            g_sorted["merchant_id"].values,
        )

    results = []
    for cid, txn_group in txn_df.groupby("customer_id"):
        txn_sorted = txn_group.sort_values("event_timestamp")
        txn_ts     = txn_sorted["event_timestamp"].values.astype("int64")

        otp_ts = otp_map.get(cid, _EMPTY)
        dec_ts = dec_map.get(cid, _EMPTY)
        vel_ts = vel_map.get(cid, _EMPTY)

        otp_hi = np.searchsorted(otp_ts, txn_ts,           side="right")
        otp_lo = np.searchsorted(otp_ts, txn_ts - W30M_NS, side="left")

        dec_hi = np.searchsorted(dec_ts, txn_ts,           side="right")
        dec_lo = np.searchsorted(dec_ts, txn_ts - W30M_NS, side="left")

        vel_hi = np.searchsorted(vel_ts, txn_ts,          side="right")
        vel_lo = np.searchsorted(vel_ts, txn_ts - W1H_NS, side="left")

        # burst_activity_flag from transaction timestamp
        ts_pd = txn_sorted["event_timestamp"]
        burst = (
            ((ts_pd.dt.hour == 12) & (ts_pd.dt.minute <= 20)) |
            ((ts_pd.dt.hour == 22) & (ts_pd.dt.minute <= 20))
        ).astype(int).values

        # new_merchant_flag: distinct merchants in 30m window — flag if ≤1
        if cid in merch_map:
            m_ts, m_ids = merch_map[cid]
            m_hi = np.searchsorted(m_ts, txn_ts,           side="right")
            m_lo = np.searchsorted(m_ts, txn_ts - W30M_NS, side="left")
            new_merch = np.array([
                int(len(set(m_ids[lo:hi])) <= 1)
                for lo, hi in zip(m_lo, m_hi)
            ])
        else:
            new_merch = np.zeros(len(txn_ts), dtype="int64")

        cust_df = txn_sorted[["transaction_id"]].copy()
        cust_df["f_stream_otp_failed_count_30m"] = (otp_hi - otp_lo).astype(int)
        cust_df["f_stream_decline_count_30m"]    = (dec_hi - dec_lo).astype(int)
        cust_df["f_stream_txn_velocity_1h"]      = (vel_hi - vel_lo).astype(int)
        cust_df["f_stream_burst_activity_flag"]  = burst
        cust_df["f_stream_new_merchant_flag"]    = new_merch
        results.append(cust_df)

    if not results:
        return pd.DataFrame(columns=[
            "transaction_id", "f_stream_otp_failed_count_30m",
            "f_stream_decline_count_30m", "f_stream_txn_velocity_1h",
            "f_stream_burst_activity_flag", "f_stream_new_merchant_flag",
        ])
    return pd.concat(results, ignore_index=True)


def run_ml_label(cfg: dict | None = None) -> None:
    """Populate ml_fraud_label from fact_transaction."""
    if cfg is None:
        cfg = load_pipeline_config()
    engine = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()
    pipeline = "ml_label_pipeline"

    logger.info(f"[{pipeline}] Building ml_fraud_label from fact_transaction...")
    try:
        df = pd.read_sql(
            f"""
            SELECT
                transaction_id,
                customer_key,
                transaction_timestamp   AS event_timestamp,
                created_ts,
                is_fraud                AS label
            FROM {SCHEMA}.fact_transaction
            """,
            engine,
        )

        # Resolve customer_id from customer_key
        dim = pd.read_sql(
            f"SELECT customer_key, customer_id FROM {SCHEMA}.dim_customer WHERE is_current",
            engine,
        )
        df = df.merge(dim, on="customer_key", how="left")
        df = df[["transaction_id", "customer_id", "event_timestamp", "created_ts", "label"]]
        df = df.dropna(subset=["customer_id"])

        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE {SCHEMA}.ml_fraud_label"))
            rows = df.to_dict(orient="records")
            chunk = 5000
            for i in range(0, len(rows), chunk):
                conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.ml_fraud_label
                        (transaction_id, customer_id, event_timestamp, created_ts, label)
                    VALUES (:transaction_id, :customer_id, :event_timestamp, :created_ts, :label)
                    ON CONFLICT (transaction_id) DO NOTHING
                """), rows[i: i + chunk])

        log_run(pipeline, start_ts, datetime.utcnow(), "success", len(df), len(df), cfg=cfg)
        logger.info(f"[{pipeline}] Done. {len(df):,} label rows written.")

        push_metrics(pipeline, {
            "fraud_ml_label_rows_total":  len(df),
            "fraud_ml_label_fraud_count": int(df["label"].sum()),
            "fraud_ml_label_fraud_rate":  round(df["label"].mean(), 4),
        })

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise


def run_ml_training(cfg: dict | None = None) -> None:
    """
    Build ml_fraud_training in two steps:
    1. SQL PIT join against feat_customer_90d for batch features + transaction context.
    2. Per-transaction streaming features computed at actual txn time from stg_fraud_events
       (fixes the monthly-snapshot problem that zeroed out streaming signal).
    """
    if cfg is None:
        cfg = load_pipeline_config()
    engine       = get_sqlalchemy_engine(cfg)
    silver_base  = cfg["silver_base_path"]
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()
    pipeline     = "ml_training_pipeline"

    logger.info(f"[{pipeline}] Building ml_fraud_training...")
    try:
        # ── Step 1: PIT join for 90d batch features ────────────────────────────
        logger.info(f"[{pipeline}] PIT join against feat_customer_90d...")
        df = pd.read_sql(f"""
            SELECT
                l.transaction_id,
                l.customer_id,
                l.event_timestamp,
                l.label,
                f.f_customer_total_txn_90d,
                f.f_customer_avg_txn_amount_90d,
                f.f_customer_distinct_merchants_90d,
                f.f_customer_decline_rate_90d,
                f.f_customer_foreign_txn_ratio_90d,
                f.f_customer_night_txn_ratio_90d,
                f.event_timestamp                                       AS feature_snapshot_ts,
                t.amount                                                AS txn_amount,
                EXTRACT(HOUR FROM l.event_timestamp)::SMALLINT          AS txn_hour,
                COALESCE(t.is_declined, 0)                              AS is_declined_txn,
                CASE WHEN t.ip_country IS NOT NULL
                          AND dc.card_country IS NOT NULL
                          AND LOWER(t.ip_country) != LOWER(dc.card_country)
                     THEN 1 ELSE 0 END                                  AS is_foreign_txn
            FROM {SCHEMA}.ml_fraud_label l
            LEFT JOIN LATERAL (
                SELECT *
                FROM {SCHEMA}.feat_customer_90d u
                WHERE u.customer_id = l.customer_id
                  AND u.event_timestamp <= l.event_timestamp
                ORDER BY u.event_timestamp DESC
                LIMIT 1
            ) f ON TRUE
            LEFT JOIN {SCHEMA}.fact_transaction t
                ON t.transaction_id = l.transaction_id
            LEFT JOIN {SCHEMA}.dim_card dc
                ON dc.card_key = t.card_key
        """, engine)
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        logger.info(f"[{pipeline}] PIT join complete — {len(df):,} rows.")

        # ── Step 2: per-transaction streaming features from Delta Lake ─────────
        # Process date-by-date to avoid loading all 33.5M events at once (OOM).
        # Each transaction's 30m/1h window only ever spans 2 calendar days.
        logger.info(f"[{pipeline}] Computing per-txn streaming features date-by-date...")
        silver_events = f"{silver_base}/stg_fraud_events"
        df["_txn_date"] = df["event_timestamp"].dt.date
        txn_dates = sorted(df["_txn_date"].unique())
        all_stream_feats = []

        for txn_date in txn_dates:
            txn_slice    = df[df["_txn_date"] == txn_date]
            cur_date_str = str(txn_date)
            prev_date_str = (pd.Timestamp(txn_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

            parts = []
            for d in [prev_date_str, cur_date_str]:
                chunk = read_delta(silver_events, storage_options=storage_opts,
                                   partitions=[("event_date", "=", d)])
                if not chunk.empty:
                    parts.append(
                        chunk[["customer_id", "event_type", "event_timestamp", "merchant_id"]]
                    )

            if not parts:
                zeros = txn_slice[["transaction_id"]].copy()
                for col in ["f_stream_otp_failed_count_30m", "f_stream_decline_count_30m",
                            "f_stream_txn_velocity_1h", "f_stream_burst_activity_flag",
                            "f_stream_new_merchant_flag"]:
                    zeros[col] = 0
                all_stream_feats.append(zeros)
                continue

            ev_slice = pd.concat(parts, ignore_index=True)
            ev_slice["event_timestamp"] = pd.to_datetime(ev_slice["event_timestamp"])
            feats = _compute_per_txn_streaming_features(txn_slice, ev_slice)
            all_stream_feats.append(feats)

        df = df.drop(columns=["_txn_date"])
        stream_feats = pd.concat(all_stream_feats, ignore_index=True)
        logger.info(f"[{pipeline}] Streaming features computed — {len(stream_feats):,} rows.")

        df = df.merge(stream_feats, on="transaction_id", how="left")
        for col in ["f_stream_otp_failed_count_30m", "f_stream_decline_count_30m",
                    "f_stream_txn_velocity_1h", "f_stream_new_merchant_flag",
                    "f_stream_burst_activity_flag"]:
            df[col] = df[col].fillna(0).astype(int)
        df["created_ts"] = datetime.utcnow()

        # ── Step 3: write to ml_fraud_training ────────────────────────────────
        insert_cols = [
            "transaction_id", "customer_id", "event_timestamp", "label",
            "f_customer_total_txn_90d", "f_customer_avg_txn_amount_90d",
            "f_customer_distinct_merchants_90d", "f_customer_decline_rate_90d",
            "f_customer_foreign_txn_ratio_90d", "f_customer_night_txn_ratio_90d",
            "f_stream_otp_failed_count_30m", "f_stream_decline_count_30m",
            "f_stream_txn_velocity_1h", "f_stream_new_merchant_flag",
            "f_stream_burst_activity_flag",
            "feature_snapshot_ts", "txn_amount", "txn_hour",
            "is_declined_txn", "is_foreign_txn", "created_ts",
        ]
        col_str = ", ".join(insert_cols)
        val_str = ", ".join(f":{c}" for c in insert_cols)

        def _clean(v):
            """Convert NaT and float NaN to None for psycopg2 NULL serialization."""
            if v is pd.NaT:
                return None
            if isinstance(v, float) and np.isnan(v):
                return None
            return v

        rows = [
            {k: _clean(v) for k, v in row.items()}
            for row in df[insert_cols].to_dict(orient="records")
        ]

        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE {SCHEMA}.ml_fraud_training"))
            chunk = 5000
            for i in range(0, len(rows), chunk):
                conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.ml_fraud_training ({col_str})
                    VALUES ({val_str})
                    ON CONFLICT (transaction_id) DO NOTHING
                """), rows[i: i + chunk])

        log_run(pipeline, start_ts, datetime.utcnow(), "success", 0, len(df), cfg=cfg)
        logger.info(f"[{pipeline}] Done. {len(df):,} training rows written.")

        push_metrics(pipeline, {
            "fraud_ml_training_rows_total":          len(df),
            "fraud_ml_training_stream_feats_rows":   len(stream_feats),
        })

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise
