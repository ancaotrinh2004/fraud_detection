"""
src/pipelines/gold/obt_transaction_fraud_summary.py
Gold fact_transaction + dims → obt_transaction_fraud_summary.
Denormalized table for fraud analyst BI dashboards.
Incremental upsert by transaction_id. Scheduled: every 30 minutes.

Memory note: the OBT is built one transaction_date_key partition at a time
(never a single full-table read). fact_transaction now holds the full streamed
dataset — materialising the whole join in one pandas DataFrame OOM-kills the
PostgreSQL pod (`server terminated abnormally` / `connection refused` on retry).
Building date-by-date keeps both client and server memory flat, mirroring the
partition-aware pattern in fact_transaction.py.
"""

import gc
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_unique, check_no_nulls,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME  = "gold_obt_fraud"
SCHEMA         = "gold_fraud"
UPSERT_CHUNK   = 5_000
# Always reprocess this many recent days so late-arriving transactions AND
# late-arriving fraud labels (chargeback / investigation) refresh into the OBT.
LATE_REFRESH_DAYS = 7

# All OBT columns as per design/02_schema_design.md Section 4
_OBT_COLS = [
    "transaction_id", "transaction_ts", "transaction_date_key",
    "customer_id", "customer_city", "customer_risk_segment",
    "merchant_id", "merchant_category",
    "card_type", "issuing_bank", "card_country",
    "amount", "currency", "amount_base", "transaction_status",
    "city", "ip_country", "device_fingerprint",
    "fraud_label", "label_status",
    "is_cross_border", "is_night_transaction", "is_high_value",
]

_BUILD_SQL = f"""
    SELECT
        ft.transaction_id,
        ft.transaction_ts,
        ft.transaction_date_key,
        dc.customer_id,
        dc.city                     AS customer_city,
        dc.risk_segment             AS customer_risk_segment,
        dm.merchant_id,
        dm.category                 AS merchant_category,
        dcard.card_type,
        dcard.issuing_bank,
        dcard.card_country,
        ft.amount,
        ft.currency,
        ft.amount_base,
        ds.status_name              AS transaction_status,
        ft.city,
        ft.ip_country,
        ft.device_fingerprint,
        -- Fraud label comes from the label store (current best; NULL = not yet labelled)
        lbl.label                   AS fraud_label,
        lbl.label_status,
        -- Derived flags
        CASE
            WHEN ft.ip_country IS NOT NULL
             AND ft.ip_country != dcard.card_country THEN 1 ELSE 0
        END::SMALLINT               AS is_cross_border,
        CASE
            WHEN EXTRACT(HOUR FROM ft.transaction_ts) BETWEEN 1 AND 4 THEN 1 ELSE 0
        END::SMALLINT               AS is_night_transaction,
        CASE
            WHEN ft.amount_base > 5000000 THEN 1 ELSE 0
        END::SMALLINT               AS is_high_value
    FROM {SCHEMA}.fact_transaction ft
    LEFT JOIN {SCHEMA}.dim_customer dc
        ON ft.customer_key = dc.customer_key AND dc.is_current = TRUE
    LEFT JOIN {SCHEMA}.dim_merchant dm
        ON ft.merchant_key = dm.merchant_key AND dm.is_current = TRUE
    LEFT JOIN {SCHEMA}.dim_card dcard
        ON ft.card_key = dcard.card_key
    LEFT JOIN {SCHEMA}.dim_transaction_status ds
        ON ft.status_key = ds.status_key
    LEFT JOIN {SCHEMA}.ml_fraud_label lbl
        ON ft.transaction_id = lbl.transaction_id
    WHERE ft.transaction_date_key = :dk
"""


def _build_obt_for_date(engine, date_key: int) -> pd.DataFrame:
    """Build the OBT slice for a single transaction_date_key partition.

    Filtering on the fact partition column lets PostgreSQL prune to one
    partition and keeps the materialised result small enough to fit in memory.
    """
    return pd.read_sql(text(_BUILD_SQL), engine, params={"dk": int(date_key)})


def _get_fact_date_keys(engine) -> list[int]:
    """Distinct transaction_date_key present in fact_transaction (ascending)."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT DISTINCT transaction_date_key FROM {SCHEMA}.fact_transaction"
            f" WHERE transaction_date_key IS NOT NULL"
        )).fetchall()
    return sorted(int(k) for (k,) in rows)


def _get_obt_date_keys(engine) -> set[int]:
    """Distinct transaction_date_key already materialised in the OBT."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT DISTINCT transaction_date_key"
                f" FROM {SCHEMA}.obt_transaction_fraud_summary"
                f" WHERE transaction_date_key IS NOT NULL"
            )).fetchall()
        return set(int(k) for (k,) in rows)
    except Exception:
        return set()


_UPSERT_OBT_SQL = text(f"""
    INSERT INTO {SCHEMA}.obt_transaction_fraud_summary
        (transaction_id, transaction_ts, transaction_date_key,
         customer_id, customer_city, customer_risk_segment,
         merchant_id, merchant_category,
         card_type, issuing_bank, card_country,
         amount, currency, amount_base, transaction_status,
         city, ip_country, device_fingerprint,
         fraud_label, label_status,
         is_cross_border, is_night_transaction, is_high_value)
    VALUES
        (:transaction_id, :transaction_ts, :transaction_date_key,
         :customer_id, :customer_city, :customer_risk_segment,
         :merchant_id, :merchant_category,
         :card_type, :issuing_bank, :card_country,
         :amount, :currency, :amount_base, :transaction_status,
         :city, :ip_country, :device_fingerprint,
         :fraud_label, :label_status,
         :is_cross_border, :is_night_transaction, :is_high_value)
    ON CONFLICT (transaction_id) DO UPDATE SET
        transaction_ts          = EXCLUDED.transaction_ts,
        customer_city           = EXCLUDED.customer_city,
        customer_risk_segment   = EXCLUDED.customer_risk_segment,
        merchant_category       = EXCLUDED.merchant_category,
        amount                  = EXCLUDED.amount,
        amount_base             = EXCLUDED.amount_base,
        transaction_status      = EXCLUDED.transaction_status,
        ip_country              = EXCLUDED.ip_country,
        device_fingerprint      = EXCLUDED.device_fingerprint,
        fraud_label             = EXCLUDED.fraud_label,
        label_status            = EXCLUDED.label_status,
        is_cross_border         = EXCLUDED.is_cross_border,
        is_night_transaction    = EXCLUDED.is_night_transaction,
        is_high_value           = EXCLUDED.is_high_value
""")


def _upsert_obt(df: pd.DataFrame, engine) -> int:
    df = df[[c for c in _OBT_COLS if c in df.columns]].copy()
    total = len(df)
    rows_inserted = 0
    for start in range(0, total, UPSERT_CHUNK):
        chunk = df.iloc[start:start + UPSERT_CHUNK]
        records = [
            {k: None if isinstance(v, float) and v != v else v for k, v in r.items()}
            for r in chunk.to_dict("records")
        ]
        with engine.begin() as conn:
            conn.execute(_UPSERT_OBT_SQL, records)
        rows_inserted += len(chunk)
        if rows_inserted % 100_000 == 0 or rows_inserted == total:
            logger.info(f"[{PIPELINE_NAME}] Upserted {rows_inserted:,}/{total:,} rows...")
    return rows_inserted


def _safe_log_run(*args, **kwargs) -> None:
    """log_run that never masks the caller's exception.

    On failure the OBT build often leaves PostgreSQL mid-restart, so opening a
    fresh connection here can itself raise `connection refused`. Swallow that so
    the original error propagates instead of being shadowed by the log write.
    """
    try:
        log_run(*args, **kwargs)
    except Exception as log_err:  # pragma: no cover - best-effort audit log
        logger.warning(f"[{PIPELINE_NAME}] Could not write run log: {log_err}")


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    total_input = total_output = 0

    try:
        fact_dates = _get_fact_date_keys(engine)
        if not fact_dates:
            logger.info(f"[{PIPELINE_NAME}] fact_transaction is empty. Nothing to do.")
            _safe_log_run(PIPELINE_NAME, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        obt_dates  = _get_obt_date_keys(engine)
        cutoff_key = int((start_ts - timedelta(days=LATE_REFRESH_DAYS)).strftime("%Y%m%d"))
        dates_to_process = [
            dk for dk in fact_dates
            if dk not in obt_dates or dk >= cutoff_key
        ]

        if not dates_to_process:
            logger.info(
                f"[{PIPELINE_NAME}] OBT is up to date ({len(obt_dates)} dates). Nothing to do."
            )
            _safe_log_run(PIPELINE_NAME, start_ts, datetime.utcnow(), "success", 0, 0, cfg=cfg)
            return

        logger.info(
            f"[{PIPELINE_NAME}] {len(dates_to_process)} date(s) to build "
            f"(OBT has {len(obt_dates)}/{len(fact_dates)})."
        )

        for date_key in dates_to_process:
            df = _build_obt_for_date(engine, date_key)
            if df.empty:
                continue

            total_input += len(df)

            qr = QualityResult(pipeline=PIPELINE_NAME)
            check_not_empty(df, qr)
            check_unique(df, ["transaction_id"], qr)
            check_no_nulls(df, ["transaction_id", "transaction_ts", "amount"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed for {date_key}:\n{qr.summary()}")

            n_out = _upsert_obt(df, engine)
            total_output += n_out
            logger.info(f"[{PIPELINE_NAME}] {date_key}: {len(df):,} rows → {n_out:,} upserted.")

            del df
            gc.collect()

        _safe_log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                      "success", total_input, total_output, cfg=cfg)
        logger.info(f"[{PIPELINE_NAME}] Done. in={total_input:,} out={total_output:,}")

    except Exception as e:
        _safe_log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                      "failed", total_input, total_output, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
