"""
src/pipelines/gold/obt_transaction_fraud_summary.py
Gold fact_transaction + dims → obt_transaction_fraud_summary.
Denormalized table for fraud analyst BI dashboards.
Incremental upsert by transaction_id. Scheduled: every 30 minutes.
"""

import gc
import logging
from datetime import datetime

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

# All OBT columns as per design/02_schema_design.md Section 4
_OBT_COLS = [
    "transaction_id", "transaction_timestamp", "transaction_date_key",
    "customer_id", "customer_city", "customer_risk_segment",
    "merchant_id", "merchant_category",
    "card_type", "issuing_bank", "card_country",
    "amount", "currency", "transaction_status",
    "city", "ip_country", "device_fingerprint",
    "is_fraud", "is_cross_border", "is_night_transaction", "is_high_value",
]

_BUILD_SQL = f"""
    SELECT
        ft.transaction_id,
        ft.transaction_timestamp,
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
        ds.status_name              AS transaction_status,
        ft.city,
        ft.ip_country,
        ft.device_fingerprint,
        ft.is_fraud,
        -- Derived flags
        CASE
            WHEN ft.ip_country IS NOT NULL
             AND ft.ip_country != dcard.card_country THEN 1 ELSE 0
        END::SMALLINT               AS is_cross_border,
        CASE
            WHEN EXTRACT(HOUR FROM ft.transaction_timestamp) BETWEEN 1 AND 4 THEN 1 ELSE 0
        END::SMALLINT               AS is_night_transaction,
        CASE
            WHEN ft.amount > 5000000 THEN 1 ELSE 0
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
"""


def _build_obt(engine) -> pd.DataFrame:
    return pd.read_sql(_BUILD_SQL, engine)


_UPSERT_OBT_SQL = text(f"""
    INSERT INTO {SCHEMA}.obt_transaction_fraud_summary
        (transaction_id, transaction_timestamp, transaction_date_key,
         customer_id, customer_city, customer_risk_segment,
         merchant_id, merchant_category,
         card_type, issuing_bank, card_country,
         amount, currency, transaction_status,
         city, ip_country, device_fingerprint,
         is_fraud, is_cross_border, is_night_transaction, is_high_value)
    VALUES
        (:transaction_id, :transaction_timestamp, :transaction_date_key,
         :customer_id, :customer_city, :customer_risk_segment,
         :merchant_id, :merchant_category,
         :card_type, :issuing_bank, :card_country,
         :amount, :currency, :transaction_status,
         :city, :ip_country, :device_fingerprint,
         :is_fraud, :is_cross_border, :is_night_transaction, :is_high_value)
    ON CONFLICT (transaction_id) DO UPDATE SET
        transaction_timestamp   = EXCLUDED.transaction_timestamp,
        customer_city           = EXCLUDED.customer_city,
        customer_risk_segment   = EXCLUDED.customer_risk_segment,
        merchant_category       = EXCLUDED.merchant_category,
        amount                  = EXCLUDED.amount,
        transaction_status      = EXCLUDED.transaction_status,
        ip_country              = EXCLUDED.ip_country,
        device_fingerprint      = EXCLUDED.device_fingerprint,
        is_fraud                = EXCLUDED.is_fraud,
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


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()

    logger.info(f"[{PIPELINE_NAME}] Starting...")

    try:
        df = _build_obt(engine)
        input_rows = len(df)
        logger.info(f"[{PIPELINE_NAME}] Built {input_rows:,} rows from fact + dims.")

        qr = QualityResult(pipeline=PIPELINE_NAME)
        check_not_empty(df, qr)
        check_unique(df, ["transaction_id"], qr)
        check_no_nulls(df, ["transaction_id", "transaction_timestamp", "amount"], qr)
        if not qr.passed:
            raise ValueError(f"Quality checks failed:\n{qr.summary()}")
        logger.info(qr.summary())
        gc.collect()

        output_rows = _upsert_obt(df, engine)

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", input_rows, output_rows, cfg=cfg)
        logger.info(f"[{PIPELINE_NAME}] Done. in={input_rows:,} out={output_rows:,}")

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
