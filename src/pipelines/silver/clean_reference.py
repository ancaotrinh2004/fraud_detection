"""
src/pipelines/silver/clean_reference.py
Bronze raw_customers / raw_merchants / raw_cards → Silver stg_* tables.
Standardize casing, validate enums, flag expired cards, dedup by BK.
Scheduled: daily 02:00.
"""

import logging
from datetime import datetime

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, log_run
from src.pipelines.utils.delta import read_delta, write_silver, get_storage_options
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_unique,
    check_no_nulls, check_schema,
)

logger = logging.getLogger(__name__)

VALID_RISK_SEGMENTS = {"low", "medium", "high"}
VALID_KYC_STATUSES  = {"verified", "pending", "rejected"}
VALID_CARD_TYPES    = {"credit", "debit", "prepaid"}


# ── customers ────────────────────────────────────────────────────────────────

def _clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["customer_id"]   = df["customer_id"].str.strip()
    df["country"]       = df["country"].str.strip().str.upper()
    df["city"]          = df["city"].str.strip().str.title()
    df["risk_segment"]  = df["risk_segment"].str.strip().str.lower()
    df["kyc_status"]    = df["kyc_status"].str.strip().str.lower()

    # Replace invalid enums with 'unknown' rather than rejecting the whole row
    df.loc[~df["risk_segment"].isin(VALID_RISK_SEGMENTS), "risk_segment"] = "unknown"
    df.loc[~df["kyc_status"].isin(VALID_KYC_STATUSES),   "kyc_status"]   = "unknown"

    df["signup_ts"] = pd.to_datetime(df["signup_ts"], errors="coerce")
    df["stg_processed_ts"] = datetime.utcnow()
    # Dedup: keep latest ingest_ts per customer_id
    df = (df.sort_values("ingest_ts", ascending=False)
            .drop_duplicates(subset=["customer_id"], keep="first"))
    return df


def _qc_customers(df: pd.DataFrame, pipeline: str) -> QualityResult:
    qr = QualityResult(pipeline=pipeline)
    check_not_empty(df, qr)
    check_schema(df, ["customer_id", "country", "risk_segment", "kyc_status"], qr)
    check_unique(df, ["customer_id"], qr)
    check_no_nulls(df, ["customer_id", "country"], qr)
    return qr


# ── merchants ────────────────────────────────────────────────────────────────

def _clean_merchants(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["merchant_id"]   = df["merchant_id"].str.strip()
    df["merchant_name"] = df["merchant_name"].str.strip().str.title()
    df["category"]      = df["category"].str.strip().str.lower()
    df["country"]       = df["country"].str.strip().str.upper()
    df["city"]          = df["city"].str.strip().str.title()
    df["is_active"]     = df["is_active"].astype(bool)

    df["stg_processed_ts"] = datetime.utcnow()
    df = (df.sort_values("ingest_ts", ascending=False)
            .drop_duplicates(subset=["merchant_id"], keep="first"))
    return df


def _qc_merchants(df: pd.DataFrame, pipeline: str) -> QualityResult:
    qr = QualityResult(pipeline=pipeline)
    check_not_empty(df, qr)
    check_schema(df, ["merchant_id", "merchant_name", "category", "is_active"], qr)
    check_unique(df, ["merchant_id"], qr)
    check_no_nulls(df, ["merchant_id", "category"], qr)
    return qr


# ── cards ────────────────────────────────────────────────────────────────────

def _clean_cards(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["card_id"]      = df["card_id"].str.strip()
    df["customer_id"]  = df["customer_id"].str.strip()
    df["card_type"]    = df["card_type"].str.strip().str.lower()
    df["issuing_bank"] = df["issuing_bank"].str.strip().str.title()
    df["card_country"] = df["card_country"].str.strip().str.upper()

    df.loc[~df["card_type"].isin(VALID_CARD_TYPES), "card_type"] = "unknown"

    df["expiry_ts"] = pd.to_datetime(df["expiry_ts"], errors="coerce")
    now = datetime.utcnow()
    df["is_active"] = df["expiry_ts"].apply(
        lambda ts: bool(pd.notna(ts) and ts > now)
    )

    df["stg_processed_ts"] = datetime.utcnow()
    df = (df.sort_values("ingest_ts", ascending=False)
            .drop_duplicates(subset=["card_id"], keep="first"))
    return df


def _qc_cards(df: pd.DataFrame, pipeline: str) -> QualityResult:
    qr = QualityResult(pipeline=pipeline)
    check_not_empty(df, qr)
    check_schema(df, ["card_id", "customer_id", "card_type", "card_country"], qr)
    check_unique(df, ["card_id"], qr)
    check_no_nulls(df, ["card_id", "customer_id"], qr)
    return qr


# ── Main ─────────────────────────────────────────────────────────────────────

_TABLES = [
    ("customers", "raw_customers", "stg_customers", _clean_customers, _qc_customers),
    ("merchants", "raw_merchants", "stg_merchants", _clean_merchants, _qc_merchants),
    ("cards",     "raw_cards",     "stg_cards",     _clean_cards,     _qc_cards),
]


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    bronze_base  = cfg["bronze_base_path"]
    silver_base  = cfg["silver_base_path"]
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()

    for entity, raw_name, stg_name, clean_fn, qc_fn in _TABLES:
        pipeline_name = f"silver_clean_{entity}"
        logger.info(f"[{pipeline_name}] Starting...")

        try:
            df = read_delta(f"{bronze_base}/{raw_name}", storage_options=storage_opts)
            input_rows = len(df)

            df = clean_fn(df)

            qr = qc_fn(df, pipeline_name)
            if not qr.passed:
                raise ValueError(f"Quality checks failed:\n{qr.summary()}")
            logger.info(qr.summary())

            write_silver(df, f"{silver_base}/{stg_name}",
                         merge_keys=[entity[:-1] + "_id"],
                         storage_options=storage_opts)

            log_run(pipeline_name, start_ts, datetime.utcnow(),
                    "success", input_rows, len(df), cfg=cfg)
            logger.info(f"[{pipeline_name}] Done. in={input_rows:,} out={len(df):,}")

        except Exception as e:
            log_run(pipeline_name, start_ts, datetime.utcnow(),
                    "failed", 0, 0, str(e)[:500], cfg=cfg)
            logger.error(f"[{pipeline_name}] FAILED: {e}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
