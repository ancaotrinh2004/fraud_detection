"""
src/pipelines/bronze/ingest_reference.py
Load customers, merchants, cards Parquet → Delta Lake Bronze.
Scheduled: daily 01:00.
"""

import gc
import logging
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, log_run
from src.pipelines.utils.delta import write_bronze, get_storage_options, get_pandas_s3_opts
from src.pipelines.utils.quality import QualityResult, check_not_empty, check_schema

logger = logging.getLogger(__name__)

_TABLES = {
    "customers": {
        "source_file": "offline/customers.parquet",
        "required_cols": ["customer_id", "signup_ts", "country", "city",
                          "risk_segment", "kyc_status"],
    },
    "merchants": {
        "source_file": "offline/merchants.parquet",
        "required_cols": ["merchant_id", "merchant_name", "category",
                          "country", "city", "is_active"],
    },
    "cards": {
        "source_file": "offline/cards.parquet",
        "required_cols": ["card_id", "customer_id", "card_type",
                          "issuing_bank", "card_country"],
    },
}


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    raw_base     = cfg.get("raw_base_path", "data/raw")
    bronze_base  = cfg["bronze_base_path"]
    storage_opts = get_storage_options(cfg)
    pd_s3_opts   = get_pandas_s3_opts(cfg)
    start_ts     = datetime.utcnow()

    for table_name, meta in _TABLES.items():
        pipeline_name = f"bronze_ingest_{table_name}"
        source_path   = f"{raw_base}/{meta['source_file']}"
        logger.info(f"[{pipeline_name}] Starting...")

        try:
            df = pd.read_parquet(source_path, storage_options=pd_s3_opts)

            qr = QualityResult(pipeline=pipeline_name)
            check_not_empty(df, qr)
            check_schema(df, meta["required_cols"], qr)
            if not qr.passed:
                raise ValueError(f"Quality checks failed:\n{qr.summary()}")

            # Add ingest metadata
            df["ingest_ts"] = datetime.utcnow()
            df["batch_id"] = str(uuid.uuid4())
            df["source_file"] = source_path

            table_path = f"{bronze_base}/raw_{table_name}"
            write_bronze(df, table_path, storage_options=storage_opts)

            row_count = len(df)
            log_run(pipeline_name, start_ts, datetime.utcnow(),
                    "success", row_count, row_count, cfg=cfg)
            logger.info(f"[{pipeline_name}] Done. {row_count:,} rows.")
            del df
            gc.collect()

        except Exception as e:
            log_run(pipeline_name, start_ts, datetime.utcnow(),
                    "failed", 0, 0, str(e)[:500], cfg=cfg)
            logger.error(f"[{pipeline_name}] FAILED: {e}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()