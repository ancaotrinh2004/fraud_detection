"""
src/pipelines/bronze/ingest_events.py
Micro-batch load: fraud_events.json → Delta Lake Bronze.
Reads only records with event_timestamp > last ingested ts.
Processes in chunks to avoid OOM on large files.
Scheduled: every 5 minutes.
"""

import logging
import uuid
from datetime import datetime

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, log_run
from src.pipelines.utils.delta import (
    write_bronze, get_last_created_watermark,
    get_storage_options, get_pandas_s3_opts,
)
from src.pipelines.utils.quality import (
    QualityResult, check_not_empty, check_schema, check_no_nulls,
)

logger = logging.getLogger(__name__)

REQUIRED_COLS = [
    "event_id", "event_type", "event_timestamp",
    "created_ts", "customer_id", "session_id", "device_type", "ip_country",
]

PIPELINE_NAME = "bronze_ingest_events"
CHUNK_SIZE    = 50_000  # rows per chunk — ~50MB in memory per batch


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    raw_base     = cfg.get("raw_base_path", "data/raw")
    source_path  = f"{raw_base}/streaming/fraud_events.json"
    bronze_base  = cfg["bronze_base_path"]
    table_path   = f"{bronze_base}/raw_fraud_events"
    storage_opts = get_storage_options(cfg)
    pd_s3_opts   = get_pandas_s3_opts(cfg)
    start_ts     = datetime.utcnow()
    batch_id     = str(uuid.uuid4())

    logger.info(f"[{PIPELINE_NAME}] Starting (chunk_size={CHUNK_SIZE:,})...")

    total_input = total_output = 0
    first_chunk = True

    try:
        last_ts = get_last_created_watermark(table_path, storage_options=storage_opts)
        if last_ts is not None:
            logger.info(f"[{PIPELINE_NAME}] Incremental: created_ts watermark = {last_ts}")

        reader = pd.read_json(
            source_path,
            lines=True,
            chunksize=CHUNK_SIZE,
            storage_options=pd_s3_opts,
        )

        for chunk_num, chunk in enumerate(reader, 1):
            chunk["event_timestamp"] = pd.to_datetime(chunk["event_timestamp"])
            chunk["created_ts"]      = pd.to_datetime(chunk["created_ts"])
            total_input += len(chunk)

            # Incremental filter
            if last_ts is not None:
                chunk = chunk[chunk["created_ts"] > pd.Timestamp(last_ts)]

            if chunk.empty:
                continue

            # Quality checks on first non-empty chunk only (schema validation)
            if first_chunk:
                qr = QualityResult(pipeline=PIPELINE_NAME)
                check_not_empty(chunk, qr)
                check_schema(chunk, REQUIRED_COLS, qr)
                check_no_nulls(chunk, ["event_id", "event_type", "event_timestamp", "customer_id"], qr)
                if not qr.passed:
                    raise ValueError(f"Quality checks failed:\n{qr.summary()}")
                logger.info(qr.summary())
                first_chunk = False

            chunk["ingest_ts"]  = datetime.utcnow()
            chunk["batch_id"]   = batch_id
            chunk["event_date"] = chunk["event_timestamp"].dt.date.astype(str)

            write_bronze(chunk, table_path, storage_options=storage_opts,
                         partition_by=["event_date"])
            total_output += len(chunk)

            if chunk_num % 10 == 0:
                logger.info(f"[{PIPELINE_NAME}] Processed {chunk_num} chunks, {total_output:,} rows written...")

        if total_output == 0:
            logger.info(f"[{PIPELINE_NAME}] No new events. Skipping.")
            log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                    "success", total_input, 0, cfg=cfg)
            return

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", total_input, total_output, cfg=cfg)
        logger.info(f"[{PIPELINE_NAME}] Done. in={total_input:,} out={total_output:,}")

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", total_input, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
