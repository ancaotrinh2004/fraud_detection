"""
src/pipelines/bronze/kafka_consumer.py

Kafka consumer for fraud.events.raw → Delta Lake Bronze (raw_fraud_events).
Called by ingest_events.py when kafka.enabled=true in pipeline_config.yaml.

Pattern: micro-batch — triggered every 5 min by Airflow, polls for up to
`poll_timeout_ms` milliseconds, writes all received messages to Bronze.
"""

import json
import logging
import uuid
from datetime import datetime

import pandas as pd

from src.pipelines.utils.delta import write_bronze, get_storage_options
from src.pipelines.utils.db import log_run
from src.pipelines.utils.quality import QualityResult, check_not_empty, check_schema, check_no_nulls

logger = logging.getLogger(__name__)

PIPELINE_NAME = "bronze_ingest_events_kafka"

REQUIRED_COLS = [
    "event_id", "event_type", "event_timestamp",
    "created_ts", "customer_id", "session_id", "device_type", "ip_country",
]

# Stable Bronze schema. Events are multi-type (login / otp / transaction / ...),
# so any given micro-batch may have a column that is entirely null (e.g. a batch
# of login events has no merchant_id/card_id/failure_reason). Without an explicit
# schema, pandas→Arrow infers the 'null' type for such columns — which Delta Lake
# rejects ("Invalid data type for Delta Lake: Null") — and the inferred type can
# drift between float64 (all-NaN) and string across batches. Pinning dtypes here
# keeps every write schema-consistent.
_STRING_COLS = [
    "event_id", "event_type", "customer_id", "session_id",
    "device_type", "ip_country", "merchant_id", "card_id",
    "failure_reason", "batch_id", "event_date",
]
_FLOAT_COLS = ["amount"]


def run(cfg: dict) -> None:
    kafka_cfg     = cfg["kafka"]
    bootstrap     = kafka_cfg["bootstrap_servers"]
    topic         = kafka_cfg["topic_fraud_events"]
    group_id      = kafka_cfg["consumer_group"]
    poll_timeout  = kafka_cfg.get("poll_timeout_ms", 30_000)
    batch_size    = kafka_cfg.get("batch_size", 1000)
    bronze_base   = cfg["bronze_base_path"]
    table_path    = f"{bronze_base}/raw_fraud_events"
    storage_opts  = get_storage_options(cfg)

    start_ts  = datetime.utcnow()
    batch_id  = str(uuid.uuid4())
    total_in  = total_out = 0

    logger.info(
        f"[{PIPELINE_NAME}] Connecting to {bootstrap}, topic={topic!r}, group={group_id!r}"
    )

    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=poll_timeout,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_records=batch_size,
        )

        records = []
        for msg in consumer:
            records.append(msg.value)
            total_in += 1

            if len(records) >= batch_size:
                _flush_batch(records, table_path, batch_id, storage_opts)
                total_out += len(records)
                consumer.commit()
                records = []

        # Flush remainder
        if records:
            _flush_batch(records, table_path, batch_id, storage_opts)
            total_out += len(records)
            consumer.commit()

        consumer.close()

        if total_out == 0:
            logger.info(f"[{PIPELINE_NAME}] No new events from Kafka. Skipping.")
        else:
            logger.info(f"[{PIPELINE_NAME}] Done. in={total_in:,} out={total_out:,}")

        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "success", total_in, total_out, cfg=cfg)

    except Exception as e:
        log_run(PIPELINE_NAME, start_ts, datetime.utcnow(),
                "failed", total_in, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE_NAME}] FAILED: {e}")
        raise


def _flush_batch(records: list[dict], table_path: str, batch_id: str,
                 storage_opts: dict) -> None:
    chunk = pd.DataFrame(records)

    chunk["event_timestamp"] = pd.to_datetime(chunk["event_timestamp"])
    chunk["created_ts"]      = pd.to_datetime(chunk["created_ts"])
    chunk["ingest_ts"]       = datetime.utcnow()
    chunk["batch_id"]        = batch_id
    chunk["event_date"]      = chunk["event_timestamp"].dt.date.astype(str)

    # Enforce a stable schema so all-null columns don't collapse to Arrow 'null'
    # (rejected by Delta) or drift in type across micro-batches.
    for col in _STRING_COLS:
        if col in chunk.columns:
            chunk[col] = chunk[col].astype("string")
    for col in _FLOAT_COLS:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("float64")
    # Safety net: any remaining all-null column (unexpected field) → string,
    # never leave it as the Arrow 'null' type. (amount stays float64 above.)
    for col in chunk.columns:
        if col not in _FLOAT_COLS and chunk[col].isna().all():
            chunk[col] = chunk[col].astype("string")

    # Quality gate
    qr = QualityResult(pipeline=PIPELINE_NAME)
    check_not_empty(chunk, qr)
    check_schema(chunk, REQUIRED_COLS, qr)
    check_no_nulls(chunk, ["event_id", "event_type", "event_timestamp", "customer_id"], qr)
    if not qr.passed:
        raise ValueError(f"Quality checks failed:\n{qr.summary()}")

    # Normalise the external field name → internal lakehouse convention.
    # Raw events carry "event_timestamp"; Bronze Delta onward uses "event_ts".
    chunk = chunk.rename(columns={"event_timestamp": "event_ts"})

    write_bronze(chunk, table_path, storage_options=storage_opts, partition_by=["event_date"])
    logger.debug(f"[{PIPELINE_NAME}] Flushed {len(chunk):,} records to Bronze.")
