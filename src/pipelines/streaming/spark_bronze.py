"""
src/pipelines/streaming/spark_bronze.py

Stream Processing Engine (Part A): Spark Structured Streaming, continuous.

Replaces the legacy 5-minute micro-batch consumer (kafka_consumer.py). Reads the
Debezium CDC stream from Kafka topic `fraud.events.raw` and continuously appends
to the Delta Lake Bronze table `raw_fraud_events`. This is a long-running job
(deployed as a Kubernetes Deployment), not an Airflow task.

Pipeline
────────
  Kafka fraud.events.raw            (Debezium-unwrapped flat JSON rows)
     │  readStream
     ▼
  parse JSON → cast → derive event_ts / event_date / ingest_ts
     │  foreachBatch (stamp batch_id = micro-batch id)
     ▼
  Delta s3a://bronze/raw_fraud_events   (append, partitioned by event_date)

Exactly-once into Delta is guaranteed by the Structured Streaming checkpoint
(Kafka offsets live in the checkpoint, not in a consumer-group commit), replacing
the legacy at-least-once manual commit.

The output schema matches what the legacy consumer wrote, so the downstream
Silver job (clean_events.py) needs no change:
  event_id, event_type, event_ts, created_ts, customer_id, session_id,
  device_type, ip_country, merchant_id, card_id, amount, failure_reason,
  ingest_ts, batch_id, event_date
"""

from __future__ import annotations   # base Spark image is Python 3.8 → defer `X | None` annotations

import logging
import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logger = logging.getLogger(__name__)

# Read config standalone (yaml only) so the Spark image needs no DB/duckdb deps.
_CONFIG_PATH = Path(__file__).parents[3] / "configs" / "pipeline_config.yaml"


def load_pipeline_config() -> dict:
    with open(os.environ.get("PIPELINE_CONFIG_PATH", _CONFIG_PATH)) as f:
        return yaml.safe_load(f)

PIPELINE_NAME = "spark_stream_bronze"

# Shape of the Debezium-unwrapped Kafka message value (JsonConverter, schemas off).
# event_timestamp / created_ts arrive as ISO-8601 strings (OLTP columns are TEXT).
EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(), False),
    StructField("event_type",      StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("created_ts",      StringType(), True),
    StructField("customer_id",     StringType(), True),
    StructField("session_id",      StringType(), True),
    StructField("device_type",     StringType(), True),
    StructField("ip_country",      StringType(), True),
    StructField("merchant_id",     StringType(), True),
    StructField("card_id",         StringType(), True),
    StructField("amount",          DoubleType(), True),
    StructField("failure_reason",  StringType(), True),
])


def build_spark(cfg: dict) -> SparkSession:
    minio = cfg["minio"]
    access_key = os.environ.get("MINIO_ACCESS_KEY", minio.get("access_key", ""))
    secret_key = os.environ.get("MINIO_SECRET_KEY", minio.get("secret_key", ""))

    return (
        SparkSession.builder
        .appName("fraud-spark-bronze")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A → MinIO
        .config("spark.hadoop.fs.s3a.endpoint", minio["endpoint_url"])
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # keep shuffle small for a single-node Kind dev cluster
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _write_batch(batch_df, batch_id: int, bronze_table: str) -> None:
    """foreachBatch sink: stamp batch_id and append the micro-batch to Delta."""
    if batch_df.rdd.isEmpty():
        return
    (batch_df
        .withColumn("batch_id", F.lit(str(batch_id)))
        .withColumn("ingest_ts", F.current_timestamp().cast("timestamp_ntz"))
        .write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(bronze_table))
    logger.info(f"[{PIPELINE_NAME}] batch {batch_id}: appended {batch_df.count():,} rows")


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    ss = cfg["spark_streaming"]
    bootstrap    = cfg["kafka"]["bootstrap_servers"]
    topic        = ss.get("kafka_topic", cfg["kafka"]["topic_fraud_events"])
    bronze_table = ss["bronze_table"]
    checkpoint   = ss["checkpoint_location"]

    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"[{PIPELINE_NAME}] reading topic={topic!r} → {bronze_table}")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", ss.get("starting_offsets", "earliest"))
           .option("maxOffsetsPerTrigger", int(ss.get("max_offsets_per_trigger", 50000)))
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .where(F.col("event_id").isNotNull())
        # external field event_timestamp → internal lakehouse name event_ts.
        # Cast to timestamp_ntz (no tz) to match the existing Bronze table written
        # by the legacy delta-rs consumer; Spark's default to_timestamp is LTZ,
        # which Delta refuses to merge against the table's timestamp_ntz columns.
        .withColumn("event_ts", F.to_timestamp("event_timestamp").cast("timestamp_ntz"))
        .withColumn("created_ts", F.to_timestamp("created_ts").cast("timestamp_ntz"))
        .withColumn("event_date", F.date_format("event_ts", "yyyy-MM-dd"))
        .drop("event_timestamp")
        .select(
            "event_id", "event_type", "event_ts", "created_ts", "customer_id",
            "session_id", "device_type", "ip_country", "merchant_id", "card_id",
            "amount", "failure_reason", "event_date",
        ))

    query = (parsed.writeStream
             .foreachBatch(lambda df, bid: _write_batch(df, bid, bronze_table))
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=ss.get("trigger_interval", "30 seconds"))
             .start())

    logger.info(f"[{PIPELINE_NAME}] streaming started (trigger={ss.get('trigger_interval')})")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    run()
