"""
src/generator/generate_data.py

Main entry point for the Fraud Detection data generator.

Generates all offline data (customers, merchants, cards, transactions) and
streaming events, then optionally streams them through Kafka.

Streaming modes (--stream-mode)
────────────────────────────────
file          Write streaming events to fraud_events.json only. (default)
kafka-replay  Write to fraud_events.json, then replay to Kafka with pacing.
kafka-live    Write to fraud_events.json, then start live streaming to Kafka.
              Live mode generates events for today at real-time rates. Runs
              indefinitely until CTRL-C.

Kafka options (only used when stream-mode != file)
──────────────────────────────────────────────────
--kafka-bootstrap   Kafka bootstrap servers  (default: localhost:9092)
--kafka-topic       Topic name               (default: fraud.events.raw)
--replay-speed      Speed multiplier for replay mode (default: 3600)
                    3600 = 1 hour of events per real second
                    0    = bulk publish (no delays)

Output
──────
  data/raw/offline/customers.parquet
  data/raw/offline/merchants.parquet
  data/raw/offline/cards.parquet
  data/raw/offline/transactions/transaction_date=YYYY-MM-DD/*.parquet
  data/raw/streaming/fraud_events.json        (all modes)
  data/raw/quality_reports/quality_report.csv

Usage examples
──────────────
  # Offline + file streaming only (default)
  python -m src.generator.generate_data

  # Generate then replay through Kafka at 3600× speed
  python -m src.generator.generate_data \\
      --stream-mode kafka-replay \\
      --kafka-bootstrap kafka.fraud-infra.svc.cluster.local:9092 \\
      --replay-speed 3600

  # Generate offline data, then stream live events to Kafka
  python -m src.generator.generate_data \\
      --stream-mode kafka-live \\
      --kafka-bootstrap kafka.fraud-infra.svc.cluster.local:9092
"""

import argparse
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.generator.offline.customers import generate_customers
from src.generator.offline.merchants import generate_merchants
from src.generator.offline.cards import generate_cards
from src.generator.offline.transactions import generate_transactions
from src.generator.streaming.event_producer import generate_streaming_events
from src.generator.quality.quality_report import compute_offline_quality, save_quality_report


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"  Saved: {path}  ({len(df):,} rows)")


def save_partitioned_parquet(df: pd.DataFrame, base_path: Path, partition_col: str) -> None:
    base_path.mkdir(parents=True, exist_ok=True)
    df[partition_col] = pd.to_datetime(df[partition_col]).dt.date

    for date_val, group in df.groupby(partition_col):
        partition_dir = base_path / f"{partition_col}={date_val}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / "data.parquet"
        group.drop(columns=[partition_col]).to_parquet(out_path, index=False)

    n_partitions = df[partition_col].nunique()
    print(f"  Saved: {base_path}  ({len(df):,} rows, {n_partitions} partitions)")


def generate_all(cfg: dict, output_dir: Path) -> tuple[pd.DataFrame, dict]:
    """
    Generate all offline data + streaming events file.
    Returns (transactions_df, streaming_summary).
    """
    rng = np.random.default_rng(cfg["random_seed"])

    offline_dir   = output_dir / "offline"
    streaming_dir = output_dir / "streaming"
    quality_dir   = output_dir / "quality_reports"

    schema_change_date = datetime.strptime(cfg["schema_change_date"], "%Y-%m-%d")
    drift_enabled      = cfg.get("drift_enabled", False)
    drift_start_date   = (
        datetime.strptime(cfg["drift_start_date"], "%Y-%m-%d")
        if drift_enabled and "drift_start_date" in cfg else None
    )
    if drift_enabled:
        print(f"  [drift] Scenario B enabled — amount drift from {cfg['drift_start_date']}"
              f" (×{cfg.get('amount_drift_multiplier', 1.3)}, mode={cfg.get('drift_mode','gradual')})")

    # 1. Customers
    print("► Generating customers...")
    customers_df = generate_customers(
        n_customers=cfg["n_customers"],
        days_history=cfg["days_history"],
        rng=rng,
    )
    save_parquet(customers_df, offline_dir / "customers.parquet")
    customer_ids = customers_df["customer_id"].tolist()

    # 2. Merchants
    print("\n► Generating merchants...")
    merchants_df = generate_merchants(
        n_merchants=cfg["n_merchants"],
        skew_category_ratio=cfg["skew_category_ratio"],
        rng=rng,
    )
    save_parquet(merchants_df, offline_dir / "merchants.parquet")
    merchant_ids = merchants_df["merchant_id"].tolist()

    # 3. Cards
    print("\n► Generating cards...")
    cards_df = generate_cards(
        n_cards=cfg["n_cards"],
        customer_ids=customer_ids,
        rng=rng,
    )
    save_parquet(cards_df, offline_dir / "cards.parquet")
    card_ids = cards_df["card_id"].tolist()

    # 4. Transactions
    print("\n► Generating transactions...")
    transactions_df = generate_transactions(
        n_customers=cfg["n_customers"],
        avg_txn_per_customer=cfg["avg_txn_per_customer"],
        customer_ids=customer_ids,
        card_ids=card_ids,
        merchant_ids=merchant_ids,
        customer_df=customers_df,
        card_df=cards_df,
        days_history=cfg["days_history"],
        skew_city_ratio=cfg["skew_city_ratio"],
        schema_change_date=schema_change_date,
        fraud_rate=cfg["fraud_rate"],
        fraud_label_min_conditions=cfg["fraud_label_min_conditions"],
        duplicate_rate=cfg["duplicate_rate_offline"],
        rng=rng,
        drift_enabled=drift_enabled,
        drift_start_date=drift_start_date,
        drift_mode=cfg.get("drift_mode", "gradual"),
        amount_drift_multiplier=cfg.get("amount_drift_multiplier", 1.30),
        drift_ramp_days=cfg.get("drift_ramp_days", 30),
    )
    transactions_df["transaction_date"] = pd.to_datetime(
        transactions_df["transaction_timestamp"]
    ).dt.date.astype(str)
    save_partitioned_parquet(
        transactions_df,
        base_path=offline_dir / "transactions",
        partition_col="transaction_date",
    )

    # 5. Streaming events → file (always written, used by both file and kafka modes)
    print("\n► Generating streaming events...")
    _txn_slim_cols = [
        "transaction_id", "transaction_timestamp", "transaction_date",
        "customer_id", "card_id", "merchant_id",
        "transaction_status", "is_fraud", "amount", "ip_country",
    ]
    streaming_summary = generate_streaming_events(
        customer_ids=customer_ids,
        merchant_ids=merchant_ids,
        card_ids=card_ids,
        days_history=cfg["days_history"],
        base_events_per_min=cfg["base_events_per_min"],
        burst_multiplier=cfg["burst_multiplier"],
        burst_windows=cfg["burst_windows"],
        late_arrival_rate=cfg["late_arrival_rate"],
        late_delay_min=cfg["late_delay_min"],
        late_delay_max=cfg["late_delay_max"],
        duplicate_rate=cfg["duplicate_rate_stream"],
        output_path=streaming_dir / "fraud_events.json",
        rng=rng,
        transaction_df=transactions_df[_txn_slim_cols].copy(),
    )

    # 6. Quality report
    print("\n► Computing quality report...")
    quality_df = compute_offline_quality(
        customers_df=customers_df,
        merchants_df=merchants_df,
        cards_df=cards_df,
        transactions_df=transactions_df,
        schema_change_date=schema_change_date,
        streaming_summary=streaming_summary,
    )
    save_quality_report(quality_df, quality_dir)

    return transactions_df, streaming_summary


def stream_to_kafka(
    stream_mode: str,
    cfg: dict,
    output_dir: Path,
    transactions_df: pd.DataFrame,
    bootstrap_servers: str,
    topic: str,
    replay_speed: float,
) -> None:
    """Dispatch to kafka_producer based on stream_mode."""
    from src.generator.streaming.kafka_producer import (
        replay_to_kafka,
        stream_live_to_kafka,
    )

    if stream_mode == "kafka-replay":
        events_path = output_dir / "streaming" / "fraud_events.json"
        print(f"\n► Replaying {events_path} → Kafka topic {topic!r} at {replay_speed}× speed...")
        print(  "  (Press CTRL-C to stop early)\n")
        n = replay_to_kafka(
            source_path=events_path,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            speed_multiplier=replay_speed,
        )
        print(f"  Published {n:,} messages to Kafka.")

    elif stream_mode == "kafka-live":
        print(f"\n► Starting live streaming → Kafka topic {topic!r}")
        print(  "  Generates today's events in real-time. Press CTRL-C to stop.\n")
        stream_live_to_kafka(
            cfg=cfg,
            transactions_df=transactions_df,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )


def main(
    config_path: str  = "configs/generate_config.yaml",
    stream_mode: str  = "file",
    bootstrap: str    = "localhost:9092",
    topic: str        = "fraud.events.raw",
    replay_speed: float = 3600.0,
) -> None:
    start_time = time.time()
    print(f"\n{'='*60}")
    print("Fraud Detection — Data Generator")
    print(f"Config:      {config_path}")
    print(f"Stream mode: {stream_mode}")
    if stream_mode != "file":
        print(f"Kafka:       {bootstrap}  topic={topic}")
        if stream_mode == "kafka-replay":
            print(f"Replay speed: {replay_speed}× real-time")
    print(f"{'='*60}\n")

    cfg        = load_config(config_path)
    output_dir = Path(cfg["output_dir"])

    transactions_df, streaming_summary = generate_all(cfg, output_dir)

    elapsed = round(time.time() - start_time, 1)
    print(f"\n{'='*60}")
    print(f"✓ Generation complete in {elapsed}s")
    print(f"  Output: {output_dir.resolve()}")
    print(f"  Streaming events: {streaming_summary['total_events']:,}")
    print(f"{'='*60}\n")

    if stream_mode != "file":
        stream_to_kafka(
            stream_mode=stream_mode,
            cfg=cfg,
            output_dir=output_dir,
            transactions_df=transactions_df,
            bootstrap_servers=bootstrap,
            topic=topic,
            replay_speed=replay_speed,
        )


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Fraud Detection Data Generator")
    parser.add_argument(
        "--config",
        default="configs/generate_config.yaml",
        help="Path to generator config YAML",
    )
    parser.add_argument(
        "--stream-mode",
        choices=["file", "kafka-replay", "kafka-live"],
        default="file",
        help=(
            "file: write events to fraud_events.json only (default). "
            "kafka-replay:w write file then replay to Kafka with pacing. "
            "kafka-live: write file then generate live events to Kafka."
        ),
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="localhost:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap servers (used when --stream-mode != file)",
    )
    parser.add_argument(
        "--kafka-topic",
        default="fraud.events.raw",
        help="Kafka topic to publish events to",
    )
    parser.add_argument(
        "--replay-speed",
        type=float,
        default=3600,
        metavar="N",
        help=(
            "Speed multiplier for kafka-replay mode. "
            "3600 = 1 hour of events per real second. "
            "0 = bulk (no delays)."
        ),
    )
    args = parser.parse_args()

    main(
        config_path=args.config,
        stream_mode=args.stream_mode,
        bootstrap=args.kafka_bootstrap,
        topic=args.kafka_topic,
        replay_speed=args.replay_speed,
    )
