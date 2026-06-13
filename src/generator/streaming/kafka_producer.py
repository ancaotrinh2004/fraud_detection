"""
src/generator/streaming/kafka_producer.py

Streaming simulation: publish fraud events to a Kafka topic.

Two modes
─────────
replay_to_kafka()
    Stream fraud_events.json line-by-line (NOT loaded into memory) and
    publish with configurable time-based pacing. The file is already in
    roughly chronological order, so no global in-memory sort is needed —
    this keeps RAM flat on the ~12GB / 34M-line file and avoids OOM.

    speed_multiplier examples
      1        → real-time  (1 hour of events takes 1 real hour)
      60       → 1 minute   of events per real second
      3600     → 1 hour     of events per real second  ← default
      0 / None → bulk       (no delays, fastest possible)

    With speed_multiplier=3600 and days_history=180:
      180 × 24 hours / 3600 = 1.2 real minutes per simulated day
      Total wall-clock time ≈ 3.6 hours → use 86400 for a ~3-min run

stream_live_to_kafka()
    Generate events for today using event_producer, publish each event
    immediately after it is created (current wall-clock timestamp).
    Advances to the next day at midnight. Runs until interrupted.

Usage (standalone)
──────────────────
  # Replay mode — 3600× faster than real-time
  python -m src.generator.streaming.kafka_producer replay \\
      --source data/raw/streaming/fraud_events.json \\
      --bootstrap localhost:9092 \\
      --topic fraud.events.raw \\
      --speed 3600

  # Live mode (requires offline data already generated)
  python -m src.generator.streaming.kafka_producer live \\
      --bootstrap localhost:9092 \\
      --topic fraud.events.raw \\
      --config config/generate_config.yaml
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

_STOP = False  # graceful-shutdown flag for live mode


def _make_producer(bootstrap_servers: str):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        batch_size=32_768,
        linger_ms=10,
        compression_type="gzip",
    )


# ── Replay mode ────────────────────────────────────────────────────────────────

def replay_to_kafka(
    source_path: Path,
    bootstrap_servers: str,
    topic: str,
    speed_multiplier: float = 3600,
    batch_size: int = 500,
    max_events: int | None = None,
) -> int:
    """
    Replay fraud_events.json to Kafka with time-based pacing.

    Events are streamed line-by-line directly from the file (NOT loaded into
    memory) so RAM stays flat regardless of file size — fraud_events.json is
    ~12GB / 34M lines, and loading + sorting it all in memory OOMs the host.
    The file is already in roughly chronological order, so we rely on that
    instead of a global in-memory sort; minor out-of-order ticks just produce
    a non-positive pacing delay (handled below).

    max_events caps the run — useful for a quick streaming test without
    publishing all 34M events.

    Returns total messages published.
    """
    source_path = Path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    bulk_mode = (speed_multiplier is None or speed_multiplier == 0)
    mode_str  = "bulk (no delays)" if bulk_mode else f"{speed_multiplier}× real-time"
    cap_str   = f", max_events={max_events:,}" if max_events else ""
    logger.info(
        f"[replay] Streaming events from {source_path} → topic={topic!r}, "
        f"mode={mode_str}{cap_str}"
    )

    def _iter_events():
        with open(source_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue

    producer     = _make_producer(bootstrap_servers)
    published    = 0
    errors       = 0
    t_wall_start = time.perf_counter()

    # Simulated-time anchor: when to publish the next event
    prev_event_ts: str | None = None
    prev_wall_ts:  float      = t_wall_start

    for event in _iter_events():
        if _STOP:
            break
        if max_events and published >= max_events:
            logger.info(f"[replay] Reached max_events={max_events:,} — stopping.")
            break

        key = event.get("customer_id")

        if not bulk_mode and prev_event_ts is not None:
            cur_event_ts = event.get("event_timestamp", "")
            if cur_event_ts and prev_event_ts:
                try:
                    t_cur  = datetime.fromisoformat(cur_event_ts)
                    t_prev = datetime.fromisoformat(prev_event_ts)
                    delta_sim   = (t_cur - t_prev).total_seconds()
                    delta_wall  = max(0.0, delta_sim / speed_multiplier)
                    elapsed     = time.perf_counter() - prev_wall_ts
                    sleep_for   = delta_wall - elapsed
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                except (ValueError, TypeError):
                    pass

        producer.send(topic, key=key, value=event)
        published   += 1
        prev_event_ts = event.get("event_timestamp")
        prev_wall_ts  = time.perf_counter()

        if published % batch_size == 0:
            producer.flush()

        if published % 100_000 == 0:
            elapsed = time.perf_counter() - t_wall_start
            rate    = published / elapsed if elapsed > 0 else 0
            sim_ts  = event.get("event_timestamp", "?")
            logger.info(
                f"[replay] {published:,} published @ {rate:,.0f} msg/s  "
                f"simulated_ts={sim_ts}"
            )

    producer.flush()
    producer.close()

    elapsed  = time.perf_counter() - t_wall_start
    rate     = published / elapsed if elapsed > 0 else 0
    logger.info(
        f"[replay] Done. published={published:,}  errors={errors}  "
        f"elapsed={elapsed:.1f}s  avg={rate:,.0f} msg/s"
    )
    return published


# ── Live mode ──────────────────────────────────────────────────────────────────

def stream_live_to_kafka(
    cfg: dict,
    transactions_df,
    bootstrap_servers: str,
    topic: str,
) -> None:
    """
    Generate events in real-time and publish to Kafka continuously.

    Each event is stamped with the current wall-clock time (created_ts = now).
    Generates one day's worth of events, then advances to the next.
    Runs until interrupted (SIGINT / SIGTERM).

    transactions_df: slim copy of the offline transactions table used to
                     anchor fraud-correlated event sessions (same as batch mode).
    """
    from src.generator.streaming.event_producer import (
        _generate_day,
        _generate_transaction_sessions,
    )

    global _STOP

    rng     = np.random.default_rng(cfg.get("random_seed", 42))
    gen_cfg = cfg.get("streaming", cfg)  # accept both generate_config or nested

    customer_arr = np.array(transactions_df["customer_id"].unique())
    merchant_arr = np.array(transactions_df["merchant_id"].dropna().unique())
    card_arr     = np.array(transactions_df["card_id"].dropna().unique())

    burst_windows      = gen_cfg.get("burst_windows", [])
    base_events_per_min = int(gen_cfg.get("base_events_per_min", 50))
    burst_multiplier   = int(gen_cfg.get("burst_multiplier", 40))
    late_arrival_rate  = float(gen_cfg.get("late_arrival_rate", 0.15))
    late_delay_min     = int(gen_cfg.get("late_delay_min", 5))
    late_delay_max     = int(gen_cfg.get("late_delay_max", 60))
    duplicate_rate     = float(gen_cfg.get("duplicate_rate_stream", 0.015))

    producer   = _make_producer(bootstrap_servers)
    published  = 0
    t_start    = time.perf_counter()

    logger.info(f"[live] Streaming to topic={topic!r}. CTRL-C to stop.")

    # Pre-group transactions by date for fast daily lookup
    txn_by_date = {}
    if transactions_df is not None and len(transactions_df) > 0:
        txn_col = "transaction_date" if "transaction_date" in transactions_df.columns else None
        if txn_col:
            for date_val, grp in transactions_df.groupby(txn_col):
                txn_by_date[date_val] = grp

    current_day = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    while not _STOP:
        day_start_ts = int(current_day.timestamp())
        day_date     = current_day.date()
        logger.info(f"[live] Generating events for {day_date}...")

        # Generate background events for the day
        df_day, n_late, n_burst = _generate_day(
            day_start_ts=day_start_ts,
            customer_arr=customer_arr,
            merchant_arr=merchant_arr,
            card_arr=card_arr,
            base_events_per_min=base_events_per_min,
            burst_multiplier=burst_multiplier,
            burst_windows=burst_windows,
            late_arrival_rate=late_arrival_rate,
            late_delay_min=late_delay_min,
            late_delay_max=late_delay_max,
            rng=rng,
        )

        # Inject transaction-anchored fraud sessions
        if day_date in txn_by_date:
            import pandas as pd
            df_sessions = _generate_transaction_sessions(
                txn_by_date[day_date], rng,
                late_arrival_rate, late_delay_min, late_delay_max,
            )
            if len(df_sessions) > 0:
                import pandas as pd
                df_day = pd.concat([df_day, df_sessions], ignore_index=True)

        # Sort by event_timestamp and override created_ts to current wall-clock
        df_day = df_day.sort_values("event_timestamp").reset_index(drop=True)
        now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        df_day["created_ts"] = now_str  # live: created_ts = publish time

        # Deduplicate buffer: sample ~duplicate_rate rows for re-injection
        dupe_buffer = None
        n_to_dupe   = max(0, int(len(df_day) * duplicate_rate))
        if n_to_dupe > 0:
            sample_idx  = rng.choice(len(df_day), size=n_to_dupe, replace=False)
            dupe_buffer = df_day.iloc[sample_idx].copy()

        # Publish events at real-time pace spread evenly across the day
        n_events          = len(df_day)
        seconds_per_event = 86_400 / max(n_events, 1)
        records           = df_day.to_dict(orient="records")

        for record in records:
            if _STOP:
                break
            producer.send(topic, key=record.get("customer_id"), value=record)
            published += 1
            time.sleep(seconds_per_event)

        # Re-inject duplicates (simulate retry / at-least-once delivery)
        if dupe_buffer is not None and not _STOP:
            for _, row in dupe_buffer.iterrows():
                record = row.to_dict()
                record["created_ts"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                producer.send(topic, key=record.get("customer_id"), value=record)
                published += 1

        producer.flush()

        elapsed = time.perf_counter() - t_start
        rate    = published / elapsed if elapsed > 0 else 0
        logger.info(
            f"[live] Day {day_date} done. total_published={published:,} "
            f"({rate:.1f} msg/s). late={n_late}, burst={n_burst}"
        )

        current_day += timedelta(days=1)

    producer.close()
    logger.info(f"[live] Stopped. Total published: {published:,}")


# ── CLI entry point ────────────────────────────────────────────────────────────

def _setup_signal_handlers():
    def _handler(sig, frame):
        global _STOP
        logger.info("[kafka_producer] Shutdown signal received — draining and stopping...")
        _STOP = True
    signal.signal(signal.SIGINT,  _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    _setup_signal_handlers()

    parser = argparse.ArgumentParser(
        description="Kafka streaming producer for fraud events"
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    # ── replay sub-command ────────────────────────────────────────────────────
    rep = sub.add_parser("replay", help="Replay fraud_events.json to Kafka with pacing")
    rep.add_argument("--source",    default="data/raw/streaming/fraud_events.json")
    rep.add_argument("--bootstrap", default="localhost:9092")
    rep.add_argument("--topic",     default="fraud.events.raw")
    rep.add_argument(
        "--speed", type=float, default=3600,
        help="Speed multiplier (3600 = 1 hr of events per real second; 0 = bulk)",
    )
    rep.add_argument("--batch-size", type=int, default=500)
    rep.add_argument(
        "--max-events", type=int, default=None,
        help="Stop after N events (quick test; avoids replaying all 34M)",
    )

    # ── live sub-command ──────────────────────────────────────────────────────
    live_p = sub.add_parser("live", help="Generate and stream events to Kafka in real-time")
    live_p.add_argument("--bootstrap",   default="localhost:9092")
    live_p.add_argument("--topic",       default="fraud.events.raw")
    live_p.add_argument("--config",      default="config/generate_config.yaml")
    live_p.add_argument(
        "--transactions",
        default="data/raw/offline/transactions",
        help="Path to partitioned transactions parquet directory",
    )

    args = parser.parse_args()

    if args.mode == "replay":
        n = replay_to_kafka(
            source_path=Path(args.source),
            bootstrap_servers=args.bootstrap,
            topic=args.topic,
            speed_multiplier=args.speed if args.speed > 0 else None,
            batch_size=args.batch_size,
            max_events=args.max_events,
        )
        print(f"Published {n:,} messages.")

    elif args.mode == "live":
        import yaml, pandas as pd
        with open(args.config) as f:
            cfg = yaml.safe_load(f)

        txn_path = Path(args.transactions)
        if txn_path.exists():
            logger.info(f"[live] Loading transactions from {txn_path}...")
            txn_df = pd.read_parquet(txn_path)
            logger.info(f"[live] Loaded {len(txn_df):,} transactions.")
        else:
            logger.warning(f"[live] Transactions not found at {txn_path} — no fraud sessions.")
            import pandas as pd
            txn_df = pd.DataFrame()

        stream_live_to_kafka(
            cfg=cfg,
            transactions_df=txn_df,
            bootstrap_servers=args.bootstrap,
            topic=args.topic,
        )


if __name__ == "__main__":
    main()
