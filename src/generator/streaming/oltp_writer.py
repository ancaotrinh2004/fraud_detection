"""
src/generator/streaming/oltp_writer.py

CDC streaming source (Part B): write fraud events into the Postgres OLTP table
`oltp.transaction_events` instead of publishing straight to Kafka.

This is the generator's role in the A+B architecture: it acts as the *product
application* writing transactions to an operational database. Debezium captures
those inserts from the Postgres WAL and publishes them to Kafka — the generator
never touches Kafka directly anymore.

It mirrors `kafka_producer.py` (same `replay` / `live` modes and time-based
pacing) but swaps the sink from `producer.send(topic, ...)` to a batched
`INSERT ... ON CONFLICT (event_id, event_timestamp) DO NOTHING` (idempotent on the
same dedup key as Silver, so a re-run/replay is harmless while a genuine mobile
retry — same event_id, new timestamp — is kept as a distinct row).

Usage
─────
  # Replay the generated file into OLTP, 3600× faster than real time
  python -m src.generator.streaming.oltp_writer replay \\
      --source data/raw/streaming/fraud_events.json --speed 3600

  # Live mode — generate today's events and insert them at real-time pace
  python -m src.generator.streaming.oltp_writer live \\
      --config configs/generate_config.yaml
"""

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

from src.pipelines.utils.db import load_pipeline_config, get_pg_connection_string

logger = logging.getLogger(__name__)

_STOP = False  # graceful-shutdown flag for live mode

# Column order for the OLTP insert — matches src/pipelines/oltp/ddl.py.
_COLS = [
    "event_id", "event_type", "event_timestamp", "created_ts",
    "customer_id", "session_id", "device_type", "ip_country",
    "merchant_id", "card_id", "amount", "failure_reason",
]


def _qualified_table(cfg: dict) -> str:
    oltp = cfg.get("oltp", {})
    return f"{oltp.get('schema', 'oltp')}.{oltp.get('table', 'transaction_events')}"


def _connect(cfg: dict):
    conn = psycopg2.connect(get_pg_connection_string(cfg))
    conn.autocommit = False
    return conn


def _row_tuple(event: dict) -> tuple:
    """Project an event dict onto the OLTP column order; coerce empty → None."""
    out = []
    for c in _COLS:
        v = event.get(c)
        if c == "amount":
            try:
                v = float(v) if v is not None and v != "" else None
            except (TypeError, ValueError):
                v = None
        else:
            v = None if v in ("", None) else str(v)
        out.append(v)
    return tuple(out)


def _flush(conn, table: str, rows: list[tuple]) -> int:
    """Batch-insert rows; ON CONFLICT keeps it idempotent (retry-safe)."""
    if not rows:
        return 0
    sql = (
        f"INSERT INTO {table} ({', '.join(_COLS)}) VALUES %s "
        f"ON CONFLICT (event_id, event_timestamp) DO NOTHING"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


# ── Replay mode ────────────────────────────────────────────────────────────────

def replay_to_oltp(
    source_path: Path,
    cfg: dict,
    speed_multiplier: float | None = 3600,
    batch_size: int = 1000,
    max_events: int | None = None,
) -> int:
    """Stream fraud_events.json line-by-line into the OLTP table with pacing.

    The file is read line-by-line (never fully loaded) so RAM stays flat on the
    large event file. Returns total rows inserted.
    """
    source_path = Path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    table     = _qualified_table(cfg)
    bulk_mode = (speed_multiplier is None or speed_multiplier == 0)
    mode_str  = "bulk (no delays)" if bulk_mode else f"{speed_multiplier}× real-time"
    logger.info(f"[replay→oltp] {source_path} → {table}, mode={mode_str}")

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

    conn          = _connect(cfg)
    inserted      = 0
    buf: list[tuple] = []
    t_wall_start  = time.perf_counter()
    prev_event_ts = None
    prev_wall_ts  = t_wall_start

    try:
        for event in _iter_events():
            if _STOP:
                break
            if max_events and inserted + len(buf) >= max_events:
                logger.info(f"[replay→oltp] Reached max_events={max_events:,} — stopping.")
                break

            # Time-based pacing between consecutive event timestamps.
            if not bulk_mode and prev_event_ts is not None:
                cur_ts = event.get("event_timestamp", "")
                try:
                    delta_sim  = (datetime.fromisoformat(cur_ts)
                                  - datetime.fromisoformat(prev_event_ts)).total_seconds()
                    sleep_for  = max(0.0, delta_sim / speed_multiplier) \
                                 - (time.perf_counter() - prev_wall_ts)
                    if sleep_for > 0:
                        # Flush what we have before sleeping so pacing reflects writes.
                        inserted += _flush(conn, table, buf); buf = []
                        time.sleep(sleep_for)
                except (ValueError, TypeError):
                    pass

            buf.append(_row_tuple(event))
            prev_event_ts = event.get("event_timestamp")
            prev_wall_ts  = time.perf_counter()

            if len(buf) >= batch_size:
                inserted += _flush(conn, table, buf); buf = []

            if inserted and inserted % 100_000 == 0:
                elapsed = time.perf_counter() - t_wall_start
                logger.info(f"[replay→oltp] {inserted:,} inserted @ "
                            f"{inserted / max(elapsed, 1e-9):,.0f} rows/s")

        inserted += _flush(conn, table, buf)
    finally:
        conn.close()

    elapsed = time.perf_counter() - t_wall_start
    logger.info(f"[replay→oltp] Done. inserted={inserted:,} elapsed={elapsed:.1f}s")
    return inserted


# ── Live mode ──────────────────────────────────────────────────────────────────

def stream_live_to_oltp(cfg: dict, transactions_df, generate_cfg: dict,
                        batch_size: int = 1000) -> None:
    """Generate events day-by-day and insert them into OLTP at real-time pace."""
    from src.generator.streaming.event_producer import (
        _generate_day, _generate_transaction_sessions,
    )
    import pandas as pd

    global _STOP
    rng = np.random.default_rng(generate_cfg.get("random_seed", 42))
    g   = generate_cfg.get("streaming", generate_cfg)

    customer_arr = np.array(transactions_df["customer_id"].unique())
    merchant_arr = np.array(transactions_df["merchant_id"].dropna().unique())
    card_arr     = np.array(transactions_df["card_id"].dropna().unique())

    table   = _qualified_table(cfg)
    conn    = _connect(cfg)
    total   = 0
    t_start = time.perf_counter()
    logger.info(f"[live→oltp] Inserting to {table}. CTRL-C to stop.")

    txn_by_date = {}
    if transactions_df is not None and "transaction_date" in transactions_df.columns:
        for d, grp in transactions_df.groupby("transaction_date"):
            txn_by_date[d] = grp

    current_day = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        while not _STOP:
            df_day, n_late, n_burst = _generate_day(
                day_start_ts=int(current_day.timestamp()),
                customer_arr=customer_arr, merchant_arr=merchant_arr, card_arr=card_arr,
                base_events_per_min=int(g.get("base_events_per_min", 50)),
                burst_multiplier=int(g.get("burst_multiplier", 40)),
                burst_windows=g.get("burst_windows", []),
                late_arrival_rate=float(g.get("late_arrival_rate", 0.15)),
                late_delay_min=int(g.get("late_delay_min", 5)),
                late_delay_max=int(g.get("late_delay_max", 60)),
                rng=rng,
            )
            if current_day.date() in txn_by_date:
                df_sessions = _generate_transaction_sessions(
                    txn_by_date[current_day.date()], rng,
                    float(g.get("late_arrival_rate", 0.15)),
                    int(g.get("late_delay_min", 5)), int(g.get("late_delay_max", 60)),
                )
                if len(df_sessions) > 0:
                    df_day = pd.concat([df_day, df_sessions], ignore_index=True)

            df_day = df_day.sort_values("event_timestamp").reset_index(drop=True)
            df_day["created_ts"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

            seconds_per_event = 86_400 / max(len(df_day), 1)
            buf: list[tuple] = []
            for record in df_day.to_dict(orient="records"):
                if _STOP:
                    break
                buf.append(_row_tuple(record))
                if len(buf) >= batch_size:
                    total += _flush(conn, table, buf); buf = []
                time.sleep(seconds_per_event)
            total += _flush(conn, table, buf)

            rate = total / max(time.perf_counter() - t_start, 1e-9)
            logger.info(f"[live→oltp] {current_day.date()} done. total={total:,} "
                        f"({rate:.1f} rows/s) late={n_late} burst={n_burst}")
            current_day += timedelta(days=1)
    finally:
        conn.close()
    logger.info(f"[live→oltp] Stopped. Total inserted: {total:,}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def _setup_signal_handlers():
    def _handler(sig, frame):
        global _STOP
        logger.info("[oltp_writer] Shutdown signal received — draining and stopping...")
        _STOP = True
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    _setup_signal_handlers()
    cfg = load_pipeline_config()

    parser = argparse.ArgumentParser(description="OLTP writer (CDC source) for fraud events")
    sub = parser.add_subparsers(dest="mode", required=True)

    rep = sub.add_parser("replay", help="Replay fraud_events.json into the OLTP table")
    rep.add_argument("--source", default="data/raw/streaming/fraud_events.json")
    rep.add_argument("--speed", type=float, default=3600,
                     help="Speed multiplier (3600 = 1 hr/sec; 0 = bulk)")
    rep.add_argument("--batch-size", type=int, default=1000)
    rep.add_argument("--max-events", type=int, default=None)

    live = sub.add_parser("live", help="Generate and insert events in real-time")
    live.add_argument("--config", default="configs/generate_config.yaml")
    live.add_argument("--transactions", default="data/raw/offline/transactions")

    args = parser.parse_args()

    if args.mode == "replay":
        n = replay_to_oltp(
            source_path=Path(args.source), cfg=cfg,
            speed_multiplier=args.speed if args.speed > 0 else None,
            batch_size=args.batch_size, max_events=args.max_events,
        )
        print(f"Inserted {n:,} rows into OLTP.")
    elif args.mode == "live":
        import yaml, pandas as pd
        with open(args.config) as f:
            generate_cfg = yaml.safe_load(f)
        txn_path = Path(args.transactions)
        txn_df = pd.read_parquet(txn_path) if txn_path.exists() else pd.DataFrame()
        stream_live_to_oltp(cfg, txn_df, generate_cfg)


if __name__ == "__main__":
    main()
