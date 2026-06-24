"""
src/pipelines/oltp/ddl.py

OLTP source schema for the CDC streaming track (Part B).

This is the *operational* side of the platform: product apps (simulated by the
generator) write each fraud event as a row into `oltp.transaction_events`.
Debezium then captures the Postgres WAL (logical decoding via `pgoutput`) and
publishes every insert to Kafka topic `fraud.events.raw`.

Design notes
────────────
- `event_timestamp` / `created_ts` are stored as TEXT (ISO-8601 strings, exactly
  as the generator emits them) so the Debezium-unwrapped Kafka message is a flat
  JSON identical in shape to the legacy file-replay event. `amount` is
  DOUBLE PRECISION so Debezium emits a plain JSON number (not a base64 Decimal).
- `(event_id, event_timestamp)` is the composite PRIMARY KEY → it is the dedup
  key (a mobile retry re-sends the same `event_id` with a new timestamp, so both
  columns are needed to treat genuine retries as distinct rows and matches the
  Silver `clean_events` dedup key exactly). Debezium uses the PK as the default
  REPLICA IDENTITY, which is sufficient for an insert-only (append) workload.
- A dedicated logical-replication PUBLICATION is created for just this table so
  CDC never captures the analytics `gold_fraud` schema.
"""

import logging

from src.pipelines.utils.db import load_pipeline_config, pg_cursor

logger = logging.getLogger(__name__)


def _oltp_cfg(cfg: dict) -> tuple[str, str, str]:
    oltp = cfg.get("oltp", {})
    schema = oltp.get("schema", "oltp")
    table = oltp.get("table", "transaction_events")
    publication = cfg.get("debezium", {}).get("publication_name", "dbz_fraud_publication")
    return schema, table, publication


def create_oltp_table(cfg: dict | None = None) -> None:
    """Create the OLTP schema + transaction_events table + CDC publication."""
    if cfg is None:
        cfg = load_pipeline_config()
    schema, table, publication = _oltp_cfg(cfg)
    pg_user = cfg["postgres"]["user"]

    with pg_cursor(cfg) as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                event_id        TEXT NOT NULL,
                event_type      TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,     -- ISO-8601 string (kept verbatim)
                created_ts      TEXT,              -- ISO-8601 string
                customer_id     TEXT NOT NULL,
                session_id      TEXT,
                device_type     TEXT,
                ip_country      TEXT,
                merchant_id     TEXT,
                card_id         TEXT,
                amount          DOUBLE PRECISION,
                failure_reason  TEXT,
                PRIMARY KEY (event_id, event_timestamp)
            );
        """)
        # Index the operational read pattern (recent events per customer).
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_customer_ts
                ON {schema}.{table}(customer_id, event_timestamp);
        """)

        # Logical-replication publication scoped to this one table only, so CDC
        # never streams analytics writes. fraud_user owns the database, so it has
        # CREATE-publication privilege; it owns this table, so it can add it.
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s;", (publication,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE PUBLICATION {publication} FOR TABLE {schema}.{table};")
            logger.info(f"[oltp] Created publication {publication} for {schema}.{table}")
        else:
            logger.info(f"[oltp] Publication {publication} already exists")

    logger.info(f"[oltp] Ready: {schema}.{table} (CDC source for Debezium, user={pg_user})")


def drop_oltp_table(cfg: dict | None = None) -> None:
    """Tear down the OLTP table + publication (destructive)."""
    if cfg is None:
        cfg = load_pipeline_config()
    schema, table, publication = _oltp_cfg(cfg)
    with pg_cursor(cfg) as cur:
        cur.execute(f"DROP PUBLICATION IF EXISTS {publication};")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
    logger.info(f"[oltp] Dropped {schema}.{table} and publication {publication}")
