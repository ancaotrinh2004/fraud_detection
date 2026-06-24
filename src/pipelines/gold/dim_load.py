"""
src/pipelines/gold/dim_load.py
Silver → Gold dimension tables.
SCD2: dim_customer, dim_merchant
SCD1: dim_card
Static: dim_date, dim_transaction_status
Scheduled: daily 03:00.
"""

import logging
from datetime import datetime, date

import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.delta import read_delta, get_storage_options

logger = logging.getLogger(__name__)
SCHEMA = "gold_fraud"


# ── dim_date ──────────────────────────────────────────────────────────────────

def load_dim_date(engine, start_year: int = 2023, end_year: int = 2028) -> None:
    rows = []
    current = date(start_year, 1, 1)
    while current.year <= end_year:
        vn_holidays = {  # simplified — key public holidays Vietnam
            (1, 1), (4, 30), (5, 1), (9, 2),
        }
        rows.append({
            "date_key":   int(current.strftime("%Y%m%d")),
            "calendar_date": current,
            "day_of_week": current.isoweekday(),
            "day_name":   current.strftime("%A"),
            "month":      current.month,
            "year":       current.year,
            "is_weekend": current.isoweekday() >= 6,
            "is_public_holiday_vn": (current.month, current.day) in vn_holidays,
        })
        current = date.fromordinal(current.toordinal() + 1)

    df = pd.DataFrame(rows)
    with engine.connect() as conn:
        existing = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.dim_date")).scalar()
    if existing > 0:
        logger.info(f"[dim_date] Already loaded ({existing:,} rows), skipping.")
        return
    with engine.begin() as conn:
        df.to_sql("dim_date", conn, schema=SCHEMA, if_exists="append", index=False)
    logger.info(f"[dim_date] Loaded {len(df):,} rows.")


def load_dim_transaction_status(engine) -> None:
    statuses = ["approved", "declined", "pending", "reversed"]
    with engine.begin() as conn:
        for s in statuses:
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.dim_transaction_status (status_name)
                VALUES ('{s}') ON CONFLICT (status_name) DO NOTHING
            """))
    logger.info("[dim_transaction_status] Loaded.")


# ── SCD2 helper ───────────────────────────────────────────────────────────────

def _scd2_merge(engine, new_df: pd.DataFrame, table: str,
                bk_col: str, track_cols: list[str]) -> int:
    """
    Standard SCD2 merge:
    1. Find BKs where tracked columns changed → expire old row, insert new.
    2. Insert new BKs.
    Returns count of rows upserted.
    """
    from sqlalchemy import inspect as sa_inspect

    now = datetime.utcnow()

    # Drop pandas artifact columns (e.g. __index_level_0__ from Delta reads)
    new_df = new_df.loc[:, ~new_df.columns.str.startswith("__")]

    # Only write columns that exist in the target table (drops Silver metadata)
    inspector = sa_inspect(engine)
    target_cols = {c["name"] for c in inspector.get_columns(table, schema=SCHEMA)}

    existing = pd.read_sql(
        f"SELECT * FROM {SCHEMA}.{table} WHERE is_current = TRUE", engine
    )

    rows_upserted = 0

    if existing.empty:
        new_df["valid_from_ts"] = now
        new_df["valid_to_ts"]   = None
        new_df["is_current"]    = True
        cols_to_write = [c for c in new_df.columns if c in target_cols]
        new_df[cols_to_write].to_sql(table, engine, schema=SCHEMA, if_exists="append", index=False)
        return len(new_df)

    existing_lookup = existing.set_index(bk_col)

    to_insert = []
    bks_to_expire = []

    for _, row in new_df.iterrows():
        bk = row[bk_col]
        if bk not in existing_lookup.index:
            new_row = row.to_dict()
            new_row.update({"valid_from_ts": now, "valid_to_ts": None, "is_current": True})
            to_insert.append(new_row)
        else:
            existing_row = existing_lookup.loc[bk]
            changed = any(str(row[c]) != str(existing_row.get(c, "")) for c in track_cols)
            if changed:
                bks_to_expire.append(bk)
                new_row = row.to_dict()
                new_row.update({"valid_from_ts": now, "valid_to_ts": None, "is_current": True})
                to_insert.append(new_row)

    with engine.begin() as conn:
        if bks_to_expire:
            ids_str = ",".join(f"'{b}'" for b in bks_to_expire)
            conn.execute(text(f"""
                UPDATE {SCHEMA}.{table}
                SET is_current = FALSE, valid_to_ts = '{now}'
                WHERE {bk_col} IN ({ids_str}) AND is_current = TRUE
            """))

        if to_insert:
            insert_df = pd.DataFrame(to_insert)
            cols_to_write = [c for c in insert_df.columns if c in target_cols]
            insert_df[cols_to_write].to_sql(
                table, conn, schema=SCHEMA, if_exists="append", index=False
            )
            rows_upserted = len(to_insert)

    return rows_upserted


# ── dim_customer ──────────────────────────────────────────────────────────────

def load_dim_customer(engine, silver_base: str, storage_options=None) -> int:
    df = read_delta(f"{silver_base}/stg_customers", storage_options=storage_options)
    n = _scd2_merge(engine, df, "dim_customer", "customer_id",
                    track_cols=["risk_segment", "kyc_status", "city"])
    logger.info(f"[dim_customer] SCD2 upserted {n} rows.")
    return n


# ── dim_merchant ──────────────────────────────────────────────────────────────

def load_dim_merchant(engine, silver_base: str, storage_options=None) -> int:
    df = read_delta(f"{silver_base}/stg_merchants", storage_options=storage_options)
    n = _scd2_merge(engine, df, "dim_merchant", "merchant_id",
                    track_cols=["is_active", "category"])
    logger.info(f"[dim_merchant] SCD2 upserted {n} rows.")
    return n


# ── dim_card (SCD1 — upsert) ──────────────────────────────────────────────────

def load_dim_card(engine, silver_base: str, storage_options=None) -> int:
    df = read_delta(f"{silver_base}/stg_cards", storage_options=storage_options)
    cols = ["card_id", "customer_id", "card_type", "issuing_bank",
            "card_country", "issued_ts", "expiry_ts", "is_active"]
    df = df[[c for c in cols if c in df.columns]]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.dim_card
                    (card_id, customer_id, card_type, issuing_bank,
                     card_country, issued_ts, expiry_ts, is_active)
                VALUES
                    (:card_id, :customer_id, :card_type, :issuing_bank,
                     :card_country, :issued_ts, :expiry_ts, :is_active)
                ON CONFLICT (card_id) DO UPDATE SET
                    is_active    = EXCLUDED.is_active,
                    expiry_ts    = EXCLUDED.expiry_ts
            """), row.to_dict())
    logger.info(f"[dim_card] SCD1 upserted {len(df)} rows.")
    return len(df)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    silver_base  = cfg["silver_base_path"]
    engine       = get_sqlalchemy_engine(cfg)
    storage_opts = get_storage_options(cfg)
    start_ts     = datetime.utcnow()
    total_rows   = 0

    try:
        load_dim_date(engine)
        load_dim_transaction_status(engine)
        total_rows += load_dim_customer(engine, silver_base, storage_options=storage_opts)
        total_rows += load_dim_merchant(engine, silver_base, storage_options=storage_opts)
        total_rows += load_dim_card(engine, silver_base, storage_options=storage_opts)

        log_run("gold_dim_load", start_ts, datetime.utcnow(),
                "success", 0, total_rows, cfg=cfg)
        logger.info(f"[gold_dim_load] Done. {total_rows} dim rows upserted.")

    except Exception as e:
        log_run("gold_dim_load", start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[gold_dim_load] FAILED: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()