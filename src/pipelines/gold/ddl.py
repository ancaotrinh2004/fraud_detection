"""src/pipelines/gold/ddl.py — Create all Gold tables in PostgreSQL.

Naming conventions (standardised):
  • surrogate keys  → <entity>_key   (dim_customer.customer_key, fact_transaction.transaction_key)
  • timestamps      → <name>_ts      (transaction_ts, event_ts, created_ts, label_ts)
  • dimensional booleans → is_*       (feature flags keep the f_*_flag namespace)

Fraud labels do NOT live in fact_transaction (labels arrive late via chargeback /
investigation). The fact table is immutable transaction facts only; the label is
owned by ml_fraud_label with its own label_ts / label_status lifecycle.
"""

SCHEMA = "gold_fraud"

# Base currency for amount normalisation. amount_base = amount * fx_rate (→ VND).
BASE_CURRENCY = "VND"
_CURRENCY_RATES = {
    "VND": 1,
    "USD": 24000,
    "EUR": 26000,
    "GBP": 30000,
    "SGD": 18000,
    "JPY": 160,
}

DDL_STATEMENTS = [
    # ── dim_date ─────────────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_date (
        date_key            INT PRIMARY KEY,
        calendar_date       DATE NOT NULL,
        day_of_week         INT,
        day_name            TEXT,
        month               INT,
        year                INT,
        is_weekend          BOOLEAN,
        is_public_holiday_vn BOOLEAN DEFAULT FALSE
    );
    """,

    # ── dim_transaction_status ───────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_transaction_status (
        status_key   SERIAL PRIMARY KEY,
        status_name  TEXT UNIQUE NOT NULL
    );
    """,

    # ── dim_currency_rate ────────────────────────────────────────────────────
    # FX reference used to normalise amount → amount_base (BASE_CURRENCY).
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_currency_rate (
        currency        VARCHAR(3) PRIMARY KEY,
        rate_to_base    NUMERIC(18,6) NOT NULL,
        base_currency   VARCHAR(3) NOT NULL DEFAULT '{BASE_CURRENCY}',
        rate_ts         TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """,

    # ── dim_customer (SCD2) ──────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_customer (
        customer_key    BIGSERIAL PRIMARY KEY,
        customer_id     TEXT NOT NULL,
        signup_ts       TIMESTAMP,
        country         TEXT,
        city            TEXT,
        risk_segment    TEXT,
        kyc_status      TEXT,
        marketing_opt_in BOOLEAN,
        valid_from_ts   TIMESTAMP NOT NULL,
        valid_to_ts     TIMESTAMP,
        is_current      BOOLEAN NOT NULL DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS idx_dim_customer_id
        ON {SCHEMA}.dim_customer(customer_id) WHERE is_current;
    """,

    # ── dim_merchant (SCD2) ──────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_merchant (
        merchant_key    BIGSERIAL PRIMARY KEY,
        merchant_id     TEXT NOT NULL,
        merchant_name   TEXT,
        category        TEXT,
        country         TEXT,
        city            TEXT,
        is_active       BOOLEAN,
        valid_from_ts   TIMESTAMP NOT NULL,
        valid_to_ts     TIMESTAMP,
        is_current      BOOLEAN NOT NULL DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS idx_dim_merchant_id
        ON {SCHEMA}.dim_merchant(merchant_id) WHERE is_current;
    """,

    # ── dim_card (SCD1) ──────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_card (
        card_key        BIGSERIAL PRIMARY KEY,
        card_id         TEXT UNIQUE NOT NULL,
        customer_id     TEXT,
        card_type       TEXT,
        issuing_bank    TEXT,
        card_country    TEXT,
        issued_ts       TIMESTAMP,
        expiry_ts       TIMESTAMP,
        is_active       BOOLEAN
    );
    CREATE INDEX IF NOT EXISTS idx_dim_card_id ON {SCHEMA}.dim_card(card_id);
    """,

    # ── fact_transaction ─────────────────────────────────────────────────────
    # Immutable transaction facts. NO fraud label here (see ml_fraud_label).
    # Surrogate transaction_key is the identity; transaction_id is the natural
    # (degenerate) key. PostgreSQL requires the partition column in every PK/UNIQUE,
    # hence both keys are composite with transaction_date_key.
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_transaction (
        transaction_key     BIGSERIAL,
        transaction_id      TEXT NOT NULL,
        customer_key        BIGINT REFERENCES {SCHEMA}.dim_customer(customer_key),
        card_key            BIGINT REFERENCES {SCHEMA}.dim_card(card_key),
        merchant_key        BIGINT REFERENCES {SCHEMA}.dim_merchant(merchant_key),
        status_key          INT REFERENCES {SCHEMA}.dim_transaction_status(status_key),
        transaction_date_key INT REFERENCES {SCHEMA}.dim_date(date_key),
        transaction_ts      TIMESTAMP NOT NULL,
        created_ts          TIMESTAMP,
        amount              NUMERIC(18,2),
        currency            VARCHAR(3),
        amount_base         NUMERIC(18,2),
        fx_rate             NUMERIC(18,6),
        fx_rate_ts          TIMESTAMP,
        city                TEXT,
        device_fingerprint  TEXT,
        ip_country          TEXT,
        PRIMARY KEY (transaction_key, transaction_date_key),
        UNIQUE (transaction_id, transaction_date_key)
    ) PARTITION BY RANGE (transaction_date_key);
    CREATE INDEX IF NOT EXISTS idx_fact_txn_customer
        ON {SCHEMA}.fact_transaction(customer_key, transaction_ts);
    """,

    # ── fact_fraud_event ─────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_fraud_event (
        event_key           BIGSERIAL PRIMARY KEY,
        event_id            TEXT NOT NULL,
        customer_key        BIGINT REFERENCES {SCHEMA}.dim_customer(customer_key),
        event_date_key      INT REFERENCES {SCHEMA}.dim_date(date_key),
        event_type          TEXT,
        event_ts            TIMESTAMP NOT NULL,
        created_ts          TIMESTAMP,
        session_id          TEXT,
        device_type         TEXT,
        ip_country          TEXT,
        card_key            BIGINT,
        merchant_key        BIGINT,
        amount              NUMERIC(18,2),
        failure_reason      TEXT,
        is_otp_failed       SMALLINT DEFAULT 0,
        is_declined         SMALLINT DEFAULT 0,
        is_transaction_attempt SMALLINT DEFAULT 0,
        UNIQUE (event_id, event_ts)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_event_customer_ts
        ON {SCHEMA}.fact_fraud_event(customer_key, event_ts);
    """,

    # ── obt_transaction_fraud_summary ────────────────────────────────────────
    # Denormalised BI table. fraud_label comes from ml_fraud_label (current best
    # confirmed label; NULL = not yet labelled). is_high_value on amount_base.
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.obt_transaction_fraud_summary (
        transaction_id      TEXT PRIMARY KEY,
        transaction_ts      TIMESTAMP,
        transaction_date_key INT,
        customer_id         TEXT,
        customer_city       TEXT,
        customer_risk_segment TEXT,
        merchant_id         TEXT,
        merchant_category   TEXT,
        card_type           TEXT,
        issuing_bank        TEXT,
        card_country        TEXT,
        amount              NUMERIC(18,2),
        currency            VARCHAR(3),
        amount_base         NUMERIC(18,2),
        transaction_status  TEXT,
        city                TEXT,
        ip_country          TEXT,
        device_fingerprint  TEXT,
        fraud_label         SMALLINT,
        label_status        TEXT,
        is_cross_border     SMALLINT,
        is_night_transaction SMALLINT,
        is_high_value       SMALLINT
    );
    CREATE INDEX IF NOT EXISTS idx_obt_date_fraud
        ON {SCHEMA}.obt_transaction_fraud_summary(transaction_date_key, fraud_label);
    """,

    # ── Feature tables ────────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_customer_90d (
        customer_id                     TEXT NOT NULL,
        event_ts                        TIMESTAMP NOT NULL,
        created_ts                      TIMESTAMP,
        f_customer_total_txn_90d        INT,
        f_customer_avg_txn_amount_90d   NUMERIC(18,2),
        f_customer_distinct_merchants_90d INT,
        f_customer_decline_rate_90d     NUMERIC(6,4),
        f_customer_foreign_txn_ratio_90d NUMERIC(6,4),
        f_customer_night_txn_ratio_90d  NUMERIC(6,4),
        PRIMARY KEY (customer_id, event_ts)
    );
    CREATE INDEX IF NOT EXISTS idx_feat_c90d_pit
        ON {SCHEMA}.feat_customer_90d(customer_id, event_ts DESC);
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_stream_30m (
        customer_id                     TEXT NOT NULL,
        event_ts                        TIMESTAMP NOT NULL,
        created_ts                      TIMESTAMP,
        f_stream_otp_failed_count_30m   INT,
        f_stream_decline_count_30m      INT,
        f_stream_txn_velocity_1h        INT,
        f_stream_new_merchant_flag      SMALLINT,
        f_stream_burst_activity_flag    SMALLINT,
        PRIMARY KEY (customer_id, event_ts)
    );
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_customer_unified (
        customer_id                     TEXT NOT NULL,
        event_ts                        TIMESTAMP NOT NULL,
        created_ts                      TIMESTAMP,
        f_customer_total_txn_90d        INT,
        f_customer_avg_txn_amount_90d   NUMERIC(18,2),
        f_customer_distinct_merchants_90d INT,
        f_customer_decline_rate_90d     NUMERIC(6,4),
        f_customer_foreign_txn_ratio_90d NUMERIC(6,4),
        f_customer_night_txn_ratio_90d  NUMERIC(6,4),
        f_stream_otp_failed_count_30m   INT,
        f_stream_decline_count_30m      INT,
        f_stream_txn_velocity_1h        INT,
        f_stream_new_merchant_flag      SMALLINT,
        f_stream_burst_activity_flag    SMALLINT,
        PRIMARY KEY (customer_id, event_ts)
    );
    CREATE INDEX IF NOT EXISTS idx_feat_unified_pit
        ON {SCHEMA}.feat_customer_unified(customer_id, event_ts DESC);
    """,

    # ── Drift monitoring tables ───────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.agg_feature_health_daily (
        monitoring_date     DATE NOT NULL,
        feature_name        TEXT NOT NULL,
        mean_value          NUMERIC(18,4),
        stddev_value        NUMERIC(18,4),
        psi_vs_baseline     NUMERIC(8,4),
        alert_flag          BOOLEAN DEFAULT FALSE,
        created_ts          TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (monitoring_date, feature_name)
    );
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feature_drift_alerts (
        alert_id            BIGSERIAL PRIMARY KEY,
        alert_date          DATE NOT NULL,
        feature_name        TEXT NOT NULL,
        psi_value           NUMERIC(8,4),
        mean_before         NUMERIC(18,4),
        mean_after          NUMERIC(18,4),
        action              TEXT,
        created_ts          TIMESTAMP DEFAULT NOW()
    );
    """,

    # ── ML tables ─────────────────────────────────────────────────────────────
    # Label store — the ONLY source of truth for fraud labels. label_ts is when
    # the label was confirmed (late: chargeback / investigation); label_status
    # tracks maturity (suspected → confirmed → matured).
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ml_fraud_label (
        transaction_id      TEXT PRIMARY KEY,
        customer_id         TEXT NOT NULL,
        event_ts            TIMESTAMP NOT NULL,
        label               SMALLINT NOT NULL,
        label_ts            TIMESTAMP,
        label_status        TEXT,
        created_ts          TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_ml_label_customer_ts
        ON {SCHEMA}.ml_fraud_label(customer_id, event_ts);
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ml_fraud_scores (
        transaction_id      TEXT PRIMARY KEY,
        fraud_score         NUMERIC(6,4) NOT NULL,
        model_version       TEXT NOT NULL,
        score_ts            TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_ml_scores_model
        ON {SCHEMA}.ml_fraud_scores(model_version);
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ml_fraud_training (
        transaction_id                  TEXT PRIMARY KEY,
        customer_id                     TEXT NOT NULL,
        event_ts                        TIMESTAMP NOT NULL,
        label                           SMALLINT NOT NULL,
        f_customer_total_txn_90d        INT,
        f_customer_avg_txn_amount_90d   NUMERIC(18,2),
        f_customer_distinct_merchants_90d INT,
        f_customer_decline_rate_90d     NUMERIC(6,4),
        f_customer_foreign_txn_ratio_90d NUMERIC(6,4),
        f_customer_night_txn_ratio_90d  NUMERIC(6,4),
        f_stream_otp_failed_count_30m   INT,
        f_stream_decline_count_30m      INT,
        f_stream_txn_velocity_1h        INT,
        f_stream_new_merchant_flag      SMALLINT,
        f_stream_burst_activity_flag    SMALLINT,
        feature_snapshot_ts             TIMESTAMP,
        txn_amount                      NUMERIC(18,2),
        txn_hour                        SMALLINT,
        is_declined_txn                 SMALLINT,
        is_foreign_txn                  SMALLINT,
        created_ts                      TIMESTAMP DEFAULT NOW()
    );
    """,
]


def _seed_currency_rates_sql() -> str:
    values = ", ".join(
        f"('{cur}', {rate}, '{BASE_CURRENCY}')" for cur, rate in _CURRENCY_RATES.items()
    )
    return f"""
        INSERT INTO {SCHEMA}.dim_currency_rate (currency, rate_to_base, base_currency)
        VALUES {values}
        ON CONFLICT (currency) DO UPDATE SET
            rate_to_base = EXCLUDED.rate_to_base,
            base_currency = EXCLUDED.base_currency,
            rate_ts = NOW();
    """


def drop_all_tables(cfg: dict) -> None:
    """DROP the entire Gold schema (all tables/partitions) — destructive."""
    from src.pipelines.utils.db import pg_cursor
    schema = cfg["postgres"]["schema"]
    with pg_cursor(cfg) as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    print(f"✓ Dropped schema {schema} (all tables removed).")


def create_all_tables(cfg: dict) -> None:
    from src.pipelines.utils.db import pg_cursor, ensure_schema
    ensure_schema(cfg)
    with pg_cursor(cfg) as cur:
        for ddl in DDL_STATEMENTS:
            try:
                cur.execute(ddl)
            except Exception as e:
                # Index already exists etc — non-fatal
                import logging
                logging.getLogger(__name__).warning(f"DDL warning: {e}")
        # Seed FX rates after dim_currency_rate exists
        cur.execute(_seed_currency_rates_sql())
    print("✓ All Gold tables created (+ dim_currency_rate seeded).")
