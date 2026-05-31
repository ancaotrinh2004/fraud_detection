"""src/pipelines/gold/ddl.py — Create all Gold tables in PostgreSQL."""

SCHEMA = "gold_fraud"

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
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_transaction (
        transaction_sk      BIGSERIAL,
        transaction_id      TEXT NOT NULL,
        customer_key        BIGINT REFERENCES {SCHEMA}.dim_customer(customer_key),
        card_key            BIGINT REFERENCES {SCHEMA}.dim_card(card_key),
        merchant_key        BIGINT REFERENCES {SCHEMA}.dim_merchant(merchant_key),
        transaction_date_key INT REFERENCES {SCHEMA}.dim_date(date_key),
        status_key          INT REFERENCES {SCHEMA}.dim_transaction_status(status_key),
        transaction_timestamp TIMESTAMP NOT NULL,
        created_ts          TIMESTAMP,
        amount              NUMERIC(18,2),
        currency            VARCHAR(3),
        city                TEXT,
        device_fingerprint  TEXT,
        ip_country          TEXT,
        is_fraud            SMALLINT DEFAULT 0,
        is_approved         SMALLINT DEFAULT 0,
        is_declined         SMALLINT DEFAULT 0,
        PRIMARY KEY (transaction_id, transaction_date_key)
    ) PARTITION BY RANGE (transaction_date_key);
    CREATE INDEX IF NOT EXISTS idx_fact_txn_customer
        ON {SCHEMA}.fact_transaction(customer_key, transaction_timestamp);
    CREATE INDEX IF NOT EXISTS idx_fact_txn_fraud
        ON {SCHEMA}.fact_transaction(is_fraud);
    """,

    # ── fact_fraud_event ─────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_fraud_event (
        event_sk            BIGSERIAL PRIMARY KEY,
        event_id            TEXT NOT NULL,
        customer_key        BIGINT REFERENCES {SCHEMA}.dim_customer(customer_key),
        event_date_key      INT REFERENCES {SCHEMA}.dim_date(date_key),
        event_type          TEXT,
        event_timestamp     TIMESTAMP NOT NULL,
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
        UNIQUE (event_id, event_timestamp)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_event_customer_ts
        ON {SCHEMA}.fact_fraud_event(customer_key, event_timestamp);
    """,

    # ── obt_transaction_fraud_summary ────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.obt_transaction_fraud_summary (
        transaction_id      TEXT PRIMARY KEY,
        transaction_timestamp TIMESTAMP,
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
        transaction_status  TEXT,
        city                TEXT,
        ip_country          TEXT,
        device_fingerprint  TEXT,
        is_fraud            SMALLINT,
        is_cross_border     SMALLINT,
        is_night_transaction SMALLINT,
        is_high_value       SMALLINT
    );
    CREATE INDEX IF NOT EXISTS idx_obt_date_fraud
        ON {SCHEMA}.obt_transaction_fraud_summary(transaction_date_key, is_fraud);
    """,

    # ── Feature tables ────────────────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_customer_90d (
        customer_id                     TEXT NOT NULL,
        event_timestamp                 TIMESTAMP NOT NULL,
        created_ts                      TIMESTAMP,
        f_customer_total_txn_90d        INT,
        f_customer_avg_txn_amount_90d   NUMERIC(18,2),
        f_customer_distinct_merchants_90d INT,
        f_customer_decline_rate_90d     NUMERIC(6,4),
        f_customer_foreign_txn_ratio_90d NUMERIC(6,4),
        f_customer_night_txn_ratio_90d  NUMERIC(6,4),
        PRIMARY KEY (customer_id, event_timestamp)
    );
    CREATE INDEX IF NOT EXISTS idx_feat_c90d_pit
        ON {SCHEMA}.feat_customer_90d(customer_id, event_timestamp DESC);
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_stream_30m (
        customer_id                     TEXT NOT NULL,
        event_timestamp                 TIMESTAMP NOT NULL,
        created_ts                      TIMESTAMP,
        f_stream_otp_failed_count_30m   INT,
        f_stream_decline_count_30m      INT,
        f_stream_txn_velocity_1h        INT,
        f_stream_new_merchant_flag      SMALLINT,
        f_stream_burst_activity_flag    SMALLINT,
        PRIMARY KEY (customer_id, event_timestamp)
    );
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.feat_customer_unified (
        customer_id                     TEXT NOT NULL,
        event_timestamp                 TIMESTAMP NOT NULL,
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
        PRIMARY KEY (customer_id, event_timestamp)
    );
    CREATE INDEX IF NOT EXISTS idx_feat_unified_pit
        ON {SCHEMA}.feat_customer_unified(customer_id, event_timestamp DESC);
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
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ml_fraud_label (
        transaction_id      TEXT PRIMARY KEY,
        customer_id         TEXT NOT NULL,
        event_timestamp     TIMESTAMP NOT NULL,
        created_ts          TIMESTAMP,
        label               SMALLINT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ml_label_customer_ts
        ON {SCHEMA}.ml_fraud_label(customer_id, event_timestamp);
    """,

    # ── ML model registry & scores ────────────────────────────────────────────
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ml_model_registry (
        model_version       TEXT PRIMARY KEY,
        model_name          TEXT NOT NULL DEFAULT 'fraud_xgb',
        pr_auc              NUMERIC(6,4),
        f1_score            NUMERIC(6,4),
        precision_score     NUMERIC(6,4),
        recall_score        NUMERIC(6,4),
        status              TEXT NOT NULL DEFAULT 'candidate',
        artifact_path       TEXT,
        train_rows          INT,
        registered_ts       TIMESTAMP DEFAULT NOW()
    );
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
        event_timestamp                 TIMESTAMP NOT NULL,
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
    print("✓ All Gold tables created.")