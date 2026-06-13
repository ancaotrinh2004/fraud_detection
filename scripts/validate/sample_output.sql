-- ============================================================
-- Sample Output: Row counts for all Gold tables
-- Run: kubectl port-forward svc/fraud-postgres-postgresql -n fraud-infra 15432:5432
-- Then: PGPASSWORD=fraud_pass psql -h localhost -p 5432 -U fraud_user -d fraud_detection -f scripts/sample_output.sql 
-- ============================================================

\echo ''
\echo '======================================================'
\echo '  FRAUD DETECTION — GOLD ZONE ROW COUNTS'
\echo '======================================================'

-- ── Dimension Tables ──────────────────────────────────────
\echo ''
\echo '--- DIMENSION TABLES ---'

SELECT
    'dim_date'                   AS table_name,
    COUNT(*)                     AS row_count,
    MIN(calendar_date)::text     AS min_value,
    MAX(calendar_date)::text     AS max_value
FROM gold_fraud.dim_date

UNION ALL

SELECT
    'dim_customer',
    COUNT(*),
    MIN(valid_from_ts)::text,
    MAX(valid_from_ts)::text
FROM gold_fraud.dim_customer

UNION ALL

SELECT
    'dim_customer (is_current)',
    COUNT(*),
    NULL,
    NULL
FROM gold_fraud.dim_customer
WHERE is_current = TRUE

UNION ALL

SELECT
    'dim_merchant',
    COUNT(*),
    NULL,
    NULL
FROM gold_fraud.dim_merchant

UNION ALL

SELECT
    'dim_merchant (is_current)',
    COUNT(*),
    NULL,
    NULL
FROM gold_fraud.dim_merchant
WHERE is_current = TRUE

UNION ALL

SELECT
    'dim_card',
    COUNT(*),
    NULL,
    NULL
FROM gold_fraud.dim_card

UNION ALL

SELECT
    'dim_transaction_status',
    COUNT(*),
    NULL,
    NULL
FROM gold_fraud.dim_transaction_status

ORDER BY table_name;


-- ── Fact Tables ───────────────────────────────────────────
\echo ''
\echo '--- FACT TABLES ---'

SELECT
    'fact_transaction'           AS table_name,
    COUNT(*)                     AS row_count,
    MIN(transaction_ts)::text AS min_value,
    MAX(transaction_ts)::text AS max_value
FROM gold_fraud.fact_transaction

UNION ALL

SELECT
    'fact_fraud_event',
    COUNT(*),
    MIN(event_ts)::text,
    MAX(event_ts)::text
FROM gold_fraud.fact_fraud_event

ORDER BY table_name;


-- ── OBT Table ─────────────────────────────────────────────
\echo ''
\echo '--- OBT TABLE ---'

SELECT
    'obt_transaction_fraud_summary' AS table_name,
    COUNT(*)                        AS row_count,
    MIN(transaction_ts)::text AS min_value,
    MAX(transaction_ts)::text AS max_value
FROM gold_fraud.obt_transaction_fraud_summary;


-- ── Feature Tables ────────────────────────────────────────
\echo ''
\echo '--- FEATURE TABLES ---'

SELECT
    'feat_customer_90d'          AS table_name,
    COUNT(*)                     AS row_count,
    MIN(event_ts)::text   AS min_value,
    MAX(event_ts)::text   AS max_value
FROM gold_fraud.feat_customer_90d

UNION ALL

SELECT
    'feat_stream_30m',
    COUNT(*),
    MIN(event_ts)::text,
    MAX(event_ts)::text
FROM gold_fraud.feat_stream_30m

UNION ALL

SELECT
    'feat_customer_unified',
    COUNT(*),
    MIN(event_ts)::text,
    MAX(event_ts)::text
FROM gold_fraud.feat_customer_unified

ORDER BY table_name;


-- ── Pipeline Run Log ─────────────────────────────────────
\echo ''
\echo '--- PIPELINE RUN LOG (last 20 runs) ---'

SELECT
    pipeline_name,
    start_ts::timestamp(0)   AS start_ts,
    end_ts::timestamp(0)     AS end_ts,
    status,
    input_rows,
    output_rows,
    ROUND(EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60, 1) AS duration_min,
    LEFT(error_summary, 80)  AS error_summary
FROM gold_fraud.pipeline_run_log
ORDER BY start_ts DESC
LIMIT 20;


-- ── Fraud Stats Summary ───────────────────────────────────
\echo ''
\echo '--- FRAUD STATS (fact_transaction ⋈ ml_fraud_label) ---'

SELECT
    COUNT(*)                                        AS total_transactions,
    SUM(l.label)                                    AS fraud_count,
    ROUND(100.0 * SUM(l.label) / COUNT(*), 2)       AS fraud_rate_pct,
    COUNT(DISTINCT ft.customer_key)                 AS distinct_customers,
    COUNT(DISTINCT ft.merchant_key)                 AS distinct_merchants,
    ROUND(SUM(ft.amount_base)::numeric, 0)          AS total_amount_base,
    ROUND(AVG(ft.amount_base)::numeric, 2)          AS avg_amount_base
FROM gold_fraud.fact_transaction ft
LEFT JOIN gold_fraud.ml_fraud_label l
    ON l.transaction_id = ft.transaction_id;


-- ── OBT Derived Flags Summary ─────────────────────────────
\echo ''
\echo '--- OBT DERIVED FLAGS ---'

SELECT
    COUNT(*)                                        AS total_rows,
    SUM(fraud_label)                                AS fraud_count,
    SUM(is_cross_border)                            AS cross_border_count,
    SUM(is_night_transaction)                       AS night_txn_count,
    SUM(is_high_value)                              AS high_value_count,
    ROUND(100.0 * SUM(is_cross_border) / COUNT(*), 2)    AS cross_border_pct,
    ROUND(100.0 * SUM(is_night_transaction) / COUNT(*), 2) AS night_txn_pct,
    ROUND(100.0 * SUM(is_high_value) / COUNT(*), 2)      AS high_value_pct
FROM gold_fraud.obt_transaction_fraud_summary;
