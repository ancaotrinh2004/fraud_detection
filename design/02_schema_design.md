# Fraud Detection — Gold Zone Schema Design

## 1. Goal

A business-ready Gold model for fraud analytics/BI plus an ML feature store, served by implemented Bronze→Silver→Gold→Feature pipelines with DataHub lineage.

**Approach:** Fact–Dimension + OBT + feature tables. **Schema:** `gold_fraud`, prefixes `dim_` / `fact_` / `obt_` / `feat_`. Bronze tables `raw_`, Silver `stg_`.

**Storage (cost-focused):** Bronze and Silver are **Delta Lake on MinIO** (object storage); Gold + features are PostgreSQL.

### 1.1 Input Data Profile (from Session 01)

| Dataset | Type | Key columns | Volume |
|---|---|---|---|
| customers / merchants / cards | offline (Parquet) | *_id, attributes | 100k / 15k / 130k |
| transactions | offline (Parquet) | transaction_id, customer_id, card_id, merchant_id, transaction_timestamp, amount, currency, transaction_status, is_fraud | ~1.44M over 180 days |
| fraud.events.raw | streaming (JSON) | event_id, event_type, event_timestamp, created_ts, customer_id | ~34M events |

**Characteristics / known issues (from 01):** geographic skew (80% top-5 cities), high-cardinality IDs, schema evolution (`device_fingerprint`/`ip_country` only after 2025-07-01), 2% offline duplicates, 15% late stream arrivals, 1.5% duplicate events, ~10% fraud label rate.

**Assumptions & SLA targets:** Gold tables feed BI + ML training/scoring; out of scope: explainability/governance.

| Target | Value |
|---|---|
| Gold fact/OBT freshness | ≤ 30 min incremental |
| Feature freshness | 90d ≤ 60 min · stream ≤ 5 min · unified ≤ 15 min |
| Scheduled-run success | ≥ 99%/week |

---

## 2. Dimension Tables

| Dimension | Grain | Key Columns | SCD |
|---|---|---|---|
| `dim_customer` | one per customer version | customer_key (SK), customer_id (BK), signup_ts, country, city, risk_segment, kyc_status, marketing_opt_in, valid_from_ts, valid_to_ts, is_current | SCD2 (risk_segment / kyc_status change) |
| `dim_merchant` | one per merchant version | merchant_key (SK), merchant_id (BK), merchant_name, category, country, city, is_active, valid_*_ts, is_current | SCD2 |
| `dim_card` | one per card | card_key (SK), card_id (BK), customer_id, card_type (credit\|debit\|prepaid), issuing_bank, card_country, issued_ts, expiry_ts, is_active | SCD1 |
| `dim_currency_rate` | one per (currency, date) | currency, rate_date, fx_rate_to_vnd | static reference |
| `dim_date` | one per date | date_key (YYYYMMDD), calendar_date, day_of_week, month, year, is_weekend | static |
| `dim_transaction_status` | one per status | status_key (SK), status_name (approved\|declined\|pending\|reversed) | static |

SK = warehouse surrogate key; BK = source business key. `dim_customer` is SCD2 for point-in-time-correct ML training.

---

## 3. Fact Tables

### 3.1 `fact_transaction`
**Grain:** one per transaction (post-dedup). **Keys:** customer_key, card_key, merchant_key, transaction_date_key, status_key. **Measures:** amount, amount_base (= amount × fx_rate, normalised to VND), is_fraud, is_approved, is_declined.

Core columns: `transaction_sk`, `transaction_id` (BK), the 5 FKs, `transaction_ts`, `created_ts`, `amount`, `currency`, `fx_rate`, `amount_base`, `city`, `device_fingerprint`*, `ip_country`*, `is_fraud`, `is_approved`, `is_declined`. (* nullable pre-2025-07-01.)

- **Dedup:** Silver keeps the latest `created_ts` per `transaction_id` (removes the 2% duplicates).
- **Schema evolution:** nullable fraud-signal columns; downstream `COALESCE(ip_country,'unknown')`.

### 3.2 `fact_fraud_event`
**Grain:** one per streaming event (post-dedup). **Keys:** customer_key, event_date_key. **Measures:** is_otp_failed, is_declined, is_transaction_attempt, amount.

Core columns: `event_sk`, `event_id` (BK), `customer_key`, `event_date_key`, `event_type`, `event_ts`, `created_ts`, `session_id`, `device_type`, `ip_country`, `card_key`/`merchant_key` (nullable), `amount` (nullable), `failure_reason` (nullable), derived flags. Deduped by `(event_id, event_ts)`.

---

## 4. OBT Table

### 4.1 `obt_transaction_fraud_summary`
**Grain:** one per transaction. **Purpose:** denormalized fraud-analyst BI table (no query-time joins).

Columns: transaction_id, transaction_ts, customer_id/city/risk_segment, merchant_id/category, card_type, issuing_bank, card_country, amount, amount_base, currency, transaction_status, city, ip_country, device_fingerprint, is_fraud, transaction_date_key + derived flags:

| Flag | Logic |
|---|---|
| `is_cross_border` | `ip_country IS NOT NULL AND lower(ip_country) <> lower(card_country)` |
| `is_night_transaction` | `EXTRACT(hour FROM transaction_ts) BETWEEN 1 AND 4` |
| `is_high_value` | `amount_base > 5,000,000 VND` |

---

## 5. Refresh & Data Quality

| Table | Refresh mode | Freshness |
|---|---|---|
| dims | daily / SCD merge | ≤ 30 min |
| `fact_transaction` | incremental merge by `transaction_date` | ≤ 30 min/partition |
| `fact_fraud_event` | micro-batch from stream | ≤ 5 min |
| `obt_transaction_fraud_summary` | incremental merge by `transaction_id` | ≤ 30 min |

**Data contracts (Great Expectations)** run as the `validate_contracts` step; critical violations fail the DAG. Key checks:

| Check | Table | Rule | Action |
|---|---|---|---|
| Uniqueness | fact_transaction | `transaction_id` unique post-dedup | halt |
| Uniqueness | fact_fraud_event | `(event_id, event_ts)` unique | halt |
| Referential | fact_transaction | FKs exist in dims | quarantine |
| Null | fact_transaction | id/keys/amount/`transaction_ts` not null | halt |
| Value set | dim_card.card_type | ∈ {credit, debit, prepaid} | warn |
| Range | fact_transaction.amount_base | 0 ≤ x ≤ 2e12 VND | warn |
| Fraud rate | fact_transaction | daily ~10% | alert |

---

## 6. Feature Store

All feature tables live in `gold_fraud`. Each row carries `event_ts` (point-in-time join key) and `created_ts` (dedup).

### 6.1 `feat_customer_90d` — grain `(customer_id, event_ts)`, refresh ≤ 60 min
`f_customer_total_txn_90d`, `f_customer_avg_txn_amount_90d`, `f_customer_distinct_merchants_90d`, `f_customer_decline_rate_90d`, `f_customer_foreign_txn_ratio_90d`, `f_customer_night_txn_ratio_90d` (from `stg_transactions` + `dim_card`).

### 6.2 `feat_stream_30m` — grain `(customer_id, event_ts)`, refresh ≤ 5 min
`f_stream_otp_failed_count_30m`, `f_stream_decline_count_30m`, `f_stream_txn_velocity_1h`, `f_stream_new_merchant_flag`, `f_stream_burst_activity_flag` (rolling windows over `fact_fraud_event`).

### 6.3 `feat_customer_unified` — grain `(customer_id, event_ts)`, refresh ≤ 15 min
Point-in-time join of 6.1 + 6.2 — the direct input to ML training/scoring and the drift monitor.

### 6.4 Point-in-time Correctness
Joins use **`feat.event_ts ≤ label.event_ts`** (no future leakage). On duplicate `(customer_id, event_ts)`, keep the latest `created_ts`.

---

## 7. Data Pipeline Design & Implementation

### 7.1 Stack
Airflow 3.2.0 on Kind (Helm `apache-airflow 1.21.0`), CeleryExecutor. Data DAGs use TaskFlow `@task`; ML jobs use `KubernetesPodOperator`. Worker image `ancaotrinh/fraud-airflow:latest` bakes `src/pipelines/` (DAGs delivered via the `airflow-dags` ConfigMap, subPath-mounted). Bronze/Silver = **Delta Lake on MinIO** (read with DuckDB/deltalake). Streaming via **Kafka (Strimzi)**, topic `fraud.events.raw`. Experiment tracking + registry: **MLflow** (artifacts in MinIO `mlflow-artifacts`). Contracts: **Great Expectations**. Lineage: **DataHub 1.0.0**. Config: `configs/pipeline_config.yaml` → ConfigMap `fraud-pipeline-config`.

### 7.2 Pipeline Groups (14 DAGs)
- **Batch:** `batch_bronze` → `batch_silver` → `batch_gold` (dims/facts/OBT) and `batch_features` (`feat_customer_90d`).
- **Stream:** `stream_bronze` → `stream_silver` → `stream_features` (`feat_stream_30m`).
- **Unify/backfill:** `feat_unified` (merge → `feat_customer_unified`), `feat_backfill`.
- **ML:** `ml_label` → `ml_train` → `ml_batch_score`, plus `ml_retrain_trigger`.
- **Monitoring:** `drift_monitor`.

### 7.3 SLA Targets
Bronze ≤ 10 min · Silver ≤ 30 min · Gold fact/OBT ≤ 30 min · features per §1.1 · ≥ 99% scheduled-run success/week. (Achieved values reported post-implementation on the single-node cluster.)

### 7.4 Update Strategy
- Bronze: append-only with ingest metadata (`ingest_ts`, `batch_id`).
- Silver: incremental, dedup by business key + event time.
- Gold dims/facts/OBT: incremental merge/upsert on stable keys (SCD2 for dims).
- Features: rolling-window recompute, merge by `(customer_id, event_ts)` keeping latest `created_ts`.
- Backfill: off by default; `feat_backfill` re-runs bounded windows idempotently. Late data: reprocess affected windows.

### 7.5 Controls, Monitoring & Lineage
- **Quality gates** per run (schema/uniqueness/null/referential via GE); critical → halt.
- **Run metadata** in `pipeline_run_log` (run_id, start/end, status, row counts, error).
- **Recovery:** retry ×3 with backoff (`configs/pipeline_config.yaml`); quarantine bad rows.
- **Observability:** structured logs → Loki; metrics → Pushgateway/Prometheus; traces → Jaeger.
- **Lineage:** DataHub captures Bronze→Silver→Gold→Feature dataset + job lineage (e.g. `feat_customer_unified` upstream graph), emitted via the Airflow lineage plugin.

---

## 8. Warehouse Optimization

- **Workload:** fraud-analyst BI on `obt_transaction_fraud_summary` and the PIT feature join in ML training.
- **Bottleneck:** full scans of `fact_transaction` by date; repeated lateral PIT lookups on `feat_customer_90d`.
- **Optimization applied:**
  1. `fact_transaction` indexed on `(customer_key, transaction_ts)` and date-keyed (`transaction_date_key`) for partition pruning.
  2. `obt_transaction_fraud_summary` materialized (denormalized) so BI avoids 5-table joins.
  3. Composite index `(customer_id, event_ts)` on `feat_customer_90d` for the `ORDER BY event_ts DESC LIMIT 1` PIT lookup.
- **Result:** date-range BI and PIT joins avoid full scans (index/range seeks); OBT removes join cost at query time.
- **Trade-off:** extra write cost on incremental loads and index maintenance.

**Scope boundary with 04:** these pipelines are reused by Session 04, which adds the ML pipelines, KServe inference, and CI/CD.

---

## 9. Deliverables

1. **Gold schema** — `gold_fraud` dims/facts/OBT (Section 2–4) with SCD + dedup + schema-evolution handling.
2. **Feature store** — `feat_customer_90d`, `feat_stream_30m`, `feat_customer_unified` with PIT correctness.
3. **Pipelines** — 14 Airflow DAGs (Bronze→Silver→Gold→Feature + ML + drift), Bronze/Silver on Delta Lake.
4. **Data quality** — Great Expectations contracts, critical-fail gating, run metadata.
5. **Lineage** — DataHub 1.0.0 capturing dataset + job lineage for core tables.
6. **Warehouse optimization** — partitioning/indexing + materialized OBT per Section 8.
