# Fraud Detection Data Generator Improvement: Feature Drift & Labels

## 1. Objective

Extend the generator (01_data_generator.md) with **feature drift** simulation.

Goals:
- Simulate how feature distributions change over time (fraud-pattern evolution).
- Test feature-store monitoring and drift detection in the Gold layer.
- Create a Gold label table for ML training.
- Join label + feature tables into the training table used in 04.1_ml_design.md.
- Demonstrate covariate drift: input distribution changes while the fraud rule stays fixed.

---

## 2. What is Feature Drift?

Feature drift: the distribution of a computed feature changes over time, degrading a model trained on the old distribution.

Example: `f_customer_avg_txn_amount_90d` baseline mean ≈ 907k VND. After drift ≈ 1.18M VND (+30%). A model trained on the baseline under-scores high-amount transactions that are now normal, raising false positives.

---

## 3. Drift Scenarios: Pick At Least One

### Scenario A: Customer Transaction Frequency Drift
- **Change**: 2 → 3 tx/customer/month after `drift_start_date`. **Inject**: +50% transaction rate. **Feature**: `f_customer_total_txn_90d` ↑.

### Scenario B: Transaction Amount Drift ✅ (Chosen)
- **Change**: average amount +30% after `drift_start_date`, ramped over 30 days (gradual). **Inject**: multiply `amount` by a time-varying factor `1.0 + 0.30 × min(days_since_drift / ramp_days, 1)`. **Feature**: `f_customer_avg_txn_amount_90d` ↑ ~30%.
- **Why B**: transaction amount is the strongest fraud signal (fraud rings inflate amounts before a card is blocked); drift flows end-to-end (generator → Bronze → Silver → Gold → feature store → drift monitor), exercising the whole stack with one realistic covariate shift.

### Scenario C: Merchant Category Drift
- **Change**: category mix shift (grocery 40→20%, electronics 20→45%). **Feature**: `f_customer_distinct_merchants_90d` + fraud-rate correlation.

---

## 4. Drift Configuration Parameters

Added to `configs/generate_config.yaml`:

```yaml
# Feature drift — Scenario B: Transaction Amount Drift
drift_enabled: true
drift_start_date: "2025-08-01"
drift_mode: "gradual"          # gradual: ramp over drift_ramp_days; abrupt: instant
scenario_B_amount: true
amount_drift_multiplier: 1.30  # +30% after drift_start_date
drift_ramp_days: 30
```

---

## 5. Drift Detection & Alerting (implemented)

The `drift_monitor` pipeline (`src/pipelines/features/drift_monitor.py`, DAG `dag_drift_monitor`) computes **weekly PSI** for all 11 `feat_customer_unified` features **plus the prediction score**, using quantile bins against a 60-day baseline.

| PSI | Severity |
|---|---|
| ≥ 0.10 | Warning |
| ≥ 0.20 | Critical |

**Alerting is rule-based, not code.** The pipeline only pushes metrics (`fraud_drift_psi_<feature>`, `fraud_drift_run_status`) to the Pushgateway. Prometheus scrapes them; the `fraud-drift-alerts` PrometheusRule evaluates the thresholds and Alertmanager routes `category: drift` alerts to **Discord** (native `discord_configs`). See 04.1 §7 and `infra/k8s/fraud-drift-alerts.yaml`.

**Observed result** (Scenario B drift, real run): `f_customer_avg_txn_amount_90d` PSI ≈ **0.30 (critical)** — the injected amount drift is clearly detected. Other features also breach due to the offline distribution shifts: `f_customer_foreign_txn_ratio_90d` 1.84, `f_customer_total_txn_90d` 1.72, `f_customer_distinct_merchants_90d` 1.72 (critical); `f_customer_decline_rate_90d` 0.15, `f_customer_night_txn_ratio_90d` 0.12 (warning).

---

## 6. Gold Layer Monitoring & Label/Training Tables

### Table 1: agg_feature_health_daily
Per-feature, per-window health. The drift monitor upserts one row per `(monitoring_date, feature_name)`.

| Column | Type | Description |
|---|---|---|
| monitoring_date | DATE | window end date |
| feature_name | VARCHAR | monitored feature |
| mean_value / stddev_value | NUMERIC | window stats |
| psi_vs_baseline | NUMERIC | PSI vs 60-day baseline |
| alert_flag | BOOLEAN | PSI ≥ 0.10 |

### Table 2: feature_drift_alerts
One row per drifted window (PSI ≥ 0.10).

| Column | Type | Description |
|---|---|---|
| alert_date | DATE | window end |
| feature_name | VARCHAR | feature |
| psi_value | NUMERIC | window PSI |
| mean_before / mean_after | NUMERIC | baseline vs window mean |
| action | TEXT | recommended action |

### Table 3: ml_fraud_label
Label-only Gold table, one row per transaction.

| Column | Type | Description |
|---|---|---|
| transaction_id | UUID | PK |
| customer_id | VARCHAR | join key |
| event_ts | TIMESTAMP | transaction time — **point-in-time (PIT) join** key |
| label | SMALLINT | `is_fraud` from `fact_transaction` (≥2 conditions) |
| created_ts | TIMESTAMP | ingestion time |

Total rows ≈ **1,440,000** (~10% fraud → ~144,000 positives).

### Table 4: ml_fraud_training
Built in two steps:
1. **PIT join** `ml_fraud_label` against `feat_customer_90d` (latest snapshot with `event_ts ≤ label.event_ts`), plus context from `fact_transaction` / `dim_card`.
2. **Per-transaction streaming features** from `stg_fraud_events` sliding windows (30 min / 1 h) at each transaction's exact `event_ts` (processed date-by-date to avoid OOM on ~34M events).

Columns: `transaction_id`, `customer_id`, `event_ts`, `label`, the 6 `f_customer_*_90d` features, the 5 `f_stream_*` features, plus `txn_amount`, `txn_hour`, `is_declined_txn`, `is_foreign_txn`, `created_ts` — 17 model features total (15 stored + derived `txn_amount_ratio`, `is_night_txn`).

```sql
LEFT JOIN LATERAL (
  SELECT * FROM gold_fraud.feat_customer_90d u
  WHERE u.customer_id = l.customer_id AND u.event_ts <= l.event_ts
  ORDER BY u.event_ts DESC LIMIT 1
) f ON TRUE
```

---

## 7. Deliverables

1. **Generator code** — `src/generator/offline/transactions.py` (Scenario B injection) + drift params in `configs/generate_config.yaml`.
2. **Drift detection** — `drift_monitor` pipeline computing weekly PSI for 11 features + score.
3. **Gold monitoring tables** — `agg_feature_health_daily`, `feature_drift_alerts` populated.
4. **Rule-based alerting** — `fraud-drift-alerts` PrometheusRule → Alertmanager → Discord (no code alerts).
5. **Gold label table** — `ml_fraud_label` (~1.44M rows, `event_ts` + `label`).
6. **Gold training table** — `ml_fraud_training` (~1.44M rows) via PIT label+feature join.
7. **Explanation** — Scenario B: 30% gradual amount drift from 2025-08-01; `f_customer_avg_txn_amount_90d` PSI ≈ 0.30 (critical) → drift detected end-to-end.
