# Fraud Detection Data Generator Improvement: Feature Drift & Labels

## 1. Objective

Extend the data generator from `01_data_generator.md` with **feature drift simulation**.

Goals:

- Simulate how transaction amount distributions change over time (fraud pattern evolution).
- Test feature store monitoring and drift detection in the Gold layer.
- Create a Gold label table for ML training.
- Join label + feature tables to produce the training table used in `04_ml_design.md`.
- Demonstrate covariate drift: input feature distribution changes while fraud logic remains the same.

---

## 2. What is Feature Drift?

Feature drift occurs when the statistical distribution of a computed feature changes over time, potentially degrading ML model performance.

In fraud detection, drift is common because:
- Fraudsters adapt their behaviour (higher transaction amounts, new patterns).
- Economic changes shift normal customer spending (inflation, promotions).
- New merchant categories or payment channels change the transaction mix.

Example: `f_customer_avg_txn_amount_90d` baseline mean = 907,483 VND/transaction. After drift: mean = 1,181,214 VND (+30%). A fraud model trained on baseline data would under-score high-amount transactions that are now normal — increasing false positives.

---

## 3. Drift Scenarios: Pick At Least One

### Scenario A: Customer Transaction Frequency Drift

What changes:
- Transaction frequency increases from 2 tx/month to 3 tx/month (campaign or seasonal effect).

How to inject:
- Increase transaction creation rate by 50% after `drift_start_date`.

Feature affected:
- `f_customer_total_txn_90d` will increase.

### Scenario B: Transaction Amount Drift ✅ (Chosen)

What changes:
- Average transaction amount increases by 30% after `drift_start_date` (economic inflation or fraud ring using higher-value merchants).
- Drift is gradual: ramps up over 30 days to simulate organic change, not an abrupt jump.

How to inject:
- Multiply transaction `amount` by a time-varying multiplier after `drift_start_date`.
- Gradual mode: multiplier = `1.0 + (1.30 − 1.0) × min((days_since_drift_start / ramp_days), 1.0)`.
- Abrupt mode: multiplier = 1.30 immediately at `drift_start_date`.

Feature affected:
- `f_customer_avg_txn_amount_90d` will increase ~30% by end of ramp.

Gold monitoring:
- Track weekly PSI of raw `amount` distribution in `fact_transaction` vs baseline (first 60 days).
- Alert when PSI > 0.025 (calibrated for lognormal transaction amount distribution — see Section 5).

### Scenario C: Merchant Category Drift

What changes:
- Shift in merchant category mix: grocery 40% → 20%, electronics 20% → 45%.

How to inject:
- Change merchant category sampling weights after `drift_start_date`.

Feature affected:
- `f_customer_distinct_merchants_90d` and fraud rate correlation.

---

## 4. Why Scenario B?

Scenario B was chosen because transaction amount is the single most important signal in fraud detection:

1. **Fraud rings inflate amounts** — stolen credentials are used for high-value purchases before being blocked.
2. **Realistic temporal pattern** — amount inflation is gradual (economic) or abrupt (fraud campaign). Both modes are supported.
3. **Clear monitoring signal** — PSI on the amount distribution directly measures the covariate shift that matters most to the fraud model.
4. **End-to-end testability** — drift is injected at generator level, flows through Bronze → Silver → Gold → feature store → drift monitor, validating the full pipeline.

---

## 5. Drift Configuration Parameters

Added to `config/generate_config.yaml`:

```yaml
# Feature drift — Scenario B: Transaction Amount Drift
drift_enabled: true
drift_start_date: "2025-08-01"
drift_mode: "gradual"          # gradual: ramp up over drift_ramp_days; abrupt: instant jump
scenario_B_amount: true
amount_drift_multiplier: 1.30  # 30% increase after drift_start_date
drift_ramp_days: 30            # days to reach full multiplier (gradual mode only)
```

**PSI threshold note:** Transaction amounts follow a lognormal(13, 1.2) distribution with very high variance (std ≈ 1.8× mean). A 30% mean shift produces PSI ≈ 0.04, well below the credit-score standard of 0.15. The threshold was calibrated to **0.025** based on the observed stable-period PSI (< 0.001) versus post-drift PSI (0.027–0.045), giving a clean separation with no false positives.

---

## 6. Example Output File

File: `data/drift_validation_report.csv`

```
date        | feature_name                      | mean_value   | psi_vs_baseline | drift_status
2025-06-03  | f_customer_avg_txn_amount_90d     | 907,483      | 0.0000          | baseline
2025-07-15  | f_customer_avg_txn_amount_90d     | 902,593      | 0.0001          | stable
2025-08-12  | f_customer_avg_txn_amount_90d     | 988,001      | 0.0040          | stable
2025-08-19  | f_customer_avg_txn_amount_90d     | 1,038,797    | 0.0129          | ramp_up
2025-08-26  | f_customer_avg_txn_amount_90d     | 1,110,894    | 0.0267          | detected ⚠
2025-09-02  | f_customer_avg_txn_amount_90d     | 1,190,816    | 0.0448          | detected ⚠
2025-09-30  | f_customer_avg_txn_amount_90d     | 1,168,350    | 0.0442          | detected ⚠
```

Full report: 18 weekly windows — 10 stable, 2 ramp_up, **6 detected**.

---

## 7. Gold Layer Monitoring Tables

### Table 1: agg_feature_health_daily

Tracks weekly PSI of `amount` distribution vs baseline (first 60 days of transactions).

Schema:

| Column | Type | Description |
|--------|------|-------------|
| monitoring_date | DATE | End date of the weekly window |
| feature_name | VARCHAR | `f_customer_avg_txn_amount_90d` |
| mean_value | NUMERIC | Weekly mean amount (VND) |
| stddev_value | NUMERIC | Weekly stddev of amount |
| psi_vs_baseline | NUMERIC | PSI vs first 60-day baseline |
| alert_flag | BOOLEAN | True when PSI > 0.025 |

Sample data:

```
monitoring_date | mean_value  | psi_vs_baseline | alert_flag
2025-06-03      | 907,483     | 0.0000          | false
2025-08-26      | 1,110,894   | 0.0267          | true
2025-09-02      | 1,190,816   | 0.0448          | true
2025-09-30      | 1,168,350   | 0.0442          | true
```

Total rows: 18 (weekly windows from Jun to Sep 2025).

### Table 2: feature_drift_alerts

Created when PSI exceeds threshold. One row per alerted week.

Schema:

| Column | Type | Description |
|--------|------|-------------|
| alert_date | DATE | Week end date |
| feature_name | VARCHAR | Monitored feature |
| psi_value | NUMERIC | PSI for this window |
| mean_before | NUMERIC | Baseline mean (VND) |
| mean_after | NUMERIC | Window mean (VND) |
| action | TEXT | Recommended action |

Sample data:

```
alert_date  | psi_value | mean_before | mean_after | action
2025-08-26  | 0.0267    | 907,483     | 1,110,894  | Amount drift detected (PSI=0.027): weekly mean shifted from 907,483 to 1,110,894. Verify Scenario B amount multiplier impact.
2025-09-02  | 0.0448    | 907,483     | 1,190,816  | Amount drift detected (PSI=0.045): weekly mean shifted from 907,483 to 1,190,816. Verify Scenario B amount multiplier impact.
```

Total alerts: **6** (Aug 26, Sep 2, 9, 16, 23, 30).

### Table 3: ml_fraud_label

Label-only table in Gold zone. One row per transaction.

Schema:

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | UUID | Primary key |
| customer_id | VARCHAR | For joining features |
| event_timestamp | TIMESTAMP | Transaction time — used for PIT join |
| label | SMALLINT | 1 = fraud, 0 = legitimate |
| created_ts | TIMESTAMP | Ingestion time |

Rules:
- `event_timestamp` is used for **point-in-time (PIT) join** with feature tables.
- `label` = `is_fraud` from `fact_transaction` (derived from ≥ 2 fraud conditions met).
- Total rows: **1,440,000** (all transactions, ~10% fraud rate → 144,000 fraud labels).

### Table 4: ml_fraud_training

Training table in Gold zone, created in two steps:

**Step 1 — PIT join** of `ml_fraud_label` against `feat_customer_90d` for batch features, plus transaction context from `fact_transaction` and `dim_card`.

**Step 2 — Per-transaction streaming features** computed directly from `stg_fraud_events` sliding windows (30 min / 1 h) at each transaction's actual timestamp. Processes date-by-date (loads 2 event partitions at a time) to avoid OOM on 33.5M events.

> Note: streaming features are computed from `stg_fraud_events` at each transaction's exact timestamp, not from the `feat_stream_30m` snapshot table. This gives point-in-time accurate signal rather than a daily snapshot average.

Schema:

| Column | Type | Source |
|--------|------|--------|
| transaction_id | UUID | ml_fraud_label |
| customer_id | VARCHAR | ml_fraud_label |
| event_timestamp | TIMESTAMP | ml_fraud_label |
| label | SMALLINT | ml_fraud_label |
| f_customer_total_txn_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_customer_avg_txn_amount_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_customer_distinct_merchants_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_customer_decline_rate_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_customer_foreign_txn_ratio_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_customer_night_txn_ratio_90d | NUMERIC | feat_customer_90d (PIT join) |
| f_stream_otp_failed_count_30m | NUMERIC | stg_fraud_events (sliding window) |
| f_stream_decline_count_30m | NUMERIC | stg_fraud_events (sliding window) |
| f_stream_txn_velocity_1h | NUMERIC | stg_fraud_events (sliding window) |
| f_stream_new_merchant_flag | SMALLINT | stg_fraud_events (sliding window) |
| f_stream_burst_activity_flag | SMALLINT | stg_fraud_events (sliding window) |
| feature_snapshot_ts | TIMESTAMP | feat_customer_90d snapshot time |
| txn_amount | NUMERIC | fact_transaction — raw amount for `txn_amount_ratio` |
| txn_hour | SMALLINT | EXTRACT(HOUR FROM event_timestamp) — for `is_night_txn` |
| is_declined_txn | SMALLINT | fact_transaction.is_declined — fraud condition |
| is_foreign_txn | SMALLINT | ip_country ≠ card_country — fraud condition |
| created_ts | TIMESTAMP | Pipeline ingestion time |

PIT join logic (Step 1):

```sql
LEFT JOIN LATERAL (
    SELECT * FROM gold_fraud.feat_customer_90d u
    WHERE u.customer_id = l.customer_id
      AND u.event_timestamp <= l.event_timestamp
    ORDER BY u.event_timestamp DESC LIMIT 1
) f ON TRUE
LEFT JOIN gold_fraud.fact_transaction t ON t.transaction_id = l.transaction_id
LEFT JOIN gold_fraud.dim_card dc ON dc.card_key = t.card_key
```

Total rows: **1,440,000** (same as ml_fraud_label, ~10% fraud → 144,000 positive labels).

---

## 8. Deliverables

1. **Data generator code** — `src/generator/offline/transactions.py` with Scenario B drift injection; `config/generate_config.yaml` with drift parameters.
2. **Drift validation report** — `data/drift_validation_report.csv` (18 weekly windows, 6 detected).
3. **Gold monitoring tables** — `agg_feature_health_daily` (18 rows), `feature_drift_alerts` (6 rows) populated with PSI values.
4. **Gold label table** — `ml_fraud_label` (1,440,000 rows) with `event_timestamp` and `label`.
5. **Gold training table** — `ml_fraud_training` (1,440,000 rows) created by PIT join of label + feature snapshot.
6. **Brief explanation** — Scenario B chosen: 30% gradual transaction amount drift from 2025-08-01. Feature `f_customer_avg_txn_amount_90d` increases ~30% by Sep 2025. PSI threshold calibrated to 0.025 for lognormal distribution (standard 0.15 is designed for credit scores). 6 alert weeks detected from Aug 26 – Sep 30.
