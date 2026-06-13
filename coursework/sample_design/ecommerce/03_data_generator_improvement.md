# E-commerce Data Generator Improvement: Feature Drift & Labels

## 1. Objective

Extend the data generator from 01_data_generator.md with **feature drift** simulation.

Goals:

- Simulate how feature distributions change over time.
- Test feature store monitoring and drift detection in Gold layer.
- Create a Gold label table for ML training.
- Join label + feature tables to produce the training table used in 04_ml_design.md.
- Help students understand covariate vs concept drift.

---

## 2. What is Feature Drift?

Feature drift: computed feature distributions change over time.

Example: `f_customer_total_orders_90d` baseline mean = 3.2 orders/customer, after campaign mean = 4.8.

---

## 3. Drift Scenarios: Pick At Least One

**You must choose at least one scenario to inject into your generator.**

### Scenario A: Simple - Customer Order Frequency Drift ⭐ (Recommended)

What changes:
- Order frequency increases from 3.2 to 4.8 orders/customer/90d (campaign effect).

How to inject:
- Increase order creation rate by 50% after `drift_start_date`.

Feature affected:
- `f_customer_total_orders_90d` will increase.

Gold monitoring:
- Track daily mean of `f_customer_total_orders_90d`.
- Alert when PSI (Population Stability Index) > 0.1.

### Scenario B: Simple - Average Order Value Drift

What changes:
- AOV increases from $125 to $142 (13% increase from upsell).

How to inject:
- Multiply order amounts by 1.13 for orders after `drift_start_date`.

Feature affected:
- `f_customer_avg_order_value_90d` will increase.

Gold monitoring:
- Track mean and distribution change via PSI or KL divergence.

### Scenario C: Moderate - Streaming Conversion Rate Drift

What changes:
- Cart-to-purchase conversion rate: 28% → 42% (discount period).

How to inject:
- Increase probability of purchase event following add-to-cart after `drift_start_date`.

Feature affected:
- `f_stream_cart_to_purchase_ratio_60m` will increase.

Gold monitoring:
- Track if feature-to-label correlation drops (concept drift).

### Scenario D: Moderate - Category Diversity Drift

What changes:
- Customers buy from 2.3 → 3.1 distinct product categories (new products).

How to inject:
- Shift category mix: electronics 80% → 45%, fashion 10% → 35%, add home 20%.

Feature affected:
- `f_customer_distinct_categories_90d` will increase.

Gold monitoring:
- Track distribution of distinct categories per customer over time.

---

## 4. Drift Configuration Parameters

Add to your generator config:

```yaml
drift_enabled: true
drift_start_date: "2025-09-01"
drift_mode: "gradual"  # or "abrupt"

# Choose one scenario (set to true)
scenario_A_order_frequency: true
scenario_B_aov: false
scenario_C_conversion: false
scenario_D_category_diversity: false

# Scenario A parameters
order_frequency_multiplier: 1.50  # 50% increase after drift_start_date
```

---

## 5. Example Output File

File: `drift_validation_report.csv`

```
date         | feature_name                     | mean | stddev | psi_vs_baseline | drift_status
2025-08-31   | f_customer_total_orders_90d      | 3.2  | 1.1    | 0.00            | baseline
2025-09-10   | f_customer_total_orders_90d      | 3.8  | 1.2    | 0.05            | detected
2025-09-20   | f_customer_total_orders_90d      | 4.2  | 1.3    | 0.12            | strong
2025-10-01   | f_customer_total_orders_90d      | 4.6  | 1.4    | 0.18            | alert
```

---

## 6. Gold Layer Monitoring Tables

Minimal required Gold tables:

### Table 1: agg_feature_health_daily

```
monitoring_date | feature_name            | mean_value | psi_vs_baseline | alert_flag
2025-09-01      | f_customer_orders_90d   | 3.2        | 0.00            | false
2025-09-10      | f_customer_orders_90d   | 3.8        | 0.05            | false
2025-09-20      | f_customer_orders_90d   | 4.2        | 0.12            | false
2025-10-01      | f_customer_orders_90d   | 4.6        | 0.18            | true
```

Trigger alert when PSI > 0.15.

### Table 2: feature_drift_alerts

```
alert_date | feature_name           | psi_value | action
2025-10-01 | f_customer_orders_90d  | 0.18      | Investigate campaign impact on order frequency
```

### Table 3: ml_customer_label

Label-only table (no features) in Gold zone:

```
customer_id | event_timestamp       | created_ts           | label
C001        | 2025-09-10 10:00:00   | 2025-09-10 10:05:00  | 1
C002        | 2025-09-10 10:00:00   | 2025-09-10 10:06:00  | 0
```

Rules:
- `event_timestamp` is used for point-in-time join with feature tables.
- `created_ts` is used for dedup (keep latest row).
- `label` = `will_purchase_next_session` (1 or 0).

### Table 4: ml_customer_purchase_training

Training table in Gold zone, created by joining:
- `ml_customer_label`
- Gold feature tables from 02_schema_design.md

Output shape:

```
customer_id | event_timestamp       | created_ts           | label | f_customer_total_orders_90d | ...
```

---

## 7. Deliverables

1. **Data generator code** with drift enabled (choose one scenario).
2. **Drift validation report** (CSV with PSI values).
3. **Gold monitoring tables** (`agg_feature_health_daily`, `feature_drift_alerts`) populated.
4. **Gold label table** (`ml_customer_label`) populated with `event_timestamp`, `created_ts`, `label`.
5. **Gold training table** (`ml_customer_purchase_training`) created by label + feature join.
6. **Brief explanation**: which scenario, why, expected feature change.

Note: the label does not have to be perfectly correct for this coursework. If your label logic is correct and consistent, it is highly appreciated.