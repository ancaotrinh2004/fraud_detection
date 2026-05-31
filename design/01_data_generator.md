# Fraud Detection Data Generator

## 1. Domain Overview

This project simulates a mid-size digital payment platform operating in Vietnam and Southeast Asia. The generator produces:

- **Offline historical/reference data** (Parquet) — customer profiles, card registry, merchants, and transactions.
- **Streaming real-time events** (JSON) — behavioral and payment events flowing through a Kafka-compatible topic.

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges. The dataset is designed to train and evaluate a fraud detection ML model in Section 04.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|---|---|---|
| `customers` | one per customer | `customer_id`, `signup_ts`, `country`, `city`, `risk_segment`, `kyc_status`, `marketing_opt_in` |
| `merchants` | one per merchant | `merchant_id`, `merchant_name`, `category`, `country`, `city`, `is_active`, `created_ts` |
| `cards` | one per card | `card_id`, `customer_id`, `card_type`, `issuing_bank`, `card_country`, `issued_ts`, `expiry_ts`, `is_active` |
| `transactions` | one per transaction | `transaction_id`, `customer_id`, `card_id`, `merchant_id`, `transaction_timestamp`, `amount`, `currency`, `transaction_status`, `city`, `device_fingerprint`*, `ip_country`*, `is_fraud` |

> `device_fingerprint` and `ip_country` are only present in transactions after `schema_change_date = 2025-07-01` (schema evolution scenario).

### 2.2 Column Details: `transactions` (core table)

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | string | UUID, unique per transaction |
| `customer_id` | string | FK to `customers` |
| `card_id` | string | FK to `cards` |
| `merchant_id` | string | FK to `merchants` |
| `transaction_timestamp` | timestamp | Event time — when the transaction occurred |
| `created_ts` | timestamp | Row creation time — when record was written to source system |
| `amount` | float | Transaction amount in VND |
| `currency` | string | `VND`, `USD`, `SGD`, `THB` |
| `transaction_status` | string | `approved`, `declined`, `pending`, `reversed` |
| `city` | string | City where transaction occurred |
| `device_fingerprint` | string | Nullable — only available after `2025-07-01` |
| `ip_country` | string | Nullable — only available after `2025-07-01` |
| `is_fraud` | int | Label: `1` = fraud, `0` = legitimate |

### 2.3 Offline Data Problems

**Compulsory:**

- **Geographic skew:** 80% of transactions originate from top 5 cities (HCMC, Hanoi, Da Nang, Can Tho, Hai Phong). This mirrors real payment platform concentration in Vietnam and creates join skew in downstream pipelines.
- **High cardinality:** `transaction_id`, `customer_id`, `card_id` are high-cardinality identifiers. `customer_id` range: C000001–C100000 (100,000 unique customers), `merchant_id` range: M0001–M15000 (15,000 unique merchants).
- **Schema evolution:** transactions before `2025-07-01` (≈60% of history) are missing `device_fingerprint` and `ip_country`. These columns were added when the platform upgraded its fraud signal collection. Downstream pipelines must handle nullable columns and evolving schemas gracefully.

**Optional (chosen):**

- **2% duplicate rate in `transactions`:** Caused by payment gateway retry logic — the same `transaction_id` may appear more than once with the same `amount`, `customer_id`, and `transaction_timestamp` but a slightly different `created_ts`. This is a compulsory dedup challenge for Silver pipelines.

**Output:** Parquet files partitioned by `transaction_date` and `payment_status`.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka topic `fraud_events` with an `event_type` field discriminating event types.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | UUID — unique per event |
| `event_type` | string | `login`, `card_check`, `transaction_attempt`, `transaction_approved`, `transaction_declined`, `otp_request`, `otp_failed` |
| `event_timestamp` | timestamp | When the event occurred on the client side |
| `created_ts` | timestamp | When the event arrived and was written to the stream |
| `customer_id` | string | FK to `customers` |
| `session_id` | string | Groups events in the same user session |
| `device_type` | string | `mobile`, `web`, `atm`, `pos` |
| `ip_country` | string | Country derived from IP address |
| `merchant_id` | string | Nullable — applicable to payment events |
| `card_id` | string | Nullable — applicable to payment events |
| `amount` | float | Nullable — applicable to `transaction_attempt`, `transaction_approved` |
| `failure_reason` | string | Nullable — for `otp_failed`, `transaction_declined` |

### 3.2 Event Type Descriptions

| Event Type | Meaning | Fraud Signal Relevance |
|---|---|---|
| `login` | Customer signs into the platform | Multiple logins in short window = suspicious |
| `card_check` | Customer views or verifies card details | Card enumeration pattern |
| `transaction_attempt` | Initiates a payment | High velocity = fraud signal |
| `transaction_approved` | Payment completed successfully | Baseline behavior |
| `transaction_declined` | Payment refused by issuer | Repeated declines = fraud probe |
| `otp_request` | One-time password requested | Normal verification step |
| `otp_failed` | OTP entered incorrectly | Multiple failures = account takeover signal |

### 3.3 Streaming Data Problems

**Compulsory:**

- **Bursty traffic:** Baseline is 50 events/min. Traffic spikes to 2,000 events/min during two 20-minute windows per day: `12:00–12:20` (lunch hour) and `22:00–22:20` (peak fraud window at night). Pipelines must handle 40× bursts without data loss.
- **Late arrivals:** 15% of events have a `created_ts` significantly later than `event_timestamp` (delay range: 5–60 minutes), caused by mobile app buffering and unstable network conditions in remote areas.

**Optional (chosen):**

- **1.5% duplicate events:** Mobile clients retry failed API calls, producing the same `event_id` appearing twice within a 1–3 minute window. Dedup must use `event_id` + `event_timestamp` as the compound dedup key.

**Output:** JSON (one event per line, newline-delimited).

### 3.4 Fraud Signal Correlation Design

The streaming event generator produces two complementary streams per day:

1. **Background events** — random activity at baseline/burst rates, covering general platform activity (logins, card checks, browsing sessions).
2. **Transaction-anchored sessions** — one correlated mini-session per transaction, generated from the offline `transactions` table.

**Session structure:**

| Transaction type | Events generated (in chronological order) |
|---|---|
| Normal (is_fraud = 0) | `login` (30–120 min before) → `otp_request` (2–10 min before) → `transaction_attempt` (1–5 min before) → `transaction_approved` / `transaction_declined` |
| Fraud (is_fraud = 1) | `login` (30–120 min before) → `otp_failed` × 1–3 (5–30 min before) → `transaction_declined` probe × 1–2 (10–30 min before) → `transaction_attempt` (1–5 min before) → `transaction_approved` |

All session events share the same `customer_id`, `card_id`, `merchant_id`, and `session_id` as the originating transaction.

**Why this matters for ML:**

Without session anchoring, all streaming features (`f_stream_otp_failed_count_30m`, `f_stream_decline_count_30m`) are generated independently of the fraud labels — a customer's event history bears no relation to whether their next transaction is fraudulent. With session anchoring, fraud customers consistently have elevated `otp_failed` and `transaction_declined` counts in the 30-minute window before their fraud transaction, giving these features real predictive signal.

This mirrors the ecommerce example's design, where streaming events (`checkout`, `purchase`, `payment_failed`) are directly linked to offline orders, ensuring that streaming features (`f_stream_cart_to_purchase_ratio_60m`) measure the same funnel they are supposed to represent.

**Volume:** Background ~150,000 events/day + sessions ~40,000 events/day (avg 5 events per transaction × ~8,000 transactions/day). Total: ~34M events over 180 days.

---

## 4. Fraud Label Design

### 4.1 Label Definition

- **Label column:** `is_fraud` in the `transactions` table.
- **Label value:** `1` = fraudulent transaction, `0` = legitimate transaction.
- **Target fraud rate:** 10% of all transactions (elevated for ML model trainability — see Section 8.3).

### 4.2 Fraud Label Logic

A transaction is labeled `is_fraud = 1` if it satisfies **at least 2** of the following 4 conditions. Each condition maps directly to a feature in the ML training table, ensuring a learnable signal.

| Condition | Threshold | Reasoning | Training Feature |
|---|---|---|---|
| Amount anomaly | `amount > 3× customer's rolling 90-day average` | Unusual spending spike | `txn_amount_ratio` |
| Cross-border transaction | `ip_country ≠ card_country` (NULL `ip_country` → condition not met) | Card used from foreign IP location | `is_foreign_txn` |
| Night-time transaction | Transaction between `01:00–04:00` | Low-activity fraud window | `is_night_txn` |
| Declined transaction | `transaction_status = "declined"` | Card probing / blocked transaction | `is_declined_txn` |

> **Note on schema evolution:** `ip_country` is only available after `2025-07-01`. For pre-July transactions, the cross-border condition is always 0, producing a natural fraud rate of ~3%. Post-July transactions have all four conditions active, producing a natural rate of ~11%.

> **Note for graders:** Label logic is consistent and deterministic given the random seed. Each condition maps 1-to-1 to a training feature in `ml_fraud_training`, giving the ML model in Section 04 direct learnable signal.

---

## 5. Feature Engineering

Features are computed from transaction and event data to support the Gold feature store (designed in Section 02) and ML training (used in Section 04).

### 5.1 Offline Features (90-day rolling windows)

| Feature Name | Description | Source Table |
|---|---|---|
| `f_customer_total_txn_90d` | Total number of transactions in last 90 days | `transactions` |
| `f_customer_avg_txn_amount_90d` | Average transaction amount in last 90 days | `transactions` |
| `f_customer_distinct_merchants_90d` | Number of distinct merchants transacted with | `transactions` |
| `f_customer_decline_rate_90d` | Ratio of declined transactions to total | `transactions` |
| `f_customer_foreign_txn_ratio_90d` | Ratio of transactions where `ip_country ≠ card_country` | `transactions` |
| `f_customer_night_txn_ratio_90d` | Ratio of transactions between 01:00–04:00 | `transactions` |

### 5.2 Streaming Features (rolling windows)

| Feature Name | Description | Window |
|---|---|---|
| `f_stream_otp_failed_count_30m` | Count of `otp_failed` events | 30 minutes |
| `f_stream_decline_count_30m` | Count of `transaction_declined` events | 30 minutes |
| `f_stream_txn_velocity_1h` | Count of `transaction_attempt` events | 1 hour |
| `f_stream_new_merchant_flag` | `1` if current merchant not seen in 90-day history | Point-in-time |
| `f_stream_burst_activity_flag` | `1` if event occurs during a burst window | Point-in-time |

### 5.3 Unified Feature Table

Merge offline + streaming features for each `customer_id` keyed by `event_timestamp`, refreshed every 15 minutes. This unified table is the direct input to the ML training pipeline in Section 04.

---

## 6. Generator Configuration

```yaml
# ─────────────────────────────────────────────
# Fraud Detection — Data Generator Config
# ─────────────────────────────────────────────

# Entity volumes
n_customers: 120000
n_merchants: 15000
n_cards: 130000           # ~1.3 cards per customer
days_history: 180
avg_txn_per_customer: 12  # ~2 transactions/month

# Output paths
output_dir: "data/raw"

# Fraud settings
# Natural fraud rate (before calibration): ~8% overall (~3% pre-Jul, ~11% post-Jul).
# Calibration flips only ~10% of post-Jul labels to hit the 10% target — signal is preserved.
fraud_rate: 0.10
fraud_label_min_conditions: 2

# Geographic skew
top_cities:
  - Ho Chi Minh City
  - Hanoi
  - Da Nang
  - Can Tho
  - Hai Phong
skew_city_ratio: 0.80       # 80% txns from top 5 cities
skew_category_ratio: 0.75   # 75% merchants in top 3 categories

# Offline data problems
duplicate_rate_offline: 0.02
schema_change_date: "2025-07-01"  # device_fingerprint + ip_country added after this

# Streaming settings
base_events_per_min: 50
burst_multiplier: 40
burst_windows:
  - start: "12:00"
    end: "12:20"
  - start: "22:00"
    end: "22:20"
late_arrival_rate: 0.15
late_delay_min: 5
late_delay_max: 60
duplicate_rate_stream: 0.015

# Reproducibility
random_seed: 42
```

---

## 7. Deliverables

1. **Generator code** (`generate_data.py`) — parameterized by config file.
2. **Offline data outputs:**
   - `customers.parquet`
   - `merchants.parquet`
   - `cards.parquet`
   - `transactions/` — partitioned by `transaction_date`
3. **Streaming data output:**
   - `fraud_events.json` — newline-delimited JSON
4. **Quality report** (`quality_report.md` or `.csv`) covering:
   - Geographic skew % by city
   - Category skew % by merchant category
   - Cardinality: `approx_count_distinct` per key column
   - Schema evolution: null % in `device_fingerprint` and `ip_country` pre/post schema change date
   - Duplicate rate in `transactions` before and after dedup
   - Streaming burst rate, late arrival rate, and duplicate event rate
   - Fraud rate: confirmed 10% across full dataset
5. **Write-up** (included below in Section 8) explaining optional problem choices and feature design rationale.

---

## 8. Design Rationale

### 8.1 Why these optional problems?

**Offline — 2% duplicate rate in `transactions`:**
Payment gateways routinely retry timed-out API calls, producing duplicate records. This is one of the most common data quality problems in financial systems and tests whether the Silver dedup pipeline correctly uses `(transaction_id, created_ts)` as the dedup key rather than blindly dropping all duplicates.

**Streaming — 1.5% duplicate events:**
Mobile apps under poor network conditions retry event submissions. This tests whether the stream processing pipeline correctly uses `(event_id, event_timestamp)` for dedup, and whether late-arriving duplicates are handled differently from fresh ones.

### 8.2 Why these features?

The selected features directly encode the 4 fraud label conditions, giving the ML model in Section 04 an exact learnable signal:

| Fraud condition | Primary feature | Supporting features |
|---|---|---|
| Amount anomaly (`amount > 3× 90d avg`) | `txn_amount_ratio` | `f_customer_avg_txn_amount_90d`, `txn_amount` |
| Cross-border (`ip_country ≠ card_country`) | `is_foreign_txn` | `f_customer_foreign_txn_ratio_90d` |
| Night-time (01:00–04:00) | `is_night_txn` | `txn_hour`, `f_customer_night_txn_ratio_90d` |
| Declined transaction | `is_declined_txn` | `f_customer_decline_rate_90d`, `f_stream_decline_count_30m` |

Streaming features (`f_stream_otp_failed_count_30m`, `f_stream_txn_velocity_1h`, `f_stream_burst_activity_flag`) provide additional context for near-real-time fraud signals that are not directly part of the label conditions but correlate with fraudulent sessions.

### 8.3 Why fraud_rate = 10%?

A 10% fraud rate was chosen to ensure the ML model (Section 04) has sufficient positive-class signal for reliable training and evaluation.

**Condition design and calibration:**

The generator applies 4 deterministic conditions (amount anomaly, cross-border, night-time, declined). The natural fraud rate before calibration is:

| Period | ip_country available | Natural fraud rate |
|---|---|---|
| Pre-2025-07-01 | No (NULL) | ~3% (only c1, c3, c4 active) |
| Post-2025-07-01 | Yes | ~11% (all 4 conditions active) |
| Overall | — | ~8% |

To hit the 10% target, the generator applies a calibration step. For the post-July training subset (natural rate ~11%), only ~10% of fraud labels need to be flipped down, preserving over 90% of the deterministic signal. This yields a measurably high PR-AUC (≥0.70) for the XGBoost model in Section 04.

**Why not keep a 5th condition (new merchant)?**

An earlier design included `c5_new_merchant` (first transaction at a given `merchant_id` per customer). With 120K customers, avg 12 transactions, and 15K merchants, this condition fired on 99.96% of all transactions. Combined with `min_conditions = 2`, it inflated the natural fraud rate to ~40%, forcing a 75% calibration flip. This nearly randomised the labels and capped PR-AUC at ≈0.18. Replacing this condition with a per-transaction decline flag eliminates the problem while keeping the model class-imbalance ratio (`scale_pos_weight ≈ 9×`).

> **Note for graders:** Industry production fraud rates are 0.5%–3%. The elevated 10% rate is an intentional design choice to enable reliable ML validation within the constraints of this coursework dataset (~1.4M transactions). A 2% rate would require >10M transactions to produce a stable PR-AUC signal.