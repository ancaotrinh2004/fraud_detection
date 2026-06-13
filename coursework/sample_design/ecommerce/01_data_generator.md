# E-commerce Data Generator Sample Solution

## 1. Domain Overview

This project simulates a medium-size e-commerce platform. The generator produces:

- Offline historical/reference data (Parquet)
- Streaming real-time events (JSON)

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| customers | one per customer | customer_id, signup_ts, country, segment, marketing_opt_in |
| products | one per product | product_id, category, brand, base_price, is_active, created_ts |
| orders | one per order | order_id, customer_id, order_timestamp, status, shipping_city, shipping_method, coupon_code |
| order_items | one per line | order_item_id, order_id, product_id, quantity, unit_price, discount_amount |
| payments | one per attempt | payment_id, order_id, payment_timestamp, payment_method, amount, payment_status |

### 2.2 Offline Data Problems

**Compulsory:**
- **Skew**: 85% orders from HCMC, 80% products in electronics.
- **High cardinality**: customer_id, product_id, order_id are mostly unique.
- **Schema evolution**: old partitions (60% of timeline) missing coupon_code and shipping_method.

**Optional chosen:** 2% duplicate rate in order_items (same order_id, product_id, quantity, unit_price repeated).

**Output:** Parquet partitioned by order_date, payment_date.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka/streaming topic with `event_type` field.

Key columns:
- `event_id`, `event_type` (view|add_to_cart|checkout|purchase|payment_failed)
- `event_timestamp`, `created_ts` (event time vs row creation time)
- `customer_id`, `session_id`, `device_type`, `source` (app|web)
- `product_id` (nullable), `order_id` (nullable), `quantity` (nullable), `price` (nullable)

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursts**: 100 events/min baseline → 3000 events/min in 20-min windows at 12:00 and 20:00.
- **Late arrivals**: 12% of events have a later `created_ts` than `event_timestamp`.

**Optional chosen:** 1.5% duplicate events (same event_id, immediate or 1-3 minute delay).

**Output:** JSON or Avro.

---

## 4. Feature Engineering

Compute from customer transaction and event data:

**Offline (stable, 90-day windows):**
- `f_customer_total_orders_90d` - order count
- `f_customer_avg_order_value_90d` - average order value
- `f_customer_distinct_categories_90d` - category diversity
- `f_customer_payment_fail_rate_90d` - payment failure ratio

**Streaming (rolling windows):**
- `f_stream_views_30m` - product views
- `f_stream_add_to_cart_30m` - add-to-cart count
- `f_stream_cart_to_purchase_ratio_60m` - purchase conversion
- `f_stream_burst_activity_flag` - burst period traffic

Merge offline + streaming for unified feature table keyed by customer_id, refreshed every 15 minutes.

---

## 5. Generator Configuration

```yaml
n_customers: 120000
n_products: 45000
days_history: 180
skew_ratio_city: 0.85
skew_ratio_category: 0.80
duplicate_rate_offline: 0.02
schema_change_date: "2025-07-01"
base_events_per_min: 100
burst_multiplier: 30
burst_windows: ["12:00-12:20", "20:00-20:20"]
late_arrival_rate: 0.12
late_delay_min_max: [5, 45]
duplicate_rate_stream: 0.015
random_seed: 42
```

---

## 6. Deliverables

1. **Generator code** with configurable parameters.
2. **Data outputs**: Parquet (offline), JSON (streaming).
3. **Quality report**:
   - Skew distribution (city/category %)
   - Cardinality: approx_count_distinct by ID
   - Schema evolution: nulls in old partitions
   - Duplicate rate before/after dedup
   - Streaming burst/late/duplicate rates
4. **Write-up**: explain optional problem choice and feature design.

---

## 7. Implementation Tips

- Use deterministic seeds for reproducibility.
- Define dedup keys: order_id/product_id (offline), event_id + created_ts (streaming).