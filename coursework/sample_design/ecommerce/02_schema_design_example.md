# E-commerce Gold Zone Schema Design

## 1. Goal

Business-ready Gold model for analytics/BI, plus implemented data pipelines with lineage visibility.

**Approach:** Fact-Dimension + OBT + Optional aggregates/features.

**Coursework requirement:** design and implement data pipelines end-to-end, and capture lineage for key datasets (for example, using DataHub or an equivalent metadata/lineage tool).

**Storage requirement (cost-focused):** Bronze and Silver layers must be stored using lakehouse architecture/storage (for example, Delta Lake tables on object storage) to reduce storage and processing cost.

**Schema:** `gold_ecommerce` with naming: `dim_`, `fact_`, `obt_`, `feat_` prefix.

**Naming note:** for upstream layers, you can name Bronze tables with `raw_` prefix and Silver tables with `stg_` prefix.

**Input data profile (required before design)**
- Source from 01: list input datasets/events and key columns used for Bronze/Silver/Gold modeling.
- Data volume: estimated rows/day, historical backfill range, and expected table sizes.
- Data velocity: arrival/update frequency (batch interval or streaming rate).
- Data characteristics: key identifiers, timestamp columns, null/duplicate patterns, schema evolution risks.
- Known data issues from 01 generation: missing fields, duplicates, late-arriving records, or outliers.

Students must state this input profile first, then design schema and pipelines.

**Assumptions**
- Business objective: provide reliable, query-efficient Gold datasets for BI and downstream ML use.
- Decision usage: Gold tables and features are used for analytics/reporting and ML training/scoring support.
- Service level expectation: Gold and feature data must meet agreed freshness and reliability targets.
- Explainability expectation: out of scope for the current phase.
- Risk and governance expectation: out of scope for the current phase.

**SLA targets (example)**
- Gold table freshness: <= 30 minutes for incremental loads.
- Feature freshness: <= 5-60 minutes depending on feature type.
- Pipeline run success rate: >= 99% scheduled-run success per week.

Given coursework compute constraints, you may adjust SLA targets and report the final achieved values after implementation.

---

## 2. Dimension Tables

| Dimension | Grain | Key Columns |
|-----------|-------|------------|
| dim_customer | one per customer | customer_key (SK), customer_id (BK), signup_ts, country, segment, marketing_opt_in |
| dim_product | one per product | product_key (SK), product_id (BK), category, brand, base_price, is_active, created_ts |
| dim_date | one per date | date_key (yyyymmdd), calendar_date, day_of_week, month, year, is_weekend |
| dim_payment_method | one per method | payment_method_key (SK), payment_method (name) |
| dim_order_status | one per status | order_status_key (SK), order_status (name) |

**Notes:**
- Use SCD2 (valid_from_ts, valid_to_ts, is_current) if attributes change over time.
- SK = surrogate key (data warehouse-generated), BK = business key (natural identifier).

---

## 3. Fact Tables

### 3.1 fact_order
**Grain:** one per order. **Keys:** customer_key, order_date_key, order_status_key.  
**Measures:** order_gross_amount, order_discount_amount, order_net_amount, item_count.  
**Note:** Handles schema evolution (old orders missing coupon_code, shipping_method).

### 3.2 fact_order_item
**Grain:** one per line item. **Keys:** customer_key, product_key, order_date_key.  
**Measures:** quantity, unit_price, discount_amount, line_net_amount.  
**Note:** Apply deduplication before load (2% duplicate rate from source).

### 3.3 fact_payment_attempt
**Grain:** one per payment. **Keys:** customer_key, payment_date_key, payment_method_key.  
**Measures:** amount, is_payment_success (0/1), is_payment_failed (0/1).

---

## 4. OBT Table

### 4.1 obt_order_performance
**Grain:** one per order
**Purpose:** Denormalized table for BI queries.  
**Columns:** order_id, customer_id, order_timestamp, country, segment, total_quantity, order_net_amount, payment_status_last, shipping_city, coupon_code (+ needed fact/dimension columns).

---

## 5. Refresh & Data Quality

**Refresh SLAs:**
- Dimensions: daily (or real-time if attributes change)
- Facts: incremental append/merge every 15-30 minutes
- OBT: merge by order_id every 15-30 minutes

*Note:* SLA (Service Level Agreement) is basically an agreed target for service quality, such as freshness, latency, availability, and reliability.

**Quality checks:**
- Uniqueness: order_id, order_item_id, payment_id per fact table
- Referential: facts link to dimensions
- Total match check: sum(line_net_amount) should be close to order_net_amount
- Duplicate check: monitor order_items before and after dedup
- Null check: required keys/measures should stay filled

---

## 6. Feature Store

Keep ML features in Gold:

Each feature row should include `event_timestamp` for point-in-time joins and `created_ts` for dedup.

**Feature tables:**
1. `feat_customer_90d` (grain: customer_id, event_timestamp)
   - f_customer_total_orders_90d, f_customer_avg_order_value_90d, f_customer_distinct_categories_90d
2. `feat_stream_60m` (grain: customer_id, event_timestamp)
   - f_stream_views_30m, f_stream_add_to_cart_30m, f_stream_cart_to_purchase_ratio_60m
3. `feat_customer_unified` (grain: customer_id, event_timestamp)
   - Join offline + streaming for training/scoring

**Point-in-time correctness:** Do not use feature data later than the label/reference timestamp.

**Dedup note:** use `created_ts` to keep the latest row when multiple rows share the same entity key and `event_timestamp`.

**Refresh:** 15-60 min (feat_90d), 1-5 min (feat_stream), 5-15 min (unified).

---

## 7. Data Pipeline Design and Implementation Scope

**Requirement (for students):** In 02, you must both design and implement all data pipelines, including feature pipelines.

Pipeline groups to cover:
1. Bronze ingestion pipelines
   - load raw source events/tables into Bronze with schema checks and ingest metadata
   - store Bronze tables in lakehouse storage format
2. Silver transformation pipelines
   - clean, deduplicate, and standardize records for downstream modeling
   - store Silver tables in lakehouse storage format
3. Gold modeling pipelines
   - build/update dimensions, facts, and OBT tables in `gold_ecommerce`
4. Feature pipelines (required)
   - build/update `feat_customer_90d`, `feat_stream_60m`, and `feat_customer_unified`
   - publish freshness and data-quality checks for feature tables

### 7.1 Pipeline SLA Targets (example)

- Bronze ingest freshness: <= 10 minutes from source arrival.
- Silver table freshness: <= 30 minutes.
- Gold fact/OBT freshness: <= 30 minutes.
- Feature freshness:
   - `feat_customer_90d`: <= 60 minutes
   - `feat_stream_60m`: <= 5 minutes
   - `feat_customer_unified`: <= 15 minutes
- Pipeline availability target: >= 99% successful scheduled runs per week.

Given coursework compute constraints, students may tune SLA values, but must report the final achieved targets.

### 7.2 Pipeline Update Strategy (required)

- Bronze: append-only ingestion with ingest metadata (`ingest_ts`, `source_offset`/`batch_id`).
- Silver: incremental processing with deduplication by business key + event time.
- Gold dimensions/facts/OBT: incremental merge/upsert using stable keys.
- Feature tables: incremental recomputation by rolling window + merge by (`customer_id`, `event_timestamp`) with latest `created_ts` retained.
- Backfill policy: for coursework, use no backfill by default; if needed, limit re-runs to at most the last 1 day with idempotent writes.
- Late-arriving data policy: reprocess affected windows and reconcile downstream tables.

### 7.3 Pipeline Controls and Monitoring (required)

- Quality gates per run: schema checks, uniqueness checks, null checks, and referential checks.
- Freshness checks: alert when SLA thresholds are exceeded.
- Volume checks: alert on abnormal drops/spikes versus baseline.
- Run metadata: store run_id, start/end time, status, input/output row counts, and error summary.
- Recovery controls: retry with backoff, dead-letter/quarantine for bad records, and rerun procedure.
- Lineage tracking: publish dataset and job lineage for Bronze -> Silver -> Gold -> Feature tables (for example via DataHub).
- Lineage evidence: include at least one lineage view/screenshot or exported lineage summary for core tables.

## 8. Warehouse Optimization

Students must state what warehouse optimizations were applied and why.

- Storage/layout: partitioning strategy and clustering/sorting strategy for large tables.
- Access path optimization: indexing (or warehouse equivalent) for common filters/joins.
- Query optimization: materialized views/summary tables where justified.

**NOTE (for students): Suggested write-up format**
- Workload: which query/job was slow (for example, daily BI dashboard query on `obt_order_performance`).
- Bottleneck: what caused the issue (for example, full table scan, expensive join, skewed partition).
- Optimization applied: what you changed (for example, index on `order_timestamp`, partition by `order_date_key`, clustering by `customer_id`).
- Result: before/after metrics (runtime, scanned bytes, cost, or resource usage).
- Trade-off: one downside or maintenance cost of the optimization.

**Example (brief):**
- Workload: daily revenue dashboard query by date range and country.
- Optimization: partition `fact_order` by `order_date_key` and add index on `country`.
- Result: runtime improved from 42s to 11s; scanned data reduced by ~68%.
- Trade-off: slightly higher write cost during incremental loads.

**Scope boundary with 04:** 04 reuses these implemented data pipelines and covers CI/CD for ML pipelines and inference services.

---

## 9. Deliverables

**Submission format (required):**
- Submit one Markdown file (`.md`) as your final 02 design + implementation document.
- The file should follow the same section structure and content coverage as this example file.

1. Goal setup: define the Gold-zone objective, modeling approach, naming conventions, required input data profile from 01 (volume, velocity, key attributes, and known data issues), plus assumptions and SLA targets before design.
2. Dimension design: define grain, keys, and SCD strategy for dimension tables.
3. Fact design: define grain, keys, measures, and handling for schema evolution/dedup.
4. OBT design: define purpose, grain, and core denormalized columns.
5. Refresh and data quality plan: define freshness SLAs and required validation checks.
6. Feature store design: define feature tables, point-in-time correctness, dedup policy, and refresh targets.
7. Data pipeline plan: design and implement Bronze, Silver, Gold, and feature pipelines, including lakehouse storage for Bronze/Silver, schedules/dependencies, SLA targets, update strategy (incremental/merge, limited backfill up to 1 day if needed, late-data handling), operational controls (monitoring/alerting, run metadata, retry/recovery, rerun procedures), and lineage tracking (for example DataHub).
8. Warehouse optimization plan: document indexing/partitioning/clustering (or warehouse equivalents), maintenance operations, measured impact, and follow the Section 8 write-up format.

