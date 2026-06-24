"""
src/pipelines/governance/suites.py

GE-native data contracts for the Gold layer — the single source of truth.

Each table is described declaratively (column type / nullable / description /
checks). `build_suite()` compiles a table spec into a real Great Expectations
`ExpectationSuite` of `gxe.*` expectation objects; the descriptions are exposed
separately (no GE import needed) so the DataHub emitter can reuse them.

Severity model (carried in each expectation's meta):
  • critical → schema mismatch / not_null / unique  → fails the pipeline
  • warn     → accepted_values / value_between       → logged, non-blocking

GE itself is imported lazily inside build_suite() so importing this module
during Airflow DAG parsing stays cheap.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str
    nullable: bool = True
    unique: bool = False
    accepted_values: list | None = None
    value_between: tuple[float, float] | None = None

    @property
    def not_null(self) -> bool:
        # A non-nullable column is implicitly a not_null contract.
        return not self.nullable


@dataclass(frozen=True)
class TableSpec:
    table: str
    schema: str
    description: str
    columns: list[Column] = field(default_factory=list)

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


# ── Gold-layer contracts ────────────────────────────────────────────────────────

_AMOUNT_MAX = 2_000_000_000_000  # 2e12 VND ceiling (covers large foreign-currency amount_base = amount × fx)

TABLE_SPECS: dict[str, TableSpec] = {
    "fact_transaction": TableSpec(
        table="fact_transaction", schema="gold_fraud",
        description=(
            "One row per deduplicated transaction attempt (approved, declined, "
            "pending, reversed). Grain: transaction_id. Partitioned by transaction_date_key. "
            "Immutable facts only — the fraud label lives in ml_fraud_label, not here."
        ),
        columns=[
            Column("transaction_key", "bigint", "Surrogate identity key — auto-increment", nullable=False, unique=True),
            Column("transaction_id", "varchar", "Natural/degenerate key — deduplicated from stg_transactions", nullable=False, unique=True),
            Column("customer_key", "bigint", "FK → dim_customer", nullable=False),
            Column("card_key", "bigint", "FK → dim_card", nullable=False),
            Column("merchant_key", "bigint", "FK → dim_merchant", nullable=False),
            Column("transaction_date_key", "int", "FK → dim_date (YYYYMMDD format)", nullable=False),
            Column("transaction_ts", "timestamp", "Event time of the transaction", nullable=False),
            Column("amount", "numeric", "Transaction amount in original currency", nullable=False, value_between=(0, _AMOUNT_MAX)),
            Column("currency", "varchar", "ISO currency code (e.g. VND, USD)", nullable=True),
            Column("amount_base", "numeric", "Amount normalised to base currency (VND) = amount * fx_rate", nullable=False, value_between=(0, _AMOUNT_MAX)),
            Column("fx_rate", "numeric", "FX rate to base currency at load time", nullable=True),
            Column("fx_rate_ts", "timestamp", "Timestamp of the FX rate used", nullable=True),
            Column("device_fingerprint", "varchar", "Device fingerprint — null before 2025-07-01 (schema evolution)", nullable=True),
            Column("ip_country", "varchar", "Country from IP — null before 2025-07-01", nullable=True),
        ],
    ),
    "dim_customer": TableSpec(
        table="dim_customer", schema="gold_fraud",
        description=(
            "SCD2 customer dimension. One row per customer version. risk_segment and "
            "kyc_status can change over time — historical values tracked."
        ),
        columns=[
            Column("customer_key", "bigint", "Surrogate key — auto-increment", nullable=False, unique=True),
            Column("customer_id", "varchar", "Business key from source customers.parquet", nullable=False),
            Column("risk_segment", "varchar", "Customer risk tier: low / medium / high", nullable=False, accepted_values=["low", "medium", "high"]),
            Column("kyc_status", "varchar", "KYC verification status: verified / pending / rejected", nullable=False, accepted_values=["verified", "pending", "rejected"]),
            Column("country", "varchar", "Customer's home country", nullable=False),
            Column("city", "varchar", "Customer's city", nullable=True),
            Column("valid_from_ts", "timestamp", "SCD2 effective start timestamp", nullable=False),
            Column("valid_to_ts", "timestamp", "SCD2 effective end timestamp — NULL means currently active", nullable=True),
            Column("is_current", "boolean", "True for the active (latest) version of each customer", nullable=False, accepted_values=[True, False]),
        ],
    ),
    "dim_card": TableSpec(
        table="dim_card", schema="gold_fraud",
        description="SCD1 card dimension. One row per card — attributes stable, is_active updated in-place.",
        columns=[
            Column("card_key", "bigint", "Surrogate key", nullable=False, unique=True),
            Column("card_id", "varchar", "Business key from source cards.parquet", nullable=False, unique=True),
            Column("customer_id", "varchar", "Owner customer business key", nullable=False),
            Column("card_type", "varchar", "Card funding type: credit / debit / prepaid", nullable=False, accepted_values=["credit", "debit", "prepaid"]),
            Column("card_country", "varchar", "Country where card was issued", nullable=False),
            Column("is_active", "boolean", "False if card is expired or cancelled", nullable=False),
            Column("expiry_ts", "timestamp", "Card expiry date", nullable=True),
        ],
    ),
    "dim_currency_rate": TableSpec(
        table="dim_currency_rate", schema="gold_fraud",
        description="FX reference used to normalise transaction amount → amount_base (VND).",
        columns=[
            Column("currency", "varchar", "ISO currency code (PK)", nullable=False, unique=True),
            Column("rate_to_base", "numeric", "Multiplier: amount * rate_to_base = amount in base currency", nullable=False, value_between=(0, 1_000_000)),
            Column("base_currency", "varchar", "Base currency code (VND)", nullable=False, accepted_values=["VND"]),
            Column("rate_ts", "timestamp", "When this rate was recorded", nullable=False),
        ],
    ),
    "obt_transaction_fraud_summary": TableSpec(
        table="obt_transaction_fraud_summary", schema="gold_fraud",
        description=(
            "Denormalized OBT for fraud analyst BI dashboards. One row per transaction — "
            "avoids multi-table joins at query time. Derived flags: is_cross_border, "
            "is_night_transaction, is_high_value."
        ),
        columns=[
            Column("transaction_id", "varchar", "Business key — matches fact_transaction.transaction_id", nullable=False, unique=True),
            Column("transaction_ts", "timestamp", "Event time of the transaction", nullable=False),
            Column("customer_id", "varchar", "Customer business key", nullable=False),
            Column("amount", "numeric", "Transaction amount in original currency", nullable=False, value_between=(0, _AMOUNT_MAX)),
            Column("amount_base", "numeric", "Amount normalised to base currency (VND)", nullable=False, value_between=(0, _AMOUNT_MAX)),
            Column("fraud_label", "smallint", "Current best fraud label from ml_fraud_label — NULL if not yet labelled", nullable=True, accepted_values=[0, 1]),
            Column("label_status", "varchar", "Label maturity: matured / confirmed / suspected", nullable=True, accepted_values=["matured", "confirmed", "suspected"]),
            Column("is_cross_border", "smallint", "1 if ip_country ≠ card_country (case-insensitive)", nullable=False, accepted_values=[0, 1]),
            Column("is_night_transaction", "smallint", "1 if transaction hour between 01:00–04:00", nullable=False, accepted_values=[0, 1]),
            Column("is_high_value", "smallint", "1 if amount_base > 5,000,000 VND", nullable=False, accepted_values=[0, 1]),
            Column("transaction_status", "varchar", "Original transaction status from source", nullable=False, accepted_values=["approved", "declined", "pending", "reversed"]),
            Column("transaction_date_key", "int", "FK → dim_date (YYYYMMDD)", nullable=False),
        ],
    ),
    "feat_customer_unified": TableSpec(
        table="feat_customer_unified", schema="gold_fraud",
        description=(
            "Merged offline (90-day) + streaming (30-min) feature snapshot per customer. "
            "Grain: (customer_id, event_ts). Direct input to ML training and scoring. "
            "Point-in-time correct: only uses feature data where event_ts ≤ label timestamp."
        ),
        columns=[
            Column("customer_id", "varchar", "FK → dim_customer.customer_id", nullable=False),
            Column("event_ts", "timestamp", "Snapshot time — used as the PIT join key", nullable=False),
            Column("f_customer_total_txn_90d", "int", "Total transactions by this customer in the past 90 days", nullable=True, value_between=(0, 100_000)),
            Column("f_customer_avg_txn_amount_90d", "numeric", "Average transaction amount (VND) in the past 90 days", nullable=True, value_between=(0, 1_000_000_000)),
            Column("f_customer_distinct_merchants_90d", "int", "Number of distinct merchants transacted with in the past 90 days", nullable=True, value_between=(0, 50_000)),
            Column("f_customer_decline_rate_90d", "numeric", "Ratio of declined transactions in the past 90 days (0.0–1.0)", nullable=True, value_between=(0, 1)),
            Column("f_customer_foreign_txn_ratio_90d", "numeric", "Ratio of transactions where ip_country ≠ card_country in the past 90 days", nullable=True, value_between=(0, 1)),
            Column("f_customer_night_txn_ratio_90d", "numeric", "Ratio of transactions between 01:00–04:00 in the past 90 days", nullable=True, value_between=(0, 1)),
            Column("f_stream_otp_failed_count_30m", "int", "Count of otp_failed events for this customer in the past 30 minutes", nullable=True, value_between=(0, 1_000)),
            Column("f_stream_decline_count_30m", "int", "Count of transaction_declined events for this customer in the past 30 minutes", nullable=True, value_between=(0, 1_000)),
            Column("f_stream_txn_velocity_1h", "int", "Count of transaction_attempt events for this customer in the past 1 hour", nullable=True, value_between=(0, 10_000)),
            Column("f_stream_new_merchant_flag", "smallint", "1 if current merchant not seen in customer's 90-day history", nullable=True, accepted_values=[0, 1]),
            Column("f_stream_burst_activity_flag", "smallint", "1 if event occurred in a burst window (12:00–12:20 or 22:00–22:20)", nullable=True, accepted_values=[0, 1]),
            Column("created_ts", "timestamp", "Row creation time — dedup key when multiple snapshots share (customer_id, event_ts)", nullable=False),
        ],
    ),
    "ml_fraud_training": TableSpec(
        table="ml_fraud_training", schema="gold_fraud",
        description=(
            "Point-in-time joined training table for the fraud ML model. One row per "
            "transaction + feature snapshot at transaction time. PIT join ensures no future "
            "feature leakage: feat.event_ts ≤ txn.event_ts."
        ),
        columns=[
            Column("transaction_id", "varchar", "Business key — matches fact_transaction.transaction_id", nullable=False, unique=True),
            Column("customer_id", "varchar", "Customer business key", nullable=False),
            Column("event_ts", "timestamp", "Transaction timestamp — used as PIT join anchor", nullable=False),
            Column("label", "smallint", "Fraud label — 1 if fraudulent, 0 otherwise", nullable=False, accepted_values=[0, 1]),
            Column("f_customer_avg_txn_amount_90d", "numeric", "Average transaction amount in the past 90 days at transaction time", nullable=True),
            Column("txn_amount", "numeric", "Transaction amount in VND", nullable=False, value_between=(0, _AMOUNT_MAX)),
            Column("txn_hour", "smallint", "Hour of transaction (0–23)", nullable=False, value_between=(0, 23)),
            Column("is_declined_txn", "smallint", "1 if transaction was declined", nullable=False, accepted_values=[0, 1]),
            Column("is_foreign_txn", "smallint", "1 if ip_country ≠ card_country", nullable=False, accepted_values=[0, 1]),
        ],
    ),
}


# ── Compile a spec → GE ExpectationSuite (lazy GE import) ───────────────────────

def build_suite(table: str):
    """
    Compile the TableSpec for `table` into a Great Expectations ExpectationSuite.

    Each expectation carries meta:
      severity: "critical" | "warn"
      notes:    the column description (also surfaced in Data Docs)

    Raises KeyError if there is no contract for `table`.
    """
    spec = TABLE_SPECS[table]

    import great_expectations as gx
    from great_expectations import expectations as gxe

    suite = gx.ExpectationSuite(name=table)

    def _meta(col: Column, severity: str) -> dict:
        return {"severity": severity, "notes": col.description}

    # Table-level: the contracted columns must all be present.
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=[c.name for c in spec.columns],
            exact_match=False,  # extra audit columns (ingest_ts, etc.) are allowed
            meta={"severity": "critical", "notes": f"Schema contract for {table}"},
        )
    )

    for col in spec.columns:
        if col.not_null:
            suite.add_expectation(
                gxe.ExpectColumnValuesToNotBeNull(column=col.name, meta=_meta(col, "critical"))
            )
        if col.unique:
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeUnique(column=col.name, meta=_meta(col, "critical"))
            )
        if col.accepted_values is not None:
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeInSet(
                    column=col.name, value_set=col.accepted_values, meta=_meta(col, "warn")
                )
            )
        if col.value_between is not None:
            lo, hi = col.value_between
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeBetween(
                    column=col.name, min_value=lo, max_value=hi, meta=_meta(col, "warn")
                )
            )

    return suite


def table_names() -> list[str]:
    return list(TABLE_SPECS.keys())
