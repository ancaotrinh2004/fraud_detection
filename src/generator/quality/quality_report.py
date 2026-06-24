"""
src/generator/quality/quality_report.py
Compute and save quality metrics for offline and streaming data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path


def compute_offline_quality(
    customers_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
    cards_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    schema_change_date: datetime,
    streaming_summary: dict,
) -> pd.DataFrame:
    """
    Compute quality metrics and return as a DataFrame for the report.
    """
    report = []

    # ── 1. Geographic Skew ─────────────────────────────────────────────────────
    top_cities = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]
    city_counts = transactions_df["city"].value_counts()
    top_city_pct = city_counts[city_counts.index.isin(top_cities)].sum() / len(transactions_df)
    report.append({
        "check": "geographic_skew",
        "metric": "top_5_cities_pct",
        "value": round(top_city_pct * 100, 2),
        "expected": "~80%",
        "status": "PASS" if 0.75 <= top_city_pct <= 0.85 else "WARN",
    })

    # ── 2. Category Skew ──────────────────────────────────────────────────────
    top_cats = ["retail", "food_beverage", "travel"]
    cat_counts = merchants_df["category"].value_counts()
    top_cat_pct = cat_counts[cat_counts.index.isin(top_cats)].sum() / len(merchants_df)
    report.append({
        "check": "category_skew",
        "metric": "top_3_categories_pct",
        "value": round(top_cat_pct * 100, 2),
        "expected": "~75%",
        "status": "PASS" if 0.70 <= top_cat_pct <= 0.80 else "WARN",
    })

    # ── 3. Cardinality ────────────────────────────────────────────────────────
    for col, df, label in [
        ("transaction_id", transactions_df, "transactions"),
        ("customer_id", customers_df, "customers"),
        ("merchant_id", merchants_df, "merchants"),
        ("card_id", cards_df, "cards"),
    ]:
        n_distinct = df[col].nunique()
        n_total = len(df)
        report.append({
            "check": "cardinality",
            "metric": f"{label}.{col}.distinct",
            "value": n_distinct,
            "expected": f"~{n_total}",
            "status": "PASS",
        })

    # ── 4. Schema Evolution ───────────────────────────────────────────────────
    pre = transactions_df[transactions_df["transaction_timestamp"] < schema_change_date]
    post = transactions_df[transactions_df["transaction_timestamp"] >= schema_change_date]

    for col in ["device_fingerprint", "ip_country"]:
        pre_null_pct = pre[col].isna().mean() if len(pre) > 0 else 1.0
        post_null_pct = post[col].isna().mean() if len(post) > 0 else 0.0
        report.append({
            "check": "schema_evolution",
            "metric": f"pre_schema_change.{col}.null_pct",
            "value": round(pre_null_pct * 100, 2),
            "expected": "100%",
            "status": "PASS" if pre_null_pct == 1.0 else "FAIL",
        })
        report.append({
            "check": "schema_evolution",
            "metric": f"post_schema_change.{col}.null_pct",
            "value": round(post_null_pct * 100, 2),
            "expected": "~0%",
            "status": "PASS" if post_null_pct < 0.01 else "WARN",
        })

    # ── 5. Duplicate Rate ─────────────────────────────────────────────────────
    n_total = len(transactions_df)
    n_unique = transactions_df.drop_duplicates(
        subset=["transaction_id", "amount", "customer_id", "transaction_timestamp"]
    ).shape[0]
    dup_rate = (n_total - n_unique) / n_total
    report.append({
        "check": "duplicates_offline",
        "metric": "transactions.duplicate_rate",
        "value": round(dup_rate * 100, 2),
        "expected": "~2%",
        "status": "PASS" if 0.015 <= dup_rate <= 0.025 else "WARN",
    })
    report.append({
        "check": "duplicates_offline",
        "metric": "transactions.rows_before_dedup",
        "value": n_total,
        "expected": "-",
        "status": "INFO",
    })
    report.append({
        "check": "duplicates_offline",
        "metric": "transactions.rows_after_dedup",
        "value": n_unique,
        "expected": "-",
        "status": "INFO",
    })

    # ── 6. Fraud Rate ─────────────────────────────────────────────────────────
    # Use deduplicated for fraud rate
    deduped = transactions_df.sort_values("created_ts").drop_duplicates(
        subset=["transaction_id"], keep="last"
    )
    fraud_rate = deduped["is_fraud"].mean()
    report.append({
        "check": "fraud_label",
        "metric": "fraud_rate",
        "value": round(fraud_rate * 100, 2),
        "expected": "~10%",
        "status": "PASS" if 0.08 <= fraud_rate <= 0.12 else "WARN",
    })
    report.append({
        "check": "fraud_label",
        "metric": "fraud_count",
        "value": int(deduped["is_fraud"].sum()),
        "expected": "-",
        "status": "INFO",
    })

    # ── 7. Streaming Quality ──────────────────────────────────────────────────
    for key, expected in [
        ("late_arrival_rate_actual", "~15%"),
        ("duplicate_rate_actual", "~1.5%"),
        ("burst_event_share", ">0%"),
    ]:
        val = streaming_summary.get(key, 0)
        report.append({
            "check": "streaming",
            "metric": key,
            "value": round(val * 100, 2),
            "expected": expected,
            "status": "INFO",
        })

    report.append({
        "check": "streaming",
        "metric": "total_events",
        "value": streaming_summary.get("total_events", 0),
        "expected": "-",
        "status": "INFO",
    })

    return pd.DataFrame(report)


def save_quality_report(report_df: pd.DataFrame, output_dir: Path) -> None:
    """Save quality report as CSV and print a summary."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "quality_report.csv"
    report_df.to_csv(report_path, index=False)
    print(f"\n{'='*60}")
    print("QUALITY REPORT")
    print(f"{'='*60}")
    print(report_df.to_string(index=False))
    print(f"\nSaved to: {report_path}")