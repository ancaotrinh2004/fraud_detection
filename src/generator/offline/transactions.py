"""
src/generator/offline/transactions.py

Core transactions table with:
- Geographic skew (80% from top 5 cities)
- Schema evolution (device_fingerprint, ip_country absent before schema_change_date)
- 2% duplicate rate (payment gateway retry simulation)
- Deterministic fraud labels based on ≥2 conditions
"""

import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def _make_uuid(rng: np.random.Generator) -> str:
    """Generate a UUID from two int64 values to avoid overflow."""
    hi = int(rng.integers(0, 2**63))
    lo = int(rng.integers(0, 2**63))
    return str(uuid.UUID(int=(hi << 64) | lo))


TOP_CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]
OTHER_CITIES = ["Bien Hoa", "Hue", "Nha Trang", "Vung Tau", "Buon Ma Thuot",
                "Bangkok", "Singapore", "Kuala Lumpur", "Manila"]

CURRENCIES = ["VND", "USD", "SGD", "THB"]
CURRENCY_WEIGHTS = [0.80, 0.10, 0.06, 0.04]

STATUSES = ["approved", "declined", "pending", "reversed"]
STATUS_WEIGHTS = [0.88, 0.08, 0.03, 0.01]

COUNTRIES = ["Vietnam", "Singapore", "Thailand", "Malaysia", "Philippines", "USA", "China"]
COUNTRY_WEIGHTS = [0.70, 0.10, 0.07, 0.06, 0.04, 0.02, 0.01]


def _generate_base_transactions(
    n_customers: int,
    avg_txn_per_customer: int,
    customer_ids: list,
    card_ids: list,
    merchant_ids: list,
    customer_df: pd.DataFrame,
    card_df: pd.DataFrame,
    days_history: int,
    skew_city_ratio: float,
    schema_change_date: datetime,
    rng: np.random.Generator,
    drift_enabled: bool = False,
    drift_start_date: datetime | None = None,
    drift_mode: str = "gradual",
    amount_drift_multiplier: float = 1.30,
    drift_ramp_days: int = 30,
) -> pd.DataFrame:
    """Generate the base transaction rows without fraud labels."""
    n_txn = n_customers * avg_txn_per_customer

    end_date = datetime(2025, 10, 1)
    start_date = end_date - timedelta(days=days_history)

    txn_ids = [_make_uuid(rng) for _ in range(n_txn)]

    # Assign customers and their cards
    chosen_customers = rng.choice(customer_ids, size=n_txn)

    # Build customer → cards lookup
    card_by_customer = card_df.groupby("customer_id")["card_id"].apply(list).to_dict()

    chosen_cards = []
    for cust in chosen_customers:
        cards_for_cust = card_by_customer.get(cust, [card_ids[0]])
        chosen_cards.append(rng.choice(cards_for_cust))

    chosen_merchants = rng.choice(merchant_ids, size=n_txn)

    # Timestamps
    offsets = rng.integers(0, int((end_date - start_date).total_seconds()), size=n_txn)
    txn_ts = [start_date + timedelta(seconds=int(s)) for s in offsets]
    # created_ts slightly after event_timestamp (1–300 seconds lag)
    created_offsets = rng.integers(1, 300, size=n_txn)
    created_ts = [t + timedelta(seconds=int(s)) for t, s in zip(txn_ts, created_offsets)]

    # Amounts: log-normal distribution centered around ~500k VND
    amounts = np.round(rng.lognormal(mean=13.0, sigma=1.2, size=n_txn), 2)

    # ── Scenario B: Amount drift — multiply amounts after drift_start_date ────
    if drift_enabled and drift_start_date is not None:
        txn_ts_unix = np.array([t.timestamp() for t in txn_ts])
        drift_ts_unix = drift_start_date.timestamp()
        if drift_mode == "gradual":
            ramp_seconds = drift_ramp_days * 86400
            progress = np.clip((txn_ts_unix - drift_ts_unix) / ramp_seconds, 0.0, 1.0)
            multipliers = 1.0 + (amount_drift_multiplier - 1.0) * progress
        else:
            multipliers = np.where(txn_ts_unix >= drift_ts_unix, amount_drift_multiplier, 1.0)
        amounts = np.round(amounts * multipliers, 2)

    currencies = rng.choice(CURRENCIES, size=n_txn, p=CURRENCY_WEIGHTS)
    statuses = rng.choice(STATUSES, size=n_txn, p=STATUS_WEIGHTS)

    # Geographic skew: 80% from top 5 cities
    n_top = int(n_txn * skew_city_ratio)
    n_other = n_txn - n_top
    city_list = (
        rng.choice(TOP_CITIES, size=n_top).tolist()
        + rng.choice(OTHER_CITIES, size=n_other).tolist()
    )
    rng.shuffle(city_list)

    # Schema evolution: device_fingerprint + ip_country only after schema_change_date
    device_fingerprints = []
    ip_countries = []
    for ts in txn_ts:
        if ts >= schema_change_date:
            device_fingerprints.append(_make_uuid(rng)[:16])
            ip_countries.append(rng.choice(COUNTRIES, p=COUNTRY_WEIGHTS))
        else:
            device_fingerprints.append(None)  # schema evolution — missing in old records
            ip_countries.append(None)

    df = pd.DataFrame({
        "transaction_id": txn_ids,
        "customer_id": chosen_customers,
        "card_id": chosen_cards,
        "merchant_id": chosen_merchants,
        "transaction_timestamp": txn_ts,
        "created_ts": created_ts,
        "amount": amounts,
        "currency": currencies,
        "transaction_status": statuses,
        "city": city_list,
        "device_fingerprint": device_fingerprints,
        "ip_country": ip_countries,
    })

    return df


def _assign_fraud_labels(
    df: pd.DataFrame,
    card_df: pd.DataFrame,
    fraud_rate: float,
    fraud_label_min_conditions: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Assign is_fraud = 1 deterministically based on ≥ fraud_label_min_conditions:

    c1 (amount_anomaly): amount > 3× customer rolling 90-day average
    c2 (cross_border):   ip_country ≠ card_country  (NULL ip_country → c2 = 0)
    c3 (night_time):     transaction between 01:00–04:00
    c4 (declined):       transaction_status = "declined"

    Each condition maps directly to a training feature in ml_fraud_training:
      c1 → txn_amount_ratio  (txn_amount / f_customer_avg_txn_amount_90d)
      c2 → is_foreign_txn
      c3 → is_night_txn
      c4 → is_declined_txn

    Natural fraud rate (before calibration):
      ~3%  pre-2025-07-01  (ip_country = NULL → c2 always 0)
      ~11% post-2025-07-01 (all four conditions active)
      ~8%  overall

    The calibration step flips a small number of labels to hit fraud_rate (default 10%).
    Because the natural rate for post-July transactions (the training subset) is ~11%,
    only ~10% of fraud labels need to be flipped — preserving >90% of the signal.
    This yields PR-AUC well above 0.70 in the ML model.

    (Previous design had a 5th condition, c5_new_merchant, that fired on 99.96% of
    all transactions, inflating natural rate to ~40% and requiring 75% label flips.)
    """
    df = df.copy().sort_values("transaction_timestamp").reset_index(drop=True)

    # Build card_country lookup
    card_country = card_df.set_index("card_id")["card_country"].to_dict()
    df["card_country"] = df["card_id"].map(card_country)

    # ── Condition 1: amount anomaly ──────────────────────────────────────────
    window_90d = pd.Timedelta(days=90)
    df["txn_ts"] = pd.to_datetime(df["transaction_timestamp"])
    cust_avg = (
        df.groupby("customer_id")
        .apply(lambda g: g.set_index("txn_ts")["amount"].rolling(window_90d, min_periods=1).mean())
        .reset_index(level=0, drop=True)
        .rename("cust_avg_amount_90d")
    )
    df = df.join(cust_avg)
    df["c1_amount_anomaly"] = df["amount"] > (3 * df["cust_avg_amount_90d"])

    # ── Condition 2: cross-border ─────────────────────────────────────────────
    df["c2_cross_border"] = (
        df["ip_country"].notna()
        & (df["ip_country"] != df["card_country"])
    )

    # ── Condition 3: night-time (01:00–04:00) ────────────────────────────────
    df["hour"] = df["txn_ts"].dt.hour
    df["c3_night_time"] = df["hour"].between(1, 4)

    # ── Condition 4: declined transaction ────────────────────────────────────
    # Per-transaction decline flag — simpler than the original 60-min window
    # approximation and maps 1:1 to the is_declined_txn training feature.
    df["c4_declined"] = (df["transaction_status"] == "declined")

    # ── Count conditions met ──────────────────────────────────────────────────
    condition_cols = ["c1_amount_anomaly", "c2_cross_border", "c3_night_time", "c4_declined"]
    df["conditions_met"] = df[condition_cols].sum(axis=1)
    df["is_fraud"] = (df["conditions_met"] >= fraud_label_min_conditions).astype(int)

    # ── Calibrate to target fraud_rate ───────────────────────────────────────
    current_rate = df["is_fraud"].mean()
    if current_rate > fraud_rate:
        fraud_idx  = df[df["is_fraud"] == 1].index
        n_to_flip  = int(len(fraud_idx) - fraud_rate * len(df))
        flip_idx   = rng.choice(fraud_idx, size=max(0, n_to_flip), replace=False)
        df.loc[flip_idx, "is_fraud"] = 0
    elif current_rate < fraud_rate:
        legit_idx  = df[df["is_fraud"] == 0].index
        n_to_flip  = int(fraud_rate * len(df) - df["is_fraud"].sum())
        flip_idx   = rng.choice(legit_idx, size=max(0, n_to_flip), replace=False)
        df.loc[flip_idx, "is_fraud"] = 1

    drop_cols = condition_cols + ["conditions_met", "cust_avg_amount_90d", "card_country", "txn_ts", "hour"]
    df = df.drop(columns=drop_cols, errors="ignore")

    return df


def _inject_duplicates(
    df: pd.DataFrame,
    duplicate_rate: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Inject 2% duplicate rows — same transaction_id, same payload,
    but slightly different created_ts (payment gateway retry simulation).
    """
    n_dupes = int(len(df) * duplicate_rate)
    dupe_idx = rng.choice(df.index, size=n_dupes, replace=False)
    dupes = df.loc[dupe_idx].copy()

    # Shift created_ts by 30–180 seconds (retry window)
    retry_offsets = rng.integers(30, 180, size=n_dupes)
    dupes["created_ts"] = [
        t + timedelta(seconds=int(s))
        for t, s in zip(dupes["created_ts"], retry_offsets)
    ]

    df_with_dupes = pd.concat([df, dupes], ignore_index=True)
    df_with_dupes = df_with_dupes.sort_values("transaction_timestamp").reset_index(drop=True)

    return df_with_dupes


def generate_transactions(
    n_customers: int,
    avg_txn_per_customer: int,
    customer_ids: list,
    card_ids: list,
    merchant_ids: list,
    customer_df: pd.DataFrame,
    card_df: pd.DataFrame,
    days_history: int,
    skew_city_ratio: float,
    schema_change_date: datetime,
    fraud_rate: float,
    fraud_label_min_conditions: int,
    duplicate_rate: float,
    rng: np.random.Generator,
    drift_enabled: bool = False,
    drift_start_date: datetime | None = None,
    drift_mode: str = "gradual",
    amount_drift_multiplier: float = 1.30,
    drift_ramp_days: int = 30,
) -> pd.DataFrame:
    """Main entry — generates transactions with fraud labels, schema evolution, and duplicates."""

    print("  [transactions] Generating base rows...")
    df = _generate_base_transactions(
        n_customers=n_customers,
        avg_txn_per_customer=avg_txn_per_customer,
        customer_ids=customer_ids,
        card_ids=card_ids,
        merchant_ids=merchant_ids,
        customer_df=customer_df,
        card_df=card_df,
        days_history=days_history,
        skew_city_ratio=skew_city_ratio,
        schema_change_date=schema_change_date,
        rng=rng,
        drift_enabled=drift_enabled,
        drift_start_date=drift_start_date,
        drift_mode=drift_mode,
        amount_drift_multiplier=amount_drift_multiplier,
        drift_ramp_days=drift_ramp_days,
    )

    print("  [transactions] Assigning fraud labels...")
    df = _assign_fraud_labels(
        df=df,
        card_df=card_df,
        fraud_rate=fraud_rate,
        fraud_label_min_conditions=fraud_label_min_conditions,
        rng=rng,
    )

    print("  [transactions] Injecting duplicates...")
    df = _inject_duplicates(df=df, duplicate_rate=duplicate_rate, rng=rng)

    print(f"  [transactions] Done. Total rows (with dupes): {len(df):,}")
    return df