"""
src/generator/offline/customers.py
Generate the customers reference table.
"""

import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


COUNTRIES = ["Vietnam", "Singapore", "Thailand", "Malaysia", "Philippines"]
COUNTRY_WEIGHTS = [0.70, 0.10, 0.08, 0.07, 0.05]

ALL_CITIES = {
    "Vietnam": [
        "Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong",
        "Bien Hoa", "Hue", "Nha Trang", "Vung Tau", "Buon Ma Thuot",
    ],
    "Singapore": ["Singapore"],
    "Thailand": ["Bangkok", "Chiang Mai", "Phuket"],
    "Malaysia": ["Kuala Lumpur", "Penang", "Johor Bahru"],
    "Philippines": ["Manila", "Cebu", "Davao"],
}

RISK_SEGMENTS = ["low", "medium", "high"]
RISK_WEIGHTS = [0.60, 0.30, 0.10]

KYC_STATUSES = ["verified", "pending", "failed"]
KYC_WEIGHTS = [0.85, 0.10, 0.05]


def generate_customers(n_customers: int, days_history: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate the customers table.
    One row per customer, stable reference data.
    """
    end_date = datetime(2025, 10, 1)
    start_date = end_date - timedelta(days=days_history + 60)  # some signed up before history window

    customer_ids = [f"C{str(i).zfill(7)}" for i in range(1, n_customers + 1)]

    # Signup timestamps spread across history
    signup_offsets = rng.integers(0, int((end_date - start_date).total_seconds()), size=n_customers)
    signup_ts = [start_date + timedelta(seconds=int(s)) for s in signup_offsets]

    countries = rng.choice(COUNTRIES, size=n_customers, p=COUNTRY_WEIGHTS)

    cities = []
    for country in countries:
        city_pool = ALL_CITIES[country]
        cities.append(rng.choice(city_pool))

    risk_segments = rng.choice(RISK_SEGMENTS, size=n_customers, p=RISK_WEIGHTS)
    kyc_statuses = rng.choice(KYC_STATUSES, size=n_customers, p=KYC_WEIGHTS)
    marketing_opt_in = rng.choice([True, False], size=n_customers, p=[0.65, 0.35])

    df = pd.DataFrame({
        "customer_id": customer_ids,
        "signup_ts": signup_ts,
        "country": countries,
        "city": cities,
        "risk_segment": risk_segments,
        "kyc_status": kyc_statuses,
        "marketing_opt_in": marketing_opt_in,
        "created_ts": [datetime(2025, 10, 1)] * n_customers,  # static load
    })

    return df