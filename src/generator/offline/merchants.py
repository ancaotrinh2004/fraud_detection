"""
src/generator/offline/merchants.py
Generate the merchants reference table.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Top 3 categories hold skew_category_ratio of merchants
TOP_CATEGORIES = ["retail", "food_beverage", "travel"]
OTHER_CATEGORIES = ["entertainment", "health", "education", "utilities", "gaming", "crypto"]

COUNTRIES = ["Vietnam", "Singapore", "Thailand", "Malaysia", "Philippines"]
COUNTRY_WEIGHTS = [0.60, 0.15, 0.10, 0.10, 0.05]

CITY_BY_COUNTRY = {
    "Vietnam": ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"],
    "Singapore": ["Singapore"],
    "Thailand": ["Bangkok", "Chiang Mai"],
    "Malaysia": ["Kuala Lumpur", "Penang"],
    "Philippines": ["Manila", "Cebu"],
}


def generate_merchants(
    n_merchants: int,
    skew_category_ratio: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate merchants table.
    75% of merchants fall into top 3 categories (retail, food_beverage, travel).
    """
    end_date = datetime(2025, 10, 1)
    start_date = datetime(2023, 1, 1)

    merchant_ids = [f"M{str(i).zfill(5)}" for i in range(1, n_merchants + 1)]

    # Category skew
    n_top = int(n_merchants * skew_category_ratio)
    n_other = n_merchants - n_top

    top_cats = rng.choice(TOP_CATEGORIES, size=n_top).tolist()
    other_cats = rng.choice(OTHER_CATEGORIES, size=n_other).tolist()
    categories = top_cats + other_cats
    rng.shuffle(categories)

    countries = rng.choice(COUNTRIES, size=n_merchants, p=COUNTRY_WEIGHTS)

    cities = []
    for country in countries:
        city_pool = CITY_BY_COUNTRY[country]
        cities.append(rng.choice(city_pool))

    # Created timestamps
    created_offsets = rng.integers(0, int((end_date - start_date).total_seconds()), size=n_merchants)
    created_ts = [start_date + timedelta(seconds=int(s)) for s in created_offsets]

    is_active = rng.choice([True, False], size=n_merchants, p=[0.95, 0.05])

    df = pd.DataFrame({
        "merchant_id": merchant_ids,
        "merchant_name": [f"Merchant_{mid}" for mid in merchant_ids],
        "category": categories,
        "country": countries,
        "city": cities,
        "is_active": is_active,
        "created_ts": created_ts,
    })

    return df