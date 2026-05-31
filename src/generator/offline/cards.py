"""
src/generator/offline/cards.py
Generate the cards reference table — ~1.3 cards per customer on average.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


CARD_TYPES = ["credit", "debit", "prepaid"]
CARD_TYPE_WEIGHTS = [0.50, 0.40, 0.10]

ISSUING_BANKS = [
    "VietcomBank", "TechcomBank", "VPBank", "MBBank", "ACB",
    "BIDV", "Agribank", "HDBank", "OCB", "SacomBank",
    "DBS", "OCBC", "UOB", "Kasikorn", "Maybank",
]
BANK_WEIGHTS = [
    0.15, 0.12, 0.10, 0.10, 0.08,
    0.08, 0.07, 0.05, 0.05, 0.05,
    0.04, 0.03, 0.03, 0.03, 0.02,
]

CARD_COUNTRIES = ["Vietnam", "Singapore", "Thailand", "Malaysia", "Philippines"]
CARD_COUNTRY_WEIGHTS = [0.70, 0.10, 0.08, 0.07, 0.05]


def generate_cards(
    n_cards: int,
    customer_ids: list,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate cards table.
    Each card is assigned to a customer (some customers have multiple cards).
    """
    now = datetime(2025, 10, 1)

    card_ids = [f"CARD{str(i).zfill(8)}" for i in range(1, n_cards + 1)]

    # Assign customers — some get multiple cards
    assigned_customers = rng.choice(customer_ids, size=n_cards)

    card_types = rng.choice(CARD_TYPES, size=n_cards, p=CARD_TYPE_WEIGHTS)
    issuing_banks = rng.choice(ISSUING_BANKS, size=n_cards, p=BANK_WEIGHTS)
    card_countries = rng.choice(CARD_COUNTRIES, size=n_cards, p=CARD_COUNTRY_WEIGHTS)

    # Issued between 5 years ago and now
    issued_offsets = rng.integers(0, 5 * 365 * 24 * 3600, size=n_cards)
    issued_ts = [now - timedelta(seconds=int(s)) for s in issued_offsets]

    # Expiry 2-5 years after issue
    expiry_extra = rng.integers(2 * 365, 5 * 365, size=n_cards)
    expiry_ts = [
        iss + timedelta(days=int(d))
        for iss, d in zip(issued_ts, expiry_extra)
    ]

    is_active = [exp > now for exp in expiry_ts]

    df = pd.DataFrame({
        "card_id": card_ids,
        "customer_id": assigned_customers,
        "card_type": card_types,
        "issuing_bank": issuing_banks,
        "card_country": card_countries,
        "issued_ts": issued_ts,
        "expiry_ts": expiry_ts,
        "is_active": is_active,
        "created_ts": [now] * n_cards,
    })

    return df