"""
src/generator/offline/transaction_items.py
Generate transaction line items (one-to-many with transactions).
"""

import pandas as pd
import numpy as np


PRODUCT_CATEGORIES = [
    "electronics", "clothing", "food", "travel", "beauty",
    "sports", "home", "books", "gaming", "services",
]


def generate_transaction_items(
    transaction_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate transaction_items.
    Each transaction has 1–5 line items.
    Only approved + pending transactions get items (declined/reversed don't).
    """
    eligible = transaction_df[
        transaction_df["transaction_status"].isin(["approved", "pending"])
    ][["transaction_id", "transaction_timestamp", "amount"]].copy()

    rows = []
    item_counter = 1

    for _, txn in eligible.iterrows():
        n_items = int(rng.integers(1, 6))  # 1–5 items
        total_amount = txn["amount"]

        # Distribute total amount across items (randomly split)
        splits = rng.dirichlet(np.ones(n_items))
        item_amounts = np.round(splits * total_amount, 2)
        # Fix rounding error on last item
        item_amounts[-1] = round(total_amount - item_amounts[:-1].sum(), 2)

        for i in range(n_items):
            quantity = int(rng.integers(1, 4))
            unit_price = round(item_amounts[i] / quantity, 2)
            discount = round(unit_price * rng.uniform(0, 0.15), 2)  # 0–15% discount

            rows.append({
                "item_id": f"ITEM{str(item_counter).zfill(10)}",
                "transaction_id": txn["transaction_id"],
                "product_category": rng.choice(PRODUCT_CATEGORIES),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount,
                "transaction_date": txn["transaction_timestamp"].date()
                    if hasattr(txn["transaction_timestamp"], "date")
                    else txn["transaction_timestamp"],
            })
            item_counter += 1

    df = pd.DataFrame(rows)
    return df