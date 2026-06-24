"""
Shared pytest fixtures used across unit and integration tests.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.pipelines.ml.services import (
    FEATURE_COLS,
    ModelArtifact,
)


# ── RNG ───────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def rng():
    return np.random.default_rng(42)


# ── Config ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_cfg():
    return {
        "minio": {
            "endpoint_url": "http://localhost:9000",
            "access_key":   "test_access",
            "secret_key":   "test_secret",
        },
        "postgres": {
            "host":     "localhost",
            "port":     5432,
            "database": "fraud_detection",
            "user":     "fraud_user",
            "password": "fraud_pass",
            "schema":   "gold_fraud",
        },
    }


# ── SQLAlchemy mock engine ─────────────────────────────────────────────────────

@pytest.fixture
def mock_engine():
    engine = MagicMock()
    conn = MagicMock()
    # Use MagicMock (not lambda) so tests can access .return_value
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__  = MagicMock(return_value=False)
    engine.begin.return_value.__enter__   = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__    = MagicMock(return_value=False)
    return engine


# ── DataFrame fixtures ─────────────────────────────────────────────────────────

def _make_training_df(n: int = 200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    base_ts = datetime(2025, 4, 1)
    rows = []
    for i in range(n):
        avg = float(rng.uniform(50, 500))
        amt = float(rng.uniform(10, 1000))
        rows.append({
            "transaction_id":                    f"T{i:06d}",
            "customer_id":                       f"C{i % 30:07d}",
            "event_ts":                          base_ts + timedelta(hours=i * 2),
            "label":                             int(rng.integers(0, 2)),
            "f_customer_total_txn_90d":          float(rng.integers(1, 100)),
            "f_customer_avg_txn_amount_90d":     avg,
            "f_customer_distinct_merchants_90d": float(rng.integers(1, 20)),
            "f_customer_decline_rate_90d":       float(rng.uniform(0, 0.3)),
            "f_customer_foreign_txn_ratio_90d":  float(rng.uniform(0, 0.5)),
            "f_customer_night_txn_ratio_90d":    float(rng.uniform(0, 0.2)),
            "f_stream_otp_failed_count_30m":     float(rng.integers(0, 3)),
            "f_stream_decline_count_30m":        float(rng.integers(0, 2)),
            "f_stream_txn_velocity_1h":          float(rng.integers(0, 5)),
            "f_stream_new_merchant_flag":        float(rng.integers(0, 2)),
            "f_stream_burst_activity_flag":      float(rng.integers(0, 2)),
            "txn_amount":                        amt,
            "txn_hour":                          int(rng.integers(0, 24)),
            "is_declined_txn":                   int(rng.integers(0, 2)),
            "is_foreign_txn":                    int(rng.integers(0, 2)),
        })
    df = pd.DataFrame(rows)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df


@pytest.fixture
def training_df():
    """200-row valid ml_fraud_training DataFrame with all required columns."""
    return _make_training_df(200)


@pytest.fixture
def small_training_df():
    """30-row DataFrame used where size doesn't matter."""
    return _make_training_df(30)


# ── Model mock fixtures ────────────────────────────────────────────────────────

def _make_model_mock(fraud_prob: float = 0.1):
    model = MagicMock()
    model.predict_proba.side_effect = lambda X: np.column_stack([
        np.full(len(X), 1 - fraud_prob),
        np.full(len(X), fraud_prob),
    ])
    return model


@pytest.fixture
def mock_model():
    return _make_model_mock(fraud_prob=0.1)


@pytest.fixture
def mock_artifact(mock_model):
    return ModelArtifact(
        model=mock_model,
        feature_cols=FEATURE_COLS,
        model_version="v_20260101_120000_abc123",
        metrics={"pr_auc": 0.85, "f1": 0.72, "precision": 0.80, "recall": 0.65, "threshold": 0.5},
    )


@pytest.fixture
def high_risk_artifact():
    return ModelArtifact(
        model=_make_model_mock(fraud_prob=0.95),
        feature_cols=FEATURE_COLS,
        model_version="v_high_risk",
        metrics={"pr_auc": 0.90},
    )


@pytest.fixture
def low_risk_artifact():
    return ModelArtifact(
        model=_make_model_mock(fraud_prob=0.02),
        feature_cols=FEATURE_COLS,
        model_version="v_low_risk",
        metrics={"pr_auc": 0.80},
    )


# ── Feature dict fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def full_features():
    """Complete feature dict: all FEATURE_COLS present."""
    return {
        "f_customer_total_txn_90d":          42.0,
        "f_customer_avg_txn_amount_90d":     150.0,
        "f_customer_distinct_merchants_90d":  8.0,
        "f_customer_decline_rate_90d":        0.05,
        "f_customer_foreign_txn_ratio_90d":   0.1,
        "f_customer_night_txn_ratio_90d":     0.03,
        "f_stream_otp_failed_count_30m":      0.0,
        "f_stream_decline_count_30m":         0.0,
        "f_stream_txn_velocity_1h":           1.0,
        "f_stream_new_merchant_flag":         0.0,
        "f_stream_burst_activity_flag":       0.0,
        "txn_amount":                        200.0,
        "txn_hour":                           14,
        "is_declined_txn":                     0,
        "is_foreign_txn":                      0,
        "txn_amount_ratio":              200.0 / 150.0,
        "is_night_txn":                        0,
    }


@pytest.fixture
def night_fraud_features():
    """High-risk feature profile: night + declined + foreign + high amount."""
    return {
        "f_customer_total_txn_90d":           5.0,
        "f_customer_avg_txn_amount_90d":    200.0,
        "f_customer_distinct_merchants_90d":  1.0,
        "f_customer_decline_rate_90d":        0.8,
        "f_customer_foreign_txn_ratio_90d":   0.9,
        "f_customer_night_txn_ratio_90d":     0.7,
        "f_stream_otp_failed_count_30m":      3.0,
        "f_stream_decline_count_30m":         2.0,
        "f_stream_txn_velocity_1h":           8.0,
        "f_stream_new_merchant_flag":         1.0,
        "f_stream_burst_activity_flag":       1.0,
        "txn_amount":                      5000.0,
        "txn_hour":                            3,
        "is_declined_txn":                     1,
        "is_foreign_txn":                      1,
        "txn_amount_ratio":               25.0,
        "is_night_txn":                        1,
    }
