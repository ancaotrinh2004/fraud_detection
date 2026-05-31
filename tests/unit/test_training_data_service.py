"""
Unit tests for TrainingDataService — pure pandas/numpy, no external dependencies.
Coverage target: TrainingDataService (read_training_table, validate_schema,
get_split_boundaries, split_by_time, handle_missing_features).
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.pipelines.ml.services import (
    FEATURE_COLS,
    _DB_FEATURE_COLS,
    TrainingDataService,
)


@pytest.fixture
def svc():
    return TrainingDataService()


# ── read_training_table ────────────────────────────────────────────────────────

class TestReadTrainingTable:
    def test_returns_dataframe(self, svc, training_df):
        engine = MagicMock()
        with patch("src.pipelines.ml.services.pd.read_sql", return_value=training_df):
            result = svc.read_training_table(engine)
        assert isinstance(result, pd.DataFrame)

    def test_event_timestamp_converted_to_datetime(self, svc, training_df):
        training_df["event_timestamp"] = training_df["event_timestamp"].astype(str)
        engine = MagicMock()
        with patch("src.pipelines.ml.services.pd.read_sql", return_value=training_df):
            result = svc.read_training_table(engine)
        assert pd.api.types.is_datetime64_any_dtype(result["event_timestamp"])

    def test_queries_correct_table(self, svc, training_df):
        engine = MagicMock()
        with patch("src.pipelines.ml.services.pd.read_sql", return_value=training_df) as mock_sql:
            svc.read_training_table(engine)
        sql_call = mock_sql.call_args[0][0]
        assert "ml_fraud_training" in sql_call
        assert "ORDER BY event_timestamp" in sql_call

    def test_passes_engine_to_read_sql(self, svc, training_df):
        engine = MagicMock()
        with patch("src.pipelines.ml.services.pd.read_sql", return_value=training_df) as mock_sql:
            svc.read_training_table(engine)
        assert mock_sql.call_args[0][1] is engine


# ── validate_schema ────────────────────────────────────────────────────────────

class TestValidateSchema:
    def test_valid_dataframe_passes(self, svc, training_df):
        svc.validate_schema(training_df)  # must not raise

    def test_raises_when_feature_column_missing(self, svc, training_df):
        df = training_df.drop(columns=["f_customer_total_txn_90d"])
        with pytest.raises(ValueError, match="Missing columns"):
            svc.validate_schema(df)

    def test_raises_when_multiple_columns_missing(self, svc, training_df):
        df = training_df.drop(columns=["f_customer_total_txn_90d", "txn_amount"])
        with pytest.raises(ValueError, match="Missing columns"):
            svc.validate_schema(df)

    def test_raises_when_label_column_missing(self, svc, training_df):
        df = training_df.drop(columns=["label"])
        with pytest.raises(ValueError, match="Missing columns"):
            svc.validate_schema(df)

    def test_raises_when_event_timestamp_missing(self, svc, training_df):
        df = training_df.drop(columns=["event_timestamp"])
        with pytest.raises(ValueError, match="Missing columns"):
            svc.validate_schema(df)

    def test_raises_when_label_has_null(self, svc, training_df):
        df = training_df.copy()
        df.loc[0, "label"] = None
        with pytest.raises(ValueError, match="NULLs"):
            svc.validate_schema(df)

    def test_raises_when_all_labels_null(self, svc, training_df):
        df = training_df.copy()
        df["label"] = None
        with pytest.raises(ValueError, match="NULLs"):
            svc.validate_schema(df)

    def test_all_db_feature_cols_are_required(self, svc, training_df):
        for col in _DB_FEATURE_COLS:
            df = training_df.drop(columns=[col])
            with pytest.raises(ValueError, match="Missing columns"):
                svc.validate_schema(df)

    def test_error_message_lists_missing_columns(self, svc, training_df):
        df = training_df.drop(columns=["f_customer_total_txn_90d"])
        with pytest.raises(ValueError) as exc_info:
            svc.validate_schema(df)
        assert "f_customer_total_txn_90d" in str(exc_info.value)


# ── get_split_boundaries ───────────────────────────────────────────────────────

class TestGetSplitBoundaries:
    def test_returns_dict_with_required_keys(self, svc, training_df):
        b = svc.get_split_boundaries(training_df)
        assert "train_end" in b
        assert "val_end" in b

    def test_boundaries_in_ascending_order(self, svc, training_df):
        b = svc.get_split_boundaries(training_df)
        assert b["train_end"] < b["val_end"]

    def test_boundaries_within_data_range(self, svc, training_df):
        b = svc.get_split_boundaries(training_df)
        assert b["train_end"] >= training_df["event_timestamp"].min()
        assert b["val_end"]   <= training_df["event_timestamp"].max()

    def test_train_end_at_70pct_of_data(self, svc, training_df):
        n = len(training_df)
        b = svc.get_split_boundaries(training_df, train_frac=0.70)
        sorted_ts = training_df["event_timestamp"].sort_values()
        expected_idx = int(n * 0.70)
        assert b["train_end"] == sorted_ts.iloc[expected_idx]

    def test_val_end_at_85pct_of_data(self, svc, training_df):
        n = len(training_df)
        b = svc.get_split_boundaries(training_df, train_frac=0.70, val_frac=0.15)
        sorted_ts = training_df["event_timestamp"].sort_values()
        expected_idx = int(n * 0.85)
        assert b["val_end"] == sorted_ts.iloc[expected_idx]

    def test_custom_fractions(self, svc, training_df):
        b = svc.get_split_boundaries(training_df, train_frac=0.60, val_frac=0.20)
        n = len(training_df)
        sorted_ts = training_df["event_timestamp"].sort_values()
        assert b["train_end"] == sorted_ts.iloc[int(n * 0.60)]
        assert b["val_end"]   == sorted_ts.iloc[int(n * 0.80)]


# ── split_by_time ──────────────────────────────────────────────────────────────

class TestSplitByTime:
    def test_returns_three_dataframes(self, svc, training_df):
        result = svc.split_by_time(training_df)
        assert len(result) == 3
        for split in result:
            assert isinstance(split, pd.DataFrame)

    def test_all_rows_accounted_for(self, svc, training_df):
        train, val, test_split = svc.split_by_time(training_df)
        assert len(train) + len(val) + len(test_split) == len(training_df)

    def test_no_data_leakage_train_val(self, svc, training_df):
        train, val, _ = svc.split_by_time(training_df)
        assert train["event_timestamp"].max() <= val["event_timestamp"].min()

    def test_no_data_leakage_val_test(self, svc, training_df):
        _, val, test_split = svc.split_by_time(training_df)
        assert val["event_timestamp"].max() <= test_split["event_timestamp"].min()

    def test_train_is_largest_split_at_70pct(self, svc, training_df):
        train, val, test_split = svc.split_by_time(training_df)
        assert len(train) > len(val)
        assert len(train) > len(test_split)

    def test_approximate_proportions_70_15_15(self, svc, training_df):
        n = len(training_df)
        train, val, test_split = svc.split_by_time(training_df, train_frac=0.70, val_frac=0.15)
        assert abs(len(train) / n - 0.70) < 0.05
        assert abs(len(val)   / n - 0.15) < 0.05
        assert abs(len(test_split) / n - 0.15) < 0.05

    def test_custom_fractions(self, svc, training_df):
        n = len(training_df)
        train, val, test_split = svc.split_by_time(training_df, train_frac=0.60, val_frac=0.20)
        assert abs(len(train) / n - 0.60) < 0.05
        assert abs(len(val)   / n - 0.20) < 0.05

    def test_columns_preserved(self, svc, training_df):
        train, val, test_split = svc.split_by_time(training_df)
        for split in [train, val, test_split]:
            assert set(split.columns) == set(training_df.columns)

    def test_original_dataframe_not_mutated(self, svc, training_df):
        original_len = len(training_df)
        svc.split_by_time(training_df)
        assert len(training_df) == original_len

    def test_respects_time_ordering_not_index_ordering(self, svc, training_df):
        # Shuffle the DataFrame — split should still be time-based
        shuffled = training_df.sample(frac=1, random_state=99).reset_index(drop=True)
        train, val, test_split = svc.split_by_time(shuffled)
        assert train["event_timestamp"].max() <= val["event_timestamp"].min()


# ── handle_missing_features ────────────────────────────────────────────────────

class TestHandleMissingFeatures:
    def test_adds_txn_amount_ratio(self, svc, training_df):
        result = svc.handle_missing_features(training_df)
        assert "txn_amount_ratio" in result.columns

    def test_adds_is_night_txn(self, svc, training_df):
        result = svc.handle_missing_features(training_df)
        assert "is_night_txn" in result.columns

    def test_txn_amount_ratio_formula(self, svc, training_df):
        result = svc.handle_missing_features(training_df)
        mask = result["f_customer_avg_txn_amount_90d"] > 0
        expected = result.loc[mask, "txn_amount"] / result.loc[mask, "f_customer_avg_txn_amount_90d"]
        pd.testing.assert_series_equal(
            result.loc[mask, "txn_amount_ratio"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_txn_amount_ratio_zero_when_avg_is_zero(self, svc, training_df):
        df = training_df.copy()
        df["f_customer_avg_txn_amount_90d"] = 0.0
        result = svc.handle_missing_features(df)
        assert (result["txn_amount_ratio"] == 0.0).all()

    def test_is_night_txn_true_for_hours_1_to_4(self, svc, training_df):
        for night_hour in [1, 2, 3, 4]:
            df = training_df.copy()
            df["txn_hour"] = night_hour
            result = svc.handle_missing_features(df)
            assert (result["is_night_txn"] == 1).all(), f"Hour {night_hour} should be night"

    def test_is_night_txn_false_for_hours_outside_1_to_4(self, svc, training_df):
        for day_hour in [0, 5, 12, 14, 22, 23]:
            df = training_df.copy()
            df["txn_hour"] = day_hour
            result = svc.handle_missing_features(df)
            assert (result["is_night_txn"] == 0).all(), f"Hour {day_hour} should not be night"

    def test_rows_without_customer_history_dropped(self, svc, training_df):
        df = training_df.copy()
        df.loc[:14, "f_customer_avg_txn_amount_90d"] = np.nan
        result = svc.handle_missing_features(df)
        assert len(result) == len(training_df) - 15

    def test_all_nulls_dropped(self, svc, training_df):
        df = training_df.copy()
        df["f_customer_avg_txn_amount_90d"] = np.nan
        result = svc.handle_missing_features(df)
        assert len(result) == 0

    def test_no_nulls_in_feature_cols_after_handling(self, svc, training_df):
        # Add some nulls in non-key feature cols
        df = training_df.copy()
        df.loc[0, "f_stream_otp_failed_count_30m"] = np.nan
        df.loc[1, "f_stream_decline_count_30m"]    = np.nan
        result = svc.handle_missing_features(df)
        null_counts = result[FEATURE_COLS].isnull().sum()
        assert null_counts.sum() == 0

    def test_original_dataframe_not_mutated(self, svc, training_df):
        original_cols = set(training_df.columns)
        svc.handle_missing_features(training_df)
        assert set(training_df.columns) == original_cols
        assert "txn_amount_ratio" not in training_df.columns

    def test_rows_with_customer_history_kept(self, svc, training_df):
        df = training_df.copy()
        # Set all rows to have history — none should be dropped
        df["f_customer_avg_txn_amount_90d"] = 200.0
        result = svc.handle_missing_features(df)
        assert len(result) == len(df)

    def test_all_feature_cols_present_in_output(self, svc, training_df):
        result = svc.handle_missing_features(training_df)
        for col in FEATURE_COLS:
            assert col in result.columns, f"Feature column {col} missing from output"
