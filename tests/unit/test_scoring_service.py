"""
Unit tests for ScoringService — no external dependencies (DB/MinIO fully mocked).
Coverage: score_online, score_batch, write_scores.
"""

from datetime import datetime
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest

from src.pipelines.ml.services import (
    FEATURE_COLS,
    ModelArtifact,
    ScoringService,
)
from tests.conftest import _make_training_df


@pytest.fixture
def svc():
    return ScoringService()


@pytest.fixture
def batch_df():
    """DataFrame suitable for score_batch (includes transaction_id and all _DB_FEATURE_COLS)."""
    return _make_training_df(100)


# ── score_online ───────────────────────────────────────────────────────────────

class TestScoreOnline:
    def test_returns_fraud_score_key(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert "fraud_score" in result

    def test_returns_model_version_key(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert "model_version" in result

    def test_fraud_score_is_float(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert isinstance(result["fraud_score"], float)

    def test_fraud_score_between_0_and_1(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert 0.0 <= result["fraud_score"] <= 1.0

    def test_fraud_score_rounded_to_4_dp(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert result["fraud_score"] == round(result["fraud_score"], 4)

    def test_model_version_matches_artifact(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert result["model_version"] == mock_artifact.model_version

    def test_missing_features_default_to_zero(self, svc, mock_artifact):
        result = svc.score_online(mock_artifact, {})
        X = mock_artifact.model.predict_proba.call_args[0][0]
        assert np.all(X == 0)

    def test_partial_features_zero_fill_missing(self, svc, mock_artifact):
        partial = {"txn_amount": 200.0, "txn_hour": 10}
        svc.score_online(mock_artifact, partial)
        X = mock_artifact.model.predict_proba.call_args[0][0]
        txn_amount_idx = FEATURE_COLS.index("txn_amount")
        assert X[0, txn_amount_idx] == 200.0
        # All other feature cols not in partial should be 0
        for i, col in enumerate(FEATURE_COLS):
            if col not in partial:
                assert X[0, i] == 0.0

    def test_features_passed_in_correct_order(self, svc, mock_artifact, full_features):
        svc.score_online(mock_artifact, full_features)
        X = mock_artifact.model.predict_proba.call_args[0][0]
        for i, col in enumerate(FEATURE_COLS):
            expected = full_features.get(col, 0)
            assert X[0, i] == pytest.approx(expected), f"Feature {col} at index {i} mismatch"

    def test_model_called_once_with_shape_1_x_n_features(self, svc, mock_artifact, full_features):
        svc.score_online(mock_artifact, full_features)
        X = mock_artifact.model.predict_proba.call_args[0][0]
        assert X.shape == (1, len(FEATURE_COLS))

    def test_high_fraud_probability_returned(self, svc, high_risk_artifact, night_fraud_features):
        result = svc.score_online(high_risk_artifact, night_fraud_features)
        assert result["fraud_score"] >= 0.5

    def test_low_fraud_probability_returned(self, svc, low_risk_artifact, full_features):
        result = svc.score_online(low_risk_artifact, full_features)
        assert result["fraud_score"] < 0.5

    def test_returns_only_two_keys(self, svc, mock_artifact, full_features):
        result = svc.score_online(mock_artifact, full_features)
        assert set(result.keys()) == {"fraud_score", "model_version"}

    def test_different_artifacts_return_different_versions(self, svc, full_features):
        a1 = ModelArtifact(
            model=MagicMock(**{"predict_proba.return_value": np.array([[0.9, 0.1]])}),
            feature_cols=FEATURE_COLS, model_version="v_001", metrics={},
        )
        a2 = ModelArtifact(
            model=MagicMock(**{"predict_proba.return_value": np.array([[0.9, 0.1]])}),
            feature_cols=FEATURE_COLS, model_version="v_002", metrics={},
        )
        r1 = svc.score_online(a1, full_features)
        r2 = svc.score_online(a2, full_features)
        assert r1["model_version"] == "v_001"
        assert r2["model_version"] == "v_002"


# ── score_batch ────────────────────────────────────────────────────────────────

class TestScoreBatch:
    def test_returns_dataframe(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert isinstance(result, pd.DataFrame)

    def test_output_has_required_columns(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert set(result.columns) == {"transaction_id", "fraud_score", "model_version", "score_ts"}

    def test_output_row_count_matches_input(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert len(result) == len(batch_df)

    def test_transaction_ids_preserved(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        np.testing.assert_array_equal(result["transaction_id"].values, batch_df["transaction_id"].values)

    def test_model_version_in_output(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert (result["model_version"] == mock_artifact.model_version).all()

    def test_score_ts_is_datetime(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert pd.api.types.is_datetime64_any_dtype(result["score_ts"]) or isinstance(
            result["score_ts"].iloc[0], datetime
        )

    def test_fraud_scores_between_0_and_1(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert (result["fraud_score"] >= 0).all()
        assert (result["fraud_score"] <= 1).all()

    def test_fraud_scores_rounded_to_4dp(self, svc, mock_artifact, batch_df):
        result = svc.score_batch(mock_artifact, batch_df)
        assert (result["fraud_score"] == result["fraud_score"].round(4)).all()

    def test_chunking_does_not_change_row_count(self, svc, mock_artifact, batch_df):
        result_default = svc.score_batch(mock_artifact, batch_df, chunk_size=5000)
        mock_artifact.model.predict_proba.reset_mock()
        result_small   = svc.score_batch(mock_artifact, batch_df, chunk_size=10)
        assert len(result_default) == len(result_small)

    def test_model_called_per_chunk(self, svc, mock_artifact, batch_df):
        chunk_size = 20
        n_expected_calls = (len(batch_df) + chunk_size - 1) // chunk_size
        mock_artifact.model.predict_proba.reset_mock()
        svc.score_batch(mock_artifact, batch_df, chunk_size=chunk_size)
        assert mock_artifact.model.predict_proba.call_count == n_expected_calls

    def test_derived_features_computed_by_score_batch(self, svc, mock_artifact, batch_df):
        """score_batch must compute txn_amount_ratio and is_night_txn internally."""
        # If it doesn't compute them, KeyError will be raised when accessing FEATURE_COLS
        # because those derived cols are not in batch_df
        result = svc.score_batch(mock_artifact, batch_df)
        assert len(result) == len(batch_df)  # no exception means derived features were built

    def test_original_dataframe_not_mutated(self, svc, mock_artifact, batch_df):
        original_cols = set(batch_df.columns)
        svc.score_batch(mock_artifact, batch_df)
        assert set(batch_df.columns) == original_cols
        assert "txn_amount_ratio" not in batch_df.columns

    def test_handles_single_row_input(self, svc, mock_artifact, batch_df):
        single_row = batch_df.iloc[:1].copy()
        result = svc.score_batch(mock_artifact, single_row)
        assert len(result) == 1

    def test_nulls_in_feature_cols_filled_with_zero(self, svc, mock_artifact, batch_df):
        df = batch_df.copy()
        df.loc[0, "f_stream_otp_failed_count_30m"] = np.nan
        result = svc.score_batch(mock_artifact, df)
        assert len(result) == len(df)  # must not raise


# ── write_scores ───────────────────────────────────────────────────────────────

class TestWriteScores:
    @pytest.fixture
    def score_df(self, mock_artifact, batch_df):
        svc = ScoringService()
        return svc.score_batch(mock_artifact, batch_df)

    def test_executes_upsert_sql(self, svc, mock_engine, score_df):
        svc.write_scores(score_df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.assert_called()

    def test_upsert_sql_contains_on_conflict(self, svc, mock_engine, score_df):
        svc.write_scores(score_df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        sql = str(conn.execute.call_args[0][0])
        assert "ON CONFLICT" in sql

    def test_all_rows_written_in_chunks(self, svc, mock_engine):
        """101 rows with chunk_size 5000 → 1 execute call (single chunk)."""
        n = 101
        df = pd.DataFrame({
            "transaction_id": [f"T{i}" for i in range(n)],
            "fraud_score":    np.full(n, 0.05),
            "model_version":  "v_test",
            "score_ts":       datetime.now(),
        })
        svc.write_scores(df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        assert conn.execute.call_count == 1  # all 101 fit in one 5000-row chunk

    def test_large_df_split_across_multiple_chunks(self, svc, mock_engine):
        """10001 rows → 3 chunks at default chunk_size=5000."""
        n = 10001
        df = pd.DataFrame({
            "transaction_id": [f"T{i}" for i in range(n)],
            "fraud_score":    np.full(n, 0.05),
            "model_version":  "v_test",
            "score_ts":       datetime.now(),
        })
        svc.write_scores(df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        # 10001 rows / 5000 = 3 chunks
        assert conn.execute.call_count == 3

    def test_passes_records_as_list_of_dicts(self, svc, mock_engine, score_df):
        svc.write_scores(score_df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        records_passed = conn.execute.call_args[0][1]
        assert isinstance(records_passed, list)
        assert isinstance(records_passed[0], dict)

    def test_idempotent_upsert_sql_targets_correct_table(self, svc, mock_engine, score_df):
        svc.write_scores(score_df, mock_engine)
        conn = mock_engine.begin.return_value.__enter__.return_value
        sql = str(conn.execute.call_args[0][0])
        assert "ml_fraud_scores" in sql
