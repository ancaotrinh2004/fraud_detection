"""
Integration tests for src/pipelines/ml/batch_score.py — run_batch_score().
All external dependencies (DB, MinIO) are mocked.
Tests verify: happy path, no production model, empty DataFrame, error handling.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.pipelines.ml.services import FEATURE_COLS, ModelArtifact
from tests.conftest import _make_training_df


SCORE_MODULE = "src.pipelines.ml.batch_score"


@pytest.fixture
def score_df_raw():
    """DataFrame simulating the SQL query output in run_batch_score."""
    df = _make_training_df(50)
    return df


@pytest.fixture
def prod_model_info():
    return {
        "model_version": "v_20260101_120000",
        "pr_auc":        0.85,
        "artifact_path": "s3://models/fraud_xgb/v_20260101_120000/model.pkl",
    }


@pytest.fixture
def pipeline_mocks(mock_cfg, mock_engine, mock_artifact, score_df_raw, prod_model_info):
    registry_svc = MagicMock()
    registry_svc.get_production_version.return_value = prod_model_info

    model_svc = MagicMock()
    model_svc.load_model.return_value = mock_artifact

    scoring_svc = MagicMock()
    scored = pd.DataFrame({
        "transaction_id": score_df_raw["transaction_id"],
        "fraud_score":    np.full(len(score_df_raw), 0.05),
        "model_version":  mock_artifact.model_version,
        "score_ts":       datetime.now(),
    })
    scoring_svc.score_batch.return_value = scored

    return {
        "cfg":      mock_cfg,
        "engine":   mock_engine,
        "registry": registry_svc,
        "model":    model_svc,
        "scoring":  scoring_svc,
        "raw_df":   score_df_raw,
    }


def _run_with_mocks(mocks, read_sql_return=None):
    from src.pipelines.ml.batch_score import run_batch_score
    if read_sql_return is None:
        read_sql_return = mocks["raw_df"]
    with patch(f"{SCORE_MODULE}.load_pipeline_config", return_value=mocks["cfg"]), \
         patch(f"{SCORE_MODULE}.get_sqlalchemy_engine", return_value=mocks["engine"]), \
         patch(f"{SCORE_MODULE}.log_run") as mock_log, \
         patch(f"{SCORE_MODULE}.pd.read_sql", return_value=read_sql_return), \
         patch(f"{SCORE_MODULE}.ModelService", return_value=mocks["model"]), \
         patch(f"{SCORE_MODULE}.ModelRegistryService", return_value=mocks["registry"]), \
         patch(f"{SCORE_MODULE}.ScoringService", return_value=mocks["scoring"]):
        run_batch_score()
    return mock_log


class TestRunBatchScoreHappyPath:
    def test_loads_production_model(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].get_production_version.assert_called_once()

    def test_loads_model_artifact_from_registry(self, pipeline_mocks, prod_model_info):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["model"].load_model.assert_called_once_with(prod_model_info["artifact_path"])

    def test_calls_score_batch(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["scoring"].score_batch.assert_called_once()

    def test_calls_write_scores(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["scoring"].write_scores.assert_called_once()

    def test_log_run_called_with_success(self, pipeline_mocks):
        mock_log = _run_with_mocks(pipeline_mocks)
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "success"

    def test_score_batch_receives_loaded_artifact(self, pipeline_mocks, mock_artifact):
        _run_with_mocks(pipeline_mocks)
        artifact_arg = pipeline_mocks["scoring"].score_batch.call_args[0][0]
        assert artifact_arg is mock_artifact

    def test_write_scores_receives_score_df(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        score_df_arg = pipeline_mocks["scoring"].write_scores.call_args[0][0]
        assert isinstance(score_df_arg, pd.DataFrame)


class TestRunBatchScoreNoProductionModel:
    def test_returns_early_when_no_production_model(self, pipeline_mocks):
        pipeline_mocks["registry"].get_production_version.return_value = None
        _run_with_mocks(pipeline_mocks)
        # Should not attempt to load/score/write
        pipeline_mocks["model"].load_model.assert_not_called()
        pipeline_mocks["scoring"].score_batch.assert_not_called()
        pipeline_mocks["scoring"].write_scores.assert_not_called()


class TestRunBatchScoreEmptyDataFrame:
    def test_returns_early_when_no_transactions_to_score(self, pipeline_mocks, score_df_raw):
        empty_df = score_df_raw.iloc[:0].copy()
        _run_with_mocks(pipeline_mocks, read_sql_return=empty_df)
        pipeline_mocks["scoring"].score_batch.assert_not_called()
        pipeline_mocks["scoring"].write_scores.assert_not_called()


class TestRunBatchScoreErrorHandling:
    def test_exception_logged_as_failed(self, pipeline_mocks):
        pipeline_mocks["model"].load_model.side_effect = RuntimeError("MinIO unreachable")
        from src.pipelines.ml.batch_score import run_batch_score
        with patch(f"{SCORE_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{SCORE_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{SCORE_MODULE}.log_run") as mock_log, \
             patch(f"{SCORE_MODULE}.pd.read_sql", return_value=pipeline_mocks["raw_df"]), \
             patch(f"{SCORE_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{SCORE_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{SCORE_MODULE}.ScoringService", return_value=pipeline_mocks["scoring"]):
            with pytest.raises(RuntimeError):
                run_batch_score()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "failed"

    def test_exception_in_write_scores_re_raised(self, pipeline_mocks):
        pipeline_mocks["scoring"].write_scores.side_effect = Exception("DB write failed")
        with pytest.raises(Exception, match="DB write failed"):
            _run_with_mocks(pipeline_mocks)
