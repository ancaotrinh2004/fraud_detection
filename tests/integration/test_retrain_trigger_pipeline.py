"""
Integration tests for src/pipelines/ml/retrain_trigger.py — run_retrain_trigger().
All external dependencies are mocked.
Tests verify: no trigger, trigger fires + promoted, trigger fires + rollback.
"""

from unittest.mock import MagicMock, patch, call

import pytest


TRIGGER_MODULE = "src.pipelines.ml.retrain_trigger"


@pytest.fixture
def pipeline_mocks(mock_cfg, mock_engine):
    monitor_svc = MagicMock()
    monitor_svc.check_retrain_trigger.return_value = (False, "no trigger")

    registry_svc = MagicMock()
    registry_svc.get_production_version.return_value = {
        "model_version": "v_old_001",
        "pr_auc":        0.80,
        "artifact_path": "s3://models/v_old_001/model.pkl",
    }

    model_svc = MagicMock()

    return {
        "cfg":      mock_cfg,
        "engine":   mock_engine,
        "monitor":  monitor_svc,
        "registry": registry_svc,
        "model":    model_svc,
    }


def _run_with_mocks(mocks, run_training_side_effect=None):
    from src.pipelines.ml.retrain_trigger import run_retrain_trigger
    mock_run_training = MagicMock(side_effect=run_training_side_effect)
    with patch(f"{TRIGGER_MODULE}.load_pipeline_config", return_value=mocks["cfg"]), \
         patch(f"{TRIGGER_MODULE}.get_sqlalchemy_engine", return_value=mocks["engine"]), \
         patch(f"{TRIGGER_MODULE}.log_run") as mock_log, \
         patch(f"{TRIGGER_MODULE}.run_training", mock_run_training), \
         patch(f"{TRIGGER_MODULE}.ModelService", return_value=mocks["model"]), \
         patch(f"{TRIGGER_MODULE}.ModelRegistryService", return_value=mocks["registry"]), \
         patch(f"{TRIGGER_MODULE}.MonitoringService", return_value=mocks["monitor"]):
        run_retrain_trigger()
    return mock_log, mock_run_training


class TestNoTrigger:
    def test_checks_retrain_trigger(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["monitor"].check_retrain_trigger.assert_called_once()

    def test_does_not_call_run_training_when_no_trigger(self, pipeline_mocks):
        _, mock_train = _run_with_mocks(pipeline_mocks)
        mock_train.assert_not_called()

    def test_logs_success_when_no_trigger(self, pipeline_mocks):
        mock_log, _ = _run_with_mocks(pipeline_mocks)
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "success"

    def test_does_not_call_rollback_when_no_trigger(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].rollback.assert_not_called()


class TestTriggerFiresAndTrainSucceeds:
    @pytest.fixture(autouse=True)
    def set_trigger(self, pipeline_mocks):
        pipeline_mocks["monitor"].check_retrain_trigger.return_value = (
            True, "3 consecutive drift alert weeks (PSI > 0.025)"
        )
        # After retraining: registry returns a new production model with better PR-AUC
        new_prod = {"model_version": "v_new_002", "pr_auc": 0.88, "artifact_path": "s3://test"}
        pipeline_mocks["registry"].get_production_version.side_effect = [
            {"model_version": "v_old_001", "pr_auc": 0.80, "artifact_path": "s3://old"},  # before retrain
            new_prod,  # after retrain
        ]

    def test_calls_run_training_when_triggered(self, pipeline_mocks):
        _, mock_train = _run_with_mocks(pipeline_mocks)
        mock_train.assert_called_once()

    def test_run_training_called_with_cfg(self, pipeline_mocks):
        _, mock_train = _run_with_mocks(pipeline_mocks)
        mock_train.assert_called_once_with(cfg=pipeline_mocks["cfg"])

    def test_does_not_rollback_when_new_model_is_better(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].rollback.assert_not_called()

    def test_logs_success_after_successful_retrain(self, pipeline_mocks):
        mock_log, _ = _run_with_mocks(pipeline_mocks)
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "success"

    def test_gets_production_version_before_and_after_retrain(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        assert pipeline_mocks["registry"].get_production_version.call_count == 2


class TestTriggerFiresAndNewModelWorse:
    @pytest.fixture(autouse=True)
    def set_trigger_worse_model(self, pipeline_mocks):
        pipeline_mocks["monitor"].check_retrain_trigger.return_value = (
            True, "PSI drift triggered"
        )
        # New model has worse PR-AUC than current
        old_prod = {"model_version": "v_old_001", "pr_auc": 0.80, "artifact_path": "s3://old"}
        new_prod = {"model_version": "v_new_002", "pr_auc": 0.77, "artifact_path": "s3://new"}
        pipeline_mocks["registry"].get_production_version.side_effect = [old_prod, new_prod]

    def test_calls_rollback_when_new_model_not_better(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].rollback.assert_called_once_with("v_old_001")

    def test_rollback_uses_pre_retrain_version(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        rollback_version = pipeline_mocks["registry"].rollback.call_args[0][0]
        assert rollback_version == "v_old_001"


class TestTriggerFiresNoExistingModel:
    def test_no_rollback_when_no_previous_prod_model(self, pipeline_mocks):
        """First retrain: no previous production model → can't rollback."""
        pipeline_mocks["monitor"].check_retrain_trigger.return_value = (True, "triggered")
        pipeline_mocks["registry"].get_production_version.side_effect = [
            None,  # no current prod model before retrain
            {"model_version": "v_new", "pr_auc": 0.85, "artifact_path": "s3://new"},
        ]
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].rollback.assert_not_called()


class TestTriggerErrorHandling:
    def test_exception_in_run_training_logged_as_failed(self, pipeline_mocks):
        pipeline_mocks["monitor"].check_retrain_trigger.return_value = (True, "triggered")
        from src.pipelines.ml.retrain_trigger import run_retrain_trigger
        mock_train = MagicMock(side_effect=RuntimeError("Training OOM"))
        with patch(f"{TRIGGER_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{TRIGGER_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{TRIGGER_MODULE}.log_run") as mock_log, \
             patch(f"{TRIGGER_MODULE}.run_training", mock_train), \
             patch(f"{TRIGGER_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{TRIGGER_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{TRIGGER_MODULE}.MonitoringService", return_value=pipeline_mocks["monitor"]):
            with pytest.raises(RuntimeError):
                run_retrain_trigger()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "failed"

    def test_exception_in_run_training_re_raised(self, pipeline_mocks):
        pipeline_mocks["monitor"].check_retrain_trigger.return_value = (True, "triggered")
        with pytest.raises(RuntimeError, match="Training OOM"):
            _run_with_mocks(pipeline_mocks, run_training_side_effect=RuntimeError("Training OOM"))
