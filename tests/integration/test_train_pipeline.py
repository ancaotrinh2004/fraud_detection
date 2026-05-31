"""
Integration tests for src/pipelines/ml/train.py — run_training().
All external dependencies (DB, MinIO, Airflow) are mocked.
Tests verify the full pipeline control flow including:
  - Happy path: train → evaluate → register → promote
  - PR-AUC below threshold: pipeline aborts with ValueError
  - No existing production model (first run)
  - New model not better than current: registered as candidate, not promoted
  - Error in any step: log_run called with 'failed', exception re-raised
"""

from unittest.mock import MagicMock, patch

import pytest


TRAIN_MODULE = "src.pipelines.ml.train"

GOOD_METRICS   = {"pr_auc": 0.85, "f1": 0.72, "precision": 0.80, "recall": 0.65, "threshold": 0.5}
WEAK_METRICS   = {"pr_auc": 0.60, "f1": 0.40, "precision": 0.55, "recall": 0.45, "threshold": 0.5}


@pytest.fixture
def pipeline_mocks(mock_cfg, mock_engine, training_df):
    """Provide pre-wired mocks for all services used by run_training()."""
    splits = (
        training_df.iloc[:140].copy(),  # train
        training_df.iloc[140:170].copy(),  # val
        training_df.iloc[170:].copy(),   # test
    )

    data_svc = MagicMock()
    data_svc.read_training_table.return_value = training_df
    data_svc.validate_schema.return_value = None
    data_svc.handle_missing_features.return_value = training_df
    data_svc.split_by_time.return_value = splits

    model_svc = MagicMock()
    model_svc.train.return_value = MagicMock(name="trained_model")
    model_svc.evaluate.return_value = GOOD_METRICS.copy()
    model_svc.save_model.return_value = "s3://models/fraud_xgb/v_test/model.pkl"

    registry_svc = MagicMock()
    registry_svc.get_production_version.return_value = {"pr_auc": 0.80, "model_version": "v_old"}

    monitor_svc = MagicMock()

    return {
        "cfg":      mock_cfg,
        "engine":   mock_engine,
        "data":     data_svc,
        "model":    model_svc,
        "registry": registry_svc,
        "monitor":  monitor_svc,
    }


def _run_with_mocks(mocks):
    from src.pipelines.ml.train import run_training
    with patch(f"{TRAIN_MODULE}.load_pipeline_config", return_value=mocks["cfg"]), \
         patch(f"{TRAIN_MODULE}.get_sqlalchemy_engine", return_value=mocks["engine"]), \
         patch(f"{TRAIN_MODULE}.log_run"), \
         patch(f"{TRAIN_MODULE}.push_metrics"), \
         patch(f"{TRAIN_MODULE}.TrainingDataService", return_value=mocks["data"]), \
         patch(f"{TRAIN_MODULE}.ModelService", return_value=mocks["model"]), \
         patch(f"{TRAIN_MODULE}.ModelRegistryService", return_value=mocks["registry"]), \
         patch(f"{TRAIN_MODULE}.MonitoringService", return_value=mocks["monitor"]):
        run_training()


class TestRunTrainingHappyPath:
    def test_calls_read_training_table(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["data"].read_training_table.assert_called_once()

    def test_calls_validate_schema(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["data"].validate_schema.assert_called_once()

    def test_calls_handle_missing_features(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["data"].handle_missing_features.assert_called_once()

    def test_calls_split_by_time(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["data"].split_by_time.assert_called_once()

    def test_calls_model_train(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["model"].train.assert_called_once()

    def test_calls_model_evaluate(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["model"].evaluate.assert_called_once()

    def test_calls_publish_model_metrics(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["monitor"].publish_model_metrics.assert_called_once()

    def test_calls_trigger_alerts(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["monitor"].trigger_alerts.assert_called_once()

    def test_calls_save_model(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["model"].save_model.assert_called_once()

    def test_calls_register_as_candidate(self, pipeline_mocks):
        _run_with_mocks(pipeline_mocks)
        reg_call_kwargs = pipeline_mocks["registry"].register.call_args[1]
        assert reg_call_kwargs.get("status") == "candidate" or \
               pipeline_mocks["registry"].register.call_args[0][4] == "candidate" or \
               "candidate" in str(pipeline_mocks["registry"].register.call_args)

    def test_promotes_when_new_pr_auc_better_than_current(self, pipeline_mocks):
        """New PR-AUC=0.85 > current 0.80 - 0.01 = 0.79 → should promote."""
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].promote.assert_called_once()

    def test_calls_log_run_success(self, pipeline_mocks):
        from src.pipelines.ml.train import run_training
        with patch(f"{TRAIN_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{TRAIN_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{TRAIN_MODULE}.log_run") as mock_log, \
             patch(f"{TRAIN_MODULE}.push_metrics"), \
             patch(f"{TRAIN_MODULE}.TrainingDataService", return_value=pipeline_mocks["data"]), \
             patch(f"{TRAIN_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{TRAIN_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{TRAIN_MODULE}.MonitoringService", return_value=pipeline_mocks["monitor"]):
            run_training()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "success"

    def test_pushes_metrics_after_success(self, pipeline_mocks):
        from src.pipelines.ml.train import run_training
        with patch(f"{TRAIN_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{TRAIN_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{TRAIN_MODULE}.log_run"), \
             patch(f"{TRAIN_MODULE}.push_metrics") as mock_push, \
             patch(f"{TRAIN_MODULE}.TrainingDataService", return_value=pipeline_mocks["data"]), \
             patch(f"{TRAIN_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{TRAIN_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{TRAIN_MODULE}.MonitoringService", return_value=pipeline_mocks["monitor"]):
            run_training()
        mock_push.assert_called_once()
        pushed_metrics = mock_push.call_args[0][1]
        assert "fraud_model_pr_auc" in pushed_metrics


class TestRunTrainingPrAucBelowThreshold:
    def test_raises_value_error_when_pr_auc_below_0_70(self, pipeline_mocks):
        pipeline_mocks["model"].evaluate.return_value = WEAK_METRICS.copy()
        with pytest.raises(ValueError, match="PR-AUC"):
            _run_with_mocks(pipeline_mocks)

    def test_does_not_save_model_when_pr_auc_below_threshold(self, pipeline_mocks):
        pipeline_mocks["model"].evaluate.return_value = WEAK_METRICS.copy()
        with pytest.raises(ValueError):
            _run_with_mocks(pipeline_mocks)
        pipeline_mocks["model"].save_model.assert_not_called()

    def test_does_not_register_when_pr_auc_below_threshold(self, pipeline_mocks):
        pipeline_mocks["model"].evaluate.return_value = WEAK_METRICS.copy()
        with pytest.raises(ValueError):
            _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].register.assert_not_called()

    def test_logs_failure_when_pr_auc_below_threshold(self, pipeline_mocks):
        pipeline_mocks["model"].evaluate.return_value = WEAK_METRICS.copy()
        from src.pipelines.ml.train import run_training
        with patch(f"{TRAIN_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{TRAIN_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{TRAIN_MODULE}.log_run") as mock_log, \
             patch(f"{TRAIN_MODULE}.push_metrics"), \
             patch(f"{TRAIN_MODULE}.TrainingDataService", return_value=pipeline_mocks["data"]), \
             patch(f"{TRAIN_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{TRAIN_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{TRAIN_MODULE}.MonitoringService", return_value=pipeline_mocks["monitor"]):
            with pytest.raises(ValueError):
                run_training()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "failed"


class TestRunTrainingFirstRun:
    def test_no_prod_model_still_promotes(self, pipeline_mocks):
        """First run: no existing production model → new model should be promoted."""
        pipeline_mocks["registry"].get_production_version.return_value = None
        _run_with_mocks(pipeline_mocks)
        # pr_auc=0.85 >= 0.0 - 0.01 = -0.01 → promote
        pipeline_mocks["registry"].promote.assert_called_once()


class TestRunTrainingNewModelWorse:
    def test_does_not_promote_when_pr_auc_not_better(self, pipeline_mocks):
        """New PR-AUC=0.78, current=0.80 → 0.78 < 0.79 → should NOT promote."""
        pipeline_mocks["model"].evaluate.return_value = {
            "pr_auc": 0.78, "f1": 0.65, "precision": 0.70, "recall": 0.60, "threshold": 0.5
        }
        pipeline_mocks["registry"].get_production_version.return_value = {"pr_auc": 0.80, "model_version": "v_old"}
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].promote.assert_not_called()

    def test_registers_as_candidate_when_not_promoted(self, pipeline_mocks):
        pipeline_mocks["model"].evaluate.return_value = {
            "pr_auc": 0.78, "f1": 0.65, "precision": 0.70, "recall": 0.60, "threshold": 0.5
        }
        pipeline_mocks["registry"].get_production_version.return_value = {"pr_auc": 0.80, "model_version": "v_old"}
        _run_with_mocks(pipeline_mocks)
        pipeline_mocks["registry"].register.assert_called_once()


class TestRunTrainingErrorHandling:
    def test_exception_in_data_load_logged_as_failed(self, pipeline_mocks):
        pipeline_mocks["data"].read_training_table.side_effect = RuntimeError("DB unreachable")
        from src.pipelines.ml.train import run_training
        with patch(f"{TRAIN_MODULE}.load_pipeline_config", return_value=pipeline_mocks["cfg"]), \
             patch(f"{TRAIN_MODULE}.get_sqlalchemy_engine", return_value=pipeline_mocks["engine"]), \
             patch(f"{TRAIN_MODULE}.log_run") as mock_log, \
             patch(f"{TRAIN_MODULE}.push_metrics"), \
             patch(f"{TRAIN_MODULE}.TrainingDataService", return_value=pipeline_mocks["data"]), \
             patch(f"{TRAIN_MODULE}.ModelService", return_value=pipeline_mocks["model"]), \
             patch(f"{TRAIN_MODULE}.ModelRegistryService", return_value=pipeline_mocks["registry"]), \
             patch(f"{TRAIN_MODULE}.MonitoringService", return_value=pipeline_mocks["monitor"]):
            with pytest.raises(RuntimeError):
                run_training()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][3] == "failed"

    def test_exception_in_train_step_re_raised(self, pipeline_mocks):
        pipeline_mocks["model"].train.side_effect = MemoryError("OOM")
        with pytest.raises(MemoryError):
            _run_with_mocks(pipeline_mocks)
