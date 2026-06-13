"""
Unit tests for ModelService — MLflow fully mocked.

ModelService was migrated from boto3/MinIO pickle persistence to MLflow:
  - save_model() logs via mlflow.sklearn.log_model() and resolves the logged
    model's physical artifact location for the registry.
  - load_model() loads via mlflow.sklearn.load_model() from a models:/ or runs:/ URI.

Coverage: __init__, train, evaluate, save_model, load_model.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.pipelines.ml.services import (
    FEATURE_COLS,
    ModelArtifact,
    ModelService,
)
from tests.conftest import _make_training_df


@pytest.fixture
def svc(mock_cfg):
    # __init__ calls _set_mlflow_uri() → mlflow.set_tracking_uri(); patch to no-op.
    with patch("mlflow.set_tracking_uri"):
        return ModelService(mock_cfg)


@pytest.fixture
def train_df_processed():
    """Small training DataFrame with all FEATURE_COLS including derived ones."""
    df = _make_training_df(80)
    df["txn_amount_ratio"] = df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan).fillna(1)
    df["is_night_txn"]     = df["txn_hour"].between(1, 4).astype(int)
    return df


@contextmanager
def _mock_mlflow_logging(model_id="m-abc",
                         artifact_location="s3://mlflow-artifacts/1/models/m-abc/artifacts",
                         model_uri="models:/m-abc"):
    """Patch the MLflow logging calls save_model() makes within an active run."""
    info   = MagicMock(model_id=model_id, model_uri=model_uri)
    logged = MagicMock(artifact_location=artifact_location)
    with patch("mlflow.active_run", return_value=MagicMock()), \
         patch("mlflow.sklearn.log_model", return_value=info) as log_model, \
         patch("mlflow.get_logged_model", return_value=logged) as get_logged:
        yield {"log_model": log_model, "get_logged": get_logged, "info": info}


# ── __init__ ────────────────────────────────────────────────────────────────────

class TestInit:
    def test_classifier_defaults_to_none(self, mock_cfg):
        with patch("mlflow.set_tracking_uri"):
            svc = ModelService(mock_cfg)
        assert svc.classifier is None

    def test_injected_classifier_stored(self, mock_cfg):
        clf = MagicMock()
        with patch("mlflow.set_tracking_uri"):
            svc = ModelService(mock_cfg, classifier=clf)
        assert svc.classifier is clf

    def test_tracking_uri_set_from_cfg(self, mock_cfg):
        cfg = {**mock_cfg, "mlflow": {"tracking_uri": "http://mlflow:5000"}}
        with patch("mlflow.set_tracking_uri") as mock_set:
            ModelService(cfg)
        mock_set.assert_called_with("http://mlflow:5000")

    def test_model_name_from_cfg(self, mock_cfg):
        cfg = {**mock_cfg, "mlflow": {"model_name": "custom-model"}}
        with patch("mlflow.set_tracking_uri"):
            svc = ModelService(cfg)
        assert svc._model_name == "custom-model"

    def test_model_name_defaults_when_absent(self, mock_cfg):
        with patch("mlflow.set_tracking_uri"):
            svc = ModelService(mock_cfg)  # mock_cfg has no "mlflow" key
        assert svc._model_name == "fraud-xgboost"


# ── train ──────────────────────────────────────────────────────────────────────

class TestTrain:
    def test_calls_classifier_fit(self, svc, train_df_processed):
        clf = MagicMock()
        clf.get_model.return_value = MagicMock()
        svc.classifier = clf
        svc.train(train_df_processed)
        clf.fit.assert_called_once()

    def test_passes_correct_feature_matrix(self, svc, train_df_processed):
        clf = MagicMock()
        clf.get_model.return_value = MagicMock()
        svc.classifier = clf
        svc.train(train_df_processed)
        X_passed, y_passed = clf.fit.call_args[0]
        assert X_passed.shape == (len(train_df_processed), len(FEATURE_COLS))

    def test_passes_label_vector(self, svc, train_df_processed):
        clf = MagicMock()
        clf.get_model.return_value = MagicMock()
        svc.classifier = clf
        svc.train(train_df_processed)
        _, y_passed = clf.fit.call_args[0]
        np.testing.assert_array_equal(y_passed, train_df_processed["label"].values)

    def test_returns_model_from_get_model(self, svc, train_df_processed):
        model_obj = MagicMock(name="trained_model")
        clf = MagicMock()
        clf.get_model.return_value = model_obj
        svc.classifier = clf
        result = svc.train(train_df_processed)
        assert result is model_obj

    def test_auto_creates_xgboost_when_classifier_is_none(self, svc, train_df_processed):
        assert svc.classifier is None
        with patch("src.pipelines.ml.services.XGBoostClassifier") as MockXGB:
            mock_clf = MagicMock()
            mock_clf.get_model.return_value = MagicMock()
            MockXGB.return_value = mock_clf
            svc.train(train_df_processed)
        MockXGB.assert_called_once()

    def test_scale_pos_weight_computed_from_label_ratio(self, svc, train_df_processed):
        """Auto-created XGBoost should receive scale_pos_weight = neg/pos."""
        df = train_df_processed.copy()
        df["label"] = [0] * 60 + [1] * 20  # 60 neg, 20 pos → spw = 3.0
        with patch("src.pipelines.ml.services.XGBoostClassifier") as MockXGB:
            mock_clf = MagicMock()
            mock_clf.get_model.return_value = MagicMock()
            MockXGB.return_value = mock_clf
            svc.train(df)
        spw = MockXGB.call_args[1]["scale_pos_weight"]
        assert spw == pytest.approx(3.0)

    def test_scale_pos_weight_safe_when_no_positives(self, svc, train_df_processed):
        """If all labels are 0, spw = neg/max(0,1) = neg — should not ZeroDivisionError."""
        df = train_df_processed.copy()
        df["label"] = 0
        with patch("src.pipelines.ml.services.XGBoostClassifier") as MockXGB:
            mock_clf = MagicMock()
            mock_clf.get_model.return_value = MagicMock()
            MockXGB.return_value = mock_clf
            svc.train(df)  # must not raise
        MockXGB.assert_called_once()


# ── evaluate ─────────────────────────────────────────────────────────────────────

class TestEvaluate:
    @pytest.fixture
    def eval_df(self, train_df_processed):
        df = train_df_processed.copy()
        # Ensure both classes present for meaningful metrics
        df.loc[:19, "label"] = 0
        df.loc[20:29, "label"] = 1
        return df

    @pytest.fixture
    def mock_model_for_eval(self):
        model = MagicMock()
        model.predict_proba.side_effect = lambda X: np.column_stack([
            np.random.default_rng(0).uniform(0.1, 0.9, len(X)),
            np.random.default_rng(1).uniform(0.1, 0.9, len(X)),
        ])
        return model

    def test_returns_all_required_keys(self, svc, eval_df, mock_model_for_eval):
        metrics = svc.evaluate(mock_model_for_eval, eval_df)
        for key in ["pr_auc", "f1", "precision", "recall", "threshold"]:
            assert key in metrics

    def test_threshold_is_0_5(self, svc, eval_df, mock_model_for_eval):
        metrics = svc.evaluate(mock_model_for_eval, eval_df)
        assert metrics["threshold"] == 0.5

    def test_metrics_in_valid_range(self, svc, eval_df, mock_model_for_eval):
        metrics = svc.evaluate(mock_model_for_eval, eval_df)
        for key in ["pr_auc", "f1", "precision", "recall"]:
            assert 0.0 <= metrics[key] <= 1.0, f"{key}={metrics[key]} out of range"

    def test_metrics_rounded_to_4dp(self, svc, eval_df, mock_model_for_eval):
        metrics = svc.evaluate(mock_model_for_eval, eval_df)
        for key in ["pr_auc", "f1", "precision", "recall"]:
            assert metrics[key] == round(metrics[key], 4)

    def test_perfect_model_scores(self, svc, eval_df):
        """Model that always predicts class correctly → f1=1.0, precision=1.0, recall=1.0."""
        model = MagicMock()
        labels = eval_df["label"].values
        proba = np.column_stack([1 - labels, labels]).astype(float)
        model.predict_proba.return_value = proba
        metrics = svc.evaluate(model, eval_df)
        assert metrics["f1"] == pytest.approx(1.0, abs=0.01)
        assert metrics["precision"] == pytest.approx(1.0, abs=0.01)
        assert metrics["recall"] == pytest.approx(1.0, abs=0.01)

    def test_model_called_with_feature_matrix(self, svc, eval_df, mock_model_for_eval):
        svc.evaluate(mock_model_for_eval, eval_df)
        X_passed = mock_model_for_eval.predict_proba.call_args[0][0]
        assert X_passed.shape == (len(eval_df), len(FEATURE_COLS))


# ── save_model ────────────────────────────────────────────────────────────────────

class TestSaveModel:
    def test_returns_artifact_location(self, svc):
        with _mock_mlflow_logging(artifact_location="s3://bkt/models/m-1/artifacts"):
            uri = svc.save_model(MagicMock(), {"pr_auc": 0.85}, "v1")
        assert uri == "s3://bkt/models/m-1/artifacts"

    def test_logs_model_with_name_model(self, svc):
        model = MagicMock()
        with _mock_mlflow_logging() as m:
            svc.save_model(model, {}, "v1")
        kwargs = m["log_model"].call_args[1]
        assert kwargs["name"] == "model"
        assert kwargs["sk_model"] is model

    def test_resolves_artifact_via_logged_model(self, svc):
        with _mock_mlflow_logging(model_id="m-xyz") as m:
            svc.save_model(MagicMock(), {}, "v1")
        m["get_logged"].assert_called_once_with("m-xyz")

    def test_raises_when_no_active_run(self, svc):
        with patch("mlflow.active_run", return_value=None):
            with pytest.raises(RuntimeError, match="active mlflow.start_run"):
                svc.save_model(MagicMock(), {}, "v1")

    def test_falls_back_to_model_uri_when_logged_model_lookup_fails(self, svc):
        info = MagicMock(model_id="m-abc", model_uri="models:/m-abc")
        with patch("mlflow.active_run", return_value=MagicMock()), \
             patch("mlflow.sklearn.log_model", return_value=info), \
             patch("mlflow.get_logged_model", side_effect=RuntimeError("no logged model")):
            uri = svc.save_model(MagicMock(), {}, "v1")
        assert uri == "models:/m-abc"


# ── load_model ────────────────────────────────────────────────────────────────────

class TestLoadModel:
    @contextmanager
    def _mock_load(self, model=None):
        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.sklearn.load_model", return_value=model or MagicMock()) as load:
            yield load

    def test_returns_model_artifact(self, svc):
        with self._mock_load():
            result = svc.load_model("models:/fraud-xgboost/Production")
        assert isinstance(result, ModelArtifact)

    def test_loads_from_given_uri(self, svc):
        with self._mock_load() as load:
            svc.load_model("models:/fraud-xgboost/Production")
        load.assert_called_once_with("models:/fraud-xgboost/Production")

    def test_feature_cols_restored(self, svc):
        with self._mock_load():
            result = svc.load_model("models:/fraud-xgboost/Production")
        assert result.feature_cols == FEATURE_COLS

    def test_metrics_default_to_empty_dict(self, svc):
        with self._mock_load():
            result = svc.load_model("models:/fraud-xgboost/Production")
        assert result.metrics == {}

    def test_version_from_models_stage_uri(self, svc):
        with self._mock_load():
            result = svc.load_model("models:/fraud-xgboost/Production")
        assert result.model_version == "Production"

    def test_version_from_runs_uri_is_run_id_prefix(self, svc):
        with self._mock_load():
            result = svc.load_model("runs:/abcdef1234567890/model")
        assert result.model_version == "abcdef12"

    def test_version_unknown_for_other_uri(self, svc):
        with self._mock_load():
            result = svc.load_model("s3://some/random/path")
        assert result.model_version == "unknown"
