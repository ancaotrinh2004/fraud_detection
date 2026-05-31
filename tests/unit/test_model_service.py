"""
Unit tests for ModelService — boto3/MinIO fully mocked.
Coverage: __init__, _ensure_bucket, train, evaluate, save_model, load_model.
"""

import io
import pickle
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest

from src.pipelines.ml.services import (
    FEATURE_COLS,
    ModelArtifact,
    ModelService,
)
from tests.conftest import _make_training_df, make_pickled_artifact, _PicklableModel


@pytest.fixture
def mock_s3_factory(mock_s3):
    """Patch boto3.client to return our mock S3 object for all ModelService tests."""
    with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
        yield mock_s3


@pytest.fixture
def svc(mock_cfg, mock_s3_factory):
    return ModelService(mock_cfg)


@pytest.fixture
def train_df_processed():
    """Small training DataFrame with all FEATURE_COLS including derived ones."""
    df = _make_training_df(80)
    df["txn_amount_ratio"] = df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan).fillna(1)
    df["is_night_txn"]     = df["txn_hour"].between(1, 4).astype(int)
    return df


# ── __init__ / _ensure_bucket ──────────────────────────────────────────────────

class TestInit:
    def test_s3_client_created_with_correct_params(self, mock_cfg, mock_s3):
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3) as mock_client:
            ModelService(mock_cfg)
        mock_client.assert_called_once()
        kwargs = mock_client.call_args[1]
        assert kwargs["endpoint_url"] == mock_cfg["minio"]["endpoint_url"]
        assert kwargs["aws_access_key_id"] == mock_cfg["minio"]["access_key"]
        assert kwargs["aws_secret_access_key"] == mock_cfg["minio"]["secret_key"]

    def test_ensure_bucket_calls_list_buckets(self, mock_cfg, mock_s3):
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
            ModelService(mock_cfg)
        mock_s3.list_buckets.assert_called_once()

    def test_bucket_created_when_not_exists(self, mock_cfg, mock_s3):
        mock_s3.list_buckets.return_value = {"Buckets": []}  # no buckets
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
            ModelService(mock_cfg)
        mock_s3.create_bucket.assert_called_once_with(Bucket="models")

    def test_bucket_not_created_when_already_exists(self, mock_cfg, mock_s3):
        mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "models"}]}
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
            ModelService(mock_cfg)
        mock_s3.create_bucket.assert_not_called()

    def test_classifier_defaults_to_none(self, mock_cfg, mock_s3):
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
            svc = ModelService(mock_cfg)
        assert svc.classifier is None

    def test_injected_classifier_stored(self, mock_cfg, mock_s3):
        clf = MagicMock()
        with patch("src.pipelines.ml.services.boto3.client", return_value=mock_s3):
            svc = ModelService(mock_cfg, classifier=clf)
        assert svc.classifier is clf


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


# ── evaluate ───────────────────────────────────────────────────────────────────

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


# ── save_model ─────────────────────────────────────────────────────────────────

class TestSaveModel:
    # Use _PicklableModel (not MagicMock) because save_model pickles the model
    def test_returns_s3_path_string(self, svc, mock_s3_factory):
        path = svc.save_model(_PicklableModel(), {"pr_auc": 0.85}, "v_20260101_120000")
        assert path.startswith("s3://models/")

    def test_path_contains_model_version(self, svc, mock_s3_factory):
        version = "v_20260101_120000_abc"
        path = svc.save_model(_PicklableModel(), {}, version)
        assert version in path

    def test_s3_upload_called(self, svc, mock_s3_factory):
        svc.save_model(_PicklableModel(), {}, "v_test")
        mock_s3_factory.upload_fileobj.assert_called_once()

    def test_s3_key_format(self, svc, mock_s3_factory):
        version = "v_test_123"
        svc.save_model(_PicklableModel(), {}, version)
        args, _ = mock_s3_factory.upload_fileobj.call_args
        bucket, key = args[1], args[2]
        assert bucket == "models"
        assert f"fraud_xgb/{version}/model.pkl" == key

    def test_serialized_artifact_contains_model_and_feature_cols(self, svc, mock_s3_factory):
        metrics = {"pr_auc": 0.85}
        captured = {}

        def capture_upload(buf, bucket, key):
            buf.seek(0)
            captured["artifact"] = pickle.loads(buf.read())

        mock_s3_factory.upload_fileobj.side_effect = capture_upload
        svc.save_model(_PicklableModel(), metrics, "v_check")

        assert "model" in captured["artifact"]
        assert "feature_cols" in captured["artifact"]
        assert captured["artifact"]["feature_cols"] == FEATURE_COLS
        assert captured["artifact"]["metrics"] == metrics


# ── load_model ─────────────────────────────────────────────────────────────────

class TestLoadModel:
    # Use picklable_artifact (real model) — MagicMock is not picklable

    def test_returns_model_artifact(self, svc, mock_s3_factory, picklable_artifact):
        pickled = make_pickled_artifact(picklable_artifact)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(pickled)}
        result = svc.load_model("s3://models/fraud_xgb/v_20260101/model.pkl")
        assert isinstance(result, ModelArtifact)

    def test_model_version_extracted_from_path(self, svc, mock_s3_factory, picklable_artifact):
        pickled = make_pickled_artifact(picklable_artifact)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(pickled)}
        result = svc.load_model("s3://models/fraud_xgb/v_20260101_120000/model.pkl")
        assert result.model_version == "v_20260101_120000"

    def test_feature_cols_restored(self, svc, mock_s3_factory, picklable_artifact):
        pickled = make_pickled_artifact(picklable_artifact)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(pickled)}
        result = svc.load_model("s3://models/fraud_xgb/v_test/model.pkl")
        assert result.feature_cols == FEATURE_COLS

    def test_metrics_restored(self, svc, mock_s3_factory, picklable_artifact):
        pickled = make_pickled_artifact(picklable_artifact)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(pickled)}
        result = svc.load_model("s3://models/fraud_xgb/v_test/model.pkl")
        assert result.metrics == picklable_artifact.metrics

    def test_s3_get_object_called_with_correct_key(self, svc, mock_s3_factory, picklable_artifact):
        pickled = make_pickled_artifact(picklable_artifact)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(pickled)}
        svc.load_model("s3://models/fraud_xgb/v_test/model.pkl")
        mock_s3_factory.get_object.assert_called_once_with(
            Bucket="models", Key="fraud_xgb/v_test/model.pkl"
        )

    def test_missing_metrics_defaults_to_empty_dict(self, svc, mock_s3_factory):
        # Old artifact format without 'metrics' key — must use picklable model
        payload = {"model": _PicklableModel(), "feature_cols": FEATURE_COLS}
        buf = io.BytesIO()
        pickle.dump(payload, buf)
        mock_s3_factory.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}
        result = svc.load_model("s3://models/fraud_xgb/v_old/model.pkl")
        assert result.metrics == {}
