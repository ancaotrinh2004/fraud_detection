"""
Unit tests for classifier Strategy implementations.
XGBoostClassifier and LogisticRegressionClassifier are tested with
real training on tiny (40-sample) synthetic datasets to verify end-to-end behavior.
BaseClassifier is verified to raise NotImplementedError for all abstract methods.
"""

import numpy as np
import pytest

from src.pipelines.ml.services import (
    BaseClassifier,
    LogisticRegressionClassifier,
    XGBoostClassifier,
)


# ── Shared data fixture (module scope — created once) ─────────────────────────

@pytest.fixture(scope="module")
def tiny_dataset():
    """40 samples, 17 features (matching FEATURE_COLS length), binary labels."""
    rng = np.random.default_rng(0)
    X = rng.uniform(0, 1, (40, 17)).astype(np.float32)
    y = rng.integers(0, 2, 40)
    return X, y


# ── Fitted classifier fixtures (class scope — fit once per test class) ─────────

@pytest.fixture(scope="class")
def fitted_xgb(tiny_dataset):
    """XGBoostClassifier trained once, shared across all tests in the class."""
    X, y = tiny_dataset
    clf = XGBoostClassifier()
    clf.fit(X, y)
    return clf, X


@pytest.fixture(scope="class")
def fitted_lr(tiny_dataset):
    """LogisticRegressionClassifier trained once, shared across all tests in the class."""
    X, y = tiny_dataset
    clf = LogisticRegressionClassifier()
    clf.fit(X, y)
    return clf, X


# ── BaseClassifier ─────────────────────────────────────────────────────────────

class TestBaseClassifier:
    def test_fit_raises_not_implemented(self):
        clf = BaseClassifier()
        with pytest.raises(NotImplementedError):
            clf.fit(np.array([[1]]), np.array([0]))

    def test_predict_proba_raises_not_implemented(self):
        clf = BaseClassifier()
        with pytest.raises(NotImplementedError):
            clf.predict_proba(np.array([[1]]))

    def test_get_model_raises_not_implemented(self):
        clf = BaseClassifier()
        with pytest.raises(NotImplementedError):
            clf.get_model()


# ── XGBoostClassifier ─────────────────────────────────────────────────────────

class TestXGBoostClassifier:
    def test_can_be_instantiated(self):
        clf = XGBoostClassifier(scale_pos_weight=9.0)
        assert clf is not None

    def test_default_scale_pos_weight_is_49(self):
        clf = XGBoostClassifier()
        assert clf._model.scale_pos_weight == 49.0

    def test_custom_scale_pos_weight(self):
        clf = XGBoostClassifier(scale_pos_weight=3.0)
        assert clf._model.scale_pos_weight == 3.0

    def test_fit_runs_without_error(self, tiny_dataset):
        X, y = tiny_dataset
        clf = XGBoostClassifier()
        clf.fit(X, y)  # must not raise

    def test_predict_proba_returns_2d_array(self, fitted_xgb):
        clf, X = fitted_xgb
        assert clf.predict_proba(X).ndim == 2

    def test_predict_proba_shape(self, fitted_xgb):
        clf, X = fitted_xgb
        assert clf.predict_proba(X).shape == (len(X), 2)

    def test_predict_proba_rows_sum_to_1(self, fitted_xgb):
        clf, X = fitted_xgb
        np.testing.assert_allclose(clf.predict_proba(X).sum(axis=1), 1.0, atol=1e-5)

    def test_predict_proba_values_in_0_1(self, fitted_xgb):
        clf, X = fitted_xgb
        proba = clf.predict_proba(X)
        assert (proba >= 0).all() and (proba <= 1).all()

    def test_get_model_returns_underlying_xgb_model(self, fitted_xgb):
        clf, _ = fitted_xgb
        assert clf.get_model() is clf._model

    def test_get_model_has_predict_proba(self, fitted_xgb):
        clf, _ = fitted_xgb
        assert hasattr(clf.get_model(), "predict_proba")


# ── LogisticRegressionClassifier ──────────────────────────────────────────────

class TestLogisticRegressionClassifier:
    def test_can_be_instantiated(self):
        clf = LogisticRegressionClassifier()
        assert clf is not None

    def test_default_class_weight_is_balanced(self):
        clf = LogisticRegressionClassifier()
        assert clf._model.named_steps["clf"].class_weight == "balanced"

    def test_pipeline_has_scaler_and_clf_steps(self):
        clf = LogisticRegressionClassifier()
        assert "scaler" in clf._model.named_steps
        assert "clf"    in clf._model.named_steps

    def test_custom_class_weight(self):
        clf = LogisticRegressionClassifier(class_weight=None)
        assert clf._model.named_steps["clf"].class_weight is None

    def test_fit_runs_without_error(self, tiny_dataset):
        X, y = tiny_dataset
        clf = LogisticRegressionClassifier()
        clf.fit(X, y)  # must not raise

    def test_predict_proba_returns_2d_array(self, fitted_lr):
        clf, X = fitted_lr
        assert clf.predict_proba(X).ndim == 2

    def test_predict_proba_shape(self, fitted_lr):
        clf, X = fitted_lr
        assert clf.predict_proba(X).shape == (len(X), 2)

    def test_predict_proba_rows_sum_to_1(self, fitted_lr):
        clf, X = fitted_lr
        np.testing.assert_allclose(clf.predict_proba(X).sum(axis=1), 1.0, atol=1e-5)

    def test_predict_proba_values_in_0_1(self, fitted_lr):
        clf, X = fitted_lr
        proba = clf.predict_proba(X)
        assert (proba >= 0).all() and (proba <= 1).all()

    def test_get_model_returns_sklearn_pipeline(self, fitted_lr):
        clf, _ = fitted_lr
        assert clf.get_model() is clf._model
