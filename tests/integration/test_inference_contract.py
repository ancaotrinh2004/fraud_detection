"""
Contract tests for src/inference/server.py — the KServe FraudModel.

The server was migrated from a FastAPI app to a KServe custom-container model
(open-inference-protocol V2). These tests drive FraudModel.predict() directly
with V2 payloads; all external dependencies (DB, MLflow, OTel) are mocked.

`kserve` is stubbed in sys.modules so the module imports without the (heavy)
KServe runtime — the server only needs kserve.Model as a base class here.

Coverage:
  - _extract_instances: V2 dict payload and InferRequest object
  - predict: response shape, batch, fraud logic, derived features
  - measure_latency decorator: passthrough + structured latency log
"""

import json
import sys
import types
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


# ── Stub kserve before importing the server ────────────────────────────────────

if "kserve" not in sys.modules:
    _kserve = types.ModuleType("kserve")

    class _StubModel:
        def __init__(self, name):
            self.name = name
            self.ready = False

    _kserve.Model = _StubModel
    _kserve.ModelServer = MagicMock()
    sys.modules["kserve"] = _kserve


from src.pipelines.ml.services import FEATURE_COLS, ModelArtifact
from src.inference.server import FraudModel, measure_latency, MODEL_NAME, SCORE_THRESH


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_artifact(fraud_prob: float = 0.05, version: str = "v_contract_test"):
    model = MagicMock()
    model.predict_proba.side_effect = lambda X: np.column_stack([
        np.full(len(X), 1 - fraud_prob),
        np.full(len(X), fraud_prob),
    ])
    return ModelArtifact(model=model, feature_cols=FEATURE_COLS,
                         model_version=version, metrics={"pr_auc": 0.85})


def _zero_features():
    return {c: 0.0 for c in FEATURE_COLS}


def _payload(instances: list) -> dict:
    """Build a KServe V2 infer request with JSON-encoded instances."""
    return {"inputs": [{
        "name":     "instances",
        "datatype": "BYTES",
        "shape":    [len(instances)],
        "data":     [json.dumps(i) for i in instances],
    }]}


@pytest.fixture
def model():
    m = FraudModel(MODEL_NAME)
    m._artifact = _make_artifact()
    m._engine   = MagicMock()
    m.ready     = True
    return m


def _predict(model, instances):
    with patch.object(model, "_fetch_features", return_value=_zero_features()):
        resp = model.predict(_payload(instances))
    preds = [json.loads(s) for s in resp["outputs"][0]["data"]]
    return resp, preds


# ── _extract_instances ─────────────────────────────────────────────────────────

class TestExtractInstances:
    def test_dict_payload_returns_data_list(self):
        data = FraudModel._extract_instances(_payload([{"a": 1}, {"a": 2}]))
        assert len(data) == 2

    def test_missing_inputs_key_returns_empty(self):
        assert FraudModel._extract_instances({}) == []

    def test_empty_inputs_returns_empty(self):
        assert FraudModel._extract_instances({"inputs": []}) == []

    def test_infer_request_object_returns_data(self):
        req = MagicMock()
        req.inputs = [MagicMock(data=["a", "b", "c"])]
        assert FraudModel._extract_instances(req) == ["a", "b", "c"]

    def test_infer_request_with_no_inputs_returns_empty(self):
        req = MagicMock()
        req.inputs = []
        assert FraudModel._extract_instances(req) == []


# ── predict — response shape ───────────────────────────────────────────────────

class TestPredictResponseShape:
    def test_model_name_in_response(self, model):
        resp, _ = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert resp["model_name"] == MODEL_NAME

    def test_has_request_id(self, model):
        resp, _ = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert resp["id"]

    def test_empty_instances_returns_empty_outputs(self, model):
        resp = model.predict(_payload([]))
        assert resp == {"outputs": []}

    def test_one_prediction_per_instance(self, model):
        _, preds = _predict(model, [
            {"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10},
            {"customer_id": "C2", "txn_amount": 200.0, "txn_hour": 3},
            {"customer_id": "C3", "txn_amount": 50.0,  "txn_hour": 20},
        ])
        assert len(preds) == 3

    def test_output_datatype_is_bytes(self, model):
        resp, _ = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert resp["outputs"][0]["datatype"] == "BYTES"

    def test_output_shape_matches_count(self, model):
        resp, _ = _predict(model, [
            {"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10},
            {"customer_id": "C2", "txn_amount": 100.0, "txn_hour": 10},
        ])
        assert resp["outputs"][0]["shape"] == [2]

    def test_prediction_has_required_fields(self, model):
        _, preds = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        for field in ("customer_id", "fraud_score", "is_fraud"):
            assert field in preds[0]

    def test_fraud_score_is_numeric_in_range(self, model):
        _, preds = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert isinstance(preds[0]["fraud_score"], (int, float))
        assert 0.0 <= preds[0]["fraud_score"] <= 1.0

    def test_is_fraud_is_bool(self, model):
        _, preds = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert isinstance(preds[0]["is_fraud"], bool)

    def test_customer_id_echoed(self, model):
        _, preds = _predict(model, [{"customer_id": "UNIQUE_XYZ", "txn_amount": 100.0, "txn_hour": 10}])
        assert preds[0]["customer_id"] == "UNIQUE_XYZ"

    def test_optional_fields_default_to_zero(self, model):
        # is_declined_txn / is_foreign_txn omitted → must still score
        _, preds = _predict(model, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert preds[0]["customer_id"] == "C1"

    def test_missing_required_field_raises(self, model):
        with patch.object(model, "_fetch_features", return_value=_zero_features()):
            with pytest.raises((KeyError, ValueError, TypeError)):
                model.predict(_payload([{"customer_id": "C1", "txn_amount": 100.0}]))  # no txn_hour


# ── predict — fraud logic ──────────────────────────────────────────────────────

class TestPredictFraudLogic:
    def test_high_prob_is_fraud_true(self):
        m = FraudModel(MODEL_NAME)
        m._artifact = _make_artifact(fraud_prob=0.95)
        _, preds = _predict(m, [{"customer_id": "C1", "txn_amount": 5000.0, "txn_hour": 3}])
        assert preds[0]["is_fraud"] is True
        assert preds[0]["fraud_score"] >= SCORE_THRESH

    def test_low_prob_is_fraud_false(self):
        m = FraudModel(MODEL_NAME)
        m._artifact = _make_artifact(fraud_prob=0.02)
        _, preds = _predict(m, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 14}])
        assert preds[0]["is_fraud"] is False
        assert preds[0]["fraud_score"] < SCORE_THRESH

    def test_threshold_boundary_is_fraud(self):
        m = FraudModel(MODEL_NAME)
        m._artifact = _make_artifact(fraud_prob=SCORE_THRESH)  # exactly 0.5 → >= → fraud
        _, preds = _predict(m, [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}])
        assert preds[0]["is_fraud"] is True


# ── predict — derived features ─────────────────────────────────────────────────

class TestDerivedFeatures:
    def test_txn_amount_ratio_and_night_flag_passed_to_scoring(self, model):
        feats = _zero_features()
        feats["f_customer_avg_txn_amount_90d"] = 100.0
        captured = {}

        def fake_score(artifact, features):
            captured.update(features)
            return {"fraud_score": 0.1, "model_version": "v"}

        with patch.object(model, "_fetch_features", return_value=feats), \
             patch.object(model._scoring, "score_online", side_effect=fake_score):
            model.predict(_payload([{"customer_id": "C1", "txn_amount": 300.0, "txn_hour": 2}]))

        assert captured["txn_amount_ratio"] == pytest.approx(3.0)  # 300 / 100
        assert captured["is_night_txn"] == 1                       # hour 2 ∈ [1,4]

    def test_ratio_zero_when_no_history(self, model):
        captured = {}

        def fake_score(artifact, features):
            captured.update(features)
            return {"fraud_score": 0.1, "model_version": "v"}

        with patch.object(model, "_fetch_features", return_value=_zero_features()), \
             patch.object(model._scoring, "score_online", side_effect=fake_score):
            model.predict(_payload([{"customer_id": "C1", "txn_amount": 300.0, "txn_hour": 14}]))

        assert captured["txn_amount_ratio"] == 0     # avg 0 → ratio 0
        assert captured["is_night_txn"] == 0


# ── batch ──────────────────────────────────────────────────────────────────────

class TestBatchPredict:
    def test_batch_of_10(self, model):
        _, preds = _predict(model, [
            {"customer_id": f"C{i:07d}", "txn_amount": float(i * 10), "txn_hour": i % 24}
            for i in range(10)
        ])
        assert len(preds) == 10

    def test_customer_ids_preserve_order(self, model):
        ids = [f"C_{i:05d}" for i in range(5)]
        _, preds = _predict(model, [
            {"customer_id": cid, "txn_amount": 100.0, "txn_hour": 10} for cid in ids
        ])
        assert [p["customer_id"] for p in preds] == ids


# ── measure_latency decorator ──────────────────────────────────────────────────

class TestMeasureLatency:
    def test_returns_result_unchanged(self):
        @measure_latency
        def handler(self):
            return {"id": "abc", "outputs": [{"shape": [3]}]}

        result = handler(object())
        assert result == {"id": "abc", "outputs": [{"shape": [3]}]}

    def test_logs_func_name_and_latency(self):
        with patch("src.inference.server.logger") as mock_logger:
            @measure_latency
            def handler(self):
                return {"id": "r1", "outputs": [{"shape": [2]}]}

            handler(object())

        mock_logger.info.assert_called_once()
        args, kwargs = mock_logger.info.call_args
        assert args[0] == "handler"
        assert "latency_ms" in kwargs["extra"]

    def test_derives_request_id_and_n_instances_from_response(self):
        with patch("src.inference.server.logger") as mock_logger:
            @measure_latency
            def handler(self):
                return {"id": "req-9", "outputs": [{"shape": [4]}]}

            handler(object())

        extra = mock_logger.info.call_args[1]["extra"]
        assert extra["request_id"] == "req-9"
        assert extra["n_instances"] == 4

    def test_tolerates_response_without_id_or_outputs(self):
        with patch("src.inference.server.logger") as mock_logger:
            @measure_latency
            def handler(self):
                return {"outputs": []}

            handler(object())

        extra = mock_logger.info.call_args[1]["extra"]
        assert "latency_ms" in extra
        assert "request_id" not in extra
        assert "n_instances" not in extra
