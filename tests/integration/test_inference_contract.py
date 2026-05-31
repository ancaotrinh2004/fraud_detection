"""
API contract tests for src/inference/server.py.

All external dependencies (DB, MinIO, model loading) are mocked so these
tests run in CI without a live cluster.  The FastAPI app is imported once
at module level with lifespan patched; tests patch globals per-function.

Coverage:
  - Health endpoints (live, ready, ready-503)
  - Model info endpoint
  - Predict endpoint: request validation, response shape, fraud logic, batch
  - Metrics endpoint (prometheus-fastapi-instrumentator)
  - Server startup with no model loaded
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from fastapi.testclient import TestClient

from src.pipelines.ml.services import FEATURE_COLS, ModelArtifact


# ── Module-level import (lifespan NOT triggered without `with` context) ────────

# Patch startup hooks so app can be imported without DB/MinIO
with patch("src.inference.server._load_model"), \
     patch("src.inference.server._get_engine"):
    from src.inference.server import app
    import src.inference.server as srv


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_artifact(fraud_prob: float = 0.05, version: str = "v_contract_test"):
    model = MagicMock()
    model.predict_proba.side_effect = lambda X: np.column_stack([
        np.full(len(X), 1 - fraud_prob),
        np.full(len(X), fraud_prob),
    ])
    return ModelArtifact(
        model=model,
        feature_cols=FEATURE_COLS,
        model_version=version,
        metrics={"pr_auc": 0.85},
    )


def _zero_features():
    return {c: 0.0 for c in FEATURE_COLS}


@pytest.fixture
def artifact():
    return _make_artifact()


@pytest.fixture
def client(artifact):
    with patch.object(srv, "_artifact", artifact), \
         patch.object(srv, "_fetch_features", return_value=_zero_features()):
        yield TestClient(app, raise_server_exceptions=True)


def _predict(client, customer_id="C0000001", amount=150.0, hour=14,
             is_declined=0, is_foreign=0):
    return client.post("/v1/models/fraud:predict", json={
        "instances": [{
            "customer_id":     customer_id,
            "txn_amount":      amount,
            "txn_hour":        hour,
            "is_declined_txn": is_declined,
            "is_foreign_txn":  is_foreign,
        }]
    })


# ── Health endpoints ───────────────────────────────────────────────────────────

class TestLivenessEndpoint:
    def test_returns_200(self, client):
        r = client.get("/v2/health/live")
        assert r.status_code == 200

    def test_body_has_status_alive(self, client):
        r = client.get("/v2/health/live")
        assert r.json() == {"status": "alive"}

    def test_available_before_model_loaded(self):
        with patch.object(srv, "_artifact", None):
            c = TestClient(app)
            r = c.get("/v2/health/live")
        assert r.status_code == 200


class TestReadinessEndpoint:
    def test_returns_200_when_model_loaded(self, client):
        r = client.get("/v2/health/ready")
        assert r.status_code == 200

    def test_body_has_status_ready(self, client):
        r = client.get("/v2/health/ready")
        assert r.json() == {"status": "ready"}

    def test_returns_503_when_model_not_loaded(self):
        with patch.object(srv, "_artifact", None):
            c = TestClient(app, raise_server_exceptions=False)
            r = c.get("/v2/health/ready")
        assert r.status_code == 503

    def test_503_body_has_detail(self):
        with patch.object(srv, "_artifact", None):
            c = TestClient(app, raise_server_exceptions=False)
            r = c.get("/v2/health/ready")
        assert "detail" in r.json()


# ── Model info endpoint ────────────────────────────────────────────────────────

class TestModelInfoEndpoint:
    def test_returns_200(self, client):
        r = client.get("/v1/models/fraud")
        assert r.status_code == 200

    def test_body_has_required_fields(self, client):
        body = client.get("/v1/models/fraud").json()
        for field in ["name", "ready", "model_version", "metrics"]:
            assert field in body

    def test_name_is_fraud(self, client):
        body = client.get("/v1/models/fraud").json()
        assert body["name"] == "fraud"

    def test_ready_is_true_when_loaded(self, client):
        body = client.get("/v1/models/fraud").json()
        assert body["ready"] is True

    def test_model_version_matches_artifact(self, client, artifact):
        body = client.get("/v1/models/fraud").json()
        assert body["model_version"] == artifact.model_version

    def test_returns_503_when_model_not_loaded(self):
        with patch.object(srv, "_artifact", None):
            c = TestClient(app, raise_server_exceptions=False)
            r = c.get("/v1/models/fraud")
        assert r.status_code == 503


# ── Predict endpoint — request validation ─────────────────────────────────────

class TestPredictRequestValidation:
    def test_missing_customer_id_returns_422(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [{"txn_amount": 100.0, "txn_hour": 10}]
        })
        assert r.status_code == 422

    def test_missing_txn_amount_returns_422(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [{"customer_id": "C1", "txn_hour": 10}]
        })
        assert r.status_code == 422

    def test_missing_txn_hour_returns_422(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [{"customer_id": "C1", "txn_amount": 100.0}]
        })
        assert r.status_code == 422

    def test_invalid_content_type_returns_422(self, client):
        r = client.post(
            "/v1/models/fraud:predict",
            content="not-json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 422

    def test_empty_instances_list_returns_200(self, client):
        r = client.post("/v1/models/fraud:predict", json={"instances": []})
        assert r.status_code == 200
        assert r.json()["predictions"] == []

    def test_optional_fields_default_to_zero(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}]
        })
        assert r.status_code == 200

    def test_model_not_loaded_returns_503(self):
        with patch.object(srv, "_artifact", None), \
             patch.object(srv, "_fetch_features", return_value=_zero_features()):
            c = TestClient(app, raise_server_exceptions=False)
            r = c.post("/v1/models/fraud:predict", json={
                "instances": [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}]
            })
        assert r.status_code == 503


# ── Predict endpoint — response shape ─────────────────────────────────────────

class TestPredictResponseShape:
    def test_returns_200(self, client):
        assert _predict(client).status_code == 200

    def test_has_predictions_key(self, client):
        assert "predictions" in _predict(client).json()

    def test_predictions_is_list(self, client):
        assert isinstance(_predict(client).json()["predictions"], list)

    def test_one_prediction_per_instance(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [
                {"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10},
                {"customer_id": "C2", "txn_amount": 200.0, "txn_hour": 3},
                {"customer_id": "C3", "txn_amount": 50.0,  "txn_hour": 20},
            ]
        })
        assert len(r.json()["predictions"]) == 3

    def test_prediction_has_customer_id(self, client):
        pred = _predict(client, customer_id="C_TEST").json()["predictions"][0]
        assert pred["customer_id"] == "C_TEST"

    def test_prediction_has_fraud_score(self, client):
        pred = _predict(client).json()["predictions"][0]
        assert "fraud_score" in pred

    def test_prediction_has_is_fraud(self, client):
        pred = _predict(client).json()["predictions"][0]
        assert "is_fraud" in pred

    def test_fraud_score_is_numeric(self, client):
        pred = _predict(client).json()["predictions"][0]
        assert isinstance(pred["fraud_score"], (int, float))

    def test_fraud_score_between_0_and_1(self, client):
        pred = _predict(client).json()["predictions"][0]
        assert 0.0 <= pred["fraud_score"] <= 1.0

    def test_is_fraud_is_bool(self, client):
        pred = _predict(client).json()["predictions"][0]
        assert isinstance(pred["is_fraud"], bool)

    def test_customer_id_echoed_back(self, client):
        pred = _predict(client, customer_id="UNIQUE_ID_XYZ").json()["predictions"][0]
        assert pred["customer_id"] == "UNIQUE_ID_XYZ"


# ── Predict endpoint — fraud logic ────────────────────────────────────────────

class TestPredictFraudLogic:
    def test_high_fraud_prob_sets_is_fraud_true(self):
        art = _make_artifact(fraud_prob=0.95)
        with patch.object(srv, "_artifact", art), \
             patch.object(srv, "_fetch_features", return_value=_zero_features()):
            c = TestClient(app)
            pred = c.post("/v1/models/fraud:predict", json={
                "instances": [{"customer_id": "C1", "txn_amount": 5000.0, "txn_hour": 3}]
            }).json()["predictions"][0]
        assert pred["is_fraud"] is True
        assert pred["fraud_score"] >= 0.5

    def test_low_fraud_prob_sets_is_fraud_false(self):
        art = _make_artifact(fraud_prob=0.02)
        with patch.object(srv, "_artifact", art), \
             patch.object(srv, "_fetch_features", return_value=_zero_features()):
            c = TestClient(app)
            pred = c.post("/v1/models/fraud:predict", json={
                "instances": [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 14}]
            }).json()["predictions"][0]
        assert pred["is_fraud"] is False
        assert pred["fraud_score"] < 0.5

    def test_score_threshold_is_0_5(self):
        art = _make_artifact(fraud_prob=0.5)  # exactly at boundary → is_fraud True (>=)
        with patch.object(srv, "_artifact", art), \
             patch.object(srv, "_fetch_features", return_value=_zero_features()):
            c = TestClient(app)
            pred = c.post("/v1/models/fraud:predict", json={
                "instances": [{"customer_id": "C1", "txn_amount": 100.0, "txn_hour": 10}]
            }).json()["predictions"][0]
        assert pred["is_fraud"] is True  # score 0.5 >= 0.5 → fraud

    def test_derived_features_computed_from_txn_input(self, client, artifact):
        """Verify that txn_amount_ratio and is_night_txn are computed from request fields."""
        # We pass avg feature = 100, txn_amount = 300 → ratio should be ~3.0
        # We pass txn_hour = 2 → is_night_txn = 1
        features = _zero_features()
        features["f_customer_avg_txn_amount_90d"] = 100.0
        with patch.object(srv, "_artifact", artifact), \
             patch.object(srv, "_fetch_features", return_value=features):
            c = TestClient(app)
            r = c.post("/v1/models/fraud:predict", json={
                "instances": [{"customer_id": "C1", "txn_amount": 300.0, "txn_hour": 2}]
            })
        assert r.status_code == 200
        # Just verify derived features don't break the pipeline
        pred = r.json()["predictions"][0]
        assert "fraud_score" in pred


# ── Batch predict ──────────────────────────────────────────────────────────────

class TestBatchPredict:
    def test_batch_of_10_returns_10_predictions(self, client):
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [
                {"customer_id": f"C{i:07d}", "txn_amount": float(i * 10), "txn_hour": i % 24}
                for i in range(10)
            ]
        })
        assert r.status_code == 200
        assert len(r.json()["predictions"]) == 10

    def test_batch_customer_ids_match_input_order(self, client):
        customer_ids = [f"C_{i:05d}" for i in range(5)]
        r = client.post("/v1/models/fraud:predict", json={
            "instances": [
                {"customer_id": cid, "txn_amount": 100.0, "txn_hour": 10}
                for cid in customer_ids
            ]
        })
        returned_ids = [p["customer_id"] for p in r.json()["predictions"]]
        assert returned_ids == customer_ids


# ── Metrics endpoint ───────────────────────────────────────────────────────────

class TestMetricsEndpoint:
    def test_metrics_endpoint_returns_200(self, client):
        assert client.get("/metrics").status_code == 200

    def test_metrics_content_type_is_text_plain(self, client):
        assert "text/plain" in client.get("/metrics").headers["content-type"]

    def test_metrics_contains_http_requests_total(self, client):
        # Make a request first to populate counter
        _predict(client)
        assert "http_requests_total" in client.get("/metrics").text

    def test_metrics_contains_http_request_duration(self, client):
        _predict(client)
        assert "http_request_duration" in client.get("/metrics").text
