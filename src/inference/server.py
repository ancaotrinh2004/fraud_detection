"""
src/inference/server.py

Fraud inference server — FastAPI, deployed as KServe InferenceService custom container.
POST /v1/models/fraud:predict

Client sends transaction data → server fetches features from feat_customer_unified
→ computes derived features → scores model → returns fraud_score.
"""

import functools
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from src.pipelines.ml.services import (
    ModelService,
    ModelRegistryService,
    ScoringService,
    ModelArtifact,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA = "gold_fraud"
SCORE_THRESH = 0.5
_CONFIG_PATH = Path(__file__).parents[2] / "config" / "pipeline_config.yaml"

_artifact: ModelArtifact | None = None
_scoring_svc = ScoringService()
_engine = None

_CUSTOMER_FEATURE_COLS = [
    "f_customer_total_txn_90d",
    "f_customer_avg_txn_amount_90d",
    "f_customer_distinct_merchants_90d",
    "f_customer_decline_rate_90d",
    "f_customer_foreign_txn_ratio_90d",
    "f_customer_night_txn_ratio_90d",
    "f_stream_otp_failed_count_30m",
    "f_stream_decline_count_30m",
    "f_stream_txn_velocity_1h",
    "f_stream_new_merchant_flag",
    "f_stream_burst_activity_flag",
]


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _get_engine(cfg: dict):
    pg = cfg["postgres"]
    password = os.environ.get("POSTGRES_PASSWORD", pg["password"])
    url = f"postgresql://{pg['user']}:{password}@{pg['host']}:{pg['port']}/{pg['database']}"
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 30})


def _fetch_features(customer_id: str) -> dict:
    """Point-in-time lookup: latest features for customer from feat_customer_unified."""
    with _engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT {", ".join(_CUSTOMER_FEATURE_COLS)}
            FROM {SCHEMA}.feat_customer_unified
            WHERE customer_id = :cid
            ORDER BY event_timestamp DESC
            LIMIT 1
        """), {"cid": customer_id}).fetchone()

    if row is None:
        # New/unknown customer — zero-fill
        logger.warning(f"No feature snapshot for customer {customer_id}, using zeros.")
        return {col: 0.0 for col in _CUSTOMER_FEATURE_COLS}

    return {col: float(val or 0) for col, val in zip(_CUSTOMER_FEATURE_COLS, row)}


def _load_model() -> None:
    global _artifact, _engine
    cfg = _load_config()
    _engine = _get_engine(cfg)

    registry_svc = ModelRegistryService(_engine)
    prod = registry_svc.get_production_version()
    if prod is None:
        raise RuntimeError("No production model found in registry.")

    model_svc = ModelService(cfg)
    _artifact = model_svc.load_model(prod["artifact_path"])
    logger.info(
        f"Model loaded: {_artifact.model_version} "
        f"(PR-AUC={_artifact.metrics.get('pr_auc')})"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="Fraud Inference Service", lifespan=lifespan)

Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
).instrument(app).expose(app, endpoint="/metrics")


# ── Health endpoints ──────────────────────────────────────────────────────────

@app.get("/v2/health/live")
def health_live():
    return {"status": "alive"}


@app.get("/v2/health/ready")
def health_ready():
    if _artifact is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")
    return {"status": "ready"}


@app.get("/v1/models/fraud")
def model_info():
    if _artifact is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")
    return {
        "name": "fraud",
        "ready": True,
        "model_version": _artifact.model_version,
        "metrics": _artifact.metrics,
    }


# ── Inference endpoint ────────────────────────────────────────────────────────

class TransactionInput(BaseModel):
    customer_id: str
    txn_amount: float
    txn_hour: int           # 0–23
    is_declined_txn: int = 0
    is_foreign_txn: int = 0


class PredictRequest(BaseModel):
    instances: list[TransactionInput]


def monitor_latency(func: Callable) -> Callable:
    """Decorator: logs inference latency for monitoring."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        result = func(*args, **kwargs)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        n = len(result.get("predictions", []))
        logger.info(f"[predict] {n} instance(s) scored in {latency_ms}ms")
        return result
    return wrapper


@app.post("/v1/models/fraud:predict")
@monitor_latency
def predict(request: PredictRequest):
    if _artifact is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    predictions = []
    for txn in request.instances:
        # 1. Fetch pre-computed features from feature store
        features = _fetch_features(txn.customer_id)

        # 2. Add transaction-level features
        features["txn_amount"]      = txn.txn_amount
        features["txn_hour"]        = txn.txn_hour
        features["is_declined_txn"] = txn.is_declined_txn
        features["is_foreign_txn"]  = txn.is_foreign_txn

        # 3. Compute derived features
        avg_90d = features.get("f_customer_avg_txn_amount_90d") or 0
        features["txn_amount_ratio"] = txn.txn_amount / avg_90d if avg_90d > 0 else 0
        features["is_night_txn"]     = 1 if txn.txn_hour in range(1, 5) else 0

        # 4. Score
        scored = _scoring_svc.score_online(_artifact, features)
        predictions.append({
            "customer_id": txn.customer_id,
            "fraud_score": scored["fraud_score"],
            "is_fraud":    scored["fraud_score"] >= SCORE_THRESH,
        })

    return {"predictions": predictions}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
