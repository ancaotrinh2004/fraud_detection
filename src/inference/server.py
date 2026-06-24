"""
src/inference/server.py

Fraud inference server using KServe Model SDK.
Deployed as KServe InferenceService custom container.

Inference protocol: KServe V2 (open-inference-protocol)
  POST /v2/models/fraud-predictor/infer

Input:
  {"inputs": [{"name": "instances", "datatype": "BYTES", "shape": [n],
               "data": ["<json-encoded TransactionInput>", ...]}]}

Output:
  {"model_name": "fraud-predictor",
   "outputs": [{"name": "predictions", "datatype": "BYTES", "shape": [n],
                "data": ["<json-encoded prediction>", ...]}]}

Observability:
  - Prometheus metrics: /metrics (built-in KServe)
  - Distributed tracing: OpenTelemetry → Jaeger (manual spans)
  - Structured JSON logging for Loki ingestion
"""

import functools
import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import yaml

import kserve

from src.pipelines.ml.services import (
    ModelService,
    ModelRegistryService,
    ScoringService,
)

# ── Structured JSON logging ───────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts":      self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        }
        for key in ("request_id", "customer_id", "fraud_score", "latency_ms", "n_instances"):
            if hasattr(record, key):
                doc[key] = getattr(record, key)
        return json.dumps(doc)


_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

SCHEMA       = "gold_fraud"
SCORE_THRESH = 0.5
MODEL_NAME   = "fraud-predictor"
_CONFIG_PATH = Path(__file__).parents[2] / "configs" / "pipeline_config.yaml"

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


# ── OpenTelemetry helpers ─────────────────────────────────────────────────────

def _setup_tracing(endpoint: str) -> None:
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource

        resource  = Resource.create({SERVICE_NAME: MODEL_NAME})
        provider  = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing → {endpoint}")
    except ImportError:
        logger.warning("opentelemetry packages not installed — tracing disabled.")


def _get_tracer():
    try:
        from opentelemetry import trace
        return trace.get_tracer(MODEL_NAME)
    except ImportError:
        return None


@contextmanager
def _span(name: str, **attributes):
    """Open an OTel span if tracing is available, otherwise a no-op.

    Yields the span (or None) so the caller can set extra attributes without
    branching on whether tracing is enabled.
    """
    tracer = _get_tracer()
    if tracer is None:
        yield None
        return
    with tracer.start_as_current_span(name) as span:
        for key, value in attributes.items():
            span.set_attribute(key, value)
        yield span


# ── Latency decorator ──────────────────────────────────────────────────────────

def measure_latency(func):
    """Time a handler and emit a structured latency log line.

    Keeps timing concerns out of the handler body. Correlation fields
    (request_id, n_instances) are derived from the KServe-style response so the
    response stays the single source of truth.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        start  = time.perf_counter()
        result = func(self, *args, **kwargs)
        extra  = {"latency_ms": int((time.perf_counter() - start) * 1000)}

        if isinstance(result, dict):
            if result.get("id"):
                extra["request_id"] = result["id"]
            outputs = result.get("outputs") or []
            if outputs and outputs[0].get("shape"):
                extra["n_instances"] = outputs[0]["shape"][0]

        logger.info(func.__name__, extra=extra)
        return result
    return wrapper


# ── FraudModel ─────────────────────────────────────────────────────────────────

class FraudModel(kserve.Model):

    def __init__(self, name: str = MODEL_NAME):
        super().__init__(name)
        self._artifact   = None
        self._engine     = None
        self._scoring    = ScoringService()
        self._cfg        = None

    def load(self) -> bool:
        self._cfg    = self._load_config()
        self._engine = self._get_engine(self._cfg)

        jaeger_endpoint = os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://jaeger.fraud-infra.svc.cluster.local:4317",
        )
        _setup_tracing(jaeger_endpoint)

        registry = ModelRegistryService(self._cfg)
        prod = registry.get_production_version()
        if prod is None:
            raise RuntimeError("No production model found in MLflow registry.")

        model_svc      = ModelService(self._cfg)
        self._artifact = model_svc.load_model(prod["artifact_path"])

        logger.info(json.dumps({
            "message":       "Model loaded",
            "model_version": self._artifact.model_version,
            "pr_auc":        prod.get("pr_auc"),
        }))
        self.ready = True
        return self.ready

    @measure_latency
    def predict(self, payload, headers: dict | None = None) -> dict:
        instances = self._extract_instances(payload)
        if not instances:
            return {"outputs": []}

        request_id  = str(uuid.uuid4())[:8]
        predictions = [self._score_instance(raw) for raw in instances]

        # KServe >=0.13 InferenceResponse requires model_name and id fields.
        return {
            "model_name": MODEL_NAME,
            "id":         request_id,
            "outputs": [{
                "name":     "predictions",
                "shape":    [len(predictions)],
                "datatype": "BYTES",
                "data":     [json.dumps(p) for p in predictions],
            }]
        }

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_instances(payload) -> list:
        """Pull the instance list out of either an InferRequest or a raw dict.

        KServe >=0.13 passes an InferRequest object; older versions pass a dict.
        """
        if hasattr(payload, "inputs"):
            inputs = payload.inputs
            return list(inputs[0].data) if inputs else []
        inputs = payload.get("inputs", [])
        return inputs[0].get("data", []) if inputs else []

    def _score_instance(self, raw) -> dict:
        """Assemble features for one transaction and return its fraud prediction."""
        txn         = json.loads(raw) if isinstance(raw, str) else raw
        customer_id = str(txn["customer_id"])
        txn_amount  = float(txn["txn_amount"])
        txn_hour    = int(txn["txn_hour"])

        with _span("fetch_features", customer_id=customer_id):
            features = self._fetch_features(customer_id)

        features["txn_amount"]      = txn_amount
        features["txn_hour"]        = txn_hour

        # Derive these flags server-side from raw event attributes so the logic
        # matches the training definition exactly (ml_label.py) — the client sends
        # the raw transaction, not pre-computed flags, removing a train/serve skew
        # surface. Definitions must stay in sync with ml_label.py:
        #   is_declined_txn = (status == 'declined')
        #   is_foreign_txn  = ip_country and card_country present and differ (case-insensitive)
        status       = str(txn.get("transaction_status", txn.get("status", ""))).lower()
        ip_country   = txn.get("ip_country")
        card_country = txn.get("card_country")
        features["is_declined_txn"] = 1 if status == "declined" else 0
        features["is_foreign_txn"]  = (
            1 if ip_country and card_country
                 and str(ip_country).lower() != str(card_country).lower()
            else 0
        )

        avg_90d = features.get("f_customer_avg_txn_amount_90d") or 0
        features["txn_amount_ratio"] = txn_amount / avg_90d if avg_90d > 0 else 0
        features["is_night_txn"]     = 1 if txn_hour in range(1, 5) else 0

        with _span("score_model") as span:
            scored = self._scoring.score_online(self._artifact, features)
            if span is not None:
                span.set_attribute("fraud_score", scored["fraud_score"])

        return {
            "customer_id": customer_id,
            "fraud_score": scored["fraud_score"],
            "is_fraud":    scored["fraud_score"] >= SCORE_THRESH,
        }

    @staticmethod
    def _load_config() -> dict:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)

    @staticmethod
    def _get_engine(cfg: dict):
        from sqlalchemy import create_engine
        pg       = cfg["postgres"]
        password = os.environ.get("POSTGRES_PASSWORD", pg["password"])
        url      = f"postgresql://{pg['user']}:{password}@{pg['host']}:{pg['port']}/{pg['database']}"
        return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 30})

    def _fetch_features(self, customer_id: str) -> dict:
        from sqlalchemy import text
        with self._engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT {", ".join(_CUSTOMER_FEATURE_COLS)}
                FROM {SCHEMA}.feat_customer_unified
                WHERE customer_id = :cid
                ORDER BY event_ts DESC
                LIMIT 1
            """), {"cid": customer_id}).fetchone()

        if row is None:
            logger.warning(f"No features for customer {customer_id} — using zeros.")
            return {col: 0.0 for col in _CUSTOMER_FEATURE_COLS}

        return {col: float(val or 0) for col, val in zip(_CUSTOMER_FEATURE_COLS, row)}


if __name__ == "__main__":
    model = FraudModel(MODEL_NAME)
    # KServe >=0.13 checks model.ready BEFORE calling model.start().
    # Explicitly call load() here so model.ready=True before ModelServer registers it.
    model.load()
    kserve.ModelServer().start([model])
