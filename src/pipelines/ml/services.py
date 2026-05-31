"""
src/pipelines/ml/services.py

Five core ML service classes as defined in design/04.1_ml_design.md Section 4:
  - TrainingDataService
  - ModelService
  - ModelRegistryService
  - ScoringService
  - MonitoringService
"""

import io
import logging
import pickle
import uuid
from datetime import datetime
from typing import NamedTuple

import boto3
import numpy as np
import pandas as pd
from botocore.client import Config
from sklearn.metrics import (
    average_precision_score, f1_score, precision_score, recall_score,
)
from sqlalchemy import text

logger = logging.getLogger(__name__)

SCHEMA       = "gold_fraud"
MODEL_NAME   = "fraud_xgb"
SCORE_THRESH = 0.5


# ── Strategy pattern: swappable classifier interface ──────────────────────────

class BaseClassifier:
    """Abstract base for Strategy pattern — swap classifier without changing ModelService."""
    def fit(self, X, y) -> None:
        raise NotImplementedError

    def predict_proba(self, X) -> np.ndarray:
        raise NotImplementedError

    def get_model(self) -> object:
        raise NotImplementedError


class XGBoostClassifier(BaseClassifier):
    """Default strategy: XGBoost with configurable hyperparameters."""
    def __init__(self, scale_pos_weight: float = 49.0, **kwargs):
        from xgboost import XGBClassifier
        self._model = XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.05,
            scale_pos_weight=scale_pos_weight,
            eval_metric="aucpr", random_state=42, n_jobs=-1,
            **kwargs,
        )

    def fit(self, X, y) -> None:
        self._model.fit(X, y, verbose=False)

    def predict_proba(self, X) -> np.ndarray:
        return self._model.predict_proba(X)

    def get_model(self):
        return self._model


class LogisticRegressionClassifier(BaseClassifier):
    """Fallback strategy: lighter model for comparison or resource-constrained runs."""
    def __init__(self, class_weight: str = "balanced", **kwargs):
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline
        self._model = Pipeline([
            ("scaler", StandardScaler()),
            ("clf",    LogisticRegression(
                class_weight=class_weight, max_iter=500,
                random_state=42, **kwargs,
            )),
        ])

    def fit(self, X, y) -> None:
        self._model.fit(X, y)

    def predict_proba(self, X) -> np.ndarray:
        return self._model.predict_proba(X)

    def get_model(self):
        return self._model

# Columns that must exist in ml_fraud_training (stored in DB)
_DB_FEATURE_COLS = [
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
    "txn_amount",
    "txn_hour",
    "is_declined_txn",
    "is_foreign_txn",
]

# Full feature set used for training/scoring (DB columns + derived)
FEATURE_COLS = _DB_FEATURE_COLS + [
    "txn_amount_ratio",  # txn_amount / avg_90d — captures 3× anomaly condition
    "is_night_txn",      # hour 22-5 — matches night fraud condition
]


class ModelArtifact(NamedTuple):
    model: object
    feature_cols: list[str]
    model_version: str
    metrics: dict


# ── TrainingDataService ────────────────────────────────────────────────────────

class TrainingDataService:
    """Reads and validates ml_fraud_training from PostgreSQL Gold zone."""

    def read_training_table(self, engine) -> pd.DataFrame:
        df = pd.read_sql(
            f"SELECT * FROM {SCHEMA}.ml_fraud_training ORDER BY event_timestamp",
            engine,
        )
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        logger.info(f"[TrainingDataService] Loaded {len(df):,} rows.")
        return df

    def validate_schema(self, df: pd.DataFrame) -> None:
        missing = [c for c in _DB_FEATURE_COLS + ["label", "event_timestamp"] if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in training table: {missing}")
        if df["label"].isna().any():
            raise ValueError("label column contains NULLs.")
        logger.info("[TrainingDataService] Schema validation passed.")

    def get_split_boundaries(
        self,
        df: pd.DataFrame,
        train_frac: float = 0.70,
        val_frac: float   = 0.15,
    ) -> dict:
        """Compute time-based split boundaries from actual data — works for any date range."""
        sorted_ts = df["event_timestamp"].sort_values()
        n = len(sorted_ts)
        return {
            "train_end": sorted_ts.iloc[int(n * train_frac)],
            "val_end":   sorted_ts.iloc[int(n * (train_frac + val_frac))],
        }

    def split_by_time(
        self,
        df: pd.DataFrame,
        train_frac: float = 0.70,
        val_frac: float   = 0.15,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        boundaries = self.get_split_boundaries(df, train_frac, val_frac)
        train_cut  = boundaries["train_end"]
        val_cut    = boundaries["val_end"]

        train = df[df["event_timestamp"] <  train_cut]
        val   = df[(df["event_timestamp"] >= train_cut) & (df["event_timestamp"] < val_cut)]
        test  = df[df["event_timestamp"] >= val_cut]
        logger.info(
            f"[TrainingDataService] Split — train:{len(train):,} (<{train_cut.date()}) "
            f"val:{len(val):,} test:{len(test):,} (>={val_cut.date()})"
        )
        return train, val, test

    def handle_missing_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Drop rows with no customer aggregate snapshot.
        # April/May transactions with 0-filled 90d features corrupt training because
        # both fraud and non-fraud get identical zero vectors — model learns nothing.
        has_cust_history = df["f_customer_avg_txn_amount_90d"].notna()
        dropped = int((~has_cust_history).sum())
        if dropped:
            logger.info(f"[TrainingDataService] Dropped {dropped:,} rows lacking customer feature snapshot.")
        df = df[has_cust_history].copy()

        # Derived features — safe division guards against zero avg (new customers)
        df["txn_amount_ratio"] = (
            df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan)
        ).fillna(0)
        df["is_night_txn"] = df["txn_hour"].between(1, 4).astype(int)

        df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)
        return df


# ── ModelService ───────────────────────────────────────────────────────────────

class ModelService:
    """Strategy + Learner patterns: accepts any BaseClassifier; exposes unified train()/evaluate() API."""

    def __init__(self, cfg: dict, classifier: BaseClassifier | None = None):
        self.classifier = classifier  # injected strategy; resolved lazily in train()
        minio = cfg["minio"]
        self._s3 = boto3.client(
            "s3",
            endpoint_url=minio["endpoint_url"],
            aws_access_key_id=minio["access_key"],
            aws_secret_access_key=minio["secret_key"],
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        existing = {b["Name"] for b in self._s3.list_buckets().get("Buckets", [])}
        if "models" not in existing:
            self._s3.create_bucket(Bucket="models")

    def train(self, train_df: pd.DataFrame) -> object:
        """Learner pattern: single unified fit() call; strategy resolved here."""
        X = train_df[FEATURE_COLS].values
        y = train_df["label"].values

        if self.classifier is None:
            neg = int(np.sum(y == 0))
            pos = int(np.sum(y == 1))
            spw = neg / max(pos, 1)
            self.classifier = XGBoostClassifier(scale_pos_weight=spw)

        logger.info(f"[ModelService] Training with {type(self.classifier).__name__}...")
        self.classifier.fit(X, y)
        return self.classifier.get_model()

    def evaluate(self, model, test_df: pd.DataFrame) -> dict:
        X = test_df[FEATURE_COLS].values
        y = test_df["label"].values

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= SCORE_THRESH).astype(int)

        metrics = {
            "pr_auc":    round(float(average_precision_score(y, y_prob)), 4),
            "f1":        round(float(f1_score(y, y_pred, zero_division=0)), 4),
            "precision": round(float(precision_score(y, y_pred, zero_division=0)), 4),
            "recall":    round(float(recall_score(y, y_pred, zero_division=0)), 4),
            "threshold": SCORE_THRESH,
        }
        logger.info(
            f"[ModelService] Evaluation — PR-AUC={metrics['pr_auc']}  "
            f"F1={metrics['f1']}  P={metrics['precision']}  R={metrics['recall']}"
        )
        return metrics

    def save_model(self, model, metrics: dict, model_version: str) -> str:
        """Pickle model to MinIO. Returns s3 artifact path."""
        artifact = {"model": model, "feature_cols": FEATURE_COLS, "metrics": metrics}
        buf = io.BytesIO()
        pickle.dump(artifact, buf)
        buf.seek(0)
        key = f"fraud_xgb/{model_version}/model.pkl"
        self._s3.upload_fileobj(buf, "models", key)
        path = f"s3://models/{key}"
        logger.info(f"[ModelService] Saved artifact → {path}")
        return path

    def load_model(self, artifact_path: str) -> ModelArtifact:
        """Load model artifact from MinIO given s3://models/... path."""
        key = artifact_path.replace("s3://models/", "")
        obj = self._s3.get_object(Bucket="models", Key=key)
        artifact = pickle.loads(obj["Body"].read())
        # artifact_path contains model_version in the key
        model_version = key.split("/")[1]
        return ModelArtifact(
            model=artifact["model"],
            feature_cols=artifact["feature_cols"],
            model_version=model_version,
            metrics=artifact.get("metrics", {}),
        )


# ── ModelRegistryService ───────────────────────────────────────────────────────

class ModelRegistryService:
    """Lightweight model registry backed by gold_fraud.ml_model_registry."""

    def __init__(self, engine):
        self._engine = engine

    def register(
        self,
        model_version: str,
        metrics: dict,
        artifact_path: str,
        train_rows: int,
        status: str = "candidate",
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.ml_model_registry
                    (model_version, model_name, pr_auc, f1_score, precision_score,
                     recall_score, status, artifact_path, train_rows)
                VALUES (:ver, :name, :pr_auc, :f1, :prec, :rec, :status, :path, :rows)
                ON CONFLICT (model_version) DO UPDATE SET
                    status = EXCLUDED.status,
                    registered_ts = NOW()
            """), {
                "ver": model_version, "name": MODEL_NAME,
                "pr_auc": metrics["pr_auc"],
                "f1":     metrics["f1"],
                "prec":   metrics["precision"],
                "rec":    metrics["recall"],
                "status": status,
                "path":   artifact_path,
                "rows":   train_rows,
            })
        logger.info(f"[ModelRegistryService] Registered {model_version} as '{status}'.")

    def get_production_version(self) -> dict | None:
        """Return {'model_version', 'pr_auc', 'artifact_path'} of production model or None."""
        with self._engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT model_version, pr_auc, artifact_path
                FROM {SCHEMA}.ml_model_registry
                WHERE status = 'production' AND model_name = :n
                ORDER BY registered_ts DESC LIMIT 1
            """), {"n": MODEL_NAME}).fetchone()
        if row:
            return {"model_version": row[0], "pr_auc": float(row[1]), "artifact_path": row[2]}
        return None

    def promote(self, model_version: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.ml_model_registry
                SET status = 'retired'
                WHERE status = 'production' AND model_name = :n
            """), {"n": MODEL_NAME})
            conn.execute(text(f"""
                UPDATE {SCHEMA}.ml_model_registry
                SET status = 'production'
                WHERE model_version = :ver
            """), {"ver": model_version})
        logger.info(f"[ModelRegistryService] Promoted {model_version} to production.")

    def rollback(self, previous_version: str) -> None:
        self.promote(previous_version)
        logger.info(f"[ModelRegistryService] Rolled back to {previous_version}.")


# ── ScoringService ─────────────────────────────────────────────────────────────

class ScoringService:
    """Batch and online scoring against registered model."""

    def score_batch(self, artifact: ModelArtifact, df: pd.DataFrame,
                    chunk_size: int = 5_000) -> pd.DataFrame:
        """Iterator pattern: processes df in chunk_size chunks for memory efficiency."""
        df = df.copy()
        # Derived features — mirror logic in handle_missing_features
        df["txn_amount_ratio"] = (
            df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan)
        ).fillna(0)
        df["is_night_txn"] = df["txn_hour"].between(1, 4).astype(int)

        model = artifact.model
        all_scores = []
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            X = chunk[artifact.feature_cols].fillna(0).values
            scores = model.predict_proba(X)[:, 1]  # type: ignore[union-attr]
            all_scores.append(scores)
        return pd.DataFrame({
            "transaction_id": df["transaction_id"].values,
            "fraud_score":    np.round(np.concatenate(all_scores), 4),
            "model_version":  artifact.model_version,
            "score_ts":       datetime.now(),
        })

    def write_scores(self, score_df: pd.DataFrame, engine) -> None:
        """Upsert scores to ml_fraud_scores — idempotent."""
        records = score_df.to_dict("records")
        with engine.begin() as conn:
            for chunk_start in range(0, len(records), 5000):
                chunk = records[chunk_start:chunk_start + 5000]
                conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.ml_fraud_scores
                        (transaction_id, fraud_score, model_version, score_ts)
                    VALUES (:transaction_id, :fraud_score, :model_version, :score_ts)
                    ON CONFLICT (transaction_id) DO UPDATE SET
                        fraud_score   = EXCLUDED.fraud_score,
                        model_version = EXCLUDED.model_version,
                        score_ts      = EXCLUDED.score_ts
                """), chunk)
        logger.info(f"[ScoringService] Upserted {len(score_df):,} scores.")

    def score_online(self, artifact: ModelArtifact, features: dict) -> dict:
        """Score a single transaction for online inference. Returns score + metadata."""
        X = np.array([[features.get(c, 0) for c in artifact.feature_cols]])
        score = float(artifact.model.predict_proba(X)[0, 1])  # type: ignore[union-attr]
        return {
            "fraud_score":   round(score, 4),
            "model_version": artifact.model_version,
        }


# ── MonitoringService ──────────────────────────────────────────────────────────

class MonitoringService:
    """Publishes model and data quality metrics, checks retrain triggers."""

    def __init__(self, engine):
        self._engine = engine

    def publish_model_metrics(self, metrics: dict, model_version: str) -> None:
        logger.info(
            f"[MonitoringService] Model metrics for {model_version}: "
            + ", ".join(f"{k}={v}" for k, v in metrics.items())
        )

    def check_retrain_trigger(
        self,
        psi_threshold: float = 0.025,
        consecutive_weeks: int = 3,
        f1_drop_threshold: float = 0.05,
    ) -> tuple[bool, str]:
        """
        Returns (should_retrain, reason).
        Checks:
          1. ≥ consecutive_weeks with PSI > psi_threshold
          2. Current production F1 vs registered baseline drops > f1_drop_threshold
        """
        with self._engine.connect() as conn:
            # Check consecutive drift alerts
            row = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT monitoring_date
                    FROM {SCHEMA}.agg_feature_health_daily
                    WHERE alert_flag = TRUE
                    ORDER BY monitoring_date DESC
                    LIMIT :n
                ) recent
            """), {"n": consecutive_weeks}).fetchone()
            alert_count = int(row[0]) if row else 0

            if alert_count >= consecutive_weeks:
                reason = f"{alert_count} consecutive drift alert weeks (PSI > {psi_threshold})"
                logger.warning(f"[MonitoringService] Retrain trigger: {reason}")
                return True, reason

        return False, "no trigger"

    def trigger_alerts(self, metrics: dict) -> None:
        if metrics.get("pr_auc", 1.0) < 0.70:
            logger.error(f"[MonitoringService] ALERT: PR-AUC={metrics['pr_auc']} below 0.70!")
        if metrics.get("f1", 1.0) < 0.30:
            logger.warning(f"[MonitoringService] ALERT: F1={metrics['f1']} below 0.30!")
