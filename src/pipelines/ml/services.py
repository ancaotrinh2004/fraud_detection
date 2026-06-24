"""
src/pipelines/ml/services.py

Five core ML service classes:
  - TrainingDataService
  - ModelService        (MLflow artifact logging/loading)
  - ModelRegistryService (MLflow Model Registry — replaces PostgreSQL ml_model_registry)
  - ScoringService
  - MonitoringService   (Discord alerts)
"""

import logging
from datetime import datetime
from typing import NamedTuple

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score, f1_score, precision_score, recall_score,
)
from sqlalchemy import text

logger = logging.getLogger(__name__)

SCHEMA       = "gold_fraud"
MODEL_NAME   = "fraud-xgboost"
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
    "txn_amount_ratio",  # txn_amount / avg_90d
    "is_night_txn",      # hour 1–4
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
            f"SELECT * FROM {SCHEMA}.ml_fraud_training ORDER BY event_ts",
            engine,
        )
        df["event_ts"] = pd.to_datetime(df["event_ts"])
        logger.info(f"[TrainingDataService] Loaded {len(df):,} rows.")
        return df

    def validate_schema(self, df: pd.DataFrame) -> None:
        missing = [c for c in _DB_FEATURE_COLS + ["label", "event_ts"] if c not in df.columns]
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
        sorted_ts = df["event_ts"].sort_values()
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

        train = df[df["event_ts"] <  train_cut]
        val   = df[(df["event_ts"] >= train_cut) & (df["event_ts"] < val_cut)]
        test  = df[df["event_ts"] >= val_cut]
        logger.info(
            f"[TrainingDataService] Split — train:{len(train):,} (<{train_cut.date()}) "
            f"val:{len(val):,} test:{len(test):,} (>={val_cut.date()})"
        )
        return train, val, test

    def handle_missing_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        has_cust_history = df["f_customer_avg_txn_amount_90d"].notna()
        dropped = int((~has_cust_history).sum())
        if dropped:
            logger.info(f"[TrainingDataService] Dropped {dropped:,} rows lacking customer feature snapshot.")
        df = df[has_cust_history].copy()

        df["txn_amount_ratio"] = (
            df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan)
        ).fillna(0)
        df["is_night_txn"] = df["txn_hour"].between(1, 4).astype(int)

        df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)
        return df


# ── ModelService ───────────────────────────────────────────────────────────────

class ModelService:
    """Handles training, evaluation, and MLflow artifact logging/loading."""

    def __init__(self, cfg: dict, classifier: BaseClassifier | None = None):
        self.classifier = classifier
        mlflow_cfg = cfg.get("mlflow", {})
        self._tracking_uri = mlflow_cfg.get("tracking_uri", "http://localhost:5000")
        self._model_name   = mlflow_cfg.get("model_name", MODEL_NAME)
        self._set_mlflow_uri()

    def _set_mlflow_uri(self) -> None:
        import mlflow
        mlflow.set_tracking_uri(self._tracking_uri)

    def train(self, train_df: pd.DataFrame) -> object:
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
        """Log model artifact to the active MLflow run. Returns artifact URI for registry."""
        import mlflow
        import mlflow.sklearn
        active = mlflow.active_run()
        if active is None:
            raise RuntimeError("save_model() must be called within an active mlflow.start_run() context.")

        # MLflow 3.x stores Logged Model artifacts at models/m-<id>/artifacts/ under the
        # artifact root, NOT at <run_id>/artifacts/model/. ModelInfo no longer exposes
        # `artifact_uri`; resolve the physical storage location via the LoggedModel so
        # the registry's create_model_version() gets a real S3 source path (not a
        # "models:/m-..." logical URI, which 500s on the SQLite-backed server).
        model_info = mlflow.sklearn.log_model(sk_model=model, name="model")

        artifact_uri = None
        model_id = getattr(model_info, "model_id", None)
        if model_id:
            try:
                logged_model = mlflow.get_logged_model(model_id)
                artifact_uri = logged_model.artifact_location
            except Exception as e:  # pragma: no cover - version/shape fallback
                logger.warning(f"[ModelService] get_logged_model({model_id}) failed: {e}")
        if not artifact_uri:
            # Fallback for older MLflow or unexpected ModelInfo shapes.
            artifact_uri = getattr(model_info, "model_uri", None) or getattr(
                model_info, "artifact_path", None
            )

        logger.info(f"[ModelService] Model logged → {artifact_uri}")
        return artifact_uri

    def load_model(self, artifact_path: str) -> ModelArtifact:
        """Load model from MLflow registry URI or run URI."""
        import mlflow.sklearn
        self._set_mlflow_uri()
        model = mlflow.sklearn.load_model(artifact_path)

        # Derive a readable version label from the URI
        if artifact_path.startswith("models:/"):
            parts = artifact_path.replace("models:/", "").split("/")
            version = parts[1] if len(parts) > 1 else "unknown"
        elif artifact_path.startswith("runs:/"):
            version = artifact_path.split("/")[1][:8]
        else:
            version = "unknown"

        return ModelArtifact(
            model=model,
            feature_cols=FEATURE_COLS,
            model_version=version,
            metrics={},
        )


# ── ModelRegistryService ───────────────────────────────────────────────────────

class ModelRegistryService:
    """MLflow Model Registry — replaces PostgreSQL ml_model_registry table."""

    def __init__(self, cfg: dict):
        import mlflow
        mlflow_cfg = cfg.get("mlflow", {})
        self._tracking_uri = mlflow_cfg.get("tracking_uri", "http://localhost:5000")
        self._model_name   = mlflow_cfg.get("model_name", MODEL_NAME)
        mlflow.set_tracking_uri(self._tracking_uri)
        self._client = mlflow.tracking.MlflowClient()

    def register(
        self,
        run_id: str,
        metrics: dict,
        artifact_path: str,
        train_rows: int,
        status: str = "candidate",
    ) -> str:
        """Register a logged model run to the MLflow Model Registry. Returns version string."""
        from mlflow.exceptions import MlflowException

        # Ensure registered model exists before creating a version
        try:
            self._client.create_registered_model(self._model_name)
        except MlflowException:
            pass  # Already exists

        # artifact_path is the actual S3 path returned by ModelService.save_model()
        # (e.g., s3://mlflow-artifacts/1/models/m-<id>/artifacts).
        # Using create_model_version() directly avoids register_model() which in MLflow 3.x
        # resolves to a "models:/m-..." Logged Model URI causing 500s on SQLite-backed server.
        source = artifact_path
        mv = self._client.create_model_version(
            name=self._model_name,
            source=source,
            run_id=run_id,
        )
        stage = "Staging" if status == "candidate" else "Production"
        self._client.transition_model_version_stage(
            name=self._model_name,
            version=mv.version,
            stage=stage,
            archive_existing_versions=False,
        )
        self._client.set_model_version_tag(self._model_name, mv.version, "pr_auc", str(metrics.get("pr_auc", 0)))
        self._client.set_model_version_tag(self._model_name, mv.version, "f1",     str(metrics.get("f1", 0)))
        self._client.set_model_version_tag(self._model_name, mv.version, "train_rows", str(train_rows))
        logger.info(f"[ModelRegistryService] Registered version {mv.version} as '{stage}'.")
        return str(mv.version)

    def get_production_version(self) -> dict | None:
        """Return {'model_version', 'pr_auc', 'artifact_path'} for the Production model, or None."""
        try:
            versions = self._client.get_latest_versions(self._model_name, stages=["Production"])
        except Exception:
            # Registered model doesn't exist yet (first training run)
            return None
        if not versions:
            return None
        v = versions[0]
        try:
            run = self._client.get_run(v.run_id)
            pr_auc = float(run.data.metrics.get("pr_auc", 0.0))
        except Exception:
            pr_auc = float(v.tags.get("pr_auc", 0.0))

        artifact_path = f"models:/{self._model_name}/Production"
        return {
            "model_version": v.version,
            "pr_auc":        pr_auc,
            "artifact_path": artifact_path,
        }

    def promote(self, model_version: str) -> None:
        """Transition the given version to Production, archiving all previous Production versions."""
        self._client.transition_model_version_stage(
            name=self._model_name,
            version=str(model_version),
            stage="Production",
            archive_existing_versions=True,
        )
        logger.info(f"[ModelRegistryService] Promoted version {model_version} to Production.")

    def rollback(self, previous_version: str) -> None:
        self.promote(previous_version)
        logger.info(f"[ModelRegistryService] Rolled back to version {previous_version}.")


# ── ScoringService ─────────────────────────────────────────────────────────────

class ScoringService:
    """Batch and online scoring against a loaded ModelArtifact."""

    def score_batch(self, artifact: ModelArtifact, df: pd.DataFrame,
                    chunk_size: int = 5_000) -> pd.DataFrame:
        df = df.copy()
        df["txn_amount_ratio"] = (
            df["txn_amount"] / df["f_customer_avg_txn_amount_90d"].replace(0, np.nan)
        ).fillna(0)
        df["is_night_txn"] = df["txn_hour"].between(1, 4).astype(int)

        model = artifact.model
        all_scores = []
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            X = chunk[artifact.feature_cols].fillna(0).values
            scores = model.predict_proba(X)[:, 1]
            all_scores.append(scores)
        return pd.DataFrame({
            "transaction_id": df["transaction_id"].values,
            "fraud_score":    np.round(np.concatenate(all_scores), 4),
            "model_version":  artifact.model_version,
            "score_ts":       datetime.now(),
        })

    def write_scores(self, score_df: pd.DataFrame, engine) -> None:
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
        X = np.array([[features.get(c, 0) for c in artifact.feature_cols]])
        score = float(artifact.model.predict_proba(X)[0, 1])
        return {
            "fraud_score":   round(score, 4),
            "model_version": artifact.model_version,
        }


# ── MonitoringService ──────────────────────────────────────────────────────────

class MonitoringService:
    """Publishes model metrics, checks retrain triggers, sends Discord alerts."""

    def __init__(self, engine, cfg: dict | None = None):
        self._engine = engine
        self._webhook_url = (cfg or {}).get("alerting", {}).get("discord_webhook_url", "")

    def publish_model_metrics(self, metrics: dict, model_version: str) -> None:
        logger.info(
            f"[MonitoringService] Metrics for {model_version}: "
            + ", ".join(f"{k}={v}" for k, v in metrics.items())
        )

    def check_retrain_trigger(
        self,
        psi_threshold: float = 0.1,
        consecutive_weeks: int = 3,
        f1_drop_threshold: float = 0.05,
    ) -> tuple[bool, str]:
        """Returns (should_retrain, reason). Checks consecutive drift alert weeks."""
        with self._engine.connect() as conn:
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
        from src.pipelines.utils.alerting import send_discord_alert
        if metrics.get("pr_auc", 1.0) < 0.70:
            logger.error(f"[MonitoringService] ALERT: PR-AUC={metrics['pr_auc']} below 0.70!")
            send_discord_alert(
                title="ALERT: Model PR-AUC below threshold",
                message=(
                    f"**PR-AUC**: {metrics['pr_auc']:.4f} (threshold: 0.70)\n"
                    f"**F1**: {metrics.get('f1', 'N/A')}\n"
                    f"**Precision**: {metrics.get('precision', 'N/A')}"
                ),
                level="critical",
                webhook_url=self._webhook_url,
            )
        if metrics.get("f1", 1.0) < 0.30:
            logger.warning(f"[MonitoringService] ALERT: F1={metrics['f1']} below 0.30!")
            send_discord_alert(
                title="WARNING: Low F1 score after training",
                message=f"**F1**: {metrics['f1']:.4f} (threshold: 0.30)",
                level="warning",
                webhook_url=self._webhook_url,
            )
