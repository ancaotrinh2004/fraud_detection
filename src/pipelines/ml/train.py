"""
src/pipelines/ml/train.py

Training pipeline — uses the 5 service classes from services.py:
  TrainingDataService → ModelService → ModelRegistryService → MonitoringService
"""

import logging
import uuid
from datetime import datetime

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.metrics import push_metrics
from src.pipelines.ml.services import (
    TrainingDataService,
    ModelService,
    ModelRegistryService,
    MonitoringService,
)

logger   = logging.getLogger(__name__)
PIPELINE = "ml_train_pipeline"
MIN_PR_AUC = 0.70


def run_training(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.now()

    data_svc     = TrainingDataService()
    model_svc    = ModelService(cfg)
    registry_svc = ModelRegistryService(engine)
    monitor_svc  = MonitoringService(engine)

    logger.info(f"[{PIPELINE}] Starting...")

    try:
        # 1. Load + validate
        df = data_svc.read_training_table(engine)
        data_svc.validate_schema(df)
        df = data_svc.handle_missing_features(df)

        # 2. Split
        train_df, val_df, test_df = data_svc.split_by_time(df)

        # 3. Train
        model = model_svc.train(train_df)

        # 4. Evaluate
        metrics = model_svc.evaluate(model, test_df)
        monitor_svc.publish_model_metrics(metrics, model_version="pending")
        monitor_svc.trigger_alerts(metrics)

        if metrics["pr_auc"] < MIN_PR_AUC:
            raise ValueError(
                f"PR-AUC {metrics['pr_auc']} below acceptance threshold {MIN_PR_AUC}."
            )

        # 5. Save artifact
        model_version = f"v_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        artifact_path = model_svc.save_model(model, metrics, model_version)

        # 6. Register as candidate, then promote if new model is not worse than current production
        prod = registry_svc.get_production_version()
        prod_pr_auc = prod["pr_auc"] if prod else 0.0

        registry_svc.register(
            model_version=model_version,
            metrics=metrics,
            artifact_path=artifact_path,
            train_rows=len(train_df),
            status="candidate",
        )

        if metrics["pr_auc"] >= prod_pr_auc - 0.01:
            registry_svc.promote(model_version)
            logger.info(f"[{PIPELINE}] Promoted {model_version} to production.")
        else:
            logger.info(f"[{PIPELINE}] Registered {model_version} as candidate (PR-AUC {metrics['pr_auc']} < {prod_pr_auc - 0.01:.4f}).")

        log_run(PIPELINE, start_ts, datetime.now(), "success", len(df), 1, cfg=cfg)

        # ── Level 2: push model performance metrics to Pushgateway ──────────
        push_metrics(PIPELINE, {
            "fraud_model_pr_auc":        metrics.get("pr_auc"),
            "fraud_model_f1":            metrics.get("f1"),
            "fraud_model_precision":     metrics.get("precision"),
            "fraud_model_recall":        metrics.get("recall"),
            "fraud_model_train_rows":    len(train_df),
            "fraud_model_test_rows":     len(test_df),
        })

    except Exception as e:
        log_run(PIPELINE, start_ts, datetime.now(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE}] FAILED: {e}")
        raise
