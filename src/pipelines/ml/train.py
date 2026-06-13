"""
src/pipelines/ml/train.py

Training pipeline — uses the service classes from services.py.
Model experiments tracked in MLflow; production version registered in MLflow Model Registry.
"""

import logging
from datetime import datetime

import mlflow
import mlflow.sklearn

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.metrics import push_metrics
from src.pipelines.utils.alerting import send_discord_alert
from src.pipelines.utils.contract_validator import validate_contract
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

    mlflow_cfg      = cfg.get("mlflow", {})
    tracking_uri    = mlflow_cfg.get("tracking_uri", "http://localhost:5000")
    experiment_name = mlflow_cfg.get("experiment_name", "fraud-detection")
    webhook_url     = cfg.get("alerting", {}).get("discord_webhook_url", "")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.now()

    data_svc     = TrainingDataService()
    model_svc    = ModelService(cfg)
    registry_svc = ModelRegistryService(cfg)
    monitor_svc  = MonitoringService(engine, cfg)

    logger.info(f"[{PIPELINE}] Starting...")

    try:
        # 1. Load + validate
        df = data_svc.read_training_table(engine)
        data_svc.validate_schema(df)
        validate_contract("ml_fraud_training", df)
        df = data_svc.handle_missing_features(df)

        # 2. Split
        train_df, val_df, test_df = data_svc.split_by_time(df)

        with mlflow.start_run() as run:
            run_id = run.info.run_id

            mlflow.log_params({
                "train_rows": len(train_df),
                "val_rows":   len(val_df),
                "test_rows":  len(test_df),
            })

            # 3. Train
            model = model_svc.train(train_df)

            # Log XGBoost hyperparameters if available
            if hasattr(model, "n_estimators"):
                mlflow.log_params({
                    "classifier":       "XGBoostClassifier",
                    "n_estimators":     model.n_estimators,
                    "max_depth":        model.max_depth,
                    "learning_rate":    float(model.learning_rate),
                    "scale_pos_weight": float(model.scale_pos_weight),
                })

            # 4. Evaluate
            metrics = model_svc.evaluate(model, test_df)
            mlflow.log_metrics(metrics)
            monitor_svc.publish_model_metrics(metrics, model_version="pending")
            monitor_svc.trigger_alerts(metrics)

            if metrics["pr_auc"] < MIN_PR_AUC:
                raise ValueError(
                    f"PR-AUC {metrics['pr_auc']} below acceptance threshold {MIN_PR_AUC}."
                )

            # 5. Log artifact to MLflow
            artifact_path = model_svc.save_model(model, metrics, run_id)

            # 6. Get current production baseline
            prod = registry_svc.get_production_version()
            prod_pr_auc = prod["pr_auc"] if prod else 0.0

            # 7. Register as Staging candidate
            mlflow_version = registry_svc.register(
                run_id=run_id,
                metrics=metrics,
                artifact_path=artifact_path,
                train_rows=len(train_df),
                status="candidate",
            )

            # 8. Promote to Production if new model is at least as good
            if metrics["pr_auc"] >= prod_pr_auc - 0.01:
                registry_svc.promote(mlflow_version)
                logger.info(f"[{PIPELINE}] Promoted version {mlflow_version} to Production.")
                send_discord_alert(
                    title=f"Model v{mlflow_version} promoted to Production",
                    message=(
                        f"**PR-AUC**: {metrics['pr_auc']:.4f}\n"
                        f"**F1**: {metrics['f1']:.4f}\n"
                        f"**Precision**: {metrics['precision']:.4f} | "
                        f"**Recall**: {metrics['recall']:.4f}\n"
                        f"**Train rows**: {len(train_df):,}"
                    ),
                    level="success",
                    webhook_url=webhook_url,
                )
            else:
                logger.info(
                    f"[{PIPELINE}] Registered v{mlflow_version} as candidate "
                    f"(PR-AUC {metrics['pr_auc']} < {prod_pr_auc - 0.01:.4f})."
                )

        log_run(PIPELINE, start_ts, datetime.now(), "success", len(df), 1, cfg=cfg)

        push_metrics(PIPELINE, {
            "fraud_model_pr_auc":     metrics.get("pr_auc"),
            "fraud_model_f1":         metrics.get("f1"),
            "fraud_model_precision":  metrics.get("precision"),
            "fraud_model_recall":     metrics.get("recall"),
            "fraud_model_train_rows": len(train_df),
            "fraud_model_test_rows":  len(test_df),
        })

    except Exception as e:
        log_run(PIPELINE, start_ts, datetime.now(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE}] FAILED: {e}")
        send_discord_alert(
            title="CRITICAL: Training pipeline failed",
            message=f"```{str(e)[:500]}```",
            level="critical",
            webhook_url=webhook_url,
        )
        raise
