"""
src/pipelines/ml/retrain_trigger.py

Retrain trigger pipeline — uses MonitoringService to check drift + model quality.
If trigger fires, calls run_training() to retrain, then promotes or rolls back.
"""

import logging
from datetime import datetime

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.ml.services import (
    ModelService,
    ModelRegistryService,
    MonitoringService,
)
from src.pipelines.ml.train import run_training

logger   = logging.getLogger(__name__)
PIPELINE = "ml_retrain_trigger_pipeline"


def run_retrain_trigger(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    engine   = get_sqlalchemy_engine(cfg)
    start_ts = datetime.now()

    registry_svc = ModelRegistryService(engine)
    monitor_svc  = MonitoringService(engine)
    model_svc    = ModelService(cfg)

    logger.info(f"[{PIPELINE}] Checking retrain triggers...")

    try:
        # 1. Check drift + quality triggers
        should_retrain, reason = monitor_svc.check_retrain_trigger()

        if not should_retrain:
            logger.info(f"[{PIPELINE}] No retrain needed. Reason: {reason}")
            log_run(PIPELINE, start_ts, datetime.now(), "success", 0, 0, cfg=cfg)
            return

        logger.info(f"[{PIPELINE}] Retrain triggered: {reason}")

        # 2. Get current production model for comparison
        prod_before = registry_svc.get_production_version()
        prod_pr_auc = prod_before["pr_auc"] if prod_before else 0.0

        # 3. Retrain candidate
        run_training(cfg=cfg)

        # 4. Promote or rollback
        new_prod = registry_svc.get_production_version()
        if new_prod and new_prod["pr_auc"] >= prod_pr_auc - 0.01:
            logger.info(
                f"[{PIPELINE}] Promoted {new_prod['model_version']} "
                f"(PR-AUC={new_prod['pr_auc']:.4f} vs previous={prod_pr_auc:.4f})."
            )
        elif prod_before:
            logger.warning(
                f"[{PIPELINE}] Candidate did not improve. "
                f"Rolling back to {prod_before['model_version']}."
            )
            registry_svc.rollback(prod_before["model_version"])

        log_run(PIPELINE, start_ts, datetime.now(), "success", 0, 1, cfg=cfg)

    except Exception as e:
        log_run(PIPELINE, start_ts, datetime.now(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE}] FAILED: {e}")
        raise
