"""
src/pipelines/ml/batch_score.py

Batch scoring pipeline — uses ScoringService + ModelService + ModelRegistryService.
Scores yesterday's transactions and upserts to ml_fraud_scores.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.ml.services import (
    ModelService,
    ModelRegistryService,
    ScoringService,
)

logger   = logging.getLogger(__name__)
PIPELINE = "ml_batch_score_pipeline"
SCHEMA   = "gold_fraud"


def run_batch_score(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()

    engine    = get_sqlalchemy_engine(cfg)
    start_ts  = datetime.now()

    model_svc    = ModelService(cfg)
    registry_svc = ModelRegistryService(engine)
    scoring_svc  = ScoringService()

    logger.info(f"[{PIPELINE}] Starting batch scoring...")

    try:
        # 1. Get production model
        prod = registry_svc.get_production_version()
        if prod is None:
            logger.warning(f"[{PIPELINE}] No production model found. Skipping.")
            return

        artifact = model_svc.load_model(prod["artifact_path"])
        logger.info(f"[{PIPELINE}] Loaded model {artifact.model_version}.")

        # 2. Read transactions to score (last 2 days for safety overlap)
        score_from = (datetime.now() - timedelta(days=2)).date()
        df = pd.read_sql(
            f"""
            SELECT t.transaction_id, t.customer_id, t.event_timestamp,
                   f.f_customer_total_txn_90d,
                   f.f_customer_avg_txn_amount_90d,
                   f.f_customer_distinct_merchants_90d,
                   f.f_customer_decline_rate_90d,
                   f.f_customer_foreign_txn_ratio_90d,
                   f.f_customer_night_txn_ratio_90d,
                   f.f_stream_otp_failed_count_30m,
                   f.f_stream_decline_count_30m,
                   f.f_stream_txn_velocity_1h,
                   f.f_stream_new_merchant_flag,
                   f.f_stream_burst_activity_flag,
                   ft.amount                                          AS txn_amount,
                   EXTRACT(HOUR FROM t.event_timestamp)::int         AS txn_hour,
                   COALESCE(ft.is_declined, 0)                       AS is_declined_txn,
                   CASE WHEN ft.ip_country IS NOT NULL
                             AND dc.card_country IS NOT NULL
                             AND LOWER(ft.ip_country) != LOWER(dc.card_country)
                        THEN 1 ELSE 0 END                            AS is_foreign_txn
            FROM {SCHEMA}.ml_fraud_label t
            LEFT JOIN LATERAL (
                SELECT * FROM {SCHEMA}.feat_customer_unified u
                WHERE u.customer_id = t.customer_id
                  AND u.event_timestamp <= t.event_timestamp
                ORDER BY u.event_timestamp DESC LIMIT 1
            ) f ON TRUE
            LEFT JOIN {SCHEMA}.fact_transaction ft
                ON ft.transaction_id = t.transaction_id
            LEFT JOIN {SCHEMA}.dim_card dc
                ON dc.card_key = ft.card_key
            WHERE t.event_timestamp >= %(score_from)s
            """,
            engine,
            params={"score_from": str(score_from)},
        )

        if df.empty:
            logger.info(f"[{PIPELINE}] No transactions to score since {score_from}.")
            return

        logger.info(f"[{PIPELINE}] Scoring {len(df):,} transactions...")

        # 3. Score + write
        score_df = scoring_svc.score_batch(artifact, df)
        scoring_svc.write_scores(score_df, engine)

        log_run(PIPELINE, start_ts, datetime.now(), "success", len(df), len(score_df), cfg=cfg)
        logger.info(f"[{PIPELINE}] Done. {len(score_df):,} scores written.")

    except Exception as e:
        log_run(PIPELINE, start_ts, datetime.now(), "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{PIPELINE}] FAILED: {e}")
        raise
