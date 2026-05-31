"""
src/pipelines/features/drift_monitor.py

Scenario B drift monitoring: detect amount drift in fact_transaction.
- Groups transactions by week and computes weekly distribution of `amount`.
- Computes PSI vs baseline (first 60 days) for each week.
- Writes to agg_feature_health_daily and feature_drift_alerts.

Works on existing data. Will show higher PSI after drift_start_date
if data was re-generated with amount_drift_multiplier > 1.
"""

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.pipelines.utils.db import load_pipeline_config, log_run, get_sqlalchemy_engine
from src.pipelines.utils.metrics import push_metrics

logger = logging.getLogger(__name__)
SCHEMA = "gold_fraud"

MONITORED_FEATURE = "f_customer_avg_txn_amount_90d"
# Transaction amounts follow lognormal(13, 1.2) — very high variance (std ≈ 1.8× mean).
# A 30% mean shift produces PSI ≈ 0.04 with quantile bins, far below the credit-score
# standard of 0.15. Threshold calibrated to this distribution's spread.
PSI_ALERT_THRESHOLD = 0.025
PSI_BINS = 10
BASELINE_DAYS = 60      # first N days = baseline period
MIN_ROWS_PER_WINDOW = 500


def _compute_psi(baseline: np.ndarray, current: np.ndarray, n_bins: int = PSI_BINS) -> float:
    """Population Stability Index between baseline and current distributions.

    Uses quantile-based bins (equal-frequency on baseline) so PSI is sensitive
    to mean shifts on skewed/lognormal distributions like transaction amounts.
    """
    eps = 1e-8
    # Bin edges from baseline percentiles — each bin holds 1/n_bins of baseline
    percentiles = np.linspace(0, 100, n_bins + 1)
    bins = np.unique(np.percentile(baseline, percentiles))
    if len(bins) < 3:
        # Degenerate baseline — fall back to equal-width
        bins = np.linspace(baseline.min(), baseline.max() + eps, n_bins + 1)
    else:
        bins[0]  -= eps   # so baseline.min() falls inside the first bin
        bins[-1] += eps   # so current values above baseline.max() are captured

    base_pct = np.histogram(baseline, bins=bins)[0] / max(len(baseline), 1)
    curr_pct = np.histogram(current,  bins=bins)[0] / max(len(current),  1)

    base_pct = np.where(base_pct == 0, eps, base_pct)
    curr_pct = np.where(curr_pct == 0, eps, curr_pct)

    return float(round(np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct)), 4))


def run_drift_monitor(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    engine = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()
    pipeline = "drift_monitor_pipeline"

    logger.info(f"[{pipeline}] Starting — monitoring 'amount' distribution from fact_transaction...")
    try:
        # ── Load transaction amounts with timestamps ───────────────────────────
        df = pd.read_sql(
            f"""
            SELECT transaction_timestamp, amount
            FROM {SCHEMA}.fact_transaction
            WHERE amount IS NOT NULL AND amount > 0
            ORDER BY transaction_timestamp
            """,
            engine,
        )
        if df.empty:
            logger.warning(f"[{pipeline}] No data in fact_transaction — skipping.")
            return

        df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"])
        df["txn_date"] = df["transaction_timestamp"].dt.date

        # ── Establish baseline ────────────────────────────────────────────────
        min_date = df["txn_date"].min()
        baseline_end = min_date + timedelta(days=BASELINE_DAYS)
        baseline_vals = df[df["txn_date"] <= baseline_end]["amount"].values

        baseline_mean = float(np.mean(baseline_vals))
        baseline_std  = float(np.std(baseline_vals))
        logger.info(
            f"[{pipeline}] Baseline: {len(baseline_vals):,} rows, "
            f"mean={baseline_mean:,.0f}, std={baseline_std:,.0f}"
        )

        health_rows = []
        alert_rows  = []

        # Baseline record
        health_rows.append({
            "monitoring_date": baseline_end,
            "feature_name":    MONITORED_FEATURE,
            "mean_value":      round(baseline_mean, 4),
            "stddev_value":    round(baseline_std, 4),
            "psi_vs_baseline": 0.0,
            "alert_flag":      False,
        })

        # ── Weekly windows after baseline ─────────────────────────────────────
        window_start = baseline_end + timedelta(days=1)
        max_date = df["txn_date"].max()

        while window_start <= max_date:
            window_end = window_start + timedelta(days=6)
            window_df  = df[
                (df["txn_date"] >= window_start) &
                (df["txn_date"] <= window_end)
            ]
            if len(window_df) < MIN_ROWS_PER_WINDOW:
                window_start += timedelta(days=7)
                continue

            current_vals = window_df["amount"].values
            psi      = _compute_psi(baseline_vals, current_vals)
            mean_val = float(np.mean(current_vals))
            std_val  = float(np.std(current_vals))
            is_alert = psi > PSI_ALERT_THRESHOLD

            health_rows.append({
                "monitoring_date": window_end,
                "feature_name":    MONITORED_FEATURE,
                "mean_value":      round(mean_val, 4),
                "stddev_value":    round(std_val, 4),
                "psi_vs_baseline": psi,
                "alert_flag":      is_alert,
            })

            if is_alert:
                alert_rows.append({
                    "alert_date":    window_end,
                    "feature_name":  MONITORED_FEATURE,
                    "psi_value":     psi,
                    "mean_before":   round(baseline_mean, 4),
                    "mean_after":    round(mean_val, 4),
                    "action": (
                        f"Amount drift detected (PSI={psi:.3f}): "
                        f"weekly mean shifted from {baseline_mean:,.0f} "
                        f"to {mean_val:,.0f}. "
                        f"Verify Scenario B amount multiplier impact."
                    ),
                })
                logger.warning(
                    f"[{pipeline}] ALERT week ending {window_end}: "
                    f"PSI={psi:.3f}, mean={mean_val:,.0f}"
                )

            window_start += timedelta(days=7)

        # ── Write agg_feature_health_daily ────────────────────────────────────
        with engine.begin() as conn:
            for row in health_rows:
                conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.agg_feature_health_daily
                        (monitoring_date, feature_name, mean_value, stddev_value,
                         psi_vs_baseline, alert_flag)
                    VALUES (:monitoring_date, :feature_name, :mean_value, :stddev_value,
                            :psi_vs_baseline, :alert_flag)
                    ON CONFLICT (monitoring_date, feature_name) DO UPDATE SET
                        mean_value      = EXCLUDED.mean_value,
                        stddev_value    = EXCLUDED.stddev_value,
                        psi_vs_baseline = EXCLUDED.psi_vs_baseline,
                        alert_flag      = EXCLUDED.alert_flag,
                        created_ts      = NOW()
                """), row)

        # ── Write feature_drift_alerts ────────────────────────────────────────
        if alert_rows:
            with engine.begin() as conn:
                for row in alert_rows:
                    conn.execute(text(f"""
                        INSERT INTO {SCHEMA}.feature_drift_alerts
                            (alert_date, feature_name, psi_value,
                             mean_before, mean_after, action)
                        VALUES (:alert_date, :feature_name, :psi_value,
                                :mean_before, :mean_after, :action)
                    """), row)

        log_run(pipeline, start_ts, datetime.utcnow(),
                "success", len(df), len(health_rows), cfg=cfg)
        logger.info(
            f"[{pipeline}] Done. {len(health_rows)} health rows, "
            f"{len(alert_rows)} alerts written."
        )

        # ── Level 2: push summary metrics to Pushgateway ──────────────────
        latest = health_rows[-1] if health_rows else {}
        push_metrics(pipeline, {
            "fraud_drift_health_windows_total": len(health_rows),
            "fraud_drift_alert_windows_total":  len(alert_rows),
            "fraud_drift_psi_latest":           latest.get("psi_vs_baseline", 0.0),
            "fraud_drift_mean_amount_latest":   latest.get("mean_value", 0.0),
            "fraud_drift_baseline_mean_amount": round(baseline_mean, 4),
        })

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        raise
