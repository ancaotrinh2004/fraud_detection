"""
src/pipelines/features/drift_monitor.py

Feature drift monitoring — weekly PSI for all 11 feat_customer_unified features
plus prediction score distribution from ml_fraud_scores.

Tiered thresholds:
  PSI >= 0.1  → Warning
  PSI >= 0.2  → Critical

Results written to agg_feature_health_daily and feature_drift_alerts.

Alerting is NOT done in code: this pipeline only pushes per-feature PSI and a
run-status gauge to the Pushgateway. Prometheus alert rules
(infra/k8s/fraud-drift-alerts.yaml) evaluate those metrics and Alertmanager
routes the alerts to Discord.
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

MONITORED_FEATURES = [
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

PSI_WARNING_THRESHOLD  = 0.10
PSI_CRITICAL_THRESHOLD = 0.20
PSI_BINS               = 10
BASELINE_DAYS          = 60
MIN_ROWS_PER_WINDOW    = 200


def _compute_psi(baseline: np.ndarray, current: np.ndarray, n_bins: int = PSI_BINS) -> float:
    """PSI between baseline and current distributions using quantile-based bins."""
    eps = 1e-8
    percentiles = np.linspace(0, 100, n_bins + 1)
    bins = np.unique(np.percentile(baseline, percentiles))
    if len(bins) < 3:
        bins = np.linspace(baseline.min(), baseline.max() + eps, n_bins + 1)
    else:
        bins[0]  -= eps
        bins[-1] += eps

    base_pct = np.histogram(baseline, bins=bins)[0] / max(len(baseline), 1)
    curr_pct = np.histogram(current,  bins=bins)[0] / max(len(current),  1)

    base_pct = np.where(base_pct == 0, eps, base_pct)
    curr_pct = np.where(curr_pct == 0, eps, curr_pct)

    return float(round(np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct)), 4))


def _compute_feature_drift(
    df: pd.DataFrame,
    feature: str,
    ts_col: str,
) -> tuple[list[dict], list[dict]]:
    """
    Compute weekly PSI for a single feature column.
    Returns (health_rows, alert_rows).
    """
    col_df = df[[ts_col, feature]].dropna(subset=[feature])
    if col_df.empty:
        logger.warning(f"[drift_monitor] No data for feature '{feature}' — skipping.")
        return [], []

    col_df = col_df.copy()
    col_df["txn_date"] = col_df[ts_col].dt.date
    vals = col_df[feature].values.astype(float)

    min_date     = col_df["txn_date"].min()
    baseline_end = min_date + timedelta(days=BASELINE_DAYS)
    baseline_vals = col_df[col_df["txn_date"] <= baseline_end][feature].values.astype(float)

    if len(baseline_vals) < MIN_ROWS_PER_WINDOW:
        logger.warning(
            f"[drift_monitor] Not enough baseline rows for '{feature}' "
            f"({len(baseline_vals)} < {MIN_ROWS_PER_WINDOW}) — skipping."
        )
        return [], []

    baseline_mean = float(np.mean(baseline_vals))
    baseline_std  = float(np.std(baseline_vals))

    health_rows: list[dict] = [{
        "monitoring_date": baseline_end,
        "feature_name":    feature,
        "mean_value":      round(baseline_mean, 4),
        "stddev_value":    round(baseline_std, 4),
        "psi_vs_baseline": 0.0,
        "alert_flag":      False,
    }]
    alert_rows: list[dict] = []

    window_start = baseline_end + timedelta(days=1)
    max_date = col_df["txn_date"].max()

    while window_start <= max_date:
        window_end = window_start + timedelta(days=6)
        window_df  = col_df[
            (col_df["txn_date"] >= window_start) &
            (col_df["txn_date"] <= window_end)
        ]
        if len(window_df) < MIN_ROWS_PER_WINDOW:
            window_start += timedelta(days=7)
            continue

        current_vals = window_df[feature].values.astype(float)
        psi      = _compute_psi(baseline_vals, current_vals)
        mean_val = float(np.mean(current_vals))
        std_val  = float(np.std(current_vals))
        is_alert = psi >= PSI_WARNING_THRESHOLD

        health_rows.append({
            "monitoring_date": window_end,
            "feature_name":    feature,
            "mean_value":      round(mean_val, 4),
            "stddev_value":    round(std_val, 4),
            "psi_vs_baseline": psi,
            "alert_flag":      is_alert,
        })

        if is_alert:
            level  = "critical" if psi >= PSI_CRITICAL_THRESHOLD else "warning"
            action = (
                f"Drift detected for '{feature}' (PSI={psi:.3f}): "
                f"mean shifted from {baseline_mean:.4f} to {mean_val:.4f}."
            )
            alert_rows.append({
                "alert_date":   window_end,
                "feature_name": feature,
                "psi_value":    psi,
                "mean_before":  round(baseline_mean, 4),
                "mean_after":   round(mean_val, 4),
                "action":       action[:500],
            })
            logger.warning(
                f"[drift_monitor] {level.upper()} '{feature}' "
                f"week ending {window_end}: PSI={psi:.3f}"
            )

        window_start += timedelta(days=7)

    return health_rows, alert_rows


def run_drift_monitor(cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    engine = get_sqlalchemy_engine(cfg)
    start_ts = datetime.utcnow()
    pipeline = "drift_monitor_pipeline"

    logger.info(f"[{pipeline}] Starting — monitoring {len(MONITORED_FEATURES)} features + prediction scores...")
    try:
        # ── Load feat_customer_unified ────────────────────────────────────────
        feat_cols = ", ".join(MONITORED_FEATURES)
        feat_df = pd.read_sql(
            f"""
            SELECT customer_id, event_ts, {feat_cols}
            FROM {SCHEMA}.feat_customer_unified
            ORDER BY event_ts
            """,
            engine,
        )
        if feat_df.empty:
            logger.warning(f"[{pipeline}] No data in feat_customer_unified — skipping.")
            return

        feat_df["event_ts"] = pd.to_datetime(feat_df["event_ts"])

        # ── Load prediction scores ────────────────────────────────────────────
        score_df = pd.read_sql(
            f"""
            SELECT fraud_score, score_ts AS event_ts
            FROM {SCHEMA}.ml_fraud_scores
            WHERE fraud_score IS NOT NULL
            ORDER BY score_ts
            """,
            engine,
        )
        score_df["event_ts"] = pd.to_datetime(score_df["event_ts"])

        # ── Compute PSI per feature ───────────────────────────────────────────
        all_health: list[dict] = []
        all_alerts: list[dict] = []

        for feature in MONITORED_FEATURES:
            h_rows, a_rows = _compute_feature_drift(feat_df, feature, "event_ts")
            all_health.extend(h_rows)
            all_alerts.extend(a_rows)

        # ── Compute PSI for prediction score distribution ─────────────────────
        if not score_df.empty:
            h_rows, a_rows = _compute_feature_drift(score_df, "fraud_score", "event_ts")
            all_health.extend(h_rows)
            all_alerts.extend(a_rows)
        else:
            logger.info(f"[{pipeline}] No prediction scores found — skipping score drift.")

        # ── Write agg_feature_health_daily ────────────────────────────────────
        if all_health:
            with engine.begin() as conn:
                for row in all_health:
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
        if all_alerts:
            with engine.begin() as conn:
                for row in all_alerts:
                    conn.execute(text(f"""
                        INSERT INTO {SCHEMA}.feature_drift_alerts
                            (alert_date, feature_name, psi_value,
                             mean_before, mean_after, action)
                        VALUES (:alert_date, :feature_name, :psi_value,
                                :mean_before, :mean_after, :action)
                    """), row)

        total_alerts = len(all_alerts)
        log_run(pipeline, start_ts, datetime.utcnow(),
                "success", len(feat_df), len(all_health), cfg=cfg)
        logger.info(
            f"[{pipeline}] Done. {len(all_health)} health rows, {total_alerts} alerts."
        )

        # ── Push summary metrics to Pushgateway ───────────────────────────────
        latest_psis = {}
        for row in reversed(all_health):
            fn = row["feature_name"]
            if fn not in latest_psis:
                latest_psis[fn] = row["psi_vs_baseline"]

        # Latest PSI per feature + run status drive the Prometheus alert rules.
        push_metrics(pipeline, {
            "fraud_drift_health_windows_total": len(all_health),
            "fraud_drift_alert_windows_total":  total_alerts,
            "fraud_drift_features_monitored":   len(MONITORED_FEATURES) + 1,
            "fraud_drift_run_status":           1,  # 1 = success
            **{f"fraud_drift_psi_{fn}": psi for fn, psi in latest_psis.items()},
        })

    except Exception as e:
        log_run(pipeline, start_ts, datetime.utcnow(),
                "failed", 0, 0, str(e)[:500], cfg=cfg)
        logger.error(f"[{pipeline}] FAILED: {e}")
        # Expose failure as a metric (0 = failed) so the FraudDriftMonitorFailed
        # alert rule fires; alerting is handled by Prometheus/Alertmanager, not code.
        push_metrics(pipeline, {"fraud_drift_run_status": 0})
        raise
