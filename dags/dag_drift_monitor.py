"""Drift monitoring DAG — runs daily; computes weekly PSI for all 11 unified
features plus prediction-score drift, writes health/alert tables, and pushes
per-feature PSI + a run-status gauge to the Pushgateway. Alerting is handled by
Prometheus alert rules (infra/k8s/fraud-drift-alerts.yaml) → Alertmanager →
Discord, NOT by code."""

from datetime import timedelta
from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="drift_monitor",
    schedule="0 6 * * *",
    start_date=datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "drift", "ml"],
    default_args={
        "owner": "fraud-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def drift_monitor_dag():

    @task()
    def monitor_feature_drift():
        from src.pipelines.features.drift_monitor import run_drift_monitor
        run_drift_monitor()

    monitor_feature_drift()


drift_monitor_dag()
