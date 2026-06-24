"""
src/pipelines/utils/metrics.py

Pushgateway helper for batch pipeline metrics.
Called at the end of each pipeline run to push key metrics so Prometheus
can scrape them even though the job pod is short-lived.

Push failures are non-fatal: if the Pushgateway is unavailable the
pipeline still succeeds — metrics are simply not updated that run.
"""

import logging
import os

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = logging.getLogger(__name__)

_PUSHGATEWAY_URL = os.getenv(
    "PUSHGATEWAY_URL",
    "http://pushgateway.monitoring.svc.cluster.local:9091",
)


def push_metrics(job: str, metrics: dict[str, float]) -> None:
    """Push a flat dict of {metric_name: float_value} to the Pushgateway.

    Each call replaces the previous metric group for this job name,
    so re-running a pipeline automatically overwrites stale values.

    Args:
        job:     Logical pipeline name, e.g. "drift_monitor_pipeline".
                 Becomes the Pushgateway grouping key.
        metrics: Dict of Prometheus metric name → numeric value.
                 None values are silently skipped.
    """
    registry = CollectorRegistry()
    for name, value in metrics.items():
        if value is None:
            continue
        Gauge(name, name, registry=registry).set(float(value))

    try:
        push_to_gateway(_PUSHGATEWAY_URL, job=job, registry=registry)
        logger.info("[metrics] pushed %d metrics for job=%s", len(metrics), job)
    except Exception as exc:
        logger.warning("[metrics] push failed (non-fatal): %s", exc)
