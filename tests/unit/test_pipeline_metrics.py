"""
Unit tests for src/pipelines/utils/metrics.py (push_metrics helper).
prometheus_client calls are mocked — no real Pushgateway required.
"""

import logging
from unittest.mock import MagicMock, call, patch

import pytest

from src.pipelines.utils.metrics import push_metrics


@pytest.fixture(autouse=True)
def patch_pushgateway():
    """Patch push_to_gateway and CollectorRegistry for all tests in this module."""
    with patch("src.pipelines.utils.metrics.push_to_gateway") as mock_push, \
         patch("src.pipelines.utils.metrics.CollectorRegistry") as MockRegistry, \
         patch("src.pipelines.utils.metrics.Gauge") as MockGauge:
        mock_registry_instance = MagicMock()
        MockRegistry.return_value = mock_registry_instance
        mock_gauge_instance = MagicMock()
        MockGauge.return_value = mock_gauge_instance
        yield {
            "push":     mock_push,
            "Registry": MockRegistry,
            "Gauge":    MockGauge,
            "registry": mock_registry_instance,
            "gauge":    mock_gauge_instance,
        }


class TestPushMetrics:
    def test_push_to_gateway_called(self, patch_pushgateway):
        push_metrics("test_job", {"my_metric": 1.0})
        patch_pushgateway["push"].assert_called_once()

    def test_push_called_with_correct_job_name(self, patch_pushgateway):
        push_metrics("fraud_train_pipeline", {"metric": 1.0})
        call_kwargs = patch_pushgateway["push"].call_args[1]
        assert call_kwargs["job"] == "fraud_train_pipeline"

    def test_gauge_created_for_each_metric(self, patch_pushgateway):
        metrics = {"fraud_model_pr_auc": 0.85, "fraud_model_f1": 0.72, "fraud_model_train_rows": 10000}
        push_metrics("job", metrics)
        assert patch_pushgateway["Gauge"].call_count == len(metrics)

    def test_gauge_set_called_for_each_metric(self, patch_pushgateway):
        push_metrics("job", {"a": 1.0, "b": 2.0})
        assert patch_pushgateway["gauge"].set.call_count == 2

    def test_gauge_values_are_float(self, patch_pushgateway):
        push_metrics("job", {"int_metric": 100})
        set_call_args = patch_pushgateway["gauge"].set.call_args_list
        for c in set_call_args:
            value = c[0][0]
            assert isinstance(value, float)

    def test_none_values_skipped(self, patch_pushgateway):
        push_metrics("job", {"good": 1.0, "none_metric": None, "also_good": 2.0})
        # Only 2 gauges created (None skipped)
        assert patch_pushgateway["Gauge"].call_count == 2

    def test_empty_metrics_dict_no_gauges_created(self, patch_pushgateway):
        push_metrics("job", {})
        patch_pushgateway["Gauge"].assert_not_called()
        patch_pushgateway["push"].assert_called_once()  # push still called

    def test_push_failure_is_non_fatal(self, patch_pushgateway):
        patch_pushgateway["push"].side_effect = Exception("Connection refused")
        # Must not raise
        push_metrics("job", {"metric": 1.0})

    def test_push_failure_logs_warning(self, patch_pushgateway, caplog):
        patch_pushgateway["push"].side_effect = ConnectionError("refused")
        with caplog.at_level(logging.WARNING):
            push_metrics("job", {"metric": 1.0})
        assert any("non-fatal" in r.message or "push failed" in r.message for r in caplog.records)

    def test_each_call_creates_fresh_registry(self, patch_pushgateway):
        push_metrics("job1", {"m": 1.0})
        push_metrics("job2", {"m": 2.0})
        assert patch_pushgateway["Registry"].call_count == 2

    def test_registry_passed_to_push_gateway(self, patch_pushgateway):
        push_metrics("job", {"metric": 0.5})
        call_kwargs = patch_pushgateway["push"].call_args[1]
        assert call_kwargs["registry"] is patch_pushgateway["registry"]

    def test_pushgateway_url_passed_to_push_gateway(self, patch_pushgateway):
        with patch("src.pipelines.utils.metrics._PUSHGATEWAY_URL", "http://custom:9091"):
            push_metrics("job", {"metric": 1.0})
        url_arg = patch_pushgateway["push"].call_args[0][0]
        assert url_arg == "http://custom:9091"
