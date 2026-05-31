"""
Unit tests for MonitoringService — SQLAlchemy engine fully mocked.
Coverage: publish_model_metrics, check_retrain_trigger, trigger_alerts.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.pipelines.ml.services import MonitoringService


@pytest.fixture
def svc(mock_engine):
    return MonitoringService(mock_engine)


def _setup_alert_count(mock_engine, count: int):
    """Configure mock engine to return `count` consecutive drift alert rows."""
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (count,)
    return conn


# ── publish_model_metrics ──────────────────────────────────────────────────────

class TestPublishModelMetrics:
    def test_does_not_raise(self, svc):
        svc.publish_model_metrics({"pr_auc": 0.85, "f1": 0.72}, "v_001")

    def test_logs_info_message(self, svc, caplog):
        with caplog.at_level(logging.INFO):
            svc.publish_model_metrics({"pr_auc": 0.85}, "v_001")
        assert any("v_001" in r.message for r in caplog.records)

    def test_logs_all_metrics(self, svc, caplog):
        metrics = {"pr_auc": 0.85, "f1": 0.72, "precision": 0.80}
        with caplog.at_level(logging.INFO):
            svc.publish_model_metrics(metrics, "v_test")
        # At least one log record should contain metric values
        combined = " ".join(r.message for r in caplog.records)
        assert "0.85" in combined or "pr_auc" in combined

    def test_works_with_empty_metrics(self, svc):
        svc.publish_model_metrics({}, "v_empty")  # must not raise


# ── check_retrain_trigger ──────────────────────────────────────────────────────

class TestCheckRetrainTrigger:
    def test_returns_tuple(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 0)
        result = svc.check_retrain_trigger()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_first_element_is_bool(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 0)
        should_retrain, _ = svc.check_retrain_trigger()
        assert isinstance(should_retrain, bool)

    def test_second_element_is_str(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 0)
        _, reason = svc.check_retrain_trigger()
        assert isinstance(reason, str)

    def test_no_trigger_when_alert_count_below_threshold(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 2)  # threshold=3 by default
        should_retrain, reason = svc.check_retrain_trigger()
        assert should_retrain is False
        assert reason == "no trigger"

    def test_trigger_fires_when_alert_count_equals_threshold(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 3)  # exactly 3 = threshold
        should_retrain, reason = svc.check_retrain_trigger()
        assert should_retrain is True

    def test_trigger_fires_when_alert_count_exceeds_threshold(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 5)
        should_retrain, reason = svc.check_retrain_trigger()
        assert should_retrain is True

    def test_no_trigger_when_zero_alerts(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 0)
        should_retrain, _ = svc.check_retrain_trigger()
        assert should_retrain is False

    def test_trigger_reason_mentions_alert_count(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 4)
        should_retrain, reason = svc.check_retrain_trigger()
        assert should_retrain is True
        assert "4" in reason or "alert" in reason.lower()

    def test_custom_consecutive_weeks_threshold(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 2)
        # With consecutive_weeks=2, count=2 should trigger
        should_retrain, _ = svc.check_retrain_trigger(consecutive_weeks=2)
        assert should_retrain is True

    def test_higher_custom_threshold_prevents_trigger(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 3)
        # With consecutive_weeks=5, count=3 should NOT trigger
        should_retrain, _ = svc.check_retrain_trigger(consecutive_weeks=5)
        assert should_retrain is False

    def test_handles_none_db_row_gracefully(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = None
        should_retrain, _ = svc.check_retrain_trigger()
        assert should_retrain is False  # None → 0 alerts → no trigger

    def test_query_uses_limit_equal_to_consecutive_weeks(self, svc, mock_engine):
        _setup_alert_count(mock_engine, 0)
        svc.check_retrain_trigger(consecutive_weeks=7)
        conn = mock_engine.connect.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["n"] == 7


# ── trigger_alerts ─────────────────────────────────────────────────────────────

class TestTriggerAlerts:
    def test_logs_error_when_pr_auc_below_0_70(self, svc, caplog):
        with caplog.at_level(logging.ERROR):
            svc.trigger_alerts({"pr_auc": 0.65, "f1": 0.50})
        error_msgs = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_msgs) > 0
        assert any("0.65" in r.message or "pr_auc" in r.message.lower() for r in error_msgs)

    def test_logs_warning_when_f1_below_0_30(self, svc, caplog):
        with caplog.at_level(logging.WARNING):
            svc.trigger_alerts({"pr_auc": 0.80, "f1": 0.25})
        warn_msgs = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and ("f1" in r.message.lower() or "0.25" in r.message)
        ]
        assert len(warn_msgs) > 0

    def test_no_error_when_pr_auc_above_0_70(self, svc, caplog):
        with caplog.at_level(logging.ERROR):
            svc.trigger_alerts({"pr_auc": 0.75, "f1": 0.60})
        error_msgs = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_msgs) == 0

    def test_no_warning_when_f1_above_0_30(self, svc, caplog):
        with caplog.at_level(logging.WARNING):
            svc.trigger_alerts({"pr_auc": 0.80, "f1": 0.35})
        warn_msgs = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "f1" in r.message.lower()
        ]
        assert len(warn_msgs) == 0

    def test_both_alerts_fire_when_both_below_threshold(self, svc, caplog):
        with caplog.at_level(logging.DEBUG):
            svc.trigger_alerts({"pr_auc": 0.60, "f1": 0.20})
        error_msgs = [r for r in caplog.records if r.levelno >= logging.ERROR]
        warn_msgs  = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(error_msgs) >= 1
        assert len(warn_msgs) >= 1

    def test_does_not_raise_when_metrics_missing(self, svc):
        svc.trigger_alerts({})  # defaults: pr_auc=1.0, f1=1.0 — no alerts

    def test_boundary_pr_auc_exactly_0_70_no_error(self, svc, caplog):
        with caplog.at_level(logging.ERROR):
            svc.trigger_alerts({"pr_auc": 0.70, "f1": 0.50})
        error_msgs = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_msgs) == 0

    def test_boundary_f1_exactly_0_30_no_warning(self, svc, caplog):
        with caplog.at_level(logging.WARNING):
            svc.trigger_alerts({"pr_auc": 0.80, "f1": 0.30})
        warn_msgs = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "f1" in r.message.lower()
        ]
        assert len(warn_msgs) == 0
