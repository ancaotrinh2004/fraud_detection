"""
Unit tests for ModelRegistryService — SQLAlchemy engine fully mocked.
Coverage: register, get_production_version, promote, rollback.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from src.pipelines.ml.services import ModelRegistryService


@pytest.fixture
def svc(mock_engine):
    return ModelRegistryService(mock_engine)


@pytest.fixture
def sample_metrics():
    return {
        "pr_auc":    0.85,
        "f1":        0.72,
        "precision": 0.80,
        "recall":    0.65,
    }


# ── register ───────────────────────────────────────────────────────────────────

class TestRegister:
    def test_executes_insert_sql(self, svc, mock_engine, sample_metrics):
        svc.register("v_001", sample_metrics, "s3://models/v_001/model.pkl", 10000)
        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.assert_called_once()

    def test_passes_model_version(self, svc, mock_engine, sample_metrics):
        svc.register("v_test_version", sample_metrics, "s3://test", 100)
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["ver"] == "v_test_version"

    def test_passes_metrics_values(self, svc, mock_engine, sample_metrics):
        svc.register("v_001", sample_metrics, "s3://test", 100)
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["pr_auc"] == sample_metrics["pr_auc"]
        assert params["f1"]     == sample_metrics["f1"]
        assert params["prec"]   == sample_metrics["precision"]
        assert params["rec"]    == sample_metrics["recall"]

    def test_default_status_is_candidate(self, svc, mock_engine, sample_metrics):
        svc.register("v_001", sample_metrics, "s3://test", 100)
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["status"] == "candidate"

    def test_custom_status_passed_through(self, svc, mock_engine, sample_metrics):
        svc.register("v_001", sample_metrics, "s3://test", 100, status="production")
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["status"] == "production"

    def test_artifact_path_passed(self, svc, mock_engine, sample_metrics):
        path = "s3://models/fraud_xgb/v_001/model.pkl"
        svc.register("v_001", sample_metrics, path, 100)
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["path"] == path

    def test_train_rows_passed(self, svc, mock_engine, sample_metrics):
        svc.register("v_001", sample_metrics, "s3://test", 50000)
        conn = mock_engine.begin.return_value.__enter__.return_value
        params = conn.execute.call_args[0][1]
        assert params["rows"] == 50000


# ── get_production_version ─────────────────────────────────────────────────────

class TestGetProductionVersion:
    def test_returns_dict_when_production_model_exists(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (
            "v_20260101_120000", 0.85, "s3://models/v_20260101/model.pkl"
        )
        result = svc.get_production_version()
        assert isinstance(result, dict)

    def test_returned_dict_has_required_keys(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (
            "v_001", 0.85, "s3://test/model.pkl"
        )
        result = svc.get_production_version()
        assert "model_version" in result
        assert "pr_auc" in result
        assert "artifact_path" in result

    def test_returned_values_match_db_row(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (
            "v_20260101", 0.8148, "s3://models/v_20260101/model.pkl"
        )
        result = svc.get_production_version()
        assert result["model_version"] == "v_20260101"
        assert result["pr_auc"]        == pytest.approx(0.8148)
        assert result["artifact_path"] == "s3://models/v_20260101/model.pkl"

    def test_pr_auc_is_float(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("v_001", "0.85", "s3://test")
        result = svc.get_production_version()
        assert isinstance(result["pr_auc"], float)

    def test_returns_none_when_no_production_model(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = None
        result = svc.get_production_version()
        assert result is None

    def test_queries_production_status(self, svc, mock_engine):
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = None
        svc.get_production_version()
        sql = str(conn.execute.call_args[0][0])
        assert "production" in sql.lower()


# ── promote ─────────────────────────────────────────────────────────────────────

class TestPromote:
    def test_executes_two_update_statements(self, svc, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        svc.promote("v_new")
        assert conn.execute.call_count == 2

    def test_first_update_retires_current_production(self, svc, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        svc.promote("v_new")
        first_sql = str(conn.execute.call_args_list[0][0][0])
        assert "retired" in first_sql

    def test_second_update_sets_new_production(self, svc, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        svc.promote("v_new")
        second_sql  = str(conn.execute.call_args_list[1][0][0])
        second_params = conn.execute.call_args_list[1][0][1]
        assert "production" in second_sql
        assert second_params["ver"] == "v_new"

    def test_retire_happens_before_promote(self, svc, mock_engine):
        """Retire must come first to avoid two simultaneous production rows."""
        order = []
        conn = mock_engine.begin.return_value.__enter__.return_value

        def capture(sql, params=None):
            sql_str = str(sql)
            order.append("retire" if "retired" in sql_str else "promote")

        conn.execute.side_effect = capture
        svc.promote("v_new")
        assert order == ["retire", "promote"]


# ── rollback ───────────────────────────────────────────────────────────────────

class TestRollback:
    def test_rollback_delegates_to_promote(self, svc, mock_engine):
        with patch.object(svc, "promote") as mock_promote:
            svc.rollback("v_previous")
        mock_promote.assert_called_once_with("v_previous")

    def test_rollback_passes_correct_version(self, svc, mock_engine):
        conn = mock_engine.begin.return_value.__enter__.return_value
        svc.rollback("v_good_old_version")
        # Second execute call should set v_good_old_version as production
        second_params = conn.execute.call_args_list[1][0][1]
        assert second_params["ver"] == "v_good_old_version"
