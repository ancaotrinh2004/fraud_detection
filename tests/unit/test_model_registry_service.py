"""
Unit tests for ModelRegistryService — MLflow Model Registry fully mocked.

The registry was migrated from a PostgreSQL ml_model_registry table to the
MLflow Model Registry, so these tests mock mlflow.tracking.MlflowClient and
assert on the client calls (create_model_version, transition_model_version_stage,
get_latest_versions, get_run) rather than on SQL.

Coverage: register, get_production_version, promote, rollback.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pipelines.ml.services import ModelRegistryService


@pytest.fixture
def mock_client():
    client = MagicMock()
    # create_model_version() returns a ModelVersion-like object exposing `.version`
    client.create_model_version.return_value = MagicMock(version="3")
    return client


@pytest.fixture
def svc(mock_cfg, mock_client):
    # MlflowClient is instantiated in __init__; patch it so no server is contacted.
    with patch("mlflow.tracking.MlflowClient", return_value=mock_client), \
         patch("mlflow.set_tracking_uri"):
        service = ModelRegistryService(mock_cfg)
    return service


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
    def test_creates_model_version(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art/m-1/artifacts", 10000)
        mock_client.create_model_version.assert_called_once()

    def test_create_model_version_gets_source_and_run_id(self, svc, mock_client, sample_metrics):
        svc.register("run_xyz", sample_metrics, "s3://art/m-9/artifacts", 100)
        kwargs = mock_client.create_model_version.call_args[1]
        assert kwargs["source"] == "s3://art/m-9/artifacts"
        assert kwargs["run_id"] == "run_xyz"
        assert kwargs["name"] == svc._model_name

    def test_returns_version_string(self, svc, sample_metrics):
        version = svc.register("run_001", sample_metrics, "s3://art", 100)
        assert version == "3"

    def test_candidate_transitions_to_staging(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 100, status="candidate")
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["stage"] == "Staging"
        assert kwargs["version"] == "3"

    def test_default_status_is_candidate(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 100)
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["stage"] == "Staging"

    def test_non_candidate_transitions_to_production(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 100, status="production")
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["stage"] == "Production"

    def test_sets_pr_auc_tag(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 100)
        tag_calls = {c.args[2]: c.args[3] for c in mock_client.set_model_version_tag.call_args_list}
        assert tag_calls["pr_auc"] == str(sample_metrics["pr_auc"])

    def test_sets_train_rows_tag(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 50000)
        tag_calls = {c.args[2]: c.args[3] for c in mock_client.set_model_version_tag.call_args_list}
        assert tag_calls["train_rows"] == "50000"

    def test_ensures_registered_model_exists(self, svc, mock_client, sample_metrics):
        svc.register("run_001", sample_metrics, "s3://art", 100)
        mock_client.create_registered_model.assert_called_once_with(svc._model_name)

    def test_tolerates_already_existing_registered_model(self, svc, mock_client, sample_metrics):
        from mlflow.exceptions import MlflowException
        mock_client.create_registered_model.side_effect = MlflowException("already exists")
        # Must not raise — the existing-model case is swallowed.
        version = svc.register("run_001", sample_metrics, "s3://art", 100)
        assert version == "3"


# ── get_production_version ─────────────────────────────────────────────────────

class TestGetProductionVersion:
    @pytest.fixture
    def prod_version(self, mock_client):
        v = MagicMock(version="5", run_id="run_5", tags={"pr_auc": "0.90"})
        mock_client.get_latest_versions.return_value = [v]
        run = MagicMock()
        run.data.metrics = {"pr_auc": 0.88}
        mock_client.get_run.return_value = run
        return v

    def test_returns_dict_with_required_keys(self, svc, prod_version):
        result = svc.get_production_version()
        assert set(["model_version", "pr_auc", "artifact_path"]).issubset(result)

    def test_model_version_matches(self, svc, prod_version):
        assert svc.get_production_version()["model_version"] == "5"

    def test_pr_auc_from_run_metrics(self, svc, prod_version):
        result = svc.get_production_version()
        assert result["pr_auc"] == pytest.approx(0.88)
        assert isinstance(result["pr_auc"], float)

    def test_artifact_path_is_models_production_uri(self, svc, prod_version):
        result = svc.get_production_version()
        assert result["artifact_path"] == f"models:/{svc._model_name}/Production"

    def test_queries_production_stage(self, svc, prod_version, mock_client):
        svc.get_production_version()
        kwargs = mock_client.get_latest_versions.call_args
        assert "Production" in str(kwargs)

    def test_pr_auc_falls_back_to_tag_when_run_lookup_fails(self, svc, prod_version, mock_client):
        mock_client.get_run.side_effect = RuntimeError("run gone")
        result = svc.get_production_version()
        assert result["pr_auc"] == pytest.approx(0.90)  # from v.tags

    def test_returns_none_when_no_production_model(self, svc, mock_client):
        mock_client.get_latest_versions.return_value = []
        assert svc.get_production_version() is None

    def test_returns_none_when_registry_errors(self, svc, mock_client):
        mock_client.get_latest_versions.side_effect = RuntimeError("registry down")
        assert svc.get_production_version() is None


# ── promote ─────────────────────────────────────────────────────────────────────

class TestPromote:
    def test_transitions_to_production(self, svc, mock_client):
        svc.promote("7")
        mock_client.transition_model_version_stage.assert_called_once()
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["version"] == "7"
        assert kwargs["stage"] == "Production"

    def test_archives_existing_production_versions(self, svc, mock_client):
        svc.promote("7")
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["archive_existing_versions"] is True

    def test_promotes_against_configured_model_name(self, svc, mock_client):
        svc.promote("7")
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["name"] == svc._model_name


# ── rollback ───────────────────────────────────────────────────────────────────

class TestRollback:
    def test_rollback_delegates_to_promote(self, svc):
        with patch.object(svc, "promote") as mock_promote:
            svc.rollback("v_previous")
        mock_promote.assert_called_once_with("v_previous")

    def test_rollback_promotes_previous_version(self, svc, mock_client):
        svc.rollback("v_good_old")
        kwargs = mock_client.transition_model_version_stage.call_args[1]
        assert kwargs["version"] == "v_good_old"
        assert kwargs["stage"] == "Production"
