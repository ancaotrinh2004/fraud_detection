"""
dag_feat_backfill.py

One-time backfill DAG — trigger manually to populate historical feature snapshots
in feat_customer_90d, feat_stream_30m, and feat_customer_unified.

Data range: 2025-04-04 → 2025-09-30 (180 days, end_date = 2025-10-01).

Runs sequentially for 6 month-end snapshot dates covering the full data range:
  2025-04-30 → 2025-05-31 → 2025-06-30 → 2025-07-31 → 2025-08-31 → 2025-09-30

Starting at 2025-04-30 ensures PIT join can find a valid feature snapshot for
transactions from 2025-04-30 onwards (~97% of training data). Transactions in the
first 26 days of April (2025-04-04 to 2025-04-29) will have no prior snapshot and
are dropped by the ML pipeline's missing-feature filter (~2% of training data).

After this DAG completes, feat_customer_unified will have 6 historical snapshots
per customer, enabling point-in-time correct features for the ML training PIT join.
"""

from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

IMAGE     = "ancaotrinh/fraud-airflow:latest"
NAMESPACE = "airflow"

BACKFILL_DATES = ["2025-04-30", "2025-05-31", "2025-06-30", "2025-07-31", "2025-08-31", "2025-09-30"]

_env = [
    k8s.V1EnvVar(name="PYTHONPATH",        value="/opt/airflow"),
    k8s.V1EnvVar(name="POSTGRES_PASSWORD",  value="fraud_pass"),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY",   value="fraud_minio_user"),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY",   value="fraud_minio_pass"),
]

_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "2Gi"},
    limits={"memory": "4Gi"},
)

_volume_mounts = [
    k8s.V1VolumeMount(
        name="pipeline-config",
        mount_path="/opt/airflow/configs/pipeline_config.yaml",
        sub_path="pipeline_config.yaml",
        read_only=True,
    )
]

_volumes = [
    k8s.V1Volume(
        name="pipeline-config",
        config_map=k8s.V1ConfigMapVolumeSource(name="fraud-pipeline-config"),
    )
]


def _make_pod(task_id: str, python_cmd: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"feat-backfill-{task_id.replace('_', '-')}",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="Always",
        cmds=["python", "-c"],
        arguments=[python_cmd],
        env_vars=_env,
        container_resources=_resources,
        volume_mounts=_volume_mounts,
        volumes=_volumes,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )


@dag(
    dag_id="dag_feat_backfill",
    schedule=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["features", "backfill"],
)
def feat_backfill():
    prev = None
    for date in BACKFILL_DATES:
        safe = date.replace("-", "")

        t90d = _make_pod(
            f"feat_customer_90d_{safe}",
            f"from src.pipelines.features.feat_pipelines import run_feat_customer_90d; "
            f"run_feat_customer_90d(as_of_date='{date}')",
        )
        t30m = _make_pod(
            f"feat_stream_30m_{safe}",
            f"from src.pipelines.features.feat_pipelines import run_feat_stream_30m; "
            f"run_feat_stream_30m(as_of_date='{date}')",
        )
        tunified = _make_pod(
            f"feat_unified_{safe}",
            f"from src.pipelines.features.feat_pipelines import run_feat_unified; "
            f"run_feat_unified(as_of_date='{date}')",
        )

        t90d >> t30m >> tunified

        if prev is not None:
            prev >> t90d

        prev = tunified


feat_backfill()
