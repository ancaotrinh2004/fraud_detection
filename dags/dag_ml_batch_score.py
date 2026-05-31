from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

IMAGE = "ancaotrinh/fraud-airflow:latest"
NAMESPACE = "airflow"

_env = [
    k8s.V1EnvVar(name="PYTHONPATH",       value="/opt/airflow"),
    k8s.V1EnvVar(name="POSTGRES_PASSWORD", value="fraud_pass"),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY",  value="fraud_minio_user"),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY",  value="fraud_minio_pass"),
]

_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "200m", "memory": "1Gi"},
    limits={"memory": "2Gi"},
)

_volume_mounts = [
    k8s.V1VolumeMount(
        name="pipeline-config",
        mount_path="/opt/airflow/config/pipeline_config.yaml",
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


@dag(
    dag_id="dag_ml_batch_score",
    schedule="0 5 * * *",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "scoring"],
)
def ml_batch_score():

    score_transactions = KubernetesPodOperator(
        task_id="score_transactions",
        name="ml-batch-score-pod",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="Always",
        cmds=["python", "-c"],
        arguments=["from src.pipelines.ml.batch_score import run_batch_score; run_batch_score()"],
        env_vars=_env,
        container_resources=_resources,
        volume_mounts=_volume_mounts,
        volumes=_volumes,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    score_transactions


ml_batch_score()
