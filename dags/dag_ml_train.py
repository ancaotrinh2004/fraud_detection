from airflow.sdk import dag, Asset
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

IMAGE = "ancaotrinh/fraud-airflow:latest"
NAMESPACE = "airflow"

_PG = "postgres://fraud-postgres/fraud_detection/gold_fraud"

ml_training      = Asset(f"{_PG}/ml_fraud_training")
ml_registry      = Asset(f"{_PG}/ml_model_registry")

_env = [
    k8s.V1EnvVar(name="PYTHONPATH",       value="/opt/airflow"),
    k8s.V1EnvVar(name="POSTGRES_PASSWORD", value="fraud_pass"),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY",  value="fraud_minio_user"),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY",  value="fraud_minio_pass"),
]

_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "2Gi"},
    limits={"memory": "4Gi"},
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
    dag_id="dag_ml_train",
    schedule="0 2 * * 0",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training"],
)
def ml_train():

    train_model = KubernetesPodOperator(
        task_id="train_model",
        name="ml-train-pod",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="Always",
        cmds=["python", "-c"],
        arguments=["from src.pipelines.ml.train import run_training; run_training()"],
        env_vars=_env,
        container_resources=_resources,
        volume_mounts=_volume_mounts,
        volumes=_volumes,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        inlets=[ml_training],
        outlets=[ml_registry],
    )

    train_model


ml_train()
