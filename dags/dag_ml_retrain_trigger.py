from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

IMAGE = "ancaotrinh/fraud-airflow:latest"
NAMESPACE = "airflow"

_env = [
    k8s.V1EnvVar(name="PYTHONPATH",              value="/opt/airflow"),
    k8s.V1EnvVar(name="POSTGRES_PASSWORD",        value="fraud_pass"),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY",         value="fraud_minio_user"),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY",         value="fraud_minio_pass"),
    k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID",        value="fraud_minio_user"),
    k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY",    value="fraud_minio_pass"),
    k8s.V1EnvVar(name="MLFLOW_S3_ENDPOINT_URL",   value="http://fraud-minio.fraud-infra.svc.cluster.local:9000"),
    k8s.V1EnvVar(name="MLFLOW_TRACKING_URI",      value="http://mlflow.fraud-infra.svc.cluster.local:5000"),
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


@dag(
    dag_id="dag_ml_retrain_trigger",
    schedule="0 7 * * *",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retraining"],
)
def ml_retrain_trigger():

    check_and_retrain = KubernetesPodOperator(
        task_id="check_and_retrain",
        name="ml-retrain-trigger-pod",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="Always",
        cmds=["python", "-c"],
        arguments=[
            "from src.pipelines.ml.retrain_trigger import run_retrain_trigger; "
            "run_retrain_trigger()"
        ],
        env_vars=_env,
        container_resources=_resources,
        volume_mounts=_volume_mounts,
        volumes=_volumes,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_and_retrain


ml_retrain_trigger()
