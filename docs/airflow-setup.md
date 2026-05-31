# Airflow Setup Guide

Setup toàn bộ stack **Fraud Detection** trên Kubernetes (Kind): PostgreSQL, MinIO, và Apache Airflow.

| Component | App Version | Helm Chart |
|---|---|---|
| Apache Airflow | **3.2.0** | `apache-airflow 1.21.0` |
| PostgreSQL | 18.3.0 | `bitnami/postgresql 18.6.2` |
| MinIO | RELEASE.2024-12-18 | `minio/minio 5.4.0` |
| Kubernetes | Kind (local) | — |
| Executor | CeleryExecutor | — |

---

## Prerequisites

| Tool | Dùng để |
|---|---|
| [Docker](https://docs.docker.com/get-docker/) | Build và push image |
| [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) | Tạo Kubernetes cluster local |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Tương tác với cluster |
| [helm](https://helm.sh/docs/intro/install/) | Deploy services lên Kubernetes |
| [uv](https://docs.astral.sh/uv/) | Chạy scripts Python (upload data, init schema) |

---

## Cấu trúc file liên quan

```
config/
└── pipeline_config.yaml      # Cấu hình paths, credentials cho pipelines

infra/
├── docker/airflow/
│   ├── Dockerfile                  # Custom image — extend apache/airflow:3.2.0
│   ├── requirements.txt            # Pipeline deps (deltalake, boto3, s3fs, psycopg2)
│   └── requirements-governance.txt # DataHub lineage plugin (separate layer)
├── helm/airflow/
│   └── values.yaml           # Helm values cho Kind cluster
└── k8s/
    └── fraud-pipeline-secret.yaml  # K8s Secret (tham khảo — không dùng cho local)

dags/                          # DAG files (TaskFlow API)
scripts/
├── upload_raw_to_minio.py     # Upload raw data lên MinIO
└── init_gold_schema.py        # Khởi tạo Gold schema trên PostgreSQL
src/pipelines/                 # Pipeline source code (baked vào Docker image)
```

---

## Bước 1 — Tạo Kind cluster

```bash
kind create cluster --name fraud-detection
```

Kiểm tra cluster:

```bash
kind get clusters
# fraud-detection

kubectl config current-context
# kind-fraud-detection
```

---

## Bước 2 — Thêm Helm repos

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add minio https://charts.min.io/
helm repo update
```

---

## Bước 3 — Tạo namespaces

```bash
kubectl create namespace fraud-infra
kubectl create namespace airflow
```

---

## Bước 4 — Deploy PostgreSQL

PostgreSQL lưu **Gold tables** (dim, fact, feature tables) và **pipeline run logs**.

```bash
helm install fraud-postgres bitnami/postgresql \
  --namespace fraud-infra \
  --set auth.username=fraud_user \
  --set auth.password=fraud_pass \
  --set auth.database=fraud_detection \
  --set primary.persistence.size=5Gi
```

Kiểm tra PostgreSQL đang chạy:

```bash
kubectl get pods -n fraud-infra -l app.kubernetes.io/name=postgresql
# fraud-postgres-postgresql-0   1/1   Running
```

**Service URL trong cluster:**
```
fraud-postgres-postgresql.fraud-infra.svc.cluster.local:5432
```

---

## Bước 5 — Deploy MinIO

MinIO là S3-compatible object storage cho **Bronze và Silver Delta Lake** tables.

```bash
helm install fraud-minio minio/minio \
  --namespace fraud-infra \
  --set mode=standalone \
  --set rootUser=fraud_minio_user \
  --set rootPassword=fraud_minio_pass \
  --set persistence.size=20Gi \
  --set resources.requests.memory=512Mi
```

Kiểm tra MinIO đang chạy:

```bash
kubectl get pods -n fraud-infra -l app=minio
# fraud-minio-xxx   1/1   Running
```

**Service URLs trong cluster:**
```
http://fraud-minio.fraud-infra.svc.cluster.local:9000   (S3 API)
http://fraud-minio-console.fraud-infra.svc.cluster.local:9001  (Console UI)
```

### Truy cập MinIO Console từ host


```bash
kubectl port-forward svc/fraud-minio -n fraud-infra 9000:9000 9001:9001
```

Mở trình duyệt tại **http://localhost:9001** (Console UI).

| Field    | Giá trị            |
|----------|--------------------|
| Username | `fraud_minio_user` |
| Password | `fraud_minio_pass` |

---

## Bước 6 — Upload raw data lên MinIO

Port-forward MinIO trước (xem Bước 5), sau đó chạy script upload từ thư mục gốc project:

```bash
uv run scripts/upload_raw_to_minio.py
```

Script sẽ tự tạo buckets (`raw`, `bronze`, `silver`) nếu chưa có, rồi upload:
- `s3://raw/offline/customers.parquet`
- `s3://raw/offline/merchants.parquet`
- `s3://raw/offline/cards.parquet`
- `s3://raw/offline/transactions/` (partitioned by `transaction_date`)
- `s3://raw/offline/transaction_items/` (partitioned by `transaction_date`)
- `s3://raw/streaming/fraud_events.json` (~9.4 GB)


---

## Bước 7 — Build và push Docker image

Image extend từ `apache/airflow:3.2.0`, bake `src/pipelines/` vào trong.

> **Chạy từ thư mục gốc project** (build context phải là root để `COPY src/` hoạt động đúng).

```bash
# Build
docker build \
  -f infra/docker/airflow/Dockerfile \
  -t ancaotrinh/fraud-airflow:latest \
  .

# Push lên Docker Hub
docker push ancaotrinh/fraud-airflow:latest
```

> **2 layer build**: Layer 1 cài `requirements.txt` (ML/lakehouse deps — cached). Layer 2 cài `requirements-governance.txt` (DataHub lineage plugin — layer riêng để không invalidate cache khi chỉ update governance deps).

---

## Bước 8 — Tạo ConfigMaps

### ConfigMap DAGs

DAG files được mount vào pods qua ConfigMap. Dùng `subPath` per file để tránh lỗi [ConfigMap symlink loop](#tại-sao-dùng-subpath-khi-mount-configmap-cho-dags).

```bash
kubectl create configmap airflow-dags -n airflow \
  --from-file=dag_bronze_ingest.py=dags/dag_bronze_ingest.py \
  --from-file=dag_silver_transform.py=dags/dag_silver_transform.py \
  --from-file=dag_gold_model.py=dags/dag_gold_model.py \
  --from-file=dag_feat_customer_90d.py=dags/dag_feat_customer_90d.py \
  --from-file=dag_feat_stream_30m.py=dags/dag_feat_stream_30m.py \
  --from-file=dag_feat_unified.py=dags/dag_feat_unified.py \
  --from-file=dag_ml_label.py=dags/dag_ml_label.py \
  --from-file=dag_ml_train.py=dags/dag_ml_train.py \
  --from-file=dag_ml_batch_score.py=dags/dag_ml_batch_score.py \
  --from-file=dag_drift_monitor.py=dags/dag_drift_monitor.py \
  --from-file=dag_feat_backfill.py=dags/dag_feat_backfill.py \
  --from-file=dag_ml_retrain_trigger.py=dags/dag_ml_retrain_trigger.py
```

### ConfigMap Pipeline Config

Pipeline config được inject vào pods qua ConfigMap (best practice — không bake vào image).

```bash
kubectl create configmap fraud-pipeline-config -n airflow \
  --from-file=pipeline_config.yaml=config/pipeline_config.yaml
```

---

## Bước 9 — Cài đặt Airflow bằng Helm

```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.21.0 \
  --values infra/helm/airflow/values.yaml \
  --timeout 10m
```


---

## Bước 10 — Tạo admin user

```bash
kubectl exec -n airflow deployment/airflow-api-server -- \
  airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@fraud-detection.local \
  --password admin123
```

---

## Bước 11 — Khởi tạo Gold schema trên PostgreSQL

Chạy script init schema (cần port-forward PostgreSQL):

```bash
kubectl port-forward svc/fraud-postgres-postgresql -n fraud-infra 15432:5432
```

Cập nhật `config/pipeline_config.yaml` để dùng `localhost:15432` khi chạy script local, sau đó:

```bash
uv run scripts/init_gold_schema.py
```

Script tạo schema `gold_fraud` và toàn bộ bảng: `dim_date`, `dim_customer`, `dim_merchant`, `dim_card`, `fact_transaction`, `fact_fraud_event`, `feat_customer_90d`, `feat_stream_30m`, `feat_customer_unified`.

> Khi Airflow chạy pipeline, config dùng cluster-internal URL nên không cần port-forward.

---

## Bước 12 — Truy cập Airflow UI

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

Mở **http://localhost:8080**

| Field    | Giá trị    |
|----------|------------|
| Username | `admin`    |
| Password | `admin123` |

Kiểm tra 12 DAGs đã được nhận diện:

```bash
kubectl exec -n airflow deployment/airflow-dag-processor -c dag-processor \
  -- airflow dags list
```

---

## Bước 13 — Chạy pipelines theo thứ tự

Trigger thủ công từ Airflow UI hoặc CLI theo thứ tự:

```
bronze_ingest → silver_transform → gold_model
                                       │
                          ┌────────────┴────────────┐
                feat_customer_90d           feat_stream_30m
                          └────────────┬────────────┘
                                  feat_unified
                                       │
                                   ml_label
                                       │
                                   ml_train
```

DAGs chạy tự động theo schedule sau khi trigger lần đầu. `ml_batch_score`, `drift_monitor`, `ml_retrain_trigger` và `feat_backfill` chạy theo lịch riêng.

---

## Cập nhật sau khi thay đổi code

### Thay đổi `src/pipelines/` — rebuild image

```bash
# 1. Rebuild và push
docker build -f infra/docker/airflow/Dockerfile -t ancaotrinh/fraud-airflow:latest .
docker push ancaotrinh/fraud-airflow:latest

# 2. Rolling restart các Airflow components
kubectl rollout restart deployment -n airflow
kubectl rollout restart statefulset/airflow-worker -n airflow
```

### Thay đổi `dags/` — chỉ cần cập nhật ConfigMap

```bash
kubectl create configmap airflow-dags -n airflow \
  --from-file=dag_bronze_ingest.py=dags/dag_bronze_ingest.py \
  --from-file=dag_silver_transform.py=dags/dag_silver_transform.py \
  --from-file=dag_gold_model.py=dags/dag_gold_model.py \
  --from-file=dag_feat_customer_90d.py=dags/dag_feat_customer_90d.py \
  --from-file=dag_feat_stream_30m.py=dags/dag_feat_stream_30m.py \
  --from-file=dag_feat_unified.py=dags/dag_feat_unified.py \
  --from-file=dag_ml_label.py=dags/dag_ml_label.py \
  --from-file=dag_ml_train.py=dags/dag_ml_train.py \
  --from-file=dag_ml_batch_score.py=dags/dag_ml_batch_score.py \
  --from-file=dag_drift_monitor.py=dags/dag_drift_monitor.py \
  --from-file=dag_feat_backfill.py=dags/dag_feat_backfill.py \
  --from-file=dag_ml_retrain_trigger.py=dags/dag_ml_retrain_trigger.py \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment/airflow-dag-processor -n airflow
```

### Thay đổi `config/pipeline_config.yaml`

```bash
kubectl create configmap fraud-pipeline-config -n airflow \
  --from-file=pipeline_config.yaml=config/pipeline_config.yaml \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart statefulset/airflow-worker -n airflow
kubectl rollout restart deployment/airflow-dag-processor -n airflow
```

### Thay đổi `infra/helm/airflow/values.yaml`

```bash
helm upgrade airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.21.0 \
  --values infra/helm/airflow/values.yaml \
  --timeout 5m
```

---

## Ghi chú kỹ thuật

### Tại sao dùng subPath khi mount ConfigMap cho DAGs?

Khi mount cả thư mục từ ConfigMap, Kubernetes tạo symlinks nội bộ (`..data/`, `..2026_xx_xx.../`). Airflow 3.x's DAG file walker phát hiện vòng lặp và crash:

```
RuntimeError: Detected recursive loop when walking DAG directory /opt/airflow/dags
```

Giải pháp: mount từng file DAG riêng lẻ bằng `subPath`. Xem `infra/helm/airflow/values.yaml`.

### Tại sao `src/pipelines/` bake vào image thay vì ConfigMap?

ConfigMap có giới hạn 1 MB và không hỗ trợ cấu trúc thư mục lồng nhau. Bake vào image đảm bảo mọi worker pod đều có đủ code, nhất quán với quy trình production.

### Tại sao dùng `PYTHONPATH=/opt/airflow`?

Airflow task runner chạy trong working directory `/opt/airflow`. Không có `PYTHONPATH`, Python không tìm thấy package `src` và throw `ModuleNotFoundError: No module named 'src'`. Env var này được set trong `values.yaml` section `env:`.

### Tại sao không pin phiên bản pandas/sqlalchemy trong requirements?

Airflow base image đã quản lý các packages này. Pin phiên bản cũ hơn sẽ downgrade và phá vỡ Airflow core (ví dụ: `apache-airflow-core` yêu cầu `sqlalchemy>=2.0.48`). Chỉ thêm packages **mới** không có trong base image.
