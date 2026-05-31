# DataHub Setup Guide

Deploy DataHub trên Kind cluster để tracking data lineage cho fraud detection project.

| Component | Version | Helm Chart |
|---|---|---|
| DataHub | v1.5.0 | `datahub/datahub 0.9.12` |
| OpenSearch | (bundled) | `datahub/datahub-prerequisites 0.3.0` |
| MySQL 8.0 | (bundled) | `datahub/datahub-prerequisites 0.3.0` |
| Kafka | (bundled) | `datahub/datahub-prerequisites 0.3.0` |

---

## Prerequisites

- Kind cluster `fraud-detection` đang chạy
- Helm repos: `apache-airflow`, `minio`, `bitnami` đã có
- DataHub lineage plugin (`acryl-datahub` + `acryl-datahub-airflow-plugin`) được bake vào Docker image qua `infra/docker/airflow/requirements-governance.txt` — không cần cài thủ công

---

## 1. Add Helm Repo

```bash
helm repo add datahub https://helm.datahubproject.io/
helm repo update
```

---

## 2. Deploy Prerequisites

Prerequisites bao gồm OpenSearch, Kafka, và MySQL.

```bash
kubectl create namespace datahub

# Tạo MySQL secret trước khi deploy
kubectl create secret generic mysql-secrets \
  --namespace datahub \
  --from-literal=mysql-root-password=datahub \
  --from-literal=mysql-password=datahub \
  --from-literal=mysql-replication-password=datahub

helm install prerequisites datahub/datahub-prerequisites \
  --namespace datahub \
  --values infra/helm/datahub/prerequisites-values.yaml \
  --timeout 10m
```

> **`opensearch.persistence.enabled: true`** (đã set trong `prerequisites-values.yaml`): OpenSearch cần PVC để không mất indices khi pod restart. Nếu không có persistence, GMS sẽ crash vào lần restart tiếp theo với lỗi `index_not_found_exception`.

Chờ 3 pods Ready:

```bash
kubectl get pods -n datahub -w
# opensearch-cluster-master-0        1/1   Running
# prerequisites-kafka-controller-0   1/1   Running
# prerequisites-mysql-0              1/1   Running
```

---

## 3. Deploy DataHub

```bash
kubectl create secret generic datahub-mysql-secrets \
  --namespace datahub \
  --from-literal=mysql-password=datahub

helm install datahub datahub/datahub \
  --namespace datahub \
  --values infra/helm/datahub/datahub-values.yaml \
  --timeout 15m
```

Chờ tất cả pods Running/Completed (~5 phút):

```bash
kubectl get pods -n datahub
# datahub-datahub-frontend-xxx   1/1   Running
# datahub-datahub-gms-xxx        1/1   Running
# datahub-system-update-xxx      0/1   Completed
# datahub-system-update-nonblk   0/1   Completed
```

---

## 4. Truy cập UI

Port-forward frontend (terminal riêng):

```bash
kubectl port-forward svc/datahub-datahub-frontend 9002:9002 -n datahub
```

Mở `http://localhost:9002` — login: `datahub` / `datahub`

---

## 5. Lineage tự động qua Airflow Plugin

Lineage được emit **tự động** sau mỗi Airflow task thông qua `acryl-datahub-airflow-plugin` — không cần chạy script thủ công.

### Cách hoạt động

Plugin v2 hooks vào lifecycle của Airflow task (pre/post execute). Sau khi task hoàn thành, plugin đọc `inlets` và `outlets` được khai báo trên `@task(...)` rồi emit dataset lineage lên DataHub GMS.

### Cấu hình (đã có trong `infra/helm/airflow/values.yaml`)

```yaml
env:
  - name: AIRFLOW__LINEAGE__BACKEND
    value: "datahub_provider.lineage.datahub.DatahubLineageBackend"
  - name: AIRFLOW__LINEAGE__DATAHUB_KWARGS
    value: '{"datahub_conn_id": "datahub_rest_default"}'
  - name: AIRFLOW_CONN_DATAHUB_REST_DEFAULT
    value: "http://datahub-datahub-gms.datahub.svc.cluster.local:8080"
```

### Kiểm tra lineage đã được emit

Sau khi chạy bất kỳ DAG nào, vào DataHub UI → **Lineage** của một dataset (ví dụ `feat_customer_unified`) để xem graph.

```bash
# Port-forward DataHub frontend (nếu chưa mở)
kubectl port-forward svc/datahub-datahub-frontend 9002:9002 -n datahub
```

Mở `http://localhost:9002` → search dataset → tab **Lineage**.


---

## 6. Data Lineage
![Data Lineage](../evidence/lineage.png) 


---

## Teardown

```bash
helm uninstall datahub --namespace datahub
helm uninstall prerequisites --namespace datahub
kubectl delete namespace datahub
```
