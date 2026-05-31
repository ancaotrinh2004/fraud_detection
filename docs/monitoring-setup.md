# Monitoring Setup Guide

Setup stack monitoring **Fraud Detection** trên Kubernetes (Kind): Prometheus, Pushgateway, và Grafana.

| Component | App Version | Helm Chart |
|---|---|---|
| Prometheus | 3.4.0 | `prometheus-community/kube-prometheus-stack 85.2.0` |
| Grafana | 13.0.1 | `grafana/grafana 12.3.3` (subchart) |
| Pushgateway | — | `infra/k8s/pushgateway.yaml` |
| Namespace | `monitoring` | — |

---

## Prerequisites

| Tool | Dùng để |
|---|---|
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Tương tác với cluster |
| [helm](https://helm.sh/docs/intro/install/) | Deploy services lên Kubernetes |

---

## Cấu trúc file liên quan

```
infra/
├── helm/monitoring/
│   └── values.yaml          # kube-prometheus-stack overrides
└── k8s/
    └── pushgateway.yaml     # Pushgateway Deployment + Service

scripts/
└── test_monitoring.py       # Script kiểm tra scrape targets + Pushgateway
```

---

## Bước 1 — Tạo namespace

```bash
kubectl create namespace monitoring
```

---

## Bước 2 — Thêm Helm repo

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

---

## Bước 3 — Deploy Pushgateway

```bash
kubectl apply -f infra/k8s/pushgateway.yaml
```

Kiểm tra:

```bash
kubectl get pods -n monitoring -l app=pushgateway
# pushgateway-xxx   1/1   Running
```

---

## Bước 4 — Deploy kube-prometheus-stack

```bash
helm upgrade --install kube-prom prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --version 85.2.0 \
  -f infra/helm/monitoring/values.yaml \
  --timeout 10m
```

Kiểm tra tất cả pods Running (chờ ~3 phút để Grafana khởi động hoàn toàn):

```bash
kubectl get pods -n monitoring
```

Expected:
```
alertmanager-kube-prom-kube-prometheus-alertmanager-0   2/2   Running
kube-prom-grafana-xxx                                   3/3   Running
kube-prom-kube-prometheus-operator-xxx                  1/1   Running
kube-prom-kube-state-metrics-xxx                        1/1   Running
kube-prom-prometheus-node-exporter-xxx                  1/1   Running
prometheus-kube-prom-kube-prometheus-prometheus-0       2/2   Running
pushgateway-xxx                                         1/1   Running
```

---

## Bước 5 — Kiểm tra

```bash
python scripts/test_monitoring.py
```

Expected output:
```
[PASS] airflow-statsd          → UP
[PASS] minio                   → UP
[PASS] pushgateway             → UP
[PASS] Pushgateway is reachable on :9091
[PASS] Pushed test metric to Pushgateway (HTTP 200)
[PASS] Prometheus scraped test metric from Pushgateway ✓
```

---

## Bước 6 — Truy cập Grafana

```bash
kubectl port-forward svc/kube-prom-grafana -n monitoring 3000:80
```

Mở **http://localhost:3000**

| Field    | Giá trị              |
|----------|----------------------|
| Username | `admin`              |
| Password | `fraud-grafana-2025` |

---

## Bước 7 — Truy cập Prometheus UI

```bash
kubectl port-forward svc/kube-prom-kube-prometheus-prometheus -n monitoring 9090:9090
```

Mở **http://localhost:9090** → **Status → Targets** để kiểm tra 3 scrape targets UP:
- `airflow-statsd`
- `minio`
- `pushgateway`

---

## Cập nhật values.yaml

```bash
helm upgrade kube-prom prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --version 85.2.0 \
  -f infra/helm/monitoring/values.yaml \
  --timeout 10m
```
