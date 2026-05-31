# KServe Setup & Deploy

## Prerequisites
- Kind cluster (Kubernetes 1.27+)
- Helm 3.x
- cert-manager v1.19.0 (already installed)

## 1. Install Istio

```bash
helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update istio

kubectl create namespace istio-system

helm install istio-base istio/base -n istio-system --set defaultRevision=default
helm install istiod istio/istiod -n istio-system --wait
helm install istio-ingress istio/gateway -n istio-system --wait
```

Verify:
```bash
kubectl get pods -n istio-system
# istiod-xxx        Running
# istio-ingress-xxx Running
```

## 2. Install Knative Serving v1.19.6

```bash
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.19.6/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.19.6/serving-core.yaml
```

> **Kind/K8s < 1.32 only** — patch version check trên tất cả deployments:
> ```bash
> kubectl set env deployment/controller deployment/webhook \
>   deployment/autoscaler deployment/activator \
>   -n knative-serving KUBERNETES_MIN_VERSION=v1.27.0
> ```

Verify:
```bash
kubectl get pods -n knative-serving
# activator, autoscaler, controller, webhook → Running
```

## 3. Configure Knative + Istio

```bash
kubectl apply -f infra/k8s/net-istio.yaml
```

> **Kind/K8s < 1.32 only** — patch net-istio deployments:
> ```bash
> kubectl set env deployment/net-istio-controller deployment/net-istio-webhook \
>   -n knative-serving KUBERNETES_MIN_VERSION=v1.27.0
> ```

Configure no-DNS (cho local/Kind):
```bash
kubectl patch configmap/config-domain \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"example.com":""}}'
```

## 4. Install KServe v0.12.0

```bash
helm install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.12.0
helm install kserve oci://ghcr.io/kserve/charts/kserve --version v0.12.0
```

> **gcr.io deprecated fix** — `kube-rbac-proxy` đã chuyển sang `quay.io`. Patch cả 2 containers:
> ```bash
> # Container index 0 (kserve-controller-manager)
> kubectl patch deployment kserve-controller-manager -n default --type=json -p='[
>   {"op":"replace","path":"/spec/template/spec/containers/0/image",
>    "value":"quay.io/brancz/kube-rbac-proxy:v0.13.1"}
> ]'
> # Container index 1 (kube-rbac-proxy sidecar)
> kubectl patch deployment kserve-controller-manager -n default --type=json -p='[
>   {"op":"replace","path":"/spec/template/spec/containers/1/image",
>    "value":"quay.io/brancz/kube-rbac-proxy:v0.13.1"}
> ]'
> ```

Verify:
```bash
kubectl get pods -n default | grep kserve
# kserve-controller-manager-xxx   2/2   Running

kubectl get crd | grep kserve
# inferenceservices.serving.kserve.io
# clusterservingruntimes.serving.kserve.io
```

## 5. Build & Push Inference Image

Server được implement bằng FastAPI thuần (không dùng kserve Python SDK do conflict pydantic v1/v2).

```bash
# Build từ project root
docker build --no-cache -f infra/docker/inference/Dockerfile \
  -t <dockerhub-user>/fraud-inference:<tag> .

docker push <dockerhub-user>/fraud-inference:<tag>
```

Cập nhật `infra/k8s/fraud-inference.yaml` với image name của bạn.

## 6. Deploy InferenceService

```bash
kubectl apply -f infra/k8s/fraud-inference.yaml
```

Verify:
```bash
kubectl get inferenceservice -n fraud-infra
# NAME    URL                                    READY
# fraud   http://fraud.fraud-infra.example.com   True

kubectl get pods -n fraud-infra | grep fraud-predictor
# fraud-predictor-xxxxx   2/2   Running
```

> **Update image sau khi push mới** 
```bash
kubectl apply -f infra/k8s/fraud-inference.yaml
```

## 7. Test

### Health check

```bash
kubectl port-forward -n fraud-infra \
  $(kubectl get pod -n fraud-infra -l serving.knative.dev/service=fraud-predictor -o name | head -1) \
  8080:8080

curl http://localhost:8080/v2/health/ready
# {"status":"ready"}
```

### Inference

```bash
# Single transaction
curl -s -X POST http://localhost:8080/v1/models/fraud:predict \
  -H "Content-Type: application/json" \
  -d '{
    "instances": [{
      "customer_id": "C0000019",
      "txn_amount": 150.0,
      "txn_hour": 14
    }]
  }'

# Response:
# {"predictions":[{"customer_id":"C0000019","fraud_score":0.0312,"is_fraud":false}]}
```

### Test script

```bash
# Port-forward hết (xem trên), sau đó:
python scripts/test_inference.py

# Hoặc chỉ định host/port:
python scripts/test_inference.py --host localhost --port 8080
```

Script chạy 4 test cases: normal, suspicious (đêm + foreign + declined), unknown customer (zero-fill), batch.

### Swagger UI

```
http://localhost:8080/docs
```

## API Reference

`POST /v1/models/fraud:predict`

**Request**
```json
{
  "instances": [
    {
      "customer_id": "C0000019",
      "txn_amount": 150.0,
      "txn_hour": 14,
      "is_declined_txn": 0,
      "is_foreign_txn": 0
    }
  ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `customer_id` | string | yes | ID khách hàng — dùng để lookup feature store |
| `txn_amount` | float | yes | Số tiền giao dịch |
| `txn_hour` | int (0–23) | yes | Giờ giao dịch |
| `is_declined_txn` | int (0/1) | no | Giao dịch bị từ chối (default 0) |
| `is_foreign_txn` | int (0/1) | no | Giao dịch nước ngoài (default 0) |

**Response**
```json
{
  "predictions": [
    {
      "customer_id": "C0000019",
      "fraud_score": 0.0312,
      "is_fraud": false
    }
  ]
}
```

Server tự fetch pre-computed features từ `gold_fraud.feat_customer_unified` theo `customer_id`. Nếu không tìm thấy, dùng zero-fill.

## Version Table

| Component | Version |
|---|---|
| cert-manager | v1.19.0 |
| Istio | latest (helm) |
| Knative Serving | v1.19.6 |
| KServe | v0.12.0 |
