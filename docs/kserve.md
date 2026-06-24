# 🚀 KServe Inference Setup

> Deploy the **online fraud-scoring service** on KServe — a custom KServe model (`FraudModel`) speaking the open-inference-protocol **V2**, served via Knative + Istio. It loads the MLflow Production model and fetches features from `feat_customer_unified` per request.

<table>
<tr><th>Component</th><th>Version</th></tr>
<tr><td>cert-manager</td><td>v1.19.0</td></tr>
<tr><td>Istio</td><td>latest (helm)</td></tr>
<tr><td>Knative Serving</td><td>v1.19.6</td></tr>
<tr><td>KServe</td><td>v0.12.0</td></tr>
</table>

> [!NOTE]
> **Prerequisites:** Kind (Kubernetes 1.27+), Helm 3, and **cert-manager v1.19.0** installed (`kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.0/cert-manager.yaml`).

---

## 1. Istio

```bash
helm repo add istio https://istio-release.storage.googleapis.com/charts && helm repo update istio
kubectl create namespace istio-system
helm install istio-base istio/base -n istio-system --set defaultRevision=default
helm install istiod istio/istiod -n istio-system --wait
helm install istio-ingress istio/gateway -n istio-system --wait
```

## 2. Knative Serving v1.19.6

```bash
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.19.6/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.19.6/serving-core.yaml
```

> [!IMPORTANT]
> **Kind / K8s < 1.32 only** — patch the version check on all Knative deployments, else they crash:
> ```bash
> kubectl set env deployment/controller deployment/webhook deployment/autoscaler deployment/activator \
>   -n knative-serving KUBERNETES_MIN_VERSION=v1.27.0
> ```

## 3. net-istio + config-domain

```bash
kubectl apply -f infra/k8s/net-istio.yaml
kubectl set env deployment/net-istio-controller deployment/net-istio-webhook \
  -n knative-serving KUBERNETES_MIN_VERSION=v1.27.0      # Kind/K8s < 1.32 only
kubectl patch configmap/config-domain -n knative-serving --type merge \
  --patch '{"data":{"example.com":""}}'                   # no-DNS for local
```

## 4. KServe v0.12.0

```bash
helm install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.12.0
helm install kserve oci://ghcr.io/kserve/charts/kserve --version v0.12.0
```

> [!WARNING]
> **gcr.io deprecated fix** — only the `kube-rbac-proxy` sidecar uses a dead `gcr.io` image (ErrImagePull). The `manager` container pulls fine — **do not patch it**. In v0.12.0 the real order is `containers[0]=kube-rbac-proxy`, `containers[1]=manager`, so patch **index 0 only**:
> ```bash
> kubectl get deploy kserve-controller-manager -n default \
>   -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{" -> "}{.image}{"\n"}{end}'   # confirm order
> kubectl patch deployment kserve-controller-manager -n default --type=json -p='[
>   {"op":"replace","path":"/spec/template/spec/containers/0/image","value":"quay.io/brancz/kube-rbac-proxy:v0.13.1"}]'
> ```
>
> **`helm release: failed` is harmless** — the modelmesh `ClusterServingRuntime` objects can fail at install (webhook cert race). We use a custom-container predictor, so no serving runtime is needed. **Don't** `helm upgrade` to "fix" it — that hits field-manager conflicts with the cert-manager caBundle + your image patch.

```bash
kubectl get pods -n default | grep kserve   # kserve-controller-manager-xxx  2/2  Running
```

---

## 5. Build & push the inference image

The server (`src/inference/server.py`) is a **KServe custom model** (`class FraudModel(kserve.Model)` on `kserve.ModelServer`) — open-inference-protocol **V2**, not FastAPI.

```bash
# from project root — image must contain src/ + configs/ (plural!)
docker build --no-cache -f infra/docker/inference/Dockerfile -t ancaotrinh/fraud-inference:latest .
docker push ancaotrinh/fraud-inference:latest
```

> [!TIP]
> The predictor uses `imagePullPolicy: Always`, but pushing a new image with the **same** `:latest` tag won't cut a new Knative revision — restart to force a re-pull and verify the digest matches:
> ```bash
> kubectl rollout restart deployment -n fraud-infra -l serving.knative.dev/service=fraud-predictor
> kubectl get pod -n fraud-infra -l serving.knative.dev/service=fraud-predictor \
>   -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="kserve-container")].imageID}'
> ```

---

## 6. Deploy the InferenceService

> [!IMPORTANT]
> 1. Apply the secret: `kubectl apply -f infra/k8s/fraud-pipeline-secret.yaml`
> 2. A **Production model must exist in MLflow** (run `ml_train`) — the predictor loads it at startup and crashes without it. Check: `curl -s http://mlflow.fraud-infra.svc.cluster.local:5000/api/2.0/mlflow/registered-models/search` → `fraud-xgboost` with `current_stage: Production`.

```bash
kubectl apply -f infra/k8s/fraud-inference.yaml
```

```bash
kubectl get inferenceservice -n fraud-infra
# NAME    URL                                    READY
# fraud   http://fraud.fraud-infra.example.com   True
kubectl get pods -n fraud-infra | grep fraud-predictor
# fraud-predictor-00001-deployment-xxxxx   2/2   Running
```

---

## ✅ Test

```bash
kubectl port-forward -n fraud-infra \
  $(kubectl get pod -n fraud-infra -l serving.knative.dev/service=fraud-predictor -o name | head -1) 8080:8080

curl http://localhost:8080/v2/health/ready    # {"ready":true}

curl -s -X POST http://localhost:8080/v2/models/fraud-predictor/infer \
  -H "Content-Type: application/json" \
  -d '{"inputs":[{"name":"instances","datatype":"BYTES","shape":[1],
       "data":["{\"customer_id\": \"C0000019\", \"txn_amount\": 150.0, \"txn_hour\": 14}"]}]}'
# {"model_name":"fraud-predictor","outputs":[{"name":"predictions","datatype":"BYTES","shape":[1],
#   "data":["{\"customer_id\": \"C0000019\", \"fraud_score\": 0.2517, \"is_fraud\": false}"]}]}
```

Or the test script (4 cases — normal / suspicious / unknown-customer / batch):

```bash
python scripts/validate/test_inference.py --host localhost --port 8080
```

---

## 📖 API Reference

`POST /v2/models/fraud-predictor/infer` — open-inference-protocol V2. Each transaction is a `json.dumps`-encoded element (datatype `BYTES`) in `inputs[0].data`:

```json
{ "inputs": [ { "name": "instances", "datatype": "BYTES", "shape": [1],
  "data": ["{\"customer_id\": \"C0000019\", \"txn_amount\": 150.0, \"txn_hour\": 14, \"is_declined_txn\": 0, \"is_foreign_txn\": 0}"] } ] }
```

| Field | Type | Required | Description |
|---|---|---|---|
| `customer_id` | string | yes | feature-store lookup key |
| `txn_amount` | float | yes | transaction amount |
| `txn_hour` | int (0–23) | yes | transaction hour |
| `is_declined_txn` | int (0/1) | no | default 0 |
| `is_foreign_txn` | int (0/1) | no | default 0 |

**Response** — `outputs[0].data` holds the prediction JSONs in input order: `{customer_id, fraud_score, is_fraud}`. The server fetches pre-computed features from `gold_fraud.feat_customer_unified` by `customer_id` (zero-fill if missing).
