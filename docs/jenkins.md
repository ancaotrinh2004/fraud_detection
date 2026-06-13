# Jenkins CI/CD Setup

Jenkins chạy trên Kubernetes (Kind cluster) qua Helm chart.
Ba pipeline Multibranch dùng changeset detection để chỉ deploy component bị thay đổi.

---

## 0.1. Install Jenkins

```bash
helm repo add jenkins https://charts.jenkins.io
helm repo update
kubectl create namespace jenkins
helm install jenkins jenkins/jenkins -n jenkins
```

Lấy password admin:

```bash
kubectl exec --namespace jenkins -it svc/jenkins -c jenkins \
  -- /bin/cat /run/secrets/additional/chart-admin-password && echo
```

Truy cập UI (port-forward):

```bash
kubectl --namespace jenkins port-forward svc/jenkins 8080:8080
```

```
URL:      http://localhost:8080
Username: admin
Password: <output từ lệnh trên>
```

---

## 0.2. Install Jenkins Plugins

Vào **Manage Jenkins → Plugins → Available plugins**, cài:

- `GitHub Branch Source`
- `Docker`
- `Docker Pipeline`
- `Kubernetes CLI`

---

## 0.3. Init repo và push lên GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <REMOTE_URL>
git push -u origin main
```

Tạo **Personal Access Token** trên GitHub (Settings → Developer settings → Tokens), cấp quyền `repo` — dùng ở bước 0.6.

---

## 0.4. Expose Jenkins qua ngrok

Trong terminal riêng:

```bash
kubectl --namespace jenkins port-forward svc/jenkins 8080:8080
```

```bash
ngrok http 8080
```

Lấy URL ngrok (dạng `https://<id>.ngrok-free.app`).

---

## 0.5. Add Webhook vào GitHub

Vào **GitHub repo → Settings → Webhooks → Add webhook**:

- **Payload URL**: `https://<id>.ngrok-free.app/github-webhook/`
- **Content type**: `application/json`
- **Events**: `Just the push event` (hoặc `Send me everything`)

---

## 0.6. Configure Jenkins Credentials

Vào **Manage Jenkins → Credentials → Global → Add Credentials**:

**GitHub token**:
- Kind: `Username with password`
- Username: GitHub username
- Password: Personal Access Token (bước 0.3)
- ID: `github-token`

**Docker Hub token**:
- Kind: `Username with password`
- Username: `ancaotrinh`
- Password: Docker Hub Access Token
- ID: `dockerhub-token`

---

## 0.7. Tạo Multibranch Pipelines

Tạo **3 job**, mỗi job cho 1 track:

| Job Name | Jenkinsfile |
|---|---|
| `fraud-track-a` | `jenkins/Jenkinsfile.track_a` |
| `fraud-track-b` | `jenkins/Jenkinsfile.track_b` |
| `fraud-track-c` | `jenkins/Jenkinsfile.track_c` |

Các bước cho mỗi job:

1. **New Item** → nhập tên → chọn **Multibranch Pipeline**
2. **Branch Sources** → Add source → **GitHub**
   - Credentials: chọn `github-token`
   - Repository URL: URL repo GitHub
3. **Build Configuration** → Script Path: ví dụ `jenkins/Jenkinsfile.track_a`
4. **Scan Multibranch Pipeline Triggers** → tick `Periodically if not otherwise run` → interval `1 minute`
5. Save → **Scan Multibranch Pipeline Now**

---

## 0.8. Apply RBAC cho Jenkins Service Account

```bash
kubectl apply -f infra/k8s/jenkins-role.yaml
```

File này tạo `ClusterRole` + `ClusterRoleBinding` cho service account `default` trong namespace `jenkins`.
ClusterRole là bắt buộc vì pipeline cần tương tác cross-namespace và tạo namespace mới.

**Quyền được cấp:**

| API Group | Resources | Dùng bởi |
|---|---|---|
| core (`""`) | pods, pods/exec, pods/portforward, pods/log | Track A (exec), Track B (port-forward) |
| core (`""`) | configmaps, secrets | Track A (DAG ConfigMap), Track C (secrets) |
| core (`""`) | namespaces, services, serviceaccounts | Track C (tạo namespace) |
| `apps` | deployments, statefulsets, replicasets | Track A (rollout restart) |
| `batch` | jobs, cronjobs | Airflow task execution |
| `rbac.authorization.k8s.io` | roles, rolebindings, clusterroles | Helm charts tạo RBAC |
| `serving.kserve.io` | inferenceservices | Track B (patch/apply ISVC) |
| `monitoring.coreos.com` | servicemonitors, prometheusrules | Track C (ServiceMonitor) |
| `networking.knative.dev` | ingresses | Track C (net-istio.yaml) |
| `apiextensions.k8s.io` | customresourcedefinitions | Helm upgrade kiểm tra CRDs |

---

## 0.9. Build và Push Jenkins Agent Image

Pipeline agent cần `helm`, `kubectl`, `docker`, `python3`. Build custom image một lần:

```bash
docker build \
  -f infra/docker/jenkins/Dockerfile \
  -t ancaotrinh/jenkins:latest \
  infra/docker/jenkins/

docker push ancaotrinh/jenkins:latest
```

Image bao gồm:

| Tool | Version |
|---|---|
| kubectl | v1.29.0 |
| Helm | 3.x (latest) |
| Docker CLI | latest stable (CLI only) |
| Python 3 + venv | system |

> Base image: `jenkins/inbound-agent:latest-jdk17` — đây là agent JNLP, không phải Jenkins controller.
> Kind cluster dùng containerd — không có `/var/run/docker.sock` trong node.
> Mỗi pipeline pod chạy thêm **DinD sidecar** (`docker:27-dind`, privileged); agent kết nối qua `DOCKER_HOST=tcp://localhost:2376`.

---

## Changeset Logic

Mỗi stage chỉ chạy khi đúng file thay đổi:

### Track A — Data/ML Pipelines

| Files changed | Stages chạy |
|---|---|
| `src/pipelines/**` | Lint + Tests + Build image + Push + Helm upgrade Airflow |
| `infra/docker/airflow/**` | Build image + Push + Helm upgrade Airflow |
| `infra/helm/airflow/**` | Helm upgrade only (no image rebuild) |
| `dags/**` | Lint + DAG validation + Update ConfigMap + Restart dag-processor |
| `config/pipeline_config.yaml` | Update fraud-pipeline-config ConfigMap + Restart workers |

### Track B — Inference Service

| Files changed | Stages chạy |
|---|---|
| `src/inference/**` | Lint + Tests + Contract + Load test + Build + Push + Patch ISVC + Smoke test |
| `infra/docker/inference/**` | Build + Push + Patch ISVC + Smoke test |
| `infra/k8s/fraud-inference.yaml` | `kubectl apply` (no image rebuild) + Smoke test |

### Track C — IaC

| Files changed | Stages chạy |
|---|---|
| `infra/helm/monitoring/**` | Helm lint + `helm upgrade kube-prometheus-stack 85.2.0` |
| `infra/helm/airflow/**` | Helm lint + `helm upgrade airflow 1.21.0` (values-only) |
| `infra/helm/datahub/prerequisites-values.yaml` | `helm upgrade datahub-prerequisites 0.3.0` |
| `infra/helm/datahub/datahub-values.yaml` | `helm upgrade datahub 0.9.12` |
| `infra/k8s/fraud-pipeline-secret.yaml` | `kubectl apply` secret |
| `infra/k8s/pushgateway.yaml` | `kubectl apply` + rollout status |
| `infra/k8s/fraud-inference-monitor.yaml` | `kubectl apply` ServiceMonitor |
| `infra/k8s/net-istio.yaml` | `kubectl apply` Istio/Knative config |

---

## Manual Deploy

Tất cả 3 pipeline hỗ trợ **manual trigger**:

**Cách 1 — Trigger thủ công trên `main` không có code change:**
```
Changeset rỗng → tự động detect → RUN_DEPLOY=true → tất cả CD stages chạy
```

**Cách 2 — Checkbox `FORCE_DEPLOY`:**
```
Build with Parameters → FORCE_DEPLOY ✓ → tất cả stages chạy (kể cả trên feature branch)
```

---

## Rollback

| Component | Lệnh |
|---|---|
| Airflow | `helm rollback airflow 0 -n airflow` |
| kube-prometheus-stack | `helm rollback kube-prom 0 -n monitoring` |
| DataHub | `helm rollback datahub 0 -n datahub` |
| Inference image | `kubectl patch inferenceservice fraud -n fraud-infra --type=json -p='[{"op":"replace","path":"/spec/predictor/containers/0/image","value":"ancaotrinh/fraud-inference:<prev-sha>"}]'` |

---

## Helm Chart Versions

| Component | Chart | Version |
|---|---|---|
| kube-prometheus-stack | `prometheus-community/kube-prometheus-stack` | `85.2.0` |
| Apache Airflow | `apache-airflow/airflow` | `1.21.0` |
| DataHub prerequisites | `datahub/datahub-prerequisites` | `0.3.0` |
| DataHub | `datahub/datahub` | `0.9.12` |
