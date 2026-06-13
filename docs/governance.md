# 🛡️ Data Governance Setup

> Gold-layer governance in two halves that share one source of truth:
> **Great Expectations (GE-native)** for data **quality & contracts** (per-column rules → Data Docs + a pipeline quality gate), and **DataHub** for the **catalog & lineage** (Bronze → Silver → Gold → Feature graph, plus the contracts/assertions pushed from GE).

<table>
<tr><th>Layer</th><th>Tool</th><th>Version / Chart</th></tr>
<tr><td>Quality & contracts</td><td>Great Expectations (GE-native suites)</td><td><code>src/pipelines/governance/</code></td></tr>
<tr><td>Catalog & lineage</td><td>DataHub</td><td>app <b>v1.6.0</b> · <code>datahub/datahub 1.0.0</code></td></tr>
<tr><td>DataHub backing services</td><td>OpenSearch / Kafka / MySQL</td><td><code>datahub/datahub-prerequisites 0.3.0</code></td></tr>
</table>

> [!NOTE]
> **Prerequisites:** Kind cluster running. The DataHub lineage plugin (`acryl-datahub` + `acryl-datahub-airflow-plugin`) is baked into the Airflow image via `infra/docker/airflow/requirements-governance.txt` — no manual install.
>
> ⚠️ DataHub is heavy (OpenSearch + Kafka + MySQL + GMS). Check free RAM (`free -h`) before installing — budget ~3 GiB for the core on top of the prerequisites.

---

## Part A — Data Quality & Contracts (Great Expectations)

| Component | Role | File |
|---|---|---|
| GE ExpectationSuites | Single source of truth: per-column rules + descriptions | `src/pipelines/governance/suites.py` |
| GE runner | Validate DataFrame → Data Docs, classify critical/warn | `src/pipelines/governance/ge_validate.py` |
| Quality gate (DAG) | Block pipeline on critical violation | `dags/dag_batch_gold.py` (`validate_contracts`) |
| Standalone validator | Validate Postgres + build Data Docs | `scripts/validate/validate_contracts.py` |
| DataHub emitter | Push description + contract + assertions | `scripts/governance/emit_contracts.py` |

```
suites.py  ← SINGLE SOURCE OF TRUTH (6 Gold tables: per-column type/nullable/description/checks)
   ├──▶ GE Checkpoint ──▶ Data Docs (HTML, per-column pass/fail)
   │           └──▶ raise on critical fail   ← quality gate in batch_gold DAG
   └──▶ DataHub (description + DataContract + Assertions)   ← catalog (Part B)
```

> [!NOTE]
> **Severity** (in each expectation's `meta`): `critical` (schema / `not_null` / `unique`) → **fail pipeline**; `warn` (`accepted_values` / `value_between`) → log only.
> **6 contracted tables:** `fact_transaction`, `dim_customer`, `dim_card`, `obt_transaction_fraud_summary`, `feat_customer_unified`, `ml_fraud_training`.

### 1. Automatic quality gate (in-pipeline)

The `validate_contracts` task at the end of `batch_gold` validates `dim_customer / dim_card / fact_transaction / obt_transaction_fraud_summary` after the Gold load; a **critical** violation fails the task and blocks downstream. `ml_fraud_training` is validated inside `train.py` before training. No manual action — just run the DAG.

### 2. Build Data Docs (standalone)

The HTML report showing **per-column validation** — used for evidence.

```bash
kubectl port-forward svc/fraud-postgres-postgresql -n fraud-infra 15432:5432 &
python scripts/validate/validate_contracts.py
# → Data Docs (per-column results): file:///.../gx_project/gx/uncommitted/data_docs/local_site/index.html
```

Open that file — each table shows every expectation with ✅/❌, observed value, and unexpected-row count:

<p align="center">
  <img src="assets/data_governance.png" width="820" alt="Great Expectations Data Docs — per-column contract results"/>
  <br/><em>GE Data Docs — per-column data-contract results (type, not-null, accepted-values, ranges) per Gold table.</em>
</p>

### 3. Add / edit a contract

Edit `src/pipelines/governance/suites.py` — add a `Column(...)` to the right `TableSpec`. Validation, Data Docs, and DataHub all update from this single source.

```python
Column("new_col", "numeric", "Column description", nullable=False, value_between=(0, 100))
```

> [!TIP]
> Legacy `contracts/*.yaml` are **replaced** by `suites.py` (GE-native) — kept for reference only, no longer read by code.

---

## Part B — Catalog & Lineage (DataHub)

### 1. Prerequisites (OpenSearch + Kafka + MySQL)

```bash
helm repo add datahub https://helm.datahubproject.io/ && helm repo update
kubectl create namespace datahub

kubectl create secret generic mysql-secrets --namespace datahub \
  --from-literal=mysql-root-password=datahub \
  --from-literal=mysql-password=datahub \
  --from-literal=mysql-replication-password=datahub

helm install prerequisites datahub/datahub-prerequisites \
  --namespace datahub --values infra/helm/datahub/prerequisites-values.yaml --timeout 10m
```

> [!IMPORTANT]
> `opensearch.persistence.enabled: true` is required — without a PVC, GMS crashes on the next restart with `index_not_found_exception`.

Wait for 3 pods Ready: `opensearch-cluster-master-0`, `prerequisites-kafka-controller-0`, `prerequisites-mysql-0`.

### 2. DataHub core

```bash
kubectl create secret generic datahub-mysql-secrets --namespace datahub \
  --from-literal=mysql-password=datahub

helm install datahub datahub/datahub \
  --namespace datahub --values infra/helm/datahub/datahub-values.yaml --timeout 15m
```

Wait (~5 min) — `datahub-datahub-gms` + `datahub-datahub-frontend` Running, `datahub-system-update-*` Completed. (The mae/mce consumers are embedded in GMS.)

### 3. Auto-lineage via the Airflow plugin

Lineage is emitted **automatically** after each Airflow task — the v2 plugin reads `inlets`/`outlets` on `@task(...)` and pushes dataset lineage to GMS. Config (in `infra/helm/airflow/values.yaml`):

```yaml
env:
  - name: AIRFLOW__LINEAGE__BACKEND
    value: "datahub_provider.lineage.datahub.DatahubLineageBackend"
  - name: AIRFLOW_CONN_DATAHUB_REST_DEFAULT
    value: "http://datahub-datahub-gms.datahub.svc.cluster.local:8080"
```

### 4. Push GE contracts to the catalog

The contracts/assertions defined in Part A's `suites.py` are pushed into DataHub so each Gold table carries its column descriptions, rules, and DataContract:

```bash
kubectl port-forward svc/datahub-datahub-gms 8080:8080 -n datahub &
python scripts/governance/emit_contracts.py
```

### 5. Verify

```bash
kubectl port-forward svc/datahub-datahub-frontend 9002:9002 -n datahub
# http://localhost:9002  (datahub / datahub)
```

After any DAG runs, search a dataset (e.g. `feat_customer_unified`) → **Lineage** tab for the upstream graph; the **Schema / Assertions / Contracts** tabs show what `emit_contracts.py` pushed:

<p align="center">
  <img src="assets/data_lineage.png" width="820" alt="DataHub lineage graph for the fraud Gold tables"/>
  <br/><em>DataHub — Bronze → Silver → Gold → Feature lineage, auto-emitted by the Airflow plugin.</em>
</p>

> [!NOTE]
> Lineage populates only after pipelines run. Until then DataHub is empty.

---

## 🧹 Teardown

```bash
rm -rf gx_project/                              # local GE project + Data Docs
helm uninstall datahub --namespace datahub
helm uninstall prerequisites --namespace datahub
kubectl delete namespace datahub
```
