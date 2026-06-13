# 🔀 Kafka Setup

> Deploy **Apache Kafka** (Strimzi operator, KRaft mode) on Kind so the streaming track reads from a real Kafka stream instead of an NDJSON file:
> **Generator → topic `fraud.events.raw` → Airflow consumer → Bronze.**

<table>
<tr><th>Component</th><th>Version</th><th>Deploy</th></tr>
<tr><td>Strimzi Operator</td><td>latest</td><td><code>kubectl create -f strimzi.io/install/latest</code></td></tr>
<tr><td>Kafka (KRaft)</td><td><b>4.1.0</b></td><td><code>infra/k8s/kafka/kafka.yaml</code></td></tr>
<tr><td>Topic <code>fraud.events.raw</code></td><td>3 partitions · RF 1 · 24h</td><td><code>infra/k8s/kafka/kafka-topics.yaml</code></td></tr>
</table>

> [!NOTE]
> **Prerequisites:** Kind cluster running, namespace `fraud-infra` created, and `kafka-python` in your venv (`uv pip install kafka-python`) to run the producer.

---

## 🚀 Setup

### 1. Strimzi operator

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=fraud-infra' -n fraud-infra
kubectl get pods -n fraud-infra | grep strimzi   # strimzi-cluster-operator-xxx  Running
```

### 2. Deploy the Kafka cluster

```bash
kubectl apply -f infra/k8s/kafka/kafka.yaml
kubectl wait kafka/fraud-kafka -n fraud-infra --for=condition=Ready --timeout=300s
```

> [!NOTE]
> **KRaft mode** (no Zookeeper): pod `fraud-kafka-fraud-pool-0` is both controller + broker. Storage is `ephemeral` for dev — switch to `persistent` to survive restarts.

### 3. Create the topic

```bash
kubectl apply -f infra/k8s/kafka/kafka-topics.yaml
kubectl get kafkatopic -n fraud-infra
# fraud-events-raw   fraud-kafka   3   1   True
```

> [!TIP]
> The `KafkaTopic` CR is named `fraud-events-raw` but the real Kafka topic is **`fraud.events.raw`** (`spec.topicName`). In-cluster bootstrap: `fraud-kafka-kafka-bootstrap.fraud-infra.svc.cluster.local:9092`.

<p align="center">
  <img src="assets/kafka.png" width="760" alt="Kafka cluster + topic ready"/>
  <br/><em>Strimzi Kafka cluster + <code>fraud.events.raw</code> topic Ready.</em>
</p>

### 4. Enable Kafka in pipeline config

`configs/pipeline_config.yaml`:

```yaml
kafka:
  enabled: true
  bootstrap_servers: "fraud-kafka-kafka-bootstrap.fraud-infra.svc.cluster.local:9092"
  topic_fraud_events: "fraud.events.raw"
  consumer_group: "bronze-ingest-events"
  poll_timeout_ms: 30000
  batch_size: 1000
```

```bash
kubectl create configmap fraud-pipeline-config -n airflow \
  --from-file=pipeline_config.yaml=configs/pipeline_config.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart statefulset/airflow-worker deployment/airflow-dag-processor -n airflow
```

### 5. Publish events (test)

> [!WARNING]
> **Internal listener** — the broker advertises an in-cluster DNS. To produce **from the host** via port-forward, map it to localhost first:
> ```bash
> echo "127.0.0.1 fraud-kafka-fraud-pool-0.fraud-kafka-kafka-brokers.fraud-infra.svc" | sudo tee -a /etc/hosts
> ```

> [!CAUTION]
> **OOM risk** — `kafka_producer.py replay` loads the **entire** source file into RAM to sort. `fraud_events.json` (~12 GB) can blow up to 30–60 GB → OOMs the whole machine and the Kind node. Use a small file or Ctrl+C early.

```bash
kubectl port-forward svc/fraud-kafka-kafka-bootstrap -n fraud-infra 9092:9092 &

python -m src.generator.streaming.kafka_producer replay \
  --source data/raw/streaming/fraud_events.json \
  --bootstrap localhost:9092 --topic fraud.events.raw --speed 3600
```

---

## ✅ Verify

```bash
# offsets must be > 0
kubectl exec -n fraud-infra fraud-kafka-fraud-pool-0 -- \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic fraud.events.raw

# trigger the in-cluster consumer DAG (uses internal bootstrap — no port-forward)
kubectl exec -n airflow deployment/airflow-api-server -- airflow dags trigger stream_bronze
```

<p align="center">
  <img src="assets/airflow_consume_event.png" width="760" alt="Airflow consuming Kafka events into Bronze"/>
  <br/><em><code>stream_bronze</code> micro-batch consumer draining <code>fraud.events.raw</code> into the Bronze layer.</em>
</p>

---

## 🧹 Teardown

```bash
kubectl delete -f infra/k8s/kafka/kafka-topics.yaml
kubectl delete -f infra/k8s/kafka/kafka.yaml
kubectl delete -f 'https://strimzi.io/install/latest?namespace=fraud-infra' -n fraud-infra
```
