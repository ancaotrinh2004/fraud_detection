# Kafka Connect image with the Debezium PostgreSQL connector baked in (Part B).
#
# Built on the exact Strimzi Kafka image the cluster operator uses (so the
# Connect runtime version matches), with the Debezium plugin added under the
# Strimzi plugin path. Built + pushed manually (like the airflow/inference
# images) — no in-cluster build, no push secret.
#
#   docker build -f infra/docker/kafka-connect/Dockerfile -t ancaotrinh/fraud-kafka-connect:latest .
#   docker push ancaotrinh/fraud-kafka-connect:latest
#
# The base tag must match the running Strimzi operator's Kafka image — find it with:
#   kubectl get deploy strimzi-cluster-operator -n fraud-infra -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_KAFKA_IMAGES")].value}'
FROM quay.io/strimzi/kafka:1.0.0-kafka-4.1.0

USER root

ARG DEBEZIUM_VERSION=3.5.2.Final
ARG MVN=https://repo1.maven.org/maven2

# Debezium Postgres connector → Strimzi plugin path (/opt/kafka/plugins).
RUN mkdir -p /opt/kafka/plugins \
 && curl -fsSL "$MVN/io/debezium/debezium-connector-postgres/$DEBEZIUM_VERSION/debezium-connector-postgres-$DEBEZIUM_VERSION-plugin.tar.gz" \
    | tar -xz -C /opt/kafka/plugins/

USER 1001
