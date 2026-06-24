# Spark Structured Streaming image for the Bronze ingestion job (Part A).
#
# Build from project ROOT so `COPY src/ configs/` works:
#   docker build -f infra/docker/spark/Dockerfile -t ancaotrinh/fraud-spark:latest .
#   docker push ancaotrinh/fraud-spark:latest
#
# Base: apache/spark 3.5.3 (Scala 2.12, bundled Hadoop 3.3.4, Python 3).
FROM apache/spark:3.5.3-python3

USER root

# ── Connector jars (baked in → no runtime --packages download on Kind) ─────────
# Versions pinned to Spark 3.5.x / Scala 2.12 / Hadoop 3.3.4.
ARG MVN=https://repo1.maven.org/maven2
ARG DELTA=3.2.0
ARG SPARK=3.5.3
ARG KAFKA=3.4.1
RUN set -eux; \
    cd /opt/spark/jars; \
    for url in \
      "$MVN/io/delta/delta-spark_2.12/$DELTA/delta-spark_2.12-$DELTA.jar" \
      "$MVN/io/delta/delta-storage/$DELTA/delta-storage-$DELTA.jar" \
      "$MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/$SPARK/spark-sql-kafka-0-10_2.12-$SPARK.jar" \
      "$MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/$SPARK/spark-token-provider-kafka-0-10_2.12-$SPARK.jar" \
      "$MVN/org/apache/kafka/kafka-clients/$KAFKA/kafka-clients-$KAFKA.jar" \
      "$MVN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
      "$MVN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
      "$MVN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    ; do curl -fsSL -O "$url"; done

# ── App code + Python deps ─────────────────────────────────────────────────────
COPY infra/docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY src/ /app/src/
COPY configs/ /app/configs/
ENV PYTHONPATH=/app

USER 185

# Long-running streaming driver in local[*] mode (single-pod, light for Kind).
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--conf", "spark.driver.memory=2g", \
  "/app/src/pipelines/streaming/spark_bronze.py"]
