FROM apache/airflow:3.2.0

# Layer 1 — heavy ML/lakehouse deps (cached unless requirements.txt changes)
COPY infra/docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Layer 2 — data governance plugin (separate layer so ML layer stays cached)
COPY infra/docker/airflow/requirements-governance.txt /tmp/requirements-governance.txt
RUN pip install --no-cache-dir -r /tmp/requirements-governance.txt

# Bake pipeline source code into image — workers import src.pipelines.*
COPY --chown=airflow:root src/ /opt/airflow/src/
