FROM python:3.11-slim

WORKDIR /opt/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY infra/docker/inference/requirements.txt /tmp/requirements.txt

# Install xgboost first with --no-deps to skip nvidia-nccl-cu12 (~300MB GPU libs).
# Then install remaining requirements excluding xgboost so pip never re-resolves its deps.
RUN pip install --no-cache-dir --timeout 300 --no-deps xgboost==2.1.4 scipy numpy && \
    grep -v '^xgboost' /tmp/requirements.txt > /tmp/requirements-no-xgb.txt && \
    pip install --no-cache-dir --timeout 300 -r /tmp/requirements-no-xgb.txt

COPY src/ src/
COPY configs/ configs/

ENV PYTHONPATH=/opt/app

CMD ["python", "src/inference/server.py"]
