FROM python:3.11-slim

WORKDIR /opt/app

COPY infra/docker/inference/requirements.txt /tmp/requirements.txt
# Install xgboost without GPU deps (saves ~300MB nvidia-nccl download)
RUN pip install --no-cache-dir --timeout 300 --no-deps xgboost==2.1.4 scipy numpy && \
    pip install --no-cache-dir --timeout 300 -r /tmp/requirements.txt

COPY src/ src/
COPY config/ config/

ENV PYTHONPATH=/opt/app

CMD ["python", "src/inference/server.py"]
