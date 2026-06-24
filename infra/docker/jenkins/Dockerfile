# Jenkins inbound-agent with kubectl, Helm, Docker CLI, and Python 3
# Base: inbound-agent (JNLP) — correct base for Kubernetes plugin agent pods
FROM jenkins/inbound-agent:latest-jdk17

USER root

# ── kubectl (pinned) ───────────────────────────────────────────────────────────
RUN curl -LO "https://dl.k8s.io/release/v1.29.0/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/kubectl

# ── Helm 3 ─────────────────────────────────────────────────────────────────────
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# ── Docker CLI only (daemon runs as DinD sidecar in the pod) ───────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
        https://download.docker.com/linux/debian \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# ── Python 3 + venv ────────────────────────────────────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# ── Verify ─────────────────────────────────────────────────────────────────────
RUN kubectl version --client \
    && helm version --short \
    && docker --version \
    && python3 --version

USER jenkins
