---
title: "Dockerfile Best Practices - Intermediate"
topic: docker-and-kubernetes
subtopic: dockerfile-best-practices
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker, dockerfile, multi-stage, buildkit, optimization]
---

# Dockerfile Best Practices — Intermediate

## Multi-Stage Build for PySpark

```dockerfile
# Stage 1: Build Python dependencies (requires gcc for native extensions)
FROM python:3.11-slim AS python-builder
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime with Java + Spark
FROM eclipse-temurin:11-jre-jammy AS runtime

# Install Python (minimal)
RUN apt-get update && apt-get install -y python3.11 python3.11-venv && \
    rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder (no gcc in final image)
COPY --from=python-builder /root/.local /root/.local

# Add Spark
ENV SPARK_VERSION=3.5.0
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /opt/
RUN tar -xzf /opt/spark-*.tgz -C /opt/ && mv /opt/spark-* /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin:/root/.local/bin
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-*.zip

WORKDIR /app
COPY jobs/ /app/jobs/
USER 1000:1000

CMD ["spark-submit", "--master", "local[*]", "jobs/revenue_job.py"]
```

---

## BuildKit Cache Mounts (Fast Builds)

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Cache mount: pip cache persists between builds (not in image)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install numpy pandas pyspark==3.5.0

# Cache apt packages between builds
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && apt-get install -y libpq-dev
```

```bash
# Enable BuildKit
DOCKER_BUILDKIT=1 docker build .
# or add to /etc/docker/daemon.json:
# {"features": {"buildkit": true}}

# With cache mount, second build is much faster:
# First build:  4m 20s
# Second build: 0m 45s (pip cache hit)
```

---

## Build Arguments vs Environment Variables

```dockerfile
# ARG: build-time only (not in running container)
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

ARG APP_VERSION
LABEL version=$APP_VERSION

# ENV: build-time AND runtime
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

# ❌ Never put secrets in ARG or ENV — visible in docker history
ARG DB_PASSWORD  # WRONG — shows in docker inspect

# ✅ Use secret mounts for build-time secrets
RUN --mount=type=secret,id=pip_conf \
    PIP_CONFIG_FILE=/run/secrets/pip_conf pip install -r requirements.txt
```

```bash
# Pass build args
docker build \
  --build-arg PYTHON_VERSION=3.12 \
  --build-arg APP_VERSION=2.0.0 \
  --secret id=pip_conf,src=./pip.conf \
  -t my-pipeline:2.0.0 .
```

---

## Image Labeling for Production Tracking

```dockerfile
FROM python:3.11-slim

# OCI standard labels
LABEL org.opencontainers.image.source="https://github.com/org/repo"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.title="Revenue Pipeline"
LABEL org.opencontainers.image.description="Daily revenue aggregation pipeline"
```

```bash
# Build with dynamic labels
docker build \
  --label org.opencontainers.image.revision=$(git rev-parse HEAD) \
  --label org.opencontainers.image.created=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  -t my-pipeline:$(git rev-parse --short HEAD) .

# Query labels
docker inspect my-pipeline:abc1234 | jq '.[0].Config.Labels'
```

---

## Dockerfile Linting with Hadolint

```bash
# Install
brew install hadolint

# Lint
hadolint Dockerfile

# Common rules violated:
# DL3006: Always tag the version of an image explicitly
# DL3007: Using latest is prone to errors if the image will ever be updated
# DL3008: Pin versions in apt-get install (apt-get install -y curl=7.74.0*)
# DL3009: Delete apt-get lists after installing
# DL4006: Set the SHELL option -o pipefail before RUN with a pipe
```

```dockerfile
# ✅ Hadolint-compliant example
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev=15.* \
        curl=7.88.* \
    && rm -rf /var/lib/apt/lists/*
```

---

## Size Comparison Techniques

```bash
# See each layer's contribution
docker history my-image:latest

# Dive: interactive layer explorer
docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  wagoodman/dive:latest my-image:latest

# Compare before/after
docker images | grep my-image
# my-image   after    123 MB
# my-image   before   1.2 GB
```
