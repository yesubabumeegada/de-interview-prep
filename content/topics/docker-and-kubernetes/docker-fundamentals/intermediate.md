---
title: "Docker Fundamentals - Intermediate"
topic: docker-and-kubernetes
subtopic: docker-fundamentals
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker, containers, networking, volumes, multi-stage]
---

# Docker Fundamentals — Intermediate

## Multi-Stage Builds for Smaller Images

```dockerfile
# Without multi-stage: ~1.5 GB image (includes build tools)
FROM python:3.11
RUN apt-get install -y gcc g++ build-essential
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "pipeline.py"]

# With multi-stage: ~200 MB image
FROM python:3.11 AS builder
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11-slim AS runtime
# Copy only the installed packages, not the build tools
COPY --from=builder /root/.local /root/.local
COPY . .
ENV PATH=/root/.local/bin:$PATH
CMD ["python", "pipeline.py"]
```

Multi-stage cuts image size dramatically — smaller images pull faster in CI and K8s.

---

## Docker Networking

```bash
# List networks
docker network ls

# Types:
# bridge (default): containers on same host communicate by name
# host: container shares host network (faster, less isolation)
# none: no networking

# Create custom network
docker network create de-network

# Run containers on same network (they can reach each other by name)
docker run -d --name postgres --network de-network postgres:15
docker run -d --name airflow --network de-network \
  -e DB_HOST=postgres \
  apache/airflow:2.8.0

# postgres is reachable at hostname "postgres" from airflow container
```

---

## Volume Strategies

```bash
# 1. Named volume (managed by Docker — survives container restart)
docker run -v postgres-data:/var/lib/postgresql/data postgres:15

# 2. Bind mount (maps host dir to container — great for dev)
docker run -v $(pwd)/dags:/opt/airflow/dags airflow:latest
# Changes on host immediately visible in container

# 3. tmpfs (in-memory — for temp data)
docker run --tmpfs /tmp:rw,size=512m my-pipeline

# Check volumes
docker volume ls
docker volume inspect postgres-data
docker volume rm postgres-data   # careful — deletes data!
```

---

## Docker Resource Limits

```bash
# Memory limit (OOM killer if exceeded)
docker run --memory 2g --memory-swap 2g spark-job:v1

# CPU limit
docker run --cpus 2.0 spark-job:v1

# Both
docker run \
  --memory 4g \
  --memory-swap 4g \
  --cpus 4 \
  --name spark-driver \
  spark-job:v1

# View resource usage
docker stats
docker stats --no-stream  # one-time snapshot
```

---

## Health Checks

```dockerfile
# In Dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Or at runtime
docker run \
  --health-cmd="curl -f http://localhost:8080/health || exit 1" \
  --health-interval=30s \
  airflow-webserver:v1
```

```bash
# Check health status
docker inspect --format='{{.State.Health.Status}}' container-name
# → healthy | unhealthy | starting
```

---

## Common Pitfalls

| Pitfall | Fix |
|---|---|
| Running as root inside container | Add `USER nonroot` in Dockerfile |
| Secrets in environment variables (visible in `docker inspect`) | Use Docker secrets or mount files |
| Image size > 1GB | Use slim base image + multi-stage build |
| No `.dockerignore` | Create one to exclude `data/`, `logs/`, `.git/` |
| `latest` tag in production | Always pin exact versions: `python:3.11.4-slim` |
| All data in container (lost on restart) | Use named volumes or external storage |

---

## .dockerignore for DE Projects

```dockerignore
.git/
.gitignore
.env
*.env.*
data/
logs/
__pycache__/
*.pyc
.venv/
*.egg-info/
.pytest_cache/
target/           # dbt
dbt_packages/
.ipynb_checkpoints/
README.md
tests/            # don't ship test code in production image
```
