---
title: "Docker Fundamentals — Scenarios"
topic: docker-and-kubernetes
subtopic: docker-fundamentals
content_type: scenario_question
tags: [docker, containers, interview, scenarios, data-engineering]
---

# Docker Fundamentals — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Containerize a Python Pipeline

**Scenario:** You have a Python script `etl.py` that reads from a CSV, transforms data, and writes to PostgreSQL. How do you containerize it so it runs identically in dev, CI, and production?

<details>
<summary>💡 Hint</summary>

Write a Dockerfile that starts from `python:3.11-slim`, installs dependencies from `requirements.txt`, and copies your script. Don't hardcode database credentials — pass them as environment variables at runtime. Create a `.dockerignore` to exclude `data/`, `.env`, and any large files. Test it locally with `docker run -e DB_URL=...` before assuming it works in CI.

</details>

<details>
<summary>✅ Solution</summary>

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Install system deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl.py .

CMD ["python", "etl.py"]
```

```text
# .dockerignore
.env
data/
*.csv
*.parquet
__pycache__/
.venv/
```

```bash
# Build
docker build -t my-etl:v1 .

# Run — inject secrets at runtime, never bake into image
docker run \
  -e DB_URL="postgresql://user:pass@host:5432/db" \
  -e INPUT_FILE="/data/orders.csv" \
  -v $(pwd)/data:/data \
  my-etl:v1
```

**etl.py — read config from environment:**
```python
import os
DB_URL = os.environ["DB_URL"]  # fail fast if not set
INPUT_FILE = os.environ.get("INPUT_FILE", "/data/orders.csv")
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Docker Image Too Large

**Scenario:** Your data science team built a Docker image for a feature engineering pipeline. It's 4.2 GB. CI pulls take 8 minutes and deployments are slow. How do you reduce the image size?

<details>
<summary>💡 Hint</summary>

Large images usually come from: (1) full base image instead of slim/distroless, (2) build tools left in the final image, (3) no `.dockerignore`, (4) pip cache not cleared, (5) installing unnecessary packages. Use multi-stage builds to separate build environment from runtime environment. Use `docker history my-image:latest` to find which layers are biggest.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Diagnose first
docker history feature-pipeline:latest --no-trunc | sort -k4 -h -r | head -10
# → Finds the biggest layers

docker image inspect feature-pipeline:latest | jq '.[0].Size'
# → 4,200,000,000 bytes
```

```dockerfile
# BEFORE: 4.2 GB
FROM python:3.11   # includes gcc, make, full stdlib
COPY requirements.txt .
RUN pip install -r requirements.txt   # pip cache still in layer
COPY . .

# AFTER: ~650 MB — multi-stage build
FROM python:3.11 AS builder
# Build tools available here for compiling native extensions
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11-slim AS runtime
# Only copy installed packages — no build tools
COPY --from=builder /root/.local /root/.local
COPY pipeline/ /app/pipeline/
ENV PATH=/root/.local/bin:$PATH
USER 1000:1000
WORKDIR /app
CMD ["python", "-m", "pipeline.feature_engineering"]
```

```dockerignore
# Add comprehensive .dockerignore
data/
models/weights/      # large model files → use object storage
notebooks/
tests/
docs/
.git/
__pycache__/
*.pyc
.venv/
```

**Result:** 4.2 GB → 650 MB (85% reduction). CI pull time: 8 min → 1.5 min.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Containerized DE Platform

**Scenario:** You're leading the infrastructure for a DE team that runs 30 Airflow DAGs, custom Spark jobs, and dbt models. Currently everything runs on a single large EC2 instance. Design a containerized architecture.

<details>
<summary>💡 Hint</summary>

Think in layers: base images (shared dependencies), service images (Airflow, Spark), job images (per-pipeline). Use Docker Compose for local development, Kubernetes for production (EKS). Airflow should use KubernetesExecutor so each task runs in its own pod — isolation + horizontal scaling. Spark should run with spark-submit targeting K8s. dbt runs as a one-shot container job per environment. Image registry strategy: tag by git SHA, never use `latest` in production.

</details>

<details>
<summary>✅ Solution</summary>

**Image hierarchy:**
```
python:3.11-slim (public)
  └── de-base:2024.01 (shared system deps + base Python pkgs)
       ├── airflow:2024.01 (Airflow + providers)
       ├── spark:2024.01 (Spark + Python + job libs)
       └── dbt:2024.01 (dbt-core + adapters)
            └── pipeline-revenue:abc1234 (per-pipeline image, git SHA tag)
```

**Local development:**
```yaml
# docker-compose.yml — full local stack
services:
  airflow-webserver: {image: de-airflow:2024.01, ...}
  airflow-scheduler: {image: de-airflow:2024.01, command: scheduler}
  spark-master: {image: de-spark:2024.01, ...}
  postgres: {image: postgres:15}
  # Mount local dags/ so changes reflect immediately
```

**Production — Airflow KubernetesExecutor:**
```python
# Each Airflow task runs in its own K8s pod
# Different tasks can use different images
task = SparkSubmitOperator(
    executor_config={
        "KubernetesExecutor": {
            "image": "de-spark:2024.01",
            "resources": {"requests": {"memory": "4Gi", "cpu": "2"}},
        }
    }
)
```

**CI/CD pipeline:**
```yaml
# Build → Scan → Push → Deploy
build:
  docker build -t de-pipeline:$GIT_SHA .
scan:
  trivy image de-pipeline:$GIT_SHA --exit-code 1 --severity CRITICAL
push:
  docker push registry/de-pipeline:$GIT_SHA
deploy:
  kubectl set image deployment/pipeline container=registry/de-pipeline:$GIT_SHA
```

**Key decisions:**
```
1. Tag by git SHA (not :latest) — know exactly what's running
2. Scan every image in CI — block on CRITICAL CVEs
3. Non-root user in all images — security baseline
4. Resource limits on every container — prevent noisy neighbor
5. Separate image per pipeline — no shared dependency conflicts
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a Docker image and a container?**
A: An image is a read-only template (like a class). A container is a running instance of that image (like an object). Multiple containers can run from the same image simultaneously.

**Q: What is a multi-stage Docker build and why is it useful?**
A: Multi-stage builds use multiple `FROM` statements in one Dockerfile. Early stages can include build tools (compilers, pip); the final stage copies only compiled artifacts. This produces a small production image without build tools, reducing attack surface and pull time.

**Q: What is a Docker volume and when would you use one?**
A: A volume is persistent storage that survives container restarts. Use named volumes for databases (PostgreSQL data), bind mounts for local dev (live code reload), and tmpfs for temporary in-memory data. Anything written only inside the container (no volume) is lost when the container is removed.

**Q: What does `.dockerignore` do and what should DE teams include in it?**
A: `.dockerignore` prevents files from being sent to the Docker build context, reducing build time and image size. DE teams should ignore: `data/`, `*.csv`, `*.parquet`, `.env`, `.git/`, `__pycache__/`, `.venv/`, and test files.

**Q: How do you pass secrets to a Docker container safely?**
A: At runtime via environment variables (`-e KEY=value`) sourced from a secrets manager (not hardcoded). Better: mount secrets as files via Docker secrets or Kubernetes secrets. Never bake credentials into the image layer — they appear in `docker history` and registry pulls.

**Q: How does Docker layer caching work and how do you optimize for it?**
A: Docker caches each Dockerfile instruction as a layer. If a layer's input hasn't changed, it reuses the cache. To maximize caching: copy `requirements.txt` and run `pip install` before copying application code — so code changes don't invalidate the dependency install layer.

**Q: What is `docker compose` used for in DE development?**
A: Docker Compose defines multi-container applications in a YAML file. DE teams use it to run the full local stack — Airflow webserver + scheduler + Postgres + Redis — with one `docker compose up` command, eliminating the "install everything locally" setup burden.

---

## 💼 Interview Tips

- Lead with the "shipping container" analogy — it's universally understood and immediately explains why containers exist.
- Always mention non-root users and no credentials in images when discussing Docker security — these are baseline practices that distinguish experienced engineers from beginners.
- For image optimization questions, mention multi-stage builds first, then slim base images — these are the two highest-impact changes with the least effort.
- Bring up `.dockerignore` proactively — many candidates forget it, and it shows you've actually worked with Docker in production where accidental large files or `.git/` directories cause real pain.
- Connect Docker to the broader pipeline — show how images are built in CI, scanned for vulnerabilities, tagged by git SHA, pushed to a registry, and deployed. End-to-end thinking impresses senior interviewers.
- Avoid describing `:latest` as acceptable for anything beyond local experimentation — pinned versions are essential for reproducibility and are a clear sign of production experience.
