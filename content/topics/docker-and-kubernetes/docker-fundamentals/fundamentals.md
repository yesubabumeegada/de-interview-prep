---
title: "Docker Fundamentals - Fundamentals"
topic: docker-and-kubernetes
subtopic: docker-fundamentals
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker, containers, images, data-engineering]
---

# Docker Fundamentals — Fundamentals


## 🎯 Analogy

Think of Docker containers like shipping containers: the container packages your app and all its dependencies so it runs identically on your laptop, in CI, and in production — no 'works on my machine' surprises.

---
## The Shipping Container Analogy

Before shipping containers, loading a ship was chaos — every item needed custom rigging and stacking. Shipping containers standardized everything: you pack your goods into a standard box, and the crane, ship, and port equipment all know exactly how to handle it. Docker containers work the same way for software. You package your Spark job, Python dependencies, and config into one standard container image. Whether it runs on your laptop, a CI server, or a production Kubernetes cluster, it behaves identically. No more "works on my machine" — the container *is* the machine environment.

---

## Containers vs Virtual Machines

```mermaid
flowchart TD
    subgraph VM["Virtual Machine"]
        H1[Hardware]
        OS1[Host OS]
        HY[Hypervisor]
        GOS1[Guest OS]
        APP1[App + Deps]
    end

    subgraph CON["Container"]
        H2[Hardware]
        OS2[Host OS]
        DE[Docker Engine]
        APP2[App + Deps only]
    end
```

| | Containers | VMs |
|---|---|---|
| Startup time | Seconds | Minutes |
| Size | MBs | GBs |
| OS | Shares host kernel | Full guest OS |
| Isolation | Process-level | Hardware-level |
| Use case | Microservices, pipelines | Full OS isolation |

Containers are faster and lighter — ideal for data pipelines that need to start quickly and scale horizontally.

---

## Core Concepts

**Image** — a read-only template (blueprint). Like a class in OOP.
**Container** — a running instance of an image. Like an object.
**Dockerfile** — instructions to build an image.
**Registry** — image storage (Docker Hub, ECR, GCR).
**Volume** — persistent storage mounted into a container.

---

## Your First Dockerfile

```dockerfile
# Start from official Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency file first (for layer caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Command to run when container starts
CMD ["python", "pipeline.py"]
```

```bash
# Build image
docker build -t my-pipeline:v1 .

# Run container
docker run my-pipeline:v1

# Run interactively (explore the container)
docker run -it my-pipeline:v1 bash

# Run with environment variable
docker run -e DB_PASSWORD=secret my-pipeline:v1

# Run with mounted volume (persist output)
docker run -v $(pwd)/output:/app/output my-pipeline:v1
```

---

## Essential Docker Commands

```bash
# Images
docker images                       # list local images
docker pull python:3.11-slim        # download image
docker rmi my-pipeline:v1           # delete image
docker push myregistry/pipeline:v1  # push to registry

# Containers
docker ps                           # running containers
docker ps -a                        # all containers (including stopped)
docker logs <container-id>          # view container output
docker exec -it <container> bash    # shell into running container
docker stop <container>             # graceful stop
docker rm <container>               # delete stopped container

# Cleanup
docker system prune                 # remove unused resources
docker volume prune                 # remove unused volumes
```

---

## Docker Compose for Local DE Development

```yaml
# docker-compose.yml — local Airflow stack
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data

  airflow-webserver:
    image: apache/airflow:2.8.0
    depends_on:
      - postgres
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.8.0
    depends_on:
      - postgres
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    command: scheduler

volumes:
  postgres-data:
```

```bash
docker compose up -d        # start all services in background
docker compose logs -f      # follow logs
docker compose down         # stop and remove containers
docker compose down -v      # also remove volumes
```

---

## Why DE Teams Use Docker

| Use Case | Benefit |
|---|---|
| Reproducible Spark jobs | Same dependencies everywhere |
| Airflow custom operators | Package operator + deps together |
| CI/CD pipeline execution | Consistent test environment |
| Local development | Full stack without installing services |
| Production deployment | Immutable, versioned artifacts |

## ▶️ Try It Yourself

```bash
# Build an image from a Dockerfile
docker build -t my-etl-job:1.0 .

# Run the container
docker run --rm my-etl-job:1.0

# Run interactively (for debugging)
docker run -it --rm my-etl-job:1.0 /bin/bash

# Run with environment variables and volume mount
docker run --rm \
  -e DB_HOST=localhost \
  -v $(pwd)/data:/data \
  my-etl-job:1.0

# List running containers
docker ps

# View logs
docker logs <container_id>
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
