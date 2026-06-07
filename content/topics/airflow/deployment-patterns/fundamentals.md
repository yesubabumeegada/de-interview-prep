---
title: "Airflow Deployment Patterns - Fundamentals"
topic: airflow
subtopic: deployment-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, deployment, kubernetes, docker, local, architecture]
---

# Airflow Deployment Patterns — Fundamentals

## Airflow's Core Components

Before choosing a deployment pattern, understand what needs to run:

```mermaid
flowchart TD
    WEB["Webserver\n(Airflow UI)"] --> DB[("Metadata DB\n(PostgreSQL)")]
    SCHED["Scheduler"] --> DB
    EXEC["Executor"] --> WORKERS["Workers"]
    SCHED --> EXEC
    WORKERS --> DB
    TRIG["Triggerer\n(deferrable ops)"] --> DB
```

| Component | Role | Required? |
|-----------|------|-----------|
| **Webserver** | UI + REST API | Yes |
| **Scheduler** | DAG parsing, task scheduling | Yes |
| **Metadata DB** | State storage (task states, DAG runs) | Yes |
| **Executor** | Manages how tasks are distributed | Yes (built-in) |
| **Workers** | Actually run the tasks | Depends on executor |
| **Triggerer** | Handles deferrable operators | Only if using deferrable ops |
| **Message broker** | Task queue for Celery | Only for CeleryExecutor |

---

## Executor Options

The **executor** determines where and how tasks run:

```mermaid
flowchart LR
    SCHED["Scheduler"] -->|"LocalExecutor"| LP["Local process\n(same machine)"]
    SCHED -->|"CeleryExecutor"| CW["Celery worker\n(any machine)"]
    SCHED -->|"KubernetesExecutor"| KP["K8s pod\n(ephemeral)"]
```

### LocalExecutor
```ini
[core]
executor = LocalExecutor

# Tasks run as subprocesses on the scheduler machine
parallelism = 16
```

- ✅ Simple — no extra infrastructure
- ✅ Good for dev and small workloads
- ❌ Single machine — can't scale horizontally
- ❌ Worker and scheduler compete for CPU/memory

### CeleryExecutor
```ini
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow
worker_concurrency = 16
```

- ✅ Horizontal scaling — add worker machines
- ✅ Separate scheduler and worker resources
- ❌ Requires Redis or RabbitMQ broker
- ❌ More infrastructure to manage

### KubernetesExecutor
```ini
[core]
executor = KubernetesExecutor

[kubernetes]
namespace = airflow
worker_pods_creation_batch_size = 16
```

- ✅ Perfect isolation — each task gets its own pod
- ✅ Dynamic scaling — pods spin up/down per task
- ✅ Different resource profiles per task
- ❌ Requires Kubernetes cluster
- ❌ Pod startup adds latency (~30s per task)

---

## Deployment Options

### Option 1: Docker Compose (Development/Small Teams)

```yaml
# docker-compose.yml (simplified)
version: '3'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: airflow
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow

  redis:
    image: redis:7
    ports: ["6379:6379"]

  airflow-webserver:
    image: apache/airflow:2.8.1
    command: webserver
    ports: ["8080:8080"]
    depends_on: [postgres, redis]

  airflow-scheduler:
    image: apache/airflow:2.8.1
    command: scheduler
    depends_on: [postgres, redis]

  airflow-worker:
    image: apache/airflow:2.8.1
    command: celery worker
    depends_on: [postgres, redis]
```

```bash
docker compose up -d
# Access UI at http://localhost:8080
# Username/password: airflow/airflow
```

### Option 2: Managed Airflow Services

| Service | Provider | Best for |
|---------|---------|---------|
| **MWAA** | AWS | AWS-native shops already on ECS/EKS |
| **Cloud Composer** | GCP | GCP-native, BigQuery-heavy workloads |
| **Astronomer** | Astronomer.io | Enterprise features, multi-team isolation |

```bash
# AWS MWAA — deploy via Terraform
resource "aws_mwaa_environment" "prod" {
  name               = "prod-airflow"
  airflow_version    = "2.8.1"
  environment_class  = "mw1.medium"
  max_workers        = 10
  source_bucket_arn  = aws_s3_bucket.dags.arn
  dag_s3_path        = "dags/"
  # ...
}
```

---

## DAG Deployment Strategies

### Strategy 1: Shared File System (NFS/EFS)

```
NFS Volume
├── dags/           ← All Airflow components mount this
│   ├── pipeline_a.py
│   └── pipeline_b.py
└── plugins/
```

- Simple — copy a file to deploy
- No versioning or rollback
- All environments see the same files

### Strategy 2: Git Sync (Recommended)

```yaml
# git-sync sidecar container
git-sync:
  image: registry.k8s.io/git-sync/git-sync:v4.1.0
  args:
    - --repo=https://github.com/company/airflow-dags
    - --branch=main
    - --period=60s          # Sync every 60 seconds
    - --dest=/opt/airflow/dags
```

- DAGs are version-controlled in Git
- Pull request review before deployment
- Easy rollback (revert commit)
- Supports branch-based dev/prod separation

---

## Interview Tips

> **Tip 1:** "What executor would you choose for production?" — The right answer depends on scale and infrastructure. For a team running < 50 concurrent tasks on a single cloud VM, `LocalExecutor` is fine. For horizontal scaling needs, `CeleryExecutor`. For cloud-native with strong isolation requirements, `KubernetesExecutor`. Avoid giving a blanket answer — ask about the constraints first.

> **Tip 2:** The Airflow metadata DB is the single most critical piece of infrastructure. A slow or unavailable metadata DB = broken scheduler + broken workers + broken UI. Always run it on managed PostgreSQL (RDS, Cloud SQL, etc.) with backups, not on the same machine as Airflow components.

> **Tip 3:** Managed Airflow (MWAA, Cloud Composer, Astronomer) is increasingly the default for production. These services handle infrastructure, upgrades, and HA automatically. Know their trade-offs: less flexibility but far less operational burden.
