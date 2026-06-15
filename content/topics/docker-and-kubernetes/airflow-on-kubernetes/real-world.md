---
title: "Airflow on Kubernetes - Real World"
topic: docker-and-kubernetes
subtopic: airflow-on-kubernetes
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [docker-and-kubernetes, airflow, kubernetes, production, incident-response, data-engineering]
---

# Airflow on Kubernetes — Real World

## Production Architecture: What a Mature DE Platform Looks Like

After running Airflow on Kubernetes at scale, the architecture that emerges looks quite different from the Helm defaults. Here's what a production-grade setup includes and why each decision was made.

### Reference Architecture (AWS/EKS)

```
┌─────────────────────────────────────────────────────────┐
│  EKS Cluster                                            │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ system nodes │  │ task nodes   │  │ spark nodes  │  │
│  │ (on-demand)  │  │ (spot)       │  │ (spot+mem)   │  │
│  │ Airflow sched│  │ Task pods    │  │ Spark jobs   │  │
│  │ Airflow web  │  │ (ephemeral)  │  │ via operator │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│                                                         │
│  Namespaces:  airflow | finance-de | marketing-de       │
└─────────────────────────────────────────────────────────┘
         │                    │                  │
    RDS Aurora           S3 (logs)         ECR (images)
    (metadata DB)        (DAG artifacts)   (task images)
         │
   AWS Secrets Manager
   (via External Secrets Operator)
```

### Why This Specific Setup

**System nodes on-demand:** The Airflow scheduler and webserver are long-running, always-on services. A spot instance interruption would cause a scheduler restart and potentially lose state for in-flight tasks. On-demand is worth the 3x cost premium for these components.

**Task pods on spot:** Each task pod is short-lived and Airflow retries failed tasks. If a spot node is reclaimed by AWS, the pod is killed, the task moves to "failed" state, and the retry mechanism kicks in within minutes. The 70-80% cost savings more than offset the occasional retry overhead.

**External Secrets Operator over K8s Secrets:** At the enterprise level, secrets need to be centrally managed, audited, and rotated. The ESO pattern means secrets never exist as YAML in git, and a single Secrets Manager audit log shows every secret access across all environments.

---

## Real Incident: Scheduler Crash Loop Under High Task Load

**The situation:** A DE team migrated 200 DAGs to a new Airflow cluster with KubernetesExecutor. Under full load (500 concurrent tasks), the Airflow scheduler started crashing every 15-30 minutes with `MemoryError`.

**Initial diagnosis:**
```bash
kubectl describe pod airflow-scheduler-xxx -n airflow
# Events: OOMKilled
# Memory usage at crash: 8Gi / 4Gi limit

kubectl logs airflow-scheduler-xxx -n airflow --previous | tail -50
# Last log entries: "Queuing task: finance.revenue_daily.transform..."
# No Python traceback — just sudden termination (OOM)
```

**Root cause investigation:**
```bash
# The scheduler was holding DAG file content in memory
# 200 DAGs × average 50KB parsed = 10MB... not the problem
# Check: how many DAG files are being parsed?
kubectl exec -it airflow-scheduler-xxx -n airflow -- \
  airflow dags list | wc -l
# 847 DAGs! — including many duplicates from bad git branch management

# The real problem: DAG serialization overhead
kubectl exec -it airflow-scheduler-xxx -n airflow -- \
  psql $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN -c \
  "SELECT COUNT(*) FROM serialized_dag;"
# 847 rows, each up to 1MB — serialization was happening every 30s for all DAGs
```

**Fixes applied:**

```yaml
# 1. Increase scheduler memory — short-term fix
scheduler:
  resources:
    limits:
      memory: "8Gi"  # was 4Gi

# 2. Tune serialization interval — reduce DB load
config:
  core:
    min_serialized_dag_fetch_interval: 120  # was 30s; reduce serialization frequency
    min_serialized_dag_update_interval: 60   # only update if DAG changed

# 3. Add scheduler replicas for HA
scheduler:
  replicas: 3  # was 1; HA prevents total outage when one OOMs

# 4. Clean up unused DAGs (the real fix)
# Remove stale DAG files from git, add CI check for max DAG count
```

**Lesson:** Airflow's scheduler memory is proportional to the number of active DAGs, not just running tasks. Unbounded DAG growth creates a slow-moving memory issue that manifests as scheduler OOM under peak load.

---

## Real Pattern: Per-Team Custom Airflow Images

Large DE platforms typically have teams with different Python dependencies. The solution is a multi-image Airflow setup where each team maintains their own image that inherits from a base image.

```dockerfile
# Base image — maintained by platform team
# base/Dockerfile
FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Common dependencies for all teams
COPY base-requirements.txt /
RUN pip install --no-cache-dir -r /base-requirements.txt
```

```dockerfile
# Finance team image — inherits from base
# finance/Dockerfile
FROM your-registry/airflow-base:2.9.2-v2.1

COPY finance-requirements.txt /
RUN pip install --no-cache-dir -r /finance-requirements.txt
# finance-requirements.txt: pandas==2.1.0, openpyxl==3.1.2, boto3==1.34.0
```

```python
# Finance team DAG: uses finance image via KubernetesPodOperator
finance_etl = KubernetesPodOperator(
    task_id="finance_monthly_close",
    name="finance-etl",
    namespace="finance-de",
    image="your-registry/airflow-finance:2.9.2-v1.4",  # their image
    cmds=["python", "/opt/airflow/dags/finance/monthly_close.py"],
    # ...
)
```

**CI/CD for images:**
```yaml
# .github/workflows/airflow-images.yml
on:
  push:
    paths:
      - 'airflow/finance/**'   # only rebuild finance image when finance files change

jobs:
  build-finance-image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build and push
        run: |
          docker build -t $ECR_REGISTRY/airflow-finance:$GITHUB_SHA \
            -f airflow/finance/Dockerfile .
          docker push $ECR_REGISTRY/airflow-finance:$GITHUB_SHA
      - name: Update Helm values
        run: |
          # Update the image tag in team's values file
          sed -i "s/airflow-finance:.*/airflow-finance:$GITHUB_SHA/" \
            helm/finance-team/values.yaml
          git commit -am "Update finance image to $GITHUB_SHA"
          git push
```

---

## Common Mistakes and How to Avoid Them

### Mistake 1: Not Setting `restartPolicy: Never` in Pod Template

```yaml
# WRONG — Kubernetes will restart the task pod on failure
spec:
  restartPolicy: Always  # or OnFailure

# RIGHT — Airflow owns retry logic, not Kubernetes
spec:
  restartPolicy: Never
```

**Why it matters:** If K8s restarts a failed pod, Airflow doesn't know the task is being retried. You end up with two instances of the task running simultaneously, potential data corruption, and the Airflow UI showing the task as "running" while the original pod is actually being retried by K8s.

### Mistake 2: Using `emptyDir` for Logs in KubernetesExecutor

```yaml
# WRONG — logs are lost when the pod is deleted
volumes:
  - name: logs
    emptyDir: {}

# RIGHT — use S3/GCS remote logging or EFS PVC
config:
  core:
    remote_logging: "True"
  logging:
    remote_base_log_folder: "s3://your-bucket/airflow-logs"
```

### Mistake 3: Not Configuring Image Pull Secrets

```yaml
# When using private ECR/GCR/ACR, pods fail with ImagePullBackOff
# Add imagePullSecrets to the pod template:
spec:
  imagePullSecrets:
    - name: ecr-registry-secret

# Or: use IRSA/Workload Identity so nodes can pull from ECR without explicit secrets
# (Recommended — secrets rotation is automatic)
```

### Mistake 4: Setting CPU Limits Too Low

```yaml
# This causes CPU throttling — tasks run 3-5x slower but don't fail
resources:
  requests:
    cpu: "500m"
  limits:
    cpu: "500m"  # Bad: hard limit; container is throttled at 50% of 1 core

# Better: set requests to guide scheduling, omit limits or set much higher
resources:
  requests:
    cpu: "500m"
  limits:
    cpu: "2"    # Burst allowed; throttling only if node is fully contended
```

---

## Observability: What to Monitor

```yaml
# Prometheus alerts for Airflow on K8s
# Install airflow-exporter or use built-in StatsD → Prometheus bridge

groups:
  - name: airflow
    rules:
      # Scheduler is not heartbeating — likely crashed
      - alert: AirflowSchedulerNotRunning
        expr: time() - airflow_scheduler_heartbeat > 60
        severity: critical

      # Many tasks queued but not running (scheduler or worker issue)
      - alert: AirflowHighTaskQueueDepth
        expr: airflow_task_instance_count{state="queued"} > 100
        for: 10m
        severity: warning

      # Task pod pending too long (no cluster capacity)
      - alert: AirflowTaskPodsPending
        expr: |
          count(kube_pod_status_phase{namespace="airflow",phase="Pending"} == 1) > 10
        for: 5m
        severity: warning

      # Metadata DB connection saturation
      - alert: AirflowDBConnectionsHigh
        expr: pg_stat_database_numbackends{datname="airflow"} > 80
        severity: warning
```

**Key Grafana dashboard panels:**
- Task success/failure rate over time (% SLA)
- Active task pods count (see if autoscaling is working)
- Scheduler heartbeat age (is scheduler alive?)
- Metadata DB query latency (slow DB = slow scheduling)
- Task duration p50/p95 by DAG (spot regressions quickly)

---

## Deployment Checklist: Before Going to Production

```
✓ Metadata DB: managed Postgres (RDS Multi-AZ or Cloud SQL HA), NOT the Helm bundled SQLite
✓ Remote logging: S3/GCS configured — pod logs must survive pod deletion
✓ External Secrets Operator: all credentials from Secrets Manager, not plain K8s Secrets
✓ Custom Airflow image: pre-installed dependencies, not pip install at runtime
✓ HA scheduler: 2+ replicas with correct Postgres advisory lock setup
✓ Resource requests/limits: set on scheduler, webserver, AND pod template
✓ PodDisruptionBudget: for scheduler and webserver deployments
✓ Network Policy: restrict task pods from accessing other namespaces
✓ RBAC: service account per team namespace, principle of least privilege
✓ Monitoring: Prometheus alerts for scheduler heartbeat and queue depth
✓ DB cleanup CronJob: prevents metadata DB from growing unbounded
✓ Spot instances: task pods on spot with on-demand fallback via Karpenter
✓ Image pull: IRSA/Workload Identity for ECR/GCR access — no hardcoded credentials
```

---
