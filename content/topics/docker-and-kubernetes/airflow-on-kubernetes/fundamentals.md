---
title: "Airflow on Kubernetes - Fundamentals"
topic: docker-and-kubernetes
subtopic: airflow-on-kubernetes
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker-and-kubernetes, airflow, kubernetes, kubernetesexecutor, helm, data-engineering]
---

# Airflow on Kubernetes — Fundamentals

## 🎯 Analogy

Think of Airflow executors like different kitchen staffing models. LocalExecutor is one chef handling one dish at a time. CeleryExecutor is a kitchen with multiple chefs (workers) coordinated by a head chef (broker). KubernetesExecutor is a ghost kitchen where you spin up a private chef for every single dish order — they cook it and go home, and you never pay for idle chefs.

---

## Why Run Airflow on Kubernetes?

Airflow is the most widely used workflow orchestration tool in data engineering. Running it on Kubernetes provides elastic scaling, better isolation, and simpler dependency management compared to traditional VM-based deployments. Every major cloud provider (AWS/EKS, GCP/GKE, Azure/AKS) supports it natively, and the official Helm chart makes production deployment repeatable and auditable.

Understanding Airflow on Kubernetes is a core skill for senior DE roles because it sits at the intersection of pipeline orchestration and infrastructure.

---

## The Three Executor Types

The executor is the component that determines *how* Airflow runs individual tasks. This is the most important architectural decision when deploying Airflow.

### LocalExecutor
- Tasks run as subprocesses on the **same machine** as the Airflow scheduler
- Simple to set up — no additional infrastructure
- Limited by the resources of a single node
- **Use when:** Small teams, low task concurrency (< 50 concurrent tasks), development environments
- **Problem:** The scheduler becomes a bottleneck and a single point of failure for all tasks

### CeleryExecutor
- Tasks are sent to a **message broker** (Redis or RabbitMQ) and picked up by **worker pods**
- Worker pods are long-running — they stay alive even when idle, consuming resources
- Flower UI provides a dashboard to monitor worker state and task queues
- **Use when:** Predictable, steady-state workloads where you know how many workers you need
- **Problem:** Over-provisioning workers wastes money; under-provisioning causes queue buildup

```
CeleryExecutor architecture:
Scheduler → Redis Queue → Worker Pod 1 (always running)
                       → Worker Pod 2 (always running)
                       → Worker Pod 3 (always running)
```

### KubernetesExecutor
- Each task gets its **own Kubernetes pod** — spun up on demand, terminated when done
- Zero idle resource consumption: if no tasks are running, no task pods exist
- Each task can specify its own Docker image, CPU/memory, and environment
- **Use when:** Variable workloads, tasks with different resource needs, large-scale platforms
- **Best for:** Production DE platforms — it's the most operationally sound at scale

```
KubernetesExecutor architecture:
Scheduler → K8s API → Task Pod (created, runs, deleted)
                    → Task Pod (created, runs, deleted)
                    → Task Pod (created, runs, deleted — different image!)
```

---

## KubernetesExecutor Deep Dive

### How It Works Step-by-Step

1. Airflow scheduler determines a task is ready to run (all upstream dependencies met)
2. Scheduler calls the Kubernetes API to create a Pod running the Airflow worker image
3. The pod downloads the task's DAG file (via git-sync or shared volume)
4. The task runs inside the pod — logs stream to your configured log backend
5. Pod completes (success or failure), and the scheduler records the result
6. The pod is deleted (or retained briefly for debugging)

### Pod Templates

The KubernetesExecutor uses a **pod template file** to define the base configuration for all task pods. This YAML file looks like a normal Kubernetes pod spec and is applied to every task:

```yaml
# pod_template_file.yaml — base template for all KubernetesExecutor task pods
apiVersion: v1
kind: Pod
metadata:
  name: placeholder  # overridden per task
  namespace: airflow
spec:
  restartPolicy: Never  # CRITICAL: tasks must not restart on failure (Airflow handles retry)
  serviceAccountName: airflow-worker
  containers:
    - name: base
      image: apache/airflow:2.9.2  # default image; overridden per task if needed
      imagePullPolicy: IfNotPresent
      resources:
        requests:
          memory: "512Mi"
          cpu: "250m"
        limits:
          memory: "2Gi"
          cpu: "1000m"
      volumeMounts:
        - name: airflow-logs
          mountPath: /opt/airflow/logs
        - name: dags
          mountPath: /opt/airflow/dags
  volumes:
    - name: airflow-logs
      persistentVolumeClaim:
        claimName: airflow-logs-pvc
    - name: dags
      emptyDir: {}  # git-sync sidecar writes here
```

### Per-Task Resource Overrides

One of KubernetesExecutor's biggest advantages is that individual tasks can override CPU/memory:

```python
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG("variable_resource_dag", schedule_interval="@daily") as dag:

    # Lightweight task — minimal resources
    light_task = BashOperator(
        task_id="check_upstream",
        bash_command="python check_sources.py",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "256Mi", "cpu": "100m"},
                            limits={"memory": "512Mi", "cpu": "500m"},
                        )
                    )]
                )
            )
        }
    )

    # Heavy Spark job — needs more memory
    heavy_task = BashOperator(
        task_id="run_aggregation",
        bash_command="python run_spark_agg.py",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "4Gi", "cpu": "2"},
                            limits={"memory": "8Gi", "cpu": "4"},
                        )
                    )]
                )
            )
        }
    )

    light_task >> heavy_task
```

---

## DAG Distribution: Two Approaches

### 1. Git-Sync Sidecar (Recommended)
A sidecar container runs alongside the scheduler and webserver, continuously polling a Git repository and writing DAG files to a shared volume. Task pods mount the same volume via a PVC or also run the git-sync sidecar.

**Pros:** DAGs are always up to date; no image rebuild required to deploy a new DAG  
**Cons:** Slightly more complex; requires a git repo accessible from the cluster

### 2. Baked-in Image
DAG files are copied into the Docker image at build time. Every DAG change requires building and pushing a new image and restarting Airflow pods.

**Pros:** Immutable and auditable — you know exactly which DAGs were running at any point  
**Cons:** Slower iteration; image can become large; requires CI/CD pipeline for every DAG change

---

## Essential kubectl Commands for Airflow Debugging

```bash
# List all Airflow pods in the namespace
kubectl get pods -n airflow

# Check scheduler and webserver health
kubectl logs -n airflow deployment/airflow-scheduler -f
kubectl logs -n airflow deployment/airflow-webserver

# Watch task pods appear and disappear during a DAG run
kubectl get pods -n airflow -w

# Debug a failed task pod (pod may still exist briefly)
kubectl describe pod <task-pod-name> -n airflow
kubectl logs <task-pod-name> -n airflow

# If pod is gone, check Airflow logs backend (S3, GCS, or PVC)
# Or use the Airflow UI → Task Instance → Log tab
```

---

## Key Concepts Summary

| Concept | What It Means |
|---|---|
| Executor | How Airflow distributes tasks (Local/Celery/Kubernetes) |
| Pod Template | Base K8s pod spec applied to all KubernetesExecutor task pods |
| executor_config | Per-task override for pod resources, image, env vars |
| git-sync | Sidecar that keeps DAGs in sync from a git repo |
| KubernetesPodOperator | Run any container as a task (not just Airflow worker image) |
| restartPolicy: Never | Prevents K8s from restarting failed tasks — Airflow owns retry logic |

---

## ▶️ Try It Yourself

```bash
# Install Airflow via Helm (basic, local dev)
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install with KubernetesExecutor enabled
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  --set executor=KubernetesExecutor \
  --set dags.gitSync.enabled=true \
  --set dags.gitSync.repo=https://github.com/your-org/your-dags \
  --set dags.gitSync.branch=main

# Check status
kubectl get pods -n airflow
```

---
