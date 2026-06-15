---
title: "Airflow on Kubernetes - Senior Deep Dive"
topic: docker-and-kubernetes
subtopic: airflow-on-kubernetes
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [docker-and-kubernetes, airflow, kubernetes, kubernetesexecutor, resource-optimization, production-architecture, data-engineering]
---

# Airflow on Kubernetes — Senior Deep Dive

## KubernetesExecutor Internals: What Happens When a Task Runs

Understanding the exact execution path is critical for diagnosing subtle production issues and for architectural conversations in senior interviews.

### Full Execution Flow

```
1. DAG file parsed by scheduler → task instance marked "scheduled"
2. Scheduler calls Kubernetes API: POST /api/v1/namespaces/airflow/pods
   → Pod spec = pod_template_file + per-task overrides (executor_config)
   → Pod name format: {dag_id}-{task_id}-{run_id}-{try_number}
3. Kubernetes scheduler places pod on a node
4. kubelet on the node pulls the container image (or uses cache)
5. Pod's init containers run (if any — e.g., git-sync to fetch DAG file)
6. Main container starts: runs `airflow tasks run {dag_id} {task_id} {execution_date}`
7. Task reads the DAG file from the mounted volume
8. Task executes, logs stream to configured backend (S3/GCS or PVC)
9. Pod exits: exit code 0 = success, non-zero = failure
10. Airflow scheduler detects pod completion via K8s watch API
    → Updates task instance state in the metadata DB
11. Pod is deleted (if is_delete_operator_pod=True or via TTL)
```

### The Critical Race: Scheduler ↔ K8s State Sync

A subtle issue at scale: if the Airflow scheduler restarts mid-flight while hundreds of task pods are running, it must **reconcile** its state with Kubernetes. The scheduler does this by listing pods with the label `airflow-worker=True` and comparing their state to the metadata DB. Pods in "Running" state whose task instance is not "running" in the DB will be adopted. This is why the pod naming convention includes `dag_id`, `task_id`, and `try_number` — the scheduler can reconstruct context purely from the pod name.

---

## Resource Optimization: Right-Sizing Task Pods

Over-provisioning task pods is one of the most common (and expensive) mistakes on K8s-based Airflow platforms. At 10,000 task runs/day with 2Gi memory requests each, you're reserving 20TB-hours of memory per day — even if tasks use 200MB on average.

### Strategy 1: Metrics-Driven Right-Sizing

```python
# Add resource telemetry to your tasks
import resource
import time

def measure_task_resources(task_callable):
    """Decorator to measure peak memory and CPU time."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        
        result = task_callable(*args, **kwargs)
        
        end_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        elapsed = time.time() - start_time
        peak_mb = (end_mem - start_mem) / 1024  # Linux: KB → MB
        
        print(f"[RESOURCE_TELEMETRY] peak_memory_mb={peak_mb:.1f} elapsed_s={elapsed:.1f}")
        return result
    return wrapper

@measure_task_resources
def transform_revenue_data(**context):
    # ... your task logic
    pass
```

Collect this telemetry in CloudWatch/Stackdriver for 2-4 weeks, then set:
- `requests.memory` = p95 peak memory (what 95% of runs need)
- `limits.memory` = p99.9 peak memory (only 1-in-1000 runs might hit this)
- `requests.cpu` = average CPU during execution  
- `limits.cpu` = leave unset or set very high (CPU throttling is preferable to OOMKill)

### Strategy 2: Task Class Profiles

Define resource profiles for task classes rather than per-task:

```python
# resources.py — shared profiles
from kubernetes.client import models as k8s

RESOURCE_PROFILES = {
    "lightweight": k8s.V1ResourceRequirements(
        requests={"memory": "256Mi", "cpu": "100m"},
        limits={"memory": "512Mi", "cpu": "500m"},
    ),
    "standard": k8s.V1ResourceRequirements(
        requests={"memory": "1Gi", "cpu": "500m"},
        limits={"memory": "2Gi", "cpu": "1"},
    ),
    "heavy": k8s.V1ResourceRequirements(
        requests={"memory": "4Gi", "cpu": "2"},
        limits={"memory": "8Gi", "cpu": "4"},
    ),
    "spark_driver": k8s.V1ResourceRequirements(
        requests={"memory": "8Gi", "cpu": "4"},
        limits={"memory": "16Gi", "cpu": "8"},
    ),
}

def make_executor_config(profile: str, node_selector: dict = None) -> dict:
    resources = RESOURCE_PROFILES[profile]
    pod_spec = k8s.V1PodSpec(
        containers=[k8s.V1Container(name="base", resources=resources)],
        node_selector=node_selector or {},
    )
    return {"pod_override": k8s.V1Pod(spec=pod_spec)}
```

```python
# Usage in DAG
from resources import make_executor_config

check_task = PythonOperator(
    task_id="check_data_quality",
    python_callable=check_quality,
    executor_config=make_executor_config("lightweight"),
)

agg_task = PythonOperator(
    task_id="aggregate_revenue",
    python_callable=run_aggregation,
    executor_config=make_executor_config("heavy"),
)
```

---

## Multi-Namespace Task Pod Isolation

For organizations with multiple data teams, running all task pods in the `airflow` namespace creates a blast radius problem. A single misconfigured task can consume all CPU in the namespace. Production solution: route task pods to team-specific namespaces.

```python
# Airflow config: allow tasks to specify their namespace
# airflow.cfg
[kubernetes]
namespace = airflow           # default namespace for task pods
multi_namespace_mode = True   # allow pods in other namespaces
```

```python
# DAG: finance team's tasks run in finance-de namespace
finance_task = PythonOperator(
    task_id="calculate_revenue",
    python_callable=calculate_revenue,
    executor_config={
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="finance-de"),
            spec=k8s.V1PodSpec(
                service_account_name="finance-de-airflow-worker",
                containers=[k8s.V1Container(
                    name="base",
                    resources=RESOURCE_PROFILES["standard"],
                )],
            ),
        )
    },
)
```

This requires the Airflow scheduler's ServiceAccount to have RBAC permissions to create pods in each team namespace:

```yaml
# ClusterRole for Airflow scheduler
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: airflow-scheduler-multi-namespace
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["create", "get", "list", "watch", "delete", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["list", "watch", "create"]

---
# Bind to each team namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-scheduler-binding
  namespace: finance-de  # repeat for each team namespace
subjects:
  - kind: ServiceAccount
    name: airflow-scheduler
    namespace: airflow
roleRef:
  kind: ClusterRole
  name: airflow-scheduler-multi-namespace
  apiGroup: rbac.authorization.k8s.io
```

---

## HA Airflow Scheduler: Active-Active at Scale

Since Airflow 2.0, multiple scheduler instances can run concurrently in an active-active configuration. This eliminates the scheduler as a single point of failure and increases task scheduling throughput.

```yaml
# Helm values: HA scheduler
scheduler:
  replicas: 3
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  # Scheduler HA requires a lock mechanism — uses Postgres advisory locks
  # All 3 schedulers compete for the "schedule" lock; only one wins per DAG
```

**How it works:** Each scheduler instance holds an advisory lock on the Postgres metadata DB for the DAGs it owns. If one scheduler fails, the lock expires and another instance picks up those DAGs within 30-60 seconds (configurable via `scheduler_heartbeat_sec`).

**Required:** A HA metadata DB (Postgres with Multi-AZ on RDS, or Cloud SQL with HA) — the schedulers themselves are stateless but all share the DB.

---

## Debugging Failed Task Pods: Production Checklist

```bash
# Step 1: Find the failed pod (may be deleted — check recent history)
kubectl get pods -n airflow --sort-by='.metadata.creationTimestamp' | tail -20

# If pod was deleted too fast, increase TTL for failed pods
# In Helm values: workers.terminationGracePeriodSeconds: 600

# Step 2: Examine the pod
kubectl describe pod <failed-pod-name> -n airflow
# Key sections to check:
# - Events: OOMKilled, ImagePullBackOff, CreateContainerConfigError
# - Last State: exit code (1=app error, 137=OOMKill, 139=segfault)
# - Conditions: PodScheduled=False means no node has capacity

# Step 3: Get logs
kubectl logs <failed-pod-name> -n airflow              # current/last run
kubectl logs <failed-pod-name> -n airflow --previous   # previous restart

# Step 4: Common failure patterns and fixes
# OOMKilled (exit 137):  Increase memory limit or fix memory leak
# ImagePullBackOff:      Check image name, tag, ECR/registry credentials
# CrashLoopBackOff:      App error — check logs --previous for traceback
# Pending (no node):     Insufficient cluster capacity — check node autoscaler
# CreateContainerConfigError: Missing Secret or ConfigMap reference
# Init container failed: git-sync error (bad credentials, repo not found)

# Step 5: Reproduce locally
docker run --rm \
  -e AIRFLOW__CORE__EXECUTOR=LocalExecutor \
  your-registry/airflow:2.9.2-custom-v1.3 \
  airflow tasks test your_dag_id your_task_id 2024-01-01
```

---

## Cost Optimization: Spot Instances for Task Pods

KubernetesExecutor is uniquely well-suited for spot/preemptible instances because tasks are short-lived and easily retried. The Airflow retry mechanism handles spot instance preemptions gracefully.

```yaml
# Karpenter NodePool for Airflow task pods on Spot
apiVersion: karpenter.sh/v1beta1
kind: NodePool
metadata:
  name: airflow-tasks-spot
spec:
  template:
    metadata:
      labels:
        node-type: airflow-task
    spec:
      requirements:
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot", "on-demand"]  # prefer spot, fallback to on-demand
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["m5.2xlarge", "m5.4xlarge", "m5a.2xlarge", "m5a.4xlarge"]
      taints:
        - key: airflow-task
          value: "true"
          effect: NoSchedule
  limits:
    cpu: 1000
    memory: 4000Gi
  disruption:
    consolidationPolicy: WhenEmpty  # scale down idle nodes aggressively
```

```yaml
# Pod template: tolerate the airflow-task taint (only task pods land here)
# In Airflow pod_template_file.yaml
spec:
  tolerations:
    - key: airflow-task
      operator: Equal
      value: "true"
      effect: NoSchedule
  nodeSelector:
    node-type: airflow-task
```

**Business impact:** Spot instances are 60-80% cheaper than on-demand. With 1000 task-pod-hours/day at $0.20/hour on-demand, moving to spot saves ~$45,000-55,000/year for a mid-size DE platform.

---

## Airflow Metadata DB: Production Considerations

The metadata DB is the most critical component — if it's unavailable, the scheduler cannot update task states and the webserver cannot display anything.

```sql
-- Monitor metadata DB health: key tables and their growth
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    n_live_tup AS live_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- task_instance grows fast — clean up old records
-- In airflow.cfg:
-- [core]
-- min_serialized_dag_fetch_interval = 30
-- [scheduler]
-- dag_dir_list_interval = 30
```

```bash
# Airflow DB cleanup (built-in since 2.4)
airflow db clean --clean-before-timestamp "2024-01-01T00:00:00" --yes

# Or via K8s CronJob
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: CronJob
metadata:
  name: airflow-db-cleanup
  namespace: airflow
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: airflow-worker
          containers:
          - name: cleanup
            image: your-registry/airflow:2.9.2-custom-v1.3
            command: ["airflow", "db", "clean", "--clean-before-timestamp", "$(date -d '90 days ago' +%Y-%m-%dT%H:%M:%S)", "--yes"]
          restartPolicy: OnFailure
EOF
```

---

## Senior Interview Talking Points

1. **Scheduler HA is a DB bottleneck problem, not a compute problem** — all schedulers share one Postgres connection pool; right-size the DB instance before adding more schedulers.

2. **KubernetesExecutor cold start latency** — each task has 10-30s overhead for pod creation and image pull. For tasks that run in under 60 seconds, this overhead is significant. Consider LocalExecutor or CeleryExecutor for very short tasks at high concurrency.

3. **DAG serialization** — Airflow 2.x serializes DAGs to the metadata DB so the webserver doesn't need to parse DAG files. This means the webserver can display DAG structure even without DAG file access — important for K8s deployments where pods may not have DAG volumes mounted.

4. **The git-sync sidecar creates a symlink** — `dags/` actually points to `dags/repoX/commit-hash/dags/`, so the "current" version changes atomically when git-sync pulls. This prevents a partially-written DAG from being parsed.

5. **Never use the Airflow webserver for triggering tasks in CI** — use the REST API or `airflow dags trigger` from a CI pod. This is stateless and doesn't create race conditions with the webserver.

---
