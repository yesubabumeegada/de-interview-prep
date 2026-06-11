---
title: "Kubernetes Basics — Scenarios"
topic: docker-and-kubernetes
subtopic: kubernetes-basics
content_type: scenario_question
tags: [kubernetes, k8s, interview, scenarios, data-engineering]
---

# Kubernetes Basics — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: A Pipeline Pod Is Failing

**Scenario:** Your revenue pipeline pod is in `CrashLoopBackOff` status. Walk through diagnosing and fixing it.

<details>
<summary>💡 Hint</summary>

`CrashLoopBackOff` means the container starts and then crashes, and K8s keeps retrying. First: get logs from the crashed container (use `--previous` flag). Then describe the pod to see events. Common causes: wrong startup command, missing environment variable, out of memory, missing secret/configmap.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Check pod status and recent events
kubectl get pod revenue-pipeline-abc123 -n data-platform
kubectl describe pod revenue-pipeline-abc123 -n data-platform
# → Look at: Last State (exit code), Events (OOMKilled, Error pulling image, etc.)

# Step 2: Get logs from the crashed container
kubectl logs revenue-pipeline-abc123 --previous -n data-platform
# → The actual Python traceback / error message is here

# Common findings:
# Exit Code 1 + Python traceback → application error (KeyError, missing env var)
# OOMKilled → increase memory limit
# ImagePullBackOff → wrong image tag or registry auth issue
# CreateContainerConfigError → missing ConfigMap or Secret

# Step 3: Fix based on finding
# Example: Missing DB_URL env var
kubectl create secret generic pipeline-secrets \
  --from-literal=DB_URL="postgresql://user:pass@host:5432/db" \
  -n data-platform

# Step 4: Verify fix
kubectl rollout restart deployment/revenue-pipeline -n data-platform
kubectl get pods -n data-platform -w  # watch pods restart
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Pipeline Uses Too Much Memory

**Scenario:** A data pipeline pod keeps getting OOMKilled (out of memory). Current memory limit is 2Gi. The pipeline processes variable-sized batches. How do you fix this sustainably?

<details>
<summary>💡 Hint</summary>

OOMKilled has two solutions: (1) increase the limit (quick fix, may just defer the problem), (2) fix the pipeline to use less memory (right fix). Investigate first: is the pipeline legitimately processing more data, or is there a memory leak? Add monitoring to understand actual peak usage. Then increase limits based on observed data, add HPA to scale when needed, or redesign the pipeline to process in smaller chunks.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Understand actual memory usage
kubectl top pods -n data-platform            # current usage
kubectl describe pod <pod> | grep -A5 "OOM" # OOM events

# Step 2: Add memory monitoring to pipeline
```

```python
import resource, psutil

def log_memory_usage(stage: str):
    process = psutil.Process()
    mem_mb = process.memory_info().rss / 1e6
    print(f"[MEMORY] {stage}: {mem_mb:.1f} MB")

log_memory_usage("after_extract")    # 450 MB
log_memory_usage("after_transform")  # 1.8 GB ← spike here
log_memory_usage("after_load")       # 500 MB
```

```yaml
# Step 3: Increase limit based on actual peak + buffer
resources:
  requests:
    memory: "1Gi"   # typical usage
  limits:
    memory: "4Gi"   # peak + 50% buffer
```

```python
# Step 4 (right fix): Process in chunks to reduce peak memory
# Instead of loading all at once:
df = spark.read.table("orders").toPandas()  # 1.8 GB in memory

# Process in date partitions:
for date in date_range:
    chunk = spark.read.table("orders").filter(f"date = '{date}'").toPandas()
    process_and_write(chunk)
    del chunk  # explicitly free
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design K8s Architecture for DE Platform

**Scenario:** Design the Kubernetes architecture for a DE platform supporting 20 teams, each with different pipeline resource needs, using Airflow and Spark. Consider isolation, cost, and security.

<details>
<summary>💡 Hint</summary>

Namespace isolation per team provides blast radius containment and resource quotas. Airflow KubernetesExecutor means each task runs in its own pod — natural isolation. Spark uses the Spark Operator. Use node pools with taints for different workload types (spot instances for batch, on-demand for critical). RBAC ensures teams can only see their own resources. GitOps (ArgoCD) manages all manifests.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Cluster design:
# Node pools (EKS):
# 1. system:   on-demand, 2 nodes, for Airflow scheduler/webserver
# 2. batch:    spot, auto-scaling 2-50, for pipeline tasks (tainted)
# 3. spark:    spot with memory-optimized, auto-scaling 0-20, for Spark

# Namespace per team:
# finance-de, marketing-de, operations-de, platform

# Per namespace:
# - ResourceQuota (CPU/memory/pod limits)
# - NetworkPolicy (team isolation)
# - RBAC (ServiceAccount per team)
# - ArgoCD Application (GitOps management)

# Airflow KubernetesExecutor:
# - Scheduler runs in 'platform' namespace
# - Task pods run in team namespaces
# - Each task uses team's ServiceAccount

# Spark Operator:
# - SparkApplication CR triggers driver + executor pods
# - Executor pods on 'spark' node pool via nodeSelector
# - Auto-scales executor count based on data volume

# Cost optimization:
# - Spot instances for all batch/Spark (70% savings)
# - Karpenter for just-in-time node provisioning
# - Resource quotas prevent runaway costs per team
```

```python
# Teams submit pipelines via GitOps — not direct kubectl
# Team creates: k8s-manifests/finance-de/batch-jobs/revenue.yaml
# ArgoCD syncs it to finance-de namespace
# Finance team RBAC: read/watch pods in their namespace only
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between a Pod and a Deployment?**
A: A Pod is a single running instance of one or more containers. A Deployment manages a set of identical Pods, ensuring the desired number of replicas are always running and handling rolling updates and rollbacks.

**Q: What is `CrashLoopBackOff` and how do you diagnose it?**
A: CrashLoopBackOff means a container is crashing repeatedly and K8s is waiting longer between each restart. Diagnose with `kubectl logs <pod> --previous` (logs from the crashed container) and `kubectl describe pod <pod>` (events showing OOMKilled, exit code, image pull errors).

**Q: What is the difference between a request and a limit in Kubernetes resources?**
A: A request is the amount the scheduler uses to decide where to place the pod — the pod is guaranteed at least this much. A limit is the maximum the container can use — exceeding memory limit causes OOMKill; exceeding CPU limit causes throttling.

**Q: What is a ConfigMap and how does it differ from a Secret?**
A: ConfigMap stores non-sensitive configuration as key-value pairs or files. Secret stores sensitive data (passwords, API keys) encoded in base64. Secrets are marked as sensitive and can be encrypted at rest and restricted by RBAC. Never store passwords in ConfigMaps.

**Q: What does `kubectl rollout undo` do?**
A: It rolls back a Deployment to its previous revision. K8s keeps a history of Deployment revisions (default: 10). `--to-revision=N` rolls back to a specific revision. This is the fastest way to recover from a bad deploy.

**Q: What is a Kubernetes Job and when would a DE team use one?**
A: A Job runs a pod to completion (not indefinitely like a Deployment). Use it for batch data processing: daily ETL runs, one-time data migrations, ad-hoc Spark jobs. CronJob is a Job on a schedule — equivalent to cron but in Kubernetes.

---

## 💼 Interview Tips

- Know the `CrashLoopBackOff` debugging flow cold — it comes up in almost every K8s interview and in real incidents. Practice: describe → logs --previous → events.
- Always mention resource requests AND limits as a pair — setting only limits without requests causes scheduling issues; setting only requests without limits allows unbounded memory growth.
- For DE-specific K8s questions, connect to Airflow KubernetesExecutor — it's the most common K8s pattern for DE teams and shows practical knowledge.
- Distinguish between ConfigMap (public config) and Secret (sensitive config) explicitly — treating them as interchangeable signals you haven't worked with K8s security seriously.
- For senior architecture questions, mention namespace isolation + ResourceQuota as the multi-tenancy pattern — it's how production K8s platforms are actually organized.
- Avoid describing manual `kubectl apply` as the deployment mechanism for production — mention GitOps (ArgoCD/Flux) or CI/CD pipelines as the right approach.
