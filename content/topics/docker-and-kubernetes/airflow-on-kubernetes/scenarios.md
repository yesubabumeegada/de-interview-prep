---
title: "Airflow on Kubernetes - Scenario Questions"
topic: docker-and-kubernetes
subtopic: airflow-on-kubernetes
content_type: scenario_question
tags: [docker-and-kubernetes, airflow, kubernetes, kubernetesexecutor, helm, interview, scenarios]
---

# Airflow on Kubernetes — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Airflow Task Pod is Stuck in Pending

**Scenario:** You deploy Airflow with KubernetesExecutor on a new EKS cluster. When a DAG runs, the task shows as "queued" in the Airflow UI for 10 minutes and never starts. In `kubectl get pods -n airflow`, the task pod is in `Pending` state. How do you diagnose and fix this?

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Describe the pending pod to see WHY it's pending
kubectl describe pod <task-pod-name> -n airflow

# Look at the Events section — common causes:
# "0/2 nodes are available: 2 Insufficient memory"
#   → The pod requests more memory than any node has available
# "0/2 nodes are available: 2 node(s) had untolerated taint"
#   → Pod needs to tolerate a node taint (if nodes have taints)
# "no nodes available to schedule pods"
#   → Cluster autoscaler hasn't provisioned nodes yet — wait 2-3 minutes

# Step 2: Check current node capacity
kubectl describe nodes | grep -A5 "Allocated resources"
# Compare requested resources vs allocatable capacity

# Step 3: Fix based on finding

# Fix A: Pod requests too much memory — reduce in pod template
# In Helm values.yaml → podTemplate:
# resources:
#   requests:
#     memory: "512Mi"   # was 4Gi (way too much for a simple task)
#     cpu: "250m"

# Fix B: Cluster autoscaler not configured — verify it's running
kubectl get pods -n kube-system | grep cluster-autoscaler
kubectl logs -n kube-system deployment/cluster-autoscaler | tail -30
# If autoscaler is working, it will provision new nodes within 2-3 minutes

# Fix C: Node taint not tolerated — add toleration to pod template
# In pod_template_file.yaml:
# spec:
#   tolerations:
#     - key: "dedicated"
#       operator: "Equal"
#       value: "airflow"
#       effect: "NoSchedule"

# Step 4: Verify the fix
kubectl get pods -n airflow -w
# Watch the task pod transition from Pending → Running → Completed
```

**Key concepts to mention in interview:**
- `kubectl describe pod` events section is always the first diagnostic step for Pending pods
- Resource requests determine scheduling — if no node has enough free capacity, the pod stays Pending
- Cluster autoscaler typically takes 2-5 minutes to provision a new node — this is normal, not a bug
- Always check that pod tolerations match node taints on a new cluster

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Migrate from CeleryExecutor to KubernetesExecutor

**Scenario:** Your team runs Airflow with CeleryExecutor (Redis broker, 5 always-on worker pods). The platform team asks you to migrate to KubernetesExecutor to reduce costs and improve task isolation. Walk through the migration plan, including what changes, what risks exist, and how you validate success.

<details>
<summary>✅ Solution</summary>

**What changes between executors:**

| Aspect | CeleryExecutor | KubernetesExecutor |
|---|---|---|
| Task isolation | Shared worker process | Separate pod per task |
| Dependencies | All workers must have all deps | Per-task image possible |
| Resource usage | Fixed (5 idle workers always running) | Zero when idle |
| Startup latency | Low (workers already running) | 10-30s pod startup per task |
| Debugging | Worker pod logs (shared) | Individual task pod logs |
| Flower UI | Available | Not applicable |

**Migration steps:**

```bash
# Step 1: Update Helm values — switch executor
# BEFORE: executor: "CeleryExecutor"
# AFTER:  executor: "KubernetesExecutor"

# Also remove Celery-specific components:
# redis:
#   enabled: false    # was true
# flower:
#   enabled: false    # was true
# workers:
#   replicas: 0       # or remove entirely
```

```yaml
# Step 2: Add pod template file with resource defaults
# Celery workers had resources defined in the worker deployment
# KubernetesExecutor needs the same resources in the pod template
podTemplate: |
  apiVersion: v1
  kind: Pod
  spec:
    restartPolicy: Never   # CRITICAL — do not forget this
    serviceAccountName: airflow-worker
    containers:
      - name: base
        image: your-registry/airflow:2.9.2-custom
        resources:
          requests:
            memory: "1Gi"    # match your previous worker request
            cpu: "500m"
          limits:
            memory: "4Gi"
            cpu: "2"
```

```bash
# Step 3: Validate DAGs work with the new executor
# Test in a staging environment first — run a representative sample of DAGs:
# - Simple PythonOperator tasks
# - BashOperator tasks
# - Any tasks using executor_config overrides
# - KubernetesPodOperator tasks (these should work unchanged)

# Step 4: Check for CeleryExecutor-specific DAG patterns that need updating
grep -r "queue=" your-dags/     # Celery queue routing — not used in K8s
grep -r "CeleryQueue" your-dags/

# Step 5: Migrate production
helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f values.yaml \
  --atomic           # roll back automatically if upgrade fails

# Step 6: Verify task pods appear during DAG run
kubectl get pods -n airflow -w  # watch task pods come and go
```

**Risks and mitigations:**
- **Startup latency:** Tasks that ran in 10s now have 20-30s overhead. For latency-sensitive tasks, keep using CeleryExecutor on a separate cluster or use LocalExecutor for that specific pipeline.
- **Missing task pod logs:** In Celery, logs stayed on worker pods indefinitely. In KubernetesExecutor, pods are deleted after completion. Ensure S3/GCS remote logging is configured BEFORE migrating.
- **executor_config not previously used:** DAGs that relied on Celery queues for routing need to be updated to use `node_selector` or `executor_config` for resource routing in K8s.

**Cost calculation:**
```
CeleryExecutor: 5 workers × m5.xlarge ($0.192/hr) × 720 hr/month = $691/month
KubernetesExecutor: task pods run for ~2 hours/day total × 730 hr/month ratio
  → ~60 pod-hours/month × $0.05/hr (spot) = ~$3/month in task compute
  (system nodes for scheduler/webserver still exist — ~$100/month)
Net savings: ~$590/month
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Airflow on Kubernetes for a 50-Team Data Platform

**Scenario:** Your company has 50 data engineering teams, each running 10-30 DAGs with different Python dependency requirements, different resource needs (some teams do heavy Spark work, others do lightweight API calls), and different security requirements (finance team handles PCI data). Design the Airflow on Kubernetes architecture to support this at scale with proper isolation, cost efficiency, and operational simplicity.

<details>
<summary>✅ Solution</summary>

**Core design principles:**
1. Team isolation via namespaces + RBAC
2. Dependency isolation via per-team Docker images
3. Resource isolation via ResourceQuotas + per-task overrides
4. Secret isolation via namespace-scoped External Secrets
5. Cost optimization via Spot + Karpenter

**Cluster topology:**

```yaml
# Node pools:
# 1. system (on-demand, m5.2xlarge x3):
#    - Airflow scheduler (3 replicas, HA)
#    - Airflow webserver (2 replicas)
#    - Karpenter controller, ArgoCD, monitoring stack

# 2. standard-tasks (Karpenter-managed, spot, m5/m5a family):
#    - 95% of task pods land here
#    - Scales 0 → 200 nodes dynamically

# 3. memory-tasks (Karpenter-managed, spot, r5/r5a family):
#    - Heavy Spark/pandas tasks
#    - nodeSelector: node-type=memory-optimized
#    - Taint: dedicated=memory:NoSchedule

# 4. pci-tasks (on-demand, m5.xlarge, restricted AZ):
#    - Finance team tasks only
#    - Taint: compliance=pci:NoSchedule
#    - PodSecurityPolicy: restricted
#    - NetworkPolicy: no egress except to approved endpoints
```

**Namespace structure:**

```yaml
# One namespace per team
# finance-de, marketing-de, logistics-de, ... (50 namespaces)

# ResourceQuota per team namespace — prevents runaway costs
apiVersion: v1
kind: ResourceQuota
metadata:
  name: team-quota
  namespace: finance-de
spec:
  hard:
    requests.cpu: "100"          # max 100 cores requested at once
    requests.memory: "400Gi"     # max 400Gi memory requested
    limits.memory: "800Gi"
    count/pods: "200"            # max 200 pods in the namespace
    count/persistentvolumeclaims: "10"
```

**Per-team image management:**

```
Base image (platform team, updated monthly):
  your-registry/airflow-base:2.9.2-platform-v3
    ├── finance image (finance team owns, updated as needed)
    │   your-registry/airflow-finance:2.9.2-v1.8
    │   deps: pandas, openpyxl, snowflake-connector
    ├── marketing image
    │   your-registry/airflow-marketing:2.9.2-v2.1
    │   deps: google-ads, facebook-business, dbt-snowflake
    └── ml-platform image
        your-registry/airflow-ml:2.9.2-v1.2
        deps: scikit-learn, mlflow, feast
```

**DAG routing to correct namespace:**

```python
# teams define their namespace in a shared config
# config/team_config.py
TEAM_CONFIG = {
    "finance": {
        "namespace": "finance-de",
        "image": "your-registry/airflow-finance:2.9.2-v1.8",
        "service_account": "finance-de-airflow-worker",
        "default_resources": RESOURCE_PROFILES["standard"],
        "node_selector": {},  # standard nodes
    },
    "ml_platform": {
        "namespace": "ml-de",
        "image": "your-registry/airflow-ml:2.9.2-v1.2",
        "service_account": "ml-de-airflow-worker",
        "default_resources": RESOURCE_PROFILES["heavy"],
        "node_selector": {"node-type": "memory-optimized"},
    },
}

def team_executor_config(team: str, profile: str = "default") -> dict:
    """Build executor_config for a given team's tasks."""
    cfg = TEAM_CONFIG[team]
    resources = RESOURCE_PROFILES.get(profile) or cfg["default_resources"]
    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace=cfg["namespace"]),
            spec=k8s.V1PodSpec(
                service_account_name=cfg["service_account"],
                node_selector=cfg["node_selector"],
                containers=[k8s.V1Container(
                    name="base",
                    image=cfg["image"],
                    resources=resources,
                )],
            ),
        )
    }
```

**PCI isolation for finance team:**

```yaml
# NetworkPolicy: finance task pods can only reach approved destinations
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: finance-task-isolation
  namespace: finance-de
spec:
  podSelector:
    matchLabels:
      team: finance
  policyTypes:
    - Ingress
    - Egress
  egress:
    - to:
        - ipBlock:
            cidr: 10.0.0.0/8  # internal VPC only
      ports:
        - port: 5439  # Redshift
        - port: 443   # S3/Secrets Manager
    # No public internet egress for PCI data
  ingress: []  # no inbound traffic to task pods
```

**Operational model:**

```
Platform team responsibilities:
- Manage EKS cluster, Karpenter, base image
- Operate Airflow scheduler/webserver
- Maintain ResourceQuotas and RBAC
- Own monitoring stack

Team responsibilities:
- Maintain their own Docker image (update deps as needed)
- Write and deploy their own DAGs via GitOps (ArgoCD)
- Debug their own task pods (RBAC grants them read access to their namespace)
- Set resource profiles on their tasks

GitOps flow:
Developer → PR to team's DAG repo
→ CI: lint, test, build image if changed
→ ArgoCD syncs DAG repo to team namespace
→ Tasks use team's image and namespace automatically
```

**Cost accountability:**

```bash
# Kubecost or OpenCost for per-team cost reporting
# Tag all task pods with team label for attribution
# Monthly cost report per team → chargeback to business units

# Query Kubecost API for team spend
curl "https://kubecost.internal/model/allocation?window=month&aggregate=label:team"
```

**Interview summary points:**
- Namespace per team provides blast radius containment, quota enforcement, and RBAC isolation
- KubernetesExecutor's multi-namespace mode is essential at this scale — single namespace creates contention
- Image separation solves the dependency conflict problem that plagues shared Airflow installations
- PCI isolation requires both network policies AND dedicated nodes with proper taints
- GitOps (ArgoCD) enables 50 teams to self-serve without platform team bottlenecks

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the main advantage of KubernetesExecutor over CeleryExecutor?**
A: KubernetesExecutor has zero idle resource consumption — pods are created per task and deleted after completion. CeleryExecutor keeps workers running 24/7 regardless of load. KubernetesExecutor also provides true task isolation (separate pods) and allows per-task Docker images and resource configurations.

**Q: Why must pod template have `restartPolicy: Never`?**
A: Airflow manages task retries through its scheduler — it decides when and how many times to retry a failed task. If Kubernetes also restarts the pod (via `OnFailure` or `Always`), you get duplicate task executions, Airflow's retry count becomes inaccurate, and there's risk of data corruption from parallel runs.

**Q: What is the git-sync sidecar and why is it used?**
A: git-sync is a sidecar container that continuously polls a git repository and writes the latest commit to a shared volume. It keeps DAG files up to date without rebuilding the Airflow image. The alternative (baking DAGs into the image) requires a full image build and rolling restart for every DAG change.

**Q: How do you debug a task whose pod was already deleted?**
A: Enable remote logging (S3/GCS) — Airflow writes logs to the remote backend during task execution, so logs persist even after pod deletion. Without remote logging, you must configure a PVC with `ReadWriteMany` (EFS) so logs survive. Alternatively, increase `TTLSecondsAfterFinished` on the task pod to keep failed pods alive longer for debugging.

**Q: What is the External Secrets Operator and why is it preferred over plain K8s Secrets?**
A: ESO reads secrets from external backends (AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault) and syncs them into Kubernetes Secrets. It's preferred because: secrets live in a centrally audited store, rotation is automatic, engineers never commit secret values to git, and K8s Secrets (ephemeral) can be recreated automatically if the namespace is destroyed.

---

## 💼 Interview Tips

- When asked about Airflow executor types, always frame the tradeoffs: Local (simple/limited), Celery (steady workloads, Flower monitoring), Kubernetes (elastic, isolated, per-task resources) — and state that KubernetesExecutor is the production standard for DE platforms at scale.
- Mention `restartPolicy: Never` unprompted when discussing KubernetesExecutor — it signals real operational experience and catches a critical gotcha.
- For secrets questions, go beyond "use K8s Secrets" to mention the External Secrets Operator pattern with Secrets Manager — this shows enterprise-level thinking.
- Remote logging (S3/GCS) should be mentioned proactively in any production Airflow discussion — it's a non-negotiable for production use, and many candidates miss it.
- If asked about HA, mention that Airflow 2.x supports multiple schedulers via Postgres advisory locks — it's a relatively recent and less-known feature that signals depth of knowledge.
