---
title: "Data Platform Infrastructure - Scenario Questions"
topic: docker-and-kubernetes
subtopic: data-platform-infrastructure
content_type: scenario_question
tags: [docker-and-kubernetes, kubernetes, data-platform, spark, flink, argocd, rbac, interview, scenarios]
---

# Data Platform Infrastructure — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: A Spark Job Fails with "Namespace Quota Exceeded"

**Scenario:** You submit a new SparkApplication for a batch aggregation job. The job fails immediately with the error: `Error from server: pods "revenue-agg-driver" is forbidden: exceeded quota: finance-de-quota, requested: requests.memory=16Gi, used: requests.memory=192Gi, limited: requests.memory=200Gi`. What does this mean, and how do you resolve it?

<details>
<summary>✅ Solution</summary>

**Understanding the error:**
- Your namespace `finance-de` has a ResourceQuota limiting total memory requests to 200Gi
- Currently 192Gi is already in use by running pods
- Your new Spark driver is requesting 16Gi, which would bring the total to 208Gi — 8Gi over the limit
- Kubernetes refuses to schedule the pod

```bash
# Step 1: Check the current quota usage in detail
kubectl describe resourcequota finance-de-quota -n finance-de

# Output shows:
# requests.memory: 192Gi / 200Gi  ← only 8Gi available
# requests.cpu:    45 / 50         ← only 5 cores available

# Step 2: Find what's consuming the quota
kubectl get pods -n finance-de \
  -o custom-columns="NAME:.metadata.name,MEM_REQUEST:.spec.containers[0].resources.requests.memory" \
  | sort

# Look for:
# - Idle Spark jobs that have completed but pods haven't been cleaned up
# - Other SparkApplications that are still running
# - JupyterHub notebook servers left running

# Step 3: Clean up completed/failed Spark applications
kubectl get sparkapplications -n finance-de
# Check for COMPLETED or FAILED applications — their driver pods still hold quota!

kubectl delete sparkapplication old-completed-job-20240110 -n finance-de
# Deleting the SparkApplication cleans up driver + executor pods → quota freed

# Step 4: Short-term workaround — reduce your job's resource requests
# Edit SparkApplication:
# driver.memory: "4096m"   (was 16384m)
# executor.instances: 3    (was 10)
# This lets the job start now; optimize resources after validating correctness

# Step 5: Long-term — request quota increase
# File a PR to: data-platform-infra/cluster/quotas/finance-de-quota.yaml
# Increase requests.memory from 200Gi to 300Gi
# Requires platform team review and approval
```

**Key points for the interview:**
- ResourceQuota is a hard limit — K8s enforces it at admission time, not at scheduling
- Always check running SparkApplications first; completed job driver pods linger and hold quota
- Right-sizing resource requests (not over-provisioning) is the sustainable long-term solution
- The `kubectl describe resourcequota` output shows used vs. hard limits — always your first debugging step

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design RBAC for a New Data Team Onboarding

**Scenario:** A new team called "operations-analytics" is joining the data platform. They need to: (1) run SparkApplications in their own namespace, (2) read secrets for their database connections, (3) view logs from their pods, (4) NOT be able to access other teams' namespaces or secrets. Design the complete RBAC setup including namespace, ServiceAccount, Role, and RoleBinding.

<details>
<summary>✅ Solution</summary>

```yaml
# 1. Create the namespace with proper labels for monitoring/cost tracking
apiVersion: v1
kind: Namespace
metadata:
  name: operations-analytics
  labels:
    team: operations-analytics
    cost-center: ops-001
    managed-by: platform-team

---
# 2. ServiceAccount for automated pipelines (Airflow tasks, CI/CD jobs)
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ops-analytics-pipeline-sa
  namespace: operations-analytics
  annotations:
    # IRSA: allow this SA to assume an IAM role for AWS API access (S3, Secrets Manager)
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789:role/ops-analytics-pipeline-role"

---
# 3. Role: what pipelines (and team lead humans) can do in their namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ops-analytics-role
  namespace: operations-analytics
rules:
  # Spark jobs
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications", "scheduledsparkapplications"]
    verbs: ["create", "get", "list", "watch", "delete", "patch", "update"]

  # Batch jobs (dbt, one-off scripts)
  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["create", "get", "list", "watch", "delete", "patch"]

  # Pod inspection (not exec — principle of least privilege)
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]

  # ConfigMaps (non-sensitive config)
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "update", "delete"]

  # Secrets: READ ONLY, only specific named secrets
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["ops-db-credentials", "s3-access-keys", "snowflake-creds"]
    verbs: ["get"]
    # Note: cannot LIST secrets (prevents discovering secret names)
    # Cannot CREATE/UPDATE/DELETE secrets (platform team manages these)

  # ExternalSecrets: team can view but not create ExternalSecrets
  - apiGroups: ["external-secrets.io"]
    resources: ["externalsecrets"]
    verbs: ["get", "list", "watch"]

---
# 4. RoleBinding: bind the Role to the ServiceAccount
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ops-analytics-pipeline-binding
  namespace: operations-analytics
subjects:
  - kind: ServiceAccount
    name: ops-analytics-pipeline-sa
    namespace: operations-analytics
roleRef:
  kind: Role
  name: ops-analytics-role
  apiGroup: rbac.authorization.k8s.io

---
# 5. RoleBinding: bind team lead humans via SSO group
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ops-analytics-humans-binding
  namespace: operations-analytics
subjects:
  - kind: Group
    name: "operations-analytics-leads"  # SSO group mapped via aws-auth configmap
    apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: ops-analytics-role
  apiGroup: rbac.authorization.k8s.io

---
# 6. ResourceQuota: enforce cost limits
apiVersion: v1
kind: ResourceQuota
metadata:
  name: ops-analytics-quota
  namespace: operations-analytics
spec:
  hard:
    requests.cpu: "40"
    requests.memory: "160Gi"
    limits.memory: "320Gi"
    count/pods: "100"
    count/sparkapplications.sparkoperator.k8s.io: "5"

---
# 7. LimitRange: defaults for pods without resource specs
apiVersion: v1
kind: LimitRange
metadata:
  name: ops-analytics-limits
  namespace: operations-analytics
spec:
  limits:
    - type: Container
      default: {memory: "1Gi", cpu: "500m"}
      defaultRequest: {memory: "256Mi", cpu: "100m"}
      max: {memory: "32Gi", cpu: "16"}
      min: {memory: "64Mi", cpu: "10m"}
```

```bash
# Verification: confirm team cannot access other namespaces
kubectl auth can-i get pods \
  --as=system:serviceaccount:operations-analytics:ops-analytics-pipeline-sa \
  -n finance-de
# Output: "no" ✓

kubectl auth can-i list secrets \
  --as=system:serviceaccount:operations-analytics:ops-analytics-pipeline-sa \
  -n operations-analytics
# Output: "no" ✓ (can GET specific named secrets, but cannot LIST all secrets)

kubectl auth can-i create sparkapplications \
  --as=system:serviceaccount:operations-analytics:ops-analytics-pipeline-sa \
  -n operations-analytics
# Output: "yes" ✓
```

**Interview notes:**
- Use `resourceNames` on secrets to restrict access to specific named secrets, not all secrets in the namespace
- Separate ServiceAccount for pipelines vs human team leads (pipelines shouldn't have everything a human team lead has)
- IRSA annotation on the ServiceAccount is how AWS API access works — no access keys needed
- Always verify with `kubectl auth can-i --as=` after creating RBAC — test the negative cases (cross-namespace access) not just the positive cases

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Architect a Cost-Optimized Kubernetes Data Platform for $2M/year Budget

**Scenario:** You're joining a company that runs its data platform on Kubernetes (EKS) with a $2M/year infrastructure budget. The CEO sees the bill and wants to cut costs 40% without reducing capability. The current setup: 200 always-on m5.4xlarge nodes ($0.768/hr each) running Spark, Airflow, JupyterHub, and dbt jobs for 30 teams. Design the architecture changes to hit the 40% reduction target, quantify the savings, and explain the risks.

<details>
<summary>✅ Solution</summary>

**Current state analysis:**

```
Current: 200 × m5.4xlarge × $0.768/hr × 8760 hr/year = $1,346,688/year
+ Other costs (RDS, S3, networking, data transfer): ~$650,000/year
Total: ~$2M/year

Problem: Always-on nodes for batch workloads = massive idle waste
Typical utilization of batch-heavy clusters: 20-35% average CPU utilization
That means 65-80% of compute is paying for idle capacity
```

**The 5-lever strategy:**

**Lever 1: Karpenter + Spot Instances (saves ~$600K/year)**

```yaml
# Replace always-on nodes with Karpenter-managed spot instances

# Before: 200 × m5.4xlarge on-demand = $1,346,688/year
# After: Karpenter scales to actual demand

# Workload analysis:
# - Airflow task pods: run 2-4 hours/day, bursty → perfect for spot
# - Spark jobs: batch, retryable → ideal for spot
# - Flink streaming: stateful, intolerant to preemption → keep on-demand

# Spot pricing: m5.4xlarge spot ≈ $0.23/hr (vs $0.768/hr on-demand = 70% discount)

# With Karpenter:
# - System nodes (Airflow sched, webserver, operators): 6 × m5.2xlarge on-demand
#   = 6 × $0.384 × 8760 = $20,183/year
# - Batch nodes (Spark, Airflow tasks, dbt): Karpenter spot, ~60 avg nodes
#   = 60 × $0.23 × 8760 × 0.6 utilization factor = $72,634/year
# - Streaming nodes (Flink): 8 × m5.2xlarge on-demand
#   = 8 × $0.384 × 8760 = $26,910/year
# Total compute: ~$120,000/year
# Savings: $1,346,688 - $120,000 = $1,226,688/year ← major lever
```

**Lever 2: Karpenter Consolidation for Batch Nodes (saves ~$80K/year)**

```yaml
# WhenUnderutilized consolidation on batch pool
# Without consolidation: nodes stay up even at 10% utilization
# With consolidation: 40-50% node reduction through bin-packing
# (Already partly accounted in Lever 1 estimate)
```

**Lever 3: JupyterHub Notebook Culling (saves ~$120K/year)**

```python
# Analysis: 30 teams × 10 users each = 300 notebook users
# Without culling: average 60% notebooks idle at any time
# = 180 idle notebooks × m5.xlarge ($0.192/hr) × 8760 hr × 60% idle
# = $193,000 in idle notebook cost

# With 1-hour culling:
# Idle rate drops to ~10% → 30 idle notebooks
# = $32,000/year → savings of ~$161,000

# Plus switch notebooks to spot (notebooks are restartable):
# m5.xlarge spot ≈ $0.058/hr vs $0.192/hr → additional 70% savings on notebook compute
```

**Lever 4: RDS Right-Sizing (saves ~$40K/year)**

```bash
# Check actual RDS utilization
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS \
  --metric-name CPUUtilization \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-31T23:59:59Z \
  --period 3600 \
  --statistics Average \
  --dimensions Name=DBInstanceIdentifier,Value=airflow-metadata-db

# Typical finding: Airflow metadata DB on db.r5.4xlarge at 5-8% CPU
# Right-size: db.r5.xlarge + Aurora Serverless v2 for MLflow (usage-based billing)
# Savings: $40,000-60,000/year
```

**Lever 5: S3 Lifecycle Policies for Spark Logs and Checkpoints (saves ~$30K/year)**

```python
# Spark logs and checkpoints accumulate indefinitely without cleanup
# Typical S3 bill breakdown:
# - Spark job logs (kept forever): 50TB × $0.023/GB/month × 12 = $13,800/year
# - Flink checkpoints (never purged): 30TB = $8,280/year
# - Airflow logs (kept forever): 20TB = $5,520/year

# S3 lifecycle policy: archive to Glacier after 90 days, delete after 1 year
# Cost reduction: 80-90% of storage cost → saves ~$25,000-$30,000/year
```

**Summary and risk assessment:**

```
Lever          Annual Savings    Risk Level    Mitigation
-----          --------------    ----------    ----------
Spot instances     $1,226,688    Medium        Karpenter interruption handling,
                                               retry mechanisms in Spark/Airflow
Consolidation      included ↑    Low           PDB for streaming workloads
Notebook culling   $161,000      Low           Teams warned, culling period tunable
RDS right-sizing   $50,000       Low           Monitor CPU/connections for 2 weeks after
S3 lifecycle       $30,000       Very Low      Keep 1-year retention minimum for compliance
─────────────────────────────────────
Total saves:     ~$800,000-900,000 (~40-45%)

Residual risks:
1. Spot interruptions during critical end-of-month Spark jobs (finance team)
   Mitigation: on-demand fallback capacity type in Karpenter for critical jobs only

2. Flink state corruption on spot preemption
   Mitigation: streaming nodes stay on-demand; only batch pool on spot

3. User resistance to notebook culling
   Mitigation: 2-hour warning notification before cull; save-checkpoint functionality

Implementation timeline: 3 months phased rollout
Month 1: Karpenter + spot for non-critical workloads; monitor reliability
Month 2: Notebook culling; RDS right-sizing
Month 3: S3 lifecycle; consolidation tuning based on Month 1 data
```

**Interview notes:**
- Always quantify before recommending — "use spot instances" without numbers is weak
- The biggest savings are always in compute (spot instances), not optimization of peripherals
- Stateful streaming workloads (Flink) need different treatment than batch — don't treat them the same
- Present risks with mitigations, not as blockers — interviewers want to see you can handle trade-offs
- Implementation timeline shows operational maturity — you don't change everything at once

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between ResourceQuota and LimitRange?**
A: ResourceQuota enforces aggregate limits across all objects in a namespace (e.g., max 200Gi total memory requested across all pods). LimitRange enforces per-object defaults and min/max (e.g., each container must request at least 64Mi and at most 32Gi). They complement each other: LimitRange catches pods with missing resource specs; ResourceQuota prevents a namespace from overclaiming cluster capacity.

**Q: Why use Karpenter instead of Cluster Autoscaler?**
A: Cluster Autoscaler scales down only completely empty nodes — in batch workloads, nodes are rarely empty, just underutilized. Karpenter's `WhenUnderutilized` consolidation actively bin-packs pods and removes underutilized nodes, typically achieving 50-70% better cost efficiency for batch-heavy clusters. Karpenter also provisions the exact right node size for pending pods (no wasted capacity from fixed node group sizes).

**Q: What storage class should you use for Spark shuffle on Kubernetes?**
A: Local instance store (NVMe SSDs) via `hostPath` volumes on NVMe-enabled instances (i3, i4i family on AWS). Spark shuffle is extremely I/O intensive — EFS is too slow and expensive, EBS has limited IOPS per volume. Local NVMe is ephemeral (lost on node termination), but Spark handles this via stage retry. For stateful checkpoints, use S3 — not local storage.

**Q: What is ArgoCD and why is it used for data platforms?**
A: ArgoCD is a GitOps continuous delivery tool that continuously reconciles Kubernetes cluster state to match manifests in a git repository. For data platforms, it provides: audit trail of every infrastructure change, automatic drift detection and correction, self-service deployments for teams via PR workflows, and fast disaster recovery (restore full platform from git). It replaces manual `kubectl apply` workflows that are error-prone and un-auditable.

**Q: How do you prevent a team's Spark job from consuming all cluster resources?**
A: Two-layer defense: (1) ResourceQuota at the namespace level caps total CPU/memory requests across all the team's pods — they literally cannot schedule more than their budget. (2) LimitRange ensures every pod has resource requests set — without it, pods with no requests bypass quota accounting. Additionally, configure Spark's `spark.dynamicAllocation.maxExecutors` below the team's quota to prevent a single job from consuming the entire quota.

**Q: What is the External Secrets Operator and how does it work?**
A: ESO is a Kubernetes operator that reads secrets from external backends (AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault) and creates Kubernetes Secrets from them. It consists of a `ClusterSecretStore` (how to connect to the external backend) and `ExternalSecret` resources (what to sync and where). ESO refreshes secrets on a configurable interval, enabling automatic rotation. It's preferred over storing secrets as K8s YAML because secrets stay in a centrally audited, rotatable backend and never exist in git.

---

## 💼 Interview Tips

- For data platform infrastructure questions, always anchor answers to real DE tools: Spark Operator, Flink Operator, JupyterHub, MLflow, Airflow — not abstract Kubernetes concepts.
- Namespace isolation is the foundation of multi-tenant data platforms — if asked "how do you isolate teams," go beyond just "use namespaces" to include ResourceQuotas, LimitRanges, RBAC, and NetworkPolicies as a complete package.
- Know the storage type tradeoffs cold: EBS (RWO, high-IOPS), EFS (RWX, shared), instance store (ephemeral, Spark shuffle), S3 (durable, object). Using EFS for everything or EBS for shared logs signals you haven't run these workloads in production.
- GitOps (ArgoCD) should come up naturally when discussing how changes are deployed — mentioning manual `kubectl apply` as your deployment mechanism signals a lack of production experience.
- Cost optimization questions are extremely common at senior levels — always quantify. "Use spot instances" is weak; "spot instances at 70% discount for batch workloads saves $X/year" shows engineering rigor.
- For RBAC design, always mention the principle of least privilege and give concrete examples: ServiceAccounts for pipelines (automated) should have fewer permissions than human team leads; `resourceNames` on secrets prevents a team from accessing all secrets in their namespace.
