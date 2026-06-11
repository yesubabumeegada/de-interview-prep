---
title: "Kubernetes for Data - Scenarios"
topic: docker-and-kubernetes
subtopic: kubernetes-for-data
content_type: scenario_question


tags: [docker, kubernetes, kubernetes-for-data]
---

# Kubernetes for Data — Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Running a Spark Job on K8s

**Scenario:** You have a PySpark job that reads from S3 and writes to Snowflake. How do you run it on Kubernetes?

<details>
<summary>💡 Hint</summary>

Build a Docker image with your Spark code. Submit via spark-submit pointing to the K8s API. The job creates a driver pod which creates executor pods, runs the job, and cleans up.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# 1. Build image containing your job
docker build -t registry/revenue-spark:v1 .
docker push registry/revenue-spark:v1

# 2. Submit to K8s
spark-submit   --master k8s://https://k8s-endpoint:6443   --deploy-mode cluster   --conf spark.executor.instances=4   --conf spark.executor.memory=4g   --conf spark.kubernetes.container.image=registry/revenue-spark:v1   --conf spark.kubernetes.namespace=data-platform   local:///app/revenue_job.py

# 3. Monitor
kubectl logs -f <driver-pod-name> -n data-platform
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Airflow Tasks Need Different Resources

**Scenario:** You have Airflow DAGs where some tasks need 512MB RAM (API calls) and others need 16GB RAM (Spark aggregations). Currently all tasks run on shared Celery workers with 4GB RAM. How do you fix this with KubernetesExecutor?

<details>
<summary>💡 Hint</summary>

KubernetesExecutor runs each task in its own pod with configurable resources. Specify per-task resource requests in the task's `executor_config`. Light tasks get minimal resources; heavy tasks get what they need.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Light task: API call
api_task = PythonOperator(
    task_id="call_external_api",
    python_callable=call_api,
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "256Mi",
            "request_cpu": "100m",
            "limit_memory": "512Mi",
        }
    },
)

# Heavy task: Spark aggregation
spark_task = KubernetesPodOperator(
    task_id="spark_aggregation",
    image="registry/spark-job:v1",
    namespace="data-platform",
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "4", "memory": "16Gi"},
        limits={"cpu": "8", "memory": "24Gi"},
    ),
)
```

Each task runs in isolated pod — no resource contention, no shared worker limits.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Spark Platform for 20 Teams

**Scenario:** Design a shared Kubernetes-based Spark platform for 20 data teams. Each team needs: isolated resources, cost visibility, and self-service job submission without kubectl access.

<details>
<summary>💡 Hint</summary>

Namespace per team for isolation. ResourceQuota per namespace for cost control. Spark Operator for job submission (teams create SparkApplication CRs, not run spark-submit). RBAC: teams can create SparkApplication in their namespace only. Karpenter for cost-efficient scaling. Cost labels for showback.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# Team namespace + quota
apiVersion: v1
kind: Namespace
metadata:
  name: finance-de
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: finance-quota
  namespace: finance-de
spec:
  hard:
    requests.cpu: "20"
    requests.memory: 80Gi
    limits.cpu: "40"
    count/sparkapplications.sparkoperator.k8s.io: "10"
---
# RBAC: finance team can manage SparkApplications in their namespace only
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: finance-spark-user
  namespace: finance-de
subjects:
  - kind: Group
    name: finance-de-team
roleRef:
  kind: Role
  name: spark-user
  apiGroup: rbac.authorization.k8s.io
```

```yaml
# Teams submit via SparkApplication CR (self-service, no kubectl cluster access)
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: revenue-daily
  namespace: finance-de
  labels:
    team: finance-de
    cost-center: FIN-001
spec:
  type: Python
  image: registry/finance-spark:v1
  mainApplicationFile: local:///app/revenue.py
  driver:
    cores: 1
    memory: "2g"
  executor:
    cores: 4
    memory: "8g"
    instances: 5
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is KubernetesExecutor in Airflow and how does it differ from CeleryExecutor?**
A: KubernetesExecutor runs each Airflow task in its own K8s pod — complete isolation, per-task resource specifications, and no shared worker queue. CeleryExecutor uses shared worker nodes (all tasks run on the same machines with fixed resources).

**Q: What are the advantages of running Spark on Kubernetes over dedicated Spark clusters?**
A: Cost (scale to zero when idle), isolation (namespaces per team), resource sharing (K8s bin-packing), and unified infrastructure (one cluster for all workloads vs. separate EMR/YARN clusters).

**Q: What is the Spark Operator and why use it over spark-submit?**
A: The Spark Operator is a K8s controller that manages SparkApplication custom resources. Teams declare their Spark job as a YAML spec and the operator handles submission, monitoring, and cleanup. More K8s-native than raw spark-submit and integrates with K8s RBAC.

**Q: How do you handle Spark shuffle data on Kubernetes?**
A: Spark shuffle spills intermediate data during wide transformations. On K8s: use local SSDs (host paths or local PVs) for fastest shuffle, or NVMe-backed PVCs. Configure `spark.local.dir` to point to the fast storage. Avoid shuffling to S3 directly (latency).

**Q: What is Karpenter and how does it help DE workloads on K8s?**
A: Karpenter is a K8s autoscaler that provisions nodes just-in-time based on pending pod requirements. For Spark/DE workloads, this means: zero nodes when idle (cost savings), and exactly the right instance type for each job (memory-optimized for Spark, GPU for ML).

---

## 💼 Interview Tips

- Connect Kubernetes for data to cost savings — "scale to zero" is the most compelling business argument for K8s over static clusters.
- Know KubernetesExecutor for Airflow — it's the modern production pattern and comes up in almost every senior Airflow architecture question.
- Mention the Spark Operator as the preferred way to run Spark on K8s — it's more production-grade than spark-submit and shows awareness of the ecosystem.
- For multi-team questions, namespace isolation + ResourceQuota is the standard pattern — know it specifically.
- Discuss the trade-off of K8s Spark: 90-second cold start vs. always-on EMR. For batch jobs it's fine; for interactive queries it may not be acceptable.
- Avoid claiming K8s is always better than managed services — for very large Spark jobs or streaming, EMR or Databricks may still be appropriate. Show nuanced judgment.
