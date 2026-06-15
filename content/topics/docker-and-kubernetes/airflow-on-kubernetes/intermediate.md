---
title: "Airflow on Kubernetes - Intermediate"
topic: docker-and-kubernetes
subtopic: airflow-on-kubernetes
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker-and-kubernetes, airflow, kubernetes, helm, secrets-management, hpa, data-engineering]
---

# Airflow on Kubernetes — Intermediate

## Helm Chart Deep Dive: `values.yaml` Configuration

The official `apache/airflow` Helm chart is the standard way to deploy Airflow on Kubernetes in production. Understanding `values.yaml` is essential — it controls every aspect of the deployment.

```yaml
# values.yaml — production Airflow configuration (key sections)

# Executor selection
executor: "KubernetesExecutor"

# Airflow version
airflowVersion: "2.9.2"
defaultAirflowTag: "2.9.2"

# Image — use a custom image with your dependencies pre-installed
images:
  airflow:
    repository: your-ecr-registry/airflow
    tag: "2.9.2-custom-v1.3"
    pullPolicy: IfNotPresent

# DAG distribution via git-sync
dags:
  gitSync:
    enabled: true
    repo: https://github.com/your-org/airflow-dags.git
    branch: main
    depth: 1
    wait: 60        # poll interval in seconds
    subPath: "dags" # subdirectory within the repo
    credentialsSecret: airflow-git-secret  # K8s secret with git credentials

# Scheduler configuration
scheduler:
  replicas: 2  # HA: run 2 schedulers (Airflow 2.x supports this)
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1"

# Webserver
webserver:
  replicas: 2
  resources:
    requests:
      memory: "512Mi"
      cpu: "250m"
    limits:
      memory: "1Gi"
      cpu: "500m"
  service:
    type: ClusterIP  # use with an Ingress for external access

# Database — use external managed Postgres (RDS/Cloud SQL) in production
data:
  metadataConnection:
    user: airflow
    pass: ~  # set via secret
    host: your-rds-endpoint.us-east-1.rds.amazonaws.com
    port: 5432
    db: airflow_metadata

# Secrets backend
secret:
  - envName: "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    secretName: "airflow-metadata-db-secret"
    secretKey: "connection"

# Logging — remote logging to S3
config:
  core:
    remote_logging: "True"
  logging:
    remote_log_conn_id: "aws_default"
    remote_base_log_folder: "s3://your-bucket/airflow-logs"

# Pod template for KubernetesExecutor tasks
podTemplate: |
  apiVersion: v1
  kind: Pod
  metadata:
    name: placeholder
  spec:
    restartPolicy: Never
    serviceAccountName: airflow-worker
    containers:
      - name: base
        image: your-ecr-registry/airflow:2.9.2-custom-v1.3
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "2Gi"
            cpu: "1"

# Persistent volume for logs (fallback when S3 is unavailable)
logs:
  persistence:
    enabled: true
    size: 50Gi
    storageClassName: gp3
```

---

## Secrets Management: Two Approaches

### Approach 1: Kubernetes Secrets (Simple)

Kubernetes Secrets are base64-encoded and stored in etcd. They're acceptable for lower-sensitivity environments when etcd encryption at rest is enabled.

```bash
# Create a secret for Airflow's metadata DB connection
kubectl create secret generic airflow-metadata-db-secret \
  --from-literal=connection="postgresql+psycopg2://airflow:PASSWORD@rds-endpoint:5432/airflow" \
  -n airflow

# Create a secret for git credentials
kubectl create secret generic airflow-git-secret \
  --from-literal=gitSshKey="$(cat ~/.ssh/id_rsa)" \
  -n airflow
```

```yaml
# Reference in Airflow Helm values
env:
  - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    valueFrom:
      secretKeyRef:
        name: airflow-metadata-db-secret
        key: connection
```

### Approach 2: AWS Secrets Manager via External Secrets Operator (Production)

The External Secrets Operator (ESO) is the industry standard for production — it reads secrets from AWS Secrets Manager (or GCP Secret Manager, HashiCorp Vault) and syncs them into Kubernetes Secrets automatically, with rotation support.

```yaml
# Install ESO
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets -n external-secrets --create-namespace

# ClusterSecretStore — tells ESO how to authenticate to AWS
---
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secretsmanager
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
      auth:
        jwt:
          serviceAccountRef:
            name: external-secrets-sa
            namespace: external-secrets

# ExternalSecret — maps AWS Secrets Manager secret to K8s Secret
---
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: airflow-db-credentials
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secretsmanager
    kind: ClusterSecretStore
  target:
    name: airflow-metadata-db-secret  # K8s Secret name
  data:
    - secretKey: connection  # K8s Secret key
      remoteRef:
        key: prod/airflow/metadata-db  # AWS Secrets Manager path
        property: connection_string
```

**Why ESO over plain K8s Secrets:**
- Secrets live in AWS Secrets Manager (audit trail, rotation, fine-grained IAM)
- Rotation happens automatically — ESO re-syncs on the `refreshInterval`
- Engineers never see raw secret values; they just reference secret names in code
- Kubernetes Secrets become ephemeral — easily recreated from the source of truth

---

## CeleryExecutor: Flower UI and Worker Scaling

When using CeleryExecutor, Flower provides a web UI to monitor worker state, task queues, and throughput. In Kubernetes, it runs as a separate deployment:

```yaml
# Flower enabled in Helm values
flower:
  enabled: true
  resources:
    requests:
      memory: "128Mi"
      cpu: "50m"
    limits:
      memory: "256Mi"
      cpu: "200m"
```

```bash
# Access Flower UI via port-forward
kubectl port-forward svc/airflow-flower 5555:5555 -n airflow
# Open http://localhost:5555
```

### Horizontal Pod Autoscaling for Celery Workers

With CeleryExecutor, workers are long-running pods. HPA scales them based on CPU/memory or custom metrics:

```yaml
# HPA for Celery workers
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
  namespace: airflow
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: External
      external:
        metric:
          name: celery_queue_length  # custom metric from Celery/Redis
          selector:
            matchLabels:
              queue: default
        target:
          type: AverageValue
          averageValue: "10"  # scale up when more than 10 tasks per worker
```

**Note:** KubernetesExecutor doesn't need HPA — it's already inherently elastic (one pod per task).

---

## KubernetesPodOperator: Run Any Container

The `KubernetesPodOperator` allows you to run any Docker image as an Airflow task — not just the Airflow worker image. This is critical for running tasks with different dependencies (a dbt container, a Spark job, a custom ML inference image):

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Run dbt in its own container
dbt_run = KubernetesPodOperator(
    task_id="dbt_run_revenue_models",
    name="dbt-runner",
    namespace="airflow",
    image="your-registry/dbt-runner:1.8.0",
    cmds=["dbt", "run", "--select", "tag:revenue"],
    env_vars={
        "DBT_TARGET": "prod",
        "DBT_PROFILES_DIR": "/dbt",
    },
    secrets=[
        k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name="dbt-snowflake-creds",
                key="password"
            )
        )
    ],
    resources=k8s.V1ResourceRequirements(
        requests={"memory": "1Gi", "cpu": "500m"},
        limits={"memory": "2Gi", "cpu": "1"},
    ),
    is_delete_operator_pod=True,   # clean up pod after completion
    get_logs=True,                 # stream logs to Airflow task log
    dag=dag,
)
```

**KubernetesPodOperator vs executor_config:**
- `executor_config` overrides resources/image for tasks that still use the Airflow CLI entrypoint
- `KubernetesPodOperator` runs a completely arbitrary container with any command — use when the task has different Python dependencies or needs a totally different runtime

---

## Persistent Volumes for Logs

Airflow task logs must be accessible from the webserver for debugging. On Kubernetes, two patterns work:

### Pattern 1: Remote Logging (Best for Production)
Configure Airflow to write logs directly to S3 or GCS:
```yaml
# In Helm values
config:
  core:
    remote_logging: "True"
  logging:
    remote_log_conn_id: "aws_s3"
    remote_base_log_folder: "s3://your-data-platform-bucket/airflow-logs"
    encrypt_s3_logs: "True"
```

### Pattern 2: Shared PVC (Simpler, but requires ReadWriteMany)
A PVC with `ReadWriteMany` access mode allows both worker pods (writers) and the webserver (reader) to mount the same volume. This requires a storage class that supports RWX — on AWS, EFS (via the EFS CSI driver) works; standard EBS/gp3 does NOT support RWX.

```yaml
# EFS StorageClass for shared Airflow logs
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: fs-0123456789abcdef
  directoryPerms: "700"

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-logs-pvc
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-sc
  resources:
    requests:
      storage: 100Gi
```

---

## Deployment Patterns: EKS vs GKE vs AKS

| Concern | AWS EKS | GCP GKE | Azure AKS |
|---|---|---|---|
| Managed Postgres | RDS Aurora | Cloud SQL | Azure Database for PostgreSQL |
| Secret Management | AWS Secrets Manager | Secret Manager | Azure Key Vault |
| Remote Logging | S3 | GCS | Azure Blob Storage |
| Container Registry | ECR | Artifact Registry | ACR |
| Load Balancer | ALB Ingress Controller | GKE Ingress | AGIC |
| IAM for Pods | IRSA (Pod Identity) | Workload Identity | Azure Workload Identity |

The Airflow Helm chart is portable — the major differences are in the cloud-specific integrations (storage backend, secret store, IAM model).

---
