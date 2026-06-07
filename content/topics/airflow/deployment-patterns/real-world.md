---
title: "Airflow Deployment Patterns - Real-World Scenarios"
topic: airflow
subtopic: deployment-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, deployment, production, kubernetes, mwaa, helm, ci-cd]
---

# Airflow Deployment Patterns — Real-World Scenarios

## Scenario 1: Migrating from VM to Kubernetes

A data engineering team runs Airflow on a single EC2 instance with LocalExecutor. As pipelines grow to 200 DAGs and 50+ concurrent tasks, the instance hits resource limits. They migrate to Kubernetes.

**Migration plan:**

```bash
# Phase 1: Containerise (week 1-2)
# 1. Build custom Docker image with all dependencies
FROM apache/airflow:2.8.1
RUN pip install apache-airflow-providers-snowflake==4.4.2 \
    apache-airflow-providers-amazon==8.12.0

# 2. Test image locally
docker run -p 8080:8080 \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://... \
  -v ./dags:/opt/airflow/dags \
  my-airflow:2.8.1 webserver

# Phase 2: Deploy to k8s staging (week 2-3)
helm install airflow apache-airflow/airflow \
  --namespace airflow-staging \
  --values values-staging.yaml

# Phase 3: Validate (week 3-4)
# - Run parallel: old VM + new k8s (both pointed at different DBs)
# - Compare task outputs for same execution dates
# - Verify all connections work in new environment

# Phase 4: Cutover (day of)
# 1. Pause all DAGs on old instance
# 2. Wait for active runs to finish
# 3. Export connections and variables from old → new
airflow connections export - | airflow connections import -
airflow variables export - | airflow variables import -

# 4. Point DNS to new ingress
# 5. Unpause all DAGs on new instance
```

**Common pitfalls during migration:**

| Issue | Cause | Fix |
|-------|-------|-----|
| Tasks fail with "module not found" | Docker image missing dependencies | Update Dockerfile, rebuild image |
| Connections broken | Fernet key different between old and new | Export decrypted, re-import on new |
| DAGs missing | git-sync not configured correctly | Check git-sync container logs |
| Task logs not visible | Log volume not mounted | Configure remote logging (S3/GCS) |

---

## Scenario 2: Production AWS MWAA Setup with Terraform

A company standardises on MWAA (Managed Workflows for Apache Airflow) to reduce operational overhead.

```hcl
# terraform/airflow/main.tf

# S3 bucket for DAGs and plugins
resource "aws_s3_bucket" "airflow" {
  bucket = "company-airflow-${var.env}"
  
  versioning {
    enabled = true    # Required for MWAA; enables rollback
  }
}

# Upload DAGs to S3
resource "aws_s3_object" "dags" {
  for_each = fileset("${path.module}/../../dags", "*.py")
  bucket   = aws_s3_bucket.airflow.id
  key      = "dags/${each.value}"
  source   = "${path.module}/../../dags/${each.value}"
  etag     = filemd5("${path.module}/../../dags/${each.value}")
}

# MWAA Environment
resource "aws_mwaa_environment" "prod" {
  name               = "company-airflow-prod"
  airflow_version    = "2.8.1"
  environment_class  = "mw1.large"    # 16 vCPU, 64 GB RAM schedulers
  max_workers        = 25
  min_workers        = 1

  source_bucket_arn  = aws_s3_bucket.airflow.arn
  dag_s3_path        = "dags/"
  plugins_s3_path    = "plugins.zip"
  requirements_s3_path = "requirements.txt"

  execution_role_arn = aws_iam_role.mwaa_execution.arn

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids   # Private subnets only
  }

  airflow_configuration_options = {
    "core.parallelism"                   = "200"
    "core.max_active_tasks_per_dag"      = "50"
    "scheduler.min_file_process_interval" = "120"
    "scheduler.parsing_processes"         = "4"
    "webserver.warn_deployment_exposure"  = "False"
  }

  logging_configuration {
    dag_processing_logs { enabled = true; log_level = "WARNING" }
    scheduler_logs      { enabled = true; log_level = "INFO" }
    task_logs           { enabled = true; log_level = "INFO" }
    webserver_logs      { enabled = true; log_level = "ERROR" }
    worker_logs         { enabled = true; log_level = "INFO" }
  }
}

# IAM role for MWAA with access to Secrets Manager
resource "aws_iam_role_policy" "mwaa_secrets" {
  role = aws_iam_role.mwaa_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret",
      ]
      Resource = "arn:aws:secretsmanager:${var.region}:*:secret:airflow/*"
    }]
  })
}
```

```python
# requirements.txt — deployed to MWAA via S3
apache-airflow-providers-snowflake==4.4.2
apache-airflow-providers-amazon==8.12.0
apache-airflow-providers-slack==8.5.1
dbt-snowflake==1.7.4
```

```yaml
# GitHub Actions: sync DAGs to S3 on push to main
deploy-dags:
  if: github.ref == 'refs/heads/main'
  steps:
    - name: Sync DAGs to S3
      run: |
        aws s3 sync dags/ s3://company-airflow-prod/dags/ \
          --delete \
          --exclude "__pycache__/*" \
          --exclude "*.pyc"
    # MWAA picks up new files within ~1 minute
```

---

## Scenario 3: Self-Hosted Kubernetes with GitOps

A platform engineering team uses Argo CD for GitOps-driven Airflow deployment:

```
infra-repo/
├── airflow/
│   ├── Chart.yaml              # Helm chart reference
│   ├── values-staging.yaml
│   ├── values-production.yaml
│   └── templates/
│       ├── extra-secrets.yaml
│       └── network-policies.yaml
```

```yaml
# ArgoCD Application manifest
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow-production
  namespace: argocd
spec:
  project: data-platform
  source:
    repoURL: https://github.com/company/infra-repo
    targetRevision: main
    path: airflow
    helm:
      valueFiles:
        - values-production.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: airflow-production
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

**DAG repo separation:**

```yaml
# dags-repo: separate from infra-repo
# Changes to DAGs don't trigger infra sync
# git-sync sidecar handles DAG updates independently

dags:
  gitSync:
    enabled: true
    repo: https://github.com/company/airflow-dags
    branch: main
    period: 60s
    credentialsSecret: github-dags-secret
```

---

## Scenario 4: Disaster Recovery Playbook

```bash
#!/bin/bash
# airflow-dr.sh — Disaster Recovery Playbook

# SCENARIO: Metadata DB crashed and was restored from backup

# Step 1: Verify DB is accessible
psql $AIRFLOW_DB_URL -c "SELECT COUNT(*) FROM dag_run WHERE state='running'"

# Step 2: Reset zombie running tasks (tasks that were "running" when DB crashed)
airflow tasks clear \
  --dag-id all \
  --only-failed \
  --yes

# Manually update tasks stuck in 'running' state
psql $AIRFLOW_DB_URL << 'SQL'
UPDATE task_instance
SET state = 'failed', end_date = NOW()
WHERE state = 'running'
  AND last_heartbeat_at < NOW() - INTERVAL '10 minutes';
SQL

# Step 3: Re-serialize all DAGs (rebuilds serialized_dag table)
airflow dags reserialize

# Step 4: Verify scheduler health
airflow jobs check --job-type SchedulerJob --limit 1

# Step 5: Restart all Airflow components
kubectl rollout restart deployment -n airflow
kubectl rollout status deployment/airflow-scheduler -n airflow --timeout=120s

# Step 6: Verify DAGs are running
airflow dags list | grep -v paused
airflow dags list-runs --state running --limit 10
```

**RTO/RPO targets for Airflow:**

| Component | Strategy | RTO | RPO |
|-----------|---------|-----|-----|
| Metadata DB | Managed DB with PITR (AWS RDS) | < 30 min | < 5 min |
| Scheduler | Multiple replicas (HA) | Instant | N/A |
| DAG files | Git repository + git-sync | < 60 sec | 0 (git) |
| Connections/Variables | Secrets Manager | < 5 min | 0 |
| Task history | DB backup | < 30 min | < 5 min |
