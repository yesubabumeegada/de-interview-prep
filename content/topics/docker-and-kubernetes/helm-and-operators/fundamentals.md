---
title: "Helm and Operators - Fundamentals"
topic: docker-and-kubernetes
subtopic: helm-and-operators
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker, kubernetes, helm-and-operators]
---

# Helm and Operators — Fundamentals

## The Package Manager Analogy

Helm is Kubernetes's package manager — like pip for Python or apt for Linux. Instead of writing 500 lines of YAML to deploy Airflow (Deployment, Service, ConfigMap, RBAC, PVC, Ingress...), you run `helm install airflow apache-airflow/airflow -f my-values.yaml`. Helm bundles all the K8s resources into a "chart" and templates out the differences via values. Operators take this further: they encode operational knowledge (how to upgrade Kafka, how to take Spark snapshots) into a controller that runs in your cluster.

---

## Helm Basics

```bash
# Add chart repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install with default values
helm install airflow apache-airflow/airflow

# Install with custom values
helm install airflow apache-airflow/airflow   --namespace airflow   --create-namespace   -f custom-values.yaml

# Upgrade (rolling update)
helm upgrade airflow apache-airflow/airflow -f custom-values.yaml

# List releases
helm list --all-namespaces

# Rollback
helm rollback airflow 1    # rollback to revision 1

# Uninstall
helm uninstall airflow -n airflow
```

---

## Airflow Helm Values (Key Sections)

```yaml
# values.yaml
executor: KubernetesExecutor

webserver:
  replicas: 2
  resources:
    limits:
      memory: 2Gi
      cpu: 1

scheduler:
  replicas: 1
  resources:
    limits:
      memory: 4Gi

workers:
  replicas: 0  # KubernetesExecutor: no persistent workers

dags:
  gitSync:
    enabled: true
    repo: https://github.com/org/dags
    branch: main
    subPath: "dags/"

postgresql:
  enabled: false  # use external RDS

externalDatabase:
  type: postgres
  host: my-rds-instance.region.rds.amazonaws.com
  database: airflow
```

---

## Kubernetes Operators for DE

| Operator | Manages | Use Case |
|---|---|---|
| Spark Operator | SparkApplication | Spark jobs on K8s |
| Strimzi | Kafka cluster | Kafka on K8s |
| Flink Operator | FlinkDeployment | Flink streaming |
| PostgreSQL Operator (Zalando) | Postgres cluster | DB on K8s |
