---
title: "Helm and Operators - Real World"
topic: docker-and-kubernetes
subtopic: helm-and-operators
content_type: study_material
difficulty_level: senior
layer: real-world


tags: [docker, kubernetes, helm-and-operators]
---

# Helm and Operators — Real World

## Case Study: Helm-Managed Airflow Upgrade Without Downtime

### Background

A startup manually managed Airflow by applying YAML files with kubectl. Upgrading Airflow version meant: manually editing 15+ YAML files, testing in staging, applying one by one to production, hoping the ordering was right.

### The Helm Migration

```bash
# Migrated to official Airflow Helm chart
helm repo add apache-airflow https://airflow.apache.org

# All configuration in values.yaml (version controlled)
# Upgrade Airflow version:
# Old: manually edit 15 YAML files
# New: change one line in values.yaml
image:
  tag: 2.8.0   # changed from 2.7.3

helm upgrade airflow apache-airflow/airflow   -f values.yaml   --atomic \       # rollback if upgrade fails
  --timeout 10m
```

### Results

| Metric | Manual YAML | Helm |
|---|---|---|
| Time for Airflow version upgrade | 4-6 hours | 15 minutes |
| Rollback after bad upgrade | Manual + 2 hours | `helm rollback` in 2 minutes |
| New engineer onboarding | "Read 15 YAMLs" | "Edit values.yaml" |
| Config drift risk | High | None (all in values.yaml) |
