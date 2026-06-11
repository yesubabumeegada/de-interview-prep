---
title: "Helm and Operators - Senior Deep Dive"
topic: docker-and-kubernetes
subtopic: helm-and-operators
content_type: study_material
difficulty_level: senior
layer: senior_deep_dive
tags: [docker, kubernetes, helm-and-operators]
---

# Helm and Operators — Senior Deep Dive

## Strimzi Kafka Operator

```yaml
# Kafka cluster managed by Strimzi (operators handle scaling, upgrades, rebalancing)
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: data-platform-kafka
spec:
  kafka:
    replicas: 3
    version: 3.6.0
    storage:
      type: persistent-claim
      size: 500Gi
      class: gp3
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      default.replication.factor: 3
    resources:
      requests:
        memory: 8Gi
        cpu: "2"
      limits:
        memory: 16Gi
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 50Gi
```

```bash
# Strimzi handles: rolling upgrades, TLS cert rotation, topic management
kubectl apply -f kafka.yaml  # Strimzi operator creates/manages all K8s resources
```

## Helm Release Management at Scale

```bash
# Helmfile: manage multiple Helm releases declaratively
# helmfile.yaml
releases:
  - name: airflow
    namespace: airflow
    chart: apache-airflow/airflow
    version: 1.11.0
    values:
      - airflow-values.yaml
      - secrets.yaml

  - name: kafka
    namespace: kafka
    chart: strimzi/strimzi-kafka-operator
    version: 0.38.0

# Deploy all
helmfile sync

# Diff before deploy
helmfile diff
```

## ⚡ Cheat Sheet

```bash
# Helm commands
helm repo add <name> <url>
helm repo update
helm search repo airflow
helm install <release> <chart> [-f values.yaml] [-n namespace]
helm upgrade <release> <chart> [-f values.yaml]
helm list -A                     # all releases all namespaces
helm history <release>           # revision history
helm rollback <release> <rev>    # rollback to revision
helm uninstall <release>
helm template <release> <chart>  # render without installing
helm lint <chart-dir>            # validate chart

# Values override (precedence: --set > -f file > chart defaults)
helm install my-app ./chart -f prod.yaml --set image.tag=v2.0

# Inspect
helm show values apache-airflow/airflow  # default values
helm get values my-release               # deployed values
helm get manifest my-release             # rendered K8s manifests

# Operators
kubectl get sparkapplications -A
kubectl get kafkas -A
kubectl describe kafka my-kafka
```
