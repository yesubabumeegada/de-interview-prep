---
title: "Kubernetes Basics - Intermediate"
topic: docker-and-kubernetes
subtopic: kubernetes-basics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [kubernetes, k8s, hpa, rbac, persistent-volumes]
---

# Kubernetes Basics — Intermediate

## Horizontal Pod Autoscaler

```yaml
# hpa.yaml — scale pipeline workers based on CPU
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: pipeline-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: pipeline-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
```

---

## Persistent Volumes for Data

```yaml
# PersistentVolumeClaim — request storage
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: pipeline-scratch-storage
spec:
  accessModes:
    - ReadWriteOnce      # one pod reads/writes
  storageClassName: gp3  # AWS EBS gp3
  resources:
    requests:
      storage: 100Gi

---
# Use in pod
spec:
  containers:
    - name: pipeline
      volumeMounts:
        - name: scratch
          mountPath: /tmp/pipeline-data
  volumes:
    - name: scratch
      persistentVolumeClaim:
        claimName: pipeline-scratch-storage
```

---

## RBAC for Data Platform

```yaml
# ServiceAccount for pipeline pods
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pipeline-sa
  namespace: data-platform

---
# Role: what the service account can do
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pipeline-role
  namespace: data-platform
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "delete"]

---
# Bind role to service account
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: pipeline-role-binding
  namespace: data-platform
subjects:
  - kind: ServiceAccount
    name: pipeline-sa
roleRef:
  kind: Role
  name: pipeline-role
  apiGroup: rbac.authorization.k8s.io
```

---

## Jobs and CronJobs for Batch Processing

```yaml
# Job: run pipeline to completion
apiVersion: batch/v1
kind: Job
metadata:
  name: daily-revenue-job
spec:
  backoffLimit: 3         # retry 3 times on failure
  activeDeadlineSeconds: 3600  # fail if not done in 1 hour
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: pipeline
          image: registry/revenue-pipeline:abc1234
          command: ["python", "daily_revenue.py"]
          resources:
            requests:
              memory: "2Gi"
              cpu: "500m"

---
# CronJob: scheduled batch
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-revenue
spec:
  schedule: "0 2 * * *"    # 2 AM daily
  concurrencyPolicy: Forbid  # don't run if previous still running
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: pipeline
              image: registry/revenue-pipeline:abc1234
```

---

## Network Policies

```yaml
# Restrict pod-to-pod communication
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pipeline-network-policy
spec:
  podSelector:
    matchLabels:
      app: revenue-pipeline
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: airflow-scheduler  # only Airflow can call pipeline
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: postgres           # pipeline can reach postgres
    - ports:
        - port: 443                   # HTTPS to external APIs
```
