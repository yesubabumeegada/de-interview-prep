---
title: "Deployment Strategies - Fundamentals"
topic: ci-cd
subtopic: deployment-strategies
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, deployment, blue-green, canary, rollback]
---

# Deployment Strategies — Fundamentals


## 🎯 Analogy

Think of deployment strategies like changing an airplane engine mid-flight: blue/green swaps to a fresh plane and transfers passengers over (zero downtime), canary sends 5% of passengers to the new plane first (low risk), rolling upgrades one seat at a time.

---
## The Restaurant Menu Change Analogy

A restaurant testing a new dish doesn't pull all the old menus and replace them overnight. Instead, they test the new dish at 10% of tables (canary), gather feedback, then roll it out fully. Deployment strategies for data pipelines follow the same logic: never flip a switch that could break everything at once. Test on a small slice, verify, then commit — with a rollback path.

---

## Rolling Deployment (Default)

```yaml
# Kubernetes rolling update
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
```

```bash
kubectl set image deployment/pipeline container=registry/pipeline:v2.0
kubectl rollout status deployment/pipeline
kubectl rollout undo deployment/pipeline  # rollback
```

---

## Blue-Green Deployment

```bash
# Two environments: blue (current), green (new)
# 1. Deploy to green, test
kubectl apply -f green-deployment.yaml

# 2. Switch traffic
kubectl patch service pipeline-svc -p '{"spec":{"selector":{"version":"green"}}}'

# 3. Instant rollback: switch selector back to blue
kubectl patch service pipeline-svc -p '{"spec":{"selector":{"version":"blue"}}}'
```

---

## Canary Deployment

```bash
# Route 10% of traffic to new version
kubectl scale deployment pipeline-canary --replicas=1   # 10%
kubectl scale deployment pipeline-stable --replicas=9  # 90%

# Monitor for errors
# If healthy: promote
kubectl scale deployment pipeline-canary --replicas=10
kubectl scale deployment pipeline-stable --replicas=0
```

---

## Feature Flags for Pipelines

```python
import os

ENABLE_REVENUE_V2 = os.getenv("FF_REVENUE_V2", "false") == "true"

def run_revenue_pipeline():
    if ENABLE_REVENUE_V2:
        return revenue_v2()
    return revenue_v1()
```

Enable in staging first, validate, then enable in production — zero code deploy for the switch.

---

## Key Rule: Every Deploy Needs a Rollback Plan

| Change Type | Rollback Method |
|---|---|
| K8s deployment | `kubectl rollout undo` |
| dbt model | `git revert` + redeploy |
| Schema migration | Backward-compatible migration |
| Feature logic | Feature flag off |

## ▶️ Try It Yourself

```yaml
# Kubernetes blue/green deployment
# 1. Deploy Green (new version) alongside Blue (current)
# 2. Switch Service selector to Green
# 3. Delete Blue after validation

# Step 1: Deploy green
kubectl apply -f deployment-green.yaml  # image: my-app:v2

# Step 2: Switch traffic instantly
kubectl patch service my-app -p '{"spec":{"selector":{"version":"green"}}}'

# Step 3: Verify, then delete blue
kubectl delete deployment my-app-blue

# Canary: route 10% of traffic to new version
# (Using Argo Rollouts or Istio VirtualService weight splitting)
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
