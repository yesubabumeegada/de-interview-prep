---
title: "Container Security - Intermediate"
topic: docker-and-kubernetes
subtopic: container-security
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [docker, kubernetes, container-security]
---

# Container Security — Intermediate

## Secrets Management — No Environment Variables

```yaml
# ❌ Env var — visible in docker inspect and pod describe
env:
  - name: DB_PASSWORD
    value: "supersecret"

# ✅ K8s Secret mounted as file
volumeMounts:
  - name: db-creds
    mountPath: /var/secrets
    readOnly: true
volumes:
  - name: db-creds
    secret:
      secretName: pipeline-secrets
```

```python
# Read from file (not env var)
with open("/var/secrets/db_password") as f:
    DB_PASSWORD = f.read().strip()
```

## Pod Security Standards

```yaml
# Namespace-level enforcement (K8s 1.25+)
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform
  labels:
    pod-security.kubernetes.io/enforce: restricted    # no privileged, no root
    pod-security.kubernetes.io/warn: restricted       # warn on violations
    pod-security.kubernetes.io/audit: restricted
```

## RBAC Audit

```bash
# Check what a service account can do
kubectl auth can-i --list --as=system:serviceaccount:data-platform:pipeline-sa

# Check specific action
kubectl auth can-i create pods --as=system:serviceaccount:data-platform:pipeline-sa

# View all role bindings
kubectl get rolebindings -n data-platform -o wide

# Audit: find service accounts with cluster-admin (should be 0)
kubectl get clusterrolebindings -o json |   jq '.items[] | select(.roleRef.name=="cluster-admin") | .subjects[]'
```

## Image Pull Policy

```yaml
# Always pull — verify against registry on every pod start
# (catches if tag was overwritten with different image)
imagePullPolicy: Always

# For immutable tags (sha or specific version) — IfNotPresent is fine
image: registry/pipeline:sha256:abc123...
imagePullPolicy: IfNotPresent
```
