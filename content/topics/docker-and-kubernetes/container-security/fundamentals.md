---
title: "Container Security - Fundamentals"
topic: docker-and-kubernetes
subtopic: container-security
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker, kubernetes, container-security]
---

# Container Security — Fundamentals

## The Bank Vault Analogy

Container security is like bank vault design. A vault isn't just about the strong door — it's defense in depth: steel walls, time locks, alarm systems, limited access, video surveillance, and audit logs. Container security is the same: no root processes (minimize blast radius), no unnecessary tools in the image (reduce attack surface), network policies (isolate the vault), immutable filesystems (tamper detection), and image scanning (verify the steel before you build the vault).

---

## Security Baseline Checklist

```dockerfile
# ✅ Non-root user
RUN useradd -m -u 1000 appuser
USER appuser

# ✅ Read-only filesystem
# (set at runtime: --read-only with writable tmp)

# ✅ No unnecessary packages
FROM python:3.11-slim   # not full image
RUN apt-get install -y --no-install-recommends libpq-dev

# ✅ No secrets baked in
RUN --mount=type=secret,id=creds pip install ...
```

---

## Image Scanning

```bash
# Trivy: scan for CVEs
trivy image my-pipeline:v1.0.0

# Output:
# CRITICAL: 0
# HIGH: 2      ← investigate
# MEDIUM: 8
# LOW: 23

# Fail CI on CRITICAL or HIGH
trivy image --exit-code 1 --severity CRITICAL,HIGH my-pipeline:v1.0.0

# Scan before push (in GitHub Actions)
- uses: aquasecurity/trivy-action@master
  with:
    image-ref: my-pipeline:${{ github.sha }}
    exit-code: '1'
    severity: 'CRITICAL'
```

---

## Kubernetes Security Context

```yaml
spec:
  containers:
    - name: pipeline
      image: registry/pipeline:v1
      securityContext:
        runAsNonRoot: true          # must not run as root
        runAsUser: 1000             # specific UID
        runAsGroup: 1000
        readOnlyRootFilesystem: true  # immutable filesystem
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]             # drop all Linux capabilities
      volumeMounts:
        - name: tmp
          mountPath: /tmp           # writable tmp (for read-only FS)
  volumes:
    - name: tmp
      emptyDir: {}                  # in-memory writable volume
  securityContext:
    seccompProfile:
      type: RuntimeDefault          # restrict syscalls
```

---

## Network Policies

```yaml
# Deny all by default, allow only what's needed
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
spec:
  podSelector: {}   # applies to all pods in namespace
  policyTypes: [Ingress, Egress]
---
# Allow pipeline to reach S3 and Snowflake
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pipeline-egress
spec:
  podSelector:
    matchLabels:
      app: pipeline
  policyTypes: [Egress]
  egress:
    - ports:
        - port: 443   # HTTPS to S3/Snowflake
```
