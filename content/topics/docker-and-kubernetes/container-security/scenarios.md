---
title: "Container Security - Scenarios"
topic: docker-and-kubernetes
subtopic: container-security
content_type: scenario_question


tags: [docker, kubernetes, container-security]
---

# Container Security — Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Secure a Dockerfile

**Scenario:** Review this Dockerfile and identify security issues:

```dockerfile
FROM python:3.11
COPY . /app
RUN pip install -r /app/requirements.txt
WORKDIR /app
ENV DB_PASSWORD=supersecret
CMD python pipeline.py
```

<details>
<summary>💡 Hint</summary>

Four issues: runs as root, full base image (unnecessary tools), secret hardcoded in ENV (visible in docker inspect and layer history), no security scanning.

</details>

<details>
<summary>✅ Solution</summary>

```dockerfile
FROM python:3.11-slim   # minimal base — fewer CVEs

# Non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /home/appuser/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

USER appuser

# ❌ NEVER: hardcoded secret in ENV
# ENV DB_PASSWORD=supersecret

# ✅ Read from environment at runtime (injected via K8s Secret)
CMD ["python", "pipeline.py"]
```

```yaml
# Pass secret via K8s Secret at runtime
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: pipeline-secrets
        key: db_password
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Container Escape Risk

**Scenario:** A security audit found your pipeline container runs as root and mounts the Docker socket (`/var/run/docker.sock`). Why is this critical and how do you fix it?

<details>
<summary>💡 Hint</summary>

Mounting the Docker socket gives the container full control over the host Docker daemon — effectively root on the host. Running as root means a container breakout gives root access. This combination is a complete host compromise. Fix: remove Docker socket mount (use kaniko or a build service instead), add non-root user, drop all capabilities.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# ❌ Current (critically insecure)
spec:
  containers:
    - name: pipeline
      securityContext:
        runAsUser: 0    # root
      volumeMounts:
        - name: docker-sock
          mountPath: /var/run/docker.sock
  volumes:
    - name: docker-sock
      hostPath:
        path: /var/run/docker.sock

# ✅ Fixed
spec:
  containers:
    - name: pipeline
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      # No Docker socket mount
```

If you need to build images in CI: use Kaniko, Buildah, or img — they build OCI images without a Docker daemon, so no socket mount needed.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Platform Security Policy

**Scenario:** Design a container security policy for a DE platform on Kubernetes that prevents: privileged containers, root users, images from unapproved registries, and containers without resource limits — enforced automatically without trusting teams to follow guidelines.

<details>
<summary>💡 Hint</summary>

Use Kyverno (or OPA Gatekeeper) for admission control — policies enforced at deploy time, not as suggestions. Combine with: Pod Security Standards at namespace level, image signing verification (cosign), and scheduled Trivy scans for drift detection.

</details>

<details>
<summary>✅ Solution</summary>

```yaml
# 1. Pod Security Standards (K8s built-in)
kubectl label namespace data-platform   pod-security.kubernetes.io/enforce=restricted

# 2. Kyverno policies
---
# Require approved registry
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: approved-registry
spec:
  validationFailureAction: Enforce
  rules:
    - name: check-registry
      validate:
        pattern:
          spec:
            containers:
              - image: "registry.company.com/*"

---
# Require resource limits
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: require-limits
spec:
  validationFailureAction: Enforce
  rules:
    - name: check-limits
      validate:
        message: "Containers must have memory and CPU limits"
        pattern:
          spec:
            containers:
              - resources:
                  limits:
                    memory: "?*"
                    cpu: "?*"

# 3. Image signing verification (cosign)
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: verify-signatures
spec:
  rules:
    - name: check-signature
      match:
        resources:
          kinds: [Pod]
      verifyImages:
        - image: "registry.company.com/*"
          attestors:
            - entries:
                - keyless:
                    issuer: "https://token.actions.githubusercontent.com"
                    subject: "https://github.com/org/repo/.github/workflows/build.yml@refs/heads/main"
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: Why is running a container as root dangerous?**
A: A container escape vulnerability (kernel bug or misconfiguration) gives the attacker root on the host. Non-root users limit the blast radius — an escaped non-root container can only do what that user can do on the host.

**Q: What is `readOnlyRootFilesystem` in a K8s security context and why is it used?**
A: It makes the container's filesystem immutable — nothing can be written. This prevents malware from modifying container files, stops attackers from installing tools after a breach, and makes the container behavior fully determined by the image.

**Q: What is image signing and what threat does it prevent?**
A: Image signing (cosign) creates a cryptographic signature proving an image was built by a specific CI pipeline. It prevents: using a tampered image (replaced in registry), pulling images not built by your approved pipeline, and supply chain attacks that substitute a legitimate image with a malicious one.

**Q: What is admission control in Kubernetes?**
A: Admission controllers intercept requests to the K8s API before resources are created. Kyverno and OPA Gatekeeper are policy engines that use admission webhooks to enforce rules: reject pods running as root, require resource limits, allow only approved registries.

**Q: What is the difference between image scanning and runtime security?**
A: Image scanning (Trivy) checks the image before deployment for known CVEs and misconfigurations. Runtime security (Falco) monitors running containers for suspicious behavior (unexpected file writes, shell spawning, network connections). Both are needed — scanning prevents known bad images; runtime catches post-compromise activity.

**Q: What syscall restrictions are available for containers in Kubernetes?**
A: Seccomp profiles restrict which Linux syscalls a container can make. The RuntimeDefault profile blocks rarely-used dangerous syscalls. You can create custom profiles to further restrict. Combined with capability drops (`capabilities: drop: [ALL]`), this significantly limits what a compromised container can do.

---

## 💼 Interview Tips

- Lead with defense in depth — the security story is scan → sign → admit control → runtime monitoring. Don't just mention one layer.
- Non-root containers and read-only filesystems are the baseline — know them by heart and describe them as the minimum, not advanced features.
- Image signing with cosign is the modern supply chain answer — it's increasingly asked about as software supply chain attacks become more common.
- Kyverno for admission control is the practical implementation detail for platform security — it shows you've moved beyond "set a policy in the docs" to "enforce it automatically."
- For the Docker socket mount question, know why it's dangerous (full Docker daemon access = effective host root) and the alternative (Kaniko for rootless builds).
- Avoid describing security as purely the image scanning step — interviewers testing security depth want to hear about the entire lifecycle: build, push, deploy, runtime.
