---
title: "Dockerfile Best Practices - Senior Deep Dive"
topic: docker-and-kubernetes
subtopic: dockerfile-best-practices
content_type: study_material
difficulty_level: senior
layer: senior_deep_dive
tags: [docker, dockerfile, sbom, provenance, supply-chain, distroless]
---

# Dockerfile Best Practices — Senior Deep Dive

## Supply Chain Security (SBOM + Provenance)

```bash
# Generate Software Bill of Materials (SBOM)
docker sbom my-pipeline:v1.0.0 --format spdx-json > sbom.json

# Or with syft:
syft my-pipeline:v1.0.0 -o spdx-json > sbom.json

# Provenance: prove where the image came from
# Build with provenance attestation:
docker buildx build \
  --attest type=provenance,mode=max \
  --attest type=sbom \
  --push \
  -t registry/my-pipeline:v1.0.0 .

# Verify provenance:
cosign verify-attestation \
  --type slsaprovenance \
  registry/my-pipeline:v1.0.0
```

---

## Distroless for Production

```dockerfile
# Full multi-stage: compile → runtime in distroless
FROM python:3.11 AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM gcr.io/distroless/python3-debian12:nonroot
# No shell, no apt, no curl — minimal attack surface
COPY --from=builder /install /usr/local
COPY --from=builder /app /app
WORKDIR /app
COPY pipeline/ pipeline/
# nonroot tag runs as uid 65532 automatically
CMD ["pipeline/main.py"]
```

**Distroless advantages:**
- No shell → no RCE via shell injection
- No package manager → no dependency confusion
- 15-20x fewer CVEs than full Debian
- ~20 MB image size

---

## Reproducible Builds

```dockerfile
# Pin everything — base image, apt packages, pip packages
FROM python:3.11.4-slim-bookworm@sha256:3d3763537a840eb9432f87ba98d8ccd75ff3be8a0498a2437ca52afe4fe25279

# Pin apt packages (exact version)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev=15.4-0+deb12u1 \
    && rm -rf /var/lib/apt/lists/*
```

```txt
# requirements.txt — pin everything including transitive deps
# Generate with: pip-compile requirements.in
pandas==2.1.1
numpy==1.26.0
sqlalchemy==2.0.21
psycopg2-binary==2.9.9
# ... all transitive deps pinned
```

```bash
# pip-compile workflow
pip install pip-tools
pip-compile requirements.in         # generates pinned requirements.txt
pip-compile requirements-dev.in     # generates pinned requirements-dev.txt
pip-sync requirements.txt           # installs exactly what's pinned
```

---

## Base Image Update Strategy

```yaml
# Renovate bot config for automatic base image updates
# .github/renovate.json
{
  "extends": ["config:base"],
  "docker": {
    "enabled": true,
    "pinDigests": true
  },
  "packageRules": [{
    "matchDatasources": ["docker"],
    "matchPackageNames": ["python"],
    "schedule": ["every weekend"],
    "automerge": false,
    "reviewers": ["@data-platform-team"]
  }]
}
```

Renovate opens automatic PRs when base images have security patches, with your CI running to verify nothing broke.

---

## ⚡ Cheat Sheet

```dockerfile
# Template: production-grade Dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11.4-slim-bookworm@sha256:<pinned-digest> AS builder

# Build-time secrets (not baked in)
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=secret,id=pip_conf \
    pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11.4-slim-bookworm@sha256:<pinned-digest> AS runtime

# Non-root user
RUN useradd -m -u 1000 appuser

# Minimal system deps
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev=15.* && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local
WORKDIR /home/appuser/app
COPY --chown=appuser:appuser . .

USER appuser
ENV PATH=/home/appuser/.local/bin:$PATH

HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import pipeline" || exit 1

CMD ["python", "pipeline.py"]
```

```bash
# Build with all security features
DOCKER_BUILDKIT=1 docker build \
  --secret id=pip_conf,src=pip.conf \
  --label org.opencontainers.image.revision=$(git rev-parse HEAD) \
  --no-cache \
  -t registry/pipeline:$(git rev-parse --short HEAD) .

# Lint
hadolint Dockerfile

# Scan
trivy image registry/pipeline:latest

# Generate SBOM
syft registry/pipeline:latest -o spdx-json > sbom.json

# Sign
cosign sign --yes registry/pipeline:v1.0.0
```
