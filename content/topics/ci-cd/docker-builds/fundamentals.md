---
title: "Docker Builds - Fundamentals"
topic: ci-cd
subtopic: docker-builds
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, docker, builds, ci, automation]
---

# Docker Builds — Fundamentals

## Automated Builds: The Stamp Press Analogy

Manual Docker builds are like hand-stamping each coin. Automated builds in CI are like a coin press: you set up the die once (Dockerfile), feed in raw materials (code), and it produces identical coins (images) every time, stamped with the batch number (git SHA). Every PR automatically produces a testable image. Every merge to main produces a deployable image tagged with the commit that created it.

---

## Building in GitHub Actions

```yaml
# .github/workflows/build.yml
name: Build Docker Image

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Build image
        uses: docker/build-push-action@v6
        with:
          context: .
          push: false           # don't push on PRs
          tags: my-pipeline:${{ github.sha }}
          cache-from: type=gha  # GitHub Actions cache
          cache-to: type=gha,mode=max
```

---

## Tagging Strategy

```bash
# Never use :latest in production — it's ambiguous
docker tag my-pipeline:build my-pipeline:latest  # ❌

# ✅ Tag by git SHA (immutable — always know exactly what's running)
docker tag my-pipeline:build my-pipeline:$GIT_SHA

# ✅ Also add semantic version for releases
docker tag my-pipeline:$GIT_SHA my-pipeline:v2.1.0

# In GitHub Actions:
IMAGE_TAG=${{ github.sha }}        # full SHA
SHORT_TAG=${GITHUB_SHA::8}         # first 8 chars
```

---

## Push to Registry

```yaml
# Push to Amazon ECR
- name: Configure AWS credentials
  uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: arn:aws:iam::123456:role/GitHubActions
    aws-region: us-east-1

- name: Login to ECR
  id: login-ecr
  uses: aws-actions/amazon-ecr-login@v2

- name: Build and push to ECR
  uses: docker/build-push-action@v6
  with:
    context: .
    push: true
    tags: |
      ${{ steps.login-ecr.outputs.registry }}/my-pipeline:${{ github.sha }}
      ${{ steps.login-ecr.outputs.registry }}/my-pipeline:latest
```

---

## Scan Before Push

```yaml
- name: Build image
  run: docker build -t my-pipeline:${{ github.sha }} .

- name: Scan for vulnerabilities
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: my-pipeline:${{ github.sha }}
    format: 'sarif'
    output: 'trivy-results.sarif'
    exit-code: '1'           # fail CI on CRITICAL CVEs
    severity: 'CRITICAL'

- name: Push (only if scan passes)
  run: docker push my-pipeline:${{ github.sha }}
```

---

## Build Arguments for CI

```yaml
- name: Build with CI metadata
  run: |
    docker build \
      --build-arg GIT_SHA=${{ github.sha }} \
      --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
      --build-arg VERSION=${{ github.ref_name }} \
      -t my-pipeline:${{ github.sha }} .
```

```dockerfile
# In Dockerfile:
ARG GIT_SHA
ARG BUILD_DATE
ARG VERSION

LABEL git_sha=$GIT_SHA
LABEL build_date=$BUILD_DATE
LABEL version=$VERSION
```
