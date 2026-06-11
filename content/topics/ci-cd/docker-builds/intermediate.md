---
title: "Docker Builds - Intermediate"
topic: ci-cd
subtopic: docker-builds
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, docker, builds, caching, multi-platform]
---

# Docker Builds — Intermediate

## GitHub Actions Cache for Docker

```yaml
- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Build with GitHub Actions cache
  uses: docker/build-push-action@v6
  with:
    context: .
    push: false
    tags: my-pipeline:${{ github.sha }}
    # Cache layers in GitHub Actions cache storage
    cache-from: type=gha
    cache-to: type=gha,mode=max  # mode=max caches all layers, not just final

# Alternative: cache in registry
    cache-from: type=registry,ref=registry/my-pipeline:buildcache
    cache-to: type=registry,ref=registry/my-pipeline:buildcache,mode=max
```

---

## Multi-Platform Builds

```yaml
- name: Set up QEMU (for cross-compilation)
  uses: docker/setup-qemu-action@v3

- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Build and push multi-platform
  uses: docker/build-push-action@v6
  with:
    context: .
    platforms: linux/amd64,linux/arm64
    push: true
    tags: registry/my-pipeline:${{ github.sha }}
```

Build once for both x86 (most cloud VMs) and ARM64 (AWS Graviton, Apple Silicon) — one image tag works everywhere.

---

## Image Promotion Pattern

```yaml
# Don't rebuild for prod — promote the same image that passed staging tests
jobs:
  build:
    steps:
      - name: Build and push to dev registry
        run: |
          docker build -t dev-registry/pipeline:$GIT_SHA .
          docker push dev-registry/pipeline:$GIT_SHA

  deploy-staging:
    needs: build
    steps:
      - name: Deploy to staging (same image)
        run: kubectl set image deployment/pipeline container=dev-registry/pipeline:$GIT_SHA

  promote-to-prod:
    needs: [deploy-staging, staging-tests]
    steps:
      - name: Copy image to prod registry (promotion, no rebuild)
        run: |
          docker pull dev-registry/pipeline:$GIT_SHA
          docker tag dev-registry/pipeline:$GIT_SHA prod-registry/pipeline:$GIT_SHA
          docker push prod-registry/pipeline:$GIT_SHA
```

The same binary artifact (image) that was tested in staging is what runs in production — no "works in staging but not prod" scenarios.

---

## Build Matrix for Multiple Services

```yaml
jobs:
  build:
    strategy:
      matrix:
        service:
          - name: revenue-pipeline
            context: ./pipelines/revenue
          - name: customer-pipeline
            context: ./pipelines/customer
          - name: dbt-runner
            context: ./dbt
    steps:
      - uses: docker/build-push-action@v6
        with:
          context: ${{ matrix.service.context }}
          push: true
          tags: registry/${{ matrix.service.name }}:${{ github.sha }}
```

---

## Registry Cleanup

```yaml
# Scheduled job: delete images older than 30 days
on:
  schedule:
    - cron: "0 3 * * 0"  # Sundays at 3 AM

jobs:
  cleanup:
    runs-on: ubuntu-latest
    steps:
      - name: Delete old ECR images
        run: |
          aws ecr describe-images \
            --repository-name my-pipeline \
            --query 'imageDetails[?imagePushedAt<`'$(date -d '30 days ago' --utc +%Y-%m-%dT%H:%M:%SZ)'`].imageDigest' \
            --output text | \
          while read digest; do
            aws ecr batch-delete-image \
              --repository-name my-pipeline \
              --image-ids imageDigest=$digest
          done
```
