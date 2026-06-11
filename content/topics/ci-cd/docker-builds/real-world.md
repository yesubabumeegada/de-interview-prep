---
title: "Docker Builds - Real World"
topic: ci-cd
subtopic: docker-builds
content_type: real_world_example
tags: [ci-cd, docker, builds, real-world, ecr]
---

# Docker Builds — Real World

## Case Study: Image Promotion Saves a Production Deploy

### Background

A SaaS analytics company built Docker images for their DE pipelines in CI. Their original process: rebuild the Docker image when deploying to production. This seemed fine until a critical incident revealed the flaw.

### The Incident

- PR merged Monday 9 AM. CI built and tested image `abc1234`.
- Tuesday 3 PM: the engineer deployed to production.
- CI rebuilt the image on the production deploy job (same Dockerfile, same code).
- The new build pulled `FROM python:3.11-slim` — which had been updated overnight to a new patch that introduced a breaking change in a dependency.
- The "tested" image (`abc1234`) was different from the deployed image (new build).
- Production broke 20 minutes after deploy.

**Same code, different image, different behavior.** The rebuild made them non-identical.

### The Fix: Image Promotion

```yaml
# BEFORE: Two separate build jobs (bad)
pr-build:
  - docker build -t pipeline:pr-sha .
  - docker push pipeline:pr-sha

production-deploy:
  - docker build -t pipeline:latest .   # ← different image!
  - kubectl apply -f k8s/

# AFTER: Build once, promote the tested artifact
build:
  steps:
    - docker build -t pipeline:$GIT_SHA .
    - docker push registry/pipeline:$GIT_SHA
    # Tag: registry/pipeline:abc1234def5

deploy-staging:
  needs: build
  steps:
    - kubectl set image deployment/pipeline container=registry/pipeline:$GIT_SHA
    # Now staging runs exactly registry/pipeline:abc1234def5

deploy-production:
  needs: [smoke-tests]
  steps:
    # Promote: copy, don't rebuild
    - docker pull registry/pipeline:$GIT_SHA
    - docker tag registry/pipeline:$GIT_SHA prod-registry/pipeline:$GIT_SHA
    - docker push prod-registry/pipeline:$GIT_SHA
    - kubectl set image deployment/pipeline container=prod-registry/pipeline:$GIT_SHA
    # Production runs the SAME binary that passed staging
```

### Results

| Before | After |
|---|---|
| Production image = different from tested image | Production image = exact tested image |
| Rebuild on every deploy (5-6 min) | Promote in <30 seconds |
| "Same code, different behavior" possible | Impossible by construction |
| Image tag = `:latest` (ambiguous) | Image tag = git SHA (traceable) |

**Key principle:** Build once, test once, deploy many times. The artifact that passed your tests is the artifact that runs in production — never rebuild.
