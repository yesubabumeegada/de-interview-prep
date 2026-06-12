---
title: "Dockerfile Best Practices - Real World"
topic: docker-and-kubernetes
subtopic: dockerfile-best-practices
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [docker, dockerfile, real-world, optimization, security]
---

# Dockerfile Best Practices — Real World

## Case Study: From 4.8 GB to 280 MB in One Week

### Background

A fintech company's ML feature engineering pipeline had a Docker image that had grown organically over 18 months. The original engineer added "just in case" packages, left pip caches in layers, and used the full Python base image. By the time the team noticed, it was 4.8 GB and:

- CI pull time: 11 minutes per job
- K8s pod startup: 8 minutes
- ECR storage cost: $400/month for 150 image versions

### The Audit

```bash
# Step 1: Find biggest layers
docker history feature-pipeline:latest --no-trunc | head -20

# Findings:
# RUN pip install -r requirements.txt    → 2.1 GB
# FROM python:3.8                        → 920 MB
# RUN apt-get install ...               → 800 MB
# COPY . .                              → 900 MB (included data/ folder!)

# The .dockerignore was missing entirely
```

### The Fixes

**Fix 1: Add .dockerignore (immediate — 900 MB gone)**
```dockerignore
data/
models/weights/         # 400 MB of model artifacts
notebooks/
tests/
*.csv
*.parquet
__pycache__/
.git/
.venv/
```

**Fix 2: Switch to slim base image**
```dockerfile
# Before
FROM python:3.8          # 920 MB

# After  
FROM python:3.11-slim    # 128 MB
```

**Fix 3: Multi-stage build (remove build tools)**
```dockerfile
FROM python:3.11-slim AS builder
# Need gcc for numpy/scipy native extensions — compile here
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11-slim AS runtime
# gcc not needed at runtime — don't copy it
COPY --from=builder /root/.local /root/.local
COPY pipeline/ /app/pipeline/
ENV PATH=/root/.local/bin:$PATH
USER 1000:1000
CMD ["python", "-m", "pipeline.features"]
```

**Fix 4: Clean pip cache in same layer**
```dockerfile
# Before
RUN pip install -r requirements.txt     # cache stays in layer

# After
RUN pip install --no-cache-dir -r requirements.txt  # cache never written
```

### Results

| Layer | Before | After |
|---|---|---|
| Base image | 920 MB | 128 MB |
| Python packages | 2.1 GB | 580 MB |
| App code | 900 MB | 4 MB |
| System packages | 800 MB | 45 MB |
| **Total** | **4.8 GB** | **280 MB** |

| Metric | Before | After |
|---|---|---|
| CI pull time | 11 min | 1.2 min |
| K8s pod startup | 8 min | 55 sec |
| ECR storage cost | $400/mo | $23/mo |
| Build time (cache hit) | 8 min | 45 sec |

**Total engineering time invested:** 3 days. ROI: immediate.
