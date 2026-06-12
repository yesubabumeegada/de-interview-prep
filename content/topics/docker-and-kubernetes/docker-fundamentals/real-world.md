---
title: "Docker Fundamentals - Real World"
topic: docker-and-kubernetes
subtopic: docker-fundamentals
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [docker, containers, real-world, data-engineering, airflow]
---

# Docker Fundamentals — Real World

## Case Study: "Works on My Machine" → Containerized DE Platform

### Background

A 15-person DE team at a media company managed 50+ Python pipelines. Each engineer had a slightly different local setup: different Python versions (3.8 to 3.11), different library versions, macOS vs Linux. The shared deployment environment ran Ubuntu 20.04.

### The Problem

**Month 1:** Engineer A pushes a pipeline that uses `pandas 2.0` features. It works locally. In production (pandas 1.5.3 on the shared VM), it silently produces wrong output — no error, just wrong numbers.

**Month 3:** Two pipelines fail in production because engineer B's `requirements.txt` install overwrote a shared library version used by engineer A's pipeline.

**Month 5:** Onboarding a new engineer takes 3 days of environment setup.

---

### The Docker Solution

**Step 1: Base image strategy**
```dockerfile
# base/Dockerfile — shared base for all DE pipelines
FROM python:3.11.4-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 deuser
USER deuser
WORKDIR /home/deuser/app

# Pinned base dependencies all pipelines share
COPY base-requirements.txt .
RUN pip install --user --no-cache-dir -r base-requirements.txt
```

**Step 2: Per-pipeline image**
```dockerfile
# pipelines/revenue/Dockerfile
FROM company-registry/de-base:2024.01

# Only pipeline-specific additions
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "revenue_pipeline.py"]
```

**Step 3: Local dev with docker compose**
```yaml
# docker-compose.dev.yml
services:
  pipeline-dev:
    build: .
    volumes:
      - .:/home/deuser/app  # live code reload
    environment:
      - ENV=development
      - DB_URL=${DB_URL}
    command: python -m pytest tests/ -v
```

---

### Results

| Metric | Before | After |
|---|---|---|
| "Works on my machine" incidents | 3-4/month | 0 |
| New engineer onboarding time | 3 days | 2 hours |
| Dependency conflict incidents | Monthly | None in 6 months |
| Build reproducibility | 0% | 100% |
| CI/CD deployment confidence | Low | High |

**The killer metric:** New engineers ran `docker compose up` and had a working dev environment in under 2 hours — vs 3 days of Slack messages and Stack Overflow.

### Lesson

Docker doesn't eliminate environment complexity — it encapsulates it. The goal is that your pipeline's environment is defined in a `Dockerfile` in git, reviewed like any other code, and identical from laptop to CI to production.
