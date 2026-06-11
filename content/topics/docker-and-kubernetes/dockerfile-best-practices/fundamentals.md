---
title: "Dockerfile Best Practices - Fundamentals"
topic: docker-and-kubernetes
subtopic: dockerfile-best-practices
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [docker, dockerfile, best-practices, optimization]
---

# Dockerfile Best Practices — Fundamentals


## 🎯 Analogy

Think of a Dockerfile like a recipe: the order matters (Docker caches each step), smaller ingredients mean a smaller image, and you don't want to leave raw meat (credentials) on the counter.

---
## The Recipe Card Analogy

A Dockerfile is like a recipe card for building your application's environment. A badly written recipe says "add some flour" — vague and non-reproducible. A good recipe card specifies "200g bread flour (King Arthur, unbleached)" — exact and repeatable. Good Dockerfiles pin exact versions, order instructions to maximize caching, and include only what's needed for cooking (not every utensil in the kitchen). The recipe card should produce the exact same dish every time, anywhere.

---

## Layer Caching — The Most Important Concept

Docker builds images as a stack of layers. When a layer changes, all layers after it are rebuilt. **Order instructions from least to most frequently changing.**

```dockerfile
# ❌ BAD: code change invalidates pip install (slow!)
FROM python:3.11-slim
COPY . .                          # code changes often
RUN pip install -r requirements.txt  # this reruns every time code changes

# ✅ GOOD: pip install cached unless requirements.txt changes
FROM python:3.11-slim
COPY requirements.txt .           # changes rarely
RUN pip install --no-cache-dir -r requirements.txt  # cached ✓
COPY . .                          # code changes here — only copies, fast
```

---

## Pin Exact Versions

```dockerfile
# ❌ Unpinned — different builds may get different versions
FROM python:3
FROM python:3.11

# ✅ Pinned to exact digest — 100% reproducible
FROM python:3.11.4-slim-bookworm

# For base images in production, use digest:
FROM python:3.11.4-slim-bookworm@sha256:abc123...
```

---

## Use Slim/Minimal Base Images

```dockerfile
# Image sizes (approximate):
# python:3.11         → 1.0 GB   (full Debian + build tools)
# python:3.11-slim    → 130 MB   (Debian slim, no build tools)
# python:3.11-alpine  → 50 MB    (Alpine Linux — smaller, but glibc issues)

# Recommendation for DE: python:3.11-slim
# Alpine can cause issues with native extensions (numpy, pandas, psycopg2)

FROM python:3.11.4-slim-bookworm

# Install only what you need
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \       # needed for psycopg2
    curl \            # needed for health checks
    && rm -rf /var/lib/apt/lists/*   # clean apt cache in same layer!
```

---

## Non-Root User

```dockerfile
FROM python:3.11-slim

# Create non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Switch to non-root AFTER installing deps (which need root)
USER appuser

CMD ["python", "pipeline.py"]
```

Running as root means a container escape gives an attacker root on the host. Non-root is a baseline security control.

---

## WORKDIR Over cd

```dockerfile
# ❌ Don't use cd — fragile and confusing
RUN cd /app && pip install -r requirements.txt

# ✅ Use WORKDIR — sets context for all subsequent instructions
WORKDIR /app
RUN pip install -r requirements.txt
```

---

## One Process Per Container

```dockerfile
# ❌ Don't run multiple services in one container
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]

# ✅ One container, one process
# Run webserver and scheduler as separate containers (docker compose)
# webserver: CMD ["airflow", "webserver"]
# scheduler: CMD ["airflow", "scheduler"]
```

---

## Complete Best-Practice Dockerfile

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11.4-slim-bookworm

# System deps (rarely change — first, for caching)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /home/appuser/app

# Python deps (change occasionally)
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code (changes often — last for caching)
COPY --chown=appuser:appuser . .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import pipeline; pipeline.health_check()" || exit 1

USER appuser

CMD ["python", "pipeline.py"]
```

## ▶️ Try It Yourself

```dockerfile
# Multi-stage build: builder stage (large) → runtime stage (minimal)
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.11-slim AS runtime
WORKDIR /app
# Copy only installed packages from builder (no pip, no compilers)
COPY --from=builder /root/.local /root/.local
COPY src/ ./src/

# Security: run as non-root
RUN useradd -m appuser
USER appuser

# Cache-friendly: copy requirements before source code
# (requirements change rarely; source changes often)
ENV PATH=/root/.local/bin:$PATH
CMD ["python", "src/main.py"]
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
