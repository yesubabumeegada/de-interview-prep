---
title: "Python Packaging - Intermediate"
topic: python
subtopic: packaging
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, packaging, poetry, pyproject, docker, wheel]
---

# Python Packaging — Intermediate

## Poetry — Modern Dependency Management

Poetry handles dependency resolution, lock files, and packaging in one tool.

```bash
# Install poetry
curl -sSL https://install.python-poetry.org | python3 -

# Create a new project
poetry new etl-pipeline
# Creates: etl_pipeline/, tests/, pyproject.toml, README.md

# Or init in existing project
cd existing-project/
poetry init

# Add dependencies
poetry add pandas sqlalchemy boto3
poetry add pydantic --group dev   # Dev-only dependency

# Install from lock file (CI/CD)
poetry install --no-dev

# Run commands within the poetry venv
poetry run python src/pipeline.py
poetry run pytest

# Activate the shell
poetry shell
```

---

## pyproject.toml Structure

The modern standard for Python project configuration (replaces setup.py, setup.cfg).

```toml
[tool.poetry]
name = "etl-pipeline"
version = "1.2.0"
description = "Daily ETL pipeline for analytics warehouse"
authors = ["Data Team <data@company.com>"]
readme = "README.md"
packages = [{include = "etl_pipeline", from = "src"}]

[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.1"
sqlalchemy = "^2.0"
boto3 = "^1.34"
pydantic = "^2.5"
pyarrow = "^14.0"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4"
mypy = "^1.8"
ruff = "^0.1"
black = "^23.12"

[tool.poetry.scripts]
run-pipeline = "etl_pipeline.main:cli"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"

[tool.mypy]
strict = true

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
```

---

## Lock Files — Reproducibility

`poetry.lock` pins every dependency (including transitive) to exact versions.

```bash
# poetry.lock is auto-generated and should be committed to git
# It ensures everyone gets identical dependency versions

# Update lock file (resolve new versions)
poetry update

# Update a specific package
poetry update pandas

# Install exactly what's in the lock file (for CI)
poetry install --no-root
```

| File | Purpose | Commit to Git? |
|------|---------|---------------|
| `pyproject.toml` | Direct dependencies + version ranges | Yes |
| `poetry.lock` | All deps pinned to exact versions | Yes (apps), No (libraries) |
| `requirements.txt` | Flat list of pinned versions | Yes (if not using Poetry) |

---

## Building Distributable Packages

### Wheel (Binary Distribution)

```bash
# Build a wheel (fast to install, no compilation needed)
poetry build
# Creates:
#   dist/etl_pipeline-1.2.0-py3-none-any.whl
#   dist/etl_pipeline-1.2.0.tar.gz

# Install a wheel directly
pip install dist/etl_pipeline-1.2.0-py3-none-any.whl

# Or with standard build tools
pip install build
python -m build
```

### Publishing to PyPI (or Private Registry)

```bash
# Configure private registry
poetry config repositories.internal https://pypi.internal.company.com/simple/
poetry config http-basic.internal $USERNAME $PASSWORD

# Publish
poetry publish --repository internal

# Or build and upload separately
poetry build
twine upload --repository-url https://pypi.internal.company.com/ dist/*
```

---

## Docker for Python (Multi-Stage Builds)

### Basic Dockerfile (Not Optimized)

```dockerfile
# BAD: Large image, slow builds, includes dev tools
FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "src/pipeline.py"]
# Image size: ~1.2 GB
```

### Optimized Multi-Stage Build

```dockerfile
# Stage 1: Build dependencies (includes compilers, dev tools)
FROM python:3.11-slim AS builder

WORKDIR /app
RUN pip install poetry==1.7.1

# Copy only dependency files first (cache layer)
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --without-hashes > requirements.txt
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# Stage 2: Runtime (minimal image)
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy only installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ ./src/

# Non-root user for security
RUN useradd -m appuser
USER appuser

# Health check
HEALTHCHECK --interval=30s CMD python -c "import src; print('ok')"

CMD ["python", "src/pipeline.py"]
# Image size: ~250 MB (vs 1.2 GB)
```

### Docker Build Caching Strategy

```dockerfile
# Layer order matters for cache efficiency:
# 1. System deps (rarely change)
# 2. Python deps (change occasionally)
# 3. Application code (changes frequently)

FROM python:3.11-slim

# System packages first (cached unless Dockerfile changes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Dependencies next (cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code last (rebuilt every change)
COPY src/ ./src/

CMD ["python", "src/pipeline.py"]
```

---

## Private Registries

### AWS CodeArtifact

```bash
# Login to CodeArtifact
aws codeartifact login --tool pip --domain mycompany --repository internal

# This configures pip to pull from CodeArtifact
# Publish packages:
aws codeartifact login --tool twine --domain mycompany --repository internal
twine upload --repository codeartifact dist/*

# In poetry:
poetry config repositories.codeartifact https://mycompany-123456789.d.codeartifact.us-east-1.amazonaws.com/pypi/internal/simple/
```

### JFrog Artifactory

```bash
# pip.conf (or pip.ini on Windows)
[global]
index-url = https://user:token@company.jfrog.io/artifactory/api/pypi/python-local/simple
extra-index-url = https://pypi.org/simple/

# Poetry configuration
poetry config repositories.artifactory https://company.jfrog.io/artifactory/api/pypi/python-local/
poetry config http-basic.artifactory $USER $TOKEN
```

---

## Comparison: Dependency Management Tools

| Feature | pip + requirements.txt | Poetry | pip-tools |
|---------|----------------------|--------|-----------|
| Dependency resolution | Basic | Advanced (SAT solver) | Good |
| Lock file | Manual (pip freeze) | Automatic (poetry.lock) | requirements.txt |
| Virtual env management | External (venv) | Built-in | External |
| Build & publish | External (build, twine) | Built-in | External |
| Groups (dev, test) | Separate files | Built-in groups | Separate .in files |
| Speed | Fast | Slower resolution | Fast |
| Learning curve | Low | Medium | Low |

---

## Common Patterns

### Separate Base and Service Requirements

```
# requirements/
#   base.txt        — shared across all services
#   pipeline.txt    — specific to ETL pipeline
#   api.txt         — specific to FastAPI service

# base.txt
pandas>=2.0,<3.0
boto3>=1.28
pydantic>=2.0

# pipeline.txt
-r base.txt
apache-airflow>=2.7
pyspark>=3.5

# api.txt
-r base.txt
fastapi>=0.104
uvicorn>=0.24
```

---

## Interview Tips

> **Tip 1:** "Why use Poetry over pip + requirements.txt?" — "Poetry provides deterministic dependency resolution (SAT solver), automatic lock files for reproducibility, and built-in virtual environment management. The lock file guarantees everyone gets the same versions — `pip freeze` only captures what's currently installed, not the resolution logic. Poetry also handles development dependencies separately without multiple requirements files."

> **Tip 2:** "How do you optimize Docker images for Python?" — "Multi-stage builds: use a builder stage with full tooling to install dependencies, then copy only the installed packages to a slim runtime stage. Put `COPY requirements.txt` before `COPY src/` so dependency installation is cached when only code changes. Use `python:3.11-slim` (not full `python:3.11`) as base. Remove pip cache (`--no-cache-dir`). This typically reduces image size from 1+ GB to 200-300 MB."

> **Tip 3:** "When would you use a private PyPI registry?" — "When you have internal shared libraries (common ETL utilities, data models, company SDKs) used across teams. Publishing to a private registry (CodeArtifact, Artifactory) lets teams `pip install company-utils` like any public package. It also provides dependency caching (faster CI builds), vulnerability scanning, and license compliance checking."
