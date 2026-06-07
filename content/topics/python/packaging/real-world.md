---
title: "Python Packaging - Real-World Production Examples"
topic: python
subtopic: packaging
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, packaging, production, cicd, docker]
---

# Python Packaging — Real-World Production Examples

## Pattern 1: Poetry + Docker for ETL Service

A complete setup for a containerized ETL service with proper dependency management.

```toml
# pyproject.toml
[tool.poetry]
name = "analytics-etl"
version = "2.1.0"
description = "Daily analytics ETL pipeline"
authors = ["Data Team <data@company.com>"]
packages = [{include = "analytics_etl", from = "src"}]

[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.1"
sqlalchemy = "^2.0"
boto3 = "^1.34"
pydantic = "^2.5"
pyarrow = "^14.0"
structlog = "^23.2"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4"
pytest-cov = "^4.1"
mypy = "^1.8"
ruff = "^0.1"

[tool.poetry.scripts]
run-etl = "analytics_etl.main:main"
healthcheck = "analytics_etl.health:check"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

```dockerfile
# Dockerfile — production-grade multi-stage build
# Stage 1: Export dependencies
FROM python:3.11-slim AS deps

RUN pip install poetry==1.7.1
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --without-hashes --without dev > requirements.txt

# Stage 2: Build wheels
FROM python:3.11-slim AS builder

WORKDIR /app
COPY --from=deps /app/requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /wheels -r requirements.txt

# Stage 3: Runtime
FROM python:3.11-slim AS runtime

WORKDIR /app

# Install pre-built wheels (fast, no compilers needed)
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

# Copy application
COPY src/ ./src/

# Security: non-root user
RUN useradd -r -s /bin/false etluser
USER etluser

# Metadata
LABEL version="2.1.0"
LABEL maintainer="data-team"

ENTRYPOINT ["python", "-m", "analytics_etl.main"]
```

```yaml
# docker-compose.yml for local development
services:
  etl:
    build: .
    environment:
      - AWS_PROFILE=dev
      - ENV=development
      - LOG_LEVEL=DEBUG
    volumes:
      - ./src:/app/src  # Hot reload in dev
      - ~/.aws:/home/etluser/.aws:ro
    command: ["--pipeline", "daily_users", "--date", "2024-01-15"]
```

---

## Pattern 2: Shared Internal Library (Internal PyPI)

Building and publishing a shared library used across multiple data pipelines.

```
# Project structure
company-data-utils/
├── src/
│   └── company_data_utils/
│       ├── __init__.py
│       ├── connectors/
│       │   ├── __init__.py
│       │   ├── redshift.py
│       │   └── s3.py
│       ├── models/
│       │   ├── __init__.py
│       │   └── schemas.py
│       ├── quality/
│       │   ├── __init__.py
│       │   └── validators.py
│       └── logging.py
├── tests/
├── pyproject.toml
└── CHANGELOG.md
```

```toml
# pyproject.toml for the shared library
[tool.poetry]
name = "company-data-utils"
version = "3.4.1"
description = "Shared data engineering utilities"
authors = ["Data Platform Team <platform@company.com>"]
packages = [{include = "company_data_utils", from = "src"}]

[tool.poetry.dependencies]
python = ">=3.10,<3.13"
pandas = ">=2.0,<3.0"
boto3 = ">=1.28"
pydantic = ">=2.0,<3.0"
structlog = ">=23.0"

[tool.poetry.extras]
redshift = ["sqlalchemy", "psycopg2-binary"]
spark = ["pyspark"]
all = ["sqlalchemy", "psycopg2-binary", "pyspark"]
```

```python
# src/company_data_utils/__init__.py
"""Company Data Utils — shared data engineering library.

Usage:
    pip install company-data-utils
    pip install company-data-utils[redshift]  # with Redshift support
    pip install company-data-utils[all]       # everything
"""
from company_data_utils.logging import configure_logging
from company_data_utils.models.schemas import EventSchema, UserSchema

__version__ = "3.4.1"
__all__ = ["configure_logging", "EventSchema", "UserSchema"]
```

```yaml
# .github/workflows/publish.yml
name: Publish Library
on:
  push:
    tags: ['v*']

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install poetry
      - run: poetry build
      - name: Publish to CodeArtifact
        env:
          CODEARTIFACT_TOKEN: ${{ secrets.CODEARTIFACT_TOKEN }}
        run: |
          poetry config repositories.internal ${{ vars.PYPI_URL }}
          poetry config http-basic.internal aws $CODEARTIFACT_TOKEN
          poetry publish --repository internal
```

**Consuming the library in other projects:**
```toml
# Other project's pyproject.toml
[tool.poetry.dependencies]
company-data-utils = {version = "^3.4", source = "internal"}

[[tool.poetry.source]]
name = "internal"
url = "https://company-123456.d.codeartifact.us-east-1.amazonaws.com/pypi/internal/simple/"
priority = "supplemental"
```

---

## Pattern 3: Spark Job Dependency Packaging

```bash
#!/bin/bash
# build_spark_deps.sh — Build dependencies for EMR/Dataproc submission

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"
DEPS_DIR="${BUILD_DIR}/deps"

# Clean
rm -rf "${BUILD_DIR}"
mkdir -p "${DEPS_DIR}"

# Install dependencies to a folder
pip install \
    -r requirements-spark.txt \
    --target "${DEPS_DIR}" \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.11 \
    --no-deps

# Remove unnecessary files to reduce zip size
find "${DEPS_DIR}" -name "*.pyc" -delete
find "${DEPS_DIR}" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "${DEPS_DIR}" -name "*.dist-info" -type d -exec rm -rf {} + 2>/dev/null || true

# Create deps zip
cd "${DEPS_DIR}"
zip -r "${BUILD_DIR}/dependencies.zip" .

# Create source zip
cd "${PROJECT_DIR}/src"
zip -r "${BUILD_DIR}/pipeline.zip" .

# Upload to S3
aws s3 cp "${BUILD_DIR}/dependencies.zip" s3://spark-artifacts/jobs/daily_etl/deps/
aws s3 cp "${BUILD_DIR}/pipeline.zip" s3://spark-artifacts/jobs/daily_etl/src/

echo "Artifacts uploaded. Submit with:"
echo "spark-submit --py-files s3://spark-artifacts/jobs/daily_etl/deps/dependencies.zip,s3://spark-artifacts/jobs/daily_etl/src/pipeline.zip main.py"
```

```python
# src/main.py — Spark job entry point
import sys
import os

# Add zipped dependencies to path (needed for --py-files)
for zip_path in sys.path:
    if zip_path.endswith('.zip'):
        sys.path.insert(0, zip_path)

from pyspark.sql import SparkSession
from pipeline.etl import run_daily_etl

if __name__ == "__main__":
    spark = SparkSession.builder.appName("daily_etl").getOrCreate()
    run_daily_etl(spark, date=sys.argv[1])
```

---

## Pattern 4: Lambda Deployment with Layers

```yaml
# serverless.yml (Serverless Framework)
service: data-processor

provider:
  name: aws
  runtime: python3.11
  region: us-east-1
  architecture: x86_64

layers:
  PandasLayer:
    path: layers/pandas
    compatibleRuntimes:
      - python3.11
    description: "pandas + pyarrow for data processing"

functions:
  process-events:
    handler: src/handlers/events.handler
    layers:
      - {Ref: PandasLayerLambdaLayer}
    timeout: 300
    memorySize: 1024
    environment:
      POWERTOOLS_SERVICE_NAME: data-processor
```

```bash
#!/bin/bash
# build_lambda_layer.sh
set -euo pipefail

LAYER_DIR="layers/pandas/python/lib/python3.11/site-packages"
mkdir -p "${LAYER_DIR}"

# Install with platform targeting (Lambda runs Amazon Linux)
pip install \
    pandas==2.1.4 \
    pyarrow==14.0.0 \
    --target "${LAYER_DIR}" \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.11

# Remove tests and docs to save space
find "${LAYER_DIR}" -name "tests" -type d -exec rm -rf {} + 2>/dev/null || true
find "${LAYER_DIR}" -name "*.pyi" -delete
find "${LAYER_DIR}" -name "*.dist-info" -type d -exec rm -rf {} + 2>/dev/null || true

# Check size (Lambda layer limit: 250 MB unzipped)
du -sh layers/pandas/
# Should be under 250 MB
```

---

## CI/CD Pipeline for Library Publishing

```yaml
# .github/workflows/library-ci.yml
name: Library CI/CD
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.10', '3.11', '3.12']
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: pip install poetry
      - run: poetry install
      - run: poetry run pytest --cov=src --cov-report=xml
      - run: poetry run mypy src/
      - run: poetry run ruff check src/

  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pip-audit
      - run: pip-audit --strict -r <(poetry export -f requirements.txt)

  publish:
    needs: [test, security]
    if: github.ref == 'refs/heads/main' && contains(github.event.head_commit.message, 'release:')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install poetry
      - name: Extract version and publish
        run: |
          VERSION=$(poetry version -s)
          poetry build
          poetry publish --repository internal
      - name: Create git tag
        run: |
          VERSION=$(poetry version -s)
          git tag "v${VERSION}"
          git push origin "v${VERSION}"
```

---

## Decision Matrix: When to Use What

| Scenario | Tool | Why |
|----------|------|-----|
| Simple script | pip + requirements.txt | Low overhead |
| Team project | Poetry + pyproject.toml | Reproducibility, lock file |
| Shared library | Poetry + private PyPI | Versioned distribution |
| Spark on YARN | zip + --py-files | Cluster doesn't have pip |
| Spark on K8s | Docker image | Full control |
| Lambda function | Layers or Docker | Size constraints |
| Glue job | --additional-python-modules | Simplest for Glue |

---

## Interview Tips

> **Tip 1:** "How would you set up a shared library for your data team?" — "Poetry for dependency management with pyproject.toml. Publish to a private registry (CodeArtifact/Artifactory). Use extras for optional heavy dependencies (`pip install our-lib[spark]`). Semantic versioning with a CHANGELOG. CI pipeline that tests on multiple Python versions, runs security scanning, and auto-publishes on tagged releases."

> **Tip 2:** "How do you deploy Python to Lambda with large dependencies like pandas?" — "Lambda layers or container images. For layers: install packages targeting `manylinux2014_x86_64` with `--only-binary=:all:`, strip test files and dist-info to reduce size. If over 250MB unzipped, switch to Lambda container images (10GB limit). Share layers across functions to avoid redundant storage."

> **Tip 3:** "Walk me through your CI/CD for a Python data pipeline." — "Triggered on PR: lint (ruff), type check (mypy), unit tests (pytest), security scan (pip-audit). On merge to main: build Docker image, push to ECR, deploy to staging. On tag: promote staging image to production. Key details: multi-stage Docker builds for small images, hash-pinned dependencies for supply chain security, separate dev/prod dependency groups."
