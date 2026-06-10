---
title: "Python Packaging - Senior Deep Dive"
topic: python
subtopic: packaging
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, packaging, monorepo, spark-dependencies, lambda-layers]
---

# Python Packaging — Senior Deep Dive

## Mono-Repo vs Multi-Repo for Data Teams

| Aspect | Mono-Repo | Multi-Repo |
|--------|-----------|------------|
| Atomic changes | Cross-package changes in one PR | Coordinated releases needed |
| Dependency management | Shared versions, single lock file | Each repo pins independently |
| CI/CD complexity | Smart builds (only changed packages) | Simpler per-repo pipelines |
| Code discovery | Easy to find/share code | Harder to discover internal libs |
| Team autonomy | Less (shared standards) | More (own tools, pace) |
| Versioning | Trunk-based, single version | Semantic versioning per package |

### Mono-Repo Structure for Data Engineering

```
data-platform/
├── pyproject.toml          # Root: workspace config
├── packages/
│   ├── core/               # Shared utilities
│   │   ├── pyproject.toml
│   │   └── src/core/
│   ├── etl-framework/      # ETL base classes
│   │   ├── pyproject.toml
│   │   └── src/etl_framework/
│   ├── dq-validator/       # Data quality
│   │   ├── pyproject.toml
│   │   └── src/dq_validator/
│   └── models/             # Shared data models
│       ├── pyproject.toml
│       └── src/models/
├── pipelines/
│   ├── daily-users/
│   ├── hourly-events/
│   └── weekly-reports/
└── services/
    ├── api-gateway/
    └── scheduler/
```

```toml
# Root pyproject.toml (workspace mode)
[tool.poetry]
name = "data-platform"
version = "0.1.0"
packages = []

[tool.poetry.dependencies]
python = "^3.11"

# Local path dependencies
core = {path = "packages/core", develop = true}
etl-framework = {path = "packages/etl-framework", develop = true}
```

---

## Dependency Vendoring for Spark

Spark jobs run on clusters where you can't `pip install`. You must ship dependencies with the job.

### Method 1: --py-files (ZIP Archive)

```bash
# Create a zip of your dependencies
pip install -t ./deps pandas pyarrow
cd deps && zip -r ../dependencies.zip . && cd ..

# Submit with --py-files
spark-submit \
    --py-files dependencies.zip,my_pipeline.zip \
    --conf spark.executors.extraPythonPath=dependencies.zip \
    main.py
```

### Method 2: Conda Pack (Full Environment)

```bash
# Create a conda environment
conda create -n spark_env python=3.11 pandas pyarrow pydantic
conda activate spark_env

# Pack the entire environment
conda pack -n spark_env -o spark_env.tar.gz

# Use in spark-submit
spark-submit \
    --archives spark_env.tar.gz#environment \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    main.py
```

### Method 3: Custom Docker Image (Kubernetes)

```dockerfile
# Spark on K8s: package deps in the Docker image
FROM apache/spark-py:3.5.0

USER root
COPY requirements.txt /opt/spark/work-dir/
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

COPY src/ /opt/spark/work-dir/src/
USER spark
```

```bash
# Submit to Kubernetes
spark-submit \
    --master k8s://https://k8s-api:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=myregistry/spark-job:v1.2 \
    local:///opt/spark/work-dir/src/main.py
```

---

## Lambda Layers for Python Packages

AWS Lambda has a 250MB deployment limit. Layers let you share dependencies across functions.

```bash
# Build a Lambda layer
mkdir -p python/lib/python3.11/site-packages
pip install \
    -t python/lib/python3.11/site-packages \
    pandas==2.1.4 \
    pyarrow==14.0.0 \
    --platform manylinux2014_x86_64 \
    --only-binary=:all:

# Package the layer
zip -r pandas-layer.zip python/

# Deploy the layer
aws lambda publish-layer-version \
    --layer-name pandas-pyarrow \
    --zip-file fileb://pandas-layer.zip \
    --compatible-runtimes python3.11 \
    --compatible-architectures x86_64
```

```python
# Lambda function using the layer
# The layer is available at runtime — just import normally
import pandas as pd
import pyarrow.parquet as pq

def handler(event, context):
    df = pd.read_parquet(f"s3://{event['bucket']}/{event['key']}")
    return {"rows": len(df)}
```

**Layer size management:**

| Approach | Size | When to Use |
|----------|------|------------|
| Slim packages only | < 50 MB | Simple ETL (requests, boto3) |
| pandas + pyarrow | ~150 MB | Data processing Lambdas |
| Docker container image | < 10 GB | Large ML models, heavy deps |

---

## AWS Glue Job Dependencies

```bash
# Method 1: --additional-python-modules (Glue downloads from PyPI)
aws glue create-job \
    --name daily_etl \
    --role GlueServiceRole \
    --command '{"name":"glueetl","scriptLocation":"s3://scripts/etl.py"}' \
    --default-arguments '{
        "--additional-python-modules": "pydantic==2.5.2,structlog==23.2.0",
        "--python-modules-installer-option": "--upgrade"
    }'

# Method 2: --extra-py-files (pre-built zip on S3)
# Build locally and upload
pip install -t ./package pydantic structlog
cd package && zip -r ../my_libs.zip . && cd ..
aws s3 cp my_libs.zip s3://glue-libs/my_libs.zip

# Reference in Glue job
aws glue create-job \
    --default-arguments '{
        "--extra-py-files": "s3://glue-libs/my_libs.zip"
    }'

# Method 3: Wheel files on S3
pip wheel pydantic -w ./wheels
aws s3 sync ./wheels s3://glue-libs/wheels/
# Reference: --extra-py-files s3://glue-libs/wheels/pydantic-2.5.2-py3-none-any.whl
```

---

## Reproducible Builds with Hash Pinning

Ensure no package is tampered with between build and deploy.

```bash
# Generate requirements with hashes
pip-compile --generate-hashes requirements.in > requirements.txt

# Output includes SHA256 hashes:
# pandas==2.1.4 \
#     --hash=sha256:abc123... \
#     --hash=sha256:def456...

# Install with hash verification
pip install --require-hashes -r requirements.txt
# Fails if any package doesn't match its hash
```

```toml
# Poetry lock file includes hashes by default
# poetry.lock snippet:
[[package]]
name = "pandas"
version = "2.1.4"

[package.source]
type = "legacy"
url = "https://pypi.org/simple"

[package.files]
hash = "sha256:abc123..."
```

---

## Security Scanning

### pip-audit (Official PyPA Tool)

```bash
# Scan for known vulnerabilities
pip-audit

# Output:
# Name     Version  ID             Fix Versions
# -------- -------- -------------- ------------
# certifi  2023.7.22 GHSA-xqr8-7jwr-rhp7  2023.7.22.1
# pillow   9.5.0    CVE-2023-44271  10.0.1

# In CI:
pip-audit --strict --require-hashes -r requirements.txt
# Exit code 1 if vulnerabilities found
```

### Safety (Alternative Scanner)

```bash
# Scan against Safety DB
safety check -r requirements.txt

# Generate SBOM (Software Bill of Materials)
safety generate sbom --output sbom.json
```

### CI Integration

```yaml
# .github/workflows/security.yml
name: Dependency Security
on:
  pull_request:
    paths: ['**/requirements*.txt', '**/pyproject.toml', '**/poetry.lock']
  schedule:
    - cron: '0 6 * * 1'  # Weekly Monday scan

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install pip-audit
      - run: pip-audit --strict -r requirements.txt
```

---

## Comparison: Deployment Strategies

| Platform | Dependency Method | Key Constraint |
|----------|------------------|----------------|
| Lambda | Layers or container image | 250 MB (zip), 10 GB (container) |
| Glue | `--additional-python-modules` or S3 zip | Network access at startup |
| EMR/Spark | `--py-files`, conda-pack, Docker | All executors need packages |
| ECS/Fargate | Docker image | No size limit (practical: 5 GB) |
| Step Functions | Lambda layers or ECS tasks | Varies by compute type |

---

## Interview Tips

> **Tip 1:** "How do you manage dependencies for Spark jobs?" — "Three approaches depending on infrastructure: (1) `--py-files` with a zipped dependency folder — simple but brittle for packages with C extensions. (2) conda-pack — ships the entire Python environment, handles native libraries. (3) Custom Docker images for Spark on Kubernetes — most reproducible, best for complex dependency trees. I prefer Docker for production because it's fully deterministic."

> **Tip 2:** "Mono-repo or multi-repo for a data team?" — "Mono-repo for small-to-medium teams (5-20 engineers) sharing core libraries. Benefits: atomic cross-package changes, shared tooling, easier code discovery. Multi-repo when teams are autonomous and have different release cadences. The key consideration is CI/CD complexity — mono-repos need smart builds (only test changed packages), while multi-repos need coordination for shared library updates."

> **Tip 3:** "How do you handle security in Python dependencies?" — "Three layers: (1) `pip-audit` in CI to block PRs with known CVEs. (2) Hash pinning (`--require-hashes`) to prevent supply-chain attacks (tampered packages). (3) Private registry (CodeArtifact/Artifactory) as a proxy that caches and scans packages. Weekly scheduled scans catch newly-discovered vulnerabilities in existing dependencies."

## ⚡ Cheat Sheet

**Deployment Platform Constraints**
| Platform | Method | Size Limit |
|----------|--------|------------|
| Lambda (zip) | Layers | 250 MB unzipped |
| Lambda (container) | Docker image | 10 GB |
| Glue | `--additional-python-modules` or S3 zip | Network access at start |
| Spark on YARN | `--py-files` zip or conda-pack | All executors need packages |
| Spark on K8s | Docker image | Practical ~5 GB |
| ECS/Fargate | Docker image | No hard limit |

**Spark Dependency Methods (priority order)**
1. **Docker image** (K8s) — most reproducible; handles native/C extensions
2. **conda-pack** — ships entire Python env; handles native libs; YARN-friendly
3. **`--py-files`** zip — simple; breaks for packages with C extensions

**Lambda Layer Build Commands**
```bash
pip install -t python/lib/python3.11/site-packages \
    --platform manylinux2014_x86_64 --only-binary=:all: pandas pyarrow
zip -r layer.zip python/
```
- `--only-binary=:all:` ensures Linux-compatible wheels (not macOS .so files)
- pandas + pyarrow layer: ~150 MB; hits limit → use container image for heavy deps

**Reproducible Builds**
- `pip-compile --generate-hashes` → `requirements.txt` with SHA256 per package
- `pip install --require-hashes -r requirements.txt` → fails if tampered
- Poetry lock file includes hashes by default
- Hash pinning prevents supply-chain attacks (tampered PyPI packages)

**Security Scanning**
- `pip-audit` (PyPA official): scan for CVEs; `--strict` exits non-zero if found
- `safety check -r requirements.txt`: alternative; can generate SBOM
- Run in CI on: PR (block merge), weekly schedule (catch new CVEs in existing deps)
- Private registry (CodeArtifact/Artifactory): proxy + scan + cache = supply-chain control

**Mono-Repo Rules**
- Use for teams of 5–20 sharing core libraries; atomic cross-package PRs
- Need smart CI: only test packages affected by changed files
- Multi-repo when teams have autonomous release cadences and minimal shared code
