---
title: "Python Packaging - Scenario Questions"
topic: python
subtopic: packaging
content_type: scenario_question
tags: [python, packaging, interview, scenarios]
---

# Scenario Questions — Python Packaging

<article data-difficulty="junior">

## 🟢 Junior: Set Up a Virtualenv and Install Packages

**Scenario:** You just cloned a data pipeline repository. The README says "Python 3.11 required." Set up a development environment from scratch and install the project's dependencies from `requirements.txt`. Also add `pytest` for running tests.

**Given requirements.txt:**
```
pandas==2.1.4
sqlalchemy==2.0.23
boto3==1.34.0
pyarrow==14.0.0
```

<details>
<summary>💡 Hint</summary>

Steps: verify Python version, create venv, activate it, install requirements, add pytest, verify everything works.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Verify Python version
python --version
# If not 3.11, use pyenv:
# pyenv install 3.11.7
# pyenv local 3.11.7

# Step 2: Create virtual environment
python -m venv .venv

# Step 3: Activate it
# Linux/Mac:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1

# Step 4: Upgrade pip (optional but recommended)
pip install --upgrade pip

# Step 5: Install project dependencies
pip install -r requirements.txt

# Step 6: Add dev dependency
pip install pytest pytest-cov

# Step 7: Verify installation
python -c "import pandas; print(f'pandas {pandas.__version__}')"
python -c "import sqlalchemy; print(f'sqlalchemy {sqlalchemy.__version__}')"
pip check  # Verify no conflicts

# Step 8: Save dev requirements
pip freeze > requirements-dev.txt
# Or maintain manually:
cat > requirements-dev.txt << 'EOF'
-r requirements.txt
pytest==7.4.3
pytest-cov==4.1.0
EOF

# Step 9: Run tests to verify
pytest tests/ -v

# Step 10: Confirm venv isolation
which python   # Should show .venv/bin/python
which pip      # Should show .venv/bin/pip
```

```python
# Quick verification script: verify_env.py
import sys

def verify_environment():
    """Verify the development environment is correctly set up."""
    # Check Python version
    assert sys.version_info >= (3, 11), f"Need Python 3.11+, got {sys.version}"
    
    # Check we're in a venv
    assert hasattr(sys, 'prefix') and sys.prefix != sys.base_prefix, \
        "Not in a virtual environment! Activate .venv first."
    
    # Check required packages
    required = ['pandas', 'sqlalchemy', 'boto3', 'pyarrow', 'pytest']
    for package in required:
        __import__(package)
        print(f"  {package} OK")
    
    print("\nEnvironment verified successfully!")

if __name__ == '__main__':
    verify_environment()
```

**Common mistakes to avoid:**
- Forgetting to activate the venv before installing
- Using `sudo pip install` (never needed with venv)
- Committing the `.venv/` folder to git (add to .gitignore)
- Not running `pip check` after installation

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Resolve a Dependency Conflict

**Scenario:** Your ETL pipeline uses two libraries:
- `data-validator==3.2.0` which requires `pydantic>=1.10,<2.0`
- `api-client==2.1.0` which requires `pydantic>=2.0`

When you run `pip install`, you get:
```
ERROR: Cannot install data-validator==3.2.0 and api-client==2.1.0 because these package versions have conflicting dependencies.
```

Resolve this conflict so your pipeline can use both libraries.

<details>
<summary>💡 Hint</summary>

Options: (1) Find newer versions with compatible ranges, (2) Use Pydantic v1 compatibility layer, (3) Vendor one dependency, (4) Fork and patch. Consider what's sustainable long-term.

</details>

<details>
<summary>✅ Solution</summary>

```bash
# Step 1: Understand the conflict
pip install data-validator==3.2.0 api-client==2.1.0 --dry-run
# Shows: pydantic<2.0 vs pydantic>=2.0 — mutually exclusive

# Step 2: Check if newer versions resolve it
pip index versions data-validator
pip index versions api-client

# Check if data-validator has a pydantic v2 compatible release
pip install "data-validator>=3.2" --dry-run
# Maybe data-validator 3.3.0 supports pydantic v2?
```

```python
# Step 3: If no compatible versions exist, use Pydantic's v1 compatibility

# Pydantic v2 ships a v1 compatibility module:
# Instead of: from pydantic import BaseModel (v1)
# Use:        from pydantic.v1 import BaseModel

# Check if data-validator uses this pattern:
# pip show data-validator → look at source code

# If data-validator is a thin wrapper, patch at import time:
# conftest.py or early in your app:
import sys
import pydantic.v1
sys.modules['pydantic'] = pydantic.v1  # Redirect old imports
# WARNING: This is a hack — only for bridging period
```

```python
# Step 4: Better solution — isolate the conflicting dependency

# Option A: Use subprocess isolation for the validator
import subprocess
import json

def validate_with_isolated_process(data: dict) -> dict:
    """Run data-validator in a separate venv to avoid pydantic conflict."""
    result = subprocess.run(
        ['/opt/validator-env/bin/python', '-m', 'data_validator', '--json'],
        input=json.dumps(data),
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

# Option B: Replace data-validator with inline validation (Pydantic v2)
from pydantic import BaseModel, field_validator
from typing import Any

class RecordValidator(BaseModel):
    """Replace data-validator with native Pydantic v2 validation."""
    user_id: int
    email: str
    amount: float
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        if '@' not in v or '.' not in v.split('@')[1]:
            raise ValueError('Invalid email')
        return v.lower()
    
    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v < 0:
            raise ValueError('Amount must be non-negative')
        return round(v, 2)
```

```toml
# Step 5: Long-term solution with Poetry (proper resolution)
# pyproject.toml
[tool.poetry.dependencies]
python = "^3.11"
# Pin to compatible versions
api-client = "^2.1"
pydantic = "^2.5"

# Fork data-validator with pydantic v2 support
data-validator = {git = "https://github.com/our-org/data-validator.git", branch = "pydantic-v2"}

# Or vendor it
# data-validator = {path = "./vendor/data-validator"}
```

**Resolution strategy (order of preference):**

| Option | Effort | Sustainability |
|--------|--------|---------------|
| Update both to compatible versions | Low | Best |
| Use Pydantic v1 compat shim | Low | Temporary |
| Replace smaller library with inline code | Medium | Good |
| Fork and patch the library | High | Good (if maintained) |
| Subprocess isolation | Medium | Last resort |

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Dependency Management for a 20-Person Data Team

**Scenario:** You're leading a data platform team with:
- 20 engineers across 4 squads
- 30+ ETL pipelines (Spark, Airflow, Lambda)
- 5 shared internal libraries
- Deployments to EMR, Lambda, Glue, and ECS
- Current pain points: "works on my machine" issues, dependency conflicts between teams, slow CI builds, no visibility into security vulnerabilities

Design a comprehensive dependency management strategy covering: repo structure, tooling, versioning, CI/CD, security, and developer experience.

<details>
<summary>💡 Hint</summary>

Consider: mono-repo vs multi-repo, lockfile strategy, shared base images, internal package registry, automated security scanning, and developer onboarding experience.

</details>

<details>
<summary>✅ Solution</summary>

```
# === ARCHITECTURE DECISION: HYBRID APPROACH ===
#
# Mono-repo for shared libraries + platform tooling
# Multi-repo for pipeline code (team autonomy)
# Private PyPI registry for distribution

# Repository Structure:
#
# data-platform/ (mono-repo — shared code)
# ├── libraries/
# │   ├── data-core/          # Logging, config, connectors
# │   ├── dq-framework/       # Data quality checks
# │   ├── etl-base/           # Base classes for ETL
# │   ├── schema-registry/    # Shared Pydantic models
# │   └── test-utils/         # Test fixtures, mocks
# ├── docker/
# │   ├── base-python/        # Base image all teams use
# │   ├── spark-runtime/      # Spark + common deps
# │   └── lambda-base/        # Lambda base + layers
# ├── templates/
# │   ├── new-pipeline/       # Cookiecutter template
# │   └── new-library/
# └── .github/workflows/
#
# team-alpha-pipelines/ (multi-repo)
# team-beta-pipelines/ (multi-repo)
```

```toml
# libraries/data-core/pyproject.toml
[tool.poetry]
name = "data-core"
version = "4.2.1"  # Semantic versioning
description = "Core utilities for data platform"

[tool.poetry.dependencies]
python = ">=3.10,<3.13"
structlog = ">=23.0"
pydantic = ">=2.0,<3.0"
boto3 = ">=1.28"

# Extras for optional heavy dependencies
[tool.poetry.extras]
spark = ["pyspark>=3.4"]
redshift = ["sqlalchemy>=2.0", "psycopg2-binary"]
all = ["pyspark", "sqlalchemy", "psycopg2-binary"]
```

```yaml
# === CI/CD: Library Publishing Pipeline ===
# .github/workflows/library-release.yml
name: Library Release
on:
  push:
    tags: ['*-v*']  # e.g., data-core-v4.2.1

jobs:
  determine-package:
    runs-on: ubuntu-latest
    outputs:
      package: ${{ steps.parse.outputs.package }}
      version: ${{ steps.parse.outputs.version }}
    steps:
      - id: parse
        run: |
          TAG="${{ github.ref_name }}"
          echo "package=${TAG%-v*}" >> $GITHUB_OUTPUT
          echo "version=${TAG#*-v}" >> $GITHUB_OUTPUT

  test:
    needs: determine-package
    strategy:
      matrix:
        python: ['3.10', '3.11', '3.12']
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: |
          cd libraries/${{ needs.determine-package.outputs.package }}
          poetry install
          poetry run pytest --cov
          poetry run mypy src/

  security:
    needs: determine-package
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pip-audit safety
      - run: |
          cd libraries/${{ needs.determine-package.outputs.package }}
          poetry export -f requirements.txt | pip-audit -r /dev/stdin --strict

  publish:
    needs: [test, security]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: |
          cd libraries/${{ needs.determine-package.outputs.package }}
          poetry build
          poetry publish --repository codeartifact
```

```dockerfile
# === BASE IMAGES: Consistency Across Teams ===
# docker/base-python/Dockerfile
FROM python:3.11-slim AS platform-base

# System deps shared by all services
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Pre-install platform libraries (cached layer)
RUN pip install --no-cache-dir \
    data-core==4.2.1 \
    dq-framework==2.0.3 \
    etl-base==3.1.0 \
    --index-url https://company.d.codeartifact.us-east-1.amazonaws.com/pypi/internal/simple/ \
    --extra-index-url https://pypi.org/simple/

# Security: non-root by default
RUN useradd -r -s /bin/false appuser
USER appuser

LABEL org.opencontainers.image.source="https://github.com/company/data-platform"
```

```python
# === DEVELOPER EXPERIENCE: Project Template ===
# templates/new-pipeline/{{cookiecutter.project_name}}/pyproject.toml

"""
Cookiecutter template for new pipelines.
Usage: cookiecutter https://github.com/company/data-platform//templates/new-pipeline
"""

# Generated pyproject.toml
[tool.poetry]
name = "{{ cookiecutter.project_name }}"
version = "0.1.0"
description = "{{ cookiecutter.description }}"
authors = ["{{ cookiecutter.team }} <{{ cookiecutter.team_email }}>"]

[tool.poetry.dependencies]
python = "^3.11"
data-core = "^4.2"
dq-framework = "^2.0"

[[tool.poetry.source]]
name = "internal"
url = "https://company.d.codeartifact.us-east-1.amazonaws.com/pypi/internal/simple/"
priority = "supplemental"
```

```yaml
# === SECURITY: Automated Scanning ===
# Runs weekly + on every PR that changes dependencies
name: Dependency Security Scan
on:
  schedule:
    - cron: '0 8 * * 1'  # Monday 8am
  pull_request:
    paths: ['**/pyproject.toml', '**/poetry.lock', '**/requirements*.txt']

jobs:
  scan-all-repos:
    strategy:
      matrix:
        repo: [data-platform, team-alpha-pipelines, team-beta-pipelines]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          repository: company/${{ matrix.repo }}
      - run: |
          pip install pip-audit
          find . -name "poetry.lock" -exec sh -c '
            dir=$(dirname {}); cd $dir;
            echo "Scanning: $dir";
            poetry export -f requirements.txt 2>/dev/null | pip-audit -r /dev/stdin
          ' \;
```

**Summary of key decisions:**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Repo structure | Hybrid (shared mono + team multi) | Balance consistency with autonomy |
| Dep management | Poetry + lock files | Reproducibility, proper resolution |
| Distribution | CodeArtifact (private PyPI) | Standard pip workflow for consumers |
| Base images | Shared Docker images with platform libs | Consistency, faster builds |
| Security | pip-audit in CI + weekly scans | Catch CVEs before and after deploy |
| Versioning | Semantic versioning for libraries | Clear compatibility communication |
| Templates | Cookiecutter for new projects | Fast onboarding, consistent structure |
| Python versions | Support 3.10-3.12 in libs | Balance compatibility with features |

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Python package vs. a module?**
A: A module is a single `.py` file. A package is a directory containing an `__init__.py` file (which may be empty) and one or more modules or sub-packages. The `__init__.py` marks the directory as a Python package and controls what is exposed when users `import package_name`.

**Q: What is `pyproject.toml` and why is it replacing `setup.py`?**
A: `pyproject.toml` is the modern, PEP 517/518-compliant project configuration file that declares build system requirements and project metadata. It replaces `setup.py` + `setup.cfg` with a single, declarative, tool-agnostic format supported by pip, Poetry, Hatch, and flit. It separates build-time dependencies from runtime dependencies cleanly.

**Q: What is the difference between `dependencies` and `optional-dependencies` in `pyproject.toml`?**
A: `dependencies` are required for the package to function—always installed. `optional-dependencies` (extras) are installed only when explicitly requested with `pip install mypackage[dev]`. Use extras for development tools (`pytest`, `black`), documentation (`sphinx`), and optional integrations (`mypackage[spark]` for PySpark-specific features).

**Q: What is a virtual environment and why is it essential for Python development?**
A: A virtual environment is an isolated Python installation with its own site-packages directory. It prevents dependency conflicts between projects (Project A needs `pandas==1.5`, Project B needs `pandas==2.0`) and ensures reproducibility. `python -m venv .venv` or `conda create` are the standard creation methods.

**Q: What is the difference between `pip install -r requirements.txt` and a lock file?**
A: `requirements.txt` specifies constraints (e.g., `pandas>=1.5`), which may install different versions on different machines as new releases appear. A lock file (`pip-compile` → `requirements.lock`, or Poetry's `poetry.lock`) pins exact versions of every dependency and transitive dependency, guaranteeing identical environments across machines and CI runs.

**Q: What is `__all__` in a Python module and what does it control?**
A: `__all__` is a list of names that should be exported when `from module import *` is used. It also serves as an explicit public API declaration—documentation tools and IDEs use it to determine the module's public interface. Without `__all__`, `import *` exports all names not starting with underscore.

**Q: How do you structure a data engineering Python package for a DE team?**
A: Common layout: `src/` layout (PEP 517 recommended) with `src/mypackage/` containing `extractors/`, `transformers/`, `loaders/`, `utils/`, `config/`, and `models/`. Tests in `tests/` mirroring the source tree. `pyproject.toml` at root. Separate extras for `[dev]` (pytest, black, mypy) and `[spark]` (pyspark). This structure prevents test code from being importable in production.

**Q: What is semantic versioning and how does it apply to DE packages?**
A: Semantic versioning uses `MAJOR.MINOR.PATCH`. PATCH for backward-compatible bug fixes, MINOR for backward-compatible new features, MAJOR for breaking API changes. For internal DE packages shared across pipelines, strict semver prevents unintended breaking changes from propagating—pipelines pin to `>=1.2,<2.0` to get bug fixes without breaking changes.

---

## 💼 Interview Tips

- Know `pyproject.toml` deeply—setup.py is legacy. If you still use setup.py, explain why (legacy constraint) and show you know the modern alternative.
- Lock files are a production requirement, not optional. Describe a real incident or scenario where lack of a lock file caused a CI/prod environment divergence to show the stakes.
- Senior interviewers ask about internal package distribution for DE teams: PyPI private registries (AWS CodeArtifact, JFrog Artifactory, Nexus) for sharing common DE utilities across teams. Walk through the publish workflow.
- `src/` layout prevents the classic "import from the project root instead of the installed package" bug—explain why it forces correct install-then-import workflows and catches packaging errors early.
- Connect packaging to DE platform work: a shared `de-common` package with reusable extractors, retry utilities, and schema validation is a senior DE contribution. Demonstrate you think about packaging as a platform capability, not just an artifact.
