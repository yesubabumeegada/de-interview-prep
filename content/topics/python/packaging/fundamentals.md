---
title: "Python Packaging - Fundamentals"
topic: python
subtopic: packaging
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, packaging, virtualenv, pip, dependencies, venv]
---

# Python Packaging — Fundamentals

## Why Virtual Environments?

Without virtual environments, all Python projects share the same packages. This causes dependency conflicts.

```bash
# Problem: Project A needs pandas 1.5, Project B needs pandas 2.0
# Both can't exist in the same site-packages

# Solution: Each project gets its own isolated environment
project_a/venv/ → pandas==1.5.3
project_b/venv/ → pandas==2.1.0
```

| Without venv | With venv |
|-------------|-----------|
| All projects share packages | Each project has its own packages |
| Version conflicts between projects | No conflicts possible |
| Upgrading one project breaks another | Upgrades are isolated |
| Unclear what a project actually needs | requirements.txt lists exact deps |
| "Works on my machine" issues | Reproducible on any machine |

---

## Creating and Using Virtual Environments

### venv (Built-in, Python 3.3+)

```bash
# Create a virtual environment
python -m venv .venv

# Activate it
# Linux/Mac:
source .venv/bin/activate

# Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Windows (CMD):
.venv\Scripts\activate.bat

# Your prompt changes to show the active venv:
(.venv) $ python --version
(.venv) $ pip install pandas

# Deactivate when done
deactivate
```

### Where packages live

```bash
# System Python (DON'T install here)
/usr/lib/python3.11/site-packages/

# Virtual environment (INSTALL HERE)
.venv/lib/python3.11/site-packages/

# Check which python/pip you're using
which python    # Should show .venv/bin/python
which pip       # Should show .venv/bin/pip
```

---

## pip and requirements.txt

### Installing Packages

```bash
# Install a package
pip install pandas

# Install a specific version
pip install pandas==2.1.0

# Install minimum version
pip install "pandas>=2.0,<3.0"

# Install multiple at once
pip install pandas numpy sqlalchemy

# Install from requirements file
pip install -r requirements.txt
```

### Creating requirements.txt

```bash
# Option 1: Freeze ALL installed packages (exact versions)
pip freeze > requirements.txt

# Output:
# numpy==1.26.2
# pandas==2.1.4
# python-dateutil==2.8.2
# pytz==2023.3.post1
# six==1.16.0
# SQLAlchemy==2.0.23

# Option 2: Hand-written (preferred for libraries)
# requirements.txt
pandas>=2.0,<3.0
sqlalchemy>=2.0
boto3>=1.28
pydantic>=2.0,<3.0
```

### requirements.txt Best Practices

```bash
# requirements.txt — production dependencies
pandas==2.1.4
sqlalchemy==2.0.23
boto3==1.34.0
pydantic==2.5.2

# requirements-dev.txt — development tools
-r requirements.txt    # Include production deps
pytest==7.4.3
mypy==1.8.0
black==23.12.1
ruff==0.1.9

# Install dev dependencies:
pip install -r requirements-dev.txt
```

---

## pip freeze — Capture Current State

```bash
# See everything installed in current environment
pip freeze

# Piped to file for reproducibility
pip freeze > requirements.txt

# Check what you have installed (human-readable)
pip list

# Check for outdated packages
pip list --outdated

# Show details about a package
pip show pandas
# Name: pandas
# Version: 2.1.4
# Location: .venv/lib/python3.11/site-packages
# Requires: numpy, python-dateutil, pytz
```

---

## Dependency Conflicts Explained

```bash
# Scenario: You install two packages that need different versions of a shared dep
pip install package-A   # Requires numpy>=1.24,<1.26
pip install package-B   # Requires numpy>=1.26

# pip will try to resolve this and may:
# 1. Install a version that satisfies both (if possible)
# 2. Show an error: "Cannot install package-A and package-B because..."
# 3. Silently break one of them (older pip versions)
```

```python
# Check for conflicts manually
# pip check — verifies all installed packages are compatible
```

```bash
pip check
# Output if conflicts exist:
# package-a 1.0 requires numpy<1.26, but you have numpy 1.26.2

# Resolution strategies:
# 1. Find versions of A and B that agree on numpy
# 2. Pin the shared dependency: numpy==1.25.2
# 3. Contact package maintainers for updated compatibility
```

---

## pyenv — Python Version Management

Different projects may need different Python versions.

```bash
# Install pyenv (manages multiple Python versions)
# Linux/Mac:
curl https://pyenv.run | bash

# List available Python versions
pyenv install --list

# Install specific versions
pyenv install 3.11.7
pyenv install 3.12.1

# Set global default
pyenv global 3.11.7

# Set per-project Python version
cd my-project/
pyenv local 3.12.1
# Creates .python-version file

# Check current version
pyenv version        # Shows active version
python --version     # Confirms it matches

# Common pattern: pyenv + venv
pyenv local 3.11.7
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## .gitignore for Virtual Environments

```gitignore
# Virtual environments — NEVER commit these
.venv/
venv/
env/
.env/

# Python bytecode
__pycache__/
*.py[cod]
*$py.class
*.so

# Distribution / packaging
dist/
build/
*.egg-info/
*.egg

# IDE
.idea/
.vscode/
*.swp

# OS files
.DS_Store
Thumbs.db
```

> **Always commit:** `requirements.txt` (or `pyproject.toml`). 
> **Never commit:** the virtual environment folder itself (it's regeneratable).

---

## Common Workflow for a New Data Engineering Project

```bash
# 1. Set Python version
pyenv local 3.11.7

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\Activate.ps1 on Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add new packages as needed
pip install boto3
pip freeze > requirements.txt  # Update lock

# 5. Verify no conflicts
pip check

# 6. Run your code
python src/pipeline.py
```

The flow below summarizes this standard setup sequence, from pinning a Python version through freezing dependencies and committing the lock so the environment is reproducible.

```mermaid
flowchart LR
    A[Set Python version] --> B[Create venv]
    B --> C[Install deps]
    C --> D[Develop]
    D --> E[Freeze deps]
    E --> F[Commit requirements.txt]
```

---

## Troubleshooting Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError` | Package not installed in active venv | `pip install <package>` |
| Wrong Python version | venv created with different Python | Recreate venv with correct Python |
| `pip` installs to system | venv not activated | Activate first: `source .venv/bin/activate` |
| Dependency conflict | Two packages need different versions | Check `pip check`, resolve manually |
| "Permission denied" on install | Installing without venv (system Python) | Always use venv, never `sudo pip` |

---

## Interview Tips

> **Tip 1:** "Why use virtual environments?" — "Dependency isolation. Without venvs, all projects share one set of packages, so upgrading pandas for Project A might break Project B. Venvs give each project its own isolated `site-packages` directory. They also make deployments reproducible — `pip freeze` captures exact versions that can be replicated anywhere."

> **Tip 2:** "What's the difference between requirements.txt and pip freeze?" — "`pip freeze` dumps everything currently installed (including transitive dependencies) with exact versions. Hand-written `requirements.txt` lists only direct dependencies with version ranges. Best practice: maintain a hand-written file for clarity, and use `pip freeze` or a lock file for reproducibility in CI/CD."

> **Tip 3:** "How do you handle a dependency conflict?" — "First, `pip check` to identify the conflict. Then: (1) Try finding compatible versions of both packages. (2) Check if either package has a newer release that widens compatibility. (3) If impossible, consider vendoring the conflicting dependency or using separate environments for conflicting tools."
