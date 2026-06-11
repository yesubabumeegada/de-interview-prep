---
title: "Git Hooks and Automation - Fundamentals"
topic: git-and-github
subtopic: git-hooks-and-automation
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [git, github, git-hooks-and-automation]
---

# Git Hooks and Automation — Fundamentals

Git hooks are scripts that run automatically at specific points in the Git workflow: before committing, after committing, before pushing. They're your last line of defense before code hits the remote repository — catching issues that CI would catch, but faster (locally, in seconds).


## 🎯 Analogy

Think of Git hooks like automatic quality gates at a building entrance: pre-commit hooks run linting and tests before you commit (if they fail, no entry), and pre-push hooks run heavier checks before your code reaches the remote server.

---
## The Pre-Commit Framework

```yaml
# .pre-commit-config.yaml — installed with: pre-commit install
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.3.0
    hooks:
      - id: ruff              # Python linting + auto-fix
      - id: ruff-format       # Python formatting

  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.0.0
    hooks:
      - id: sqlfluff-fix      # SQL formatting
        args: [--dialect, snowflake]

  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets    # Block secrets from being committed

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-json
```

```bash
# Install
pip install pre-commit
pre-commit install            # hooks run on every git commit

# Run manually
pre-commit run --all-files    # run on everything (useful in CI too)

# Skip for one commit (use sparingly)
git commit --no-verify -m "wip: quick save"
```

## How Hook Types Differ

| Hook | When | Common Uses |
|---|---|---|
| pre-commit | Before commit created | Lint, format, detect-secrets |
| commit-msg | After message entered | Enforce conventional commits |
| pre-push | Before git push | Run tests, validate branch name |

## ▶️ Try It Yourself

```bash
# Install pre-commit framework: pip install pre-commit

# .pre-commit-config.yaml
# repos:
#   - repo: https://github.com/astral-sh/ruff-pre-commit
#     rev: v0.3.0
#     hooks:
#       - id: ruff             # Python linting
#       - id: ruff-format      # Python formatting
#   - repo: https://github.com/pre-commit/pre-commit-hooks
#     rev: v4.5.0
#     hooks:
#       - id: check-yaml       # Validate YAML syntax
#       - id: detect-aws-credentials  # Block AWS key commits
#       - id: end-of-file-fixer
#   - repo: https://github.com/sqlfluff/sqlfluff
#     rev: 3.0.0
#     hooks:
#       - id: sqlfluff-lint    # Lint SQL files

# Install hooks
pre-commit install

# Run manually on all files
pre-commit run --all-files

# Manual hook script (no framework): .git/hooks/pre-commit
# #!/bin/bash
# ruff check . || exit 1
# pytest tests/unit/ -q || exit 1
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
