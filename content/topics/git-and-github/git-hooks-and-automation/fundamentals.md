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
