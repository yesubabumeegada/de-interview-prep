---
title: "Branching Strategies - Intermediate"
topic: git-and-github
subtopic: branching-strategies
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [git, branching, release-management, hotfix, feature-flags]
---

# Branching Strategies — Intermediate

## Gitflow In Practice

```bash
# Full Gitflow lifecycle for a data pipeline feature

# 1. Start feature from develop
git checkout develop
git pull origin develop
git checkout -b feature/add-customer-segmentation

# 2. Work in small commits
git commit -m "feat: add RFM scoring model"
git commit -m "test: add unit tests for RFM calculation"
git commit -m "feat: wire RFM to customer_segments table"

# 3. Merge back to develop via PR
git push origin feature/add-customer-segmentation
# → Open PR: feature/add-customer-segmentation → develop

# 4. When ready for release
git checkout develop
git checkout -b release/2024-q1

# 5. Stabilize (only bug fixes here, no new features)
git commit -m "fix: handle null RFM scores for new customers"

# 6. Release: merge to both main AND develop
git checkout main
git merge --no-ff release/2024-q1
git tag -a v2024-q1 -m "Q1 2024 release"
git push origin main --tags

git checkout develop
git merge --no-ff release/2024-q1
git push origin develop

git branch -d release/2024-q1
```

---

## Hotfix Pattern

```bash
# Production is broken — patch directly from main
git checkout main
git pull origin main
git checkout -b hotfix/DE-emergency-null-fix

# Fix the bug
git commit -m "fix: handle null customer_id in revenue pipeline"

# Merge to main AND develop
git checkout main
git merge --no-ff hotfix/DE-emergency-null-fix
git tag -a v2024-q1.1 -m "Hotfix: null customer_id"
git push origin main --tags

git checkout develop
git merge --no-ff hotfix/DE-emergency-null-fix
git push origin develop

git branch -d hotfix/DE-emergency-null-fix
```

---

## Feature Flags for Pipeline Work-in-Progress

Feature flags let you merge incomplete work to main without enabling it in production:

```python
# config/feature_flags.py
import os

FEATURE_FLAGS = {
    "new_revenue_v2": os.getenv("FF_REVENUE_V2", "false").lower() == "true",
    "ml_segmentation": os.getenv("FF_ML_SEGMENTATION", "false").lower() == "true",
}

# In your DAG
from config.feature_flags import FEATURE_FLAGS

if FEATURE_FLAGS["new_revenue_v2"]:
    transform = new_revenue_v2_transform
else:
    transform = legacy_revenue_transform
```

```yaml
# Enable in staging (env var in CI)
FF_REVENUE_V2: "true"

# Keep disabled in production until ready
FF_REVENUE_V2: "false"
```

This enables continuous integration without continuous delivery — code ships to main, release is controlled by flags.

---

## Release Branch Strategy for dbt

```bash
# dbt-specific: branches map to dbt targets
git checkout -b feat/DE-101-new-marts

# dbt profiles.yml: branch name → target
# dev target: reads from dev schema
# staging target: reads from staging schema
# prod target: reads from prod schema

# In CI: auto-detect which dbt target based on branch
if [[ "$GITHUB_REF" == "refs/heads/main" ]]; then
  DBT_TARGET=prod
elif [[ "$GITHUB_REF" == refs/heads/staging ]]; then
  DBT_TARGET=staging
else
  DBT_TARGET=dev
fi
dbt run --target $DBT_TARGET
```

---

## Comparing Strategies at Scale

| Factor | Gitflow | Trunk-Based |
|---|---|---|
| Merge frequency | Weekly/sprint-end | Daily or more |
| Long-lived branches | Yes (develop, release) | No |
| Release cadence | Scheduled | Continuous |
| Merge conflict risk | High (long branches) | Low (frequent merges) |
| CI feedback cycle | Slow (days) | Fast (hours) |
| Best for | Scheduled releases | Continuous delivery |
| DE pipelines recommendation | ❌ | ✅ |

For data pipelines that should deploy frequently and safely, trunk-based is almost always better. Gitflow's long-lived branches create painful merge conflicts and delay feedback.
