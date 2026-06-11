---
title: "Monorepo Patterns - Fundamentals"
topic: git-and-github
subtopic: monorepo-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [git, github, monorepo-patterns]
---

# Monorepo Patterns — Fundamentals

A monorepo stores multiple projects (dbt models, Airflow DAGs, Spark jobs, Python utilities) in a single git repository. This enables atomic changes across components, shared tooling, and unified CI — but requires discipline to keep CI fast.


## 🎯 Analogy

Think of a monorepo like a single office building that houses multiple teams: the engineering team, data team, and ML team all work in the same repo. You get easy cross-team PRs and shared tooling, but you need smart CI/CD that only builds what changed.

---
## Monorepo vs. Polyrepo

| | Monorepo | Polyrepo |
|---|---|---|
| Cross-component changes | One PR | Multiple PRs |
| CI speed | Needs selective triggers | Simpler but all-or-nothing |
| Shared libraries | Easy | Requires packages |
| Team independence | Shared CI state | Full independence |

## Typical DE Monorepo Structure

```
repo/
├── dbt/                  # dbt project
│   ├── models/
│   └── tests/
├── airflow/              # Airflow DAGs
│   └── dags/
├── pipelines/            # Python pipeline code
│   ├── revenue/
│   └── customer/
├── infra/                # Terraform / K8s manifests
└── shared/               # Shared utilities
    └── utils/
```

## Path-Filtered CI

```yaml
# Only run dbt CI when dbt files change
on:
  pull_request:
    paths: ["dbt/**"]

# Only run pipeline tests when pipelines change
on:
  pull_request:
    paths: ["pipelines/**"]
```

This keeps CI fast — a change to a DAG doesn't rebuild the Spark Docker image.

## ▶️ Try It Yourself

```yaml
# Monorepo: only run CI for changed paths
# .github/workflows/ci.yml

name: Monorepo CI
on:
  pull_request:

jobs:
  detect-changes:
    runs-on: ubuntu-latest
    outputs:
      dbt_changed: ${{ steps.filter.outputs.dbt }}
      spark_changed: ${{ steps.filter.outputs.spark }}
      airflow_changed: ${{ steps.filter.outputs.airflow }}
    steps:
      - uses: actions/checkout@v4
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: |
            dbt:
              - 'pipelines/dbt/**'
            spark:
              - 'pipelines/spark/**'
            airflow:
              - 'pipelines/airflow/**'

  test-dbt:
    needs: detect-changes
    if: needs.detect-changes.outputs.dbt_changed == 'true'
    runs-on: ubuntu-latest
    steps:
      - run: echo "Running dbt tests (only because dbt files changed)"
      - run: dbt test --select state:modified+
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
