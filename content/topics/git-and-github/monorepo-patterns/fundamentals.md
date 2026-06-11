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
