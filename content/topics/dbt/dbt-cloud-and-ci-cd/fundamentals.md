---
title: "dbt Cloud & CI/CD"
topic: dbt
subtopic: dbt-cloud-and-ci-cd
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, dbt-cloud, ci-cd, github-actions, deployment]
---

# dbt Cloud & CI/CD

## dbt Core vs dbt Cloud

| Feature | dbt Core (open source) | dbt Cloud |
|---|---|---|
| Running models | ✅ CLI only | ✅ CLI + UI + scheduled jobs |
| IDE | ✅ Your editor | ✅ Browser-based IDE |
| Job scheduling | Manual (Airflow/cron) | ✅ Built-in scheduler |
| CI/CD | You configure (GitHub Actions) | ✅ Built-in CI/CD |
| Docs hosting | Self-host | ✅ Managed |
| Semantic layer | dbt-metricflow CLI | ✅ Managed API |
| Cost | Free | Paid (free tier available) |

## dbt Cloud Key Concepts

### Environments

```
Development   →  Each developer has own schema (dbt_jsmith)
Staging/CI    →  PR testing environment (ci_pr_123)
Production    →  Scheduled runs against prod schemas
```

### Jobs

A job is a collection of dbt commands run on a schedule or trigger:

```yaml
# Example: Daily production job
Job name: "Daily Production Run"
Environment: Production
Schedule: Daily at 5:00 AM UTC
Commands:
  - dbt source freshness
  - dbt build --exclude tag:weekly
  - dbt test --select tag:critical

Notifications:
  On failure: Slack #data-alerts
  On success: Slack #data-ops
```

### Run Triggers

- **Schedule**: Cron-based (every hour, daily, weekly)
- **API trigger**: From Airflow, GitHub Actions, webhooks
- **PR trigger**: Runs CI on pull requests automatically

## Basic CI/CD with GitHub Actions + dbt Core

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'models/**'
      - 'tests/**'
      - 'macros/**'
      - 'seeds/**'

jobs:
  dbt-ci:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: Install packages
        run: dbt deps

      - name: Run dbt
        run: dbt build --target ci
        env:
          DBT_USER: ${{ secrets.SNOWFLAKE_USER }}
          DBT_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}

      - name: Upload artifacts
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: dbt-artifacts
          path: target/
```

## dbt Cloud CI Setup

In dbt Cloud:
1. **Connect GitHub** → Settings → Integrations → Link GitHub repo
2. **Create CI Environment** with a CI schema (e.g., `ci_{{ pr_number }}`)
3. **Enable CI job** → Jobs → Create Job → toggle "Run on PR"
4. dbt Cloud posts status checks to GitHub PR automatically

## Deployment Environments — Best Practice

```
Developer laptop (dev)
  ↓ git push → PR
GitHub PR (ci)
  ↓ PR merged
Staging (staging) — optional
  ↓ schedule / manual
Production (prod)
```

## Key dbt Cloud Features

### IDE (Integrated Development Environment)

- Write and run dbt SQL in browser
- Lineage graph visualizer
- Preview results
- Auto-complete for `ref()` and `source()`

### Explore (formerly Docs)

- Full project documentation
- Interactive lineage graph
- Column-level lineage (dbt Cloud only)
- Search across all models

### Discovery API

Query project metadata programmatically:

```graphql
# GraphQL query to get model metadata
{
  models(filter: {projectId: "12345"}) {
    name
    description
    tags
    tests { name status }
  }
}
```
