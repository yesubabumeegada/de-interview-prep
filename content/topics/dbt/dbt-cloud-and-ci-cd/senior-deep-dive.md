---
title: "dbt Cloud & CI/CD - Senior Deep Dive"
topic: dbt
subtopic: dbt-cloud-and-ci-cd
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, advanced-cicd, multi-project, deployment-patterns, observability]
---

# dbt Cloud & CI/CD — Senior Deep Dive

## Advanced State Management

### State Comparison Strategies

```bash
# Compare to prod (most common)
dbt build --select state:modified+ --state ./prod-manifest

# Compare to a specific historical run
aws s3 cp s3://artifacts/2024-01-15/manifest.json ./baseline/
dbt build --select state:modified+ --state ./baseline

# State-based freshness check
dbt source freshness --select state:modified
```

### State-Based Selector Types

```bash
# Modified: files changed vs comparison state
state:modified

# Modified and all downstream
state:modified+

# New models (didn't exist in comparison state)
state:new

# Modified test files
state:modified.tests

# Anything changed (models, tests, macros, snapshots)
state:modified+
```

## Multi-Project CI/CD (dbt Mesh)

Each project has its own CI/CD pipeline, but must coordinate:

```yaml
# .github/workflows/platform-project-ci.yml
name: Platform Project CI

on:
  push:
    branches: [main]

jobs:
  ci:
    steps:
      - name: Build and test changed models
        run: dbt build --select state:modified+

      - name: Update shared manifest
        if: success()
        run: |
          aws s3 cp target/manifest.json \
            s3://artifacts/platform-project/latest/manifest.json

      - name: Notify downstream projects
        run: |
          # Trigger CI in projects that depend on platform_project
          curl -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            https://api.github.com/repos/org/finance-project/dispatches \
            -d '{"event_type": "platform_updated"}'
```

## Automated Rollback Strategy

```bash
#!/bin/bash
# deploy_with_rollback.sh

set -e

echo "Starting dbt deployment..."

# Save current prod manifest as backup
aws s3 cp \
  s3://artifacts/prod/manifest.json \
  s3://artifacts/prod/manifest.json.backup

# Deploy
dbt build --target prod --full-refresh-models fct_orders

# Run smoke tests
dbt test --select tag:smoke_test --target prod

echo "✅ Deployment successful"
```

```bash
#!/bin/bash
# rollback.sh - triggered on failure

echo "Rolling back..."

# Restore previous manifest
aws s3 cp \
  s3://artifacts/prod/manifest.json.backup \
  s3://artifacts/prod/manifest.json

# Restore data from Time Travel (Snowflake)
dbt run-operation restore_from_time_travel \
  --args '{"minutes_ago": 30, "models": ["fct_orders"]}'
```

## CI Performance Optimization

### Caching dbt packages

```yaml
# GitHub Actions
- name: Cache dbt packages
  uses: actions/cache@v3
  with:
    path: dbt_packages/
    key: dbt-packages-${{ hashFiles('packages.yml') }}

- name: dbt deps (cached)
  run: dbt deps
```

### Parallel CI across PR files changed

```yaml
# Matrix strategy for large projects
jobs:
  ci-matrix:
    strategy:
      matrix:
        domain: [finance, marketing, operations, core]
    steps:
      - name: Build domain
        run: dbt build --select tag:${{ matrix.domain }} state:modified+
```

### Skip CI for doc-only changes

```yaml
on:
  pull_request:
    paths:
      - 'models/**/*.sql'
      - 'models/**/*.yml'
      - 'tests/**'
      - 'macros/**'
    paths-ignore:
      - '**.md'
      - 'docs/**'
      - '.github/**'
```

## Observability and Alerting

### Custom Run Monitoring

```python
# scripts/monitor_dbt_run.py
import json
import requests

def analyze_run_results(results_path: str):
    with open(results_path) as f:
        results = json.load(f)
    
    failed_models = [
        r for r in results['results']
        if r['status'] in ['error', 'fail']
    ]
    
    slow_models = [
        r for r in results['results']
        if r.get('execution_time', 0) > 300  # > 5 minutes
    ]
    
    if failed_models:
        send_pagerduty_alert(failed_models)
    
    if slow_models:
        send_slack_warning(slow_models, channel='#data-performance')
    
    # Post metrics to Datadog
    for result in results['results']:
        post_metric(
            'dbt.model.execution_time',
            result.get('execution_time', 0),
            tags=[f"model:{result['unique_id']}", f"status:{result['status']}"]
        )
```

## dbt Cloud Admin Patterns

### Job Chaining

```
Daily pipeline:
  Job 1: Source freshness check (5:00 AM)
    → If pass → Job 2: Full dbt build (5:05 AM)
    → If fail → Alert + abort
  
  Job 2: Full dbt build
    → If pass → Job 3: Semantic layer export (7:00 AM)
    → If fail → Rollback + PagerDuty alert
  
  Job 3: Semantic layer KPI export
    → On complete → Job 4: Notify stakeholders
```

### Access Control

```yaml
# dbt Cloud groups and permissions
Groups:
  Analytics Engineers:
    - View all projects
    - Develop in dev environment
    - Cannot modify production jobs

  Data Platform Team:
    - Admin access to all environments
    - Can create/modify production jobs
    - Can manage connections

  Business Analysts:
    - View docs and lineage only
    - Can run queries in dbt Cloud IDE (read-only)
```
