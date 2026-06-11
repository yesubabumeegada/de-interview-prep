---
title: "GitHub Actions - Intermediate"
topic: ci-cd
subtopic: github-actions
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, github-actions, reusable-workflows, matrix, caching]
---

# GitHub Actions — Intermediate

## Dependency Caching

```yaml
- name: Cache pip dependencies
  uses: actions/cache@v4
  with:
    path: ~/.cache/pip
    key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt', 'requirements-dev.txt') }}
    restore-keys: |
      ${{ runner.os }}-pip-

- name: Install dependencies
  run: pip install -r requirements.txt -r requirements-dev.txt

# Cache dbt packages
- name: Cache dbt packages
  uses: actions/cache@v4
  with:
    path: dbt_packages/
    key: dbt-${{ hashFiles('packages.yml') }}
```

Caching can reduce CI time from 4 minutes to under 1 minute for dependency-heavy DE projects.

---

## Matrix Builds

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
        dbt-version: ["1.6", "1.7"]
      fail-fast: false  # don't cancel all if one fails
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - run: pip install dbt-core==${{ matrix.dbt-version }}
      - run: dbt compile
```

---

## Reusable Workflows

```yaml
# .github/workflows/reusable-dbt-test.yml — shared across repos
on:
  workflow_call:
    inputs:
      environment:
        required: true
        type: string
      dbt_target:
        required: false
        type: string
        default: "dev"
    secrets:
      DBT_SNOWFLAKE_PASSWORD:
        required: true

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: dbt test --target ${{ inputs.dbt_target }}
        env:
          DBT_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
```

```yaml
# Calling the reusable workflow from another workflow
jobs:
  test-staging:
    uses: org/shared-workflows/.github/workflows/reusable-dbt-test.yml@main
    with:
      environment: staging
      dbt_target: staging
    secrets:
      DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
```

---

## Conditional Steps and Outputs

```yaml
jobs:
  check-changes:
    runs-on: ubuntu-latest
    outputs:
      dbt-changed: ${{ steps.filter.outputs.dbt }}
      pipelines-changed: ${{ steps.filter.outputs.pipelines }}
    steps:
      - uses: actions/checkout@v4
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: |
            dbt:
              - 'dbt/**'
            pipelines:
              - 'pipelines/**'

  dbt-test:
    needs: check-changes
    if: needs.check-changes.outputs.dbt-changed == 'true'
    runs-on: ubuntu-latest
    steps:
      - run: dbt test

  pipeline-test:
    needs: check-changes
    if: needs.check-changes.outputs.pipelines-changed == 'true'
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/pipelines/
```

---

## Environment Protection Rules

```yaml
jobs:
  deploy-prod:
    runs-on: ubuntu-latest
    environment:
      name: production        # requires approval from production reviewers
      url: https://app.example.com
    steps:
      - name: Deploy to production
        run: ./deploy.sh production
```

GitHub environments let you set protection rules: required reviewers, wait timers, and environment-specific secrets — giving you a manual approval gate before production deploys.

---

## Self-Hosted Runners for Heavy Jobs

```yaml
jobs:
  spark-test:
    # Run on self-hosted runner with Spark pre-installed
    runs-on: [self-hosted, linux, spark]
    steps:
      - run: spark-submit --master local[4] tests/spark_integration.py
```

Use self-hosted runners when:
- Jobs need more than 7 GB RAM (GitHub's limit)
- You need access to internal network resources
- Spark jobs need pre-installed JVM/Spark
- Cost optimization (avoid GitHub Actions minutes charges)

---

## Debugging Failed Workflows

```yaml
# Enable debug logging by setting secrets:
# ACTIONS_RUNNER_DEBUG = true
# ACTIONS_STEP_DEBUG = true

# Download logs
- uses: actions/upload-artifact@v4
  if: failure()   # only upload on failure
  with:
    name: test-logs
    path: |
      logs/
      pytest-output.xml

# SSH into runner for debugging (tmate)
- uses: mxschmitt/action-tmate@v3
  if: failure()
  timeout-minutes: 30
```
