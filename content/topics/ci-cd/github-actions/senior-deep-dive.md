---
title: "GitHub Actions - Senior Deep Dive"
topic: ci-cd
subtopic: github-actions
content_type: study_material
difficulty_level: senior
layer: senior_deep_dive
tags: [ci-cd, github-actions, oidc, security, composite-actions]
---

# GitHub Actions — Senior Deep Dive

## OIDC for Secretless Cloud Authentication

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # required for OIDC
      contents: read
    
    steps:
      - name: Configure AWS credentials via OIDC
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789:role/GitHubActionsRole
          aws-region: us-east-1
          # NO secrets needed — GitHub OIDC token proves identity

      - name: Deploy to S3
        run: aws s3 sync dist/ s3://my-bucket/
```

**AWS IAM Trust Policy:**
```json
{
  "Principal": {"Federated": "arn:aws:iam::123456789:oidc-provider/token.actions.githubusercontent.com"},
  "Condition": {
    "StringEquals": {
      "token.actions.githubusercontent.com:sub": "repo:org/repo:ref:refs/heads/main"
    }
  }
}
```

OIDC eliminates long-lived secrets in GitHub → dramatically reduces credential exposure risk.

---

## Composite Actions

```yaml
# .github/actions/setup-de-env/action.yml — reusable composite action
name: 'Setup DE Environment'
description: 'Install Python, cache deps, configure dbt'

inputs:
  python-version:
    description: 'Python version'
    default: '3.11'
  dbt-profiles-dir:
    description: 'Path to dbt profiles'
    default: 'profiles/'

runs:
  using: composite
  steps:
    - uses: actions/setup-python@v5
      with:
        python-version: ${{ inputs.python-version }}

    - uses: actions/cache@v4
      with:
        path: ~/.cache/pip
        key: ${{ runner.os }}-pip-${{ hashFiles('requirements*.txt') }}

    - run: pip install -r requirements.txt -r requirements-dev.txt
      shell: bash

    - run: dbt deps --profiles-dir ${{ inputs.dbt-profiles-dir }}
      shell: bash
```

```yaml
# Use it in any workflow:
- uses: ./.github/actions/setup-de-env
  with:
    python-version: '3.12'
```

---

## Workflow Security Hardening

```yaml
# Minimal permissions (principle of least privilege)
permissions:
  contents: read      # read code only
  pull-requests: write  # only for PR comment posting
  id-token: write     # only for OIDC

# Pin third-party actions to full SHA (not tag — tags can be overwritten)
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683  # v4.2.2
- uses: aws-actions/configure-aws-credentials@e3dd6a429d7300a6a4c196c26e071d42e0343502  # v4.0.2

# Prevent script injection from PR title/body
- name: Check PR title
  run: |
    TITLE="${{ github.event.pull_request.title }}"
    # ❌ Dangerous — PR title could contain shell injection
    # run: echo ${{ github.event.pull_request.title }} | grep pattern
    
    # ✅ Safe — assign to env var first
    echo "PR_TITLE=$TITLE" >> $GITHUB_ENV
  env:
    TITLE: ${{ github.event.pull_request.title }}
```

---

## Advanced Workflow Patterns for DE

```yaml
# Pattern: Run affected dbt models only on PR
jobs:
  dbt-slim-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # need full history for diff

      - name: Get changed models
        id: changed
        run: |
          CHANGED=$(git diff origin/main --name-only | grep "models/" | \
            sed 's|dbt/models/||;s|.sql||' | tr '\n' ' ')
          echo "models=$CHANGED" >> $GITHUB_OUTPUT

      - name: Run only changed models + downstream
        if: steps.changed.outputs.models != ''
        run: |
          dbt run --select ${{ steps.changed.outputs.models }}+ --defer --state prod-artifacts/
          dbt test --select ${{ steps.changed.outputs.models }}+
```

---

## ⚡ Cheat Sheet

```yaml
# Trigger patterns
on:
  push:
    branches: [main]
    paths: ["src/**"]
  pull_request:
    types: [opened, synchronize, reopened]
  schedule:
    - cron: "0 2 * * *"
  workflow_dispatch:

# Expressions
${{ github.sha }}              # commit SHA
${{ github.ref }}              # branch ref
${{ github.event_name }}       # what triggered it
${{ secrets.MY_SECRET }}       # secrets
${{ vars.MY_VAR }}             # non-secret variables
${{ needs.job-name.outputs.key }}  # job outputs
${{ matrix.python-version }}   # matrix value

# Conditionals
if: github.ref == 'refs/heads/main'
if: failure()
if: success()
if: always()
if: needs.test.result == 'success'

# Permissions
permissions:
  contents: read
  id-token: write
  pull-requests: write
  packages: write    # push to GitHub Container Registry

# Key actions
actions/checkout@v4
actions/setup-python@v5
actions/cache@v4
actions/upload-artifact@v4
actions/download-artifact@v4
aws-actions/configure-aws-credentials@v4
docker/build-push-action@v6
```
