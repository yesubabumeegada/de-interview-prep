---
title: "GitHub Actions - Real World"
topic: ci-cd
subtopic: github-actions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, github-actions, real-world, dbt, automation]
---

# GitHub Actions — Real World

## Case Study: Automating a dbt + Airflow DE Platform

### Background

A logistics company had 3 data engineers managing 80 dbt models and 20 Airflow DAGs. CI/CD was manual: engineers would SSH into a server, run `dbt run --target prod`, and hope nothing broke. There were no automated tests, no rollback procedure, and no visibility into what changed between deploys.

### The Problem

- A dbt model rename broke 4 downstream models — discovered 6 hours later by the analytics team.
- A DAG syntax error caused 12 hours of pipeline failure before anyone noticed.
- Deployment took 45 minutes of manual steps.

### The GitHub Actions Solution

**Workflow 1: PR Validation**
```yaml
# .github/workflows/pr-check.yml
name: PR Validation
on:
  pull_request:
    branches: [main]

jobs:
  dbt-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0
      
      - name: dbt compile (catches syntax errors)
        run: dbt compile --profiles-dir profiles/
        env:
          DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD_DEV }}
          DBT_TARGET: dev
      
      - name: dbt test on changed models
        run: |
          CHANGED=$(git diff origin/main --name-only | grep "models/" | \
            sed 's|dbt/models/||;s|.sql||' | tr '\n' ' ')
          if [ -n "$CHANGED" ]; then
            dbt test --select $CHANGED+
          fi
        env:
          DBT_TARGET: dev

  dag-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install apache-airflow==2.8.0
      - name: Import all DAGs
        run: |
          python -c "
          from airflow.models import DagBag
          bag = DagBag(dag_folder='dags/', include_examples=False)
          assert not bag.import_errors, f'DAG import errors: {bag.import_errors}'
          print(f'All {len(bag.dags)} DAGs imported successfully')
          "
```

**Workflow 2: Production Deploy**
```yaml
# .github/workflows/deploy.yml
name: Deploy to Production
on:
  push:
    branches: [main]

jobs:
  deploy-dbt:
    runs-on: ubuntu-latest
    environment: production   # requires manual approval
    steps:
      - uses: actions/checkout@v4
      
      - name: Run dbt (production)
        run: dbt run --target prod --full-refresh
        env:
          DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD_PROD }}
          DBT_TARGET: prod
      
      - name: Run dbt tests
        run: dbt test --target prod
      
      - name: Notify on failure
        if: failure()
        uses: slackapi/slack-github-action@v1
        with:
          channel-id: '#data-alerts'
          slack-message: "🚨 dbt production deploy failed! PR: ${{ github.event.pull_request.html_url }}"
        env:
          SLACK_BOT_TOKEN: ${{ secrets.SLACK_BOT_TOKEN }}
```

### Results After 2 Months

| Metric | Before | After |
|---|---|---|
| Deploy time | 45 min manual | 8 min automated |
| Broken deploys per month | 4-5 | 0 |
| Time to detect DAG errors | Hours | < 5 min (PR block) |
| Schema break incidents | Monthly | None |
| Engineer confidence | Low | High |

**Key lesson:** The manual approval gate on the production environment (GitHub environment protection) preserved the human-in-the-loop while automating everything else. Engineers felt safe deploying because CI had already caught the common mistakes.
