---
title: "Repos and CI/CD - Real-World Production Examples"
topic: databricks
subtopic: repos-and-ci-cd
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, repos, ci-cd, production, deployment, patterns]
---

# Databricks Repos and CI/CD — Real-World Production Examples

## Pattern 1: Complete CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/data_pipeline_cicd.yml
name: Data Pipeline CI/CD

on:
  pull_request: { branches: [main], paths: ['src/**', 'tests/**'] }
  push: { branches: [main], paths: ['src/**'] }

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements-dev.txt
      - run: ruff check src/ tests/           # Lint
      - run: pytest tests/ -v --cov=src/      # Unit tests with coverage
      - run: pytest tests/ -v -m "not slow"   # Skip integration tests in CI

  deploy-staging:
    if: github.ref == 'refs/heads/main'
    needs: lint-and-test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install databricks-cli
      - run: databricks repos update --path /Repos/staging/pipelines --branch main
      - run: |
          RUN_ID=$(databricks jobs run-now --job-id ${{ vars.STAGING_JOB }} | jq -r '.run_id')
          databricks runs get --run-id $RUN_ID --wait
      - run: python scripts/validate_staging.py

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: databricks repos update --path /Repos/production/pipelines --branch main
      - run: |
          curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
            -d '{"text":"✅ Production deployed: data-pipelines (commit: ${{ github.sha }})"}'
```

---

## Pattern 2: Multi-Team Repository Structure

```
# Organization: 5 teams, 30 pipelines, shared libraries

data-platform/                    # Monorepo
├── lib/                          # Shared utilities (used by all teams)
│   ├── quality.py               # DQ checks
│   ├── notifications.py         # Slack/PagerDuty helpers
│   └── config.py                # Environment config loader
├── teams/
│   ├── sales/
│   │   ├── pipelines/
│   │   │   ├── ingest_orders.py
│   │   │   └── transform_orders.py
│   │   └── tests/
│   │       └── test_orders.py
│   ├── marketing/
│   │   ├── pipelines/
│   │   └── tests/
│   └── ml/
│       ├── pipelines/
│       └── tests/
├── terraform/
│   ├── shared/                  # Shared infra (catalog, policies)
│   ├── sales/                   # Sales team jobs
│   └── marketing/               # Marketing team jobs
├── .github/workflows/
│   ├── ci.yml                   # Universal: lint + unit tests
│   ├── deploy_sales.yml         # Per-team: only deploys when sales/ changes
│   └── deploy_marketing.yml
└── requirements.txt
```

```yaml
# .github/workflows/deploy_sales.yml (path-based trigger)
on:
  push:
    branches: [main]
    paths: ['teams/sales/**', 'lib/**']  # Only when sales code or shared lib changes
```

---

## Pattern 3: Databricks Asset Bundles (DABs)

```yaml
# databricks.yml — modern deployment config (replaces Terraform for some teams)
bundle:
  name: data-pipelines

workspace:
  host: https://adb-123456.azuredatabricks.net

resources:
  jobs:
    daily_etl:
      name: "daily_etl_${bundle.environment}"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/bronze/ingest_orders.py
          job_cluster_key: etl
        - task_key: transform
          depends_on: [{ task_key: ingest }]
          notebook_task:
            notebook_path: ./src/silver/transform_orders.py
          job_cluster_key: etl
      job_clusters:
        - job_cluster_key: etl
          new_cluster:
            spark_version: 14.3.x-photon-scala2.12
            node_type_id: i3.xlarge
            autoscale: { min_workers: 4, max_workers: 12 }

environments:
  staging:
    workspace: { host: https://staging.azuredatabricks.net }
  production:
    workspace: { host: https://production.azuredatabricks.net }
```

```bash
# Deploy with DABs:
databricks bundle validate        # Check config is valid
databricks bundle deploy -e staging    # Deploy to staging
databricks bundle run daily_etl -e staging  # Run in staging
databricks bundle deploy -e production  # Deploy to production
```

---

## Pattern 4: Automated Quality Gates

```python
# scripts/quality_gate.py — blocks deployment if quality drops

import sys
from databricks import sql

def run_quality_gate(environment: str) -> bool:
    """Run quality checks. Return True if all pass."""
    conn = sql.connect(server_hostname=HOST, http_path=PATH, access_token=TOKEN)
    cursor = conn.cursor()
    
    checks = [
        # Check 1: Row count within expected range
        {
            "name": "row_count",
            "query": f"SELECT COUNT(*) FROM {environment}.silver.orders WHERE _loaded_at >= current_date()",
            "assertion": lambda val: 10000 < val < 5000000,
            "message": "Row count out of expected range",
        },
        # Check 2: No null primary keys
        {
            "name": "null_pks",
            "query": f"SELECT COUNT(*) FROM {environment}.silver.orders WHERE order_id IS NULL AND _loaded_at >= current_date()",
            "assertion": lambda val: val == 0,
            "message": "Null primary keys found!",
        },
        # Check 3: Revenue is positive
        {
            "name": "revenue_positive",
            "query": f"SELECT MIN(amount) FROM {environment}.silver.orders WHERE _loaded_at >= current_date()",
            "assertion": lambda val: val is None or val >= 0,
            "message": "Negative amounts detected!",
        },
    ]
    
    failures = []
    for check in checks:
        cursor.execute(check["query"])
        value = cursor.fetchone()[0]
        if not check["assertion"](value):
            failures.append(f"❌ {check['name']}: {check['message']} (value: {value})")
        else:
            print(f"✓ {check['name']}: passed (value: {value})")
    
    if failures:
        print("\n🚫 QUALITY GATE FAILED:")
        for f in failures:
            print(f"  {f}")
        return False
    
    print("\n✅ All quality gates passed!")
    return True

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "staging"
    passed = run_quality_gate(env)
    sys.exit(0 if passed else 1)
```

---

## Pattern 5: Hotfix Process

```python
# Emergency production fix (bypasses normal staging validation)

HOTFIX_PROCESS = {
    "1_create_branch": "git checkout -b hotfix/fix-null-orders main",
    "2_fix_code": "Make the minimal fix (smallest possible change)",
    "3_unit_test": "pytest tests/test_affected_module.py (fast, local)",
    "4_direct_deploy": {
        "command": "databricks repos update --path /Repos/production/pipelines --branch hotfix/fix-null-orders",
        "note": "Deploys directly to production (bypasses staging for speed)",
    },
    "5_validate": "Run production job, verify fix works",
    "6_merge_back": "PR hotfix branch → main (so staging stays in sync)",
    "7_post_mortem": "Document: what broke, why, how to prevent in future",
}

# Safeguards for hotfixes:
# - Requires senior engineer approval (GitHub CODEOWNERS)
# - Must have unit test proving the fix works
# - Post-deploy validation within 30 minutes
# - Merge back to main immediately (or staging diverges)
# - Document in incident tracker
```

---

## Interview Tips

> **Tip 1:** "Walk me through your CI/CD for Databricks" — PR triggers: lint (ruff) + unit tests (pytest) in CI. Merge to main triggers: update staging Repos → run staging pipeline → quality gate validation → if pass: update production Repos. Infrastructure via Terraform (applied separately). Rollback: update Repos to previous Git tag.

> **Tip 2:** "How do you handle emergency hotfixes?" — Create hotfix branch from main → minimal fix → unit test → deploy directly to production Repos (bypass staging for speed) → validate → merge back to main. Requires senior approval. Document in post-mortem. Key: smallest possible change, fast deploy, immediate validation.

> **Tip 3:** "Terraform vs Databricks Asset Bundles?" — Terraform: mature, manages ALL infrastructure (not just Databricks), great for platform teams managing cross-cloud resources. DABs: Databricks-native, simpler for pipeline developers, bundles code + config together, good for teams that only use Databricks. Use Terraform for platform infra; DABs for pipeline-specific deployment.
