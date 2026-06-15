---
title: "Data Pipeline Versioning - Scenario Questions"
topic: ci-cd
subtopic: data-pipeline-versioning
content_type: scenario_question
tags: [ci-cd, versioning, data-pipelines, dbt, airflow, schema-evolution, data-contracts, breaking-changes]
---

# Data Pipeline Versioning — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Your dbt Model Has a Bug in Production

You're a junior data engineer at an e-commerce company. Yesterday, a PR you authored was merged and deployed. Today, the analytics team Slacks you saying that the daily revenue dashboard is showing numbers 15% lower than expected since this morning's dbt run. After checking your code, you realize you accidentally added a filter `WHERE status != 'pending'` to the `fct_orders` model when you should have only excluded `cancelled` orders. Pending orders represent about 15% of daily volume and should be included in revenue.

**Your tasks:**
1. What are the immediate steps you take?
2. How do you fix and deploy the corrected model?
3. What data cleanup is needed?

<details>
<summary>✅ Solution</summary>

**Step 1: Communicate immediately (don't hide it)**

```
Slack message to analytics team:
"I've identified the root cause. Yesterday's PR accidentally excluded 'pending' orders 
from fct_orders. This affects today's revenue numbers (today's morning run only).
I'm reverting and reprocessing now — ETA 30 minutes for correct data."
```

**Step 2: Revert the bad change in Git**

```bash
# Find the commit that introduced the bug
git log --oneline -10
# abc1234 feat(orders): add filter for cancelled orders

# Option A: Revert the commit (creates a new revert commit — safer)
git revert abc1234
git push origin main

# Option B: Hotfix PR (shows the fix clearly in code review)
git checkout -b hotfix/fix-orders-filter
# Edit the model to fix the filter
git commit -m "fix(orders): include pending orders in fct_orders — was accidentally excluded"
git push origin hotfix/fix-orders-filter
# Open PR, get quick review, merge
```

**Step 3: Fix the incorrect model SQL**

```sql
-- WRONG (what was deployed)
WHERE status != 'pending'

-- CORRECT (only exclude cancelled)
WHERE status NOT IN ('cancelled', 'returned')
```

**Step 4: Reprocess today's data**

```bash
# Full refresh the affected model to recompute today's incorrect partition
dbt run \
  --select fct_orders+ \
  --target prod \
  --full-refresh  # Recompute from source, don't append to bad data

# Run tests to verify the fix
dbt test --select fct_orders
```

**Step 5: Communicate resolution**

```
"Fix deployed and data reprocessed. Revenue for today now shows $X (vs. $Y incorrectly 
this morning). Historical data before today is unaffected. Added a dbt test to prevent 
this regression: `accepted_values: pending should be included in status values`."
```

**Prevention going forward:**

```yaml
# models/marts/schema.yml
models:
  - name: fct_orders
    tests:
      - dbt_utils.expression_is_true:
          name: pending_orders_included
          expression: "status = 'pending'"
          # This test fails if NO pending orders exist in the output
          # catching cases where they're accidentally filtered out
```

**Key takeaway:** Speed of communication and data reprocessing matters as much as the fix itself. Dashboards showing wrong numbers for hours erode trust more than the original bug.

</details>
</article>

<article data-difficulty="mid">

## Scenario 2: Safely Renaming a High-Traffic Column

You're a mid-level data engineer. The analytics team has decided to rename the `order_value` column in the `fct_orders` table to `gross_revenue` for clarity. This table is queried by 47 dashboards in Looker, 12 Airflow downstream tasks, 3 dbt models in the analytics engineers' project, and 2 direct SQL queries from the data science team. Your manager wants this done by end of month. Design the complete migration plan with minimal downtime.

<details>
<summary>✅ Solution</summary>

**Step 1: Impact Analysis (Day 1)**

```sql
-- Snowflake: Find all objects referencing the old column name
SELECT
    referenced_object_name,
    referencing_object_name,
    referencing_object_type
FROM snowflake.account_usage.object_dependencies
WHERE referenced_object_name = 'FCT_ORDERS'
  AND referenced_column_name = 'ORDER_VALUE';

-- Query history to find all queries using the column
SELECT DISTINCT
    query_text,
    user_name,
    role_name,
    COUNT(*) as query_count,
    MAX(start_time) as last_used
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%order_value%'
  AND query_text ILIKE '%fct_orders%'
  AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY query_count DESC;
```

**Step 2: Zero-Downtime Migration Plan**

```
Week 1: Add new column, populate it, deploy dual-write code
Week 2-3: Consumer migration period
Week 4: Cutover with view alias
Week 6: Monitor and remove old column
```

**Week 1: Add new column alongside old (backward compatible)**

```sql
-- No downtime — adding a column is online in Snowflake
ALTER TABLE analytics.fct_orders ADD COLUMN gross_revenue DECIMAL(12,2);

-- Backfill historical data
UPDATE analytics.fct_orders SET gross_revenue = order_value;
```

**dbt changes for dual-write:**

```sql
-- models/marts/fct_orders.sql — write to both columns
SELECT
    order_id,
    customer_id,
    total_amount,
    total_amount AS order_value,     -- Keep old name (backward compat)
    total_amount AS gross_revenue    -- New name (preferred going forward)
FROM {{ ref('stg_orders') }}
```

**Week 2-3: Create a tracking issue and migrate consumers**

```markdown
## Column Rename Migration: order_value → gross_revenue in fct_orders

Target completion: 2024-02-15
Contact: your-name@company.com

### Consumers to migrate:
- [ ] Looker (47 dashboards) — @looker-admin to do bulk find/replace
- [ ] Airflow task: orders_summary_dag → orders_summary task (@teammate-a)
- [ ] Airflow task: revenue_forecast_dag → feature_engineering task (@teammate-b)
- [ ] dbt analytics project: daily_revenue.sql (@analytics-engineer)
- [ ] DS team notebooks (@data-scientist) — direct Slack message sent

### How to migrate:
Replace `order_value` with `gross_revenue` in your query.
Both columns return identical values until 2024-03-01.
```

**Week 4: Cutover with view (safety net)**

```sql
-- After all consumers confirmed migrated, add deprecation comment
COMMENT ON COLUMN analytics.fct_orders.order_value
  IS 'DEPRECATED: Use gross_revenue. Will be removed 2024-03-01.';
```

**Week 6: Verify and remove**

```sql
-- Check if anything still queries the old column
SELECT COUNT(*) as old_column_queries
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%order_value%'
  AND query_text ILIKE '%fct_orders%'
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());

-- If count = 0, safe to remove
ALTER TABLE analytics.fct_orders DROP COLUMN order_value;

-- Update dbt model to remove dual-write
-- (dbt model no longer outputs order_value)
```

**Update the data contract:**

```yaml
# data-contracts/fct-orders-v3.0.0.yaml
version: 3.0.0
changelog:
  - version: "3.0.0"
    type: major
    description: "Rename order_value to gross_revenue — BREAKING. Migration period: 2024-01-15 to 2024-03-01."
```

**Key: The migration is successful when the old column is dropped AND no alerts fire.** Don't declare victory until you've verified in query history.

</details>
</article>

<article data-difficulty="senior">

## Scenario 3: Building a Pipeline Versioning and Rollback System

You're the lead data platform engineer. The company has 40 data pipelines (mix of dbt models, Airflow DAGs, and Spark jobs). In the past 6 months, there have been 4 production incidents caused by bad pipeline deployments — each took 2-4 hours to resolve because there was no systematic rollback procedure and no way to tell exactly what changed between the last good run and the bad one. Your CTO has made "pipeline deployment safety" a Q1 OKR. Design a comprehensive versioning and rollback system.

<details>
<summary>✅ Solution</summary>

**Architecture: Three-Layer Versioning System**

```
Layer 1: Code versioning (Git — already have this)
Layer 2: Artifact versioning (manifest files, compiled artifacts)
Layer 3: Data versioning (Delta time travel, partition-level rollback)
```

**Layer 2: Artifact Registry for Every Deployment**

Every deployment creates an immutable artifact bundle stored in S3:

```python
# scripts/create_deployment_artifact.py
import json
import boto3
import subprocess
from datetime import datetime

def create_deployment_artifact(pipeline_type: str, pipeline_id: str):
    """Create a versioned artifact bundle for a deployment."""

    git_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()
    git_tag = subprocess.check_output(['git', 'describe', '--tags']).decode().strip()
    deployer = os.environ.get('GITHUB_ACTOR', 'local')
    timestamp = datetime.utcnow().isoformat()

    artifact = {
        'pipeline_id': pipeline_id,
        'pipeline_type': pipeline_type,
        'git_sha': git_sha,
        'git_tag': git_tag,
        'deployed_at': timestamp,
        'deployed_by': deployer,
        'environment': os.environ.get('ENVIRONMENT', 'prod'),
    }

    if pipeline_type == 'dbt':
        # Include dbt manifest for slim CI comparison and rollback
        with open('target/manifest.json') as f:
            artifact['dbt_manifest'] = json.load(f)

    # Store artifact
    s3 = boto3.client('s3')
    artifact_key = f"artifacts/{pipeline_id}/{git_sha}/{timestamp}.json"
    s3.put_object(
        Bucket='pipeline-artifacts',
        Key=artifact_key,
        Body=json.dumps(artifact),
    )

    # Update "current" pointer
    s3.put_object(
        Bucket='pipeline-artifacts',
        Key=f"artifacts/{pipeline_id}/current.json",
        Body=json.dumps(artifact),
    )

    # Register in deployment tracking table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('pipeline-deployments')
    table.put_item(Item={
        'pk': pipeline_id,
        'sk': timestamp,
        'git_sha': git_sha,
        'git_tag': git_tag,
        'deployed_by': deployer,
        'status': 'deployed',
        'artifact_key': artifact_key,
    })

    print(f"Deployment artifact created: {artifact_key}")
    return artifact_key
```

**GitHub Actions: Deployment with Artifact Registration**

```yaml
# .github/workflows/deploy-dbt.yml
jobs:
  deploy:
    steps:
      - name: dbt build
        run: dbt build --target prod

      - name: Register deployment artifact
        run: python scripts/create_deployment_artifact.py dbt analytics-dbt

      - name: Post-deployment validation
        run: |
          # Run critical path tests after deployment
          dbt test --select tag:critical --target prod

          # Compare row counts against yesterday
          python scripts/validate_row_counts.py --tolerance 0.05

      - name: Rollback on validation failure
        if: failure()
        run: python scripts/rollback_pipeline.py --pipeline-id analytics-dbt --auto
```

**The Rollback Script**

```python
# scripts/rollback_pipeline.py
import boto3
import subprocess
import argparse

def rollback_pipeline(pipeline_id: str, target_sha: str = None):
    """
    Roll back a pipeline to a previous version.
    If target_sha not specified, rolls back to the previous deployment.
    """
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('pipeline-deployments')

    # Get deployment history
    response = table.query(
        KeyConditionExpression='pk = :pk',
        ExpressionAttributeValues={':pk': pipeline_id},
        ScanIndexForward=False,  # Newest first
        Limit=10
    )
    deployments = response['Items']

    current = deployments[0]
    if target_sha:
        rollback_target = next(d for d in deployments if d['git_sha'] == target_sha)
    else:
        rollback_target = deployments[1]  # Previous deployment

    print(f"Rolling back {pipeline_id}:")
    print(f"  FROM: {current['git_sha']} (deployed {current['sk']})")
    print(f"  TO:   {rollback_target['git_sha']} (deployed {rollback_target['sk']})")

    # Checkout the rollback commit
    subprocess.run(['git', 'checkout', rollback_target['git_sha']], check=True)

    # Re-run the pipeline with rollback version
    if pipeline_id == 'analytics-dbt':
        subprocess.run(['dbt', 'build', '--target', 'prod', '--full-refresh',
                       '--select', 'state:modified+',
                       '--state', f'/tmp/rollback-manifest/'], check=True)

    # Register the rollback as a new deployment (with rollback flag)
    table.put_item(Item={
        'pk': pipeline_id,
        'sk': datetime.utcnow().isoformat(),
        'git_sha': rollback_target['git_sha'],
        'git_tag': f"rollback-to-{rollback_target['git_sha'][:8]}",
        'deployed_by': 'rollback-automation',
        'status': 'rollback',
        'rolled_back_from': current['git_sha'],
    })

    # Layer 3: Data rollback if needed
    # For Delta tables, time-travel to before the bad deployment
    bad_deployment_time = current['sk']
    print(f"If data is corrupted, run:")
    print(f"  RESTORE TABLE curated.orders TO TIMESTAMP AS OF '{bad_deployment_time}';")
```

**Runbook as Code**

```markdown
# Incident Runbook: Pipeline Deployment Rollback

## Detection
- PagerDuty alert fires for data quality test failure
- Dashboard shows anomalous metrics
- Downstream team reports broken data

## Diagnosis (< 5 minutes)
1. Check deployment history:
   `python scripts/rollback_pipeline.py --pipeline-id <id> --list`
2. Check what changed:
   `git diff <previous-sha> <current-sha> -- models/`
3. Check if data is affected:
   `dbt test --select tag:critical --target prod`

## Rollback (< 15 minutes)
1. Auto-rollback: `python scripts/rollback_pipeline.py --pipeline-id <id>`
2. If data corrupted: `RESTORE TABLE <table> TO TIMESTAMP AS OF '<pre-deployment-time>';`
3. Notify stakeholders: `python scripts/notify_incident.py --pipeline <id> --action rollback`

## Recovery Verification
1. Rerun critical dbt tests
2. Spot-check 3 dashboards
3. Confirm with reporting team

## Post-Incident
- Open post-mortem issue in GitHub
- Add regression test for the root cause
- Update data contract if behavior changed
```

**Metrics to Track (OKR)**

```python
# Track deployment safety metrics
metrics = {
    'mean_time_to_detect': '< 15 minutes',   # Measured from deploy to alert
    'mean_time_to_rollback': '< 20 minutes', # Measured from decision to complete rollback
    'deployment_success_rate': '> 98%',      # % of deployments that don't require rollback
    'data_incident_rate': '< 1 per month',   # Data quality incidents per month
}
```

This system makes the CTO's OKR measurable: MTTD and MTTR for data incidents decrease because every deployment is tracked, every rollback is automated, and every incident has a runbook. The previous 2-4 hour recovery times come down to under 20 minutes.

</details>
</article>
