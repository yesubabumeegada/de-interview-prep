---
title: "dbt Testing Integration - Real World"
topic: data-quality
subtopic: dbt-testing-integration
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [data-quality, dbt, testing, production, real-world, case-study]
---

# dbt Testing Integration — Real World

## Case Study: E-Commerce Platform Data Quality Program

### Context

A mid-size e-commerce company runs ~300 dbt models transforming data from Shopify, Stripe, and a custom WMS into a BigQuery data warehouse. The analytics engineering team of 5 needed to:

- Catch data quality issues before finance reports ran at 8am
- Avoid blocking all deployments for non-critical failures
- Give downstream consumers confidence in data freshness

### Before: Manual Checks, No Trust

- Analysts ran ad-hoc SQL to verify numbers before presentations
- Finance team kept their own Excel spreadsheet to "double-check" revenue figures
- No alerting — issues discovered when a dashboard showed wrong numbers

### After: Tiered dbt Test Framework

**Tier 1 — Blocking CI Gate (2 minutes)**

```yaml
# Enforced on every PR, blocks merge if failing
models:
  - name: fct_orders
    config:
      tags: ['tier1', 'critical', 'finance']
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 100000000
          tags: ['tier1']
    columns:
      - name: order_id
        tests:
          - unique:
              tags: ['tier1']
          - not_null:
              tags: ['tier1']
      - name: customer_id
        tests:
          - not_null:
              tags: ['tier1']
          - relationships:
              to: ref('dim_customers')
              field: customer_id
              tags: ['tier1']
      - name: gross_revenue
        tests:
          - not_null:
              tags: ['tier1']
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
              tags: ['tier1']
```

**Tier 2 — Post-Deploy (8 minutes)**

```yaml
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
              tags: ['tier2']
      - name: customer_email
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[^@]+@[^@]+\\.[^@]+$"
              tags: ['tier2']
              config:
                severity: warn
```

---

## Real Production Issue: The Duplicate Order Bug

In production, a source system bug caused duplicate order records to be emitted during a Shopify webhook retry storm. The issue propagated silently into the warehouse for 6 hours.

### How Tests Caught It

The `unique` test on `order_id` in Tier 1 started failing:

```
Failure in test unique_fct_orders_order_id (models/marts/fct_orders.yml)
  Got 847 results, configured to fail if != 0
  
  compiled SQL:
  select order_id, count(*) as n
  from analytics.fct_orders
  group by order_id
  having count(*) > 1
```

### Debugging with store_failures

```sql
-- Query the stored failure table
SELECT *
FROM dbt_test_failures.unique_fct_orders_order_id
ORDER BY order_id
LIMIT 20;

-- Result: all duplicates had order_ids between 85000 and 87000
-- created_at timestamps showed two copies ~3 seconds apart
-- This matched the webhook retry window exactly
```

### Fix Applied

```sql
-- models/staging/stg_shopify_orders.sql
-- Added deduplication logic after identifying the root cause

WITH source AS (
    SELECT * FROM {{ source('shopify', 'orders') }}
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM source
)
SELECT * EXCEPT(rn)
FROM deduped
WHERE rn = 1
```

The test now passes, and a comment was added explaining the dedup rationale.

---

## Source Freshness in Production

### Payment Pipeline with Strict SLA

The payments team required revenue figures to be no older than 30 minutes by 7:30am.

```yaml
# models/sources.yml
sources:
  - name: stripe_raw
    database: raw
    schema: stripe
    freshness:
      warn_after: {count: 20, period: minute}
      error_after: {count: 40, period: minute}
    loaded_at_field: _fivetran_synced
    tables:
      - name: charges
        description: "Raw Stripe charge events"
        freshness:
          warn_after: {count: 15, period: minute}
          error_after: {count: 30, period: minute}
```

Integrated into Airflow DAG:

```python
# dags/morning_finance_pipeline.py
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

source_freshness = KubernetesPodOperator(
    task_id="source_freshness_check",
    image="dbt-runner:1.8",
    cmds=["dbt", "source", "freshness", "--output", "json"],
    on_failure_callback=alert_finance_slack,
)

dbt_build = KubernetesPodOperator(
    task_id="dbt_build_finance",
    image="dbt-runner:1.8",
    cmds=["dbt", "build", "--select", "tag:finance"],
)

source_freshness >> dbt_build
```

When freshness failed at 6:45am, the Slack alert fired before analysts arrived, and the data engineering on-call investigated the Fivetran sync.

---

## Integrating with Slack for Daily Quality Reports

```python
# scripts/daily_quality_report.py
"""Post a daily quality summary to #data-quality Slack channel."""
import json
from pathlib import Path
import requests
from collections import Counter

def build_daily_report(results_path: str) -> dict:
    with open(results_path) as f:
        data = json.load(f)
    
    test_results = [r for r in data["results"] if "test." in r["unique_id"]]
    status_counts = Counter(r["status"] for r in test_results)
    
    failures = [
        r for r in test_results
        if r["status"] in ("fail", "error")
    ]
    
    return {
        "total": len(test_results),
        "passed": status_counts.get("pass", 0),
        "warned": status_counts.get("warn", 0),
        "failed": status_counts.get("fail", 0) + status_counts.get("error", 0),
        "failure_details": [
            {
                "name": r["unique_id"].split(".")[-1],
                "rows": r.get("failures", "?"),
            }
            for r in failures[:5]
        ],
    }

def post_to_slack(report: dict):
    emoji = ":white_check_mark:" if report["failed"] == 0 else ":red_circle:"
    text = (
        f"{emoji} *Daily dbt Quality Report*\n"
        f"Passed: {report['passed']} | Warned: {report['warned']} | Failed: {report['failed']}\n"
    )
    if report["failure_details"]:
        text += "\n*Failures:*\n"
        for f in report["failure_details"]:
            text += f"  • `{f['name']}` — {f['rows']} failing rows\n"
    
    requests.post(SLACK_WEBHOOK_URL, json={"text": text})

report = build_daily_report("target/run_results.json")
post_to_slack(report)
```

---

## Lessons Learned

1. **Start with Tier 1 only.** Teams that add hundreds of tests day one create flaky gates that developers learn to ignore. Build trust with 10–15 rock-solid blocking tests, then expand.

2. **`store_failures: true` is non-negotiable in production.** Without stored failure rows, debugging means re-running the test manually with added logging — costly at 3am.

3. **Test names are your error messages.** dbt auto-generates test names like `not_null_fct_orders_gross_revenue`. When this appears in PagerDuty, engineers immediately know what broke.

4. **Freshness failures are usually pipeline failures, not data quality failures.** Route them to the pipeline on-call, not the data quality team.

5. **Unit tests (dbt 1.8) replaced a class of hard-to-test singular tests.** Testing a complex status mapping previously required seeding data and running the full model — now it's 10 lines of YAML.
