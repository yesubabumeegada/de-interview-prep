---
title: "dbt Testing Integration - Scenario Questions"
topic: data-quality
subtopic: dbt-testing-integration
content_type: scenario_question
tags: [data-quality, dbt, testing, scenarios, interview]
---

# dbt Testing Integration — Scenario Questions

<article data-difficulty="junior">

## Scenario (Junior): Adding Tests to an Untested dbt Project

You join a company where the dbt project has 40 models and zero tests. Your manager asks you to add data quality tests to the `fct_orders` model as a starting point. The model has columns: `order_id`, `customer_id`, `status`, `total_amount`, `created_at`.

**Question:** What tests would you add, and how would you configure them in YAML?

<details>
<summary>✅ Solution</summary>

Start with the four built-in generic tests covering the most critical quality properties:

```yaml
# models/schema.yml
models:
  - name: fct_orders
    columns:
      - name: order_id
        description: "Primary key for orders"
        tests:
          - unique
          - not_null

      - name: customer_id
        description: "FK to dim_customers"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'processing', 'shipped', 'delivered', 'cancelled']

      - name: total_amount
        tests:
          - not_null

      - name: created_at
        tests:
          - not_null
```

**Run the tests:**
```bash
dbt test --select fct_orders
```

**Prioritization rationale:**
- `order_id`: unique + not_null enforces primary key integrity — most critical
- `customer_id`: referential integrity ensures no orphaned orders
- `status`: accepted_values catches bad source data early
- `total_amount` + `created_at`: not_null prevents silent NULL propagation into reports

**Next steps after this baseline:**
1. Add `dbt-expectations` for range checks on `total_amount`
2. Add a singular test for business rules (e.g., cancelled orders should have 0 revenue)
3. Enable `store_failures: true` so failures are debuggable
</details>

</article>

---

<article data-difficulty="mid">

## Scenario (Mid-Level): Flaky Tests in CI Blocking Deployments

Your team runs `dbt test` in a GitHub Actions CI pipeline. Developers are complaining that the pipeline fails several times per week due to tests that "sometimes fail." Investigation shows the `accepted_values` test on an `event_type` column fails whenever the source team adds a new event type. Another test — a row count check — fails on Monday mornings because the weekend has lower order volume.

**Question:** How do you fix these flaky tests without removing quality coverage?

<details>
<summary>✅ Solution</summary>

**Problem 1: accepted_values fails on new event types**

The `accepted_values` list is hardcoded and the source team doesn't coordinate new types with you. Two options:

Option A — Move the check to a warning:
```yaml
      - name: event_type
        tests:
          - accepted_values:
              values: ['click', 'view', 'purchase', 'refund']
              config:
                severity: warn  # logs failure but doesn't break CI
```

Option B — Source the allowed values from a reference table (more maintainable):
```sql
-- tests/assert_valid_event_types.sql
SELECT DISTINCT e.event_type
FROM {{ ref('fct_events') }} e
LEFT JOIN {{ ref('dim_event_types') }} d ON e.event_type = d.event_type
WHERE d.event_type IS NULL
  AND e.event_type IS NOT NULL
```

This way, the source team adds new types to `dim_event_types`, and the test automatically passes.

**Problem 2: row count fails on low-volume weekends**

```yaml
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 100       # too low to be useful, just prevents empty table
          max_value: 10000000
          config:
            where: "DATE(created_at) = CURRENT_DATE - 1"  # only check yesterday
```

Or use a day-of-week aware singular test:
```sql
-- tests/assert_daily_orders_volume.sql
WITH daily_stats AS (
    SELECT
        DATE(created_at) AS order_date,
        DAYOFWEEK(created_at) AS dow,  -- 1=Sun, 7=Sat
        COUNT(*) AS order_count
    FROM {{ ref('fct_orders') }}
    WHERE DATE(created_at) = CURRENT_DATE - 1
    GROUP BY 1, 2
),
thresholds AS (
    SELECT
        order_date,
        CASE WHEN dow IN (1, 7) THEN 500 ELSE 2000 END AS min_expected
    FROM daily_stats
)
SELECT d.*
FROM daily_stats d
JOIN thresholds t ON d.order_date = t.order_date
WHERE d.order_count < t.min_expected
```

**General principle:** flaky tests usually mean the test is testing environment/timing rather than data quality. Fix by: (1) using `severity: warn` for non-critical checks, (2) making thresholds context-aware, or (3) moving static lists to reference tables.
</details>

</article>

---

<article data-difficulty="senior">

## Scenario (Senior): Designing a dbt Test Framework for a Regulated Industry

You are the lead data engineer at a fintech company preparing for a SOX audit. The auditors require evidence that: (1) financial data transformations are tested, (2) test results are retained for 7 years, (3) any test failure on financial models triggers an escalation within 30 minutes, and (4) test coverage can be reported on per-model.

**Question:** Design the end-to-end dbt testing architecture to meet these requirements.

<details>
<summary>✅ Solution</summary>

### Architecture Overview

```
Source Data → dbt build → Test Execution → Result Capture → Alert Routing → Audit Storage
                                                ↓
                                        BigQuery: monitoring.dbt_test_results
                                        (7-year retention policy applied)
```

### 1. Test Configuration for Financial Models

```yaml
# models/marts/finance/schema.yml
models:
  - name: fct_revenue
    config:
      tags: ['sox', 'finance', 'tier1']
      meta:
        owner: "finance-data@company.com"
        sla: "99.9%"
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 500000000
          tags: ['sox', 'tier1']
    columns:
      - name: transaction_id
        tests:
          - unique:
              config:
                store_failures: true
                store_failures_as: table
              tags: ['sox', 'tier1']
          - not_null:
              tags: ['sox', 'tier1']
      - name: amount_usd
        tests:
          - not_null:
              tags: ['sox', 'tier1']
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -1000000
              max_value: 10000000
              tags: ['sox', 'tier1']
```

### 2. Result Capture with 7-Year Retention

```python
# scripts/capture_test_results.py
"""Run after every dbt test invocation."""
import json, uuid
from datetime import datetime
from google.cloud import bigquery

def capture_results(run_results_path: str):
    with open(run_results_path) as f:
        data = json.load(f)
    
    rows = []
    for r in data["results"]:
        if not r["unique_id"].startswith("test."):
            continue
        rows.append({
            "capture_id": str(uuid.uuid4()),
            "invocation_id": data["metadata"]["invocation_id"],
            "run_started_at": data["metadata"]["generated_at"],
            "test_unique_id": r["unique_id"],
            "test_name": r["unique_id"].split(".")[-1],
            "model_name": r["unique_id"].split(".")[2],
            "status": r["status"],
            "failures": r.get("failures", 0),
            "execution_time_seconds": r["execution_time"],
            "compiled_sql": r.get("compiled", ""),
            "is_sox_tagged": "sox" in r.get("tags", []),
        })
    
    client = bigquery.Client(project="analytics-prod")
    errors = client.insert_rows_json(
        "analytics-prod.monitoring.dbt_test_results",
        rows
    )
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

# Table has partition expiration disabled and lifecycle policy: 7 years
# Set via: bq update --time_partitioning_expiration 0 analytics-prod:monitoring.dbt_test_results
```

### 3. Alert Routing (30-minute SLA for SOX failures)

```python
# scripts/sox_alert_router.py
import json
import requests
from datetime import datetime, timezone

SOX_MODELS = {"fct_revenue", "fct_transactions", "fct_reconciliation"}

def route_sox_alerts(run_results_path: str):
    with open(run_results_path) as f:
        data = json.load(f)
    
    sox_failures = []
    for r in data["results"]:
        if r["status"] not in ("fail", "error"):
            continue
        model = r["unique_id"].split(".")[2]
        tags = r.get("tags", [])
        if "sox" in tags or model in SOX_MODELS:
            sox_failures.append(r)
    
    if sox_failures:
        # PagerDuty: 30-minute escalation policy configured on the service
        for failure in sox_failures:
            trigger_pagerduty_incident(
                summary=f"SOX DATA TEST FAILURE: {failure['unique_id']}",
                severity="critical",
                details={
                    "failing_rows": failure.get("failures", "unknown"),
                    "model": failure["unique_id"].split(".")[2],
                    "runbook": "https://wiki/runbooks/sox-test-failure",
                    "audit_query": (
                        "SELECT * FROM monitoring.dbt_test_results "
                        f"WHERE invocation_id = '{data['metadata']['invocation_id']}'"
                    ),
                }
            )

def trigger_pagerduty_incident(summary, severity, details):
    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json={
            "routing_key": PAGERDUTY_SOX_KEY,
            "event_action": "trigger",
            "payload": {
                "summary": summary,
                "severity": severity,
                "source": "dbt-sox-monitor",
                "custom_details": details,
            }
        }
    )
```

### 4. Test Coverage Reporting for Auditors

```sql
-- models/monitoring/sox_test_coverage.sql
WITH models AS (
    SELECT DISTINCT
        node_id,
        name AS model_name,
        'sox' IN ARRAY(tags) AS is_sox_tagged
    FROM {{ source('dbt_artifacts', 'nodes') }}
    WHERE resource_type = 'model'
),
tested_models AS (
    SELECT DISTINCT
        SPLIT(test_unique_id, '.')[SAFE_OFFSET(2)] AS model_name,
        COUNT(*) AS test_count
    FROM {{ source('monitoring', 'dbt_test_results') }}
    WHERE DATE(run_started_at) >= CURRENT_DATE - 30
    GROUP BY 1
)
SELECT
    m.model_name,
    m.is_sox_tagged,
    COALESCE(t.test_count, 0) AS tests_in_last_30_days,
    CASE WHEN t.model_name IS NOT NULL THEN 'covered' ELSE 'uncovered' END AS coverage_status
FROM models m
LEFT JOIN tested_models t ON m.model_name = t.model_name
ORDER BY m.is_sox_tagged DESC, coverage_status, m.model_name
```

This report gives auditors a model-level view of test coverage — provable evidence that every SOX-tagged model has been tested.
</details>

</article>
