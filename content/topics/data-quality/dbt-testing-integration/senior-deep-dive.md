---
title: "dbt Testing Integration - Senior Deep Dive"
topic: data-quality
subtopic: dbt-testing-integration
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-quality, dbt, testing, architecture, production-monitoring, senior]
---

# dbt Testing Integration — Senior Deep Dive

## Designing a Tiered Test Strategy

Production dbt deployments need a tiered test strategy that balances thoroughness with runtime cost.

```
Tier 1 — Blocking (run on every build, ~2 min):
  - Primary key uniqueness + not_null
  - Source freshness
  - Referential integrity for critical FK relationships
  - Row count sanity checks (±50% vs yesterday)

Tier 2 — Non-blocking (run post-deploy, ~10 min):
  - Accepted values for all categorical columns
  - Regex format checks (emails, phone numbers)
  - Statistical distribution checks (p95, p99 ranges)

Tier 3 — Audit (run nightly, ~30 min):
  - Cross-domain reconciliation
  - Historical consistency checks
  - Full dbt-expectations suite
```

Implement tiers via tags and separate dbt invocations:

```bash
# Tier 1 in CI gate
dbt build --select tag:tier1

# Tier 2 post-deploy
dbt test --select tag:tier2 --no-fail-fast

# Tier 3 nightly Airflow DAG
dbt test --select tag:tier3
```

---

## Custom Generic Tests

Build reusable generic tests as macros for cross-project consistency.

```sql
-- macros/tests/test_not_null_proportion.sql
-- Fails if null proportion exceeds threshold
{% test not_null_proportion(model, column_name, threshold=0.01) %}

WITH validation AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT({{ column_name }}) AS non_null_rows,
        1.0 - COUNT({{ column_name }}) / NULLIF(COUNT(*), 0) AS null_proportion
    FROM {{ model }}
)
SELECT *
FROM validation
WHERE null_proportion > {{ threshold }}

{% endtest %}
```

```yaml
# Usage
      - name: middle_name
        tests:
          - not_null_proportion:
              threshold: 0.30  # up to 30% nulls acceptable
```

### Row-Count Anomaly Generic Test

```sql
-- macros/tests/test_row_count_within_range.sql
{% test row_count_within_range(model, min_rows, max_rows) %}

WITH cnt AS (SELECT COUNT(*) AS n FROM {{ model }})
SELECT n FROM cnt WHERE n < {{ min_rows }} OR n > {{ max_rows }}

{% endtest %}
```

### Referential Integrity with Conditions

```sql
-- macros/tests/test_conditional_relationships.sql
{% test conditional_relationships(model, column_name, to, field, condition) %}

SELECT a.{{ column_name }}
FROM {{ model }} a
WHERE {{ condition }}
  AND a.{{ column_name }} NOT IN (
      SELECT {{ field }} FROM {{ to }}
  )

{% endtest %}
```

---

## dbt + Great Expectations Integration

For teams that already run GX, dbt tests can trigger GX validations via an Airflow operator or a dbt hook.

### Pattern 1: on-run-end hook

```yaml
# dbt_project.yml
on-run-end:
  - "{{ run_gx_validations() }}"
```

```sql
-- macros/run_gx_validations.sql
{% macro run_gx_validations() %}
  {% if execute %}
    {% set py_result = run_query("SELECT 1") %}  {# dummy to ensure we're in execute mode #}
    {{ log("Triggering GX via API", info=True) }}
  {% endif %}
{% endmacro %}
```

### Pattern 2: Airflow orchestration

```python
# dags/dbt_with_gx.py
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import pendulum

@dag(schedule="0 6 * * *", start_date=pendulum.datetime(2024, 1, 1))
def dbt_with_quality_gate():

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run",
        image="dbt-runner:latest",
        cmds=["dbt", "run", "--select", "marts.*"],
    )

    dbt_test_tier1 = KubernetesPodOperator(
        task_id="dbt_test_tier1",
        image="dbt-runner:latest",
        cmds=["dbt", "test", "--select", "tag:tier1"],
    )

    gx_validate = KubernetesPodOperator(
        task_id="gx_validate",
        image="gx-runner:latest",
        cmds=["python", "run_gx_suite.py", "--suite", "marts_suite"],
    )

    dbt_test_tier2 = KubernetesPodOperator(
        task_id="dbt_test_tier2",
        image="dbt-runner:latest",
        cmds=["dbt", "test", "--select", "tag:tier2"],
        trigger_rule="all_done",  # run even if tier1 had warnings
    )

    dbt_run >> dbt_test_tier1 >> gx_validate >> dbt_test_tier2
```

---

## Production Test Monitoring Patterns

### Persisting Test Results Over Time

dbt doesn't natively track test history. Build a meta-model to track results:

```sql
-- models/monitoring/dbt_test_history.sql
-- Reads from run_results.json parsed by a loader script

SELECT
    run_id,
    invocation_id,
    test_name,
    model_name,
    status,                    -- 'pass', 'fail', 'warn', 'error'
    failures,                  -- number of failing rows
    execution_time,
    run_started_at,
    compiled_code
FROM {{ source('dbt_artifacts', 'test_results') }}
```

```python
# scripts/load_test_results.py
"""Parse dbt run_results.json and load to warehouse."""
import json
import uuid
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

def load_run_results(results_path: str, run_id: str):
    with open(results_path) as f:
        data = json.load(f)

    rows = []
    for r in data["results"]:
        if not r["unique_id"].startswith("test."):
            continue
        rows.append({
            "run_id": run_id,
            "invocation_id": data["metadata"]["invocation_id"],
            "test_name": r["unique_id"],
            "model_name": r["unique_id"].split(".")[2],
            "status": r["status"],
            "failures": r.get("failures", 0),
            "execution_time": r["execution_time"],
            "run_started_at": data["metadata"]["generated_at"],
        })

    df = pd.DataFrame(rows)
    client = bigquery.Client()
    client.load_table_from_dataframe(
        df, "project.monitoring.dbt_test_results"
    ).result()
```

---

## Alert Routing: PagerDuty vs Slack

```python
# scripts/alert_on_test_failures.py
import json
import requests
from enum import Enum

class Severity(Enum):
    CRITICAL = "critical"
    WARNING = "warning"

CRITICAL_MODELS = {"fct_payments", "fct_orders", "dim_customers"}

def route_alert(test_name: str, failures: int, status: str):
    model = test_name.split(".")[2]
    
    if status == "error" or (status == "fail" and model in CRITICAL_MODELS):
        severity = Severity.CRITICAL
        notify_pagerduty(test_name, failures, model)
    else:
        severity = Severity.WARNING
        notify_slack(test_name, failures, model)

def notify_pagerduty(test_name: str, failures: int, model: str):
    payload = {
        "routing_key": PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "payload": {
            "summary": f"dbt test FAILED: {test_name} ({failures} rows)",
            "severity": "critical",
            "source": "dbt-pipeline",
            "custom_details": {
                "model": model,
                "failing_rows": failures,
                "runbook": f"https://wiki.company.com/runbooks/dbt/{model}",
            }
        }
    }
    requests.post("https://events.pagerduty.com/v2/enqueue", json=payload)

def notify_slack(test_name: str, failures: int, model: str):
    requests.post(SLACK_WEBHOOK, json={
        "text": f":warning: dbt test warning: `{test_name}` — {failures} failing rows in `{model}`"
    })
```

---

## store_failures_as and Failure Analysis

With dbt 1.7+, use `store_failures_as` to choose between table and view:

```yaml
# dbt_project.yml
tests:
  my_project:
    +store_failures: true
    +store_failures_as: view    # cheaper for large tables
    staging:
      +store_failures_as: table # persist for audit trail
    marts:
      +store_failures_as: table
      +schema: dbt_failures_mart
```

Query failures for RCA:

```sql
-- How many failures per test over last 7 days?
SELECT
    test_name,
    DATE(run_started_at) AS run_date,
    SUM(failures) AS total_failures
FROM monitoring.dbt_test_results
WHERE run_started_at >= CURRENT_DATE - 7
  AND status = 'fail'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Which customer_ids are repeatedly failing the not_null check?
SELECT *
FROM dbt_failures_mart.not_null_orders_customer_id
ORDER BY _dbt_source_relation, order_id;
```

---

## Key Interview Points

- Design tiered tests (blocking Tier 1 in CI, non-blocking Tier 2 post-deploy, audit Tier 3 nightly)
- Custom generic tests are macros in `macros/tests/` — reusable across models via YAML
- Persist test results by parsing `target/run_results.json` and loading to a monitoring table
- Route alerts: PagerDuty for critical model failures, Slack for warnings
- `store_failures_as: table` (not view) for models requiring audit trails
- dbt + GX integration via Airflow: run dbt build → GX validate → dbt test tier2 in sequence
- Unit tests (dbt 1.8) test SQL transformation logic in isolation without warehouse data
