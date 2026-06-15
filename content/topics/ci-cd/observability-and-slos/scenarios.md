---
title: "Observability and SLOs for Data Pipelines - Scenario Questions"
topic: ci-cd
subtopic: observability-and-slos
content_type: scenario_question
tags: [ci-cd, observability, slo, sla, error-budget, burn-rate, data-freshness, incident-response, monitoring]
---

# Observability and SLOs for Data Pipelines — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Setting Up Your First Data Freshness Check

You've just joined a company as a junior data engineer. The team's dbt project has no monitoring. Business users frequently Slack the data team asking "is the data up to date?" and there's no good answer. Your tech lead asks you to add basic freshness monitoring to the 5 most important source tables in dbt. The tables are: `raw.orders`, `raw.customers`, `raw.events`, `raw.inventory`, and `raw.payments`. All have a column called `_loaded_at` that is populated by the ingestion pipeline.

**Task:** Write the dbt YAML configuration to add freshness checks with appropriate thresholds, and explain how you'd run it in CI.

<details>
<summary>✅ Solution</summary>

```yaml
# models/staging/schema.yml
version: 2

sources:
  - name: raw
    database: RAW_DB
    schema: PUBLIC
    description: "Raw ingestion tables from operational systems"

    tables:
      # High-frequency, business-critical: tight freshness window
      - name: orders
        description: "Order transactions from checkout system"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 4, period: hour}    # Warn if > 4 hours stale
          error_after: {count: 8, period: hour}   # Error if > 8 hours stale
        columns:
          - name: order_id
            tests:
              - not_null
              - unique

      - name: payments
        description: "Payment transactions — same criticality as orders"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 4, period: hour}
          error_after: {count: 8, period: hour}

      # Medium-frequency: daily batch loads expected
      - name: customers
        description: "Customer master data — updated nightly"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 25, period: hour}   # Warn if > 25 hours (daily + buffer)
          error_after: {count: 49, period: hour}  # Error if > 49 hours (2 days)

      - name: inventory
        description: "Inventory levels — hourly batch updates"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 6, period: hour}

      # Lower frequency: event data can tolerate more lag
      - name: events
        description: "User behavior events — near-real-time with some lag acceptable"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
```

**Running freshness checks:**

```bash
# Run freshness checks only (fast — no transformation)
dbt source freshness

# Sample output:
# 10:02:01 | Found 5 sources, 5 tables.
# 10:02:04 | raw.orders ................ [pass in 0.8s]
# 10:02:05 | raw.payments .............. [pass in 0.6s]
# 10:02:06 | raw.customers ............. [warn in 0.7s] — data is 26 hours old
# 10:02:07 | raw.inventory ............. [error in 0.9s] — data is 7 hours old
# 10:02:08 | raw.events ................ [pass in 0.7s]
# 10:02:08 | Completed with 1 error and 1 warning.
```

**Adding to CI/CD pipeline:**

```yaml
# .github/workflows/dbt-ci.yml — add freshness check before dbt run
jobs:
  dbt-pipeline:
    steps:
      - name: Check source freshness
        run: dbt source freshness
        # This step FAILS if any source is in "error" state
        # This prevents running transformations on stale source data

      - name: dbt build
        run: dbt build
        # Only runs if source freshness passes

      - name: dbt test
        run: dbt test
```

**Adding to daily production Airflow DAG:**

```python
from airflow.operators.bash import BashOperator

check_freshness = BashOperator(
    task_id='check_source_freshness',
    bash_command='dbt source freshness --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt build --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag,
)

# Freshness check must pass before running transformations
check_freshness >> dbt_run
```

**How to choose thresholds:** Look at historical load patterns. If `raw.inventory` always loads within 30 minutes, a warn_after of 2 hours gives a 4x safety buffer. If loads are less regular, use a wider threshold. The goal is: warn threshold catches real problems before they breach the SLA, but not so tight that it fires on normal variance.

</details>
</article>

<article data-difficulty="mid">

## Scenario 2: Designing SLOs for a New Data Platform

You're a mid-level data engineer at a company launching a new data platform for the finance team. The finance team has given you these requirements: "We need revenue data ready by 8am every business day for the morning standup. Customer lifetime value scores should be updated weekly by Monday 9am. Our external reporting to regulators requires transaction data to be available within 24 hours of the transaction." Design three SLOs covering these requirements, calculate the error budgets, and specify what alerts you'd create for each.

<details>
<summary>✅ Solution</summary>

**SLO 1: Daily Revenue Freshness**

```yaml
slo_name: daily_revenue_freshness
sli: "Percentage of business days where fct_revenue is fully loaded by 7:45am UTC"
# Note: 7:45am internal target vs 8am SLA gives a 15-minute buffer

target: 99.5%  # Internal SLO (stricter than SLA)
sla: 99%        # External SLA commitment to finance team
window: rolling 30 business days

error_budget_calculation:
  # ~22 business days per month × 30-day window ≈ 21.7 business days
  business_days_in_window: 21.7
  allowed_misses_at_995_pct: 21.7 × 0.005 ≈ 0.11  # < 1 miss per month!
  # At 99.5% SLO on business days only: < 1 miss per month budget

alerts:
  critical: "fct_revenue not loaded by 7:45am UTC on any business day — page immediately"
  warning: "Pipeline running slow — ETA suggests missing 7:45am target"

measurement:
  query: |
    SELECT
      DATE(completed_at) as run_date,
      CASE WHEN TIME(completed_at) <= '07:45:00' THEN 1 ELSE 0 END as slo_met
    FROM airflow_metadata.dag_run
    WHERE dag_id = 'revenue_pipeline'
      AND state = 'success'
      AND DAYOFWEEK(completed_at) NOT IN (1, 7)  -- Exclude weekends
    ORDER BY run_date DESC
```

**SLO 2: Weekly CLV Score Availability**

```yaml
slo_name: weekly_clv_freshness
sli: "Percentage of Mondays where clv_scores table is fully updated by 9am UTC"
target: 99%  # Weekly SLO — slightly more lenient; manual recovery possible
window: rolling 12 weeks (3 months)

error_budget_calculation:
  weeks_in_window: 12
  allowed_misses: 12 × 0.01 = 0.12  # < 1 miss per quarter budget

alerts:
  critical: "CLV scores not loaded by 9am Monday — alert data science team"
  # No SLA paging needed — finance standup is daily revenue, not CLV
  # Route to Slack #data-alerts, not PagerDuty
```

**SLO 3: Regulatory Transaction Data (24-hour SLA)**

```yaml
slo_name: transaction_regulatory_freshness
sli: "Percentage of 5-minute measurement windows where all transactions from 24h ago appear in regulatory_transactions table"
# Measure continuously, not just at end of day — regulators can query anytime

target: 99.9%  # Highest bar — regulatory breach has legal consequences
window: rolling 30 days

error_budget_calculation:
  total_5min_windows_in_30_days: 30 × 24 × 12 = 8,640 windows
  allowed_violations_at_999: 8,640 × 0.001 = 8.6 ≈ 8 windows (40 minutes of lag)
  # Only 40 minutes of budget per month — this is extremely strict

alerts:
  critical:
    condition: "Any transaction older than 23 hours not yet in regulatory_transactions"
    action: "PAGE data engineering AND compliance team immediately"
    # 1-hour buffer before SLA breach → 23-hour alert threshold

  warning:
    condition: "Transaction lag exceeds 20 hours (burn rate accelerating)"
    action: "Slack #data-alerts + ticket"
```

**Implementing the Burn Rate Alert for Regulatory SLO:**

```sql
-- Check hourly: what's the current transaction lag?
SELECT
    DATEDIFF('minute',
        MIN(transaction_timestamp),  -- Oldest unprocessed transaction
        CURRENT_TIMESTAMP()
    ) as lag_minutes,
    CASE
        WHEN DATEDIFF('hour', MIN(transaction_timestamp), CURRENT_TIMESTAMP()) > 23
        THEN 'CRITICAL_PAGE'
        WHEN DATEDIFF('hour', MIN(transaction_timestamp), CURRENT_TIMESTAMP()) > 20
        THEN 'WARNING_TICKET'
        ELSE 'OK'
    END as alert_level
FROM source.transactions
WHERE transaction_id NOT IN (
    SELECT source_transaction_id FROM regulatory_transactions
)
AND transaction_timestamp >= DATEADD('day', -2, CURRENT_TIMESTAMP());
```

**SLO Summary Card (for leadership/stakeholders):**

| SLO | Target | Error Budget/Month | Business Impact of Breach |
|-----|--------|--------------------|--------------------------|
| Daily Revenue | 99.5% (< 1 miss/month) | ~0.1 business days | Finance standup delayed |
| Weekly CLV | 99% (< 1 miss/quarter) | ~0.1 weeks | Analytics team blocked |
| Regulatory | 99.9% (40 min/month) | 40 minutes | Legal/compliance risk |

</details>
</article>

<article data-difficulty="senior">

## Scenario 3: Diagnosing a Production Data Quality Incident

You're the senior data engineer on call. At 9:15am Monday, you receive a PagerDuty page: "CRITICAL: DataFreshnessFastBurn — orders table burn rate 18x, SLA breach in < 90 minutes." The SLA is that the orders dashboard must show current data by 10am for the weekly business review. It is currently 9:15am. Walk through your complete incident response, including diagnosis steps, stakeholder communication, and the fix.

<details>
<summary>✅ Solution</summary>

**T+0 (9:15am): Acknowledge and assess**

```bash
# Don't panic. Run the diagnosis queries immediately.

# 1. What's the current freshness?
-- Snowflake
SELECT
    MAX(order_created_at) as latest_order,
    DATEDIFF('minute', MAX(order_created_at), CURRENT_TIMESTAMP()) as minutes_stale,
    CURRENT_TIMESTAMP() as checked_at
FROM analytics.fct_orders;
-- Result: latest order 6 hours ago (since 3:15am), 360 minutes stale
-- Status: BAD — should be < 60 minutes stale (updated hourly)
```

**T+2 (9:17am): Check the pipeline**

```bash
# 2. Check Airflow for what's happening
# (Use Airflow REST API or UI)
curl -X GET "http://airflow:8080/api/v1/dags/orders_etl/dagRuns?limit=5&order_by=-start_date" \
  -H "Authorization: Basic $(echo -n 'admin:admin' | base64)"

# Result: Last successful run was at 3:00am. 6:00am, 7:00am, 8:00am runs all show "failed"
# This is why the data is 6 hours stale — 3 consecutive failures

# 3. Check the failed task logs
curl -X GET "http://airflow:8080/api/v1/dags/orders_etl/dagRuns/scheduled__2024-01-22T06:00:00+00:00/taskInstances/load_to_snowflake/logs/1"
# Error: SnowflakeError: Warehouse 'ORDERS_ETL_WH' is suspended and cannot be resumed
# Root cause identified: Snowflake warehouse auto-suspend is failing to auto-resume
```

**T+5 (9:20am): Stakeholder communication (BEFORE the fix)**

```
Slack message to #data-alerts:
🚨 *[DATA INCIDENT - ACTIVE]* Orders pipeline failure — investigating

*Impact:* Orders dashboard data is 6 hours stale (last update 3:15am)
*SLA at risk:* 10am business review dashboard
*Root cause:* Snowflake warehouse suspension issue — investigating fix
*ETA:* Working now. Update in 15 minutes with resolution ETA.

CC: @business-analytics-lead @finance-vp
```

**T+5-15 (9:20-9:30am): Fix the root cause**

```sql
-- Immediate fix 1: Manually resume the warehouse
ALTER WAREHOUSE ORDERS_ETL_WH RESUME;

-- Verify it resumed
SHOW WAREHOUSES LIKE 'ORDERS_ETL_WH';
-- Status should show: STARTED

-- Root cause investigation: WHY did auto-resume fail?
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'ORDERS_ETL_WH'
  AND START_TIME >= DATEADD('hour', -8, CURRENT_TIMESTAMP())
  AND ERROR_CODE IS NOT NULL
ORDER BY START_TIME DESC
LIMIT 20;
-- Finding: SnowflakeError 003001: "Your account has been suspended for non-payment"
-- The company's Snowflake invoice wasn't paid! Account was suspended, warehouse wouldn't resume.
```

**T+15 (9:30am): Update stakeholders and implement recovery**

```
Slack update:
🔧 Root cause identified: Snowflake account suspension (billing issue).
Warehouse manually resumed. Pipeline rerunning now.

Pipeline catchup plan:
- 9:30am: Trigger manual pipeline run for 6am, 7am, 8am, 9am data
- Est. completion: 9:55am (5 min per run × 4 runs = 20 min)
- Dashboard will be current by 9:58am ✅

ACTION REQUIRED: @finance-team — please process the Snowflake invoice immediately.
We need billing resolved to prevent recurrence before tomorrow's pipelines.
```

```python
# Trigger catchup runs manually via Airflow API
import requests
from datetime import datetime, timezone

airflow_base = "http://airflow:8080/api/v1"
headers = {"Authorization": "Basic ...", "Content-Type": "application/json"}

# Backfill the missed runs
missed_runs = [
    "2024-01-22T06:00:00+00:00",
    "2024-01-22T07:00:00+00:00",
    "2024-01-22T08:00:00+00:00",
    "2024-01-22T09:00:00+00:00",
]

for logical_date in missed_runs:
    response = requests.post(
        f"{airflow_base}/dags/orders_etl/dagRuns",
        json={
            "logical_date": logical_date,
            "conf": {"catchup_run": True}
        },
        headers=headers
    )
    print(f"Triggered: {logical_date} → {response.status_code}")
```

**T+45 (9:58am): Resolution**

```
Slack update (9:58am):
✅ *[RESOLVED]* Orders pipeline recovered. Dashboard showing current data as of 9:55am.
SLA met: 2 minutes before 10am deadline.

*Root cause:* Snowflake account billing suspension prevented warehouse from auto-resuming.
*Fix:* Warehouse manually resumed; 4 catchup runs completed.
*Billing:* Invoice submitted to AP for urgent payment (Snowflake support ticket #ABC123 opened for 48h grace period).
*Prevention (GitHub issue #891):*
1. Add Snowflake account status check to daily monitoring
2. Set up billing alerts via Snowflake admin notifications
3. Add non-payment suspension to incident runbook
```

**Post-incident: Add the missing detection**

```python
# New monitoring check: detect Snowflake account suspension BEFORE pipelines fail
def check_snowflake_account_health():
    """Daily check that Snowflake account is in good standing."""
    conn = snowflake.connector.connect(**get_creds())
    cursor = conn.cursor()

    try:
        # This query will fail if account is suspended
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        cursor.execute("ALTER WAREHOUSE ORDERS_ETL_WH RESUME IF SUSPENDED")
        print("Snowflake account healthy ✅")

    except snowflake.connector.errors.ProgrammingError as e:
        if 'suspended' in str(e).lower():
            # Alert IMMEDIATELY — don't wait for pipeline failure
            trigger_pagerduty_incident(
                title="CRITICAL: Snowflake account suspended",
                body=f"Account suspension will cause all pipelines to fail. Error: {e}",
                severity='critical'
            )
        raise
```

**Incident metrics:**
- MTTD: 0 minutes (burn rate alert fired proactively)
- MTTR: 43 minutes (9:15am page → 9:58am resolution)
- SLA breach: No (met 10am SLA by 2 minutes)
- Error budget consumed: ~45 minutes of the 216-minute monthly budget (21%)

**Key takeaway for interview:** The best incident response is fast communication, systematic diagnosis (not random guessing), and stakeholder updates BEFORE the fix. The root cause here (billing suspension) is unusual but real — it illustrates why monitoring infrastructure health, not just pipeline metrics, is essential.

</details>
</article>
