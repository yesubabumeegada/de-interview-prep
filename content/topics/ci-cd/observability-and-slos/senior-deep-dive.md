---
title: "Observability and SLOs for Data Pipelines - Senior Deep Dive"
topic: ci-cd
subtopic: observability-and-slos
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ci-cd, observability, slo, error-budget, burn-rate, monte-carlo, incident-response, data-observability]
---

# Observability and SLOs for Data Pipelines — Senior Deep Dive

## SLO Engineering: Burn Rate Alerts

A burn rate alert is far superior to simple threshold alerts because it warns you about SLO violations *before* they happen. Rather than alerting when freshness exceeds 4 hours (which means you've already violated the SLO), burn rate alerts you when you're consuming your error budget faster than sustainable.

### The Math Behind Burn Rate

```
SLO: 99.5% freshness compliance over 30 days
Error budget: 0.5% of 43,200 minutes = 216 minutes

Burn rate 1x: consuming error budget exactly as fast as it replenishes
  → You'll use exactly 216 minutes of bad freshness in 30 days. Sustainable.

Burn rate 14x: consuming 14x faster than sustainable
  → You'll exhaust the 30-day error budget in ~2 days
  → Alert severity: PAGE (critical, immediate response)

Burn rate 6x: consuming 6x faster than sustainable
  → You'll exhaust the budget in ~5 days
  → Alert severity: TICKET (urgent, same day)
```

### Multi-Window Burn Rate Alerting

The best practice from Google's SRE book is **multi-window burn rate alerts** to reduce alert noise while maintaining fast detection:

```yaml
# alertmanager/rules/data-slo-alerts.yml
groups:
  - name: data-freshness-slo
    rules:

      # Fast burn: high severity, page immediately
      # 14x burn rate detected in both 1h AND 5m windows
      - alert: DataFreshnessFastBurn
        expr: |
          (
            sum(rate(freshness_violations_total[1h])) /
            sum(rate(freshness_checks_total[1h]))
          ) > (14 * 0.005)
          AND
          (
            sum(rate(freshness_violations_total[5m])) /
            sum(rate(freshness_checks_total[5m]))
          ) > (14 * 0.005)
        for: 2m
        labels:
          severity: critical
          team: data-engineering
        annotations:
          summary: "Data freshness SLO fast burn (14x rate)"
          description: |
            Freshness error budget burning at 14x rate.
            At this rate, 30-day budget exhausted in ~2 days.
            Current error budget remaining: {{ $value }}%
          runbook_url: "https://runbooks.company.com/data/freshness-incident"

      # Slow burn: lower severity, create ticket
      # 6x burn rate detected in 6h AND 30m windows
      - alert: DataFreshnessSlowBurn
        expr: |
          (
            sum(rate(freshness_violations_total[6h])) /
            sum(rate(freshness_checks_total[6h]))
          ) > (6 * 0.005)
          AND
          (
            sum(rate(freshness_violations_total[30m])) /
            sum(rate(freshness_checks_total[30m]))
          ) > (6 * 0.005)
        for: 15m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "Data freshness SLO slow burn (6x rate)"
          description: "Budget burning at 6x rate; exhausted in ~5 days if not addressed."
```

### Tracking Error Budget in Practice

```sql
-- Snowflake: Calculate current error budget consumption
-- Assuming freshness_checks table has per-check pass/fail records
WITH budget_calculation AS (
    SELECT
        COUNT(*) as total_checks,
        SUM(CASE WHEN is_fresh = FALSE THEN 1 ELSE 0 END) as failed_checks,
        SUM(CASE WHEN is_fresh = FALSE THEN 1 ELSE 0 END) / COUNT(*)::FLOAT as error_rate,
        0.005 as slo_error_budget_rate,  -- 0.5% allowed
        DATEDIFF('minute',
            DATEADD('day', -30, CURRENT_TIMESTAMP()),
            CURRENT_TIMESTAMP()
        ) as window_minutes
    FROM data_platform_monitoring.freshness_checks
    WHERE checked_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT
    total_checks,
    failed_checks,
    ROUND(error_rate * 100, 3) as error_rate_pct,
    ROUND(slo_error_budget_rate * window_minutes, 1) as budget_minutes,
    ROUND(error_rate * window_minutes, 1) as consumed_minutes,
    ROUND((slo_error_budget_rate - error_rate) * window_minutes, 1) as remaining_budget_minutes,
    ROUND((slo_error_budget_rate - error_rate) / slo_error_budget_rate * 100, 1) as budget_remaining_pct
FROM budget_calculation;
```

## Monte Carlo and Automated Data Observability

Monte Carlo is the leading commercial data observability platform. It sits between your data sources and your warehouse, monitoring data quality automatically without requiring you to write manual checks.

### What Monte Carlo Monitors Automatically

After connecting to your warehouse, Monte Carlo uses ML to learn your data's normal patterns:

1. **Volume monitoring**: Detects when table sizes deviate significantly from historical norms
2. **Freshness monitoring**: Alerts when tables stop updating
3. **Schema changes**: Notifies when columns are added, removed, or types changed
4. **Distribution shift**: Detects when the statistical distribution of a column changes (e.g., suddenly more nulls, new values in an enum column)
5. **Field-level lineage**: Shows which upstream sources caused a downstream table's anomaly

### Monte Carlo vs. Manual dbt Tests: When to Use Each

| Approach | Best For | Limitations |
|----------|----------|-------------|
| dbt tests (schema.yml) | Business rule validation ("orders must have positive amounts") | You must anticipate and write each test |
| Monte Carlo | Unknown unknowns — detecting anomalies you didn't predict | Can't encode business rules; may false-positive on intentional changes |
| dbt + Monte Carlo | Production standard: rules for known requirements + ML for unknown anomalies | Cost of Monte Carlo license |

### Implementing Data Observability Without Commercial Tools

If you can't afford Monte Carlo, the open-source alternative is **Elementary** (an open-source dbt package):

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: 0.13.0
```

```yaml
# models/schema.yml — Elementary adds ML-powered anomaly detection to dbt
models:
  - name: fct_orders
    meta:
      elementary:
        timestamp_column: "order_created_at"
    tests:
      # Elementary anomaly tests — automatically detect volume, freshness, distributions
      - elementary.volume_anomalies:
          timestamp_column: order_created_at
          sensitivity: 3   # Standard deviations threshold

      - elementary.freshness_anomalies:
          timestamp_column: order_created_at
          max_allowed_delay: 4 hours

      - elementary.column_anomalies:
          column_name: order_total
          anomaly_sensitivity: 3
          tests:
            - null_rate
            - zero_rate
            - average
```

## Incident Response Runbooks for Data SLA Breaches

A runbook is a documented procedure for responding to an incident. For data SLA breaches, having a runbook reduces MTTD (Mean Time to Detect) and MTTR (Mean Time to Resolve) significantly.

### Runbook Template: Freshness SLA Breach

```markdown
# Runbook: Data Freshness SLA Breach

## Incident Definition
Alert: DataFreshnessFastBurn
SLA: Orders data < 4 hours old by 8am, 99% of business days
SLA Breach Impact: Revenue dashboard incorrect; finance team cannot close books

## On-Call Response Steps

### T+0: Initial Assessment (< 5 minutes)
1. Check PagerDuty for alert details — which table is stale?
2. Query freshness directly:
   ```sql
   SELECT MAX(order_created_at), DATEDIFF('hour', MAX(order_created_at), CURRENT_TIMESTAMP())
   FROM analytics.fct_orders;
   ```
3. Check Airflow for failed/running tasks:
   - Go to Airflow UI > DAGs > orders_etl > Recent runs
   - Look for red (failed) or gray (queued) tasks

### T+5: Triage
**If Airflow DAG is running (late but not failed):**
- Check executor capacity: `airflow_executor_queued_tasks` in Grafana
- If queue is deep, trigger a spot instance scale-out or wait
- No action needed if ETA to complete is before SLA breach

**If Airflow DAG failed:**
- Check task logs for root cause
- Common causes and fixes:
  | Root Cause | Fix |
  |------------|-----|
  | Snowflake credit exhausted | Re-enable credits, re-trigger task |
  | S3 source data missing | Check upstream producer pipeline |
  | dbt compilation error | Check for syntax error in recent PR |
  | OOM in Spark | Increase executor memory in cluster config |

**If upstream source is missing (S3 data not arrived):**
- Alert source team (use #data-alerts Slack channel)
- Set ETA expectation with stakeholders

### T+15: Stakeholder Communication
```
Template (Slack #data-status channel):
[DATA INCIDENT] Orders freshness SLA at risk
Status: Investigating | ETA to resolution: 45 minutes
Impact: Revenue dashboard data as of {last_good_time}
Root cause: {identified_cause}
Next update: in 30 minutes
```

### T+30: Resolution or Escalation
- If resolved: run `dbt build --select fct_orders+` to reprocess
- If not resolved: escalate to senior DE or engineering manager
- Update stakeholders with new ETA

### T+60: Post-Incident (after resolution)
- Write incident report (5-minute template below)
- Create GitHub issue for root cause fix
- Update runbook if gaps identified

## 5-Minute Incident Report Template
- **Date/time**: {incident_start} to {incident_end}
- **Duration**: {minutes} minutes
- **SLA impact**: {yes/no — was 8am SLA breached?}
- **Root cause**: {one sentence}
- **Resolution**: {what was done to fix it}
- **Prevention**: {GitHub issue #{number} to prevent recurrence}
```

## Building an Internal Data Observability Dashboard

A practical dashboard for data platform teams — not for business users, but for the data engineering on-call:

```python
# dashboard/slo_dashboard.py — Streamlit-based internal SLO dashboard

import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta

st.title("Data Platform SLO Dashboard")

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(**get_creds())

conn = get_connection()

# Freshness SLO widget
st.header("Data Freshness SLO (target: < 4 hours, 99.5%)")

freshness_df = pd.read_sql("""
    SELECT
        table_name,
        DATEDIFF('minute', latest_record_time, CURRENT_TIMESTAMP()) as minutes_stale,
        CASE
            WHEN DATEDIFF('minute', latest_record_time, CURRENT_TIMESTAMP()) > 240
            THEN 'VIOLATION'
            WHEN DATEDIFF('minute', latest_record_time, CURRENT_TIMESTAMP()) > 180
            THEN 'WARNING'
            ELSE 'OK'
        END as status,
        latest_record_time
    FROM data_platform_monitoring.table_freshness
    ORDER BY minutes_stale DESC
""", conn)

# Color code by status
def color_status(val):
    color = {'VIOLATION': 'red', 'WARNING': 'orange', 'OK': 'green'}.get(val, 'black')
    return f'color: {color}; font-weight: bold'

st.dataframe(
    freshness_df.style.applymap(color_status, subset=['status']),
    use_container_width=True
)

# Error budget gauge
budget_remaining = pd.read_sql("""
    SELECT budget_remaining_pct FROM data_platform_monitoring.slo_budget_30d
    WHERE slo_name = 'orders_freshness'
""", conn).iloc[0, 0]

st.metric(
    "Error Budget Remaining (30-day rolling)",
    f"{budget_remaining:.1f}%",
    delta=f"{'Budget healthy' if budget_remaining > 50 else 'Budget at risk'}",
    delta_color="normal" if budget_remaining > 50 else "inverse"
)
```

## Key Senior Interview Takeaways

- **Burn rate alerts** are superior to threshold alerts — alerting at 6x and 14x burn rate gives early warning before the SLO is breached
- **Multi-window burn rate** (e.g., 1h + 5m windows must both trigger) reduces false positives while maintaining fast detection
- **Monte Carlo / Elementary** provide ML-based anomaly detection for unknowns; dbt tests handle known business rules — production uses both
- **Runbooks must be executable under stress** — they should be so specific that an on-call engineer who's never seen the system can follow them at 3am
- **Error budget tracking** in SQL gives the data team a quantitative answer to "how much reliability do we need?" and shifts the conversation from "maximize uptime" to "manage the budget wisely"
- Know the difference between **MTTD** (how fast you detect an incident) and **MTTR** (how fast you resolve it) — both are improved by good observability, but in different ways
