---
title: "Data SLA Enforcement - Fundamentals"
topic: data-quality
subtopic: data-sla-enforcement
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [data-quality, sla, slo, sli, freshness, data-reliability]
---

# Data SLA Enforcement — Fundamentals

## Analogy

A data SLA is like a service-level agreement between a restaurant and a catering client: "We promise your order will be ready by 7pm, will include all 200 meals, and the food will meet health code standards." If any part fails, there are consequences — complaints, refunds, lost business. Data SLAs work the same way: data must arrive on time, be complete, and be accurate.

---

## SLA vs SLO vs SLI for Data

These three terms come from Site Reliability Engineering (SRE) and have direct data engineering analogs:

| Term | Definition | Data Example |
|---|---|---|
| **SLI** (Service Level Indicator) | A specific measurable metric | `MAX(updated_at)` lag in minutes; null rate %; row count |
| **SLO** (Service Level Objective) | The internal target for an SLI | Orders table lag < 30 min; null rate < 0.1% |
| **SLA** (Service Level Agreement) | The external commitment + consequences | "Finance reports are refreshed by 8am daily; breach triggers incident" |

**SLO is internal; SLA is contractual.** Set SLOs tighter than SLAs to give yourself a buffer:
- SLA: data available by 8am
- SLO: data available by 7:30am (30-minute buffer for alerts and remediation)

---

## Types of Data SLAs

### Freshness SLA
Data must be updated within a defined time window.

- **Example:** Payment pipeline data must be no older than 15 minutes relative to the source system update.
- **Metric:** `CURRENT_TIMESTAMP() - MAX(updated_at)` in the orders table
- **Breach condition:** This lag exceeds 15 minutes

### Completeness SLA
Data must contain at least N% of expected records.

- **Example:** Daily customer activity file must contain records for at least 95% of active customers.
- **Metric:** `COUNT(DISTINCT customer_id) / expected_customer_count`
- **Breach condition:** Ratio drops below 0.95

### Accuracy SLA
Data values must be within acceptable error bounds.

- **Example:** Revenue figures must match Stripe's source-of-truth within 0.1%.
- **Metric:** `ABS(warehouse_revenue - stripe_revenue) / stripe_revenue`
- **Breach condition:** Discrepancy exceeds 0.1%

### Availability SLA
Data must be queryable and the pipeline must complete successfully.

- **Example:** The orders table must be available for queries 99.5% of the time.
- **Metric:** Minutes with successful queries / total minutes in period
- **Breach condition:** Uptime drops below 99.5%

---

## Freshness SLA Implementation

### Using dbt Source Freshness

The simplest implementation: `dbt source freshness` checks the `MAX(loaded_at_field)` and fails the pipeline if stale.

```yaml
# models/sources.yml
sources:
  - name: stripe
    database: raw
    schema: stripe
    tables:
      - name: charges
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 10, period: minute}
          error_after: {count: 20, period: minute}
        description: "Stripe charge events — SLA: fresh within 15 minutes"
```

```bash
# Run freshness check
dbt source freshness --select source:stripe

# Returns exit code 1 if error threshold exceeded
```

### Using Airflow Sensors

For event-driven pipelines, use Airflow sensors to wait for data to arrive:

```python
# dags/payment_pipeline.py
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor
from airflow.sensors.sql import SqlSensor
from airflow.decorators import dag
import pendulum

@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2024, 1, 1))
def payment_sla_pipeline():
    
    # Wait up to 20 minutes for Stripe data to be fresh
    wait_for_stripe_data = SqlSensor(
        task_id="wait_for_stripe_freshness",
        conn_id="snowflake_prod",
        sql="""
            SELECT CASE
                WHEN DATEDIFF('minute', MAX(_fivetran_synced), CURRENT_TIMESTAMP()) <= 15
                THEN 1 ELSE 0
            END
            FROM raw.stripe.charges
        """,
        mode="poke",
        poke_interval=60,   # check every 60 seconds
        timeout=1200,       # give up after 20 minutes
        soft_fail=False,    # hard fail if timeout exceeded
    )
    
    wait_for_stripe_data
```

### Custom Check Function

```python
# plugins/sla_checks.py
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

def check_freshness_sla(
    table_id: str,
    timestamp_col: str,
    sla_minutes: int,
    client: bigquery.Client = None
) -> dict:
    """
    Check if a table's most recent record is within the SLA window.
    Returns a result dict with is_breached, lag_minutes, and details.
    """
    if client is None:
        client = bigquery.Client()
    
    query = f"""
    SELECT
        MAX({timestamp_col}) AS last_updated,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({timestamp_col}), MINUTE) AS lag_minutes
    FROM `{table_id}`
    """
    
    result = list(client.query(query).result())[0]
    last_updated = result["last_updated"]
    lag_minutes = result["lag_minutes"]
    
    return {
        "table": table_id,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "lag_minutes": lag_minutes,
        "sla_minutes": sla_minutes,
        "is_breached": lag_minutes is None or lag_minutes > sla_minutes,
        "breach_severity": (
            "critical" if (lag_minutes or 9999) > sla_minutes * 2
            else "warning" if (lag_minutes or 9999) > sla_minutes
            else "ok"
        ),
    }
```

---

## Breach Detection and Basic Alerting

When an SLA breach is detected, the notification must be:
1. **Fast** — within minutes, not hours
2. **Actionable** — include the table, the lag, and a link to the runbook
3. **Routed correctly** — the right team, not everyone

```python
# scripts/sla_breach_alert.py
import requests

def alert_sla_breach(result: dict):
    if not result["is_breached"]:
        return
    
    slack_message = {
        "text": (
            f":rotating_light: *SLA Breach Detected*\n"
            f"*Table:* `{result['table']}`\n"
            f"*Lag:* {result['lag_minutes']} minutes (SLA: {result['sla_minutes']} min)\n"
            f"*Last updated:* {result['last_updated']}\n"
            f"*Runbook:* https://wiki.company.com/runbooks/sla-breach\n"
            f"*On-call:* <!subteam^DATA_ONCALL>"
        )
    }
    requests.post(SLACK_WEBHOOK_URL, json=slack_message)
```

---

## Key Interview Points

- SLI = the metric; SLO = the internal target; SLA = the external commitment with consequences
- Set SLOs 10–20% tighter than SLAs to leave remediation headroom
- Four types of data SLAs: freshness, completeness, accuracy, availability
- `dbt source freshness` is the simplest freshness check — uses `MAX(loaded_at_field)` vs wall clock
- Airflow `SqlSensor` enables event-driven freshness checks with configurable timeout
- Breach alerts must be fast, actionable, and routed to the right team (not broadcast to all)
