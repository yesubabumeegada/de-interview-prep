---
title: "Data SLA Enforcement - Senior Deep Dive"
topic: data-quality
subtopic: data-sla-enforcement
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-quality, sla, post-mortem, incident-management, data-contracts, senior]
---

# Data SLA Enforcement — Senior Deep Dive

## Payment Pipeline 99.9% Freshness SLA Design

Real example: A fintech company's payment pipeline must deliver data within 15 minutes of a source update, 99.9% of the time (allowing ~44 minutes of breach per month).

### Full Architecture

```
Stripe Webhook → Kafka → Spark Streaming → Delta Lake (raw) → dbt → BigQuery (analytics)
                                                    ↓
                                            Freshness Monitor (every 2 min)
                                                    ↓
                                            SLA Tracker → PagerDuty (if breach)
```

### Airflow Implementation

```python
# dags/payment_pipeline_with_sla.py
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor
from airflow.models import Variable
from datetime import datetime, timezone, timedelta
import pendulum
import requests

SLA_FRESHNESS_MINUTES = 15
BREACH_SLACK_CHANNEL = "#payments-data-oncall"
PAGERDUTY_ROUTING_KEY = Variable.get("PAGERDUTY_PAYMENTS_KEY")

def payment_sla_breach_callback(context):
    """Called by Airflow when a task exceeds its SLA."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    
    breach_info = {
        "dag": dag_id,
        "task": task_id,
        "execution_date": str(execution_date),
        "sla_minutes": SLA_FRESHNESS_MINUTES,
    }
    
    # Page immediately for payment SLA breaches
    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json={
            "routing_key": PAGERDUTY_ROUTING_KEY,
            "event_action": "trigger",
            "payload": {
                "summary": f"Payment pipeline SLA BREACH: {dag_id}.{task_id}",
                "severity": "critical",
                "source": "airflow-sla-monitor",
                "custom_details": breach_info,
            }
        }
    )

@dag(
    schedule="*/5 * * * *",  # check every 5 minutes
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=payment_sla_breach_callback,
    default_args={
        "sla": timedelta(minutes=SLA_FRESHNESS_MINUTES),
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
)
def payment_sla_pipeline():
    
    @task(sla=timedelta(minutes=10))
    def check_source_freshness() -> dict:
        from google.cloud import bigquery
        client = bigquery.Client()
        
        result = list(client.query("""
            SELECT
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_fivetran_synced), MINUTE) AS lag_minutes
            FROM `raw.stripe.charges`
        """).result())[0]
        
        lag = result["lag_minutes"]
        if lag > SLA_FRESHNESS_MINUTES:
            raise ValueError(f"Source freshness breach: {lag}min stale (SLA: {SLA_FRESHNESS_MINUTES}min)")
        
        return {"lag_minutes": lag, "status": "ok"}
    
    @task(sla=timedelta(minutes=13))
    def run_dbt_transforms(freshness_check: dict):
        import subprocess
        result = subprocess.run(
            ["dbt", "build", "--select", "tag:payments", "--target", "prod"],
            capture_output=True, text=True, check=True
        )
        return {"dbt_status": "success"}
    
    @task(sla=timedelta(minutes=15))
    def record_sla_result(dbt_result: dict, freshness_check: dict):
        """Write SLA compliance record to monitoring table."""
        from google.cloud import bigquery
        import uuid
        
        client = bigquery.Client()
        client.insert_rows_json(
            "project.monitoring.pipeline_runs",
            [{
                "run_id": str(uuid.uuid4()),
                "pipeline_name": "payment_sla_pipeline",
                "target_table": "analytics.fct_payments",
                "run_started_at": datetime.now(timezone.utc).isoformat(),
                "status": "success",
                "freshness_lag_minutes": freshness_check["lag_minutes"],
                "sla_freshness_minutes": SLA_FRESHNESS_MINUTES,
                "is_sla_breached": freshness_check["lag_minutes"] > SLA_FRESHNESS_MINUTES,
            }]
        )
    
    fresh = check_source_freshness()
    dbt = run_dbt_transforms(fresh)
    record_sla_result(dbt, fresh)

payment_sla_pipeline()
```

---

## Remediation Runbooks

Every SLA breach type needs a runbook — a documented, step-by-step response procedure.

### Runbook Template: Freshness Breach

```markdown
# Runbook: Payment Pipeline Freshness SLA Breach

## Alert Source
PagerDuty: payment_sla_pipeline — freshness breach

## Severity
P1 — Affects real-time payment processing reports

## Triage Steps (execute in order, ~15 min)

### Step 1: Check Fivetran sync status (2 min)
1. Go to https://fivetran.com/dashboard → Stripe connector
2. Check "Last synced" timestamp and sync status
3. If status shows ERROR: click "Sync Now" and watch for completion

### Step 2: Check Kafka consumer lag (3 min)
```bash
# Check consumer group lag
kafka-consumer-groups.sh \
  --bootstrap-server $KAFKA_BROKERS \
  --group payment-processor \
  --describe | grep -E "TOPIC|payment"
```
Normal lag: < 1000 messages. If > 50,000 → consumer is falling behind

### Step 3: Check Spark streaming job (5 min)
1. Open Databricks → Compute → `payment-streaming-job`
2. Check for recent failures in the "Event log"
3. If failed: click "Restart" and monitor for 3 minutes

### Step 4: Manual data backfill (if needed, ~30 min)
```bash
# Trigger a manual full sync for the last 2 hours
python scripts/backfill_payments.py \
  --start-time "$(date -u -d '2 hours ago' '+%Y-%m-%dT%H:%M:%SZ')" \
  --end-time "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
```

## Escalation
- > 15 min: Escalate to Kafka admin team
- > 30 min: Notify VP of Engineering + Finance stakeholders
- > 60 min: Invoke disaster recovery playbook
```

---

## Incident Post-Mortems for SLA Breaches

### Post-Mortem Template

```markdown
# Post-Mortem: Payment Pipeline SLA Breach — 2024-11-15

## Impact
- Duration: 47 minutes (08:12 – 08:59 UTC)
- SLA: 99.9% freshness within 15 minutes
- Breach: Exceeded by 32 minutes
- Monthly error budget consumed: 47/44 minutes (106% — budget exhausted)
- Downstream impact: Finance daily revenue report delayed by 1 hour

## Timeline
| Time | Event |
|---|---|
| 08:12 | Stripe Fivetran connector failed (SSL cert renewal issue) |
| 08:15 | Freshness monitor detected lag > 15 min, PagerDuty fired |
| 08:20 | On-call engineer acknowledged, began triage |
| 08:35 | Root cause identified: expired SSL certificate on Fivetran → Stripe connection |
| 08:45 | Certificate rotated, connector restarted |
| 08:59 | Data fully caught up, freshness within SLA |

## Root Cause
Fivetran's Stripe connector uses a custom SSL certificate for enhanced security. The cert expired on 2024-11-14 but the renewal alert went to a distribution list that was no longer monitored.

## Contributing Factors
1. Certificate expiry alert sent to `data-infra@company.com` (unmonitored)
2. Fivetran doesn't have a built-in Slack integration for connection failures
3. No pre-emptive monitoring of certificate expiry dates

## Action Items
| Item | Owner | Due |
|---|---|---|
| Route Fivetran failure alerts to #payments-data-oncall | DE Team | 2024-11-22 |
| Add SSL cert expiry monitor (warn 30 days before) | Platform Team | 2024-11-29 |
| Add Fivetran API health check to daily monitoring | DE Team | 2024-11-22 |
| Review all monitored email aliases | Team Lead | 2024-11-22 |

## Error Budget Impact
- November budget: 44 minutes
- Consumed this incident: 47 minutes
- Budget status: EXHAUSTED for November
- Decision: Freeze non-critical pipeline changes for remainder of November
```

---

## Implementing Error Budget Gates

When the error budget is exhausted, automatically block risky changes:

```python
# scripts/check_error_budget.py
"""
Called in CI before any pipeline deployment.
Blocks deployments if error budget is exhausted.
"""
from google.cloud import bigquery
from datetime import date

def get_error_budget_status(pipeline_name: str, sla_pct: float = 99.9) -> dict:
    client = bigquery.Client()
    
    query = f"""
    WITH monthly_runs AS (
        SELECT
            COUNT(*) AS total_runs,
            COUNTIF(is_sla_breached) AS breach_count,
            SUM(
                CASE WHEN is_sla_breached
                    THEN GREATEST(freshness_lag_minutes - sla_freshness_minutes, 0)
                    ELSE 0
                END
            ) AS total_breach_minutes
        FROM monitoring.pipeline_runs
        WHERE pipeline_name = '{pipeline_name}'
          AND DATE(run_started_at) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
    )
    SELECT
        total_runs,
        breach_count,
        total_breach_minutes,
        1 - breach_count / NULLIF(total_runs, 0) AS compliance_rate,
        -- Error budget in minutes for a 30-day month
        43200 * (1 - {sla_pct / 100}) AS budget_minutes,
        total_breach_minutes / (43200 * (1 - {sla_pct / 100})) AS budget_consumed_pct
    FROM monthly_runs
    """
    
    row = list(client.query(query).result())[0]
    
    return {
        "pipeline": pipeline_name,
        "compliance_rate": float(row["compliance_rate"] or 1.0),
        "budget_minutes": float(row["budget_minutes"]),
        "consumed_minutes": float(row["total_breach_minutes"]),
        "budget_consumed_pct": float(row["budget_consumed_pct"] or 0),
        "budget_exhausted": float(row["budget_consumed_pct"] or 0) >= 1.0,
    }

if __name__ == "__main__":
    import sys
    status = get_error_budget_status("payment_sla_pipeline")
    
    print(f"Error budget: {status['consumed_minutes']:.0f}/{status['budget_minutes']:.0f} min consumed "
          f"({status['budget_consumed_pct']*100:.1f}%)")
    
    if status["budget_exhausted"]:
        print("ERROR: Error budget exhausted. Deployment blocked.")
        print("Action required: Only deploy critical hotfixes this month.")
        sys.exit(1)
    elif status["budget_consumed_pct"] > 0.75:
        print("WARNING: >75% of error budget consumed. Review carefully before deploying.")
        sys.exit(0)
    else:
        print("OK: Error budget healthy. Deployment allowed.")
        sys.exit(0)
```

---

## Key Interview Points

- Airflow's `sla` parameter at the task or DAG level triggers `sla_miss_callback` — use this for automatic breach detection
- Post-mortems must include: timeline, root cause, contributing factors, and specific action items with owners and due dates
- Error budget gates in CI block risky deployments when the monthly breach budget is exhausted
- Runbooks reduce MTTR dramatically — a documented 15-step process takes 20 minutes; an undocumented process takes 90 minutes
- Monthly SLA compliance rate = 1 - (breach_count / total_runs); 99.9% means < 1 breach per 1000 runs
- Error budget in minutes = total_minutes_in_period × (1 - SLA_target); for 99.9% over 30 days = 43.2 minutes allowed
