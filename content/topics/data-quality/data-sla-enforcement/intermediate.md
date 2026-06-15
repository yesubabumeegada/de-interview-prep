---
title: "Data SLA Enforcement - Intermediate"
topic: data-quality
subtopic: data-sla-enforcement
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-quality, sla, data-contracts, error-budget, reporting, escalation]
---

# Data SLA Enforcement — Intermediate

## SLA Reporting Dashboards from Pipeline Runs

Track SLA compliance over time by persisting pipeline run metadata.

### Pipeline Runs Table

```sql
-- Create the tracking table
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    target_table STRING NOT NULL,
    run_started_at TIMESTAMP NOT NULL,
    run_completed_at TIMESTAMP,
    status STRING,              -- 'success', 'failed', 'running'
    rows_written INT64,
    freshness_lag_minutes FLOAT64,
    sla_freshness_minutes INT64,   -- configured SLA
    sla_completeness_pct FLOAT64,
    actual_completeness_pct FLOAT64,
    is_sla_breached BOOL,
    breach_type STRING,            -- 'freshness', 'completeness', 'accuracy', NULL
    run_duration_seconds FLOAT64
        AS (TIMESTAMP_DIFF(run_completed_at, run_started_at, SECOND))
)
PARTITION BY DATE(run_started_at)
OPTIONS (partition_expiration_days = 730);
```

### SLA Compliance Dashboard Query

```sql
-- Weekly SLA compliance summary
SELECT
    pipeline_name,
    target_table,
    DATE_TRUNC(run_started_at, WEEK) AS week,
    COUNT(*) AS total_runs,
    COUNTIF(status = 'success') AS successful_runs,
    COUNTIF(is_sla_breached) AS sla_breaches,
    ROUND(1 - COUNTIF(is_sla_breached) / COUNT(*), 4) AS compliance_rate,
    AVG(freshness_lag_minutes) AS avg_lag_minutes,
    MAX(freshness_lag_minutes) AS max_lag_minutes,
    APPROX_QUANTILES(freshness_lag_minutes, 100)[OFFSET(95)] AS p95_lag_minutes
FROM monitoring.pipeline_runs
WHERE run_started_at >= CURRENT_DATE() - 90
GROUP BY 1, 2, 3
ORDER BY compliance_rate ASC, week DESC;
```

```sql
-- Daily breach log for incident review
SELECT
    run_id,
    pipeline_name,
    target_table,
    run_started_at,
    freshness_lag_minutes,
    sla_freshness_minutes,
    freshness_lag_minutes - sla_freshness_minutes AS overage_minutes,
    breach_type
FROM monitoring.pipeline_runs
WHERE is_sla_breached = TRUE
  AND DATE(run_started_at) >= CURRENT_DATE() - 30
ORDER BY run_started_at DESC;
```

---

## Multi-Tier SLAs (Platinum / Gold / Silver)

Not all data is equally critical. Tier your data products:

```yaml
# data_products.yml — define tiers
data_products:
  - name: payment_transactions
    tier: platinum
    sla:
      freshness_minutes: 15
      completeness_pct: 99.9
      availability_pct: 99.9
      alert_channel: "#payments-data-oncall"
      pagerduty: true
      pagerduty_escalation_minutes: 5
  
  - name: customer_360
    tier: gold
    sla:
      freshness_minutes: 60
      completeness_pct: 99.0
      availability_pct: 99.5
      alert_channel: "#data-alerts"
      pagerduty: true
      pagerduty_escalation_minutes: 30
  
  - name: marketing_attribution
    tier: silver
    sla:
      freshness_minutes: 240   # 4 hours
      completeness_pct: 95.0
      availability_pct: 95.0
      alert_channel: "#data-alerts"
      pagerduty: false
      email_escalation: "data-team@company.com"
```

```python
# plugins/sla_enforcer.py
import yaml
from dataclasses import dataclass

@dataclass
class SLAConfig:
    name: str
    tier: str
    freshness_minutes: int
    completeness_pct: float
    availability_pct: float
    alert_channel: str
    pagerduty: bool
    pagerduty_escalation_minutes: int = 30

def load_sla_configs(config_path: str) -> dict[str, SLAConfig]:
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return {
        dp["name"]: SLAConfig(name=dp["name"], tier=dp["tier"], **dp["sla"])
        for dp in data["data_products"]
    }

def enforce_sla(product_name: str, check_result: dict, configs: dict[str, SLAConfig]):
    config = configs.get(product_name)
    if not config:
        return
    
    is_breached = check_result["lag_minutes"] > config.freshness_minutes
    
    if is_breached:
        notify_slack(config.alert_channel, check_result, config)
        if config.pagerduty:
            trigger_pagerduty_after_delay(
                check_result, 
                delay_minutes=config.pagerduty_escalation_minutes
            )
```

---

## Error Budgets for Data Teams

Borrowed from SRE, an error budget is the allowed amount of SLA non-compliance:

```
Error budget = 1 - SLA target

For a 99.5% availability SLA over 30 days:
  Total minutes = 30 × 24 × 60 = 43,200
  Error budget = 43,200 × (1 - 0.995) = 216 minutes of allowed downtime
```

```sql
-- Track error budget consumption
WITH monthly_stats AS (
    SELECT
        pipeline_name,
        target_table,
        COUNT(*) AS total_runs,
        COUNTIF(is_sla_breached) AS breach_count,
        SUM(
            CASE WHEN is_sla_breached
                THEN GREATEST(freshness_lag_minutes - sla_freshness_minutes, 0)
                ELSE 0
            END
        ) AS total_overage_minutes
    FROM monitoring.pipeline_runs
    WHERE DATE(run_started_at) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY 1, 2
),
sla_targets AS (
    SELECT 'payment_transactions' AS pipeline_name, 99.9 AS sla_target, 15 AS sla_minutes
    UNION ALL
    SELECT 'customer_360', 99.5, 60
)
SELECT
    m.pipeline_name,
    s.sla_target,
    1 - m.breach_count / m.total_runs AS actual_compliance,
    s.sla_target / 100 - (1 - m.breach_count / m.total_runs) AS budget_remaining,
    m.total_overage_minutes,
    CASE
        WHEN (1 - m.breach_count / m.total_runs) >= s.sla_target / 100 THEN 'within_budget'
        ELSE 'budget_exceeded'
    END AS budget_status
FROM monthly_stats m
JOIN sla_targets s ON m.pipeline_name = s.pipeline_name;
```

---

## Data Contracts as SLA Enforcement Mechanism

A data contract is a formal agreement between data producers and consumers that codifies SLAs:

```yaml
# contracts/payment_transactions_v1.yaml
apiVersion: v1
kind: DataContract
metadata:
  name: payment_transactions
  version: "1.0.0"
  owner: payments-data-team@company.com
  consumers:
    - finance-team@company.com
    - analytics@company.com

spec:
  table: analytics.fct_payments
  sla:
    freshness:
      max_lag_minutes: 15
      measurement: "MAX(updated_at) vs CURRENT_TIMESTAMP()"
    completeness:
      min_completeness_pct: 99.9
      measurement: "Non-null transaction_id / expected_count"
    accuracy:
      max_revenue_variance_pct: 0.1
      measurement: "ABS(warehouse_revenue - stripe_revenue) / stripe_revenue"
  
  schema:
    - name: transaction_id
      type: STRING
      nullable: false
      description: "Unique payment transaction identifier"
    - name: amount_usd
      type: FLOAT64
      nullable: false
      constraints:
        min: 0
        max: 1000000
    - name: status
      type: STRING
      allowed_values: ["pending", "completed", "failed", "refunded"]
  
  change_management:
    breaking_change_notice_days: 14
    deprecation_notice_days: 30
    contact: payments-data-team@company.com
```

```python
# validate_contract.py — run in CI or as an Airflow task
import yaml
from google.cloud import bigquery

def validate_contract(contract_path: str) -> dict:
    with open(contract_path) as f:
        contract = yaml.safe_load(f)
    
    client = bigquery.Client()
    table_id = contract["spec"]["table"]
    sla = contract["spec"]["sla"]
    
    results = {}
    
    # Freshness check
    row = list(client.query(f"""
        SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), MINUTE) AS lag
        FROM `{table_id}`
    """).result())[0]
    results["freshness"] = {
        "lag_minutes": row["lag"],
        "sla_minutes": sla["freshness"]["max_lag_minutes"],
        "passed": row["lag"] <= sla["freshness"]["max_lag_minutes"],
    }
    
    # Schema check
    table = client.get_table(table_id)
    actual_cols = {f.name: f.field_type for f in table.schema}
    schema_violations = []
    for col_def in contract["spec"]["schema"]:
        col_name = col_def["name"]
        if col_name not in actual_cols:
            schema_violations.append(f"Missing column: {col_name}")
        elif actual_cols[col_name] != col_def["type"]:
            schema_violations.append(
                f"Type mismatch: {col_name} expected {col_def['type']}, got {actual_cols[col_name]}"
            )
    results["schema"] = {"violations": schema_violations, "passed": not schema_violations}
    
    return results
```

---

## Downstream Consumer Notification

When an SLA breach occurs, downstream consumers must be notified:

```python
# plugins/consumer_notifier.py
"""Notify downstream consumers when their data SLA is breached."""

CONSUMER_REGISTRY = {
    "payment_transactions": [
        {"team": "Finance", "contact": "finance-data@company.com", "slack": "#finance-data"},
        {"team": "Revenue Analytics", "contact": "rev-analytics@company.com", "slack": "#revenue-analytics"},
    ],
    "customer_360": [
        {"team": "CRM", "contact": "crm-team@company.com", "slack": "#crm-ops"},
    ],
}

def notify_consumers(product_name: str, breach_info: dict):
    consumers = CONSUMER_REGISTRY.get(product_name, [])
    
    for consumer in consumers:
        # Slack notification
        post_slack(
            channel=consumer["slack"],
            text=(
                f":warning: *Data SLA Alert for {consumer['team']}*\n"
                f"The `{product_name}` dataset is experiencing a quality issue.\n"
                f"*Impact:* Data may be stale or incomplete.\n"
                f"*Lag:* {breach_info['lag_minutes']} minutes (SLA: {breach_info['sla_minutes']} min)\n"
                f"*Status:* Engineering team is investigating.\n"
                f"*ETA for resolution:* Will update in 30 minutes.\n"
                f"*Incident:* {breach_info.get('incident_url', 'TBD')}"
            )
        )
```

---

## Key Interview Points

- Persist pipeline run metadata to enable SLA compliance dashboards over time
- Multi-tier SLAs (platinum/gold/silver) allow proportional alerting — not everything needs PagerDuty
- Error budgets quantify total allowed SLA overage per period; when exhausted, freeze risky changes
- Data contracts codify SLAs as machine-readable YAML — validate in CI to catch schema drift before production
- Downstream consumer notification is a separate concern from internal alerting — consumers need ETA and impact assessment, not technical details
