---
title: "dbt Exposures & Metrics - Real-World"
topic: dbt
subtopic: exposures-and-metrics
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, metrics, exposures, production, kpi]
---

# dbt Exposures & Metrics — Real-World Examples

## Example 1: Company-Wide KPI Metric Store

Define all company KPIs in dbt:

```yaml
# models/metrics/finance_metrics.yml
metrics:
  - name: mrr
    label: "Monthly Recurring Revenue"
    type: simple
    meta:
      certified: true
      owner: "@finance"
      definition_doc: "https://notion.so/mrr-definition"
    type_params:
      measure: mrr_amount

  - name: arr
    label: "Annual Recurring Revenue"
    type: derived
    type_params:
      expr: mrr * 12
      metrics: [mrr]

  - name: churn_rate
    label: "Monthly Churn Rate"
    type: ratio
    type_params:
      numerator: churned_customers
      denominator: customers_start_of_month
```

Different teams query the SAME definitions — no more "whose revenue number is right?"

## Example 2: Impact Analysis Before Schema Change

Before renaming `total_amount` → `revenue` in `fct_orders`:

```bash
# Step 1: Find all exposures that depend on fct_orders
dbt ls --select fct_orders+ --resource-type exposure
# Output:
# exposure:my_project.executive_revenue_dashboard
# exposure:my_project.finance_monthly_report
# exposure:my_project.ml_churn_model
# exposure:my_project.order_api

# Step 2: Check metrics that reference fct_orders
mf validate-configs
# Shows: revenue, order_count, avg_order_value all reference total_amount

# Step 3: Notify all owners
cat models/exposures.yml | grep -A3 "depends_on" | grep "fct_orders" -A5
```

Result: You discover 4 downstream consumers — reach out to all owners before the rename.

## Example 3: Slack Metric Bot Integration

Query metrics from Slack via dbt Semantic Layer API:

```python
# slack_bot/metric_query.py
from slack_bolt import App
from dbt_sl_sdk import SemanticLayerClient

app = App(token=os.environ["SLACK_BOT_TOKEN"])
sl_client = SemanticLayerClient(
    environment_id=int(os.environ["DBT_ENV_ID"]),
    auth_token=os.environ["DBT_TOKEN"]
)

@app.message("revenue today")
def handle_revenue(message, say):
    df = sl_client.query(
        metrics=["total_revenue"],
        group_by=["order__order_date"],
        where=["order__order_date = today()"]
    )
    revenue = df['total_revenue'].sum()
    say(f"Today's revenue: ${revenue:,.2f}")
```

## Example 4: Exposure-Driven SLA Monitoring

```sql
-- models/monitoring/sla_risk_dashboard.sql
{{ config(materialized='view') }}

SELECT
    e.exposure_name,
    e.maturity,
    e.owner_email,
    m.model_name,
    m.last_run_status,
    m.last_run_at,
    DATEDIFF('hour', m.last_run_at, CURRENT_TIMESTAMP()) AS hours_since_update,
    CASE
        WHEN e.maturity = 'high' AND hours_since_update > 4 THEN 'SLA_BREACH_RISK'
        WHEN e.maturity = 'medium' AND hours_since_update > 12 THEN 'SLA_BREACH_RISK'
        ELSE 'OK'
    END AS sla_status
FROM {{ ref('dbt_exposures') }} e
JOIN {{ ref('dbt_model_run_history') }} m
    ON m.model_name = ANY(e.depends_on_models)
WHERE sla_status = 'SLA_BREACH_RISK'
ORDER BY hours_since_update DESC
```
