---
title: "dbt Exposures & Metrics - Intermediate"
topic: dbt
subtopic: exposures-and-metrics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, semantic-layer, cumulative-metrics, derived-metrics]
---

# dbt Exposures & Metrics — Intermediate

## Advanced Metric Types

### Cumulative Metrics

```yaml
metrics:
  - name: cumulative_revenue
    type: cumulative
    label: "Cumulative Revenue"
    type_params:
      measure: revenue
      window: unbounded  # Running total from beginning
      # Or: window: 30 days  (30-day rolling sum)
      grain_to_date: month  # Reset at start of each month
```

### Derived Metrics (Calculated from Other Metrics)

```yaml
metrics:
  - name: gross_profit_margin
    type: derived
    label: "Gross Profit Margin %"
    type_params:
      expr: (revenue - cost) / revenue * 100
      metrics:
        - name: revenue
        - name: cost_of_goods_sold
          alias: cost
```

### Conversion Metric

```yaml
metrics:
  - name: checkout_conversion_rate
    type: conversion
    label: "Checkout Conversion Rate"
    type_params:
      conversion_type_params:
        base_measure:
          name: checkout_starts
        conversion_measure:
          name: orders_completed
        entity: user
        calculation: conversion_rate
        window: 7 days
```

## Dimension Groups (Time Dimensions)

```yaml
semantic_models:
  - name: orders
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day  # Minimum grain
    # This automatically enables: day, week, month, quarter, year grouping
```

Query at different grains:
```bash
mf query --metrics revenue --group-by order__order_date__day
mf query --metrics revenue --group-by order__order_date__week
mf query --metrics revenue --group-by order__order_date__month
mf query --metrics revenue --group-by order__order_date__year
```

## Saved Queries (Pre-defined Metric Combos)

```yaml
saved_queries:
  - name: daily_revenue_report
    description: "Standard daily revenue KPIs for executive dashboard"
    query_params:
      metrics:
        - total_revenue
        - order_count
        - average_order_value
        - new_customer_count
      group_by:
        - "Dimension('order__order_date')"
        - "Dimension('order__region')"
    exports:
      - name: daily_revenue_export
        config:
          export_as: table
          schema: reporting
          alias: daily_revenue_kpis
```

Run: `mf run-saved-query daily_revenue_report`

## Exposure Impact Analysis

Run a dbt lineage query to understand impact:

```bash
# What models does the executive dashboard depend on?
dbt ls --select +exposure:executive_revenue_dashboard

# What would break if I delete fct_orders?
dbt ls --select fct_orders+
# Shows: all downstream models AND exposures

# Check all exposures in the project
dbt ls --resource-type exposure
```

## Linking Exposures to SLAs

```yaml
exposures:
  - name: finance_monthly_close_report
    type: analysis
    maturity: high
    meta:
      sla_description: "Must complete by 9am on first business day of month"
      alert_channel: "#finance-data-alerts"
      on_call: "@data-engineer-on-call"
      runbook: "https://confluence.company.com/finance-close-runbook"
    depends_on:
      - ref('fct_orders')
      - ref('rpt_revenue_monthly')
    owner:
      name: Finance Team
      email: finance@company.com
```

## Multi-Hop Metrics (Entity Joins)

Define metrics that join across entities:

```yaml
semantic_models:
  - name: customers
    model: ref('dim_customers')
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: country
        type: categorical
      - name: tier
        type: categorical

metrics:
  - name: revenue_by_customer_country
    type: simple
    type_params:
      measure: revenue  # from orders semantic model
    # MetricFlow auto-joins customers → orders via customer entity
```

Query:
```bash
mf query \
  --metrics revenue_by_customer_country \
  --group-by customer__country \
  --group-by order__order_date__month
```

MetricFlow generates the JOIN automatically based on entity relationships.
