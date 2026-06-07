---
title: "dbt Exposures & Metrics - Scenarios"
topic: dbt
subtopic: exposures-and-metrics
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, exposures, metrics, interview, scenarios]
---

# dbt Exposures & Metrics — Scenario Questions

## Scenario 1 (Junior): Add an Exposure

**Situation:** Your team just shipped a new Tableau dashboard called "Customer 360" that uses `dim_customers` and `fct_orders`. Add an appropriate exposure definition.

**Answer:**

```yaml
# models/exposures.yml
exposures:
  - name: customer_360_dashboard
    type: dashboard
    maturity: medium
    url: https://tableau.company.com/views/Customer360/Overview
    description: >
      360-degree view of customer health: order history, lifetime value,
      support tickets, and churn risk score. Used by Customer Success team.
    depends_on:
      - ref('dim_customers')
      - ref('fct_orders')
    owner:
      name: Customer Success Analytics
      email: cs-analytics@company.com
    meta:
      refresh_schedule: "Daily at 7am ET"
      stakeholders: ["@cs-team", "@account-management"]
```

This exposure now appears in `dbt docs` lineage, showing that `dim_customers` and `fct_orders` are consumed by this dashboard.

---

## Scenario 2 (Mid-Level): Inconsistent Revenue Numbers

**Situation:** At a board meeting, the CFO presents revenue of $5.2M for Q3. The Sales VP has a Tableau dashboard showing $5.8M. The Marketing team's Python analysis shows $5.0M. Everyone is using `fct_orders` but calculating differently. How do you fix this architecturally?

**Answer:**

**Root cause:** Three teams, three different `revenue` calculations:
- CFO's model: excludes refunds, excludes internal test orders
- Tableau dashboard: excludes refunds, INCLUDES test orders (bug)
- Python analysis: gross revenue (includes refunds)

**Fix: Implement dbt Semantic Layer**

```yaml
# models/metrics/schema.yml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    measures:
      - name: gross_revenue
        agg: sum
        expr: total_amount
      - name: refund_amount
        agg: sum
        expr: refund_amount

metrics:
  - name: net_revenue
    type: derived
    label: "Net Revenue (Official)"
    meta:
      certified: true
      owner: "@finance"
      definition: "Revenue minus refunds, excluding test orders (is_test=false)"
    type_params:
      expr: gross_revenue - refund_amount
      metrics: [gross_revenue, refund_amount]
    filter: "{{ Dimension('order__is_test') }} = false"
```

Now ALL teams query:
```bash
mf query --metrics net_revenue --group-by order__order_date__quarter
```

→ Everyone gets $5.2M (the audited correct number).

---

## Scenario 3 (Senior): Design Metric Governance for a Growing Org

**Situation:** Your data team has grown from 3 to 30 people. There are now 200+ metrics defined across 15 different dbt projects. Finance and Marketing define "active customer" differently. How do you establish metric governance?

**Answer:**

**Step 1 — Metric ownership model:**
```yaml
# Tiered certification system
metrics:
  - name: active_customers
    meta:
      status: certified           # certified | experimental | deprecated
      owner: "@data-governance"  # governance team owns certified metrics
      approved_by: "@cfo"
      definition_review_date: "2024-01-15"
      next_review_date: "2025-01-15"
```

**Step 2 — Central metrics repository (dbt Mesh):**
```
platform_project/
└── models/metrics/
    ├── finance_metrics.yml      # Finance-owned KPIs
    ├── marketing_metrics.yml    # Marketing-owned KPIs
    └── company_metrics.yml      # Cross-functional, governance-owned
```

All team projects: `ref('platform_project', 'certified_metrics')`

**Step 3 — Metric PR review process:**
- Any new metric or change requires review from:
  - Metric owner's team
  - Data governance representative
  - If "certified": Finance sign-off
- GitHub CODEOWNERS:
  ```
  models/metrics/company_metrics.yml @data-governance @finance
  ```

**Step 4 — Metric registry documentation:**
Generate a `metric_registry.md` from `manifest.json` and publish to Confluence weekly — showing all metrics, owners, definitions, and certification status.

**Step 5 — Deprecation workflow:**
```yaml
metrics:
  - name: old_revenue_calc
    meta:
      status: deprecated
      deprecated_date: "2024-03-01"
      replacement: net_revenue
      deprecation_notice: "Use net_revenue instead. This metric will be removed 2024-06-01."
```
