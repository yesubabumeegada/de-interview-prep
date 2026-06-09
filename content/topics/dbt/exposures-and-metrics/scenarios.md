---
title: "dbt Exposures & Metrics - Scenarios"
topic: dbt
subtopic: exposures-and-metrics
content_type: scenario_question
tags: [dbt, exposures, metrics, interview, scenarios]
---

# dbt Exposures & Metrics — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Adding an Exposure for a New Dashboard

**Scenario:** Your team just shipped a new Tableau dashboard called "Customer 360" that uses `dim_customers` and `fct_orders`. Add an appropriate exposure definition.

<details>
<summary>💡 Hint</summary>

An exposure documents downstream consumers of your dbt models. Include the type, URL, owner, maturity, and which dbt models it depends on using `ref()`.

</details>

<details>
<summary>✅ Solution</summary>

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

This exposure now appears in `dbt docs` lineage, showing that `dim_customers` and `fct_orders` are consumed by this dashboard. When you run `dbt ls --select +exposure:customer_360_dashboard`, you see exactly which models feed it.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Fixing Inconsistent Revenue Numbers Across Teams

**Scenario:** At a board meeting, the CFO presents revenue of $5.2M for Q3. The Sales VP has a Tableau dashboard showing $5.8M. The Marketing team's Python analysis shows $5.0M. Everyone is using `fct_orders` but calculating differently. How do you fix this architecturally?

<details>
<summary>💡 Hint</summary>

The root cause is divergent metric definitions. The fix is a single certified metric definition in the dbt Semantic Layer so all tools query the same canonical calculation.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Establishing Metric Governance at Scale

**Scenario:** Your data team has grown from 3 to 30 people. There are now 200+ metrics defined across 15 different dbt projects. Finance and Marketing define "active customer" differently. How do you establish metric governance?

<details>
<summary>💡 Hint</summary>

The answer has three parts: a tiered certification system (experimental → certified → deprecated), a central metrics repository in the platform project, and a PR-based review process with CODEOWNERS enforcement.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is a dbt exposure and why does it matter?" — An exposure documents who consumes your dbt models downstream (dashboards, ML models, APIs). It appears in lineage so you can see the full impact of a model change before making it, and know who to notify.

> **Tip 2:** "How would you solve inconsistent metrics across teams?" — The answer is the dbt Semantic Layer. Define one certified metric with one filter definition; all tools query via MetricFlow and get the same number. Without it, teams diverge by adding different WHERE clauses to the same base table.

> **Tip 3:** "How do you govern metrics at scale?" — Use a tiered certification system: experimental → certified → deprecated. Certified metrics live in the platform project, require PR review by the governance team, and are versioned. Every metric has an owner who approves changes. Automated CI blocks uncertified metrics from being used in reports.
