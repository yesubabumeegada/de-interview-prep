---
title: "dbt Packages - Real World"
topic: dbt
subtopic: dbt-packages
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, packages, production, patterns, governance]
---

# dbt Packages — Real World Patterns

## Pattern 1: Using dbt-utils for Date Spine Gap Filling

**Problem:** Your events table has no rows on days with zero activity. Downstream dashboards show broken trend lines.

**Solution:** Use `dbt_utils.date_spine` to create a complete date series and left-join events.

```sql
-- models/marts/fct_daily_events.sql
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast(current_date as date)"
    ) }}
),

events_per_day as (
    select
        event_date,
        event_type,
        count(*) as event_count
    from {{ ref('stg_events') }}
    group by event_date, event_type
),

-- Cross join dates with all known event types so every combination appears
event_types as (
    select distinct event_type from {{ ref('stg_events') }}
),

date_type_spine as (
    select d.date_day, et.event_type
    from date_spine d
    cross join event_types et
)

select
    s.date_day as event_date,
    s.event_type,
    coalesce(e.event_count, 0) as event_count
from date_type_spine s
left join events_per_day e
    on s.date_day = e.event_date
    and s.event_type = e.event_type
order by s.date_day, s.event_type
```

## Pattern 2: Dynamic Pivot with `get_column_values`

**Problem:** A product table has a `category` column with ~20 possible values. The business wants a wide table with one column per category showing revenue. Values change quarterly.

**Solution:** Use `get_column_values` to auto-discover categories at compile time.

```sql
-- models/marts/fct_revenue_by_category.sql
{%- set categories = dbt_utils.get_column_values(
    table=ref('dim_products'),
    column='category',
    order_by='category'
) -%}

select
    order_date,
    {% for cat in categories %}
    sum(case when p.category = '{{ cat }}' then oi.revenue else 0 end)
        as {{ dbt_utils.slugify(cat) }}_revenue
    {%- if not loop.last -%},{%- endif %}
    {% endfor %}
from {{ ref('fct_order_items') }} oi
join {{ ref('dim_products') }} p using (product_id)
group by order_date
```

**Gotcha:** `get_column_values` fails if `dim_products` doesn't exist yet (first run). Handle with `--defer` or build the dim table first in CI.

## Pattern 3: Surrogate Keys Across Systems

**Problem:** Orders come from three systems (Shopify, Amazon, Direct) with overlapping order IDs.

```sql
-- models/staging/stg_unified_orders.sql
select
    {{ dbt_utils.generate_surrogate_key(['source_system', 'source_order_id']) }}
        as order_key,
    source_system,
    source_order_id,
    customer_id,
    order_date,
    total_amount
from {{ ref('int_orders_unioned') }}
```

Downstream models join on `order_key` — stable, unique, and system-agnostic.

## Pattern 4: dbt-expectations for SLA Monitoring

**Problem:** A finance report depends on payment data being loaded by 8 AM. You need automated alerting when data is stale.

```yaml
# models/staging/schema.yml
sources:
  - name: stripe
    tables:
      - name: charges
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
        loaded_at_field: created
        columns:
          - name: amount
            tests:
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: 0
                  max_value: 100000
                  config:
                    severity: error
          - name: status
            tests:
              - dbt_expectations.expect_column_values_to_be_in_set:
                  value_set: ['succeeded', 'pending', 'failed', 'refunded']
```

## Pattern 5: audit-helper in Blue-Green Deployments

**Problem:** You're rewriting a critical `fct_revenue` model. Business stakeholders need confidence the numbers match before cut-over.

**Workflow:**

```bash
# Step 1: Build new model alongside old one
dbt run --select fct_revenue_v2

# Step 2: Generate comparison report
dbt run-operation audit_helper.compare_relations --args '{
  "a_relation": {"schema": "dbt_prod", "identifier": "fct_revenue"},
  "b_relation": {"schema": "dbt_prod", "identifier": "fct_revenue_v2"},
  "primary_key": "revenue_id",
  "summarize": true
}'

# Step 3: Spot-check column-level differences
dbt run-operation audit_helper.compare_column_values --args '{
  "a_query": "select revenue_id, gross_revenue from analytics.dbt_prod.fct_revenue",
  "b_query": "select revenue_id, gross_revenue from analytics.dbt_prod.fct_revenue_v2",
  "primary_key": "revenue_id",
  "column_to_compare": "gross_revenue"
}'
```

## Pattern 6: codegen in Source Onboarding Workflow

**Problem:** Onboarding a new Salesforce integration. A raw schema has 40 tables. Manually writing YAML would take days.

```bash
# 1. Generate source YAML for all tables
dbt run-operation generate_source --args '{
  "schema_name": "raw_salesforce",
  "generate_columns": true,
  "include_descriptions": false,
  "table_names": ["account", "contact", "opportunity", "lead", "task"]
}' > sources/salesforce_generated.yml

# 2. Review and annotate descriptions
# 3. Generate model YAML for staging models
for model in stg_sf_account stg_sf_contact stg_sf_opportunity; do
  dbt run-operation generate_model_yaml --args "{\"model_name\": \"$model\"}"
done
```

## Production Gotchas

### `dbt deps` in CI/CD

Always run `dbt deps` before any dbt command in CI. Without it, macros from packages are missing.

```yaml
# .github/workflows/dbt.yml
steps:
  - name: Install dbt packages
    run: dbt deps
  - name: Run dbt
    run: dbt build --select state:modified+
```

### Package Lock File

As of dbt Core 1.7+, `dbt deps` generates a `package-lock.yml` file. Commit this to git to ensure reproducible installs.

```bash
# After running dbt deps, commit the lock file
git add package-lock.yml
git commit -m "chore: lock package versions"
```

### Namespace Collisions with Custom Generic Tests

If your project and a package both define a generic test with the same name, dbt will error. Use the full namespaced form:

```yaml
columns:
  - name: email
    tests:
      # Use full namespace to avoid ambiguity
      - dbt_expectations.expect_column_values_to_match_regex:
          regex: "^.+@.+$"
      # Not just:
      # - expect_column_values_to_match_regex  # which namespace?
```

## Real Interview Scenarios

**Scenario:** A team member added a new package that has a macro called `clean_string` — the same name as a macro you wrote. After `dbt deps`, your builds break. How do you fix it?

**Answer:** Add a `dispatch` block to `dbt_project.yml`:
```yaml
dispatch:
  - macro_namespace: <new_package_name>
    search_order: ['my_project', '<new_package_name>']
```
This ensures your project's `clean_string` takes precedence. Long-term, rename one macro or use the package namespace explicitly.

**Scenario:** `dbt_utils.get_column_values` is causing 30-second compile times in a large project. How would you optimize?

**Answer:** Several options: (1) Hard-code the values in a Jinja variable if they're stable. (2) Move the distinct query to a seed file or small lookup table. (3) Cache the result in a macro-level variable using `set`. (4) Run the macro against a pre-aggregated summary table rather than the raw fact table.
