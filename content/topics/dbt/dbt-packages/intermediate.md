---
title: "dbt Packages - Intermediate"
topic: dbt
subtopic: dbt-packages
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, packages, dbt-utils, macros, dispatch]
---

# dbt Packages — Intermediate

## Deep Dive: dbt-utils Macros

### `pivot` and `unpivot`

**pivot** — transforms rows into columns:

```sql
-- stg_survey_responses.sql
select
    respondent_id,
    {{ dbt_utils.pivot(
        column='question_id',
        values=['q1', 'q2', 'q3', 'q4'],
        agg='max',
        then_value='answer',
        else_value='null',
        quote_identifiers=false
    ) }}
from {{ ref('raw_survey') }}
group by respondent_id
```

**unpivot** — transforms columns into rows:

```sql
-- wide_to_long.sql
{{ dbt_utils.unpivot(
    relation=ref('fct_monthly_revenue'),
    cast_to='numeric',
    exclude=['customer_id', 'year'],
    remove=['_metadata_col'],
    field_name='month',
    value_name='revenue'
) }}
```

### `get_column_values` — Dynamic SQL at Compile Time

```sql
-- Dynamically list all distinct values from a column at compile time
{% set payment_methods = dbt_utils.get_column_values(
    table=ref('stg_payments'),
    column='payment_method'
) %}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
    {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
```

### `generate_surrogate_key` — Correct Usage

```sql
select
    -- Concatenates with || and MD5 hashes
    {{ dbt_utils.generate_surrogate_key(['order_id', 'line_item']) }} as order_line_key,
    order_id,
    line_item,
    quantity,
    unit_price
from {{ ref('stg_order_lines') }}
```

Note: nulls are replaced with the string `'_dbt_utils_surrogate_key_null_'` before hashing to avoid null propagation.

### `star` — Select All Except

```sql
-- Select all columns from a model except audit columns
select
    {{ dbt_utils.star(ref('stg_orders'), except=['_fivetran_synced', '_fivetran_deleted']) }}
from {{ ref('stg_orders') }}
```

## Macro Dispatch — Deep Dive

### Why Dispatch Exists

Multiple packages (or your project + a package) may define the same macro. Dispatch controls the search order.

### Configuring Dispatch

```yaml
# dbt_project.yml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['my_project', 'dbt_utils']

  - macro_namespace: dbt
    search_order: ['my_project', 'dbt']
```

### Overriding a Package Macro

Example: override `dbt_utils.safe_add` to log usage:

```sql
-- macros/dbt_utils/safe_add.sql
{% macro safe_add(field_list) -%}
    {# Custom override — calls the original via explicit namespace #}
    {{ return(dbt_utils.safe_add(field_list)) }}
{%- endmacro %}
```

### Adapter Dispatch

dbt's adapter dispatch lets macros have database-specific implementations:

```sql
-- macros/my_concat.sql
{% macro my_concat(fields) %}
    {{ return(adapter.dispatch('my_concat', 'my_project')(fields)) }}
{% endmacro %}

{% macro default__my_concat(fields) %}
    concat({{ fields | join(', ') }})
{% endmacro %}

{% macro snowflake__my_concat(fields) %}
    -- Snowflake supports concat_ws for cleaner syntax
    concat_ws('', {{ fields | join(', ') }})
{% endmacro %}
```

## dbt-expectations: Advanced Patterns

### Row-Level Conditional Tests

```yaml
models:
  - name: fct_orders
    columns:
      - name: shipped_at
        tests:
          # Only non-null for shipped orders
          - dbt_expectations.expect_column_values_to_not_be_null:
              row_condition: "status = 'shipped'"
      - name: refund_amount
        tests:
          # Refund amount should only exist for refunded orders
          - dbt_expectations.expect_column_values_to_be_null:
              row_condition: "status != 'refunded'"
```

### Table-Level Row Count Monitoring

```yaml
models:
  - name: fct_events
    tests:
      - dbt_expectations.expect_table_row_count_to_equal_other_table:
          compare_model: ref('stg_events')
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 50000
          max_value: 5000000
          # Fail only if off by more than 10%
```

### Distribution Tests

```yaml
      - name: session_duration_seconds
        tests:
          - dbt_expectations.expect_column_mean_to_be_between:
              min_value: 30
              max_value: 3600
          - dbt_expectations.expect_column_stdev_to_be_between:
              min_value: 0
              max_value: 600
          - dbt_expectations.expect_column_quantile_values_to_be_between:
              quantile: 0.95
              min_value: 0
              max_value: 7200
```

## dbt-audit-helper: Validation Workflows

### Comparing Models After Refactoring

```bash
# Step 1: Build old model with a new name
dbt run --select old_model --vars '{"model_suffix": "_old"}'

# Step 2: Compare old vs new using audit_helper
dbt run-operation audit_helper.compare_relations --args '{
  "a_relation": {"database": "analytics", "schema": "dbt_prod", "identifier": "fct_orders_old"},
  "b_relation": {"database": "analytics", "schema": "dbt_prod", "identifier": "fct_orders"},
  "primary_key": "order_id",
  "summarize": true
}'
```

### `compare_column_values`

```sql
-- analyses/audit_refactor.sql
{{ audit_helper.compare_column_values(
    a_query=ref('old_revenue_calc'),
    b_query=ref('new_revenue_calc'),
    primary_key='order_id',
    column_to_compare='total_revenue'
) }}
```

Output shows: matching rows, rows only in A, rows only in B, rows with different values.

## dbt-codegen: Workflow Integration

### Generating Sources YAML

```bash
# Generate YAML for all tables in a schema
dbt run-operation generate_source \
  --args '{
    "schema_name": "raw_stripe",
    "database_name": "analytics",
    "table_names": ["charges", "customers", "invoices"],
    "generate_columns": true,
    "include_descriptions": true
  }'
```

### Generating Model YAML

```bash
# Generate column-level YAML for an existing model
dbt run-operation generate_model_yaml \
  --args '{
    "model_name": "stg_stripe_charges",
    "upstream_descriptions": true
  }'
```

This is especially useful when onboarding new sources — paste the output into `schema.yml` and fill in descriptions.

## Package Version Management

### Checking for Outdated Packages

```bash
# dbt does not have a built-in "outdated" command
# Check hub.getdbt.com or use:
cat dbt_packages/dbt_utils/dbt_project.yml | grep version
```

### Handling Breaking Changes

When upgrading packages, check the CHANGELOG for:
- Renamed macros
- Changed argument signatures  
- Removed macros

```yaml
# Safe upgrade pattern: test in a branch first
packages:
  - package: dbt-labs/dbt_utils
    version: 1.2.0   # upgrading from 1.1.1
```

## Interview Deep-Dive Questions

**Q: What is the difference between `dbt_utils.surrogate_key` and `dbt_utils.generate_surrogate_key`?**
A: They are aliases for the same macro. `generate_surrogate_key` is the original name; `surrogate_key` was added as a convenience alias.

**Q: When would you use `dbt_utils.get_column_values` vs a hard-coded list?**
A: `get_column_values` runs a `select distinct` at compile time, so the query dynamically adapts as new values appear. Use it when the set of values grows over time. Downside: it runs an extra query at compile time and requires the table to exist.

**Q: How does dbt-expectations differ from built-in dbt tests?**
A: Built-in tests cover basic cases (not_null, unique, accepted_values, relationships). dbt-expectations adds statistical, regex, and distributional tests, with conditional logic via `row_condition`. It follows a Great Expectations naming convention.

**Q: What happens if two packages define a macro with the same name?**
A: dbt uses the dispatch system. Without explicit dispatch configuration, it searches packages in the order they appear in `packages.yml`. Configure `dispatch` in `dbt_project.yml` to control priority explicitly.
