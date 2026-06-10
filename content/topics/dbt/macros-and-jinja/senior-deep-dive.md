---
title: "dbt Macros & Jinja - Senior Deep Dive"
topic: dbt
subtopic: macros-and-jinja
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, macros, advanced-jinja, codegen, automation]
---

# dbt Macros & Jinja — Senior Deep Dive

## Code Generation with dbt-codegen

Automate boilerplate with the codegen package:

```bash
# Install dbt-codegen
# packages.yml: dbt-labs/codegen version 0.12.0
```

```bash
# Generate source YAML from existing tables
dbt run-operation generate_source \
  --args '{"schema_name": "shopify", "table_names": ["orders", "customers"]}'

# Generate model YAML with all columns
dbt run-operation generate_model_yaml \
  --args '{"model_names": ["stg_orders", "fct_orders"]}'

# Generate base staging model from source
dbt run-operation generate_base_model \
  --args '{"source_name": "shopify", "table_name": "orders"}'
```

Output (generate_base_model):
```sql
with source as (
    select * from {{ source('shopify', 'orders') }}
),
renamed as (
    select
        id,
        customer_id,
        total_price,
        created_at,
        updated_at,
        financial_status,
        fulfillment_status
    from source
)
select * from renamed
```

## Meta-Programming: Self-Documenting Models

Generate `schema.yml` entries programmatically from the catalog:

```sql
-- macros/generate_schema_for_model.sql
{% macro generate_schema_for_model(model_name) %}
    {% set relation = ref(model_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set output %}
models:
  - name: {{ model_name }}
    description: "TODO: Add description"
    columns:
    {% for col in columns %}
      - name: {{ col.name | lower }}
        description: "TODO"
        data_type: {{ col.dtype }}
    {% endfor %}
    {% endset %}

    {{ log(output, info=True) }}
{% endmacro %}
```

## Advanced: Dynamic Model Generation

Generate multiple models from a configuration file:

```yaml
# models/generated/sources.yml (config, not a dbt source file)
entity_sources:
  - name: orders
    source: shopify
    columns: [id, customer_id, total_price, status, created_at]
    partition_by: created_at
  - name: customers
    source: shopify
    columns: [id, email, name, country, created_at]
```

```sql
-- macros/generate_staging_models.sql
{% macro generate_staging_models() %}
    {% set config = fromyaml(load_file('models/generated/sources.yml')) %}
    {% for entity in config.entity_sources %}
        -- Write to file
        {% set model_sql %}
WITH source AS (SELECT * FROM {{ "{{" }} source('{{ entity.source }}', '{{ entity.name }}') {{ "}}" }})
SELECT
    {% for col in entity.columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM source
        {% endset %}
        {{ log("-- " ~ entity.name ~ ":\n" ~ model_sql, info=True) }}
    {% endfor %}
{% endmacro %}
```

## Compile-Time SQL Execution Pattern

```sql
-- macros/get_relation_columns.sql
{% macro get_relation_columns(relation) %}
    {% if execute %}
        {% set columns = adapter.get_columns_in_relation(relation) %}
        {{ return(columns) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
```

The `{% if execute %}` guard is critical — macros run during compile AND execute phases. `adapter.get_columns_in_relation()` should only run during execute.

## Operation Macros

Macros that run as standalone operations (not tied to a model):

```sql
-- macros/operations/cleanup_test_schemas.sql
{% macro cleanup_test_schemas() %}
    {% set ci_schemas_query %}
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'ci_%'
        AND created < DATEADD('day', -7, CURRENT_DATE)
    {% endset %}

    {% set schemas = run_query(ci_schemas_query) %}

    {% if execute %}
        {% for schema in schemas.columns[0].values() %}
            {{ log("Dropping schema: " ~ schema, info=True) }}
            {% call statement('drop_schema_' ~ loop.index) %}
                DROP SCHEMA IF EXISTS {{ schema }} CASCADE
            {% endcall %}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

Run as: `dbt run-operation cleanup_test_schemas`

## Macro Testing with Unit Tests

```yaml
# models/schema.yml - test a macro's effect on a model
unit_tests:
  - name: test_cents_to_dollars_macro
    model: stg_payments
    given:
      - input: source('raw', 'payments')
        rows:
          - {payment_id: 1, amount_cents: 9999}
          - {payment_id: 2, amount_cents: 100}
          - {payment_id: 3, amount_cents: 0}
    expect:
      rows:
        - {payment_id: 1, amount_usd: 99.99}
        - {payment_id: 2, amount_usd: 1.00}
        - {payment_id: 3, amount_usd: 0.00}
```

## Performance: Macro Compilation Overhead

Large projects with complex macros can have slow compile times. Profile with:

```bash
# Time the compile step separately
time dbt compile --select fct_orders

# Enable debug logging to see macro expansion
dbt compile --select fct_orders --log-level debug 2>&1 | grep "Rendering"
```

Tips to reduce compile time:
1. Avoid `run_query()` during compile phase (use `{% if execute %}` guard)
2. Cache expensive lookups in variables: `{% set columns = ... %}` once, reuse
3. Avoid deeply nested macro calls (> 5 levels)
4. Use `dbt ls` instead of `dbt compile` for graph inspection

## ⚡ Cheat Sheet

**Jinja essentials**
```jinja
{{ ref('model_name') }}           -- compiled to full table path
{{ source('schema', 'table') }}   -- source reference with freshness
{{ config(materialized='table') }}-- model config
{{ var('run_date', '2024-01-01') }}-- project variable with default
{{ env_var('DBT_ENV', 'dev') }}   -- environment variable
```

**Macro structure**
```sql
{% macro generate_surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

{% macro cents_to_dollars(column) %}
    ({{ column }} / 100.0)::numeric(10,2)
{% endmacro %}
```

**Control flow**
```jinja
{% if target.name == 'prod' %}
    -- production-only logic
{% elif target.name == 'dev' %}
    LIMIT 1000
{% endif %}

{% for col in columns %}
    {{ col }} {% if not loop.last %},{% endif %}
{% endfor %}
```

**`run_query` for meta-programming**
```sql
{% set results = run_query("SELECT DISTINCT region FROM " ~ ref('regions')) %}
{% set regions = results.columns[0].values() %}
{% for region in regions %}
    SELECT '{{ region }}' as region, COUNT(*) FROM {{ ref('sales') }}
    WHERE region = '{{ region }}'
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
```

**dispatch (adapter polymorphism)**
```sql
-- Use adapter-specific implementation
{{ adapter.dispatch('my_macro', 'my_package')() }}
-- dbt resolves: my_package.bigquery__my_macro → my_package.default__my_macro
```

**Package ecosystem**
- `dbt_utils`: `surrogate_key`, `pivot`, `date_spine`, `get_column_values`
- `dbt_expectations`: GE-style test macros (`expect_column_values_to_be_between`)
- `dbt_audit_helper`: compare model results before/after refactor
- `codegen`: auto-generate source YAML from warehouse introspection
