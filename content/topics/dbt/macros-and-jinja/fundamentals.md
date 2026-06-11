---
title: "dbt Macros & Jinja"
topic: dbt
subtopic: macros-and-jinja
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, macros, jinja, templating, reusability]
---

# dbt Macros & Jinja


## 🎯 Analogy

Think of dbt macros like functions in SQL: write the logic once (how to generate a date spine, how to pivot columns), call it everywhere, and changing the macro updates every model that uses it.

---
## What Is Jinja?

Jinja is a Python-based templating language embedded in dbt SQL files. It enables dynamic SQL generation using variables, conditionals, and loops.

```sql
-- Jinja expressions use {{ }} for values
SELECT {{ 1 + 1 }}  -- renders to: SELECT 2

-- Jinja statements use {% %} for control flow
{% if condition %}
    SELECT 'yes'
{% else %}
    SELECT 'no'
{% endif %}

-- Jinja comments use {# #}
{# This is a comment — not rendered in SQL #}
```

## Built-In Jinja Variables

```sql
-- Target information
{{ target.name }}      -- 'dev', 'prod', 'ci'
{{ target.schema }}    -- 'dbt_jsmith'
{{ target.database }}  -- 'DEV_DB'
{{ target.type }}      -- 'snowflake', 'bigquery'
{{ target.threads }}   -- 4

-- Project information
{{ project_name }}     -- 'my_dbt_project'
{{ dbt_version }}      -- '1.7.0'

-- Model information (inside a model)
{{ this }}             -- current model relation
{{ this.name }}        -- 'fct_orders'
{{ this.schema }}      -- 'analytics'
{{ this.database }}    -- 'PROD_DB'

-- Invocation
{{ invocation_id }}    -- unique run ID (UUID)
{{ run_started_at }}   -- timestamp of run start
```

## What Is a Macro?

A macro is a reusable piece of Jinja-templated SQL, defined in `.sql` files under `macros/`:

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ROUND({{ column_name }} / 100.0, 2)
{% endmacro %}
```

Usage in a model:
```sql
SELECT
    order_id,
    {{ cents_to_dollars('price_cents') }} AS price_usd
FROM {{ source('raw', 'orders') }}
```

Rendered SQL:
```sql
SELECT
    order_id,
    ROUND(price_cents / 100.0, 2) AS price_usd
FROM raw.public.orders
```

## Macro with Multiple Arguments

```sql
-- macros/safe_divide.sql
{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
        THEN {{ default }}
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
```

```sql
-- Usage
SELECT
    {{ safe_divide('revenue', 'orders') }} AS avg_order_value,
    {{ safe_divide('revenue', 'sessions', 'NULL') }} AS revenue_per_session
FROM metrics
```

## Conditionals

```sql
-- models/staging/stg_events.sql
SELECT
    event_id,
    user_id,
    event_type,
    {% if target.name == 'prod' %}
        event_timestamp,
        user_properties
    {% else %}
        -- Mask PII in non-production
        event_timestamp,
        NULL AS user_properties
    {% endif %}
FROM {{ source('raw', 'events') }}
```

## Loops

```sql
-- macros/union_tables.sql
-- Generate a UNION ALL for multiple years
{% macro union_yearly_tables(table_prefix, start_year, end_year) %}
    {% for year in range(start_year, end_year + 1) %}
        SELECT *, {{ year }} AS data_year
        FROM {{ table_prefix }}_{{ year }}
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endmacro %}
```

```sql
-- Usage in a model
{{ union_yearly_tables('raw.orders', 2020, 2024) }}
```

## is_incremental()

Special dbt macro that returns `True` during incremental runs:

```sql
SELECT * FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
    -- Only load new records during incremental runs
    WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}
```

## run_query()

Execute SQL and use results in Jinja logic:

```sql
{% macro get_max_date(table, column) %}
    {% set query %}
        SELECT MAX({{ column }}) FROM {{ table }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}
```

```sql
-- Usage
{% set max_date = get_max_date(ref('fct_orders'), 'order_date') %}
SELECT * FROM source WHERE order_date > '{{ max_date }}'
```

## Common Built-In Macros

| Macro | Purpose |
|---|---|
| `ref('model')` | Reference another model |
| `source('src', 'table')` | Reference a source table |
| `config(...)` | Set model configuration |
| `this` | Current model relation |
| `is_incremental()` | True during incremental runs |
| `env_var('VAR')` | Read environment variable |
| `var('name')` | Read project variable |
| `log('msg', info=True)` | Print to console |

## ▶️ Try It Yourself

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)::NUMERIC(12,2)
{% endmacro %}

-- Use in any model:
-- SELECT {{ cents_to_dollars('price_cents') }} AS price_usd

-- macros/generate_date_spine.sql example usage:
{% macro last_n_days(n) %}
    DATEADD(day, -{{ n }}, CURRENT_DATE)
{% endmacro %}

-- In a model: WHERE order_date >= {{ last_n_days(30) }}
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
