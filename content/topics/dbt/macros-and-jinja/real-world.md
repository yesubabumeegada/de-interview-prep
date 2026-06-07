---
title: "dbt Macros & Jinja - Real-World"
topic: dbt
subtopic: macros-and-jinja
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, macros, production, automation, jinja]
---

# dbt Macros & Jinja — Real-World Examples

## Example 1: SCD Type 2 Macro

Reusable macro for slowly-changing dimension logic:

```sql
-- macros/scd2_merge.sql
{% macro scd2_merge(source_model, unique_key, tracked_columns) %}

WITH source AS (
    SELECT
        {{ unique_key }},
        {% for col in tracked_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
        CURRENT_TIMESTAMP() AS _valid_from,
        NULL AS _valid_to,
        TRUE AS _is_current
    FROM {{ source_model }}
),

target AS (
    SELECT * FROM {{ this }}
),

changes AS (
    SELECT s.*
    FROM source s
    LEFT JOIN target t
        ON s.{{ unique_key }} = t.{{ unique_key }} AND t._is_current = TRUE
    WHERE t.{{ unique_key }} IS NULL  -- new records
       OR {% for col in tracked_columns %}
              s.{{ col }} != t.{{ col }}
              {% if not loop.last %} OR {% endif %}
          {% endfor %}  -- changed records
)

SELECT * FROM changes

{% endmacro %}
```

## Example 2: Multi-Warehouse Column Type Macro

Handle TIMESTAMP differences across warehouses:

```sql
-- macros/type_timestamp.sql
{% macro type_timestamp() %}
    {{ return(adapter.dispatch('type_timestamp', 'my_project')()) }}
{% endmacro %}

{% macro snowflake__type_timestamp() %}TIMESTAMP_TZ{% endmacro %}
{% macro bigquery__type_timestamp() %}TIMESTAMP{% endmacro %}
{% macro redshift__type_timestamp() %}TIMESTAMPTZ{% endmacro %}
{% macro postgres__type_timestamp() %}TIMESTAMPTZ{% endmacro %}
{% macro default__type_timestamp() %}TIMESTAMP{% endmacro %}
```

Usage:
```sql
CAST(order_date AS {{ type_timestamp() }})
```

## Example 3: Dynamic Reporting Macro

Generate a rolling N-day aggregation for any metric:

```sql
-- macros/rolling_metric.sql
{% macro rolling_metric(metric_col, date_col, periods=[7, 30, 90]) %}
    {% for period in periods %}
        SUM(CASE
            WHEN {{ date_col }} >= CURRENT_DATE - {{ period }}
            THEN {{ metric_col }}
            ELSE 0
        END) AS {{ metric_col }}_last_{{ period }}_days
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
```

```sql
-- Usage in a model
SELECT
    customer_id,
    {{ rolling_metric('revenue', 'order_date', [7, 14, 30, 60, 90]) }}
FROM {{ ref('fct_orders') }}
GROUP BY customer_id
```

Renders to:
```sql
SELECT
    customer_id,
    SUM(CASE WHEN order_date >= CURRENT_DATE - 7 THEN revenue ELSE 0 END) AS revenue_last_7_days,
    SUM(CASE WHEN order_date >= CURRENT_DATE - 14 THEN revenue ELSE 0 END) AS revenue_last_14_days,
    ...
```

## Example 4: Auto-Grant Permissions Post-Run

```sql
-- macros/post_run_grants.sql
{% macro post_run_grants() %}
    {% set mart_relations = [] %}
    {% for result in results %}
        {% if result.node.fqn[1] == 'marts' and result.status == 'success' %}
            {% do mart_relations.append(result.node.relation_name) %}
        {% endif %}
    {% endfor %}

    {% for relation in mart_relations %}
        {% call statement('grant_' ~ loop.index) %}
            GRANT SELECT ON {{ relation }} TO ROLE REPORTER;
        {% endcall %}
        {{ log("Granted access to: " ~ relation, info=True) }}
    {% endfor %}
{% endmacro %}
```

```yaml
# dbt_project.yml
on-run-end:
  - "{{ post_run_grants() }}"
```

## Example 5: Environment-Aware Sampling

```sql
-- macros/sample_in_dev.sql
{% macro sample_in_dev(table_ref, sample_pct=10) %}
    {% if target.name in ['prod', 'ci'] %}
        {{ table_ref }}
    {% else %}
        (SELECT * FROM {{ table_ref }} SAMPLE ({{ sample_pct }}))
    {% endif %}
{% endmacro %}
```

```sql
-- stg_events.sql — 10% sample in dev for speed
SELECT * FROM {{ sample_in_dev(source('raw', 'events'), 5) }}
```

Dev runs: 5% sample → fast iteration
Prod runs: full table → complete data
