---
title: "dbt Macros & Jinja - Intermediate"
topic: dbt
subtopic: macros-and-jinja
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, macros, jinja, adapter-dispatch, cross-database]
---

# dbt Macros & Jinja — Intermediate

## Adapter Dispatch (Cross-Database Macros)

Write macros that work across Snowflake, BigQuery, Redshift:

```sql
-- macros/get_current_timestamp.sql
{% macro get_current_timestamp() %}
    {{ return(adapter.dispatch('get_current_timestamp', 'my_project')()) }}
{% endmacro %}

{% macro snowflake__get_current_timestamp() %}
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
{% endmacro %}

{% macro bigquery__get_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

{% macro redshift__get_current_timestamp() %}
    GETDATE() AT TIME ZONE 'UTC'
{% endmacro %}

{% macro default__get_current_timestamp() %}
    CURRENT_TIMESTAMP
{% endmacro %}
```

Usage in any model:
```sql
SELECT {{ get_current_timestamp() }} AS loaded_at
```

## Dynamic Column Generation

Generate columns from a list:

```sql
-- macros/generate_columns.sql
{% macro pivot_status_counts(status_values) %}
    {% for status in status_values %}
        SUM(CASE WHEN status = '{{ status }}' THEN 1 ELSE 0 END)
            AS {{ status | replace('-', '_') }}_count
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
```

```sql
-- Usage in a model
SELECT
    customer_id,
    {{ pivot_status_counts(['pending', 'shipped', 'delivered', 'cancelled']) }}
FROM {{ ref('fct_orders') }}
GROUP BY customer_id
```

Renders to:
```sql
SELECT
    customer_id,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
    SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) AS shipped_count,
    ...
```

## Materialization Macros

```sql
-- macros/materializations/snapshot_merge.sql
{% materialization snapshot_merge, adapter='snowflake' %}
    {%- set target_relation = this -%}

    {% call statement('main') %}
        MERGE INTO {{ target_relation }} target
        USING {{ sql }} source
        ON target.{{ unique_key }} = source.{{ unique_key }}
        WHEN MATCHED THEN UPDATE SET
            {% for col in columns %}
                target.{{ col.name }} = source.{{ col.name }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        WHEN NOT MATCHED THEN INSERT
            ({{ columns | map(attribute='name') | join(', ') }})
            VALUES ({{ columns | map(attribute='name') | join(', ') }})
    {% endcall %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
```

## Graph Operations in Macros

Access the dbt graph (DAG) from within macros:

```sql
-- macros/get_models_with_tag.sql
{% macro get_models_with_tag(tag) %}
    {% set models = [] %}
    {% for node in graph.nodes.values() %}
        {% if tag in node.tags and node.resource_type == 'model' %}
            {% do models.append(node.name) %}
        {% endif %}
    {% endfor %}
    {{ return(models) }}
{% endmacro %}
```

```sql
-- macros/union_tagged_models.sql
{% macro union_tagged_models(tag) %}
    {% set models = get_models_with_tag(tag) %}
    {% for model in models %}
        SELECT *, '{{ model }}' AS _source_model
        FROM {{ ref(model) }}
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endmacro %}
```

## Hooks as Macros

```sql
-- macros/grant_access.sql
{% macro grant_access(role='REPORTER') %}
    GRANT SELECT ON {{ this }} TO ROLE {{ role }};
{% endmacro %}
```

```yaml
# dbt_project.yml
models:
  my_project:
    marts:
      +post_hook: "{{ grant_access('REPORTER') }}"
```

## Exception Handling

```sql
{% macro require_env_var(var_name) %}
    {% set value = env_var(var_name, '') %}
    {% if value == '' %}
        {{ exceptions.raise_compiler_error(
            "Required environment variable " ~ var_name ~ " is not set!"
        ) }}
    {% endif %}
    {{ return(value) }}
{% endmacro %}
```

## Log and Debug

```sql
{% macro debug_macro(value) %}
    {{ log("DEBUG: " ~ value, info=True) }}
{% endmacro %}

-- Usage inside a model
{% if target.name == 'dev' %}
    {{ debug_macro("Processing " ~ this.name) }}
{% endif %}
```

## Jinja Filters

Built-in Jinja filters for string manipulation:

```sql
{% set table_name = 'My Table Name' %}
{{ table_name | lower }}           -- my table name
{{ table_name | replace(' ', '_') }} -- My_Table_Name
{{ table_name | lower | replace(' ', '_') }} -- my_table_name

{% set columns = ['id', 'name', 'email'] %}
{{ columns | join(', ') }}         -- id, name, email
{{ columns | length }}             -- 3
{{ columns | first }}              -- id
{{ columns | last }}               -- email
{{ columns | sort }}               -- ['email', 'id', 'name']
```

## Namespace Trick for Loop Variables

```sql
-- Problem: loop variable can't be modified inside loop
{% set ns = namespace(found=false) %}
{% for col in columns %}
    {% if col.name == 'id' %}
        {% set ns.found = true %}
    {% endif %}
{% endfor %}
{% if ns.found %}Primary key found{% endif %}
```
