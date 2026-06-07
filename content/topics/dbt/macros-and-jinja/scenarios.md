---
title: "dbt Macros & Jinja - Scenarios"
topic: dbt
subtopic: macros-and-jinja
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, macros, jinja, interview, scenarios]
---

# dbt Macros & Jinja — Scenario Questions

## Scenario 1 (Junior): Write Your First Macro

**Situation:** You have 10 models that all need to cast `price_cents` (an integer) to `price_usd` (a decimal). You're repeating `ROUND(price_cents / 100.0, 2) AS price_usd` everywhere. Write a macro to DRY this up.

**Answer:**

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, alias=none) %}
    ROUND({{ column_name }} / 100.0, 2)
    {%- if alias is not none %} AS {{ alias }}{% endif %}
{% endmacro %}
```

Usage:
```sql
SELECT
    order_id,
    {{ cents_to_dollars('price_cents', 'price_usd') }},
    {{ cents_to_dollars('shipping_cents', 'shipping_usd') }},
    {{ cents_to_dollars('tax_cents', 'tax_usd') }}
FROM {{ source('raw', 'orders') }}
```

Now changing the rounding logic only requires editing one file.

---

## Scenario 2 (Mid-Level): Cross-Database Macro

**Situation:** Your company uses Snowflake in production but BigQuery for a separate analytics team. You need a macro `date_diff_days` that computes the difference in days between two dates, working on both platforms.

**Answer:**

```sql
-- macros/date_diff_days.sql
{% macro date_diff_days(start_date, end_date) %}
    {{ return(adapter.dispatch('date_diff_days', 'my_project')(start_date, end_date)) }}
{% endmacro %}

{% macro snowflake__date_diff_days(start_date, end_date) %}
    DATEDIFF('day', {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro bigquery__date_diff_days(start_date, end_date) %}
    DATE_DIFF({{ end_date }}, {{ start_date }}, DAY)
{% endmacro %}

{% macro postgres__date_diff_days(start_date, end_date) %}
    ({{ end_date }}::date - {{ start_date }}::date)
{% endmacro %}

{% macro default__date_diff_days(start_date, end_date) %}
    DATEDIFF({{ start_date }}, {{ end_date }})
{% endmacro %}
```

Usage (same SQL in both warehouses):
```sql
SELECT
    order_id,
    {{ date_diff_days('order_date', 'delivered_date') }} AS days_to_deliver
FROM {{ ref('fct_orders') }}
```

---

## Scenario 3 (Senior): Debug a Broken Macro

**Situation:** This macro is supposed to generate a UNION ALL of last 12 months of data, but it produces empty output. Find the bugs:

```sql
{% macro union_monthly_data(table_name) %}
    {% for i in range(12) %}
        {% set month = modules.datetime.date.today().month - i %}
        {% set year = modules.datetime.date.today().year %}
        SELECT * FROM {{ table_name }}_{{ year }}_{{ month }}
        {% if loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endmacro %}
```

**Answer — Three Bugs:**

**Bug 1:** `UNION ALL` is placed after the LAST item, but it should be placed between items (after all items EXCEPT the last):
```sql
-- Wrong: UNION ALL after last
{% if loop.last %}UNION ALL{% endif %}

-- Correct: UNION ALL after all except last
{% if not loop.last %}UNION ALL{% endif %}
```

**Bug 2:** Month arithmetic doesn't handle year boundary. `month - i` goes negative for January:
```sql
-- Wrong: month can go 0, -1, -2...
{% set month = modules.datetime.date.today().month - i %}

-- Correct: use timedelta
{% set d = modules.datetime.date.today() - modules.datetime.timedelta(days=30*i) %}
{% set month = d.month %}
{% set year = d.year %}
```

**Bug 3:** Month numbers like `3` should be zero-padded to `03` for table names:
```sql
-- Add zero-padding
{{ year }}_{{ '%02d' | format(month) }}
-- Or: {{ year }}_{{ month | string | zfill(2) }}
```

**Fixed macro:**
```sql
{% macro union_monthly_data(table_name) %}
    {% for i in range(12) %}
        {% set d = modules.datetime.date.today() - modules.datetime.timedelta(days=30*i) %}
        SELECT *, {{ d.year }} AS data_year, {{ d.month }} AS data_month
        FROM {{ table_name }}_{{ d.year }}_{{ '%02d' | format(d.month) }}
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endmacro %}
```
