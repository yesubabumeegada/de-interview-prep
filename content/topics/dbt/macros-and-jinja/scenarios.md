---
title: "dbt Macros & Jinja - Scenarios"
topic: dbt
subtopic: macros-and-jinja
content_type: scenario_question
tags: [dbt, macros, jinja, interview, scenarios]
---

# dbt Macros & Jinja — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Writing a DRY Macro to Eliminate Repeated Logic

**Scenario:** You have 10 models that all need to cast `price_cents` (an integer) to `price_usd` (a decimal). You're repeating `ROUND(price_cents / 100.0, 2) AS price_usd` everywhere. Write a macro to DRY this up.

<details>
<summary>💡 Hint</summary>

Create a macro that accepts the column name as a parameter, and optionally an alias. Use Jinja's `if` block to conditionally add the `AS alias` part.

</details>

<details>
<summary>✅ Solution</summary>

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

Now changing the rounding logic only requires editing one file. If you want 4 decimal places later, change it in one place.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Writing a Cross-Database Macro

**Scenario:** Your company uses Snowflake in production but BigQuery for a separate analytics team. You need a macro `date_diff_days` that computes the difference in days between two dates, working on both platforms.

<details>
<summary>💡 Hint</summary>

Use `adapter.dispatch()` — dbt's dispatch mechanism automatically routes to the correct platform-specific implementation based on the target adapter name.

</details>

<details>
<summary>✅ Solution</summary>

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

dbt automatically selects the correct implementation at compile time based on which adapter is active.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Debugging a Broken Macro

**Scenario:** This macro is supposed to generate a UNION ALL of the last 12 months of data, but it produces empty output. Find and fix all the bugs:

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

<details>
<summary>💡 Hint</summary>

There are three bugs: the UNION ALL placement, year-boundary arithmetic, and zero-padding of month numbers. Work through the logic for December/January to find the boundary bug.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

---

## Interview Tips

> **Tip 1:** "When should you write a macro vs just repeat SQL?" — Write a macro when the same logic appears in 3+ models and involves non-trivial transformation (type casting, date math, conditional logic). Simple column aliases don't need macros. The test: if you change the business rule, how many files do you need to touch?

> **Tip 2:** "How do you handle warehouse-specific SQL in dbt?" — Use `adapter.dispatch()`. Define a default implementation and platform-specific overrides (prefixed with `snowflake__`, `bigquery__`, etc.). dbt selects the right one at compile time. This is how dbt-utils handles cross-database compatibility.

> **Tip 3:** "Debug a macro that produces wrong output." — First run `dbt compile` and inspect the compiled SQL. Common Jinja bugs: wrong loop condition (`loop.last` vs `not loop.last`), year-boundary arithmetic, missing zero-padding for dates, and variable scoping issues inside loops.
