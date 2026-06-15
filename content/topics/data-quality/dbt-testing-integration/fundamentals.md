---
title: "dbt Testing Integration - Fundamentals"
topic: data-quality
subtopic: dbt-testing-integration
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [data-quality, dbt, testing, great-expectations, data-validation]
---

# dbt Testing Integration — Fundamentals

## Analogy

Think of dbt tests like unit tests for your SQL transformations. Just as pytest checks that your Python functions return the right values, dbt tests check that your transformed data meets business rules — every time a pipeline runs.

---

## What Are dbt Tests?

dbt has a built-in testing framework that lets you assert properties about your models and sources. Tests run after models are built and produce pass/fail results that can block downstream work or trigger alerts.

There are two categories:

| Category | Description | Example |
|---|---|---|
| **Generic tests** | Pre-built, configured in YAML | `not_null`, `unique`, `accepted_values`, `relationships` |
| **Singular tests** | Custom SQL files that return failing rows | Any complex SQL assertion |

---

## The Four Built-In Generic Tests

### `not_null`
Asserts that a column contains no null values.

```yaml
# models/schema.yml
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - not_null
      - name: customer_id
        tests:
          - not_null
```

### `unique`
Asserts that all values in a column are distinct.

```yaml
      - name: order_id
        tests:
          - unique
```

### `accepted_values`
Asserts that all values belong to a defined list.

```yaml
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'delivered', 'cancelled']
```

### `relationships`
Asserts referential integrity — every value in a column exists in another table's column.

```yaml
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: customer_id
```

---

## Running Tests

```bash
# Run all tests in the project
dbt test

# Test a specific model
dbt test --select orders

# Test a model and all its upstream dependencies
dbt test --select +orders

# Test a model and all its downstream dependents
dbt test --select orders+

# Exclude a model from tests
dbt test --exclude staging_legacy

# Run only tests tagged 'critical'
dbt test --select tag:critical
```

---

## Source Freshness Checks

Source freshness checks validate that your raw data is arriving on time. Configure in `sources.yml`:

```yaml
# models/sources.yml
sources:
  - name: raw_payments
    database: raw
    schema: payments
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: transactions
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 6, period: hour}
```

```bash
# Run freshness checks
dbt source freshness
```

This checks the `MAX(_loaded_at)` value and compares it to wall clock time.

---

## Singular Tests (Custom SQL Assertions)

Create a `.sql` file in the `tests/` directory. The query should return **zero rows** for the test to pass — any returned row is a failure.

```sql
-- tests/assert_orders_total_matches_line_items.sql
-- Fails if order total doesn't match sum of line items

SELECT
    o.order_id,
    o.total_amount,
    SUM(li.amount) AS line_items_total
FROM {{ ref('orders') }} o
JOIN {{ ref('order_line_items') }} li ON o.order_id = li.order_id
GROUP BY 1, 2
HAVING ABS(o.total_amount - SUM(li.amount)) > 0.01
```

---

## Severity: warn vs error

By default, a failing test stops the pipeline (`error`). Set `severity: warn` to log the failure without halting execution.

```yaml
      - name: email
        tests:
          - not_null:
              severity: warn
          - unique:
              severity: error  # default
```

Use `warn` for:
- Non-critical quality checks during rollout
- Metrics that can tolerate small imperfections
- Monitoring rather than enforcement

Use `error` for:
- Primary keys and referential integrity
- Financial amounts and compliance data
- Any column downstream consumers rely on

---

## Storing Test Failures

Enable `store_failures: true` to persist failing rows as tables in your warehouse for debugging.

```yaml
# dbt_project.yml
tests:
  my_project:
    +store_failures: true
    +schema: dbt_test_failures
```

Or per-test:

```yaml
      - name: order_id
        tests:
          - unique:
              config:
                store_failures: true
                store_failures_as: table  # or 'view' (dbt 1.7+)
```

Failing rows appear in a schema like `dbt_test_failures.not_null_orders_order_id`.

---

## Key Interview Points

- dbt tests are `SELECT` statements — a test passes when the query returns **0 rows**
- Generic tests are YAML-configured; singular tests are custom SQL files in `tests/`
- Source freshness checks use `dbt source freshness` with `loaded_at_field`
- `store_failures: true` is essential for debugging test failures in production
- `severity: warn` lets you monitor without breaking pipelines during initial rollout
- Test selection with `--select` follows the same node selection syntax as `dbt run`
