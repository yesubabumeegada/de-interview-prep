---
title: "Advanced Testing - Fundamentals"
topic: dbt
subtopic: advanced-testing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, testing, singular-tests, generic-tests, store_failures, severity]
---

# Advanced Testing — Fundamentals

## Two Types of dbt Tests

### 1. Generic Tests

Applied to models/columns via `schema.yml`. Built-in: `unique`, `not_null`, `accepted_values`, `relationships`. Package tests: everything from `dbt-expectations`, `dbt-utils`.

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered']
```

### 2. Singular Tests (Custom SQL)

SQL files in the `tests/` directory. A test FAILS if it returns any rows.

```sql
-- tests/orders_no_future_dates.sql
-- This test fails if any order has a date in the future

select order_id, order_date
from {{ ref('fct_orders') }}
where order_date > current_date
```

```sql
-- tests/revenue_matches_orders.sql
-- Verify total revenue in fct_revenue matches sum from fct_orders

select
    abs(
        (select sum(total_amount) from {{ ref('fct_orders') }}) -
        (select sum(revenue) from {{ ref('fct_revenue') }})
    ) as discrepancy
where discrepancy > 0.01  -- allow $0.01 rounding tolerance
```

**Rule:** If the query returns 0 rows → test passes. If it returns any rows → test fails.

## Test Severity: `warn` vs. `error`

By default, test failures stop the pipeline. Use `severity: warn` for non-blocking quality checks:

```yaml
models:
  - name: fct_orders
    columns:
      - name: email
        tests:
          - not_null:
              config:
                severity: warn   # logs a warning, pipeline continues
          - unique:
              config:
                severity: error  # (default) blocks pipeline on failure
```

## `warn_if` and `error_if` Thresholds

Instead of binary pass/fail, use thresholds:

```yaml
models:
  - name: stg_events
    columns:
      - name: user_id
        tests:
          - not_null:
              config:
                warn_if: ">10"    # warn if more than 10 nulls
                error_if: ">100"  # error if more than 100 nulls
```

This is essential for noisy data sources where a few nulls are acceptable but many indicate a pipeline problem.

## `store_failures: true`

When a test fails, dbt normally only reports how many rows failed. With `store_failures`, the actual failing rows are written to a table in your warehouse for inspection:

```yaml
models:
  - name: fct_orders
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [order_id, line_item_id]
          config:
            store_failures: true
            # Failing rows stored in: dbt_test__audit.unique_combination_fct_orders_...
```

```yaml
# Global: store all test failures
# dbt_project.yml
tests:
  +store_failures: true
  +store_failures_as: table   # or 'view' (default)
  +schema: test_failures      # custom schema name
```

**Inspect failed rows:**
```sql
select * from analytics.test_failures.not_null_fct_orders_email;
```

## Running Tests

```bash
# Run all tests
dbt test

# Test specific model
dbt test --select fct_orders

# Test with store_failures
dbt test --store-failures

# Run only generic tests
dbt test --select test_type:generic

# Run only singular tests
dbt test --select test_type:singular

# Test and show all results (don't stop on first failure)
dbt test --no-fail-fast
```

## Test Configuration in `dbt_project.yml`

```yaml
# dbt_project.yml
tests:
  my_project:
    +severity: warn       # All tests are warnings by default
    marts:
      +severity: error    # Mart tests are blocking
      +store_failures: true
    staging:
      +severity: warn
```

## Key Interview Points

**Q: What is a singular test in dbt?**
A: A custom SQL file in the `tests/` directory. The query returns failing rows — if any rows are returned, the test fails. If 0 rows are returned, it passes. Great for business logic validation that can't be expressed as a generic test.

**Q: What does `store_failures: true` do?**
A: Persists the actual failing rows from a test into a table in your warehouse (in a schema called `dbt_test__audit` by default). This lets you inspect which specific rows failed and debug data quality issues.

**Q: When would you use `severity: warn` instead of `severity: error`?**
A: For quality checks where some failures are expected or acceptable — noisy upstream sources, optional fields, or new tests being rolled out gradually. The pipeline continues but the failure is logged for investigation.

**Q: What is the difference between `warn_if` and `error_if`?**
A: They set row-count thresholds for warnings and errors respectively. For example, `warn_if: ">10"` warns when more than 10 rows fail the test condition, `error_if: ">100"` errors when more than 100 fail. This prevents a single bad row from blocking a pipeline while still alerting on significant issues.
