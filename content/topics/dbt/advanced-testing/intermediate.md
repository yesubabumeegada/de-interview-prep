---
title: "Advanced Testing - Intermediate"
topic: dbt
subtopic: advanced-testing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, testing, generic-tests, dbt-expectations, custom-tests, tagging]
---

# Advanced Testing — Intermediate

## Custom Generic Tests

Generic tests are macros that accept parameters and return SQL. You define them once and apply to any model/column.

### Anatomy of a Generic Test

```sql
-- macros/tests/is_valid_email.sql
{% test is_valid_email(model, column_name) %}
    -- Returns rows that fail the email validation
    select {{ column_name }} as invalid_email
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not like '%@%.%'
      -- More complete regex would go here
{% endtest %}
```

**Apply it:**
```yaml
models:
  - name: dim_customers
    columns:
      - name: email
        tests:
          - is_valid_email
```

### Generic Test with Parameters

```sql
-- macros/tests/is_between.sql
{% test is_between(model, column_name, min_value, max_value) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and (
        {{ column_name }} < {{ min_value }}
        or {{ column_name }} > {{ max_value }}
      )
{% endtest %}
```

```yaml
columns:
  - name: discount_pct
    tests:
      - is_between:
          min_value: 0
          max_value: 100
```

### Generic Test with `model` as a Relation

```sql
-- macros/tests/row_count_within_percent_of.sql
{% test row_count_within_percent_of(model, compare_model, pct_threshold=10) %}
    -- Fails if row counts differ by more than pct_threshold%
    with a as (select count(*) as cnt from {{ model }}),
         b as (select count(*) as cnt from {{ ref(compare_model) }})
    select
        a.cnt as model_count,
        b.cnt as compare_count,
        abs(a.cnt - b.cnt) * 100.0 / nullif(b.cnt, 0) as pct_diff
    from a, b
    where abs(a.cnt - b.cnt) * 100.0 / nullif(b.cnt, 0) > {{ pct_threshold }}
{% endtest %}
```

```yaml
models:
  - name: fct_orders_v2
    tests:
      - row_count_within_percent_of:
          compare_model: fct_orders_v1
          pct_threshold: 5  # fail if counts differ by more than 5%
```

## dbt-expectations: Advanced Tests

### Distributional Tests

```yaml
models:
  - name: fct_sessions
    columns:
      - name: session_duration_seconds
        tests:
          # Mean should be 5-15 minutes
          - dbt_expectations.expect_column_mean_to_be_between:
              min_value: 300
              max_value: 900

          # Median (50th percentile) should be 3-10 minutes
          - dbt_expectations.expect_column_quantile_values_to_be_between:
              quantile: 0.50
              min_value: 180
              max_value: 600

          # 95th percentile shouldn't exceed 2 hours (likely bot/open tab)
          - dbt_expectations.expect_column_quantile_values_to_be_between:
              quantile: 0.95
              min_value: 0
              max_value: 7200

          # Standard deviation — should be consistent
          - dbt_expectations.expect_column_stdev_to_be_between:
              min_value: 0
              max_value: 1800
```

### Table-Level Row Count Tests

```yaml
models:
  - name: fct_events
    tests:
      # Row count should be between known bounds
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 10000
          max_value: 100000000

      # Today's row count should be >80% of yesterday's (detect drops)
      - dbt_expectations.expect_table_row_count_to_equal_other_table:
          compare_model: ref('fct_events_yesterday')

      # Column count should match expectation
      - dbt_expectations.expect_table_column_count_to_equal:
          value: 12
```

### Conditional / Row-Level Tests

```yaml
models:
  - name: fct_orders
    columns:
      - name: shipped_at
        tests:
          # shipped_at only applies to shipped/delivered orders
          - dbt_expectations.expect_column_values_to_not_be_null:
              row_condition: "status in ('shipped', 'delivered')"

          # For non-shipped orders, shipped_at should be null
          - dbt_expectations.expect_column_values_to_be_null:
              row_condition: "status in ('pending', 'processing')"

      - name: refund_amount
        tests:
          # Refund amount should never exceed original amount
          - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B:
              column_A: total_amount
              column_B: refund_amount
              row_condition: "refund_amount is not null"
```

### Regex Pattern Tests

```yaml
      - name: phone_number
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^\\+?[1-9]\\d{7,14}$"  # E.164 format
              config:
                severity: warn  # phone formatting is noisy

      - name: postal_code
        tests:
          - dbt_expectations.expect_column_values_to_match_like_pattern_list:
              pattern_list:
                - "[0-9][0-9][0-9][0-9][0-9]"       # US ZIP
                - "[A-Z][0-9][A-Z] [0-9][A-Z][0-9]"  # CA postal
              match_on: any
```

## Test Selection and Tagging

### Tagging Tests

```yaml
models:
  - name: fct_revenue
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 10000000
          config:
            tags: ['finance', 'critical', 'row_count']

    columns:
      - name: revenue_usd
        tests:
          - not_null:
              config:
                tags: ['finance', 'critical']
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
              config:
                tags: ['finance', 'range_check']
```

### Selecting Tests to Run

```bash
# Run only critical tests
dbt test --select "tag:critical"

# Run only finance model tests
dbt test --select "tag:finance"

# Run tests for a specific model
dbt test --select fct_revenue

# Run singular tests only
dbt test --select "test_type:singular"

# Run tests and their upstream model's tests
dbt test --select "+fct_revenue"

# Exclude slow tests in CI
dbt test --exclude "tag:slow"
```

## Storing and Analyzing Test Results

### Writing Test Results to Warehouse

```bash
# Store results in the warehouse
dbt test --store-failures

# Custom schema for test failures
# In profiles.yml:
# schema: analytics
# test results go to: analytics_dbt_test__audit
```

### Monitoring Test Trends

```sql
-- Build a test results history table
-- Run after each dbt test with store_failures
insert into analytics.dbt_test_history
select
    current_timestamp as run_at,
    test_name,
    model_name,
    status,
    failures,
    rows_affected
from {{ ref('dbt_test_results') }}  -- dbt artifacts table
```

## Test Organization Best Practices

```
models/
├── staging/
│   └── schema.yml          ← source + staging tests
├── marts/
│   ├── finance/
│   │   └── schema.yml      ← finance-specific tests
│   └── marketing/
│       └── schema.yml

tests/
├── finance/
│   ├── revenue_matches_orders.sql
│   └── no_negative_arr.sql
├── referential/
│   └── all_orders_have_customers.sql
└── freshness/
    └── events_received_today.sql
```

## Interview Questions

**Q: How do you create a custom generic test in dbt?**
A: Define a macro in `macros/tests/` with the naming convention `test_<name>.sql` or just `<name>.sql` inside a `tests` directory. The macro takes `model` and `column_name` as required parameters, plus any custom parameters. The macro body returns a SELECT that produces failing rows.

**Q: What is the advantage of `dbt-expectations` over built-in dbt tests?**
A: Built-in tests handle basic cases: not_null, unique, accepted_values, relationships. dbt-expectations adds: statistical tests (mean, stdev, quantile), regex matching, conditional row-level tests (`row_condition`), column count tests, and cross-table row count comparisons. The `row_condition` parameter is especially powerful — it lets you apply tests only to rows matching a filter.

**Q: How do you prevent one flaky test from blocking your entire pipeline?**
A: Set `severity: warn` on the flaky test. This logs the failure without exiting with a non-zero code. For volume-based flakiness (a few bad rows expected), use `warn_if` and `error_if` thresholds. Tag noisy tests with `tag:flaky` and exclude them from critical CI paths.

**Q: How do you run only tests related to models that changed in a PR?**
A: Use state selection: `dbt test --select "state:modified+"`. This runs tests on models that changed AND their downstream dependents. Combined with `--defer`, upstream models are satisfied from production state without rebuilding everything.
