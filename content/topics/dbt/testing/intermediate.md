---
title: "dbt Testing - Intermediate"
topic: dbt
subtopic: testing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, dbt-expectations, custom-tests, unit-tests, test-packages]
---

# dbt Testing — Intermediate

## dbt-expectations Package

Port of Great Expectations to dbt — 50+ additional test types:

```yaml
# Install
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: 0.10.0
```

```yaml
models:
  - name: fct_orders
    tests:
      # Table-level: check row count is reasonable
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 10000
          max_value: 10000000
      # No duplicate rows (composite key)
      - dbt_expectations.expect_compound_columns_to_be_unique:
          column_list: ['order_id', 'line_item_id']

    columns:
      - name: total_amount
        tests:
          # Range check
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 50000
          # Type check
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: numeric
          # Not too many nulls
          - dbt_expectations.expect_column_proportion_of_unique_values_to_be_between:
              min_value: 0.9
      - name: email
        tests:
          # Regex match
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

## Unit Tests (dbt 1.8+)

Test model logic with mocked inputs — no warehouse data needed:

```yaml
# models/marts/schema.yml
unit_tests:
  - name: test_customer_tier_calculation
    model: dim_customers
    given:
      - input: ref('stg_orders')
        rows:
          - {customer_id: 1, total_amount: 15000}
          - {customer_id: 1, total_amount: 200}
          - {customer_id: 2, total_amount: 5000}
          - {customer_id: 3, total_amount: 50}
    expect:
      rows:
        - {customer_id: 1, lifetime_spend: 15200, customer_tier: 'Gold'}
        - {customer_id: 2, lifetime_spend: 5000,  customer_tier: 'Silver'}
        - {customer_id: 3, lifetime_spend: 50,    customer_tier: 'Bronze'}
```

Run unit tests: `dbt test --select "test_type:unit"`

## Custom Generic Tests

Define reusable test templates as macros:

```sql
-- macros/tests/test_is_valid_email.sql
{% test is_valid_email(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
    AND {{ column_name }} NOT REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

{% endtest %}
```

Apply like a built-in test:
```yaml
columns:
  - name: email
    tests:
      - is_valid_email
```

```sql
-- macros/tests/test_row_count_equals.sql
{% test row_count_equals(model, column_name, expected_count) %}

SELECT COUNT(*) AS actual_count
FROM {{ model }}
HAVING COUNT(*) != {{ expected_count }}

{% endtest %}
```

## Audit Helper Package

Compare two relations to validate migrations or refactors:

```bash
# Install dbt-labs/audit_helper
```

```sql
-- analyses/compare_old_vs_new_orders.sql
{{ audit_helper.compare_relation_columns(
    a_relation=ref('fct_orders_old'),
    b_relation=ref('fct_orders_new')
) }}
```

```sql
-- Check column-by-column match rates
{{ audit_helper.compare_all_columns(
    a_relation=ref('fct_orders_old'),
    b_relation=ref('fct_orders_new'),
    primary_key='order_id'
) }}
```

Output shows:
```
column_name     | match_count | unmatch_count | match_rate
total_amount    | 99823       | 177           | 99.82%
status          | 100000      | 0             | 100.00%
```

## Test Orchestration Patterns

### Run Tests in CI Before Deployment

```yaml
# GitHub Actions
- name: dbt build (changed models + tests)
  run: |
    dbt build \
      --select state:modified+ \
      --state ./prod-state \
      --fail-fast          # Stop on first failure
```

### Separate Test Schedules

```bash
# Critical tests: run every hour
dbt test --select tag:hourly_critical

# All tests: run after each full daily run
dbt test --select "*"
```

### Warn-Only Tests for Exploratory Models

```yaml
models:
  - name: rpt_experimental
    tests:
      - dbt_utils.expression_is_true:
          expression: "revenue > 0"
          config:
            severity: warn   # Don't block pipeline for new/experimental models
```

## Elementary Package (Observability)

```yaml
packages:
  - package: elementary-data/elementary
    version: 0.13.0
```

```yaml
# After installing, add to schema.yml:
models:
  - name: fct_orders
    tests:
      # Anomaly detection: alert if volume drops >20%
      - elementary.volume_anomalies:
          timestamp_column: order_date
          anomaly_sensitivity: 3
      # Alert on sudden null spike
      - elementary.column_anomalies:
          column_name: customer_id
          anomaly_sensitivity: 2
```

Monitor in Elementary's CLI or Cloud dashboard.
