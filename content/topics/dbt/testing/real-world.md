---
title: "dbt Testing - Real-World"
topic: dbt
subtopic: testing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, testing, production, ci, data-quality]
---

# dbt Testing — Real-World Examples

## Example 1: Full Test Suite for a Fact Table

```yaml
# models/marts/schema.yml
models:
  - name: fct_orders
    description: "One row per order. Grain: order_id."
    config:
      contract:
        enforced: true
    tests:
      # Table-level
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000000    # At least 1M rows
          max_value: 1000000000
      - dbt_expectations.expect_compound_columns_to_be_unique:
          column_list: ['order_id']
      - elementary.volume_anomalies:
          timestamp_column: order_date
          anomaly_sensitivity: 3
    columns:
      - name: order_id
        data_type: bigint
        tests: [unique, not_null]
      - name: customer_id
        data_type: bigint
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: order_date
        data_type: date
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2015-01-01'::date"
              max_value: "CURRENT_DATE + 1"
      - name: total_amount
        data_type: numeric
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
          - elementary.column_anomalies:
              anomaly_sensitivity: 3
      - name: status
        data_type: varchar
        tests:
          - not_null
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
```

## Example 2: CI/CD Test Pipeline with Notifications

```yaml
# .github/workflows/dbt-ci.yml
- name: dbt build + test
  run: |
    dbt build \
      --select state:modified+ \
      --state ./prod-state \
      --fail-fast \
      --store-failures

- name: Upload test failures
  if: failure()
  run: |
    # Parse run_results.json for failed tests
    python3 << 'EOF'
    import json
    with open('target/run_results.json') as f:
        results = json.load(f)
    
    failed = [r for r in results['results'] if r['status'] == 'fail']
    for f in failed:
        print(f"FAILED: {f['unique_id']}")
        print(f"  Message: {f.get('message', 'no message')}")
    EOF

- name: Slack notification on failure
  if: failure()
  uses: 8398a7/action-slack@v3
  with:
    status: failure
    text: "dbt CI failed on PR #${{ github.event.number }}"
```

## Example 3: Data Quality Dashboard

```sql
-- models/monitoring/test_results_summary.sql
{{ config(materialized='table') }}

SELECT
    test_name,
    model_name,
    column_name,
    status,
    failures,
    execution_time,
    run_started_at
FROM {{ ref('elementary_test_results') }}
WHERE run_started_at >= CURRENT_DATE - 30
ORDER BY run_started_at DESC
```

Build a Power BI / Metabase dashboard on this to track:
- Test pass rate over time
- Which models have most failures
- SLA breaches by table

## Example 4: Custom Business Rule Test

A financial company rule: orders marked `delivered` must have a delivery timestamp:

```sql
-- tests/assert_delivered_orders_have_delivery_ts.sql
SELECT
    order_id,
    status,
    delivered_at
FROM {{ ref('fct_orders') }}
WHERE status = 'delivered'
    AND delivered_at IS NULL
```

```yaml
# Add to singular test config for visibility
# tests/assert_delivered_orders_have_delivery_ts.yml (dbt 1.5+)
tests:
  - name: assert_delivered_orders_have_delivery_ts
    config:
      severity: error
      description: "Delivered orders must have a delivery timestamp"
      meta:
        owner: "@order-management-team"
        jira: "DATA-4521"
```

## Example 5: Quarterly Regression Test Suite

Run exhaustive tests quarterly to catch data drift:

```bash
#!/bin/bash
# scripts/quarterly_regression_tests.sh

echo "Running quarterly regression suite..."

# 1. Full source freshness
dbt source freshness

# 2. All tests (not just changed models)
dbt test --select "*" --threads 32

# 3. Row count comparisons vs last quarter snapshot
dbt run-operation compare_row_counts_to_baseline \
  --args '{"baseline_date": "2024-01-01"}'

# 4. Statistical checks across all fact tables
dbt test --select tag:statistical

# 5. Generate docs to check for undocumented models
dbt docs generate
python3 scripts/check_documentation_coverage.py
```
