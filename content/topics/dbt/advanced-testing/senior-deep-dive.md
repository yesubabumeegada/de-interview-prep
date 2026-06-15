---
title: "Advanced Testing - Senior Deep Dive"
topic: dbt
subtopic: advanced-testing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, testing, unit-tests, audit-helper, incremental-testing, architecture]
---

# Advanced Testing — Senior Deep Dive

## dbt Unit Tests (dbt v1.8+)

Unit tests mock input data and assert on model output — no warehouse data needed. This is ideal for testing complex business logic in isolation.

```yaml
# models/tests/unit_tests.yml
unit_tests:
  - name: test_fiscal_quarter_logic
    model: fct_orders   # Model being tested
    given:
      # Mock input for ref('stg_orders')
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, order_date: "2024-02-15", total_amount: 100.00}  # Q1 (Feb)
          - {order_id: 2, order_date: "2024-05-01", total_amount: 200.00}  # Q2 (May)
          - {order_id: 3, order_date: "2024-08-20", total_amount: 300.00}  # Q3 (Aug)
          - {order_id: 4, order_date: "2024-11-10", total_amount: 150.00}  # Q4 (Nov)
      # Mock input for ref('dim_customers')
      - input: ref('dim_customers')
        rows:
          - {customer_id: 'cust_1', segment: 'enterprise'}
    expect:
      rows:
        - {order_id: 1, fiscal_quarter: 'Q1', total_amount: 100.00}
        - {order_id: 2, fiscal_quarter: 'Q2', total_amount: 200.00}
        - {order_id: 3, fiscal_quarter: 'Q3', total_amount: 300.00}
        - {order_id: 4, fiscal_quarter: 'Q4', total_amount: 150.00}
```

### Unit Test for Incremental Logic

```yaml
unit_tests:
  - name: test_incremental_filter
    model: fct_events
    overrides:
      macros:
        # Simulate incremental run (is_incremental() = true)
        is_incremental: true
    given:
      - input: source('events', 'raw_events')
        rows:
          - {event_id: 1, event_time: "2024-01-01 10:00:00"}  # old, should be excluded
          - {event_id: 2, event_time: "2024-01-15 12:00:00"}  # new, should be included
      - input: this  # mock the existing target table
        rows:
          - {event_id: 999, event_time: "2024-01-10 00:00:00"}  # max event_time = Jan 10
    expect:
      rows:
        - {event_id: 2}  # only event after max(event_time) from 'this'
```

### Running Unit Tests

```bash
# Run all unit tests
dbt test --select "test_type:unit"

# Run unit tests for a specific model
dbt test --select fct_orders --select "test_type:unit"

# Run unit tests in CI (fast, no warehouse data needed for mocked inputs)
dbt test --select "test_type:unit" --target dev
```

## audit_helper: Compare Relations at Scale

### Full Regression Test Workflow

```sql
-- analyses/regression_test.sql
-- Compare old and new model implementations

{% set old_model %}
    select * from analytics.dbt_prod_old.fct_revenue
{% endset %}

{% set new_model %}
    select * from {{ ref('fct_revenue') }}
{% endset %}

{{ audit_helper.compare_relations(
    a_relation=old_model,
    b_relation=new_model,
    primary_key='revenue_id',
    summarize=true
) }}
```

**Output format:**
```
| in_a | in_b | count |
|------|------|-------|
| true | true | 9998  |  ← rows in both (matching)
| true | false|    2  |  ← rows only in old model
| false| true |    5  |  ← rows only in new model
```

### Column-Level Comparison

```bash
# Compare a specific column across two model versions
dbt run-operation audit_helper.compare_column_values --args '{
  "a_query": "select order_id, revenue from analytics.dbt_prod.fct_revenue_v1",
  "b_query": "select order_id, revenue from analytics.dbt_prod.fct_revenue_v2",
  "primary_key": "order_id",
  "column_to_compare": "revenue"
}'
```

**Output:**
```
| match_status          | count | percent |
|-----------------------|-------|---------|
| exact_match           | 9975  | 99.75%  |
| a_null_b_not          |     0 |  0.00%  |
| b_null_a_not          |     5 |  0.05%  |
| values_don_not_match  |    20 |  0.20%  |
```

## Testing Incremental Models

### The Full Refresh vs. Incremental Equivalence Test

The gold standard: verify that running incrementally N times produces the same result as a single full refresh.

```bash
# Step 1: Full refresh baseline
dbt run --full-refresh --select fct_events

# Store row count
FULL_REFRESH_COUNT=$(dbt run-operation get_row_count --args '{"model": "fct_events"}')

# Step 2: Run incremental twice (simulating two days of runs)
dbt run --select fct_events  # incremental run 1
dbt run --select fct_events  # incremental run 2 (idempotency)

INCREMENTAL_COUNT=$(dbt run-operation get_row_count --args '{"model": "fct_events"}')

# Step 3: Compare
if [ "$FULL_REFRESH_COUNT" != "$INCREMENTAL_COUNT" ]; then
  echo "FAIL: Full refresh ($FULL_REFRESH_COUNT) != Incremental ($INCREMENTAL_COUNT)"
  exit 1
fi
```

### Automated Incremental Test in CI

```yaml
# .github/workflows/incremental_test.yml
- name: Test incremental equivalence
  run: |
    # Full refresh
    dbt run --full-refresh --select tag:incremental --target ci
    
    # Store full refresh state
    dbt run-operation store_row_counts \
      --args '{"schema": "ci_full_refresh", "tag": "incremental"}'
    
    # Run incrementally
    dbt run --select tag:incremental --target ci
    dbt run --select tag:incremental --target ci  # second run for idempotency
    
    # Compare
    dbt test --select "tag:equivalence_test"
```

## Testing Strategy Architecture

### Test Pyramid for dbt

```
              ╔═══════════════════════╗
              ║   Singular Tests      ║  ← Few, complex business rules
              ║   (Business Logic)    ║
              ╠═══════════════════════╣
              ║   dbt-expectations    ║  ← Statistical / distributional
              ║   (Statistical)       ║
              ╠═══════════════════════╣
              ║   Generic Tests       ║  ← Many, fast, structural
              ║   (Structural)        ║
              ╠═══════════════════════╣
              ║   Unit Tests          ║  ← Logic isolation, no warehouse
              ║   (Logic)             ║
              ╚═══════════════════════╝
```

### Test Coverage Matrix

```yaml
# What to test at each layer:

staging_models:
  - not_null on primary keys
  - unique on primary keys
  - relationships (FKs to sources)
  - accepted_values for status/type fields
  - Source freshness

intermediate_models:
  - unique on grain identifier
  - not_null on critical columns
  - Row count sanity (won't be zero)

mart_models:
  - unique on primary key
  - not_null on all non-optional columns
  - Referential integrity to dims
  - Statistical checks (mean, stdev, range)
  - Business rule singular tests
  - Incremental equivalence (for incremental models)
  - Cross-model consistency (revenue matches orders)
```

## Production Test Monitoring

### Elementary: dbt-native Test Observability

[Elementary](https://docs.elementary-data.com/) adds automated anomaly detection on top of dbt tests:

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: 0.14.0
```

```yaml
# schema.yml — add anomaly detection tests
models:
  - name: fct_orders
    tests:
      # Detect unusual spikes/drops in row count
      - elementary.volume_anomalies:
          timestamp_column: order_date
          days_back: 14

      # Detect null rate anomalies
      - elementary.column_anomalies:
          column_name: email
          tests:
            - null_count

      # Detect distribution shifts
      - elementary.all_columns_anomalies:
          timestamp_column: order_date
```

### Structured Test Result Logging

```python
# After dbt test, parse run_results.json and log to warehouse
import json

with open('target/run_results.json') as f:
    results = json.load(f)

test_records = []
for result in results['results']:
    if result['unique_id'].startswith('test.'):
        test_records.append({
            'test_id': result['unique_id'],
            'status': result['status'],
            'failures': result.get('failures', 0),
            'message': result.get('message', ''),
            'execution_time': result['execution_time'],
            'run_at': results['metadata']['generated_at']
        })

# Write to warehouse for trending
write_to_warehouse(test_records, 'analytics.dbt_test_history')
```

## Interview Questions for Seniors

**Q: Explain dbt unit tests and when they're preferable to data tests.**
A: Unit tests (dbt v1.8+) mock input data and assert on model output without querying warehouse data. They're fast (no warehouse round-trip), reproducible (no dependency on data state), and ideal for testing complex business logic (fiscal quarter calculation, discount tiers, scoring). Data tests (singular/generic) verify actual warehouse data against quality rules. Use unit tests for logic correctness, data tests for production data quality.

**Q: How do you design a test strategy for an incremental model?**
A: Three test categories: (1) Structural tests on the model's output: uniqueness on `unique_key`, not_null on critical fields, range checks; (2) Incremental equivalence test: periodically (or in CI) compare `--full-refresh` result against accumulated incremental result — they should match; (3) Idempotency test: run incrementally twice with the same data window and verify row counts don't change. Also test the `on_schema_change` behavior by deploying a column addition to a dev environment.

**Q: What is the difference between storing test failures as a table vs. a view?**
A: `store_failures_as: table` persists the failing rows permanently until the next test run (when the table is replaced). `store_failures_as: view` creates a view that re-executes the test query when queried — so it shows current failures, not historical ones. Tables are better for debugging after-the-fact; views always show the current state.

**Q: How would you architect tests for a mission-critical financial model (e.g., monthly ARR)?**
A: Defense in depth: (1) Unit tests for all calculation logic (churn detection, expansion/contraction categorization); (2) Referential integrity tests to all dimension tables; (3) Cross-model consistency: ARR from this model must match sum of cohort ARR model (singular test); (4) Statistical bounds: ARR can't decrease by more than 20% MoM (anomaly detection); (5) Audit helper comparison after each refactor; (6) All tests tagged `tag:critical` run blocking with `severity: error`; (7) `store_failures: true` for post-incident investigation; (8) Test results logged to warehouse for trending over time.
