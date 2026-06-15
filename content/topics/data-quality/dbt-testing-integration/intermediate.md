---
title: "dbt Testing Integration - Intermediate"
topic: data-quality
subtopic: dbt-testing-integration
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-quality, dbt, testing, dbt-expectations, ci-cd, data-validation]
---

# dbt Testing Integration — Intermediate

## dbt-expectations Package

`dbt-expectations` brings Great Expectations-style checks into dbt YAML config. Install it via `packages.yml`:

```yaml
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

```bash
dbt deps
```

### Common dbt-expectations Tests

```yaml
models:
  - name: orders
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 10000000
      - dbt_expectations.expect_table_columns_to_match_set:
          column_list: [order_id, customer_id, status, total_amount, created_at]
    columns:
      - name: total_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
          - dbt_expectations.expect_column_values_to_not_be_null:
              row_condition: "status != 'cancelled'"
      - name: email
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      - name: created_at
        tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: timestamp
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'::timestamp"
              max_value: "current_timestamp"
              row_condition: "created_at is not null"
```

---

## Cross-Model Referential Integrity

The built-in `relationships` test covers simple FK checks. For complex multi-column relationships:

```sql
-- tests/assert_payments_have_valid_orders.sql
-- Every payment must reference an existing, non-cancelled order

SELECT p.payment_id
FROM {{ ref('payments') }} p
LEFT JOIN {{ ref('orders') }} o
    ON p.order_id = o.order_id
    AND o.status != 'cancelled'
WHERE o.order_id IS NULL
```

For multi-column composite keys:

```sql
-- tests/assert_inventory_composite_key_valid.sql
SELECT i.warehouse_id, i.sku_id, i.date
FROM {{ ref('inventory_snapshots') }} i
LEFT JOIN {{ ref('warehouses') }} w ON i.warehouse_id = w.warehouse_id
LEFT JOIN {{ ref('products') }} p ON i.sku_id = p.sku_id
WHERE w.warehouse_id IS NULL OR p.sku_id IS NULL
```

---

## Test Selection Strategies

### By Tag

Tag tests for targeted runs:

```yaml
models:
  - name: orders
    config:
      tags: ['critical', 'finance']
    columns:
      - name: order_id
        tests:
          - unique:
              tags: ['primary-key']
          - not_null:
              tags: ['primary-key']
      - name: total_amount
        tests:
          - not_null:
              tags: ['finance', 'critical']
```

```bash
# In CI: run only critical tests before deploying
dbt test --select tag:critical

# Finance-specific audit run
dbt test --select tag:finance
```

### By Model Tier

```bash
# Test staging models first, then marts
dbt test --select staging.*
dbt test --select marts.*

# Test a specific domain
dbt test --select marts.finance.*
```

### Incremental Test Patterns

For large tables, test only recent data using `--vars`:

```yaml
# models/schema.yml
      - name: order_id
        tests:
          - unique:
              config:
                where: "created_at >= '{{ var(\"test_start_date\", \"2024-01-01\") }}'"
```

```bash
dbt test --select orders --vars '{"test_start_date": "2024-11-01"}'
```

---

## CI/CD Integration

### GitHub Actions Pipeline

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI
on:
  pull_request:
    paths:
      - 'models/**'
      - 'tests/**'
      - 'dbt_project.yml'

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install dbt
        run: pip install dbt-bigquery==1.8.0

      - name: dbt deps
        run: dbt deps

      - name: dbt build (slim CI)
        run: |
          # Only build/test models changed in this PR
          dbt build \
            --select state:modified+ \
            --defer \
            --state ./prod-artifacts \
            --target ci
        env:
          DBT_BIGQUERY_KEYFILE: ${{ secrets.GCP_SA_KEY }}

      - name: Source freshness check
        run: dbt source freshness
```

### Slim CI with State Comparison

```bash
# Download production manifest for comparison
dbt ls \
  --select state:modified+ \
  --defer \
  --state s3://my-bucket/dbt-artifacts/latest/

# Run only modified models + downstream tests
dbt build \
  --select state:modified+ \
  --defer \
  --state s3://my-bucket/dbt-artifacts/latest/
```

---

## dbt Unit Tests (dbt v1.8+)

dbt v1.8 introduced native unit testing for SQL logic — test your transformation logic without needing real data.

```yaml
# models/schema.yml
unit_tests:
  - name: test_order_status_logic
    model: stg_orders
    given:
      - input: ref('raw_orders')
        rows:
          - {order_id: 1, status_code: 'P', refunded: false}
          - {order_id: 2, status_code: 'S', refunded: false}
          - {order_id: 3, status_code: 'D', refunded: true}
    expect:
      rows:
        - {order_id: 1, status: 'placed', is_refunded: false}
        - {order_id: 2, status: 'shipped', is_refunded: false}
        - {order_id: 3, status: 'delivered', is_refunded: true}
```

```bash
dbt test --select test_type:unit
```

Unit tests are ideal for:
- Complex CASE WHEN logic
- Date/time calculations
- Business rule transformations that are hard to verify with data-based tests

---

## Reporting Test Results

### Sending Failures to Slack

```python
# scripts/notify_test_failures.py
import json
import subprocess
import requests

def run_dbt_tests_and_notify():
    result = subprocess.run(
        ["dbt", "test", "--output", "json"],
        capture_output=True, text=True
    )
    
    # Parse JSON output (dbt 1.5+ supports structured logging)
    failures = []
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
            if event.get("info", {}).get("level") == "error":
                failures.append(event["info"]["msg"])
        except json.JSONDecodeError:
            pass
    
    if failures:
        payload = {
            "text": f":red_circle: *dbt test failures detected*\n" +
                    "\n".join(f"• {f}" for f in failures[:10])
        }
        requests.post(SLACK_WEBHOOK_URL, json=payload)

run_dbt_tests_and_notify()
```

### Using `run_results.json`

After every `dbt test`, dbt writes `target/run_results.json`:

```python
import json

with open("target/run_results.json") as f:
    results = json.load(f)

failed_tests = [
    r for r in results["results"]
    if r["status"] in ("fail", "error")
]

for test in failed_tests:
    print(f"FAILED: {test['unique_id']} — {test.get('message', '')}")
```

---

## Key Interview Points

- `dbt-expectations` adds GE-style checks (regex, range, type assertions) via YAML config
- Singular tests in `tests/` are SQL files returning failing rows — zero rows = pass
- Slim CI (`state:modified+` with `--defer`) avoids rebuilding unchanged models in PRs
- dbt v1.8 unit tests mock input data to test SQL logic — no warehouse data needed
- `run_results.json` in `target/` is the machine-readable test report; parse it for alerting
- Test tags (`tag:critical`) enable tiered execution: fast critical gate, then full suite
