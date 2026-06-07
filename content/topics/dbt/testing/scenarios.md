---
title: "dbt Testing - Scenarios"
topic: dbt
subtopic: testing
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [dbt, testing, interview, scenarios, data-quality]
---

# dbt Testing — Scenario Questions

## Scenario 1 (Junior): Writing Your First Tests

**Situation:** You built `dim_products` with columns: `product_id`, `product_name`, `category`, `price`. Write the appropriate tests for this model.

**Answer:**

```yaml
models:
  - name: dim_products
    description: "One row per active product."
    columns:
      - name: product_id
        description: "Primary key"
        tests:
          - unique        # No two products should share an ID
          - not_null      # Every product must have an ID

      - name: product_name
        description: "Product display name"
        tests:
          - not_null      # Every product needs a name

      - name: category
        description: "Product category"
        tests:
          - not_null
          - accepted_values:
              values: ['Electronics', 'Clothing', 'Food', 'Home', 'Sports']
              # Alerts if a new unexpected category appears

      - name: price
        description: "Product price in USD"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.01    # Price must be positive
              max_value: 100000  # Reasonable upper bound
```

---

## Scenario 2 (Mid-Level): Test Fails in Production

**Situation:** Monday morning, `dbt test` fails with:
```
Failure in test unique_fct_orders_order_id (models/marts/schema.yml)
  Got 1523 results, configured to fail if != 0
```

There are 1,523 duplicate `order_id` values in `fct_orders`. The model is incremental with `unique_key='order_id'`. How do you respond?

**Answer:**

**Immediate investigation:**
```sql
-- Find the duplicates
SELECT order_id, COUNT(*) AS cnt
FROM fct_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- Check when they appeared
SELECT order_id, _loaded_at
FROM fct_orders
WHERE order_id IN (
    SELECT order_id FROM fct_orders GROUP BY order_id HAVING COUNT(*) > 1
)
ORDER BY order_id, _loaded_at;
```

**Root cause analysis:**
1. Did someone run `dbt run` manually without `unique_key`? (Check git log)
2. Was there a strategy change from `merge` to `append`?
3. Did the incremental filter miss late-arriving data that was already loaded?
4. Did `on_schema_change` cause a model rebuild without deduplication?

**Immediate fix:**
```sql
-- Deduplicate in place (run directly in warehouse)
CREATE OR REPLACE TABLE fct_orders AS
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM fct_orders
)
WHERE rn = 1;
```

**Then run tests to confirm:**
```bash
dbt test --select fct_orders
```

**Prevention:**
- Add `store_failures: true` so next failure shows exactly which order_ids duplicated
- Add `--fail-fast` to CI pipeline
- Monitor with `elementary.volume_anomalies` to catch unexpected row count spikes

---

## Scenario 3 (Senior): Design Test Strategy for New Project

**Situation:** You're leading a new dbt project at a fintech company with 30 source tables, 80 models, and strict data quality SLAs (financial reporting must be 100% accurate). Design the complete testing strategy.

**Answer:**

**Layer 1 — Source Tests (defensive, catch upstream issues early):**
- Freshness checks: error after 4 hours for payment sources, 12 hours for CRM
- `not_null` + `unique` on natural keys
- Row count anomalies via `elementary.volume_anomalies`
- Schema contracts on all source tables

**Layer 2 — Staging Tests (schema contract):**
- `not_null` on all columns used in downstream joins
- `accepted_values` on all enum/status columns
- Referential integrity between related staging models

**Layer 3 — Mart Tests (business rules):**
- Model contracts with `enforced: true` on all public marts
- Composite uniqueness on fact table grain
- Range checks: amounts ≥ 0, dates in plausible range
- Statistical anomaly detection (Elementary) on key metrics

**Layer 4 — Unit Tests (logic correctness):**
- Every complex CASE expression tested
- Every window function tested with edge cases
- Every custom macro tested

**Layer 5 — Reporting Tests (SLA-level):**
```sql
-- tests/financial/assert_daily_reconciliation.sql
-- Total from dbt mart must match source system ledger within 0.01%
SELECT
    ABS(dbt_total - ledger_total) / ledger_total AS variance_pct
FROM (
    SELECT SUM(transaction_amount) AS dbt_total FROM {{ ref('fct_transactions') }}
    WHERE transaction_date = CURRENT_DATE - 1
) dbt,
(
    SELECT SUM(amount) AS ledger_total FROM {{ source('raw', 'gl_ledger') }}
    WHERE entry_date = CURRENT_DATE - 1
) ledger
HAVING variance_pct > 0.0001  -- Fail if >0.01% discrepancy
```

**CI/CD integration:**
- PR: run tests for `state:modified+` models only (fast)
- Merge to main: run ALL tests before deployment
- Daily: run full suite including anomaly detection
- Weekly: run reconciliation tests against source systems
