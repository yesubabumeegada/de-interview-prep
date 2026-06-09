---
title: "dbt Testing - Scenarios"
topic: dbt
subtopic: testing
content_type: scenario_question
tags: [dbt, testing, interview, scenarios, data-quality]
---

# dbt Testing — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Writing Tests for a New Model

**Scenario:** You built `dim_products` with columns: `product_id`, `product_name`, `category`, `price`. Write the appropriate tests for this model.

<details>
<summary>💡 Hint</summary>

Think about what makes each column valid: primary keys need unique + not_null, categorical columns need accepted_values, and numeric columns can use range checks from dbt_expectations.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Responding to a Failing Uniqueness Test in Production

**Scenario:** Monday morning, `dbt test` fails with:
```
Failure in test unique_fct_orders_order_id (models/marts/schema.yml)
  Got 1523 results, configured to fail if != 0
```
There are 1,523 duplicate `order_id` values in `fct_orders`. The model is incremental with `unique_key='order_id'`. How do you respond?

<details>
<summary>💡 Hint</summary>

Investigate first — find which order_ids are duplicated and when they appeared. Then fix the data in place, and determine the root cause before running the model again.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Complete Testing Strategy for a Fintech Project

**Scenario:** You're leading a new dbt project at a fintech company with 30 source tables, 80 models, and strict data quality SLAs (financial reporting must be 100% accurate). Design the complete testing strategy.

<details>
<summary>💡 Hint</summary>

Layer the testing strategy: source tests (upstream data quality), staging tests (schema contracts), mart tests (business rules), unit tests (logic correctness), and reporting tests (SLA-level reconciliation against source systems).

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What tests do you always add to a new dbt model?" — At minimum: `unique` and `not_null` on the primary key, `not_null` on any column used in downstream joins, and `accepted_values` on any status or category columns. For financial models, add range checks and reconciliation tests.

> **Tip 2:** "How do you respond to a failing uniqueness test in production?" — Investigate first: find the duplicate IDs and when they appeared. Fix the data in place with a ROW_NUMBER deduplication query. Then find the root cause before rerunning — otherwise you'll get duplicates again.

> **Tip 3:** "How do you design testing at scale?" — Layer it: source tests catch upstream issues early, staging tests enforce schema contracts, mart tests validate business rules, and reconciliation tests verify financial accuracy. Don't test everything at every layer — focus on what can break at each layer and would cause the most damage undetected.
