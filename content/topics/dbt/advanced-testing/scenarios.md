---
title: "Advanced Testing - Scenario Questions"
topic: dbt
subtopic: advanced-testing
content_type: scenario_question
tags: [dbt, testing, scenarios, interview, store-failures, custom-tests]
---

# Scenario Questions — Advanced Testing

<article data-difficulty="junior">

## 🟢 Junior: Add a Custom Singular Test

**Scenario:** Your `orders` table has a business rule: `total_amount` must always equal the sum of `subtotal + tax + shipping_fee`. The built-in `not_null` and `unique` tests can't catch this. How do you write a dbt test to enforce it?

<details>
<summary>✅ Solution</summary>

Singular tests are plain SQL files in the `tests/` directory. A test passes when the query returns **zero rows** — any row returned is a failure.

```sql
-- tests/orders_total_amount_matches_components.sql
-- Returns rows where total_amount doesn't match subtotal + tax + shipping_fee
-- dbt test fails if any rows are returned

SELECT
    order_id,
    total_amount,
    subtotal + tax + shipping_fee AS expected_total,
    total_amount - (subtotal + tax + shipping_fee) AS discrepancy
FROM {{ ref('orders') }}
WHERE ABS(total_amount - (subtotal + tax + shipping_fee)) > 0.01  -- allow for rounding
```

Run it with:
```bash
dbt test --select test_type:singular
# or by name:
dbt test --select orders_total_amount_matches_components
```

**When to use singular vs generic tests:**
- **Singular**: one-off business rules specific to one model
- **Generic**: reusable patterns you'll apply to many models (→ write as a macro)

**Add severity so it warns instead of failing CI:**
```yaml
# in schema.yml — singular tests can also be referenced there
tests:
  - orders_total_amount_matches_components:
      severity: warn
```

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Persist and Alert on Test Failures

**Scenario:** Your data quality tests run in CI but when they fail, engineers just see "1 test failed" in the logs. The data team wants to: (1) store the actual failing rows so they can be investigated later, (2) get a Slack alert with how many rows failed, (3) not block the pipeline for warn-level issues.

<details>
<summary>✅ Solution</summary>

**Step 1: Enable `store_failures` in `dbt_project.yml`**

```yaml
# dbt_project.yml
tests:
  your_project:
    +store_failures: true
    +store_failures_as: table        # persist as table (vs ephemeral/view)
    +schema: dbt_test_failures       # separate schema to avoid cluttering prod
```

Or per-test in `schema.yml`:
```yaml
models:
  - name: orders
    columns:
      - name: total_amount
        tests:
          - not_null:
              severity: error
              store_failures: true
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
              severity: warn          # warn only — won't fail CI
              store_failures: true
```

**Step 2: Parse `run_results.json` and send Slack alert**

```python
# scripts/notify_test_failures.py (run after dbt test in CI)
import json
import os
import requests

with open("target/run_results.json") as f:
    results = json.load(f)

failures = []
for r in results["results"]:
    if r["status"] in ("fail", "warn"):
        failures.append({
            "test": r["unique_id"].split(".")[-1],
            "status": r["status"],
            "failures": r.get("failures", 0),
            "message": r.get("message", "")
        })

if failures:
    blocks = [{"type": "section", "text": {"type": "mrkdwn",
        "text": f"*dbt test results — {len(failures)} issue(s)*"}}]
    for f in failures:
        emoji = "🔴" if f["status"] == "fail" else "🟡"
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": f"{emoji} `{f['test']}` — {f['failures']} rows failed"}})

    requests.post(os.environ["SLACK_WEBHOOK"], json={"blocks": blocks})
```

**Step 3: Query stored failures for investigation**

```sql
-- After `dbt test`, failures are stored as tables in dbt_test_failures schema
SELECT * FROM dbt_test_failures.not_null_orders_total_amount
ORDER BY _dbt_source_relation, order_id
LIMIT 100;

-- Find the most common failure patterns
SELECT
    status,
    COUNT(*) AS failure_count,
    MIN(created_at) AS earliest_failure
FROM dbt_test_failures.not_null_orders_total_amount
GROUP BY 1;
```

**Key distinction:**
- `severity: error` → fails CI, blocks deployment
- `severity: warn` → logs warning, CI continues, still stores failures if `store_failures: true`

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Tiered Testing Strategy for a 200-Table dbt Project

**Scenario:** Your company has a dbt project with 200 models, 8 data sources (Salesforce, Stripe, MySQL CDC, S3 events, etc.), and a mix of marketing, finance, and operations data. The current state: tests take 45 minutes to run in CI, there are 2,000 tests, 15% are flaky (intermittently pass/fail), and the data team has started ignoring test failures. Design a testing architecture that makes tests trustworthy, fast, and actionable.

<details>
<summary>✅ Solution</summary>

**Root cause analysis of the current state:**
- 45-minute test runs → no one waits for CI → failures ignored
- 15% flaky → usually `accepted_values` tests on evolving enums, `unique` on tables with expected duplicates, or row-count tests without tolerance
- 2,000 tests with no triage → "1,847 passed, 153 failed" tells you nothing actionable

**Architecture: 3-tier test strategy**

```
Tier 1 — BLOCKING (must pass, fast, run every PR)
  - Source freshness checks
  - not_null on critical PK/FK columns
  - unique on primary keys
  - referential integrity between staging and intermediate
  ~200 tests, target: < 5 minutes

Tier 2 — WARNING (logged, non-blocking, run every PR)
  - Business logic assertions (amount ranges, accepted values)
  - Row count anomalies (warn if > 20% change vs yesterday)
  - dbt-expectations distribution checks
  ~600 tests, target: < 15 minutes

Tier 3 — AUDIT (blocking on schedule, not on PR)
  - Full reconciliation against source systems
  - Cross-model consistency (revenue = sum of line items)
  - Completeness checks (no missing dates in time series)
  ~1,200 tests, run nightly only
```

**Implementation with tags:**

```yaml
# schema.yml — tag tests by tier
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - not_null:
              tags: [tier1, blocking]
          - unique:
              tags: [tier1, blocking]
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
              tags: [tier2, warn]
              severity: warn
      - name: total_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
              tags: [tier2, warn]
              severity: warn
```

**CI/CD pipeline:**

```yaml
# .github/workflows/dbt-ci.yml
jobs:
  tier1-tests:
    steps:
      # Slim CI: only test models changed in this PR + their downstream deps
      - run: dbt build --select state:modified+ --defer --state prod-artifacts/
        # target: < 5 min because only runs affected models

  tier2-tests:
    needs: tier1-tests
    steps:
      - run: dbt test --select tag:tier2
        continue-on-error: true   # warn only, don't block merge
      - run: python scripts/notify_test_failures.py

  tier3-nightly:
    # Separate workflow, runs at 2am
    steps:
      - run: dbt test --select tag:tier3
```

**Fix the 15% flaky tests:**

```yaml
# Pattern 1: accepted_values — use warn + allow partial match
- accepted_values:
    values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    severity: warn    # new statuses from product changes won't break CI

# Pattern 2: unique with expected duplicates — use where clause
- unique:
    where: "is_latest_version = true"   # dedup before testing

# Pattern 3: row count with tolerance
- dbt_expectations.expect_table_row_count_to_be_between:
    min_value: "{{ (var('orders_yesterday_count') * 0.8) | int }}"
    max_value: "{{ (var('orders_yesterday_count') * 1.5) | int }}"
    tags: [tier2]
    severity: warn
```

**Store failures with audit trail:**

```yaml
# dbt_project.yml
tests:
  +store_failures: true
  +store_failures_as: table
  +schema: audit_test_failures

# Add metadata to all stored failures
vars:
  dbt_test_run_id: "{{ env_var('CI_RUN_ID', 'manual') }}"
```

**Outcome targets (60-day plan):**
- Week 1-2: Tag all tests with tier, fix flaky tests (bulk severity: warn)
- Week 3-4: Implement slim CI with `--defer --state` (5-min CI)
- Week 5-6: Enable `store_failures` + Slack alerting
- Week 7-8: Add tier3 nightly job for full audit suite
- Day 60: Test run time < 8 min in CI, 0% flaky, failures actioned within 4 hours

</details>
</article>
