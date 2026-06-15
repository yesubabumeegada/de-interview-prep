---
title: "Advanced Testing - Real World"
topic: dbt
subtopic: advanced-testing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, testing, production, patterns, data-quality, monitoring]
---

# Advanced Testing — Real World Patterns

## Pattern 1: Financial Data Integrity Testing

**Problem:** Finance depends on `fct_monthly_arr` for board reporting. One wrong calculation silently propagated for 2 quarters before discovery. Design a comprehensive test suite.

```yaml
# models/marts/finance/schema.yml
models:
  - name: fct_monthly_arr
    tests:
      # Row count sanity — should have one row per customer per month
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 500000
          config:
            severity: error
            tags: ['finance', 'critical']

      # No duplicate customer-month combinations
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [customer_id, arr_month]
          config:
            tags: ['finance', 'critical']

    columns:
      - name: arr_usd
        tests:
          - not_null:
              config:
                tags: ['finance', 'critical']
          # ARR can't be negative (unless it's a credit)
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000000  # $10M per customer per month is suspicious
              config:
                severity: warn
                tags: ['finance', 'range']

      - name: mrr_usd
        tests:
          - not_null
          # MRR should be ARR / 12 (within rounding)
          # Enforced via singular test below
```

```sql
-- tests/finance/mrr_equals_arr_divided_by_12.sql
-- ARR should always equal MRR * 12 (within $0.01 rounding)

select
    customer_id,
    arr_month,
    arr_usd,
    mrr_usd,
    abs(arr_usd - mrr_usd * 12) as discrepancy
from {{ ref('fct_monthly_arr') }}
where abs(arr_usd - mrr_usd * 12) > 0.01
```

```sql
-- tests/finance/arr_matches_cohort_totals.sql
-- Total ARR must match sum of cohort ARR within 0.1%

with monthly_arr as (
    select arr_month, sum(arr_usd) as total_arr
    from {{ ref('fct_monthly_arr') }}
    group by arr_month
),
cohort_arr as (
    select cohort_month, sum(arr_contribution) as total_arr
    from {{ ref('fct_cohort_arr') }}
    group by cohort_month
)
select
    m.arr_month,
    m.total_arr as monthly_total,
    c.total_arr as cohort_total,
    abs(m.total_arr - c.total_arr) / nullif(m.total_arr, 0) * 100 as pct_diff
from monthly_arr m
join cohort_arr c on m.arr_month = c.cohort_month
where abs(m.total_arr - c.total_arr) / nullif(m.total_arr, 0) > 0.001  -- 0.1% tolerance
```

## Pattern 2: Event Stream Quality Monitoring

**Problem:** A 50M events/day pipeline has intermittent data quality issues — bots, duplicate events, events with impossible timestamps. Tests must not block the pipeline but must alert the team.

```yaml
# models/marts/fct_events/schema.yml
models:
  - name: fct_events
    tests:
      # Daily row count — should be within 20% of trailing 7-day average
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 40000000   # 50M ± 20%
          max_value: 60000000
          config:
            severity: warn
            store_failures: true

    columns:
      - name: event_id
        tests:
          - unique:
              config:
                severity: error
                store_failures: true
          - not_null

      - name: event_time
        tests:
          - not_null
          # Events shouldn't be in the future
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'"
              max_value: "current_timestamp"
              config:
                severity: warn
                store_failures: true
                warn_if: ">100"     # warn only if more than 100 future events

      - name: user_id
        tests:
          - not_null:
              config:
                warn_if: ">1000"    # allow some anonymous events
                error_if: ">100000" # but not 100K+
                severity: warn
```

```sql
-- tests/events/no_bot_traffic.sql
-- Flag if >5% of events come from known bot user-agents

with bot_events as (
    select count(*) as bot_count
    from {{ ref('fct_events') }}
    where event_date = current_date - 1
      and lower(user_agent) like any ('%bot%', '%crawler%', '%spider%', '%googlebot%')
),
total_events as (
    select count(*) as total_count
    from {{ ref('fct_events') }}
    where event_date = current_date - 1
)
select
    bot_count,
    total_count,
    bot_count * 100.0 / nullif(total_count, 0) as bot_pct
from bot_events, total_events
where bot_count * 100.0 / nullif(total_count, 0) > 5  -- fail if >5% bot traffic
```

## Pattern 3: Referential Integrity at Scale

**Problem:** An order fact table has 500M rows. Standard `relationships` test runs a full cross-join — too slow.

**Solution: Optimized singular referential integrity test**

```sql
-- tests/referential/fct_orders_customer_id_exists.sql
-- Efficient: only scans for orphaned IDs, doesn't cross-join

select o.customer_id
from {{ ref('fct_orders') }} o
where o.order_date >= dateadd('day', -30, current_date)  -- only recent orders
  and not exists (
    select 1
    from {{ ref('dim_customers') }} c
    where c.customer_id = o.customer_id
  )
limit 100  -- surface just a sample of failures, not all 500M rows
```

## Pattern 4: store_failures-Powered Debugging Workflow

**Problem:** Test `unique_fct_orders_order_id` failed. 47 duplicate order IDs. Need to find and fix the root cause.

```sql
-- After running: dbt test --store-failures --select fct_orders

-- Inspect the stored failures
select *
from analytics.dbt_test__audit.unique_fct_orders_order_id
order by order_id
limit 20;

-- Find where duplicates originate
select order_id, count(*) as dupes
from {{ ref('fct_orders') }}
where order_id in (
    select order_id from analytics.dbt_test__audit.unique_fct_orders_order_id
)
group by order_id
having count(*) > 1;

-- Trace to source
select *
from {{ source('shopify', 'orders') }}
where id in (
    select order_id from analytics.dbt_test__audit.unique_fct_orders_order_id
)
order by id, _fivetran_synced;
```

This typically reveals the root cause in minutes rather than hours of guessing.

## Pattern 5: Unit Tests for Complex Logic

**Problem:** The discount calculation model has 8 tiers and complex business rules. Small changes keep introducing regressions.

```yaml
# models/tests/unit_tests.yml
unit_tests:
  - name: test_enterprise_discount_tier
    model: int_order_discounts
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, customer_segment: 'enterprise', subtotal: 9999}    # below tier
          - {order_id: 2, customer_segment: 'enterprise', subtotal: 10000}   # tier boundary
          - {order_id: 3, customer_segment: 'enterprise', subtotal: 50000}   # mid-tier
          - {order_id: 4, customer_segment: 'enterprise', subtotal: 100000}  # top tier
          - {order_id: 5, customer_segment: 'smb', subtotal: 100000}         # smb = no discount
    expect:
      rows:
        - {order_id: 1, discount_pct: 0,    discount_amount: 0}
        - {order_id: 2, discount_pct: 10,   discount_amount: 1000}
        - {order_id: 3, discount_pct: 15,   discount_amount: 7500}
        - {order_id: 4, discount_pct: 20,   discount_amount: 20000}
        - {order_id: 5, discount_pct: 0,    discount_amount: 0}
```

**Run only unit tests in pre-commit hook (fast):**
```bash
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: dbt-unit-tests
      name: dbt unit tests
      entry: dbt test --select "test_type:unit"
      language: system
      pass_filenames: false
```

## Real Interview Scenarios

**Scenario:** Your dbt test `not_null_fct_orders_customer_id` passes but a data analyst finds 500 orders with null customer IDs in a report. How do you investigate and prevent this?

**Answer:** Root cause analysis:
1. Check if the test was actually run against the current data: `dbt test --select not_null_fct_orders_customer_id`
2. Check the test config — maybe `severity: warn` was set and the failure was ignored
3. Check if the null customer IDs appeared after the last test run (timing gap)
4. Enable `store_failures: true` on the test so next time failing rows are persisted

Prevention:
- Set `severity: error` on this test (not warn)  
- Add `warn_if` / `error_if` thresholds to catch regressions before they accumulate
- Add a singular test that specifically checks for null customer IDs in orders created in the last 24 hours — more targeted and faster to debug

**Scenario:** You need to add tests to a legacy model but you can't change the model SQL. How do you add tests without risking breaking existing logic?

**Answer:** All dbt tests are additive — they don't modify the model. Just create or add to the `schema.yml` entry for the model:
```yaml
models:
  - name: legacy_model
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 10000000
          config:
            severity: warn  # start as warn so you can observe before making blocking
    columns:
      - name: id
        tests:
          - unique:
              config:
                severity: warn
```

Start with `severity: warn` to observe behavior without breaking the pipeline, then promote to `error` once you've confirmed the test is stable.
