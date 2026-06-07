---
title: "Backfilling - Scenario Questions"
topic: etl-concepts
subtopic: backfilling
content_type: scenario_question
tags: [etl, backfilling, airflow, interview, scenarios]
---

# Scenario Questions — Backfilling

<article data-difficulty="junior">

## 🟢 Junior: Backfill a New Analytics Table

**Scenario:** Your team just built a new `daily_active_users` table that aggregates user activity by day. The pipeline was deployed today, but the business needs this metric going back 6 months (180 days) for a trend report. How do you backfill it safely?

<details>
<summary>💡 Hint</summary>
Before running the backfill, verify the pipeline is idempotent — running it twice for the same date should produce the same result. Consider how Airflow's catchup feature or a manual backfill command can help.
</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Verify the pipeline is idempotent**

```python
def compute_daily_active_users(run_date: str, engine) -> int:
    """
    Idempotent: DELETE + INSERT for this date partition.
    Safe to run multiple times for the same date.
    """
    with engine.begin() as conn:
        # Delete existing data for this date (safe if table is empty)
        conn.execute(sa.text(
            "DELETE FROM daily_active_users WHERE activity_date = :d"
        ), {"d": run_date})

        # Compute and insert
        result = conn.execute(sa.text("""
            INSERT INTO daily_active_users (activity_date, user_count, session_count)
            SELECT
                :d AS activity_date,
                COUNT(DISTINCT user_id) AS user_count,
                COUNT(*) AS session_count
            FROM user_sessions
            WHERE DATE(session_start) = :d
        """), {"d": run_date})

    return result.rowcount
```

**Step 2: Run the Airflow backfill**

```bash
# Option A: Use Airflow's built-in backfill command
airflow dags backfill \
    --start-date 2023-10-01 \
    --end-date   2024-04-01 \
    --max-active-runs 4 \    # Run 4 dates in parallel for speed
    daily_active_users_dag

# Option B: If you want sequential (depends_on_past), just omit --max-active-runs
airflow dags backfill \
    --start-date 2023-10-01 \
    --end-date   2024-04-01 \
    daily_active_users_dag
```

**Step 3: Verify the backfill**

```python
def verify_backfill(start_date: str, end_date: str, engine):
    """
    After backfill, verify every expected date has data.
    """
    from datetime import date, timedelta

    start = date.fromisoformat(start_date)
    end   = date.fromisoformat(end_date)

    expected_dates = set()
    current = start
    while current <= end:
        expected_dates.add(str(current))
        current += timedelta(days=1)

    loaded_dates = set(pd.read_sql(
        "SELECT DISTINCT activity_date::text FROM daily_active_users WHERE activity_date >= :s",
        engine, params={"s": start_date}
    )["activity_date"].tolist())

    missing = expected_dates - loaded_dates
    if missing:
        print(f"WARNING: Missing data for {len(missing)} dates: {sorted(missing)[:10]}")
    else:
        print(f"Backfill complete! All {len(expected_dates)} dates loaded.")

    return {"missing_dates": sorted(missing), "loaded_dates": len(loaded_dates)}
```

**Key principles:**
- DELETE + INSERT (not raw INSERT) ensures no duplicates on re-run
- `--max-active-runs 4` speeds up a 180-date backfill (runs 4 days simultaneously)
- Always verify after backfill — check every date has data

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Backfill After a Transformation Bug Fix

**Scenario:** Your ETL pipeline has been running for 8 months. You discover that for the past 6 weeks, the revenue calculation was wrong: it excluded orders with `coupon_code IS NOT NULL`, so all orders with coupons were missing from the revenue total. The bug was in a dbt model. How do you fix and backfill correctly?

<details>
<summary>💡 Hint</summary>
Think about which downstream models depend on the revenue model. After fixing the dbt model, you need to rerun not just the broken model but all its dependents. Consider how to verify the correction was applied correctly.
</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Fix the dbt model**

```sql
-- models/silver/daily_revenue.sql  (BEFORE — broken)
SELECT
    DATE(created_at) AS revenue_date,
    SUM(total_usd)   AS revenue_usd
FROM {{ source('raw', 'orders') }}
WHERE coupon_code IS NULL   -- BUG: excludes coupon orders!
{% if is_incremental() %}
AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
GROUP BY 1

-- models/silver/daily_revenue.sql  (AFTER — fixed)
SELECT
    DATE(created_at) AS revenue_date,
    SUM(total_usd)   AS revenue_usd,
    SUM(CASE WHEN coupon_code IS NOT NULL THEN total_usd ELSE 0 END) AS coupon_revenue_usd,
    COUNT(CASE WHEN coupon_code IS NOT NULL THEN 1 END) AS coupon_orders
FROM {{ source('raw', 'orders') }}
-- Removed the bug: no longer filtering out coupon orders
{% if is_incremental() %}
AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
GROUP BY 1
```

**Step 2: Quantify the impact before re-running**

```sql
-- How much revenue was missing?
SELECT
    DATE(created_at) AS revenue_date,
    SUM(total_usd)   AS correct_revenue,
    SUM(CASE WHEN coupon_code IS NULL THEN total_usd ELSE 0 END) AS stored_revenue,
    SUM(CASE WHEN coupon_code IS NOT NULL THEN total_usd ELSE 0 END) AS missing_coupon_revenue
FROM orders
WHERE DATE(created_at) BETWEEN '2024-02-01' AND '2024-03-17'  -- Bug window
GROUP BY 1
ORDER BY 1;
```

**Step 3: Backfill the affected date range**

```bash
# Backfill only the affected 6-week window
dbt run \
    --full-refresh \
    --select daily_revenue+ \     # daily_revenue and all downstream models
    --vars '{
        "backfill_start": "2024-02-01",
        "backfill_end":   "2024-03-17"
    }'
```

Wait — `--full-refresh` on the entire model would wipe ALL history, then rebuild from scratch. For a targeted date-range fix, use an incremental approach:

```bash
# Better: Only reprocess the 6-week window, preserve older data
dbt run \
    --select daily_revenue \
    --vars '{
        "backfill_mode":  true,
        "backfill_start": "2024-02-01",
        "backfill_end":   "2024-03-17"
    }'

# Then rerun downstream models
dbt run --select +monthly_revenue +executive_summary
```

In `daily_revenue.sql`, add backfill mode support:

```sql
{% if is_incremental() and not var('backfill_mode', false) %}
AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% elif var('backfill_mode', false) %}
AND DATE(created_at) BETWEEN '{{ var("backfill_start") }}' AND '{{ var("backfill_end") }}'
{% endif %}
```

**Step 4: Validate the fix**

```python
def validate_revenue_correction(engine, start_date: str, end_date: str):
    # Compare corrected dbt output vs ground truth from raw orders
    sql_truth = """
        SELECT DATE(created_at) AS d, SUM(total_usd) AS correct_total
        FROM orders
        WHERE DATE(created_at) BETWEEN :s AND :e
        GROUP BY 1
    """
    sql_model = """
        SELECT revenue_date AS d, revenue_usd AS model_total
        FROM daily_revenue
        WHERE revenue_date BETWEEN :s AND :e
    """
    truth = pd.read_sql(sa.text(sql_truth), engine, params={"s": start_date, "e": end_date})
    model = pd.read_sql(sa.text(sql_model), engine, params={"s": start_date, "e": end_date})

    comparison = truth.merge(model, on="d")
    comparison["diff_pct"] = abs(comparison["correct_total"] - comparison["model_total"]) / comparison["correct_total"] * 100

    discrepancies = comparison[comparison["diff_pct"] > 0.01]
    if not discrepancies.empty:
        print(f"VALIDATION FAILED: {len(discrepancies)} dates still incorrect")
        print(discrepancies)
    else:
        print("Validation passed: all corrected dates match source truth")

    return len(discrepancies) == 0
```

**Step 5: Notify stakeholders**

```
To: Finance, Analytics
Re: Revenue Data Correction — Action Required

We've corrected a bug in our daily revenue calculation that was excluding orders
with coupon codes. The affected period is Feb 1 – Mar 17, 2024.

Corrected revenue impact:
  - Total previously missing: $847,432
  - Affected dates: 45 days
  - Corrected data now available in: daily_revenue, monthly_close, exec_dashboard

All downstream reports have been recomputed. Please refresh any Excel extracts
from the executive_summary table.
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Zero-Downtime 2-Year Backfill at Petabyte Scale

**Scenario:** You join a company as their new senior data engineer. Their main fact table has 5 years of data (3 PB in Snowflake) and was never properly deduped. Deduplication analysis shows ~8% of rows are duplicates based on a composite business key. The analytics team needs clean data, but the table is queried 24/7 by 300+ dashboards. You need to: deduplicate 3 PB of data, backfill 2 years of derived aggregate tables, and do all of this with zero downtime. How?

<details>
<summary>💡 Hint</summary>
Think about blue-green table swap for the main fact table, incremental processing of chunks (not full table at once), and coordinating the downstream aggregate recomputation with lineage analysis.
</details>

<details>
<summary>✅ Solution</summary>

**Phase 1: Non-disruptive deduplication (2-3 weeks)**

```sql
-- Step 1: Create clean table (doesn't touch production)
CREATE TABLE fact_orders_clean CLONE fact_orders;
-- Zero-copy clone in Snowflake — instant, no storage cost initially

-- Step 2: Deduplicate in chunks (avoid single massive query)
-- Process 1 month at a time to avoid warehouse timeout
BEGIN;
CREATE OR REPLACE TABLE fact_orders_clean AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, customer_id, order_date  -- Business key
            ORDER BY updated_at DESC, ingested_at DESC
        ) AS rn
    FROM fact_orders_clean
    WHERE order_month = '2024-01'   -- Process one month at a time
)
WHERE rn = 1;
COMMIT;
-- Repeat for each month in parallel across warehouses
```

```python
from snowflake.connector import connect
from concurrent.futures import ThreadPoolExecutor

def deduplicate_month(year_month: str, conn_params: dict) -> dict:
    """Deduplicate one month's data in the clean table."""
    conn = connect(**conn_params)
    cur  = conn.cursor()

    cur.execute(f"""
        UPDATE fact_orders_clean
        SET _is_duplicate = TRUE
        FROM (
            SELECT order_id, customer_id, order_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY order_id, customer_id, order_date
                       ORDER BY updated_at DESC
                   ) AS rn
            FROM fact_orders_clean
            WHERE order_month = '{year_month}'
        ) ranked
        WHERE ranked.rn > 1
          AND fact_orders_clean.order_month = '{year_month}'
    """)

    dup_count = cur.rowcount
    cur.execute(f"""
        DELETE FROM fact_orders_clean
        WHERE _is_duplicate = TRUE
          AND order_month = '{year_month}'
    """)

    conn.close()
    return {"month": year_month, "duplicates_removed": dup_count}

# Process all 60 months (5 years) in parallel across 8 Snowflake warehouses
months = [f"{y}-{m:02d}" for y in range(2019, 2024) for m in range(1, 13)]

with ThreadPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(
        lambda m: deduplicate_month(m, SNOWFLAKE_PARAMS), months
    ))

total_dupes = sum(r["duplicates_removed"] for r in results)
print(f"Removed {total_dupes:,} duplicate rows from {len(months)} months")
```

**Phase 2: Validation (1 week)**

```sql
-- Spot-check deduplication correctness across random sample
SELECT
    COUNT(*) AS total_in_clean,
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM fact_orders) AS retention_rate,
    -- Should be ~0.92 (100% - 8% duplicates)
    SUM(total_usd) AS clean_revenue,
    (SELECT SUM(total_usd) FROM fact_orders) AS original_revenue
    -- Revenue should be lower (duplicates inflated it)
FROM fact_orders_clean;

-- Verify no business keys are duplicated in clean table
SELECT order_id, customer_id, order_date, COUNT(*)
FROM fact_orders_clean
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
LIMIT 10;  -- Should return 0 rows
```

**Phase 3: Atomic swap (near-zero downtime)**

```sql
-- In Snowflake, swap table names atomically using ALTER TABLE SWAP WITH
-- This is an instantaneous metadata operation — no data moved
ALTER TABLE fact_orders SWAP WITH fact_orders_clean;

-- fact_orders now points to the clean data
-- fact_orders_clean now points to the old (duplicate-filled) data for rollback
```

This swap takes <1 second. During that time, running queries complete against the old table. New queries after the swap see the clean data. Zero dashboard downtime.

**Phase 4: Downstream aggregate recomputation (1 week)**

```python
import networkx as nx

def build_lineage_graph() -> nx.DiGraph:
    """Query dbt lineage or data catalog for fact_orders dependencies."""
    # Simplified — in practice, parse dbt manifest.json
    g = nx.DiGraph()
    g.add_edges_from([
        ("fact_orders",         "daily_revenue"),
        ("fact_orders",         "customer_ltv"),
        ("fact_orders",         "product_performance"),
        ("daily_revenue",       "monthly_revenue"),
        ("monthly_revenue",     "exec_dashboard"),
        ("customer_ltv",        "retention_report"),
        ("product_performance", "category_dashboard"),
    ])
    return g

lineage = build_lineage_graph()
downstream = list(nx.topological_sort(
    lineage.subgraph(nx.descendants(lineage, "fact_orders"))
))

# Recompute in topological order
for model in downstream:
    print(f"Recomputing {model}...")
    subprocess.run([
        "dbt", "run", "--full-refresh",
        "--select", model,
        "--target", "prod"
    ], check=True)
    print(f"{model} complete")
```

**Phase 5: Communicate and monitor**

```
Timeline Summary:
  Week 1-2:  Deduplication runs in background on fact_orders_clean
  Week 3:    Validation: row counts, revenue, no remaining dupes
  Day 1 of Week 4: 09:00 UTC — Table swap (< 1 second downtime)
  Day 1-5 of Week 4: Aggregate table recomputation (no dashboard impact)
  
Post-swap monitoring:
  - Dashboard load times: should decrease (fewer rows scanned)
  - Revenue metrics: expect ~8% decrease (duplicates removed)
  - Stakeholder notification: "Revenue figures corrected; historical reports reflect actual performance"
```

**Key design decisions:**
1. **Zero-copy clone** for deduplication — doesn't touch production, no extra storage cost initially
2. **Month-by-month processing** — avoids single 3 PB query; parallelizable
3. **Atomic SWAP** — instantaneous metadata operation; no downtime
4. **Topological recomputation** — ensures upstream models are correct before downstream ones run
5. **8% revenue decrease is expected** — pre-communicate to finance and analytics so the drop isn't alarming

</details>

</article>
