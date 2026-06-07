---
title: "Materialized Views - Scenario Questions"
topic: snowflake
subtopic: materialized-views
content_type: scenario_question
tags: [snowflake, materialized-views, interview, scenarios]
---

# Scenario Questions — Materialized Views

<article data-difficulty="junior">

## 🟢 Junior: When to Use an MV

**Scenario:** A dashboard runs `SELECT region, SUM(amount) FROM orders GROUP BY region` 100 times per day. Each execution scans 500 GB and takes 20 seconds. Should you create a Materialized View?

<details>
<summary>💡 Hint</summary>
Calculate: cost without MV (100 × scan cost) vs cost with MV (refresh cost + tiny query cost). If the query is frequent and scans a large table, an MV almost always pays for itself.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- YES: Create an MV! This is the ideal use case.

CREATE MATERIALIZED VIEW gold.mv_revenue_by_region AS
    SELECT region, SUM(amount) AS total_revenue, COUNT(*) AS order_count
    FROM silver.orders
    GROUP BY region;

-- COST ANALYSIS:
-- Without MV: 100 queries/day × 500 GB scanned × $5/TB = $0.25/query × 100 = $25/day
-- With MV: 
--   Refresh: source changes ~10x/day × ~$0.01/refresh = $0.10/day
--   Queries: 100 × 10 MB scanned × $5/TB = $0.000005/query = negligible!
-- Total with MV: $0.10/day
-- SAVINGS: $25/day → $0.10/day = 99.6% reduction! ($747/month savings)

-- Additionally: queries go from 20 seconds → <1 second (better user experience!)
```

**Key Points:**
- Ideal MV use case: frequent query + large table scan + simple aggregation
- The MV result (revenue by ~10 regions) is tiny vs the 500 GB source
- 100 queries/day easily justifies the small refresh cost
- Query rewriting is automatic — dashboard code doesn't change!
- Rule of thumb: if query runs >10x/day and scans >10 GB, consider an MV

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: MV with Query Rewriting

**Scenario:** You created an MV with GROUP BY (region, product_category, order_date). A user queries `SELECT region, SUM(amount) FROM orders GROUP BY region` (fewer dimensions than the MV). Will the optimizer use the MV?

<details>
<summary>💡 Hint</summary>
Yes! If the MV's GROUP BY is a SUPERSET of the query's GROUP BY, the optimizer can sum across the extra dimensions. The MV covers this query.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- MV definition (3 grouping columns):
CREATE MATERIALIZED VIEW gold.mv_detailed AS
    SELECT region, product_category, order_date,
           SUM(amount) AS revenue, COUNT(*) AS orders
    FROM silver.orders
    GROUP BY region, product_category, order_date;

-- User query (1 grouping column — SUBSET of MV):
SELECT region, SUM(amount) AS total_revenue
FROM silver.orders
GROUP BY region;

-- DOES THE OPTIMIZER USE THE MV? YES!
-- How: reads from MV, then SUMs revenue across product_category and order_date
-- MV has: region × category × date → SUM within each group
-- Query needs: region → SUM across ALL categories and ALL dates
-- Optimizer: SELECT region, SUM(revenue) FROM mv_detailed GROUP BY region
-- This is MUCH faster than scanning the source table!

-- VERIFY (check query plan):
EXPLAIN SELECT region, SUM(amount) FROM silver.orders GROUP BY region;
-- Plan shows: "MaterializedViewAccess" operator → confirmed MV is used!

-- RULES for query rewriting:
-- ✅ Query GROUP BY is SUBSET of MV GROUP BY → works (sum across extra dims)
-- ✅ Query has WHERE that filters MV columns → works (filter applied to MV)
-- ❌ Query GROUP BY has columns NOT in MV → can't use MV (must scan source)
-- ❌ Query uses aggregates not derivable from MV (e.g., MEDIAN from SUM/COUNT) → no
```

**Key Points:**
- MV GROUP BY = superset of query GROUP BY → optimizer rewrites (sums across extra dims)
- SUM/COUNT/MIN/MAX can be "rolled up" from finer to coarser granularity
- AVG can be derived from SUM/COUNT (AVG = SUM/COUNT)
- This is why MVs should be DETAILED (more GROUP BY columns = more queries covered)
- One detailed MV can serve dozens of different dashboard widgets!

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: MV Cost-Benefit Analysis

**Scenario:** You have 10 MVs costing $500/month total in refresh credits. 3 MVs haven't been used in the last 30 days (no matching queries). 2 MVs refresh 1000+ times/day (source updates every minute). Optimize.

<details>
<summary>💡 Hint</summary>
Drop unused MVs (zero benefit, pure cost). For high-refresh MVs on frequently-updating sources: check if the refresh cost exceeds query savings. Consider Dynamic Tables as alternative for high-churn sources.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- STEP 1: Identify unused MVs (no query benefit)
-- Check: do any queries actually hit these MVs?
-- Method: compare MV creation columns with QUERY_HISTORY patterns

-- If no queries match the MV's pattern in 30 days → DROP!
DROP MATERIALIZED VIEW gold.mv_unused_1;  -- Saves ~$50/month
DROP MATERIALIZED VIEW gold.mv_unused_2;  -- Saves ~$50/month  
DROP MATERIALIZED VIEW gold.mv_unused_3;  -- Saves ~$50/month
-- Savings: $150/month (30% of total!) from dropping 3 unused MVs

-- STEP 2: Analyze high-refresh MVs
-- 2 MVs refresh 1000+/day because source updates every minute
-- Refresh cost: ~$0.01 × 1000 = $10/day = $300/month for these 2 MVs!

-- Option A: Accept the cost if query savings justify it
-- If these MVs serve 500+ queries/day that would otherwise scan 1 TB:
-- Query savings: 500 × $0.50/query = $250/day saved → KEEP (net positive!)

-- Option B: If query volume is low (<50/day), replace with Dynamic Table
-- DT refreshes on YOUR schedule (every 15 min, not every minute)
DROP MATERIALIZED VIEW gold.mv_high_churn_1;
CREATE DYNAMIC TABLE gold.dt_replacement TARGET_LAG = '15 minutes' WAREHOUSE = 'ETL_XS' AS
    SELECT ... (same query as the old MV);
-- DT refresh: 96/day (every 15 min) vs 1440/day (every minute) = 93% fewer refreshes
-- Cost: ~$30/month vs ~$150/month = 80% savings!

-- FINAL STATE:
-- Before: 10 MVs, $500/month
-- After: 7 MVs + 2 DTs, $200/month (60% savings!)
-- Dropped: 3 unused ($150 saved)
-- Converted: 2 high-churn MVs → DTs ($100 saved, controlled refresh)
-- Kept: 5 efficient MVs ($200/month, high ROI)
```

**Key Points:**
- Drop unused MVs immediately (pure cost, zero benefit)
- High-refresh MVs: acceptable if query savings exceed refresh cost
- For high-churn sources: Dynamic Tables give you control over refresh frequency
- Monthly review: track refresh credits + query patterns → drop/convert as needed
- Rule: every MV should save at least 3× its refresh cost (otherwise it's net negative)

</details>

</article>
