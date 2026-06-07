---
title: "Teradata - Query Optimization Scenarios"
topic: teradata
subtopic: query-optimization
content_type: scenario_question
difficulty_level: senior
layer: scenarios
tags: [teradata, query-optimization, scenarios, explain, product-join, statistics]
---

# Query Optimization — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Reading an EXPLAIN Plan

You run the following EXPLAIN:

```
1) First, we lock SALES.orders for read.
   We lock SALES.customer for read.

2) Next, we do an all-AMPs RETRIEVE step from SALES.orders
   by way of an all-rows scan with no residual conditions,
   extracting rows into spool 1 (all AMPs).
   The size of spool 1 is estimated with no confidence to be 2,000 rows.

3) We do an all-AMPs PRODUCT JOIN step from spool 1 (all AMPs) and
   SALES.customer (all AMPs), resulting in spool 2 (all AMPs).
   The size of spool 2 is estimated with no confidence to be 4,000,000 rows.

4) Finally, we do a SUM step...
```

The actual `orders` table has 500 million rows. What is wrong with this plan, and how do you fix it?

<details>
<summary>💡 Hint</summary>

Look at the confidence level and the estimated row count. What does "no confidence" mean? Why would the optimizer think orders has 2,000 rows when it has 500 million? What type of join is in step 3?

</details>

<details>
<summary>✅ Solution</summary>

**Problems identified:**

1. **"No confidence"** means statistics have never been collected on `orders`. The optimizer is using a default estimate (2,000 rows) instead of the actual 500 million.

2. **PRODUCT JOIN in step 3** — because the optimizer thinks both tables are tiny, it chose a Cartesian-style product join. With 500M actual rows, this will run for hours or cause a spool error.

3. **Estimated 4M rows in spool 2** is wildly wrong. With 500M orders and (say) 10M customers, the actual join result could be 500M+ rows.

**Fix:**

```sql
-- Collect statistics on both tables
COLLECT STATISTICS ON orders COLUMN (customer_id);
COLLECT STATISTICS ON orders INDEX (customer_id);  -- PI stats
COLLECT STATISTICS ON orders COLUMN (order_date);

COLLECT STATISTICS ON customer COLUMN (customer_id);
COLLECT STATISTICS ON customer INDEX (customer_id);
```

**Re-run EXPLAIN after collecting stats.** Expected new plan:
- "high confidence" row estimates
- MERGE JOIN instead of PRODUCT JOIN (both tables have customer_id as PI → AMP-local merge)
- Much smaller spool estimates

**Key takeaway:** "No confidence" in EXPLAIN is always a red flag. The very first fix for any slow Teradata query is to check whether statistics exist and are current.

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Spool Space Error in Production

At 2 AM, your ETL batch fails with:

```
Error 2646: No more spool space in database.
```

The failing query is:

```sql
INSERT INTO daily_summary
SELECT
    c.customer_segment,
    p.product_category,
    d.fiscal_quarter,
    SUM(f.revenue)    AS total_revenue,
    COUNT(f.fact_id)  AS transaction_count
FROM fact_sales f
JOIN dim_customer   c ON f.customer_id   = c.customer_id
JOIN dim_product    p ON f.product_id    = p.product_id
JOIN dim_date       d ON f.sale_date     = d.date_key
GROUP BY 1, 2, 3;
```

`fact_sales` has 2 billion rows. The user's spool quota is 50 GB. The query is consuming 200+ GB of spool before failing.

Diagnose and propose both an immediate fix and a permanent solution.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

Run EXPLAIN on the failing query. Likely issues:
1. Missing statistics → poor join ordering → large intermediate spool
2. fact_sales may not have PPI, forcing full scan even if only some dates are needed
3. The GROUP BY produces many combinations: customer_segment × product_category × fiscal_quarter

**Immediate fixes (for tonight):**

```sql
-- Option 1: Add date filter to reduce fact_sales rows scanned
INSERT INTO daily_summary
SELECT ...
FROM fact_sales f
JOIN dim_customer   c ON f.customer_id   = c.customer_id
JOIN dim_product    p ON f.product_id    = p.product_id
JOIN dim_date       d ON f.sale_date     = d.date_key
WHERE f.sale_date >= CURRENT_DATE - 1  -- only process yesterday's data
GROUP BY 1, 2, 3;
-- Then handle the historical rows in a separate catch-up run
```

```sql
-- Option 2: Collect statistics so optimizer picks better join order
COLLECT STATISTICS ON fact_sales COLUMN (customer_id);
COLLECT STATISTICS ON fact_sales COLUMN (product_id);
COLLECT STATISTICS ON fact_sales COLUMN (sale_date);
COLLECT STATISTICS ON fact_sales INDEX (customer_id);  -- PI stats
-- Then re-run and check EXPLAIN for confidence level change
```

```sql
-- Option 3: Increase spool quota for the ETL user (temporary)
MODIFY USER etl_user AS SPOOLSPACE = 200000000000;  -- 200 GB
```

**Permanent solutions:**

1. **Add PPI to fact_sales on sale_date** — if ETL processes daily increments, PPI eliminates 364/365 partitions on daily runs

2. **Pre-aggregate dimension data** — if dim_customer and dim_product are large, create denormalized staging tables with pre-joined attribute columns, reducing join complexity at query time

3. **Break the query into steps with volatile tables:**
```sql
-- Step 1: Aggregate fact with customer dimension (smaller intermediate)
CREATE VOLATILE TABLE vt_step1 AS (
    SELECT f.product_id, f.sale_date, c.customer_segment,
           SUM(f.revenue) AS revenue, COUNT(*) AS cnt
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (product_id, sale_date) ON COMMIT PRESERVE ROWS;

-- Step 2: Join smaller aggregated result with product and date dims
INSERT INTO daily_summary
SELECT v.customer_segment, p.product_category, d.fiscal_quarter,
       SUM(v.revenue), SUM(v.cnt)
FROM vt_step1 v
JOIN dim_product p ON v.product_id = p.product_id
JOIN dim_date   d ON v.sale_date   = d.date_key
GROUP BY 1, 2, 3;
```

This reduces spool by aggregating early (step 1 produces far fewer rows than 2 billion).

4. **Collect statistics on all dimension join columns** — ensure optimizer picks correct join order (smallest dimension first)

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Query Performance Regression After Teradata Upgrade

After upgrading from Teradata 16.x to 17.x, a set of critical reports regressed from 15-minute runtimes to 2-hour runtimes. The queries haven't changed. Statistics are up-to-date. The EXPLAIN plans have changed — the new optimizer is choosing different join strategies.

How do you investigate, stabilize, and resolve this regression?

<details>
<summary>💡 Hint</summary>

Think about: how do you compare old vs new EXPLAIN plans? Are there optimizer behavior changes in 17.x? What levers do you have to force specific plan choices? What's the long-term strategy for plan stability?

</details>

<details>
<summary>✅ Solution</summary>

**Investigation approach:**

**Step 1: Document the regression precisely**
```sql
-- Compare DBQL data: pre-upgrade vs post-upgrade
SELECT
    SUBSTR(QueryText, 1, 100) AS QueryID,
    AVG(AMPCPUTime)           AS AvgCPU,
    AVG(ElapsedTime)          AS AvgElapsed,
    COUNT(*)                  AS Executions
FROM DBC.QryLogV
WHERE LogDate BETWEEN '2024-09-01' AND '2024-09-10'  -- post-upgrade
   OR LogDate BETWEEN '2024-08-01' AND '2024-08-10'  -- pre-upgrade
GROUP BY 1
ORDER BY AvgElapsed DESC;
```

**Step 2: Compare EXPLAIN plans**
- Save old EXPLAIN output (from documentation or DEV/TEST system still on 16.x)
- Generate new EXPLAIN on 17.x
- Identify specific steps that changed (join order, join type, redistribution decisions)

**Step 3: Identify 17.x optimizer changes**
- Review Teradata 17.x release notes for optimizer behavior changes
- Common changes: new statistics requirements, changed cost formulas, new join algorithms enabled by default
- Contact Teradata support for known optimizer regression patches

**Step 4: Immediate stabilization options**

```sql
-- Option A: Force old join ordering with session-level override
-- (Teradata-specific session parameters)
SET SESSION OVERRIDE JOINPLAN = 'MERGEJOIN';

-- Option B: Add query band hints
SET QUERY_BAND = 'OptLevel=3;' FOR SESSION;
-- OptLevel controls optimizer aggressiveness

-- Option C: Statistics refresh with full scan (not sample)
-- 17.x may have different sampling defaults
COLLECT STATISTICS USING SAMPLE 100 PERCENT
ON fact_sales COLUMN (customer_id);
```

**Step 5: Multi-column statistics (often needed after upgrades)**
```sql
-- 17.x optimizer may leverage multi-column stats that 16.x ignored
COLLECT STATISTICS COLUMN (customer_id, order_date) ON fact_sales;
COLLECT STATISTICS COLUMN (product_id, order_date) ON fact_sales;
```

**Step 6: Join Index consideration**
For the most critical queries that can't be tuned via stats alone:
```sql
-- Create join index to force a specific pre-computed access path
CREATE JOIN INDEX ji_critical_report AS
SELECT f.sale_date, f.customer_id, f.product_id, 
       c.segment, p.category, SUM(f.revenue) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product  p ON f.product_id  = p.product_id
GROUP BY 1, 2, 3, 4, 5
PRIMARY INDEX (f.customer_id)
PARTITION BY RANGE_N(f.sale_date BETWEEN DATE '2020-01-01' 
                     AND DATE '2026-12-31' EACH INTERVAL '1' MONTH);
```
The optimizer will use this join index for the critical report, bypassing the regression entirely.

**Long-term strategy:**

1. **Teradata QueryGrid lab environment:** Set up a pre-production environment on 17.x that mirrors production data (or a statistically representative sample). Run regression tests before future upgrades.

2. **Plan baselines (Teradata Plan Management):** Capture known-good EXPLAIN plans and lock them using Teradata's query band / plan capture features — prevents optimizer from changing plans arbitrarily.

3. **DBQL monitoring dashboard:** Set up alerts for queries where ElapsedTime increases > 50% versus rolling 30-day baseline. Catch regressions quickly before they impact SLAs.

4. **Engage Teradata Support:** Optimizer regressions after upgrades are known issues. Teradata TAR (Technical Assistance Request) often yields patches or specific DBMS configuration parameters that restore 16.x-compatible behavior for affected queries.

**Key insight for the interview:** The answer demonstrates systematic debugging (isolate → document → compare → fix) plus knowledge of multiple lever types (session parameters, statistics, join indexes, vendor support). A senior engineer doesn't just fix the immediate issue — they build the infrastructure to prevent recurrence.

</details>

</article>
