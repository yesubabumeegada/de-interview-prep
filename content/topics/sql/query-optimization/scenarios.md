---
title: "SQL Query Optimization - Scenario Questions"
topic: sql
subtopic: query-optimization
content_type: scenario_question
tags: [sql, optimization, interview, scenarios, performance]
---

# Scenario Questions — SQL Query Optimization

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Function on Indexed Column

**Scenario:** This query takes 30 seconds on a 50M-row table with an index on `order_date`. Why?

```sql
SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2024;
```

<details>
<summary>💡 Hint</summary>
Wrapping a column in a function prevents the optimizer from using the index on that column.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Rewrite as a range condition (uses the index!)
SELECT * FROM orders 
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01';
```

**Explanation:**
- `EXTRACT(YEAR FROM order_date)` computes a value for every row → can't use index → full scan
- A range condition `>= AND <` allows an Index Range Scan → reads only matching rows
- This single fix can turn a 30-second query into a sub-second query

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Choose the Best Index

**Scenario:** This query runs frequently. Design the optimal index:

```sql
SELECT customer_id, order_date, amount
FROM orders
WHERE customer_id = 42
ORDER BY order_date DESC
LIMIT 10;
```

<details>
<summary>✅ Solution</summary>

```sql
CREATE INDEX idx_orders_cust_date ON orders(customer_id, order_date DESC)
INCLUDE (amount);
```

**Explanation:**
- `customer_id` first: satisfies equality filter (index seek)
- `order_date DESC` second: data is pre-sorted for the ORDER BY (no sort needed)
- `INCLUDE (amount)`: covering index — all needed columns are in the index, zero table lookups
- Result: seek + read 10 entries + return. ~1ms execution.

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: SELECT * on Columnar Database

**Scenario:** On Snowflake (columnar storage), Query A takes 45 seconds and scans 50 GB. Query B takes 3 seconds and scans 2 GB. Same table, same filter. Why?

```sql
-- Query A (slow)
SELECT * FROM fact_events WHERE event_date = '2024-01-15';

-- Query B (fast)  
SELECT event_id, user_id, event_type FROM fact_events WHERE event_date = '2024-01-15';
```

<details>
<summary>✅ Solution</summary>

**Explanation:**
- Columnar databases store each column in separate files
- `SELECT *` (50 columns) reads ALL 50 column files = 50 GB
- `SELECT col1, col2, col3` reads only 3 column files = 2 GB (94% less I/O!)
- Rule: NEVER use `SELECT *` on columnar databases in production
- On Athena: this difference also affects cost ($5/TB scanned)

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Missing Index on Join Column

**Scenario:** This join takes 10 minutes. `orders` has 100M rows, `customers` has 5M rows. Both tables have primary key indexes. What's missing?

```sql
SELECT c.name, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

<details>
<summary>✅ Solution</summary>

```sql
-- Add index on the foreign key column
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
```

**Explanation:**
- `customers.id` has a PK index (used when customers is the inner table)
- `orders.customer_id` has NO index → full scan of 100M rows for each lookup
- Adding the FK index enables an index nested loop or efficient hash join
- Rule: every foreign key column should have an index

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: UNION vs UNION ALL

**Scenario:** This query combines data from two regional tables. It takes 3x longer than expected. Why?

```sql
SELECT order_id, amount, 'US' AS region FROM orders_us
UNION
SELECT order_id, amount, 'EU' AS region FROM orders_eu;
```

<details>
<summary>✅ Solution</summary>

```sql
-- Use UNION ALL (no dedup needed — regions are already distinct)
SELECT order_id, amount, 'US' AS region FROM orders_us
UNION ALL
SELECT order_id, amount, 'EU' AS region FROM orders_eu;
```

**Explanation:**
- `UNION` removes duplicates (requires sorting or hashing ALL rows — expensive!)
- `UNION ALL` just concatenates (no dedup — instant)
- Since the regions are separate tables, there CAN'T be duplicates → dedup is wasted work
- Rule: always use UNION ALL unless you specifically need deduplication

</details>
</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Optimize a Slow Reporting Join

**Scenario:** `fact_sales` (500M rows, partitioned by sale_date) joined with `dim_product` (100K rows) and `dim_store` (5K rows). Query takes 15 minutes for a monthly report. Optimize:

```sql
SELECT s.store_name, p.category, SUM(f.amount)
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_store s ON f.store_key = s.store_key
WHERE f.sale_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY s.store_name, p.category;
```

<details>
<summary>✅ Solution</summary>

```sql
-- 1. Partition pruning (if not automatic)
-- 2. Broadcast small dimensions
SELECT /*+ BROADCAST(p), BROADCAST(s) */
    s.store_name, p.category, SUM(f.amount)
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_store s ON f.store_key = s.store_key
WHERE f.sale_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY s.store_name, p.category;
```

**Explanation:**
- Partition pruning: with date-partitioned fact table, only January data is scanned (500M → ~15M rows)
- Broadcast: dim_product (100K) and dim_store (5K) are tiny — copying to all nodes eliminates shuffle of the 15M-row fact
- Combined: 15 min → 30 seconds (data movement eliminated)

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Stale Statistics Causing Plan Regression

**Scenario:** A query that normally runs in 5 seconds suddenly takes 10 minutes after last night's data load. No code or index changes. Diagnose and fix.

<details>
<summary>✅ Solution</summary>

```sql
-- Diagnosis: check if optimizer's estimates are wrong
EXPLAIN ANALYZE SELECT ...;
-- Look for: "rows=100 (actual rows=5000000)" ← estimate vs reality mismatch

-- Fix: refresh statistics
ANALYZE orders;                  -- PostgreSQL
UPDATE STATISTICS orders;        -- SQL Server
-- Snowflake: automatic, but force with ALTER TABLE orders REBUILD;

-- Prevention: run ANALYZE after every significant data load in your ETL pipeline
```

**Explanation:**
- Large data loads change data distribution but don't auto-update statistics
- Optimizer uses old stats → wrong row estimates → chooses bad plan (e.g., nested loop instead of hash join)
- Fix is simple: refresh statistics. Prevention: add ANALYZE to your ETL pipeline after loads.

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Correlated Subquery to JOIN

**Scenario:** This query runs a subquery for EACH row in employees (N+1 pattern). Rewrite for performance:

```sql
SELECT name, salary,
    (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department) AS dept_avg
FROM employees e1;
```

<details>
<summary>✅ Solution</summary>

```sql
-- Option 1: JOIN to pre-computed aggregate
SELECT e.name, e.salary, d.dept_avg
FROM employees e
JOIN (SELECT department, AVG(salary) AS dept_avg FROM employees GROUP BY department) d
    ON e.department = d.department;

-- Option 2: Window function (even better — single scan)
SELECT name, salary, AVG(salary) OVER (PARTITION BY department) AS dept_avg
FROM employees;
```

**Explanation:**
- Correlated subquery: executes once PER ROW in outer query (N times for N employees)
- JOIN approach: subquery runs ONCE, then joined (1 + 1 = 2 scans total)
- Window function: single table scan, computes aggregate alongside each row
- Performance: N scans → 1-2 scans. On 1M rows: minutes → seconds.

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: COUNT(DISTINCT) on 1 Billion Rows

**Scenario:** You need daily unique user counts from a 1B-row events table. The exact query takes 45 minutes:

```sql
SELECT event_date, COUNT(DISTINCT user_id) FROM events
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY event_date;
```

Propose multiple solutions with trade-offs.

<details>
<summary>✅ Solution</summary>

| Solution | Accuracy | Speed | Complexity |
|----------|----------|-------|-----------|
| **Approximate (HyperLogLog)** | ±2% | 30 sec | Low |
| **Pre-aggregate table** | Exact | Instant | Medium (ETL needed) |
| **Partition + parallel** | Exact | 3 min | Medium |
| **Bitmap index (ClickHouse)** | Exact | 5 sec | Specialized DB |

```sql
-- Solution 1: Approximate (Snowflake/BigQuery)
SELECT event_date, APPROX_COUNT_DISTINCT(user_id) FROM events GROUP BY event_date;

-- Solution 2: Pre-aggregate (nightly ETL computes per day)
-- INSERT INTO daily_uniques SELECT event_date, COUNT(DISTINCT user_id) FROM events WHERE event_date = yesterday;
-- Then: SELECT * FROM daily_uniques WHERE event_date BETWEEN ...;

-- Solution 3: Ensure partition pruning (date-partitioned table)
-- If table is partitioned by event_date, only 31 partitions are scanned (not all 365+)
```

**Explanation:**
- COUNT(DISTINCT) is expensive: must track all unique values in memory per group
- For dashboards: approximate is usually acceptable (±2% is fine for "~5M users")
- For financial/billing: pre-aggregate table provides exact counts instantly
- Rule: don't recompute expensive aggregates on every dashboard refresh — pre-compute them

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: N+1 Query Pattern

**Scenario:** Application code fetches 1000 customers, then for each one queries their orders. Total: 1001 DB queries. Redesign for performance.

```python
customers = db.query("SELECT id, name FROM customers LIMIT 1000")
for customer in customers:
    orders = db.query(f"SELECT * FROM orders WHERE customer_id = {customer.id}")
    # 1000 individual queries!
```

<details>
<summary>✅ Solution</summary>

```python
# Solution 1: Single query with JOIN
results = db.query("""
    SELECT c.id, c.name, o.order_id, o.amount
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    LIMIT 1000
""")

# Solution 2: Batch lookup with IN clause
customers = db.query("SELECT id, name FROM customers LIMIT 1000")
ids = [c.id for c in customers]
orders = db.query(f"SELECT * FROM orders WHERE customer_id IN ({','.join(map(str, ids))})")
# Group in application code: 2 queries total instead of 1001
```

**Explanation:**
- N+1 pattern: 1 query + N individual queries = terrible performance (1001 round-trips)
- JOIN approach: 1 query total (most efficient, but may produce many rows for 1:many)
- Batch IN approach: 2 queries total (clean separation, easier to cache)
- Rule: never execute queries in a loop. Always batch or join.

</details>
</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Full Diagnosis — Star Schema Query (45 min → 2 min)

**Scenario:** 5-table join on a star schema. `fact_events` (2B rows), `dim_user` (50M), `dim_product` (1M), `dim_geo` (10K), `dim_time` (365). Query takes 45 minutes. Fully diagnose and optimize.

<details>
<summary>✅ Solution</summary>

**Diagnosis checklist:**
1. ❌ `dim_user` JOIN is unnecessary (user_id exists in fact — no columns from dim_user needed in SELECT)
2. ❌ No partition pruning on fact (date filter via dim_time doesn't push down automatically)
3. ❌ All dimensions shuffled (should be broadcast — they're tiny)
4. ❌ COUNT(DISTINCT user_id) on 2B rows is expensive

**Fix:**
```sql
SELECT /*+ BROADCAST(dt), BROADCAST(dg), BROADCAST(dp) */
    dt.month_name, dg.country, dp.category,
    COUNT(DISTINCT f.user_id) AS unique_users,
    SUM(f.revenue) AS total
FROM fact_events f
JOIN dim_time dt ON f.event_date = dt.date_key
JOIN (SELECT geo_id, country FROM dim_geo WHERE country IN ('US','UK','DE')) dg 
    ON f.geo_id = dg.geo_id
JOIN dim_product dp ON f.product_id = dp.product_id
WHERE f.event_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY dt.month_name, dg.country, dp.category;
```

**Changes:** Removed dim_user (saves 50M-row join), pushed date filter to fact (partition prune), pre-filtered dim_geo, broadcast all small dims. Result: 45 min → 2 min.

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Handle Data Skew in Distributed Join

**Scenario:** Joining `orders` (2B rows) with `merchants` on `merchant_id`. One merchant has 500M orders (25% of all data). The join task for that merchant takes 40 of 45 total minutes. Fix it.

<details>
<summary>✅ Solution</summary>

```sql
-- Approach 1: Broadcast the dimension (if small enough)
SELECT /*+ BROADCAST(merchants) */ o.*, m.name
FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id;
-- Eliminates skew entirely (no shuffle of orders)

-- Approach 2: Salt the hot key (if dimension is too large to broadcast)
-- Large table: add random suffix to hot key
SELECT *, 
    CASE WHEN merchant_id = 'AMAZON' 
         THEN merchant_id || '_' || FLOOR(RANDOM() * 20)::TEXT
         ELSE merchant_id END AS salted_key
FROM orders;

-- Small table: replicate hot key row 20 times
SELECT *, merchant_id || '_' || s::TEXT AS salted_key
FROM merchants CROSS JOIN generate_series(0, 19) s
WHERE merchant_id = 'AMAZON'
UNION ALL
SELECT *, merchant_id AS salted_key FROM merchants WHERE merchant_id != 'AMAZON';

-- Join on salted_key → hot key spread across 20 partitions
```

**Explanation:**
- Skew = one partition 100x larger than others = one executor does 90% of work
- Broadcast: best if dimension fits in memory (eliminates shuffle entirely)
- Salting: splits hot key across N sub-partitions, replicates matching dim rows
- In Spark 3.0+: AQE handles this automatically (`spark.sql.adaptive.skewJoin.enabled=true`)

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Index Strategy for Multiple Access Patterns

**Scenario:** `orders` table is queried 5 different ways. Design an index strategy that covers all patterns without over-indexing:

1. `WHERE customer_id = X` (point lookup)
2. `WHERE order_date BETWEEN X AND Y` (range scan)
3. `WHERE customer_id = X ORDER BY order_date DESC LIMIT 10`
4. `WHERE status = 'pending' AND created_at < NOW() - '1 hour'`
5. `WHERE product_id = X AND order_date = Y`

<details>
<summary>✅ Solution</summary>

```sql
-- Index 1: Covers patterns 1 AND 3 (composite)
CREATE INDEX idx_cust_date ON orders(customer_id, order_date DESC);

-- Index 2: Covers pattern 2 (date range)
CREATE INDEX idx_order_date ON orders(order_date);

-- Index 3: Covers pattern 4 (partial index — only pending orders)
CREATE INDEX idx_pending_stale ON orders(created_at)
WHERE status = 'pending';
-- Tiny index! Only indexes the ~1% of rows that are pending.

-- Index 4: Covers pattern 5 (composite)
CREATE INDEX idx_product_date ON orders(product_id, order_date);

-- DO NOT create: separate index on customer_id (redundant with idx_cust_date)
-- DO NOT create: index on status alone (low cardinality, rarely selective)
```

**Explanation:**
- Composite indexes satisfy multiple patterns (customer_id, order_date covers both pattern 1 and 3)
- Partial indexes are gold for patterns filtering to a small subset (only 1% of rows are 'pending')
- Trade-off: each index slows writes by ~5-10% → only create indexes that serve frequent queries
- Review monthly: drop unused indexes using `pg_stat_user_indexes` (PostgreSQL)

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Override the Optimizer

**Scenario:** The optimizer consistently chooses a nested loop join for a query that would be 10x faster with a hash join. Statistics are fresh. How do you force a better plan?

<details>
<summary>✅ Solution</summary>

```sql
-- PostgreSQL: disable nested loop for this session
SET enable_nestloop = off;
SELECT ... -- Now uses hash join
SET enable_nestloop = on;  -- Reset

-- SQL Server: query hint
SELECT ... FROM orders o
INNER HASH JOIN customers c ON o.customer_id = c.id
OPTION (HASH JOIN);

-- SQL Server: plan guide (permanent, doesn't modify query text)
EXEC sp_create_plan_guide @name = 'force_hash_join',
    @stmt = 'SELECT ...', @type = 'SQL',
    @hints = 'OPTION (HASH JOIN)';

-- Spark: broadcast hint
SELECT /*+ BROADCAST(small_table) */ ...

-- Snowflake: no direct plan hints, but:
-- Use clustering keys, search optimization, or rewrite the query
```

**Explanation:**
- Optimizer is wrong when: statistics are misleading (correlated columns), cardinality estimation fails, or parameterized queries use cached plans from atypical values
- Hints are a last resort — always try fixing stats, rewriting the query, or adding indexes first
- In production: prefer plan guides (SQL Server) or session-level settings over permanent disabling

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Materialized View Refresh for Real-Time Dashboard

**Scenario:** Your executive dashboard queries a 2B-row fact table every 30 seconds (5-second query each time). Design a materialized view strategy that provides near-real-time data (<5 min stale) while reducing compute by 95%.

<details>
<summary>✅ Solution</summary>

```sql
-- Create MV pre-computing the exact dashboard aggregation
CREATE MATERIALIZED VIEW mv_executive_dashboard AS
SELECT 
    DATE_TRUNC('hour', event_time) AS hour,
    region, product_category,
    COUNT(*) AS events,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(revenue) AS total_revenue
FROM fact_events
WHERE event_time >= CURRENT_DATE - 7  -- Only last 7 days (bounded)
GROUP BY 1, 2, 3;

-- Refresh strategy: incremental every 5 minutes
-- Option A: Snowflake auto-refresh MV (automatic)
-- Option B: Scheduled task (every 5 min)
CREATE TASK refresh_dashboard_mv
    WAREHOUSE = refresh_wh
    SCHEDULE = '5 MINUTE'
AS
    REFRESH MATERIALIZED VIEW mv_executive_dashboard;

-- Dashboard queries the MV (not the fact table)
SELECT * FROM mv_executive_dashboard WHERE hour >= NOW() - INTERVAL '24 hours';
-- Result: <100ms (reads a few thousand rows from MV vs 2B from fact)
```

**Explanation:**
- Without MV: 2B rows scanned every 30 seconds = massive compute waste
- With MV: pre-computed result (~10K rows) queried instantly
- 5-minute refresh frequency balances freshness (< SLA) with compute cost
- Separate small warehouse for refresh (doesn't impact user queries)
- Bounded to 7 days: keeps MV small and fast to refresh

</details>
</article>
