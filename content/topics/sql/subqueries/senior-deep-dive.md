---
title: "SQL Subqueries - Senior Deep Dive"
topic: sql
subtopic: subqueries
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, subqueries, query-optimizer, semi-join, anti-join, lateral, subquery-unnesting, execution-plan]
---

# SQL Subqueries — Senior-Level Deep Dive

## How the Optimizer Transforms Subqueries

Modern query optimizers rarely execute subqueries as literally written. They transform them into equivalent join plans through a process called **subquery unnesting** (or decorrelation).

### IN → Hash Semi-Join

```sql
-- Written as:
SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM vip_customers);

-- Optimizer rewrites as (internally):
SELECT DISTINCT o.*
FROM orders o
JOIN vip_customers v ON o.customer_id = v.customer_id;

-- EXPLAIN shows: Hash Semi Join (not a subquery execution)
-- PostgreSQL EXPLAIN output:
-- Hash Semi Join (cost=15.00..1205.00)
--   Hash Cond: (o.customer_id = v.customer_id)
--   -> Seq Scan on orders
--   -> Hash
--       -> Seq Scan on vip_customers
```

### NOT IN → Hash Anti-Join (when NULL-safe)

```sql
-- Written as:
SELECT * FROM orders WHERE customer_id NOT IN (SELECT customer_id FROM vip_customers);

-- Optimizer may rewrite as:
-- Hash Anti Join
-- BUT only if the optimizer can prove no NULLs exist in the subquery result
-- If NULLs are possible → the optimizer must use a more conservative plan
-- → This is why NOT IN can be slower than NOT EXISTS

-- NOT EXISTS → always a clean anti-join:
SELECT * FROM orders o WHERE NOT EXISTS (SELECT 1 FROM vip_customers v WHERE v.customer_id = o.customer_id);
-- Optimizer: Hash Anti Join (same as NOT IN but always NULL-safe)
```

### Correlated Subquery Decorrelation

```sql
-- Written as a correlated subquery (naive — N+1):
SELECT c.customer_id, (SELECT SUM(amount) FROM orders WHERE customer_id = c.customer_id) AS total
FROM customers c;

-- Optimizer decorrelates to:
-- GroupAggregate + Hash Left Join (single pass):
-- Hash Left Join (customer_id = customer_id)
--   -> Seq Scan on customers
--   -> Hash
--       -> GroupAggregate (GROUP BY customer_id)
--           -> Seq Scan on orders

-- The optimizer runs the aggregation ONCE and joins — not N times
-- This is called "lateral decorrelation" or "subquery pullup"
-- Not all optimizers do this for all correlated patterns — PostgreSQL does it well
```

---

## Subquery Optimization Barriers

Some subquery patterns prevent the optimizer from rewriting efficiently:

### Barrier 1: Volatile Functions

```sql
-- RANDOM() or NOW() in a subquery forces re-execution per row
SELECT * FROM events
WHERE event_id IN (
    SELECT event_id FROM events WHERE RANDOM() < 0.01  -- 1% sample
);
-- The optimizer cannot pre-compute this subquery because RANDOM() changes each call
-- → Falls back to executing per row (not decorrelated)
```

### Barrier 2: DISTINCT or GROUP BY in Correlated Subquery

```sql
-- This correlated subquery with DISTINCT may not decorrelate in some engines:
SELECT c.customer_id
FROM customers c
WHERE (SELECT COUNT(DISTINCT product_id) FROM orders WHERE customer_id = c.customer_id) > 5;

-- Better: explicit aggregation in CTE
WITH product_counts AS (
    SELECT customer_id, COUNT(DISTINCT product_id) AS distinct_products
    FROM orders GROUP BY customer_id
)
SELECT c.customer_id FROM customers c
JOIN product_counts pc ON c.customer_id = pc.customer_id
WHERE pc.distinct_products > 5;
```

### Barrier 3: LIMIT in Correlated Subquery

```sql
-- LIMIT inside a correlated subquery prevents decorrelation in most engines
SELECT c.customer_id,
    (SELECT order_id FROM orders WHERE customer_id = c.customer_id ORDER BY amount DESC LIMIT 1) AS top_order
FROM customers c;
-- Cannot be decorrelated because LIMIT changes which row to pick per customer
-- → Forced correlated execution (N subquery calls)

-- LATERAL is the correct approach here (explicitly per-row, but with proper join plan):
SELECT c.customer_id, t.order_id
FROM customers c
LEFT JOIN LATERAL (
    SELECT order_id FROM orders WHERE customer_id = c.customer_id ORDER BY amount DESC LIMIT 1
) t ON TRUE;
-- Optimizer uses Nested Loop with index on (customer_id, amount DESC)
-- Still per-row but uses index efficiently — much faster
```

---

## Advanced Subquery Patterns

### Finding Islands and Gaps

```sql
-- Find consecutive date ranges (islands) from a set of dates
WITH dated AS (
    SELECT 
        event_date,
        event_date - (ROW_NUMBER() OVER (ORDER BY event_date) * INTERVAL '1 day') AS grp
    FROM (SELECT DISTINCT event_date FROM events WHERE user_id = 42) d
),
islands AS (
    SELECT MIN(event_date) AS island_start, MAX(event_date) AS island_end, COUNT(*) AS days
    FROM dated GROUP BY grp
)
SELECT island_start, island_end, days
FROM islands ORDER BY island_start;
```

### Recursive Subquery Simulation (Pre-Recursive CTE)

```sql
-- Before recursive CTEs were widely available, hierarchies were traversed using
-- self-joining subqueries up to a fixed depth. This is an anti-pattern today
-- but shows up in legacy code:
SELECT l1.id AS level_1, l2.id AS level_2, l3.id AS level_3
FROM tree l1
LEFT JOIN tree l2 ON l2.parent_id = l1.id
LEFT JOIN tree l3 ON l3.parent_id = l2.id
WHERE l1.parent_id IS NULL;
-- This works for up to 3 levels — ugly and limited
-- Modern alternative: WITH RECURSIVE (always prefer this)
```

### Subquery in CASE WHEN

```sql
-- Dynamic segmentation based on subquery results
SELECT 
    o.order_id,
    o.amount,
    CASE 
        WHEN o.customer_id IN (SELECT customer_id FROM vip_customers) THEN 'VIP'
        WHEN o.customer_id IN (SELECT customer_id FROM at_risk_customers) THEN 'AT RISK'
        ELSE 'STANDARD'
    END AS customer_segment
FROM orders o;

-- Better: avoid multiple subqueries in CASE; use LEFT JOINs instead:
SELECT 
    o.order_id,
    o.amount,
    CASE 
        WHEN vip.customer_id IS NOT NULL THEN 'VIP'
        WHEN risk.customer_id IS NOT NULL THEN 'AT RISK'
        ELSE 'STANDARD'
    END AS customer_segment
FROM orders o
LEFT JOIN vip_customers vip ON o.customer_id = vip.customer_id
LEFT JOIN at_risk_customers risk ON o.customer_id = risk.customer_id;
-- Executes in a single pass over the data
```

---

## Subqueries in Analytical SQL

### Quantile Calculations Using Subqueries

```sql
-- Find the 90th percentile order value (without PERCENTILE_CONT in older databases)
SELECT amount FROM (
    SELECT amount, PERCENT_RANK() OVER (ORDER BY amount) AS pct_rank
    FROM orders
) ranked
WHERE pct_rank >= 0.90
ORDER BY pct_rank
LIMIT 1;

-- Modern approach (PostgreSQL / Redshift / BigQuery):
SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount) AS p90_amount
FROM orders;
```

### Market Basket Analysis Using Subqueries

```sql
-- Find product pairs frequently bought together
SELECT 
    a.product_id AS product_a,
    b.product_id AS product_b,
    COUNT(*) AS co_purchase_count
FROM order_items a
JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
WHERE a.order_id IN (
    SELECT order_id FROM orders WHERE order_date >= '2024-01-01'
)
GROUP BY a.product_id, b.product_id
HAVING COUNT(*) > 10
ORDER BY co_purchase_count DESC;
```

---

## Subqueries and Statistics

The optimizer uses table statistics to estimate subquery result set sizes. Bad estimates lead to bad plans:

```sql
-- PostgreSQL: check estimated vs actual rows for a subquery
EXPLAIN ANALYZE
SELECT * FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE country = 'US' AND signup_year = 2024
);

-- If estimated rows = 100 but actual = 50000:
-- → Optimizer might choose a Nested Loop (correct for 100 rows) instead of Hash Join (needed for 50000)
-- → Result: catastrophically slow plan

-- Fix 1: Update statistics
ANALYZE customers;

-- Fix 2: Increase statistics target for skewed columns
ALTER TABLE customers ALTER COLUMN country SET STATISTICS 500;
ANALYZE customers;

-- Fix 3: Rewrite as explicit JOIN (gives optimizer more flexibility)
SELECT o.* FROM orders o
JOIN (SELECT customer_id FROM customers WHERE country = 'US' AND signup_year = 2024) c
    ON o.customer_id = c.customer_id;
-- As a JOIN, the optimizer can see the estimated join cardinality more accurately
```

---

## Platform-Specific Subquery Behavior

### BigQuery

```sql
-- BigQuery: scalar subqueries in SELECT are allowed but can be expensive
-- BigQuery processes each subquery as a separate job stage
-- Use WITH (CTEs) or window functions for better performance:

-- Slow in BigQuery:
SELECT 
    user_id,
    event_count,
    (SELECT AVG(event_count) FROM user_daily_counts) AS avg_count  -- Separate scan
FROM user_daily_counts;

-- Better in BigQuery:
SELECT 
    user_id,
    event_count,
    AVG(event_count) OVER () AS avg_count  -- Window function — single scan
FROM user_daily_counts;
```

### Snowflake

```sql
-- Snowflake: LATERAL joins are supported
-- Snowflake optimizes subqueries well — correlated subqueries are often decorrelated

-- Snowflake-specific: FLATTEN for VARIANT (JSONB-like) subqueries
SELECT u.user_id, f.value::STRING AS tag
FROM users u,
LATERAL FLATTEN(input => u.tags) f  -- LATERAL FLATTEN expands an array column
WHERE f.value::STRING = 'premium';
```

### Redshift

```sql
-- Redshift: correlated subqueries are expensive and not always optimized
-- Redshift distribution style affects subquery join performance:

-- If customers is DISTSTYLE ALL (replicated to all nodes)
-- and orders is DISTSTYLE KEY on customer_id:
-- → The IN subquery JOIN can run locally on each node — no network shuffle
SELECT * FROM orders WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE country = 'US'
);

-- Check query plan in Redshift:
EXPLAIN SELECT * FROM orders WHERE customer_id IN (...);
-- Look for: DS_DIST_NONE (no redistribution needed) vs DS_DIST_INNER (inner table redistributed)
```

---

## Interview Tips

> **Tip 1:** "How does the optimizer handle a correlated subquery?" — "Modern optimizers attempt to 'decorrelate' or 'unnest' correlated subqueries. For simple aggregations like `SELECT SUM(amount) FROM orders WHERE customer_id = c.customer_id`, the optimizer rewrites this as a grouped aggregation joined to the outer table — running the aggregation once, not once per row. However, decorrelation fails for patterns with LIMIT, DISTINCT on aggregate functions, or volatile functions — those are forced to run per row. I read the EXPLAIN plan to confirm decorrelation happened."

> **Tip 2:** "When does NOT IN perform poorly and what's the fix?" — "NOT IN is dangerous with NULLs in the subquery — any NULL in the subquery result causes the entire NOT IN to return no rows (because `x NOT IN {1, 2, NULL}` is UNKNOWN, not TRUE). Beyond correctness, NOT IN loads the full subquery result into memory. NOT EXISTS avoids both issues: it uses an anti-join plan, stops at first match, and handles NULLs correctly. I always use NOT EXISTS for anti-join patterns in production code."

> **Tip 3:** "A query with a subquery in the SELECT clause is taking 30 minutes. What do you do?" — "First I EXPLAIN the query to check whether the subquery is executing per row (correlated) or being decorrelated (single pass). If it's executing per row for a 1M-row table, I rewrite it: if the subquery is an aggregate, I replace it with a GROUP BY + LEFT JOIN; if it's a ranked selection, I use ROW_NUMBER() in a CTE; if it's an existence check, I use LEFT JOIN + IS NULL or NOT EXISTS. Then I verify the new plan with EXPLAIN ANALYZE and compare actual runtimes."
