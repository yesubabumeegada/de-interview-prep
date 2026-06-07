---
title: "SQL Query Optimization - Intermediate"
topic: sql
subtopic: query-optimization
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, optimization, indexes, join-optimization, partitioning, materialized-views]
---

# SQL Query Optimization — Intermediate Concepts

## Index Types and Strategies

### B-Tree Index (Default, Most Common)

Best for: equality (`=`), range (`>`, `<`, `BETWEEN`), and prefix matching (`LIKE 'abc%'`).

```sql
-- Single column index
CREATE INDEX idx_orders_date ON orders(order_date);

-- Composite index (multiple columns — order matters!)
CREATE INDEX idx_orders_cust_date ON orders(customer_id, order_date);
```

**Composite index rules (leftmost prefix principle):**

| Query Filter | Uses `idx_orders_cust_date`? |
|-------------|:---:|
| `WHERE customer_id = 42` | YES (uses first column) |
| `WHERE customer_id = 42 AND order_date = '2024-01-15'` | YES (uses both) |
| `WHERE order_date = '2024-01-15'` | NO (skips first column) |
| `WHERE customer_id = 42 ORDER BY order_date` | YES (filter + sort from index) |

> **Key rule:** A composite index can only be used if the query uses columns from the LEFT side. `(A, B, C)` supports queries on `A`, `A+B`, or `A+B+C` — but NOT `B` alone or `C` alone.

### Covering Index (Include Columns)

An index that contains ALL columns the query needs — the database never reads the actual table.

```sql
-- Query: SELECT name, email FROM users WHERE department = 'Engineering'
-- Covering index: includes the SELECT columns in the index
CREATE INDEX idx_users_dept_covering ON users(department) INCLUDE (name, email);
-- Now the query reads ONLY from the index (no table access needed)
```

### Hash Index

Best for exact equality lookups only. Cannot do range scans.

```sql
-- PostgreSQL: hash index for pure equality
CREATE INDEX idx_sessions_token ON sessions USING hash (session_token);
-- Fast for: WHERE session_token = 'abc123'
-- Useless for: WHERE session_token > 'abc'
```

### Partial Index (Conditional)

Index only a subset of rows — saves space and maintenance cost.

```sql
-- Only index active orders (90% of rows are completed/archived)
CREATE INDEX idx_active_orders ON orders(customer_id, order_date)
WHERE status = 'active';
-- Small index (10% of table), but covers the most common queries
```

---

## Join Optimization

### Ensure Join Columns Are Indexed

```sql
-- SLOW: orders.customer_id has no index → nested loop full scan
SELECT c.name, o.amount
FROM customers c
JOIN orders o ON c.id = o.customer_id;
-- Fix:
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
```

### Filter Before Joining

```sql
-- Instead of joining 100M rows then filtering:
SELECT c.name, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.name;

-- If optimizer doesn't push the filter down, help it:
SELECT c.name, SUM(o.amount)
FROM (SELECT customer_id, amount FROM orders WHERE order_date >= '2024-01-01') o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.name;
```

### Small Table First (Or Let Optimizer Decide)

```sql
-- In distributed SQL (Spark, Snowflake): broadcast the small table
SELECT /*+ BROADCAST(dim_product) */
    f.*, d.product_name
FROM fact_sales f
JOIN dim_product d ON f.product_id = d.product_id;
```

---

## Partitioning for Large Tables

### Table Partitioning (Physical Data Separation)

```sql
-- PostgreSQL: range partition by date
CREATE TABLE orders (
    order_id BIGINT,
    customer_id INT,
    amount DECIMAL,
    order_date DATE
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_2024_q1 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE orders_2024_q2 PARTITION OF orders
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

-- Query automatically targets only relevant partition:
SELECT * FROM orders WHERE order_date = '2024-02-15';
-- Only scans orders_2024_q1 (skips Q2, Q3, Q4 entirely)
```

### When Partitioning Helps

| Scenario | Helps? |
|----------|--------|
| Queries always filter by the partition key (date) | YES — massive speedup |
| Need to drop old data quickly | YES — `DROP PARTITION` instead of DELETE |
| Random access patterns (no dominant filter) | NO — full scan across all partitions |
| Table is small (<10 GB) | NO — overhead exceeds benefit |

---

## Materialized Views and Pre-Computation

For queries run repeatedly (dashboards, reports), pre-compute the result:

```sql
-- Create materialized view (PostgreSQL)
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT 
    order_date,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM orders
GROUP BY order_date, region;

-- Refresh when underlying data changes
REFRESH MATERIALIZED VIEW mv_daily_revenue;

-- Query the MV instead of the base table (instant response)
SELECT * FROM mv_daily_revenue WHERE order_date = '2024-01-15';
```

**MV vs creating an aggregate table manually:**

| Approach | Auto-refresh | SQL complexity | Use when |
|----------|-------------|---------------|----------|
| Materialized View | Platform-dependent | Simple CREATE | Repeated queries on same aggregation |
| Aggregate table + ETL | Manual (scheduled) | Full control | Complex transformations, cross-table |
| Query cache | Automatic (identical queries) | None | Same exact query runs often |

---

## Subquery Optimization

### Correlated Subquery → JOIN Conversion

```sql
-- SLOW: Correlated subquery (runs per row in outer query)
SELECT name, salary,
    (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department) AS dept_avg
FROM employees e1;
-- Subquery executes ONCE PER ROW in employees (N times)

-- FAST: Convert to JOIN (runs once)
SELECT e.name, e.salary, d.dept_avg
FROM employees e
JOIN (SELECT department, AVG(salary) AS dept_avg FROM employees GROUP BY department) d
    ON e.department = d.department;
-- Subquery executes ONCE, then joined
```

### EXISTS vs IN Performance

```sql
-- EXISTS: stops at first match (short-circuits)
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
-- If orders table has an index on customer_id: very fast

-- IN: may need to evaluate entire subquery first
SELECT * FROM customers
WHERE id IN (SELECT customer_id FROM orders);
-- Optimizer may convert to a semi-join (same as EXISTS)
-- But with NULLs in subquery results: different behavior!
```

> **Rule of thumb:** Use EXISTS for "does a match exist?" and IN for small, known value lists. Modern optimizers usually handle both well, but EXISTS is safer with NULLs.

---

## UNION vs UNION ALL

```sql
-- UNION: removes duplicates (requires sort/hash — expensive!)
SELECT customer_id FROM orders_2023
UNION
SELECT customer_id FROM orders_2024;
-- Deduplicates across both sets (sort + compare)

-- UNION ALL: keeps all rows (no dedup — fast!)
SELECT customer_id FROM orders_2023
UNION ALL
SELECT customer_id FROM orders_2024;
-- Just concatenates results (no extra processing)
```

> **Always use UNION ALL unless you specifically need deduplication.** In ETL, you almost always want ALL rows.

---

## Avoiding N+1 Query Pattern

```python
# BAD: N+1 pattern (1 query + N individual lookups)
customers = db.query("SELECT id FROM customers LIMIT 1000")
for customer in customers:
    orders = db.query(f"SELECT * FROM orders WHERE customer_id = {customer.id}")
    # 1000 individual queries!

# GOOD: Single query with JOIN
results = db.query("""
    SELECT c.id, c.name, o.order_id, o.amount
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    LIMIT 1000
""")
# 1 query total!

# GOOD: Batch lookup
customer_ids = [c.id for c in customers]
orders = db.query(f"SELECT * FROM orders WHERE customer_id IN ({','.join(map(str, customer_ids))})")
# 2 queries total (much better than 1001)
```

---

## Statistics and the Optimizer

### When Statistics Go Wrong

```sql
-- Optimizer thinks this returns 100 rows (uses nested loop)
-- Actually returns 5,000,000 rows → terrible performance
SELECT * FROM events WHERE event_type = 'page_view';

-- Why: statistics say event_type has uniform distribution (1/100 of types)
-- Reality: 'page_view' is 50% of all events

-- Fix: refresh statistics
ANALYZE events;  -- PostgreSQL
UPDATE STATISTICS events;  -- SQL Server
ALTER TABLE events REBUILD;  -- Forces Snowflake to recompute metadata
```

### Histogram Statistics

For columns with non-uniform distribution (skewed data), histograms help the optimizer:

```sql
-- PostgreSQL: increase statistics target for skewed columns
ALTER TABLE events ALTER COLUMN event_type SET STATISTICS 1000;
ANALYZE events;

-- SQL Server: create a detailed statistics object
CREATE STATISTICS stat_event_type ON events(event_type) WITH FULLSCAN;
```

---

## Optimization by Database Type

| Technique | Row-Store (PostgreSQL, MySQL) | Columnar (Snowflake, Redshift) | Distributed (Spark) |
|-----------|------------------------------|-------------------------------|---------------------|
| Index on WHERE column | Essential | Not applicable (no traditional indexes) | Not applicable |
| Filter early | Important | Very important (reduces bytes scanned) | Critical (reduces shuffle) |
| Avoid SELECT * | Moderate impact | Huge impact (columnar pruning) | Huge impact |
| Partition on date | Very helpful for large tables | Essential (partition pruning) | Essential |
| Pre-aggregate (MV) | Helpful | Helpful | Helpful (cache/Delta) |
| Join order | Optimizer handles it | Optimizer handles it | Broadcast small tables! |

---

## Interview Tips

> **Tip 1:** "Walk me through optimizing a slow query" — "Step 1: Read the execution plan to identify full scans and expensive operators. Step 2: Check for missing indexes on WHERE/JOIN columns. Step 3: Verify statistics are current. Step 4: Look for functions on indexed columns preventing index use. Step 5: Consider materialized views for repeated aggregation patterns."

> **Tip 2:** "How do you decide which columns to index?" — "I look at: (1) columns in WHERE clauses of frequent queries, (2) join key columns (foreign keys), (3) ORDER BY columns. I create composite indexes following the leftmost-prefix rule, putting equality columns first, then range columns."

> **Tip 3:** "What's the difference between optimization in row-store vs columnar?" — "Row-stores benefit from traditional indexes (B-tree on filter/join columns). Columnar databases benefit from column pruning (select fewer columns), partition pruning (filter on partition key), and clustering (co-locate data for filter columns). Traditional indexes don't exist in Snowflake/Redshift — they use min/max metadata instead."
