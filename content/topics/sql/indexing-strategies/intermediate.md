---
title: "SQL Indexing Strategies - Intermediate"
topic: sql
subtopic: indexing-strategies
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, indexing, composite-index, partial-index, covering-index, explain-plan, postgresql, mysql]
---

# SQL Indexing Strategies — Intermediate Concepts

## Composite Index Column Ordering

The order of columns in a composite index profoundly affects which queries can use it.

### The Leftmost Prefix Rule in Practice

```sql
-- Index: (customer_id, status, order_date)
CREATE INDEX idx_orders_comp ON orders(customer_id, status, order_date);

-- ✅ Uses index fully (all three columns)
SELECT * FROM orders 
WHERE customer_id = 1001 AND status = 'shipped' AND order_date = '2024-01-15';

-- ✅ Uses index (first two columns)
SELECT * FROM orders WHERE customer_id = 1001 AND status = 'shipped';

-- ✅ Uses index (first column only)
SELECT * FROM orders WHERE customer_id = 1001;

-- ❌ Cannot use index (skips customer_id)
SELECT * FROM orders WHERE status = 'shipped' AND order_date = '2024-01-15';

-- ⚠️ Partial use: uses index for customer_id filter, then scans
SELECT * FROM orders WHERE customer_id = 1001 AND order_date = '2024-01-15';
-- The optimizer uses the index for customer_id, but cannot skip to order_date
-- because status is in between and not filtered
```

### Optimal Column Order Strategy

**Rule:** Put the most selective column first, UNLESS range conditions are involved:

```sql
-- Scenario: frequent queries filter on (status, customer_id) or (customer_id) alone
-- status has 4 values (low selectivity); customer_id has 1M values (high selectivity)

-- ✅ Better order: high selectivity first
CREATE INDEX idx_better ON orders(customer_id, status);
-- Supports: WHERE customer_id = X (uses index)
-- Supports: WHERE customer_id = X AND status = Y (uses index fully)

-- ❌ Worse order: low selectivity first
CREATE INDEX idx_worse ON orders(status, customer_id);
-- Only supports: WHERE status = Y AND customer_id = X
-- NOT useful for: WHERE customer_id = X alone
```

**Exception — range conditions:**

```sql
-- If you have a range on order_date AND equality on customer_id:
-- Put the equality column BEFORE the range column

-- ✅ Correct: equality first, then range
CREATE INDEX idx_range_correct ON orders(customer_id, order_date);
SELECT * FROM orders WHERE customer_id = 1001 AND order_date >= '2024-01-01';
-- Uses index for both conditions

-- ❌ Wrong for this query: range before equality
CREATE INDEX idx_range_wrong ON orders(order_date, customer_id);
SELECT * FROM orders WHERE customer_id = 1001 AND order_date >= '2024-01-01';
-- Can only use the range condition; customer_id filter is applied after
```

---

## Index-Only Scans and Covering Indexes

An **Index-Only Scan** means the query can be answered entirely from the index without touching the main table. This is the fastest possible access pattern.

```sql
-- Query: find total orders and revenue per customer for 2024
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id;

-- Without covering index:
-- Index Scan on order_date → for each row, fetch the full table row to get customer_id + amount
-- This causes many random I/O reads (one per order row)

-- With covering index:
CREATE INDEX idx_covering ON orders(order_date, customer_id, amount);
-- Now: Index Only Scan — all needed columns are in the index
-- No table access at all!

-- PostgreSQL: check Index Only Scan in EXPLAIN
EXPLAIN ANALYZE
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id;
-- Look for: "Index Only Scan using idx_covering"
```

**When to use covering indexes:**
- The query is run very frequently (high volume)
- The query touches many rows but only needs a few columns
- Table rows are wide (lots of columns) — avoiding table access saves significant I/O

**Trade-off:** Covering indexes are larger (more columns stored twice), so they have higher write overhead and memory cost.

---

## Functional (Expression) Indexes

Index an expression or function result, enabling queries that call functions on columns:

```sql
-- Problem: this query can't use a regular index on email:
SELECT * FROM users WHERE LOWER(email) = 'john@example.com';

-- Solution: index the expression
CREATE INDEX idx_users_email_lower ON users(LOWER(email));

-- Now this uses the expression index:
SELECT * FROM users WHERE LOWER(email) = 'john@example.com';
-- ✅ Uses idx_users_email_lower

-- Other examples:
CREATE INDEX idx_orders_year ON orders(EXTRACT(YEAR FROM order_date));
SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2024;

-- Index on computed string length:
CREATE INDEX idx_long_descriptions ON products(LENGTH(description))
WHERE LENGTH(description) > 1000;
```

**PostgreSQL / Snowflake tip:** Expression indexes add index maintenance overhead — the expression is re-evaluated on every INSERT/UPDATE. Only use for frequently-queried expressions.

---

## Partial Indexes

A partial index only covers rows matching a WHERE condition. This makes indexes smaller, faster to maintain, and sometimes usable when a full-column index wouldn't be.

```sql
-- Only index active users (95% of queries filter on is_active = TRUE)
CREATE INDEX idx_users_active ON users(email)
WHERE is_active = TRUE;

-- This query uses the partial index:
SELECT * FROM users WHERE is_active = TRUE AND email = 'john@example.com';

-- This query does NOT use it (no filter on is_active):
SELECT * FROM users WHERE email = 'john@example.com';

-- Partial index for a high-value subset:
CREATE INDEX idx_orders_large ON orders(customer_id, order_date)
WHERE amount > 1000;

-- Effectively queries the "VIP orders" subset quickly:
SELECT * FROM orders WHERE amount > 1000 AND customer_id = 1001;
```

**Why partial indexes win for status columns:**

```sql
-- Instead of indexing status = 'pending' in a full index (low selectivity),
-- use a partial index on just the pending records:
CREATE INDEX idx_pending_orders ON orders(created_at)
WHERE status = 'pending';
-- Pending orders are 2% of total — this index is tiny and very fast to scan
```

---

## Index Impact on Writes

Every index has a write overhead. Understanding this helps in capacity planning:

```sql
-- A table with 5 indexes: each INSERT/UPDATE/DELETE must update 5 index structures
-- For a table receiving 100,000 inserts/sec, this is significant

-- Measure write amplification:
-- Base table write: 1 row
-- + 1 B-tree index update per index
-- = 1 table write + 5 index writes = 6 total writes per insert

-- For bulk loads: drop indexes first, reload, rebuild
-- PostgreSQL: this is dramatically faster for large loads
DROP INDEX idx_orders_customer_id;
DROP INDEX idx_orders_date;

-- Load data
COPY orders FROM '/data/orders_2024.csv';

-- Rebuild indexes in parallel (PostgreSQL 11+):
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);
CREATE INDEX CONCURRENTLY idx_orders_date ON orders(order_date);
```

**Index count guidelines:**

| Table Type | Typical Index Count | Reasoning |
|-----------|-------------------|-----------|
| OLTP write-heavy | 2–4 | Minimize write overhead |
| OLTP read-heavy | 4–8 | Support common query patterns |
| Analytics/warehouse | 0–3 | Columnar storage handles most reads; fewer row-level indexes needed |
| Dimension tables | 1–3 | Small tables; fewer indexes needed |
| Fact tables | 1–5 | Large tables; selective indexes on join/filter columns |

---

## Detecting Unused and Redundant Indexes

Unused indexes waste space and slow down writes:

```sql
-- PostgreSQL: find unused indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan AS times_used,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE idx_scan = 0  -- Never used
  AND indexrelname NOT LIKE '%pkey%'  -- Exclude primary keys
ORDER BY pg_relation_size(indexrelid) DESC;

-- PostgreSQL: find redundant indexes (subset of another index)
-- Example: index on (customer_id) is redundant if (customer_id, order_date) exists
-- because the composite index can satisfy all queries the single-column index can

-- MySQL: check index usage
SELECT * FROM sys.schema_unused_indexes;
SELECT * FROM sys.schema_redundant_indexes;
```

**Redundant index example:**

```sql
-- These two indexes are redundant:
CREATE INDEX idx_a ON orders(customer_id);
CREATE INDEX idx_b ON orders(customer_id, order_date);  -- Subsumes idx_a!

-- idx_a can be dropped — idx_b handles all queries idx_a would handle
DROP INDEX idx_a;
```

---

## Index Strategies by Query Pattern

| Query Pattern | Recommended Index |
|--------------|------------------|
| `WHERE id = X` | Single index on `id` |
| `WHERE a = X AND b = Y` | Composite: `(a, b)` |
| `WHERE a = X ORDER BY b` | Composite: `(a, b)` |
| `WHERE a BETWEEN X AND Y` | Single on `a` |
| `WHERE a = X AND b BETWEEN Y AND Z` | Composite: `(a, b)` — equality before range |
| `SELECT a, b WHERE c = X` | Covering: `(c, a, b)` |
| `WHERE status = 'pending'` (2% of rows) | Partial: on key column `WHERE status = 'pending'` |
| `WHERE LOWER(email) = X` | Expression: `(LOWER(email))` |
| `ORDER BY created_at DESC LIMIT 10` | Index on `created_at` (optimizer uses it for sort avoidance) |

---

## Interview Tips

> **Tip 1:** "If a query is slow and has a WHERE clause, what's your diagnostic process?" — "First EXPLAIN the query to see if it's doing a sequential scan. If yes, I check: (1) does an index exist on the filter column? (2) Is the column wrapped in a function? (3) Is the selectivity too low? (4) Is the query matching >15% of rows? Then I create or fix the appropriate index, run EXPLAIN again to confirm the plan changed, and measure the actual runtime improvement."

> **Tip 2:** "How do you choose between a single-column and composite index?" — "If multiple queries filter on the same combination of columns, a composite index is more efficient than multiple single-column indexes. I put the highest-selectivity column first, or the column used in equality conditions before columns used in ranges. I also check if adding a couple of SELECT columns to the index would enable an index-only scan for a high-frequency query."

> **Tip 3:** "How do you add an index to a production table without downtime?" — "In PostgreSQL, I use `CREATE INDEX CONCURRENTLY` — it builds the index without holding a table lock, so reads and writes continue normally. The trade-off is it takes about 2-3× longer than a regular index build. MySQL 5.6+ and SQL Server also support online index builds. I always monitor for increased I/O and replication lag during the build."
