---
title: "SQL Execution Plans - Fundamentals"
topic: sql
subtopic: execution-plans
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, execution-plans, explain, query-optimizer, seq-scan, index-scan, hash-join]
---

# SQL Execution Plans — Fundamentals


## 🎯 Analogy

Think of an execution plan like an X-ray of how the database actually runs your query — it reveals whether it's doing an expensive full table scan when it should be using an index, or a nested-loop join when a hash join would be faster.

---
## What Is a Query Execution Plan?

A **query execution plan** (or query plan) describes the sequence of steps the database engine takes to execute a SQL query. The **query optimizer** — a component of the database — analyzes the query, considers available indexes, table statistics, and estimated row counts, then chooses the most efficient strategy.

> **Analogy:** A query plan is like a GPS route — for any trip, there are many possible routes (take the highway vs. surface streets, stop at this gas station vs. that one). The GPS (optimizer) picks the route it estimates will be fastest based on current conditions. Like GPS, it can sometimes be wrong if its information (statistics) is outdated.

---

## How to View a Query Plan

### PostgreSQL: EXPLAIN

```sql
-- Basic EXPLAIN: shows the plan without running the query
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;

-- EXPLAIN ANALYZE: runs the query AND shows actual timing + row counts
EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 1001;

-- EXPLAIN with all details:
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM orders WHERE customer_id = 1001;
```

**Sample EXPLAIN output:**

```
Index Scan using idx_orders_customer_id on orders  (cost=0.43..8.45 rows=3 width=87)
  (actual time=0.045..0.048 rows=3 loops=1)
  Index Cond: (customer_id = 1001)
Buffers: shared hit=4
Planning Time: 0.8 ms
Execution Time: 0.1 ms
```

### MySQL: EXPLAIN and EXPLAIN ANALYZE

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;
-- Shows: id, select_type, table, type, possible_keys, key, key_len, ref, rows, Extra

EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 1001;  -- MySQL 8.0+
```

### SQL Server: Execution Plan

```sql
-- Text format:
SET SHOWPLAN_TEXT ON;
SELECT * FROM orders WHERE customer_id = 1001;
SET SHOWPLAN_TEXT OFF;

-- Or use SQL Server Management Studio: Ctrl+M for graphical execution plan
```

---

## Reading a PostgreSQL EXPLAIN Output

The plan is a tree — read it from **bottom to top** (innermost to outermost):

```sql
EXPLAIN ANALYZE
SELECT c.name, SUM(o.amount) AS total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.country = 'US'
GROUP BY c.name
ORDER BY total DESC;
```

```
Sort  (cost=452.31..454.81 rows=1000 width=40) (actual time=12.3..12.4 rows=450 loops=1)
  Sort Key: (sum(o.amount)) DESC
  -> HashAggregate  (cost=395.00..405.00 rows=1000 width=40) (actual time=11.8..12.0 rows=450 loops=1)
       Group Key: c.name
       -> Hash Join  (cost=250.00..345.00 rows=10000 width=36) (actual time=3.2..10.1 rows=5000 loops=1)
            Hash Cond: (o.customer_id = c.customer_id)
            -> Seq Scan on orders o  (cost=0.00..150.00 rows=10000 width=20) (actual time=0.1..3.0 rows=10000 loops=1)
            -> Hash  (cost=200.00..200.00 rows=4000 width=24) (actual time=2.5..2.5 rows=4000 loops=1)
                 Buckets: 4096  Batches: 1  Memory Usage: 256kB
                 -> Seq Scan on customers c  (cost=0.00..200.00 rows=4000 width=24) (actual time=0.1..1.5 rows=4000 loops=1)
                      Filter: (country = 'US')
                      Rows Removed by Filter: 6000
Planning Time: 1.2 ms
Execution Time: 12.5 ms
```

**Reading order (bottom to top):**
1. `Seq Scan on customers` — scan customers, filter country='US', keep 4,000 rows
2. `Hash` — build a hash table from the 4,000 customers
3. `Seq Scan on orders` — scan all 10,000 orders
4. `Hash Join` — match orders to customers hash table → 5,000 matching rows
5. `HashAggregate` — group by customer name, compute SUM
6. `Sort` — order by total DESC

---

## Key Plan Nodes

### Scan Types

| Node | Description | When Chosen |
|------|-------------|-------------|
| **Seq Scan** | Read every row in the table | No usable index, or fetching many rows |
| **Index Scan** | Read index, then fetch matching heap rows | Index exists, selective filter |
| **Index Only Scan** | Read only the index (no heap access) | All needed columns are in the index |
| **Bitmap Heap Scan** | Build bitmap of matching rows, then fetch | Moderate number of matches |

```sql
-- Force different scan types to understand them (dev only):
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;
-- With index: "Index Scan using idx_orders_customer_id"

SET enable_indexscan = off;
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;
-- Without index: "Bitmap Heap Scan" or "Seq Scan"
RESET enable_indexscan;
```

### Join Types

| Node | Description | Best For |
|------|-------------|---------|
| **Nested Loop** | For each outer row, scan inner table | Small outer table + indexed inner table |
| **Hash Join** | Build hash table from smaller table, probe with larger | Medium tables, equality joins |
| **Merge Join** | Both inputs pre-sorted, merge together | Pre-sorted inputs, large tables |

```
-- Nested Loop example:
Nested Loop (actual rows=100)
  -> Seq Scan on customers (rows=10)  ← Outer: small table
  -> Index Scan on orders (rows=10 per customer)  ← Inner: indexed lookup
-- Good: 10 outer rows × fast index lookup = 100 rows total

-- Hash Join example:
Hash Join (actual rows=50000)
  -> Seq Scan on orders (rows=100000)  ← Build side (hashed in memory)
  -> Seq Scan on customers (rows=10000)  ← Probe side
-- Good for large equality joins
```

---

## Understanding Cost Estimates

The cost numbers in EXPLAIN represent the optimizer's estimate of work:

```
Seq Scan on orders (cost=0.00..150.00 rows=10000 width=87)
                         ^^^^  ^^^^^^  ^^^^^^^^^^^^  ^^^
                         startup cost   total cost   estimated rows    avg row width (bytes)
```

- **Startup cost:** Cost before the first row is returned (e.g., building a hash table)
- **Total cost:** Cost to return ALL rows
- **rows:** Estimated number of rows this node produces
- **width:** Estimated average bytes per row

**Cost is relative — not seconds.** The unit is "cost units" (approximately one sequential page read = 1.0 cost unit). The optimizer picks the plan with the lowest total cost.

```sql
-- Check optimizer cost parameters:
SHOW seq_page_cost;       -- Cost of reading a page sequentially (default: 1.0)
SHOW random_page_cost;    -- Cost of random disk access (default: 4.0)
SHOW cpu_tuple_cost;      -- Cost of processing one row (default: 0.01)

-- Tune random_page_cost lower if data is on SSDs:
SET random_page_cost = 1.1;  -- SSD-like: random access almost as fast as sequential
```

---

## Red Flags in Execution Plans

Learn to spot these warning signs:

| Red Flag | What It Means | Fix |
|----------|--------------|-----|
| `Seq Scan` on large table | No index used — full table scan | Add an appropriate index |
| `Rows Removed by Filter: 9999999` | Index not used; filter applied after scan | Index the filter column |
| Estimated rows = 1, actual rows = 50000 | Stale statistics | Run ANALYZE |
| `Sort` without index | In-memory sort (or disk sort) | Add index matching ORDER BY |
| `Hash Join Batches: 10` | Hash table spilled to disk | Increase `work_mem` |
| `Nested Loop` on large tables | Should be Hash Join | Optimizer misestimated — check statistics |

---

## EXPLAIN ANALYZE vs EXPLAIN

```sql
-- EXPLAIN alone: shows the PLANNED execution path (estimated costs, no actual run)
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;
-- ✅ Fast — doesn't actually run the query
-- ✅ Safe for expensive queries — won't actually delete/update anything
-- ❌ Shows only estimates, not actual row counts or timing

-- EXPLAIN ANALYZE: shows the ACTUAL execution path (runs the query!)
EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 1001;
-- ✅ Shows actual rows, actual timing, buffer hit/miss counts
-- ❌ RUNS THE QUERY — avoid on expensive queries in production
-- ❌ For INSERT/UPDATE/DELETE: the changes ARE applied (wrap in BEGIN/ROLLBACK)

-- Safe pattern for EXPLAIN ANALYZE on DML:
BEGIN;
EXPLAIN ANALYZE DELETE FROM orders WHERE order_date < '2020-01-01';
ROLLBACK;  -- Undo the actual deletion
```

---


## ▶️ Try It Yourself

```sql
-- Postgres: EXPLAIN ANALYZE runs the query and shows actual times
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.name, COUNT(o.id), SUM(o.amount)
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.name;

-- Key things to look for:
-- "Seq Scan" on large table → needs an index
-- "Hash Join" vs "Nested Loop" → Hash is usually better for large tables
-- "Rows Removed by Filter: 99000" → the filter should be pushed earlier
-- Actual rows >> Estimated rows → stale statistics (run ANALYZE)

-- Snowflake: use Query Profile in the UI or:
-- SELECT * FROM TABLE(EXPLAIN_JSON($$your_query$$));
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between EXPLAIN and EXPLAIN ANALYZE?" — "EXPLAIN shows the query plan the optimizer chose, with estimated costs and row counts — without actually running the query. EXPLAIN ANALYZE actually executes the query and shows both estimated and actual metrics: actual row counts, actual execution time, and buffer hit/miss counts. The comparison between estimated and actual rows is the most valuable diagnostic tool — a large discrepancy indicates stale statistics."

> **Tip 2:** "In what order do you read a PostgreSQL EXPLAIN output?" — "Bottom to top, most indented to least indented. The deepest/most-indented nodes execute first — these are the base table scans. Their output feeds into higher nodes (joins, aggregates, sorts) which feed further up. The root node (top of the plan) is the final operation — usually the node that returns results to the client."

> **Tip 3:** "What does it mean when EXPLAIN shows Seq Scan on a large table?" — "It means the optimizer chose to read every row in the table rather than using an index. This is expected for small tables or when fetching a large percentage of rows (>15-20%). It's a problem when you're filtering on a column that should have an index but doesn't, or when a function wrapped around the column defeats the index. Check: does an index exist on the filter column? Run ANALYZE to update statistics. Check if the column is wrapped in a function in the WHERE clause."
