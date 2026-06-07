---
title: "SQL Execution Plans - Intermediate"
topic: sql
subtopic: execution-plans
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, execution-plans, explain-analyze, statistics, work-mem, parallel-query, join-types]
---

# SQL Execution Plans — Intermediate Concepts

## Estimated vs Actual Row Counts — The Most Important Signal

The gap between estimated and actual rows is the primary diagnostic for bad query plans:

```sql
EXPLAIN ANALYZE
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.country = 'US' AND o.status = 'shipped'
ORDER BY o.order_date DESC
LIMIT 10;
```

```
Limit  (cost=10000.00..10000.03 rows=10) (actual time=2340.1..2340.1 rows=10 loops=1)
  -> Sort  (cost=10000.00..10250.00 rows=100000) (actual time=2340.0..2340.1 rows=10 loops=1)
       Sort Key: o.order_date DESC
       Sort Method: external merge  Disk: 48000kB   ← PROBLEM: disk sort!
       -> Hash Join  (cost=500.00..5000.00 rows=100000) (actual time=50.2..1200.5 rows=100000 loops=1)
            Hash Cond: (o.customer_id = c.customer_id)
            -> Seq Scan on orders o  (cost=0..3000.00 rows=500000 width=40) (actual time=0.1..400.0 rows=500000)
                 Filter: (status = 'shipped')
                 Rows Removed by Filter: 200000
            -> Hash  (cost=400.00..400.00 rows=4000) (actual time=45.0..45.0 rows=4000)
                 -> Seq Scan on customers c  (cost=0..350.00 rows=4000) (actual time=0.1..40.0 rows=4000)
                      Filter: (country = 'US')
Planning Time: 1.5 ms
Execution Time: 2340.5 ms   ← 2.3 seconds for LIMIT 10!
```

**Diagnosis:**
1. The query returns 100,000 rows matching the join + filter before LIMIT 10 is applied
2. `Sort Method: external merge Disk: 48MB` — the sort spilled to disk (huge performance hit)
3. Root cause: the sort happens BEFORE the LIMIT, requiring all 100K rows to be sorted

**Fix 1: Add an index that supports the ORDER BY to avoid the sort:**
```sql
CREATE INDEX idx_orders_status_date ON orders(status, order_date DESC);
-- Now the planner can: scan orders_shipped in order_date order, join, return first 10 = done
-- No full sort needed!
```

**Fix 2: Increase work_mem to keep sort in memory:**
```sql
SET work_mem = '256MB';  -- Session-level; default is 4MB
-- Sort stays in memory → much faster even without index fix
```

---

## Join Type Selection Logic

The optimizer chooses between three join algorithms based on estimated sizes and available indexes:

### Nested Loop Join

```
Nested Loop
  -> Seq Scan on small_table (rows=50)
  -> Index Scan on large_table (rows=10 per outer row)
```

**Algorithm:** For each row in the outer (smaller) table, perform an index lookup on the inner (larger) table.

```sql
-- Nested Loop is optimal when:
-- 1. Outer table is small (50-100 rows)
-- 2. Inner table has an index on the join column

-- Force it:
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN SELECT * FROM small_customers c JOIN orders o ON c.id = o.customer_id;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- COST: O(outer_rows × inner_lookup_time)
-- For 50 outer × index lookup = 50 fast lookups = excellent
-- For 500,000 outer × index lookup = 500K lookups = terrible
```

### Hash Join

```
Hash Join
  Hash Cond: (o.customer_id = c.customer_id)
  -> Seq Scan on orders (rows=100000)   ← Probe side (larger table)
  -> Hash                                ← Build side (smaller table)
       -> Seq Scan on customers (rows=5000)
```

**Algorithm:** Build an in-memory hash table from the smaller table, then scan the larger table and look up each row in the hash table.

```sql
-- Hash Join is optimal when:
-- 1. No index on join column (or not selective enough)
-- 2. Medium-to-large tables (thousands to millions of rows)
-- 3. Equality join (can't hash for inequality joins)

-- IMPORTANT: "Batches: 1" means hash fits in memory (good)
-- "Batches: 4" means hash spilled to disk (bad — tune work_mem)
-- Check: Hash Batches: N  Memory Usage: XkB

SET work_mem = '128MB';  -- Give more memory for hash tables
```

### Merge Join

```
Merge Join
  Merge Cond: (o.customer_id = c.customer_id)
  -> Index Scan on orders (already sorted by customer_id)
  -> Sort
       -> Seq Scan on customers
```

**Algorithm:** Both inputs are sorted on the join key, then merged like a zipper.

```sql
-- Merge Join is optimal when:
-- 1. Both inputs are already sorted (e.g., using an index sort)
-- 2. Large tables where both sides need to be fully scanned anyway

-- Merge Join advantage: no memory requirement for a hash table
-- Disadvantage: requires sorted inputs (adds Sort nodes if not already sorted)
```

---

## Understanding Buffer Hit Rates

`EXPLAIN (ANALYZE, BUFFERS)` shows how many pages (8KB blocks) were read from disk vs. cache:

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM orders WHERE customer_id = 1001;
```

```
Index Scan using idx_orders_customer_id on orders
  (actual rows=15 loops=1)
Buffers: shared hit=8 read=2
Planning Time: 0.3 ms
Execution Time: 1.2 ms
```

**Interpretation:**
- `shared hit=8` — 8 pages found in the shared buffer cache (fast — no disk I/O)
- `read=2` — 2 pages read from disk (slower)
- `hit rate = 8/(8+2) = 80%` — 80% of needed pages were cached

```sql
-- For a slow query: high read count relative to hits = data not cached (I/O bound)
-- For a fast query: high hit count = everything in memory (CPU bound, not I/O)

-- The shared_buffers setting controls the PostgreSQL buffer cache size:
SHOW shared_buffers;  -- Default: 128MB; production: 25% of RAM
-- If important queries consistently show high read counts, increase shared_buffers
```

---

## Identifying Common Performance Problems

### Problem 1: Loop Count Reveals N+1

```sql
EXPLAIN ANALYZE
SELECT c.name, (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) AS cnt
FROM customers c WHERE country = 'US';
```

```
Seq Scan on customers c  (actual rows=4000 loops=1)
  Filter: (country = 'US')
  SubPlan 1
    -> Aggregate  (actual rows=1 loops=4000)  ← 4000 LOOPS!
         -> Index Scan on orders
              Index Cond: (customer_id = c.customer_id)
```

**Diagnosis:** `loops=4000` on the subplan = correlated subquery running 4,000 times (once per US customer). This is the SQL N+1 problem.

**Fix:**
```sql
-- Replace correlated subquery with join + aggregation (single pass):
EXPLAIN ANALYZE
SELECT c.name, COUNT(o.order_id) AS cnt
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.country = 'US'
GROUP BY c.name;
-- loops=1 everywhere — single pass!
```

### Problem 2: Row Count Mismatch Causing Wrong Join Type

```sql
-- EXPLAIN shows:
Nested Loop  (cost=0.00..999000.00 rows=10) (actual rows=50000 loops=1)
-- Estimated: 10 rows  Actual: 50,000 rows
-- Optimizer chose Nested Loop (good for small results) but actual result is huge
-- Should have been a Hash Join
```

**Cause:** Stale statistics. The optimizer thought the join would return 10 rows, so it chose a Nested Loop. Actually 50,000 rows were returned — Nested Loop is catastrophic at that scale.

**Fix:**
```sql
ANALYZE orders;
ANALYZE customers;
-- Re-run EXPLAIN ANALYZE to see if plan changed

-- If column is highly skewed (e.g., most orders are 'shipped', few are 'pending'):
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;  -- Sample more rows
ANALYZE orders;
-- More accurate statistics → better plan for skewed distributions
```

### Problem 3: Sort Spill to Disk

```sql
-- EXPLAIN output:
Sort  (actual rows=1000000 loops=1)
  Sort Key: order_date DESC
  Sort Method: external merge  Disk: 256000kB  ← 256MB disk sort!
```

**Fix options:**
```sql
-- Option 1: Increase work_mem (session-level for this query)
SET work_mem = '512MB';
-- Now the sort stays in memory

-- Option 2: Add an index matching the ORDER BY (avoid sort entirely)
CREATE INDEX ON orders (order_date DESC);
-- Query may now use Index Scan in the right order — no Sort node needed

-- Option 3: For global config (be careful — applies per sort/hash PER QUERY per connection):
-- In postgresql.conf: work_mem = '64MB'
-- A query with 5 sorts uses 5×64MB = 320MB per connection!
```

---

## Parallel Query Plans

PostgreSQL 9.6+ can parallelize query execution:

```sql
EXPLAIN ANALYZE
SELECT SUM(amount) FROM orders WHERE order_date >= '2024-01-01';
```

```
Finalize Aggregate  (actual rows=1 loops=1)
  -> Gather  (actual rows=3 loops=1)
       Workers Planned: 2
       Workers Launched: 2
       -> Partial Aggregate  (actual rows=1 loops=3)  ← loops=3 means 3 workers (1 leader + 2)
            -> Parallel Seq Scan on orders  (actual rows=333333 loops=3)
-- Each worker scans 1/3 of the table; aggregates merged by leader
```

**Tuning parallel query:**
```sql
-- Control parallelism:
SET max_parallel_workers_per_gather = 4;  -- Max workers per node
SET parallel_tuple_cost = 0.1;            -- Lower = more likely to parallelize
SET parallel_setup_cost = 1000;           -- Higher = less likely (setup overhead)

-- For a specific query:
EXPLAIN (ANALYZE, FORMAT TEXT)
SELECT SUM(amount) FROM orders WHERE order_date >= '2024-01-01';
-- If not using parallel: too few rows, or parallel cost exceeds benefit
```

---

## Cross-Platform Plan Reading

### MySQL EXPLAIN

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;
```

```
+----+-------------+--------+------------+------+-------------------+---------+---------+-------+------+----------+-------+
| id | select_type | table  | partitions | type | possible_keys     | key     | key_len | ref   | rows | filtered | Extra |
+----+-------------+--------+------------+------+-------------------+---------+---------+-------+------+----------+-------+
|  1 | SIMPLE      | orders | NULL       | ref  | idx_customer_id   | idx_... | 4       | const |   15 |   100.00 | NULL  |
+----+-------------+--------+------------+------+-------------------+---------+---------+-------+------+----------+-------+
```

**Key columns:**
- `type`: access method — `ALL` (full scan) < `range` < `ref` < `eq_ref` < `const` (fastest)
- `rows`: estimated rows examined
- `Extra`: "Using index" (index-only), "Using filesort" (bad — needs sort), "Using where" (filter applied)

### BigQuery Query Plan

```sql
-- BigQuery: EXPLAIN not available; use "Execution Details" in Cloud Console
-- Or query INFORMATION_SCHEMA:
SELECT * FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_id = 'your-job-id';
-- Look at: totalBytesProcessed, totalSlotMs (compute cost)
```

---

## Interview Tips

> **Tip 1:** "A query is slow. Walk me through how you'd use EXPLAIN ANALYZE to diagnose it." — "First I run EXPLAIN ANALYZE to see the actual plan. I look for: (1) estimated vs actual rows — a large gap means stale stats, which leads the optimizer to wrong join type or scan type; (2) Seq Scan on large tables — if the filter is selective and an index exists, something is preventing its use; (3) loops count on subplans — high loop counts reveal N+1 correlated subqueries; (4) Sort Method: external merge — sort is spilling to disk, fix by increasing work_mem or adding an index; (5) Hash Batches > 1 — hash join spilling to disk."

> **Tip 2:** "When would you expect a Nested Loop join and when a Hash Join?" — "Nested Loop: outer table is small (dozens to hundreds of rows), inner table has an index on the join column. The optimizer does N index lookups — fast when N is small and each lookup is fast. Hash Join: no index on join column, or both tables are large. Build a hash table from the smaller side, then scan the larger side probing the hash. Merge Join: both sides are already sorted (or the sort cost is justified by large inputs). If the optimizer chose Nested Loop but actual rows were 50K, it was tricked by bad statistics into thinking the result would be small."

> **Tip 3:** "What does it mean when EXPLAIN shows `Rows Removed by Filter: 999000`?" — "It means the database scanned 1,000,000 rows, applied the WHERE clause after reading them, and kept only 1,000. This is wasteful — the filter should be applied via an index BEFORE reading rows. Fix: create an index on the filter column so the scan only reads the 1,000 matching rows. The `Rows Removed by Filter` metric directly quantifies wasted work."
