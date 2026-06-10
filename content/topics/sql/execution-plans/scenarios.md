---
title: "SQL Execution Plans - Scenario Questions"
topic: sql
subtopic: execution-plans
content_type: scenario_question
tags: [sql, execution-plans, explain, query-optimizer, index-scan, performance, interview, scenarios]
---

# Scenario Questions — SQL Execution Plans

<article data-difficulty="junior">

## 🟢 Junior: Diagnose a Slow Query Using EXPLAIN

**Scenario:** A reporting query on an `orders` table is taking 8 seconds. The table has 5 million rows. The query is:

```sql
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE status = 'completed'
GROUP BY customer_id;
```

You run `EXPLAIN ANALYZE` and see `Seq Scan on orders` with `rows=5000000`. What does this tell you, and what would you do to fix it?

<details>
<summary>💡 Hint</summary>

A `Seq Scan` (sequential scan) means the database is reading every single row in the table — it has no index to use for the `WHERE status = 'completed'` filter. Think about what index would help the optimizer skip irrelevant rows before the aggregation.

</details>

<details>
<summary>✅ Solution</summary>

**What the plan tells you:**
- `Seq Scan` = full table scan — all 5 million rows are read, then filtered for `status = 'completed'`
- The optimizer could not use any index for this predicate
- Cost is high because 5M rows are scanned even if only a fraction are `'completed'`

**Fix — create an index on the filter column:**

```sql
-- Step 1: Create index on status
CREATE INDEX idx_orders_status ON orders (status);

-- Better: partial index (only index the rows you query)
CREATE INDEX idx_orders_status_completed ON orders (status)
WHERE status = 'completed';

-- Even better for this exact query: covering index
CREATE INDEX idx_orders_status_customer ON orders (status, customer_id, amount)
WHERE status = 'completed';
```

**After indexing — expected plan:**

```
HashAggregate  (cost=12543.00..12700.00 rows=15700 width=16)
  Group Key: customer_id
  ->  Index Scan using idx_orders_status_completed on orders
        (cost=0.43..10240.00 rows=460000 width=12)
```

**Why the partial index is best:**
- Only indexes `completed` rows — smaller index, less write overhead
- When you always filter `WHERE status = 'completed'`, the index perfectly matches every query
- The covering index also avoids a heap fetch — all needed columns (`status`, `customer_id`, `amount`) are in the index itself

**Key lesson:** `Seq Scan` on a large table filtered by a low-cardinality column (like `status`) is the most common cause of avoidable slow queries. Always check if a WHERE clause column has an index.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Read a Basic EXPLAIN Output

**Scenario:** You run the following EXPLAIN on PostgreSQL and get the output shown below. Walk through what each line means and identify any concerns.

```sql
EXPLAIN SELECT o.order_id, o.amount, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 500;
```

```
Hash Join  (cost=1845.00..9823.00 rows=24800 width=44)
  Hash Cond: (o.customer_id = c.customer_id)
  ->  Seq Scan on orders o  (cost=0.00..7654.00 rows=24800 width=16)
        Filter: (amount > 500)
  ->  Hash  (cost=1220.00..1220.00 rows=50000 width=32)
        ->  Seq Scan on customers c  (cost=0.00..1220.00 rows=50000 width=32)
```

<details>
<summary>💡 Hint</summary>

Read the plan from the innermost (most-indented) nodes outward. Each node shows cost estimates in `(cost=startup..total rows=N width=B)` format. Note that both table scans are `Seq Scan` — consider whether indexes exist and whether they would help here.

</details>

<details>
<summary>✅ Solution</summary>

**Line-by-line breakdown:**

| Node | Meaning |
|------|---------|
| `Hash Join` | Join strategy: build a hash table from one side, probe with the other |
| `Hash Cond` | The join key: `o.customer_id = c.customer_id` |
| `Seq Scan on orders` | Full scan of orders table — reads all rows, applies `amount > 500` filter |
| `Filter: (amount > 500)` | Applied after reading each row (not an index seek) |
| `rows=24800` | Optimizer estimates 24,800 orders have amount > 500 |
| `Hash on customers` | Builds an in-memory hash table from all 50,000 customers |
| `Seq Scan on customers` | Full scan of customers — no WHERE clause here, so this is expected |

**Concerns and improvements:**

```sql
-- Concern 1: Seq Scan on orders filtered by amount
-- If orders is large, index on amount would help
CREATE INDEX idx_orders_amount ON orders (amount)
WHERE amount > 500;  -- partial index for this specific pattern

-- Concern 2: If customer table is always fully scanned for joins,
-- ensure customer_id is the primary key (already indexed)
-- Check:
\d customers  -- PostgreSQL: verify PK exists

-- After adding index on orders.amount, expected improvement:
-- Index Scan on orders → only 24,800 rows fetched vs full table scan
-- Hash on customers (50k rows) is usually acceptable — small table
```

**Costs explained:**
- `cost=0.00..7654.00` means startup cost 0 (no setup), total cost 7654 arbitrary units
- Lower total cost = better plan
- `width=16` = estimated bytes per output row

**Interview takeaway:** Always read plans from inside out. The most expensive nodes (highest cost numbers) are your optimization targets.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Hash Join vs Nested Loop — Choosing the Right Strategy

**Scenario:** You have two tables — `orders` (10 million rows) and `order_items` (50 million rows). The query below is slow:

```sql
SELECT o.order_id, o.customer_id, SUM(oi.quantity * oi.unit_price) AS total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id, o.customer_id;
```

EXPLAIN shows a **Nested Loop Join**. The filter on `order_date` returns 2 million orders. Explain why Nested Loop is wrong here and how to steer the optimizer toward Hash Join.

<details>
<summary>💡 Hint</summary>

Nested Loop join is O(N × M) — it iterates the inner table once per outer row. When the outer result set is large (2 million rows), this means 2M lookups into `order_items`. Hash Join is O(N + M) — it scans each table once. The optimizer may have chosen Nested Loop due to stale statistics or an overly optimistic row estimate.

</details>

<details>
<summary>✅ Solution</summary>

**Why Nested Loop is wrong for large result sets:**

```
Nested Loop:
  - For each of the 2M orders: scan order_items for matching order_id
  - If order_items has an index on order_id: 2M index lookups × ~5 rows each
  - Total: ~10M random I/Os — slow on spinning disk, still expensive on SSD
  - Becomes quadratic if inner table isn't indexed
```

**Why Hash Join is better here:**

```
Hash Join:
  - Scan orders (filtered to 2M rows) → build hash table keyed on order_id
  - Scan order_items once → probe hash table for each row
  - Total: one pass through each table → O(N + M)
  - Ideal when both sides are large
```

**Step 1: Refresh statistics (most common fix):**

```sql
-- PostgreSQL: update table statistics so optimizer has accurate row counts
ANALYZE orders;
ANALYZE order_items;

-- Then re-run EXPLAIN — often flips to Hash Join automatically
EXPLAIN ANALYZE
SELECT o.order_id, o.customer_id, SUM(oi.quantity * oi.unit_price) AS total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id, o.customer_id;
```

**Step 2: Hint the optimizer if still wrong (database-specific):**

```sql
-- PostgreSQL: disable nested loop for this session
SET enable_nestloop = off;

-- Then run the query — optimizer falls back to Hash Join or Merge Join

-- SQL Server: use query hint
SELECT o.order_id, o.customer_id, SUM(oi.quantity * oi.unit_price) AS total
FROM orders o
INNER HASH JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id, o.customer_id;

-- BigQuery: join hints
SELECT o.order_id, SUM(oi.quantity * oi.unit_price) AS total
FROM orders o
JOIN /*+ HASH */ order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id;
```

**Step 3: Structural fix — index to support the filter:**

```sql
-- Index on order_date so the 2M-row filter is efficient
CREATE INDEX idx_orders_date ON orders (order_date)
INCLUDE (order_id, customer_id);

-- Index on order_items join key
CREATE INDEX idx_order_items_order_id ON order_items (order_id)
INCLUDE (quantity, unit_price);
```

**When each join type wins:**

| Scenario | Best Join |
|----------|-----------|
| Small outer, large inner with index | Nested Loop |
| Both sides large, no index | Hash Join |
| Both sides sorted on join key | Merge Join |
| One side fits in memory | Hash Join |
| Correlated subquery-style | Nested Loop |

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Fixing a Plan with Stale Statistics

**Scenario:** A query that used to run in 200ms now takes 45 seconds after a bulk load added 30 million rows to a `transactions` table. EXPLAIN shows the optimizer estimates 500 rows but the query actually returns 8 million. The plan uses an Index Scan that made sense for 500 rows but is catastrophic for 8 million. What happened and how do you fix it?

<details>
<summary>💡 Hint</summary>

The optimizer uses **table statistics** (row counts, column value distributions, null fractions) to estimate cardinality. If these statistics are stale (collected before the bulk load), the optimizer will make decisions based on old row counts. The fix involves refreshing statistics and possibly tuning the `autovacuum`/`autoanalyze` settings.

</details>

<details>
<summary>✅ Solution</summary>

**What happened — stale statistics cascade:**

```
Before bulk load:  transactions = 500K rows
Statistics said:   ~500 rows match the filter (0.1% selectivity)
Optimizer chose:   Index Scan (correct for 500 rows)

After bulk load:   transactions = 30.5M rows  
Statistics still:  ~500 rows estimate (out of date!)
Optimizer chose:   Index Scan (disastrous for 8M rows)
Reality:           8M rows × random index lookups = 45 second query
```

**Immediate fix — refresh statistics:**

```sql
-- PostgreSQL
ANALYZE transactions;
-- or for all tables:
VACUUM ANALYZE;

-- SQL Server
UPDATE STATISTICS transactions;
-- Full rescan (not sampled):
UPDATE STATISTICS transactions WITH FULLSCAN;

-- MySQL
ANALYZE TABLE transactions;

-- Snowflake (automatic — but you can check)
SELECT * FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
    DATABASE_NAME => 'MY_DB',
    TABLE_NAME => 'TRANSACTIONS'
));
```

**After ANALYZE — verify the plan improved:**

```sql
EXPLAIN ANALYZE
SELECT account_id, SUM(amount)
FROM transactions
WHERE tx_date >= '2024-01-01'
GROUP BY account_id;

-- Expected after ANALYZE:
-- Hash Aggregate
--   -> Seq Scan on transactions (rows=8000000 estimated — now accurate)
-- Optimizer correctly chose Seq Scan + Hash Agg over Index Scan for 8M rows
```

**Prevent recurrence — tune autovacuum for high-write tables:**

```sql
-- PostgreSQL: lower the threshold for auto-analyze on this table
ALTER TABLE transactions SET (
    autovacuum_analyze_scale_factor = 0.01,  -- analyze after 1% change (default 20%)
    autovacuum_analyze_threshold = 1000      -- or after 1000 rows changed
);
```

**After a bulk load, always run ANALYZE explicitly:**

```sql
-- Standard post-bulk-load checklist:
BEGIN;
  -- 1. Load data
  INSERT INTO transactions SELECT * FROM transactions_staging;
  
  -- 2. Create/rebuild indexes
  CREATE INDEX CONCURRENTLY idx_tx_date ON transactions (tx_date);
COMMIT;

-- 3. Refresh statistics immediately
ANALYZE transactions;

-- 4. Verify plan is sensible
EXPLAIN SELECT COUNT(*) FROM transactions WHERE tx_date >= '2024-01-01';
```

**Key interview point:** The mismatch between `estimated rows` and `actual rows` in `EXPLAIN ANALYZE` output is your #1 diagnostic signal. A ratio of estimated:actual > 10x almost always means stale statistics.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Step Plan Regression Investigation

**Scenario:** A nightly pipeline runs a complex query joining 6 tables (orders, customers, products, promotions, warehouses, regions). It ran in 3 minutes for 2 years and now takes 40 minutes. The underlying data hasn't changed significantly. You have access to both the old plan (from `pg_stat_statements` history) and the new plan. Describe your complete investigation process and how you would restore performance.

<details>
<summary>💡 Hint</summary>

Performance regressions with stable data usually come from: (1) a statistics change flipping a join order, (2) a new index changing the optimizer's cost estimates, (3) a PostgreSQL/engine version upgrade changing planner defaults, (4) a configuration change (e.g., `work_mem`, `max_parallel_workers`), or (5) table bloat affecting sequential scan costs. Work through these systematically before forcing a plan.

</details>

<details>
<summary>✅ Solution</summary>

**Investigation framework — in order:**

**Step 1: Capture and compare plans**

```sql
-- Get the current plan with full detail
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT /* ... your query ... */;

-- Compare key metrics between old and new plan:
-- 1. Which join order changed?
-- 2. Which node has the largest actual vs estimated row mismatch?
-- 3. Which node has the most Buffers hit (I/O cost)?
-- 4. Did any Hash Join become Nested Loop?
```

**Step 2: Check for statistics drift**

```sql
-- Check when statistics were last collected
SELECT schemaname, tablename, last_analyze, last_autoanalyze, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE tablename IN ('orders','customers','products','promotions','warehouses','regions')
ORDER BY last_analyze;

-- Check if row counts have changed significantly
SELECT relname, reltuples::bigint AS estimated_rows
FROM pg_class
WHERE relname IN ('orders','customers','products','promotions','warehouses','regions');

-- Refresh if stale
ANALYZE orders, customers, products, promotions, warehouses, regions;
```

**Step 3: Check for index changes**

```sql
-- List all indexes (were new ones added recently?)
SELECT indexname, indexdef, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_indexes
WHERE tablename IN ('orders','customers','products')
ORDER BY pg_relation_size(indexrelid) DESC;

-- Check index bloat
SELECT schemaname, tablename, indexname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = 'orders';
```

**Step 4: Check configuration changes**

```sql
-- work_mem affects whether Hash Joins spill to disk
SHOW work_mem;
-- If it dropped (e.g., from 256MB to 4MB), hash joins spill to temp files
-- This can flip optimizer to Nested Loop which avoids hashing

-- Check if parallelism changed
SHOW max_parallel_workers_per_gather;

-- Temporarily increase work_mem to test if this is the cause:
SET work_mem = '256MB';
EXPLAIN ANALYZE /* your query */;
```

**Step 5: Pin the old plan while fixing root cause**

```sql
-- PostgreSQL 14+: pg_hint_plan extension to force join order
/*+ Leading(orders customers products promotions warehouses regions)
    HashJoin(orders customers)
    HashJoin(orders_customers products)
*/
SELECT /* ... */;

-- Or use a plan guide (SQL Server equivalent)
EXEC sp_create_plan_guide
    @name = N'nightly_pipeline_guide',
    @stmt = N'SELECT ...',
    @type = N'SQL',
    @hints = N'OPTION (HASH JOIN, MAXDOP 8)';
```

**Step 6: Permanent fix options**

```sql
-- Option A: Increase statistics target for skewed columns
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;
-- Default is 100 histogram buckets — skewed columns need more
ANALYZE orders;

-- Option B: For a 6-table join, create a summary/pre-aggregated table
-- that the nightly pipeline queries instead of joining 6 raw tables
CREATE MATERIALIZED VIEW nightly_pipeline_summary AS
SELECT o.order_id, c.customer_id, p.product_id, ...
FROM orders o
JOIN customers c ON ...
-- ...
WITH DATA;

CREATE UNIQUE INDEX ON nightly_pipeline_summary (order_id);

-- Refresh nightly before the pipeline runs:
REFRESH MATERIALIZED VIEW CONCURRENTLY nightly_pipeline_summary;

-- Option C: Partition orders by date so nightly scans only today's partition
CREATE TABLE orders_2024_01 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
-- Nightly query with date filter will hit only one small partition
```

**Diagnosis summary checklist:**

| Check | Command | Common Cause |
|-------|---------|--------------|
| Statistics age | `pg_stat_user_tables` | Stale stats after bulk load |
| Row estimate vs actual | `EXPLAIN ANALYZE` | Stats drift, skewed data |
| New indexes | `pg_indexes` | New index changed cost model |
| work_mem | `SHOW work_mem` | Hash join spill to disk |
| Parallelism | `max_parallel_workers` | Parallel disabled/changed |
| Table bloat | `pg_stat_user_tables.n_dead_tup` | Needs VACUUM |
| Planner version | `SELECT version()` | Engine upgrade changed defaults |

**Key senior insight:** Never jump straight to hints. Plan regressions have root causes — always diagnose the cause first. Use hints only as a temporary stabiliser while implementing the structural fix.

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a SQL execution plan and why is it important?**
A: An execution plan is the sequence of operations the query optimizer chooses to execute a SQL query—including join types, index usage, sort operations, and estimated row counts. Understanding execution plans lets you diagnose why a query is slow and verify that indexes are being used as expected.

**Q: What is the difference between EXPLAIN and EXPLAIN ANALYZE?**
A: `EXPLAIN` shows the estimated execution plan (row counts and costs are estimates). `EXPLAIN ANALYZE` actually executes the query and shows actual row counts, timing, and loop counts alongside the estimates. Use EXPLAIN to preview plans; use EXPLAIN ANALYZE to compare estimated vs. actual statistics and find estimation errors.

**Q: What is a nested loop join and when is it efficient?**
A: A nested loop join iterates over the outer table row by row, and for each outer row, scans the inner table (ideally via an index). It's efficient when the outer table is small and the inner table has a supporting index. For large tables without indexes, it degrades to O(n*m) and is almost always the worst join type.

**Q: What is a hash join and when does it outperform nested loops?**
A: A hash join builds an in-memory hash table from the smaller relation, then probes it with each row from the larger relation. It's efficient for large unsorted datasets where no index exists and is the default join strategy in most analytical query engines. Performance degrades if the hash table spills to disk.

**Q: What are the main cost components in a PostgreSQL execution plan?**
A: PostgreSQL costs are in abstract units where 1.0 = reading one sequential page. Cost components include: `seq_page_cost` (sequential scan), `random_page_cost` (random I/O, default 4x more expensive), `cpu_tuple_cost` (processing each row), and `cpu_operator_cost`. The optimizer sums these to estimate total cost.

**Q: What does a high actual-vs-estimated row count discrepancy indicate?**
A: It indicates stale or inaccurate table statistics. The optimizer made a bad cardinality estimate, which likely led to a suboptimal plan (e.g., choosing a nested loop where a hash join would be better). Fix by running ANALYZE to update statistics and potentially adjusting the statistics target for frequently skewed columns.

**Q: What is a merge join and when is it used?**
A: A merge join requires both inputs to be sorted on the join key. It then scans both sorted inputs in parallel, matching rows—O(n+m) time. It's efficient when both sides are already sorted (e.g., via an index scan) or when sorting cost is low. Often used for equality joins on indexed columns in OLTP databases.

**Q: What does "rows removed by filter" mean in an EXPLAIN ANALYZE output?**
A: It shows how many rows were read from storage but then discarded by a filter condition applied after the scan. A high number relative to rows returned indicates a poor index—the query is reading many rows it doesn't need. The fix is typically a more selective index that includes the filter column.

---

## 💼 Interview Tips

- Practice reading real EXPLAIN ANALYZE output from PostgreSQL or Snowflake query profiles before interviews—being able to interpret actual plans (not just describe them theoretically) is a strong differentiator.
- When asked about a slow query, structure your answer as a diagnostic process: run EXPLAIN ANALYZE, look at the highest-cost node, check actual vs. estimated rows, then apply the appropriate fix. This signals systematic thinking.
- Know the three join types (nested loop, hash, merge) cold: when each is chosen, when each is efficient, and when each is bad. This is a frequent interview question at all levels.
- Mention statistics and ANALYZE as a root cause for bad plans—the optimizer is only as good as its statistics. Senior engineers understand that keeping statistics fresh is an operational responsibility.
- Avoid presenting execution plans as academic knowledge. Connect every concept to a real scenario: "I saw this in production when our users table grew 10x and the nested loop join that was fast for 10K rows became catastrophic at 10M rows."
