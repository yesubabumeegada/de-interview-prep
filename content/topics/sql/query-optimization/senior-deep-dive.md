---
title: "SQL Query Optimization - Senior Deep Dive"
topic: sql
subtopic: query-optimization
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, optimization, cost-based-optimizer, distributed-queries, skew, adaptive-execution]
---

# SQL Query Optimization — Senior-Level Deep Dive

## Cost-Based Optimizer (CBO) Internals

The CBO evaluates multiple execution plans and picks the one with the lowest estimated cost.

### What the Optimizer Considers

| Factor | Source | How It Uses It |
|--------|--------|---------------|
| Row count per table | Table statistics | Determines join order |
| Column cardinality (NDV) | Column statistics | Estimates selectivity |
| Data distribution | Histograms | Predicts filter output rows |
| Index availability | Metadata catalog | Chooses access method |
| Sort order (clustering) | Physical metadata | Skips explicit sort |
| Available memory | Configuration | Decides hash vs sort strategies |
| Parallelism | Cluster config | Distributes work |

### Join Order Optimization

For N tables, there are N! possible join orderings. The optimizer uses dynamic programming or heuristics to find the cheapest:

```sql
-- 4-table join: 4! = 24 possible orderings
-- Optimizer evaluates cost of each and picks cheapest

-- This query's optimal order depends on table sizes and selectivity:
SELECT *
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key       -- 365 rows
JOIN dim_product p ON f.product_key = p.product_key  -- 10K rows
JOIN dim_customer c ON f.customer_key = c.customer_key -- 5M rows
WHERE d.year = 2024;

-- Optimal: filter dim_date first (365→1/4=91 rows), then join to fact,
-- then small dim_product, then large dim_customer last
```

### When the Optimizer Gets It Wrong

**Symptoms:**
- Query plan uses nested loops on large tables (should use hash join)
- Wrong join order (starts with largest table instead of smallest)
- Underestimates rows → chooses plan that can't handle actual volume

**Causes:**
- Stale statistics (table grew 10x since last ANALYZE)
- Correlated columns (optimizer assumes independence)
- Complex predicates (optimizer can't estimate selectivity)
- Parameter sniffing (SQL Server: plan cached for atypical parameter)

**Fixes:**

```sql
-- Force statistics refresh
ANALYZE TABLE fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;

-- Hint the optimizer (when you know better)
-- PostgreSQL:
SET enable_nestloop = off;  -- Force hash/merge joins

-- SQL Server:
SELECT * FROM orders WITH (INDEX(idx_orders_date)) WHERE order_date = '2024-01-15';

-- Spark:
SELECT /*+ BROADCAST(dim_product) */ * FROM fact_sales JOIN dim_product ...;
```

---

## Distributed Query Optimization (Spark/Snowflake)

### Shuffle Reduction

Shuffles (data redistribution across nodes) are the #1 performance killer in distributed SQL.

**Operations that cause shuffles:**
- `GROUP BY` (repartition by group key)
- `JOIN` (repartition both sides by join key)
- `DISTINCT` (repartition by all columns)
- `ORDER BY` (global sort requires data movement)
- Window functions with `PARTITION BY`

**Optimization strategies:**

```sql
-- 1. Combine multiple aggregations into one GROUP BY
-- BAD: two shuffles
WITH counts AS (SELECT customer_id, COUNT(*) AS cnt FROM orders GROUP BY customer_id),
     sums AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id)
SELECT * FROM counts JOIN sums USING (customer_id);  -- Third shuffle for join!

-- GOOD: one shuffle
SELECT customer_id, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders GROUP BY customer_id;

-- 2. Broadcast small dimension tables (eliminates shuffle of large table)
SELECT /*+ BROADCAST(dim) */ f.*, dim.name
FROM fact_table f JOIN dim_table dim ON f.key = dim.key;

-- 3. Pre-partition tables on join key (collocated join, zero shuffle)
-- Redshift: DISTKEY on join column
-- Spark: df.repartition(col("key")).write.bucketBy(100, "key")
```

### Data Skew Handling

**Detection:**

```sql
-- Find hot keys (skewed join keys)
SELECT join_key, COUNT(*) AS cnt
FROM large_table
GROUP BY join_key
ORDER BY cnt DESC
LIMIT 10;

-- If top key has 100x more rows than average: you have skew
```

**Mitigation approaches:**

| Approach | When | Implementation |
|----------|------|---------------|
| Broadcast join | Small table fits in memory | `/*+ BROADCAST(small) */` |
| Salting | Known hot key | Add random suffix to hot key, replicate dim |
| Adaptive Query Execution | Spark 3.0+ | `spark.sql.adaptive.skewJoin.enabled=true` |
| Separate hot/cold paths | Few predictable hot keys | Process hot keys with broadcast, cold normally |
| Pre-aggregate | Hot key is irrelevant to output | Aggregate before joining |

---

## Query Rewrite Patterns

### Pattern 1: Anti-Join Optimization

```sql
-- SLOW: NOT IN with subquery (may scan twice)
SELECT * FROM customers
WHERE id NOT IN (SELECT customer_id FROM orders WHERE order_date > '2024-01-01');

-- FAST: LEFT JOIN anti-pattern (single scan)
SELECT c.*
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id AND o.order_date > '2024-01-01'
WHERE o.customer_id IS NULL;

-- FASTEST: NOT EXISTS (short-circuits)
SELECT * FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.order_date > '2024-01-01'
);
```

### Pattern 2: Eliminate Redundant Sorting

```sql
-- BAD: Sorts twice (once for window, once for output)
SELECT name, salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
FROM employees
ORDER BY salary DESC;  -- Redundant! Already sorted by the window function

-- GOOD: Single sort (window function's ORDER BY satisfies the outer ORDER BY)
SELECT name, salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
FROM employees;
-- Note: SQL does NOT guarantee result order without an explicit ORDER BY.
-- If you need guaranteed output order, keep the final ORDER BY — the optimizer
-- can usually satisfy both the window and the output with a single sort.
```

### Pattern 3: Predicate Pushdown Through Views

```sql
-- If you have a view:
CREATE VIEW v_recent_orders AS
SELECT * FROM orders WHERE order_date >= CURRENT_DATE - 365;

-- And query it with additional filter:
SELECT * FROM v_recent_orders WHERE customer_id = 42;

-- Optimizer should combine into:
-- SELECT * FROM orders WHERE order_date >= ... AND customer_id = 42
-- But complex views with UNION, DISTINCT, or GROUP BY can block pushdown!
```

### Pattern 4: Late Materialization

Only read expensive columns (TEXT, BLOB, JSON) after filtering:

```sql
-- BAD: Reads large JSON column for ALL 100M rows before filtering
SELECT id, metadata FROM events WHERE event_type = 'purchase';

-- BETTER: Filter first on indexed/cheap columns, then fetch expensive columns
WITH filtered AS (
    SELECT id FROM events WHERE event_type = 'purchase'
    -- Only reads id and event_type columns (cheap, indexed)
)
SELECT e.id, e.metadata  -- Now reads metadata only for matched rows
FROM filtered f
JOIN events e ON f.id = e.id;
```

---

## Query Tuning for Specific Platforms

### PostgreSQL

```sql
-- Check query plan with actual timing
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT ...;

-- Key settings to tune:
SET work_mem = '256MB';              -- Memory for sorts/hashes (per operation)
SET effective_cache_size = '8GB';     -- Optimizer hint for cache size
SET random_page_cost = 1.1;          -- Lower for SSD (default 4.0 is for HDD)

-- Parallel query tuning
SET max_parallel_workers_per_gather = 4;
```

### Snowflake

```sql
-- Check query profile in UI for:
-- 1. Partitions scanned vs total (pruning effectiveness)
-- 2. Bytes spilled to local/remote (warehouse too small)
-- 3. Queued time (need multi-cluster)

-- Key optimizations:
ALTER TABLE fact_sales CLUSTER BY (sale_date, store_id);  -- Clustering
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = 'XLARGE';  -- Scale up
```

### Spark SQL

```python
# Enable Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")

# Check physical plan
df.explain("cost")  # Shows estimated costs
```

---

## Monitoring and Continuous Optimization

### Identifying Slow Queries Automatically

```sql
-- PostgreSQL: find slowest queries
SELECT query, calls, total_exec_time / 1000 AS total_seconds,
       mean_exec_time / 1000 AS avg_seconds, rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;

-- Snowflake: find expensive queries
SELECT query_id, query_text, total_elapsed_time/1000 AS seconds,
       bytes_scanned / POWER(1024,3) AS gb_scanned,
       partitions_scanned, partitions_total
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

### Query Performance Regression Detection

```sql
-- Alert if average query time increases >50% week-over-week
WITH weekly_stats AS (
    SELECT 
        DATE_TRUNC('week', start_time) AS week,
        query_hash,
        AVG(total_elapsed_time) AS avg_time,
        COUNT(*) AS executions
    FROM query_history
    WHERE start_time > DATEADD(week, -4, CURRENT_DATE)
    GROUP BY week, query_hash
)
SELECT 
    curr.query_hash,
    curr.avg_time AS this_week_ms,
    prev.avg_time AS last_week_ms,
    (curr.avg_time - prev.avg_time) / NULLIF(prev.avg_time, 0) * 100 AS pct_change
FROM weekly_stats curr
JOIN weekly_stats prev ON curr.query_hash = prev.query_hash
    AND curr.week = prev.week + INTERVAL '1 week'
WHERE (curr.avg_time - prev.avg_time) / NULLIF(prev.avg_time, 0) > 0.5  -- >50% slower
ORDER BY pct_change DESC;
```

---

## Interview Tips

> **Tip 1:** "Explain cost-based optimization" — "The CBO evaluates multiple execution plans by estimating I/O cost, CPU cost, and memory usage for each. It uses table statistics (row counts, distinct values, histograms) to predict how much data flows through each operator. It picks the plan with lowest estimated total cost. When statistics are stale, it can pick terrible plans."

> **Tip 2:** "How do you handle data skew in distributed SQL?" — "First, detect it: GROUP BY the join key and check for hot keys. Then mitigate: broadcast join if the small side fits in memory, salting technique for known hot keys, or Spark's Adaptive Query Execution which detects and splits skewed partitions at runtime."

> **Tip 3:** "What's the most impactful optimization you've done?" — Structure your answer: "I had a query doing [X] that took [Y time]. I looked at the plan and found [specific problem — full scan/bad join/skew]. I fixed it by [adding index/broadcasting/repartitioning]. Result: [before] → [after] (quantify the improvement with actual numbers)."
