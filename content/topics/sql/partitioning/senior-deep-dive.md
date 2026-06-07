---
title: "SQL Partitioning - Senior Deep Dive"
topic: sql
subtopic: partitioning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, partitioning, partition-pruning, constraint-exclusion, parallel-query, pg_partman, sharding]
---

# SQL Partitioning — Senior-Level Deep Dive

## How the Optimizer Prunes Partitions

Understanding partition pruning mechanics prevents surprises when it fails to work.

### Static vs Dynamic Pruning (PostgreSQL)

```sql
-- STATIC pruning: happens at PLAN time (the optimizer can see the constant)
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-01-15';
-- Pruning at plan time: optimizer knows exactly which partition contains Jan 2024 rows
-- Plan lists only orders_2024_01

-- DYNAMIC pruning: happens at EXECUTION time (runtime parameters)
PREPARE get_orders(DATE) AS
    SELECT * FROM orders WHERE order_date = $1;
EXECUTE get_orders('2024-01-15');
-- Plan is generated with $1 as a placeholder → optimizer can't prune at plan time
-- PostgreSQL 12+: dynamic pruning at execution time handles this correctly
-- PostgreSQL < 12: all partitions may be listed in the plan (but runtime skips them)

-- Functions and casts that prevent pruning:
-- WRONG:
EXPLAIN SELECT * FROM orders WHERE DATE_TRUNC('month', order_date) = '2024-01-01';
-- The function DATE_TRUNC() is applied to the partition key → no pruning

-- RIGHT:
EXPLAIN SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01';
-- Direct comparison on order_date → full pruning
```

### Constraint Exclusion (Legacy Approach)

Before native partitioning in PostgreSQL 10, partitioning was implemented via table inheritance + check constraints. The optimizer used constraint exclusion instead of partition pruning:

```sql
-- Old inheritance-based partitioning (pre-PG10):
CREATE TABLE orders_2024_01 (
    CHECK (order_date >= '2024-01-01' AND order_date < '2024-02-01')
) INHERITS (orders);

-- With constraint_exclusion = ON, the optimizer uses CHECK constraints to prune:
SET constraint_exclusion = partition;  -- Only for inheritance; 'on' is slower
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-01-15';
-- Optimizer checks: does '2024-01-15 >= 2024-01-01 AND < 2024-02-01'? Yes → include
-- Does '2024-01-15 >= 2024-02-01 AND < 2024-03-01'? No → exclude

-- Modern PostgreSQL uses native partitioning instead — faster and more robust
```

---

## Partition Planning Overhead

More partitions = more planning time. This is a real production consideration:

```sql
-- Measure planning time (not execution time):
EXPLAIN (ANALYZE, TIMING) SELECT * FROM orders WHERE customer_id = 42;

-- For a table with 10 partitions: planning time ~1ms
-- For a table with 1000 partitions: planning time ~50-200ms
-- For a table with 10,000 partitions: planning time ~2000ms (2 SECONDS just to plan!)

-- The optimizer must:
-- 1. Look up all partition definitions
-- 2. Check which partitions match the WHERE clause
-- 3. Generate a plan for each qualifying partition
-- Even with pruning, listing 9,990 pruned partitions takes time

-- Rule: Keep total partitions under 1,000 for OLTP; analytics tables can handle more
-- For a daily-partitioned table: 3 years = 1,095 partitions → near the limit
-- Consider monthly partitions for 3+ year retention

-- Check planning time in pg_stat_statements:
SELECT query, calls, total_exec_time, total_plan_time
FROM pg_stat_statements
ORDER BY total_plan_time DESC;
```

---

## pg_partman: Production Partition Management

Managing partitions manually doesn't scale beyond a few tables. `pg_partman` is the standard PostgreSQL extension for automated partition management:

```sql
-- Install pg_partman:
CREATE EXTENSION pg_partman;

-- Set up automatic monthly partitioning for orders:
SELECT partman.create_parent(
    p_parent_table   => 'public.orders',
    p_control        => 'order_date',
    p_type           => 'native',
    p_interval       => 'monthly',
    p_premake        => 4  -- Pre-create 4 future partitions
);

-- pg_partman configuration:
UPDATE partman.part_config
SET 
    retention          = '2 years',     -- Keep 2 years of data
    retention_keep_table = FALSE,        -- Drop expired partitions
    premake            = 4,              -- Always 4 future partitions ready
    automatic_maintenance = 'on'         -- Enable auto-maintenance
WHERE parent_table = 'public.orders';

-- Run maintenance (typically called from pg_cron every hour):
SELECT partman.run_maintenance();
-- Creates needed future partitions
-- Drops/archives expired partitions per retention policy

-- Set up pg_cron to run maintenance automatically:
SELECT cron.schedule('0 * * * *', $$SELECT partman.run_maintenance()$$);
```

---

## Partitioning and Parallel Query

Partitioned tables can execute queries with parallelism that non-partitioned tables can't achieve as effectively:

```sql
-- PostgreSQL: parallel partition scan
SET max_parallel_workers_per_gather = 4;
SET enable_partitionwise_aggregate = on;

EXPLAIN SELECT DATE_TRUNC('month', order_date), SUM(amount)
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY DATE_TRUNC('month', order_date);

-- Without partition-wise aggregate:
-- Gather → Hash Aggregate → Parallel Seq Scan (merges all partitions first, then aggregates)

-- With partition-wise aggregate:
-- Hash Aggregate (Partition-Wise)
--   → Parallel Seq Scan on orders_2024_01
-- Hash Aggregate (Partition-Wise)
--   → Parallel Seq Scan on orders_2024_02
-- ... each month aggregated independently, then results merged
-- Better: intermediate results are smaller; less memory pressure
```

### Parallel Partition Maintenance

One of the most compelling production benefits:

```sql
-- VACUUM partitions in parallel (each is a separate table):
-- Run from shell:
-- vacuumdb --analyze --jobs=4 mydb  (processes 4 partitions simultaneously)

-- REINDEX individual partitions without blocking others:
REINDEX TABLE CONCURRENTLY orders_2024_01;
-- While orders_2024_01 is being reindexed:
-- orders_2024_02 through orders_2024_12 are fully accessible with their indexes

-- CREATE INDEX on a specific partition only:
CREATE INDEX CONCURRENTLY idx_p01_status ON orders_2024_01(status);
-- Useful for backfilling indexes on specific partitions without full-table rebuild
```

---

## Partitioning Strategies for Different Workloads

### Time-Series Data (IoT, Events, Logs)

```sql
-- High-volume time-series: partition by day or hour for very high ingest rates
CREATE TABLE sensor_readings (
    sensor_id   INT,
    recorded_at TIMESTAMPTZ NOT NULL,
    value       FLOAT
) PARTITION BY RANGE (recorded_at);

-- Daily partitions for high-volume data:
CREATE TABLE sensor_readings_2024_01_15 PARTITION OF sensor_readings
    FOR VALUES FROM ('2024-01-15') TO ('2024-01-16');

-- Retention: drop partitions older than 90 days (automated)
-- Only 90 active daily partitions + unlimited archive
-- Queries for a specific day: scan 1 of 90 partitions (1% of active data)
```

### Multi-Tenant SaaS

```sql
-- Hash partition by tenant_id for even distribution
CREATE TABLE tenant_data (
    tenant_id   INT NOT NULL,
    record_id   BIGINT NOT NULL,
    data        JSONB,
    created_at  TIMESTAMPTZ
) PARTITION BY HASH (tenant_id);

-- 64 partitions: tenant_id % 64 determines the partition
-- Each partition is ~1/64 of total data
-- Tenant-specific queries hit only 1 partition
-- Index per partition is 64× smaller than a global index

-- With Row Level Security:
ALTER TABLE tenant_data ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_data
    USING (tenant_id = current_setting('app.tenant_id')::INT);
-- Partition pruning + RLS = both physical and logical isolation
```

### Analytics Fact Tables

```sql
-- Range + Hash composite for a large fact table
-- Partition by year (lifecycle management) then by region (query performance)
CREATE TABLE fact_sales (
    sale_id     BIGINT,
    sale_date   DATE NOT NULL,
    region      TEXT NOT NULL,
    amount      NUMERIC,
    product_id  INT
) PARTITION BY RANGE (sale_date);

CREATE TABLE fact_sales_2024 PARTITION OF fact_sales
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
    PARTITION BY LIST (region);

CREATE TABLE fact_sales_2024_us   PARTITION OF fact_sales_2024 FOR VALUES IN ('US');
CREATE TABLE fact_sales_2024_eu   PARTITION OF fact_sales_2024 FOR VALUES IN ('EU');
CREATE TABLE fact_sales_2024_apac PARTITION OF fact_sales_2024 FOR VALUES IN ('APAC');

-- Query: WHERE sale_date = '2024-Q1' AND region = 'US' → prunes to 1 leaf partition
```

---

## Sharding vs Partitioning

Senior engineers must articulate this distinction clearly:

| Aspect | Partitioning | Sharding |
|--------|-------------|---------|
| Location | All partitions on ONE database server | Shards on MULTIPLE database servers |
| Purpose | I/O efficiency, lifecycle management | Horizontal scaling beyond single server |
| Transparency | Fully transparent to application | Requires shard-aware application or proxy |
| Transactions | Full ACID across partitions | Distributed transactions (complex) |
| Tooling | Native DB feature | Vitess, Citus, pg_shard, application-level |
| When | Table too large for efficient queries | Server too small for the data/load |

```sql
-- Citus (PostgreSQL extension): transparent sharding
-- Makes PostgreSQL partition data across multiple nodes
CREATE EXTENSION citus;

-- Distribute orders table across nodes:
SELECT create_distributed_table('orders', 'customer_id');
-- Citus creates shards (like partitions) across all Citus worker nodes
-- Queries are routed transparently; co-located joins work automatically
```

---

## Interview Tips

> **Tip 1:** "What's the overhead of having 10,000 partitions in PostgreSQL?" — "Each partition is a separate catalog entry. The planner must evaluate all 10,000 partition bounds to determine which to prune — this planning overhead can exceed 2 seconds per query, regardless of execution time. For daily partitions over 10+ years, this becomes a real problem. Solutions: use monthly partitions instead, upgrade to PostgreSQL 14+ which has faster pruning, or use pg_partman with aggressive archiving to keep active partitions under 1,000."

> **Tip 2:** "How do you handle a partition that grows unexpectedly large?" — "First diagnose: is the partition key skewed (all rows have the same value)? Is data being incorrectly routed to the default partition? Is the partition granularity wrong for the data volume? Remediation options: add a sub-partition dimension, split the partition into smaller ones using DETACH + CREATE + INSERT SELECT + reattach, or for Snowflake change the cluster key. Going forward, set alerts when any single partition exceeds a threshold (e.g., 100GB) using pg_relation_size monitoring."

> **Tip 3:** "Explain partition-wise join and when it helps." — "Partition-wise join works when two tables are partitioned identically on the same key. The planner can join each partition pair independently: partition 0 of table A joins only partition 0 of table B, and so on in parallel. This reduces the join's working set at each step and enables true parallelism across partition pairs. It's most beneficial for large fact-to-dimension joins in analytics workloads and requires enabling `enable_partitionwise_join`. The tables must use the same partition scheme (same key, same number of partitions, same boundaries)."
