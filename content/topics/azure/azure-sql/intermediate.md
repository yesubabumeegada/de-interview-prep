---
title: "Azure SQL & Managed Instance — Intermediate"
topic: azure
subtopic: azure-sql
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, azure-sql, performance-tuning, indexing, elastic-pool, cdc, query-store]
---

# Azure SQL & Managed Instance — Intermediate

## Elastic Pools: Multi-Tenant Cost Optimization

```sql
-- Elastic Pool: share compute resources across multiple databases
-- Great for SaaS (one pool for all tenant databases)

-- Example: 100 tenant databases, peak usage is staggered
-- Without pool: 100 databases × 100 DTU each = 10,000 DTU total cost
-- With pool:    peak concurrent load is ~30 databases = 3,000 DTU pool (70% savings)

-- Creating via T-SQL (Azure SQL):
-- Note: managed via Portal/ARM/CLI — T-SQL shows what's inside

-- Create elastic pool (Azure CLI):
-- az sql elastic-pool create \
--   --server myserver --resource-group rg-data \
--   --name tenant-pool \
--   --edition Standard --capacity 3000  -- 3000 DTU pool
--   --db-dtu-min 10 --db-dtu-max 100    -- per-db limits

-- Move database into pool:
-- az sql db update --server myserver --name tenant_acme --elastic-pool tenant-pool

-- Database-level limits:
-- db-dtu-min: guaranteed minimum per DB (even if pool is busy)
-- db-dtu-max: cap per DB (prevents one tenant hogging all resources)

-- Monitoring pool utilization:
SELECT
    elastic_pool_name,
    database_name,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    max_worker_percent,
    end_time
FROM sys.elastic_pool_resource_stats
ORDER BY end_time DESC;
-- If avg_cpu_percent consistently > 80%: pool needs more DTUs
-- If only 2-3 databases are hot: consider moving them to dedicated databases
```

---

## Change Data Capture for Event-Driven Pipelines

```sql
-- Enable CDC on SQL Managed Instance (full SQL Server CDC)
-- Requires: sysadmin or db_owner + SQL Agent running

-- 1. Enable CDC on database
USE orders_db;
EXEC sys.sp_cdc_enable_db;

-- 2. Enable CDC on specific table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'orders',
    @role_name     = NULL,          -- NULL = no role restriction
    @supports_net_changes = 1,      -- enable net change queries (after all changes applied)
    @captured_column_list = N'order_id,customer_id,amount,status,updated_at'; -- column subset

-- 3. Verify CDC is active
SELECT * FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('dbo.orders');

-- 4. Read CDC changes (incremental consumer pattern)
DECLARE @from_lsn BINARY(10), @to_lsn BINARY(10);
SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_orders');
SET @to_lsn   = sys.fn_cdc_get_max_lsn();

SELECT
    __$operation,    -- 1=Delete, 2=Insert, 3=Before Update, 4=After Update
    __$start_lsn,    -- LSN (log sequence number) — use for checkpoint
    __$seqval,
    order_id,
    customer_id,
    amount,
    status,
    updated_at
FROM cdc.fn_cdc_get_all_changes_dbo_orders(
    @from_lsn,
    @to_lsn,
    'all'           -- 'all' returns all changes including before/after update rows
);

-- ADF CDC pipeline:
-- Source: Azure SQL Managed Instance with CDC
-- ADF reads fn_cdc_get_all_changes and maps __$operation to insert/update/delete
-- Sink: Delta Lake with MERGE (upsert on primary key, handle deletes)
```

---

## Query Performance Tuning with Query Store

```sql
-- Query Store: captures query plans and runtime stats automatically
-- Enabled by default in Azure SQL DB

-- Enable Query Store (if disabled):
ALTER DATABASE orders_db SET QUERY_STORE = ON
WITH (
    OPERATION_MODE = READ_WRITE,
    CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30),
    DATA_FLUSH_INTERVAL_SECONDS = 900,
    INTERVAL_LENGTH_MINUTES = 60,
    MAX_STORAGE_SIZE_MB = 1000,
    QUERY_CAPTURE_MODE = AUTO,     -- only capture significant queries
    SIZE_BASED_CLEANUP_MODE = AUTO
);

-- Find top 10 most expensive queries by CPU time:
SELECT TOP 10
    q.query_id,
    qt.query_sql_text,
    rs.avg_cpu_time,
    rs.avg_duration,
    rs.avg_logical_io_reads,
    rs.count_executions,
    rs.avg_cpu_time * rs.count_executions AS total_cpu_time
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
ORDER BY total_cpu_time DESC;

-- Find queries with plan regression (plan changed and got worse):
SELECT
    q.query_id,
    qt.query_sql_text,
    p.plan_id,
    rs.avg_duration,
    rs.first_execution_time
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
WHERE p.is_forced_plan = 0
    AND rs.avg_duration > (
        SELECT AVG(rs2.avg_duration) * 2
        FROM sys.query_store_runtime_stats rs2
        WHERE rs2.plan_id != p.plan_id
    )
ORDER BY rs.avg_duration DESC;

-- Force a good plan (pin the optimizer to a known-good plan):
EXEC sp_query_store_force_plan @query_id = 42, @plan_id = 7;
-- Use when: optimizer keeps choosing bad plan due to parameter sniffing
```

---

## Indexing Strategy for OLTP + Analytics

```sql
-- OLTP indexes: optimize point lookups and small scans
-- Analytics indexes: optimize range scans and aggregations

-- Clustered index: physical row order of the table
-- Best on: primary key (monotonically increasing → no page splits)
CREATE CLUSTERED INDEX CIX_orders_order_id ON dbo.orders (order_id);

-- Non-clustered index (covering): include all columns needed by query
-- Query: SELECT customer_id, amount, status FROM orders WHERE order_date = '2024-01-15'
CREATE NONCLUSTERED INDEX NIX_orders_date
ON dbo.orders (order_date)
INCLUDE (customer_id, amount, status);  -- covering index → no key lookup
-- Without INCLUDE: SQL Server must do a key lookup per row (expensive)

-- Filtered index: index only subset of rows (lower maintenance, smaller size)
CREATE NONCLUSTERED INDEX NIX_orders_pending
ON dbo.orders (order_date, customer_id)
WHERE status = 'PENDING';  -- only index pending orders
-- Query: WHERE status='PENDING' AND order_date > ... → uses this small index

-- Columnstore index for analytics in OLTP (hybrid):
-- Enable row-level columnstore for aggregation queries on operational data
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_orders_analytics
ON dbo.orders (order_date, region, amount, status);
-- Read query: SELECT region, SUM(amount) FROM orders WHERE order_date >= '2024-01-01' GROUP BY region
-- With NCCI: 10× faster (columnstore compression, SIMD aggregation)
-- Write: OLTP inserts go to delta store, background merge to columnstore

-- Index maintenance:
-- Fragmentation > 30%: REBUILD (offline for non-online)
-- Fragmentation 5-30%: REORGANIZE (online, incremental)
SELECT
    OBJECT_NAME(ips.object_id) AS table_name,
    i.name AS index_name,
    ips.avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 5
ORDER BY ips.avg_fragmentation_in_percent DESC;
```

---

## Interview Tips

> **Tip 1:** "When would you use an Elastic Pool vs individual Azure SQL Databases?" — Elastic Pool when: multiple databases with variable, non-overlapping peak usage patterns (SaaS tenants, department DBs). The pool cost is lower than the sum of individual database costs when average utilization across databases is low but each has occasional peaks. When NOT to use: if databases have synchronous peak usage (all databases are busy at the same time), the pool provides no benefit. Also avoid for a single large database — it should be its own dedicated service tier.

> **Tip 2:** "What is parameter sniffing and how do you fix it in Azure SQL?" — Parameter sniffing: SQL Server compiles a query plan based on the first parameter values used, then caches and reuses that plan for all subsequent executions. If the first value was unusual (e.g., a rare customer with 1 row, vs a common customer with 10M rows), the cached plan may be terrible for other parameter values. Fixes: (a) `OPTION (RECOMPILE)` — recompile on every execution (uses current parameter values), best for rarely-run reports; (b) `OPTION (OPTIMIZE FOR UNKNOWN)` — use estimated statistics instead of actual values; (c) Force plan via Query Store (pin the good plan); (d) Rewrite the query to avoid the sniffed parameter.

> **Tip 3:** "How does Synapse Link for Azure SQL DB work and when would you use it?" — Synapse Link creates an analytical replica of Azure SQL DB tables in Synapse workspace with sub-minute latency — no ETL pipeline needed. The operational SQL DB continues serving OLTP queries unaffected (replica is separate). Analytical queries run against the Synapse replica, not the production SQL DB. Use it when: (a) you need near-real-time reporting on operational data, (b) analytical queries are heavy and would impact OLTP performance if run directly, (c) you want to avoid building and maintaining ADF incremental load pipelines for reporting.
