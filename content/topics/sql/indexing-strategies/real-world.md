---
title: "SQL Indexing Strategies - Real-World Production Examples"
topic: sql
subtopic: indexing-strategies
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, indexing, production, performance-tuning, slow-query, postgresql, mysql, snowflake]
---

# SQL Indexing Strategies — Real-World Production Examples

## Scenario 1: Diagnosing and Fixing a Slow Customer Lookup API

**Business context:** An e-commerce platform's customer service portal is timing out. The backend team reports that `GET /api/customers/{id}/recent-orders` is taking 8–12 seconds. The API runs the following query against a 50-million-row orders table with no indexes beyond the primary key.

**Step 1: Diagnose with EXPLAIN ANALYZE**

```sql
-- The slow query:
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    o.order_id,
    o.order_date,
    o.status,
    o.amount,
    oi.product_name,
    oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = 12345
  AND o.order_date >= NOW() - INTERVAL '90 days'
ORDER BY o.order_date DESC
LIMIT 25;
```

**Actual output (before fix):**
```
Limit  (cost=8234.56..8234.62 rows=25 width=87) (actual time=9842.311..9842.321 rows=25 loops=1)
  -> Sort  (actual time=9842.302..9842.308 rows=25 loops=1)
      -> Hash Join  (actual time=9841.1..9841.9 rows=42 loops=1)
          -> Seq Scan on orders  (actual time=0.02..9523.4 rows=50000000 loops=1)   ← PROBLEM
                Filter: (customer_id = 12345 AND order_date >= ...)
                Rows Removed by Filter: 49999958
          -> Hash Scan on order_items  ...
Buffers: shared hit=892345 read=231456    ← 231K disk reads
Planning time: 0.8 ms
Execution time: 9843.2 ms
```

**Step 2: Create the right indexes**

```sql
-- Index 1: support the customer_id + date filter + ORDER BY
-- Covering index adds status, amount to avoid table access for the SELECT columns
CREATE INDEX CONCURRENTLY idx_orders_customer_date 
    ON orders(customer_id, order_date DESC)
    INCLUDE (status, amount);   -- PostgreSQL 11+: INCLUDE non-key columns
-- INCLUDE columns are stored in leaf pages but not used for ordering

-- Index 2: order_items join column
CREATE INDEX CONCURRENTLY idx_order_items_order_id ON order_items(order_id);
```

**Step 3: Verify the fix**

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT o.order_id, o.order_date, o.status, o.amount, oi.product_name, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = 12345
  AND o.order_date >= NOW() - INTERVAL '90 days'
ORDER BY o.order_date DESC
LIMIT 25;
```

**Output after fix:**
```
Limit  (actual time=1.234..1.289 rows=25 loops=1)
  -> Nested Loop  (actual time=1.231..1.284 rows=25 loops=1)
      -> Index Scan Backward using idx_orders_customer_date on orders
              (actual time=0.041..0.189 rows=25 loops=1)
              Index Cond: (customer_id = 12345 AND order_date >= ...)    ← Index used
      -> Index Scan using idx_order_items_order_id on order_items
              (actual time=0.042..0.044 rows=2 loops=25)
Buffers: shared hit=178 read=0    ← 0 disk reads (all in cache)
Execution time: 1.3 ms     ← From 9843ms to 1.3ms
```

**Key decisions made:**
- `order_date DESC` in the index matches the `ORDER BY` direction — the optimizer can read the index in reverse for the LIMIT without a sort step
- `INCLUDE (status, amount)` avoids heap fetches for those columns while keeping them out of the sort key
- `CONCURRENTLY` allowed the production table to keep accepting writes during the 4-minute index build

---

## Scenario 2: Removing Index Bloat That Was Causing Storage Costs

**Business context:** The DBA team receives a PagerDuty alert that PostgreSQL disk usage will exceed capacity in 48 hours despite data growth being flat. Investigation reveals the `events` table (2 billion rows, 800GB) has indexes consuming 600GB — 75% of total storage. The table is event-log style with heavy inserts (100K rows/sec) and no deletes, but rows are frequently updated within the first 24 hours.

**Step 1: Audit current indexes**

```sql
SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid))  AS index_size,
    idx_scan                                        AS total_scans_since_restart,
    idx_tup_read                                    AS tuples_read,
    pg_get_indexdef(indexrelid)                    AS index_definition
FROM pg_stat_user_indexes
JOIN pg_indexes USING (indexrelname)
WHERE tablename = 'events'
ORDER BY pg_relation_size(indexrelid) DESC;
```

| indexname | index_size | total_scans | index_definition |
|-----------|-----------|-------------|-----------------|
| idx_events_user_ts | 210 GB | 8,234,111 | ON events(user_id, event_timestamp) |
| idx_events_session | 180 GB | 12,451 | ON events(session_id, event_type) |
| idx_events_device | 120 GB | 891 | ON events(device_id) |
| idx_events_raw_payload | 90 GB | 0 | ON events(raw_payload) |

**Step 2: Diagnose bloat**

```sql
-- Check why idx_events_session and idx_events_device are so large
-- (low scan counts suggest they may be bloated or redundant)

-- Check fragmentation via pgstattuple
SELECT * FROM pgstattuple('idx_events_session');
-- avg_leaf_density: 42%   ← Only 42% of index pages are actually used!
-- Healthy target: 70-80%+
-- Cause: frequent updates to session_id column creating page splits
```

**Step 3: Remediation plan**

```sql
-- Action 1: Drop the unused index immediately
DROP INDEX CONCURRENTLY idx_events_raw_payload;
-- Saves 90 GB immediately, zero risk

-- Action 2: Rebuild the bloated session index with lower fill factor
DROP INDEX CONCURRENTLY idx_events_session;
CREATE INDEX CONCURRENTLY idx_events_session 
    ON events(session_id, event_type) 
    WITH (fillfactor = 70);  -- Reserve 30% for updates
-- Takes ~30 min on 2B rows; table stays writable

-- Action 3: Convert device_id index to partial (low scan count suggests few callers)
-- Investigate: when is device_id queried?
SELECT query, calls, total_exec_time
FROM pg_stat_statements
WHERE query ILIKE '%device_id%'
ORDER BY total_exec_time DESC;
-- Result: only used for anomaly detection job that filters WHERE is_anomaly = TRUE (1% of rows)

DROP INDEX CONCURRENTLY idx_events_device;
CREATE INDEX CONCURRENTLY idx_events_device_anomaly 
    ON events(device_id) 
    WHERE is_anomaly = TRUE;  -- Partial index covers actual use case; 99% smaller
-- Saves ~119 GB (from 120 GB to ~1 GB for 1% of rows)

-- Total savings: 90 + (180*0.58 = 104) + 119 = ~313 GB recovered
```

**Step 4: Prevent recurrence**

```sql
-- Set up monitoring for index bloat
CREATE OR REPLACE VIEW index_health AS
SELECT 
    indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan,
    CASE 
        WHEN idx_scan = 0 THEN 'CANDIDATE_FOR_REMOVAL'
        WHEN idx_scan < 1000 THEN 'RARELY_USED'
        ELSE 'ACTIVE'
    END AS status
FROM pg_stat_user_indexes
WHERE schemaname = 'public';
-- Alert if any CANDIDATE_FOR_REMOVAL index > 1 GB
```

---

## Scenario 3: Indexing Strategy for a Multi-Tenant SaaS Analytics Database

**Business context:** A B2B SaaS company has a `metrics` table (5 billion rows) shared across 10,000 tenant accounts. Each tenant's dashboard runs queries like: "show me all metrics for my account, filtered by metric type, over the past 30 days, grouped by day." Queries from one tenant must never see another tenant's data, and p99 latency must be under 500ms.

**The challenge:** A single index on `(tenant_id, metric_type, recorded_at)` would work but is 400GB. Snowflake's cluster key approach may be more appropriate than traditional B-tree indexes.

```sql
-- Snowflake implementation:
CREATE TABLE metrics (
    metric_id     BIGINT,
    tenant_id     INT,
    metric_type   VARCHAR(50),
    recorded_at   TIMESTAMP,
    value         FLOAT,
    dimensions    VARIANT  -- JSONB-like flexible columns
)
CLUSTER BY (tenant_id, TO_DATE(recorded_at));
-- Snowflake clusters micro-partitions by (tenant_id, date)
-- Queries filtering on tenant_id + date range skip irrelevant micro-partitions entirely

-- Automatic clustering: Snowflake maintains the cluster key automatically
ALTER TABLE metrics SUSPEND RECLUSTER;  -- Can pause to save credits
ALTER TABLE metrics RESUME RECLUSTER;   -- Resume before heavy query periods
```

**For PostgreSQL on a smaller scale:**

```sql
-- Partition by tenant_id (tenant-level partitioning reduces index size per shard)
CREATE TABLE metrics (
    metric_id     BIGSERIAL,
    tenant_id     INT NOT NULL,
    metric_type   VARCHAR(50),
    recorded_at   TIMESTAMPTZ,
    value         FLOAT
) PARTITION BY HASH(tenant_id);

-- Create 64 partitions (hash partitioning distributes evenly)
DO $$
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format('CREATE TABLE metrics_p%s PARTITION OF metrics FOR VALUES WITH (MODULUS 64, REMAINDER %s)', i, i);
    END LOOP;
END $$;

-- Each partition gets its own smaller index:
CREATE INDEX ON metrics(tenant_id, metric_type, recorded_at DESC);
-- 64 smaller indexes → each 1/64 the size of one monolithic index
-- Per-partition vacuum and index maintenance
-- Queries for a single tenant only scan one partition

-- Verify partition pruning:
EXPLAIN SELECT * FROM metrics WHERE tenant_id = 42 AND recorded_at >= NOW() - INTERVAL '30 days';
-- Should show: "Seq Scan on metrics_p42" — only one of 64 partitions scanned
```

**Row-level security to enforce tenant isolation:**

```sql
ALTER TABLE metrics ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON metrics
    FOR ALL
    TO app_role
    USING (tenant_id = current_setting('app.tenant_id')::INT);

-- Application sets the tenant context at connection time:
SET app.tenant_id = '42';
-- Now all queries automatically filter to tenant 42's rows
-- Partition pruning + RLS + index = sub-100ms per query for tenants with < 10M rows
```

---

## Interview Tips

> **Tip 1:** "Walk me through how you'd diagnose a slow query in production." — "I start with `EXPLAIN ANALYZE` — not just EXPLAIN, because the actual row counts matter. I look for Sequential Scans on large tables, large discrepancies between estimated and actual rows (bad statistics), and Hash Join with large inner tables. Then I check: does an appropriate index exist? Is it being used? If not used, why — function wrap, type mismatch, low selectivity? I create the fix with CONCURRENTLY/ONLINE to avoid downtime, verify with EXPLAIN ANALYZE again, and monitor pg_stat_user_indexes to confirm the new index is being scanned."

> **Tip 2:** "How do you handle indexing for multi-tenant databases at scale?" — "The key strategies are table partitioning by tenant_id (so each tenant's data is in its own partition with a smaller index), combined with Row Level Security for isolation enforcement. For cloud warehouses like Snowflake, I use cluster keys on (tenant_id, date) rather than traditional indexes — micro-partition pruning replaces index lookups at that scale. I avoid indexing the tenant_id column alone because the optimizer often prefers a full partition scan over an index scan when fetching >1% of a partition's rows."

> **Tip 3:** "What's your process for auditing and cleaning up indexes in a legacy database?" — "I run three queries: (1) `pg_stat_user_indexes` to find indexes with zero or near-zero scans — immediate drop candidates. (2) A redundancy check — if (a,b) and (a) both exist, drop (a). (3) Size vs usage ratio — large indexes with low scan counts. Before dropping anything, I check pg_stat_statements to verify the index isn't used in a query that runs at off-peak hours or only on weekends (stats may not capture it). I then drop with CONCURRENTLY in a low-traffic window."
