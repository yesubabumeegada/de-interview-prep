---
title: "SQL Partitioning - Real-World Production Examples"
topic: sql
subtopic: partitioning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, partitioning, production, time-series, multi-tenant, bigquery, snowflake]
---

# SQL Partitioning — Real-World Production Examples

## Scenario 1: Event Log Table Growing to 10TB — Partition Migration

**Business context:** A mobile analytics platform stores all user events in a single `events` table. After 3 years, the table has grown to 10TB with 50 billion rows. Queries for specific date ranges take 20+ minutes. The team decides to migrate to a monthly partitioned table with a 2-year retention policy and automatic archiving.

**Phase 1: Create the partitioned table structure**

```sql
-- Create the new partitioned table (zero downtime — runs alongside existing table)
CREATE TABLE events_partitioned (
    event_id    BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    app_id      INT NOT NULL,
    event_type  TEXT NOT NULL,
    properties  JSONB,
    event_ts    TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (event_ts);

-- Create partitions: 2 years back + current + 3 months future
DO $$
DECLARE
    v_month DATE := DATE_TRUNC('month', NOW() - INTERVAL '24 months');
BEGIN
    WHILE v_month <= DATE_TRUNC('month', NOW() + INTERVAL '3 months') LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events_partitioned
             FOR VALUES FROM (%L) TO (%L)',
            TO_CHAR(v_month, 'YYYY_MM'),
            v_month,
            v_month + INTERVAL '1 month'
        );
        v_month := v_month + INTERVAL '1 month';
    END LOOP;
END $$;

-- Create default partition for out-of-range data:
CREATE TABLE events_default PARTITION OF events_partitioned DEFAULT;

-- Create indexes (automatically inherited by all current and future partitions in PG11+):
CREATE INDEX idx_events_user_ts ON events_partitioned (user_id, event_ts DESC);
CREATE INDEX idx_events_app_type ON events_partitioned (app_id, event_type);
```

**Phase 2: Backfill data in batches (avoid locking)**

```sql
-- Backfill month by month using INSERT SELECT (not a single massive INSERT)
DO $$
DECLARE
    v_month DATE := DATE_TRUNC('month', NOW() - INTERVAL '24 months');
    v_end DATE;
    v_inserted BIGINT;
BEGIN
    WHILE v_month <= DATE_TRUNC('month', NOW()) LOOP
        v_end := v_month + INTERVAL '1 month';
        
        INSERT INTO events_partitioned 
        SELECT event_id, user_id, app_id, event_type, properties, event_ts, ingested_at
        FROM events  -- Old unpartitioned table
        WHERE event_ts >= v_month AND event_ts < v_end;
        
        GET DIAGNOSTICS v_inserted = ROW_COUNT;
        RAISE NOTICE 'Backfilled %: % rows', TO_CHAR(v_month, 'YYYY-MM'), v_inserted;
        
        COMMIT;  -- Commit each month separately — don't hold locks for hours
        v_month := v_month + INTERVAL '1 month';
    END LOOP;
END $$;
```

**Phase 3: Cutover with minimal downtime**

```sql
-- After backfill is complete (but before cutover):
-- 1. Lock old table briefly to prevent new writes
-- 2. Sync the delta (rows inserted during backfill)
-- 3. Rename tables atomically

BEGIN;
LOCK TABLE events IN ACCESS EXCLUSIVE MODE;  -- Brief lock for rename

-- Sync any rows written after backfill started (delta sync):
INSERT INTO events_partitioned 
SELECT event_id, user_id, app_id, event_type, properties, event_ts, ingested_at
FROM events
WHERE ingested_at > (SELECT MAX(ingested_at) FROM events_partitioned)
ON CONFLICT (event_id) DO NOTHING;

-- Atomic rename:
ALTER TABLE events RENAME TO events_unpartitioned_backup;
ALTER TABLE events_partitioned RENAME TO events;

COMMIT;
-- Lock released — application now writes to partitioned table

-- After validation (1 week): drop the backup
-- DROP TABLE events_unpartitioned_backup;
```

**Phase 4: Set up automated lifecycle management**

```sql
-- pg_cron: create next month's partition 3 days before month end
SELECT cron.schedule(
    'create-next-partition',
    '0 0 28 * *',  -- 28th of every month
    $$SELECT partman.run_maintenance()$$
);

-- pg_cron: archive partitions older than 2 years (first day of month at 3am)
SELECT cron.schedule(
    'archive-old-partitions',
    '0 3 1 * *',
    $$
    DO $$
    DECLARE cutoff DATE := DATE_TRUNC('month', NOW() - INTERVAL '24 months');
    BEGIN
        -- Detach partitions older than 2 years and move to archive schema
        -- (specific implementation depends on retention policy)
    END $$
    $$
);
```

**Result:** Query for a 7-day window went from 23 minutes to 8 seconds — the query now scans only 1 of 27 partitions instead of the entire 10TB table. The monthly DROP TABLE for archival takes <1 second vs. the 4-hour DELETE that was previously required.

---

## Scenario 2: BigQuery Partitioned Table for Cost Control

**Business context:** A data analytics team's BigQuery costs are $50,000/month, mostly from dashboard queries that scan full 5TB tables. The solution: add date partitioning + clustering and enforce partition filters to prevent full-table scans.

```sql
-- Create the partitioned and clustered fact table:
CREATE OR REPLACE TABLE `analytics.fact_ad_impressions`
PARTITION BY DATE(impression_date)
CLUSTER BY campaign_id, placement_id
OPTIONS (
    partition_expiration_days = 365,      -- Auto-delete partitions after 1 year
    require_partition_filter = TRUE        -- ERROR if query doesn't filter on impression_date
)
AS
SELECT * FROM `analytics.raw_ad_impressions`;
-- Migration runs as a CREATE TABLE AS SELECT — atomic, no downtime

-- Verify partition information:
SELECT table_name, partition_id, total_rows, total_logical_bytes
FROM `analytics.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fact_ad_impressions'
ORDER BY partition_id DESC
LIMIT 10;
```

**Dashboard query patterns that benefit:**

```sql
-- Before: full table scan (5TB scanned → ~$25 per query)
SELECT campaign_id, SUM(impressions), SUM(clicks)
FROM `analytics.raw_ad_impressions`  -- Old unpartitioned table
WHERE impression_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY campaign_id;

-- After: partition-pruned + clustered (scans only January partition)
SELECT campaign_id, SUM(impressions), SUM(clicks)
FROM `analytics.fact_ad_impressions`
WHERE impression_date BETWEEN '2024-01-01' AND '2024-01-31'  -- Required!
GROUP BY campaign_id;
-- Scans: ~150GB (1/34 of total) → $0.75 per query
-- Monthly cost reduction: 34× less data scanned
```

**Cost monitoring setup:**

```sql
-- BigQuery: track bytes processed per user/query (weekly report)
SELECT 
    user_email,
    COUNT(*) AS queries_run,
    SUM(total_bytes_processed) / POW(1024, 4) AS total_tb_scanned,
    SUM(total_bytes_processed) / POW(1024, 4) * 5 AS estimated_cost_usd  -- $5/TB
FROM `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND state = 'DONE'
  AND job_type = 'QUERY'
GROUP BY user_email
ORDER BY total_tb_scanned DESC;
```

**Result after implementing partitioning:** Monthly BigQuery costs dropped from $50,000 to $8,000 (84% reduction). The `require_partition_filter` option prevented accidental full-table scans — engineers who forgot the date filter got an explicit error rather than a surprise $25 query.

---

## Scenario 3: Multi-Tenant Database Partitioning with Row Level Security

**Business context:** A SaaS company runs a PostgreSQL database for 50,000 business customers. The `transactions` table has 20 billion rows. Customer-facing API queries take 8+ seconds for large customers. The solution: hash partition by customer_id so each customer's data is in 1 of 64 partitions, with per-partition indexes that are 64× smaller.

```sql
-- Create hash-partitioned table:
CREATE TABLE transactions (
    transaction_id   BIGSERIAL,
    customer_id      INT NOT NULL,
    amount           NUMERIC(12,2),
    transaction_date DATE,
    status           TEXT,
    metadata         JSONB,
    PRIMARY KEY (transaction_id, customer_id)  -- PK must include partition key
) PARTITION BY HASH (customer_id);

-- Automate partition creation:
DO $$
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE transactions_p%s PARTITION OF transactions 
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
END $$;

-- Create indexes (inherited by all 64 partitions):
-- Index 1: customer queries (most common — API calls)
CREATE INDEX idx_txn_customer_date 
    ON transactions (customer_id, transaction_date DESC)
    INCLUDE (amount, status);  -- Covering index avoids heap fetches

-- Index 2: analytics queries (less common)
CREATE INDEX idx_txn_date_status ON transactions (transaction_date, status);
```

**Row Level Security to enforce isolation:**

```sql
ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;

CREATE POLICY customer_data_isolation ON transactions
    FOR ALL
    TO api_app_role
    USING (customer_id = current_setting('app.customer_id')::INT);

-- Application connection setup:
-- Before each query: SET LOCAL app.customer_id = '12345';

-- Resulting query plan for a customer request:
EXPLAIN ANALYZE
SELECT transaction_id, amount, transaction_date, status
FROM transactions
WHERE transaction_date >= '2024-01-01'
ORDER BY transaction_date DESC
LIMIT 50;
-- With SET LOCAL app.customer_id = '12345':
-- customer_id = 12345 % 64 = 57 → Only transactions_p57 scanned
-- Within p57: Index Scan using idx_txn_customer_date_p57 (small index for this partition)
-- Execution time: 4ms (vs. 8000ms before partitioning)
```

**Operational benefits realized:**
- Per-partition vacuum: autovacuum runs against individual partitions concurrently, keeping up with inserts without blocking
- Index size: 64 indexes at ~3GB each vs. one 200GB index — much more cache-friendly
- Partition statistics: each partition has independent statistics, so the optimizer makes better estimates for specific customers
- Future sharding: when the single server maxes out, each partition can become a shard on a separate server with Citus or application-level routing

---

## Interview Tips

> **Tip 1:** "How do you migrate a 10TB table to partitioned without downtime?" — "The key is doing it in phases: first create the new partitioned structure alongside the existing table, then backfill month-by-month with committed INSERT SELECTs (so you're not holding one massive transaction), then sync the delta (new rows during backfill), and finally do a brief atomic rename. The rename is the only moment where writes are blocked — keep it under 5 seconds by ensuring the delta sync is complete before locking. After cutover, validate for a week before dropping the backup table."

> **Tip 2:** "How does `require_partition_filter = TRUE` in BigQuery help with cost control?" — "It prevents queries from scanning the entire table by requiring a WHERE clause filter on the partition column (usually the date). If a developer runs a query without a date filter — which would scan the entire multi-TB table and cost hundreds of dollars — BigQuery returns an error instead of executing. This is a guardrail that catches accidental full-table scans before they hit the bill. You can override it per-query when genuinely needed with a date range that covers the full table."

> **Tip 3:** "How do you monitor partition health in production?" — "I track four metrics: (1) partition size distribution — flag any partition >2× the average (data skew), (2) new rows going to the default partition — means partition creation is behind or data has unexpected values, (3) vacuum lag per partition — tables with high dead tuple ratios need autovacuum parameter tuning, (4) partition pruning effectiveness — check that queries are actually using pruning via EXPLAIN. I set up alerts in Datadog or Grafana using pg_stat_user_tables and pg_relation_size."
