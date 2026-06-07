---
title: "Time Travel - Intermediate"
topic: snowflake
subtopic: time-travel
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, time-travel, cloning, auditing, retention, optimization]
---

# Snowflake Time Travel — Intermediate

## Zero-Copy Cloning with Time Travel

Cloning creates a **metadata-only copy** (no data duplication until changes are made):

```sql
-- Clone current state (instant, zero storage cost initially):
CREATE TABLE dev.orders CLONE production.orders;
-- dev.orders points to SAME micro-partitions as production.orders
-- No data copied! Just metadata pointers.
-- Storage cost: $0 (until you modify dev.orders)

-- Clone at historical point (development against yesterday's data):
CREATE TABLE dev.orders_yesterday
CLONE production.orders AT (TIMESTAMP => DATEADD('day', -1, CURRENT_TIMESTAMP()));
-- Instant clone of yesterday's data — work with it without affecting production!

-- Clone entire schema (all tables):
CREATE SCHEMA dev.testing CLONE production;
-- All tables, views, stages — cloned in seconds (regardless of data size!)

-- Clone entire database:
CREATE DATABASE dev_snapshot CLONE production AT (TIMESTAMP => '2024-03-15 00:00:00');
-- Full database snapshot for testing — zero storage until you modify
```

### Clone Storage Cost

```sql
-- Storage math:
-- Clone at creation: 0 bytes (shares partitions with source)
-- After INSERT into clone: only NEW partitions stored (delta)
-- After UPDATE: old partitions diverge (both source and clone keep their versions)

-- Check clone storage:
SELECT TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, RETAINED_FOR_CLONE_BYTES
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'DEV';
-- RETAINED_FOR_CLONE_BYTES: data kept because a clone still references it
```

---

## Data Auditing with Time Travel

```sql
-- Audit: what changed in the orders table in the last 24 hours?

-- Method 1: Compare current vs 24-hours-ago
SELECT 
    COALESCE(c.order_id, h.order_id) AS order_id,
    CASE 
        WHEN h.order_id IS NULL THEN 'INSERTED'
        WHEN c.order_id IS NULL THEN 'DELETED'
        WHEN c.amount != h.amount OR c.status != h.status THEN 'UPDATED'
        ELSE 'UNCHANGED'
    END AS change_type,
    h.amount AS old_amount, c.amount AS new_amount,
    h.status AS old_status, c.status AS new_status
FROM production.orders c
FULL OUTER JOIN production.orders AT (OFFSET => -86400) h  -- 24 hours ago
    ON c.order_id = h.order_id
WHERE c.order_id IS NULL OR h.order_id IS NULL 
   OR c.amount != h.amount OR c.status != h.status;
-- Shows: what was inserted, deleted, or updated in the last 24 hours

-- Method 2: Use CHANGES clause (more efficient for streams-compatible tables)
SELECT * FROM production.orders
CHANGES (INFORMATION => DEFAULT)
AT (TIMESTAMP => DATEADD('hour', -24, CURRENT_TIMESTAMP()));
-- Returns: all changes with METADATA$ACTION and METADATA$ISUPDATE columns
```

---

## Point-in-Time Reporting

```sql
-- Business requirement: "Show me revenue as of end-of-month (March 31 midnight)"
-- Even if someone updated March data in April, I need the March 31 view

SELECT 
    region,
    SUM(amount) AS march_revenue,
    COUNT(*) AS march_orders
FROM production.orders 
AT (TIMESTAMP => '2024-03-31 23:59:59'::TIMESTAMP_LTZ)
WHERE order_date BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY region;
-- Returns: revenue numbers as they were on March 31 at midnight
-- Even if corrections were made in April, this shows the original month-end numbers!

-- Pattern: create periodic snapshots for regulatory reporting:
CREATE TABLE reporting.orders_q1_2024 
CLONE production.orders AT (TIMESTAMP => '2024-03-31 23:59:59'::TIMESTAMP_LTZ);
-- Permanent snapshot of Q1 data (doesn't expire like Time Travel)
-- Use for: SOX compliance, auditor requests, regulatory filings
```

---

## Retention Strategy by Table Type

```sql
-- Production tables (critical, need recovery capability):
ALTER TABLE production.orders SET DATA_RETENTION_TIME_IN_DAYS = 30;
-- 30 days: plenty of time to notice and fix issues

-- Staging/ETL tables (temporary, no recovery needed):
ALTER TABLE staging.raw_load SET DATA_RETENTION_TIME_IN_DAYS = 1;
-- 1 day: minimal storage cost, basic recovery if today's load fails

-- Development/test tables (experiment freely):
ALTER TABLE dev.test_orders SET DATA_RETENTION_TIME_IN_DAYS = 0;
-- 0 days: no time travel, no storage overhead (DROP = permanently gone)

-- Large fact tables (expensive to retain full history):
ALTER TABLE production.events SET DATA_RETENTION_TIME_IN_DAYS = 7;
-- 7 days: balance between recovery capability and storage cost
-- 100 GB table × 7 days retention × daily full refresh = 700 GB time travel storage!

-- Transient tables (no fail-safe):
CREATE TRANSIENT TABLE staging.temp (...);
-- Time travel: 0 or 1 day max; No 7-day fail-safe (saves storage!)
```

---

## Recovering from Common Accidents

### Accidental DELETE

```sql
-- Someone ran: DELETE FROM production.orders WHERE status = 'pending';
-- (Should have been: WHERE status = 'cancelled')
-- Now all pending orders are gone!

-- Step 1: Find the query that caused the problem
SELECT query_id, query_text, start_time, rows_affected
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%DELETE%orders%'
ORDER BY start_time DESC LIMIT 5;
-- Found: query_id = '01abc...'

-- Step 2: Restore the deleted rows
INSERT INTO production.orders
SELECT * FROM production.orders BEFORE (STATEMENT => '01abc...')
WHERE order_id NOT IN (SELECT order_id FROM production.orders);
-- Inserts back ONLY the rows that were deleted (not already present)

-- Step 3: Verify
SELECT COUNT(*) FROM production.orders WHERE status = 'pending';
-- Should show the pending orders are back!
```

### Accidental DROP TABLE

```sql
-- Someone dropped the wrong table!
DROP TABLE production.orders;  -- Disaster!

-- Fix (within retention period):
UNDROP TABLE production.orders;
-- Table is back with ALL data, indexes, and permissions!

-- If the table name was reused (new table with same name):
-- Can't UNDROP directly (name conflict)
-- Fix: rename new table, UNDROP old one
ALTER TABLE production.orders RENAME TO production.orders_new;
UNDROP TABLE production.orders;  -- Now works (no name conflict)
```

### Bad UPDATE (Corrupted Data)

```sql
-- Someone ran: UPDATE production.orders SET amount = 0;  (missing WHERE clause!)
-- ALL amounts are now 0!

-- Fix: swap with a clone from before the update
CREATE TABLE production.orders_fixed
CLONE production.orders BEFORE (STATEMENT => 'bad_update_query_id');

-- Verify the fix:
SELECT AVG(amount) FROM production.orders_fixed;  -- Should be non-zero

-- Swap:
ALTER TABLE production.orders SWAP WITH production.orders_fixed;
-- production.orders now has the correct data!

DROP TABLE production.orders_fixed;  -- Clean up the corrupted version
```

---

## Time Travel and Storage Optimization

```sql
-- Find tables with excessive time travel storage:
SELECT 
    TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
    ACTIVE_BYTES / POWER(1024, 3) AS active_gb,
    TIME_TRAVEL_BYTES / POWER(1024, 3) AS time_travel_gb,
    (TIME_TRAVEL_BYTES::FLOAT / NULLIF(ACTIVE_BYTES, 0)) * 100 AS tt_pct_of_active
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TIME_TRAVEL_BYTES > 0
ORDER BY TIME_TRAVEL_BYTES DESC
LIMIT 20;
-- Tables where time_travel_gb >> active_gb are expensive to retain!

-- Common causes of high time travel storage:
-- 1. Full table refresh daily (INSERT OVERWRITE): creates N full copies in retention
-- 2. Frequent UPDATEs on large tables: many micro-partition versions
-- 3. Over-generous retention (30 days on a 1TB table = 30TB of history!)

-- Fixes:
-- 1. Reduce retention for high-churn tables: 30 → 7 days
-- 2. Use TRANSIENT for ETL staging tables: no fail-safe
-- 3. Consider APPEND-only pattern instead of INSERT OVERWRITE
-- 4. Use Dynamic Tables (Snowflake manages old versions efficiently)
```

---

## Interview Tips

> **Tip 1:** "How do you use Time Travel for data recovery?" — Three common patterns: (1) UNDROP for dropped tables, (2) CLONE AT timestamp + SWAP for corrupted data, (3) INSERT from historical point for selective row recovery. All are instant (metadata operations), no need for backups.

> **Tip 2:** "Time Travel storage impact?" — Old micro-partitions retained for retention_days. High-churn tables (daily full refresh) accumulate significant storage. Optimization: reduce retention for non-critical tables, use TRANSIENT for staging, prefer incremental loads over full refresh (less version history).

> **Tip 3:** "Time Travel vs Streams?" — Time Travel: access data AT any past point (on-demand, any timestamp). Streams: track CHANGES since last consumption (continuous, offset-based). They use the same underlying mechanism (micro-partition versioning) but serve different purposes. Time Travel = ad-hoc recovery/audit. Streams = automated incremental ETL.
