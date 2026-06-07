---
title: "Time Travel - Scenario Questions"
topic: snowflake
subtopic: time-travel
content_type: scenario_question
tags: [snowflake, time-travel, interview, scenarios]
---

# Scenario Questions — Time Travel

<article data-difficulty="junior">

## 🟢 Junior: Basic Data Recovery

**Scenario:** A developer accidentally ran `DELETE FROM production.orders WHERE amount > 100` (meant to delete test data, not production orders!). 500K rows are gone. Recover the data using Time Travel.

<details>
<summary>💡 Hint</summary>
Find the DELETE query ID in QUERY_HISTORY, then INSERT back the rows from the table state BEFORE that statement.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Find the bad query
SELECT query_id, query_text, start_time, rows_affected
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%DELETE%production.orders%'
ORDER BY start_time DESC LIMIT 5;
-- Found: query_id = '01abc-bad-delete-query'

-- Step 2: Verify the data exists in Time Travel
SELECT COUNT(*) FROM production.orders BEFORE (STATEMENT => '01abc-bad-delete-query');
-- Shows: 500K more rows than current table (the deleted ones!)

-- Step 3: Insert the deleted rows back
INSERT INTO production.orders
SELECT * FROM production.orders BEFORE (STATEMENT => '01abc-bad-delete-query')
WHERE order_id NOT IN (SELECT order_id FROM production.orders);
-- Inserts ONLY the rows that are missing (the deleted ones)
-- Won't create duplicates (NOT IN check)

-- Step 4: Verify recovery
SELECT COUNT(*) FROM production.orders WHERE amount > 100;
-- Should match the count before the accidental DELETE

-- Alternative (simpler if you want to restore the ENTIRE table):
CREATE OR REPLACE TABLE production.orders
CLONE production.orders BEFORE (STATEMENT => '01abc-bad-delete-query');
-- Replaces entire table with pre-DELETE version
-- WARNING: any legitimate changes AFTER the DELETE are also lost!
```

**Key Points:**
- `BEFORE (STATEMENT => query_id)`: shows table state RIGHT BEFORE that query ran
- INSERT...WHERE NOT IN: only adds back deleted rows (safe, no duplicates)
- Alternative CLONE approach: simpler but loses any changes made AFTER the incident
- Time Travel works within the retention period only (default 1 day, up to 90)
- This entire recovery takes < 5 minutes!

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: UNDROP a Table

**Scenario:** Someone accidentally dropped `production.customers`. The table had 5 million rows. The application is showing errors. Recover it immediately.

<details>
<summary>💡 Hint</summary>
UNDROP TABLE restores a dropped table (within retention period). One command, instant recovery.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Instant recovery:
UNDROP TABLE production.customers;
-- Done! Table is back with all 5 million rows, all permissions, all constraints.
-- Takes < 1 second (just restores metadata pointer to existing micro-partitions).

-- Verify:
SELECT COUNT(*) FROM production.customers;  -- Should be 5 million

-- What if someone already created a NEW table with the same name?
-- (e.g., they tried to recreate it manually)
-- Error: "Object 'PRODUCTION.CUSTOMERS' already exists"

-- Fix: rename the new table first, then UNDROP
ALTER TABLE production.customers RENAME TO production.customers_new;
UNDROP TABLE production.customers;  -- Now works!
-- Compare: old (undropped) vs new (manually recreated)
-- Drop whichever is wrong

-- ALSO works for:
UNDROP SCHEMA production;
UNDROP DATABASE analytics;
-- Restores entire schema/database with all objects inside!
```

**Key Points:**
- UNDROP is instant (metadata operation, no data copy)
- Works within DATA_RETENTION_TIME_IN_DAYS of the DROP
- Restores: data, schema, constraints, statistics — everything
- Name conflict: rename the conflicting object first, then UNDROP
- Also works for schemas and databases (restores all child objects!)
- This is why Time Travel is invaluable — DROP mistakes take seconds to fix

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Point-in-Time Comparison

**Scenario:** Revenue numbers in yesterday's dashboard don't match today's. Someone may have backdated orders or modified historical data. Use Time Travel to find what changed in the orders table between yesterday and today.

<details>
<summary>💡 Hint</summary>
Compare today's data vs yesterday's snapshot using a FULL OUTER JOIN on order_id. Flag rows as INSERTED, DELETED, or UPDATED based on presence in each version.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Compare: what's different between yesterday's snapshot and current state?
WITH 
yesterday AS (
    SELECT order_id, customer_id, amount, order_date, status
    FROM production.orders
    AT (TIMESTAMP => DATEADD('day', -1, CURRENT_DATE())::TIMESTAMP_LTZ)
    WHERE order_date BETWEEN '2024-03-01' AND '2024-03-14'  -- March data
),
today AS (
    SELECT order_id, customer_id, amount, order_date, status
    FROM production.orders
    WHERE order_date BETWEEN '2024-03-01' AND '2024-03-14'  -- Same date range
)
SELECT 
    COALESCE(t.order_id, y.order_id) AS order_id,
    CASE 
        WHEN y.order_id IS NULL THEN 'INSERTED (new row added to historical range)'
        WHEN t.order_id IS NULL THEN 'DELETED (historical row removed)'
        WHEN t.amount != y.amount THEN 'AMOUNT_CHANGED'
        WHEN t.status != y.status THEN 'STATUS_CHANGED'
        ELSE 'UNCHANGED'
    END AS change_type,
    y.amount AS yesterday_amount,
    t.amount AS today_amount,
    y.status AS yesterday_status,
    t.status AS today_status
FROM today t
FULL OUTER JOIN yesterday y ON t.order_id = y.order_id
WHERE y.order_id IS NULL 
   OR t.order_id IS NULL
   OR t.amount != y.amount 
   OR t.status != y.status
ORDER BY change_type, order_id;

-- FINDINGS:
-- | order_id | change_type | yesterday_amount | today_amount |
-- | 5001 | INSERTED | NULL | 5000.00 | ← backdated order!
-- | 5002 | AMOUNT_CHANGED | 100.00 | 150.00 | ← amount modified!
-- | 5003 | DELETED | 200.00 | NULL | ← order removed!

-- This explains the revenue discrepancy!
-- Action: investigate who made these changes (query QUERY_HISTORY for UPDATEs on these order_ids)
```

**Key Points:**
- FULL OUTER JOIN between current and historical: catches inserts, deletes, AND updates
- Filter to the date range in question (don't compare entire table — too slow)
- This identifies EXACTLY which rows changed and how
- Next step: check QUERY_HISTORY to find WHO made the changes and WHEN
- Use for: audit investigations, dashboard discrepancy debugging, data quality verification

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Zero-Copy Clone for Testing

**Scenario:** Your QA team needs a copy of production data (500 GB) to test a new ETL pipeline. They need it within 5 minutes, can't wait for a 6-hour data copy, and shouldn't see real customer PII. Design the solution.

<details>
<summary>💡 Hint</summary>
CLONE production database (instant, zero-copy). Then mask PII columns (UPDATE email/phone to fake values). Only the PII columns consume extra storage.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Clone production database (INSTANT, regardless of size!)
CREATE DATABASE qa_testing CLONE production;
-- 500 GB database cloned in < 5 seconds!
-- Zero additional storage (shares micro-partitions with production)

-- Step 2: Mask PII (so QA team doesn't see real customer data)
UPDATE qa_testing.production.customers SET
    email = 'user_' || customer_id || '@test.com',
    phone = '555-' || LPAD(MOD(customer_id, 10000), 4, '0'),
    name = 'Test Customer ' || customer_id;
-- Only these 3 columns create new storage (~5% of table size)
-- All other data (orders, amounts, dates) remains shared with production (zero cost)

-- Step 3: Grant access to QA team
GRANT USAGE ON DATABASE qa_testing TO ROLE qa_team;
GRANT USAGE ON ALL SCHEMAS IN DATABASE qa_testing TO ROLE qa_team;
GRANT SELECT ON ALL TABLES IN DATABASE qa_testing TO ROLE qa_team;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE qa_testing TO ROLE qa_team;
-- QA can modify their copy without affecting production!

-- Step 4: After testing is complete, clean up
DROP DATABASE qa_testing;
-- Frees any extra storage from QA's modifications
-- Production is completely unaffected

-- TIMELINE:
-- 0:00 - Request received
-- 0:01 - CLONE command (instant)
-- 0:03 - PII masking (2 minutes for 5M customer rows)
-- 0:04 - Grants applied
-- 0:05 - QA team has access! ✓

-- STORAGE COST:
-- Clone creation: $0 (zero-copy)
-- PII masking (new partitions for 3 columns): ~25 GB = ~$0.58/month
-- QA modifications during testing: minimal
-- After DROP: back to $0
```

**Key Points:**
- CLONE is instant regardless of data size (500 GB, 5 TB — same speed)
- Zero-copy: no data duplicated until modifications are made
- PII masking: UPDATE only the sensitive columns (minimal extra storage)
- QA team has full read/write access to their copy (isolated from production)
- Clean up: DROP DATABASE after testing (frees all extra storage)
- Total time: < 5 minutes (vs 6+ hours for traditional copy)
- Total cost: ~$1/month (vs $11.50/month for a full 500 GB copy)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Time Travel Storage Optimization

**Scenario:** Your Snowflake bill shows $8K/month in Time Travel storage. Analysis reveals: a 200 GB fact table with 30-day retention does daily INSERT OVERWRITE (full refresh), creating 200 GB × 30 days = 6 TB of time travel data. Reduce to under $2K/month without losing recovery capability.

<details>
<summary>💡 Hint</summary>
Solutions: switch INSERT OVERWRITE to incremental MERGE (less version history), reduce retention (30→7 days), or use TRANSIENT for the staging copy. The root cause is full refresh creating a complete table copy every day.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- PROBLEM: INSERT OVERWRITE creates full table copy in TT every day
-- 200 GB × 30 days retention = 6 TB time travel = ~$138/month per TB = $828/month
-- Wait, that's only one table. Total across many tables = $8K/month

-- SOLUTION 1: Switch from INSERT OVERWRITE to MERGE (biggest impact!)
-- BEFORE (full refresh — creates 200 GB new version daily):
INSERT OVERWRITE INTO production.fact_orders
SELECT * FROM staging.transformed_orders;
-- Every day: 200 GB of new partitions, old 200 GB retained in TT

-- AFTER (incremental MERGE — only changed rows create new partitions):
MERGE INTO production.fact_orders t
USING staging.new_orders s ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
-- Only ~5 GB of changes/day → TT stores 5 GB × 30 days = 150 GB (not 6 TB!)

-- Impact: 6 TB → 150 GB time travel = 97% reduction for this table!

-- SOLUTION 2: Reduce retention where appropriate
ALTER TABLE production.fact_orders SET DATA_RETENTION_TIME_IN_DAYS = 7;
-- 7 days is still sufficient for recovery (most issues caught within a week)
-- Impact: 30 days → 7 days = 77% reduction in stored versions

-- SOLUTION 3: Use TRANSIENT for staging/intermediate tables
CREATE OR REPLACE TRANSIENT TABLE staging.transformed_orders (...);
ALTER TABLE staging.transformed_orders SET DATA_RETENTION_TIME_IN_DAYS = 0;
-- Staging tables: no time travel, no fail-safe (they're rebuilt daily anyway)
-- Impact: eliminates TT storage for all staging tables

-- COMBINED RESULT:
-- Before: 6 TB time travel for fact table = $138/month
-- After (MERGE + 7 days): 5 GB/day × 7 days = 35 GB = $0.81/month
-- Savings on this one table: $137/month (99.4% reduction!)

-- Apply across all tables with similar patterns:
-- Total savings: $8K → ~$1.5K/month (81% reduction!) ✓

-- MONITORING (prevent regression):
SELECT 
    TABLE_NAME,
    ACTIVE_BYTES / POWER(1024,3) AS active_gb,
    TIME_TRAVEL_BYTES / POWER(1024,3) AS tt_gb,
    TIME_TRAVEL_BYTES / NULLIF(ACTIVE_BYTES, 0) AS tt_ratio
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TIME_TRAVEL_BYTES > 10 * POWER(1024,3)  -- > 10 GB TT storage
ORDER BY TIME_TRAVEL_BYTES DESC;
-- Alert if tt_ratio > 5 (TT is 5x the active data → likely full refresh pattern)
```

**Key Points:**
- Root cause: INSERT OVERWRITE creates a FULL table copy in TT every refresh
- Fix: MERGE (incremental) → only changed partitions stored in TT (95%+ reduction)
- Reduce retention: 30→7 days is sufficient for most recovery scenarios
- TRANSIENT tables for staging: zero TT storage (they're rebuilt daily anyway)
- Monitor: track TT-to-active ratio; ratio > 5x indicates full refresh pattern
- Combined savings: typically 70-90% reduction in TT storage costs

</details>

</article>
