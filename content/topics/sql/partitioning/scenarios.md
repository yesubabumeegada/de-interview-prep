---
title: "SQL Partitioning - Scenario Questions"
topic: sql
subtopic: partitioning
content_type: scenario_question
tags: [sql, partitioning, interview, scenarios, partition-pruning, range-partition, bigquery]
---

# Scenario Questions — SQL Partitioning

<article data-difficulty="junior">

## 🟢 Junior: Design a Partitioned Table for Order History

**Scenario:** You're designing a data warehouse table to store e-commerce orders. The table will have ~500 million rows per year, and queries almost always filter by `order_date`. Analysts regularly need data for specific months or quarters. Old data (>2 years) needs to be purged quarterly. Design the table with appropriate partitioning.

<details>
<summary>💡 Hint</summary>

Range partitioning on `order_date` is the natural choice — queries filter by date range, and dropping old partitions (instead of running DELETE) is the efficient way to handle retention. Choose a partition granularity that balances the number of partitions against query granularity.

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Design: monthly range partitions on order_date
-- Reasoning: 
-- - Queries filter by month/quarter → monthly partitions provide good pruning
-- - 2 years retention = 24 active partitions (manageable)
-- - Monthly DROP is instant vs. quarterly DELETE of 500M rows

CREATE TABLE fact_orders (
    order_id      BIGINT NOT NULL,
    customer_id   INT NOT NULL,
    product_id    INT NOT NULL,
    amount        NUMERIC(10,2),
    order_date    DATE NOT NULL,
    status        TEXT,
    region        TEXT,
    loaded_at     TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (order_date);

-- Create 24 months of partitions (2 years):
CREATE TABLE fact_orders_2023_01 PARTITION OF fact_orders FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE fact_orders_2023_02 PARTITION OF fact_orders FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
-- ... create all 24 monthly partitions ...
CREATE TABLE fact_orders_2024_12 PARTITION OF fact_orders FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- Always create 3 future partitions in advance:
CREATE TABLE fact_orders_2025_01 PARTITION OF fact_orders FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE fact_orders_2025_02 PARTITION OF fact_orders FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE fact_orders_2025_03 PARTITION OF fact_orders FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

-- Default partition to catch unexpected data:
CREATE TABLE fact_orders_default PARTITION OF fact_orders DEFAULT;

-- Create indexes (inherited by all partitions in PG11+):
CREATE INDEX ON fact_orders (order_date, customer_id);   -- Date + customer lookup
CREATE INDEX ON fact_orders (region, order_date);        -- Regional reporting

-- Verify partition pruning:
EXPLAIN SELECT SUM(amount) FROM fact_orders
WHERE order_date >= '2024-Q1' AND order_date < '2024-04-01';
-- Should show: only 3 partitions scanned (Jan, Feb, Mar 2024)

-- Retention: every quarter, drop 3 old partitions:
DROP TABLE fact_orders_2022_10;
DROP TABLE fact_orders_2022_11;
DROP TABLE fact_orders_2022_12;
-- Instant! No DELETE needed.
```

**Alternative consideration — daily vs. monthly:**
- Daily partitions: better pruning for single-day queries, but 730 partitions for 2 years (high planning overhead)
- Monthly partitions: 24 partitions, negligible planning overhead, good for month/quarter queries
- **Choose monthly** for this use case given the analyst query patterns

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Diagnose Why Partition Pruning Is Not Working

**Scenario:** A team set up monthly partitioning on an `events` table by `event_date`. They run this query and the EXPLAIN shows all 24 partitions are being scanned instead of just the expected 1:

```sql
EXPLAIN SELECT * FROM events WHERE EXTRACT(MONTH FROM event_date) = 1;
-- Expected: scan only January partition
-- Actual: all 24 partitions scanned
```

Explain why pruning fails and provide the corrected query.

<details>
<summary>💡 Hint</summary>

The partition key is `event_date` (a DATE column). The WHERE clause uses `EXTRACT(MONTH FROM event_date)` — this wraps the partition key in a function. The optimizer can't determine which partition boundaries match `EXTRACT(MONTH ...) = 1` without evaluating the function on every row in every partition.

</details>

<details>
<summary>✅ Solution</summary>

**Why pruning fails:**

The query `WHERE EXTRACT(MONTH FROM event_date) = 1` applies a function (`EXTRACT`) to the partition key column. The optimizer cannot determine partition boundaries from this — it would need to evaluate `EXTRACT(MONTH FROM ...)` for every row in every partition to know if it's January.

The partition boundaries are DATE ranges like:
- `events_2024_01`: `FROM '2024-01-01' TO '2024-02-01'`
- `events_2024_02`: `FROM '2024-02-01' TO '2024-03-01'`
- ...

`EXTRACT(MONTH FROM event_date) = 1` could match January of ANY year — so the optimizer must scan all partitions.

**Fixed queries:**

```sql
-- Fix 1: Explicit date range for a specific January
SELECT * FROM events
WHERE event_date >= '2024-01-01' AND event_date < '2024-02-01';
-- EXPLAIN: scans only events_2024_01 ✅

-- Fix 2: Multiple years of January data
SELECT * FROM events
WHERE (event_date >= '2023-01-01' AND event_date < '2023-02-01')
   OR (event_date >= '2024-01-01' AND event_date < '2024-02-01');
-- EXPLAIN: scans events_2023_01 and events_2024_01 only ✅

-- Fix 3: Use BETWEEN (inclusive but equivalent for dates)
SELECT * FROM events
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31';
-- EXPLAIN: scans only events_2024_01 ✅

-- Verify pruning works with EXPLAIN:
EXPLAIN SELECT * FROM events WHERE event_date >= '2024-01-01' AND event_date < '2024-02-01';
-- Look for: "Seq Scan on events_2024_01" — only ONE partition listed
```

**General rule:** Never wrap a partition key in a function in the WHERE clause. Use range conditions directly on the partition key column.

| Pattern | Pruning? |
|---------|---------|
| `WHERE event_date = '2024-01-15'` | ✅ Yes |
| `WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'` | ✅ Yes |
| `WHERE event_date >= '2024-01-01' AND event_date < '2024-04-01'` | ✅ Yes |
| `WHERE EXTRACT(MONTH FROM event_date) = 1` | ❌ No |
| `WHERE DATE_TRUNC('month', event_date) = '2024-01-01'` | ❌ No |
| `WHERE YEAR(event_date) = 2024` | ❌ No |

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Choose the Right Partition Type

**Scenario:** For each of the following tables, choose the best partitioning strategy (range, list, hash, or composite) and explain your reasoning:

1. **`web_logs`** — 500GB/day. Queries always filter by `log_date`. Logs older than 30 days are deleted.
2. **`user_profiles`** — 100M users across 3 major regions (US, EU, APAC). Most queries filter by `region` for compliance (GDPR: EU data must stay in EU).
3. **`transactions`** — 50B rows. No consistent filter pattern — queries vary by customer, by date, by amount range.
4. **`product_catalog`** — 2 million rows. Rarely needs partitioning but team is considering it.

<details>
<summary>💡 Hint</summary>

Match the partition type to the query patterns: Range works for continuous values with range filters. List works for discrete categories. Hash distributes evenly when there's no natural query filter on a specific column. And sometimes the answer is "don't partition."

</details>

<details>
<summary>✅ Solution</summary>

**1. `web_logs` → Range partitioning by `log_date` (daily)**

```sql
CREATE TABLE web_logs (...) PARTITION BY RANGE (log_date);
-- Daily partitions: DROP TABLE web_logs_2024_01_15 (instant)
-- Alternative to: DELETE FROM web_logs WHERE log_date < '2024-01-15' (hours!)
-- Pruning: queries for a specific day scan only 1 of 30 active partitions
-- Granularity: daily (not monthly) because logs are deleted after 30 days
-- → only 30 active partitions, daily granularity maximizes pruning for single-day queries
```

**2. `user_profiles` → List partitioning by `region`**

```sql
CREATE TABLE user_profiles (...) PARTITION BY LIST (region);
CREATE TABLE user_profiles_us   PARTITION OF user_profiles FOR VALUES IN ('US');
CREATE TABLE user_profiles_eu   PARTITION OF user_profiles FOR VALUES IN ('EU', 'UK', 'DE', 'FR', ...);
CREATE TABLE user_profiles_apac PARTITION OF user_profiles FOR VALUES IN ('JP', 'AU', 'SG', ...);
-- GDPR compliance: EU partition can be on EU-region storage, or with different backup policies
-- Pruning: WHERE region = 'EU' scans only user_profiles_eu (1 of 3 partitions)
-- Access control: GRANT SELECT ON user_profiles_eu TO eu_analytics_role only
```

**3. `transactions` → Hash partitioning by `customer_id` (or no partitioning + good indexing)**

```sql
-- If queries vary by customer: hash partition by customer_id
CREATE TABLE transactions (...) PARTITION BY HASH (customer_id);
-- Each partition is 1/N of the data; per-customer queries hit 1 partition
-- Per-partition index is N× smaller → fits in cache better

-- If no dominant filter pattern: consider whether partitioning actually helps
-- Without a consistent partition key filter, queries scan all partitions anyway
-- Better: good indexing strategy + table clustering by most-common access pattern
-- Alternative: use Snowflake or BigQuery with columnar storage
```

**4. `product_catalog` → No partitioning**

```sql
-- 2 million rows = approximately 1-2GB
-- At this size, indexes are extremely efficient
-- Table fits entirely in buffer cache → queries are sub-millisecond
-- Adding partitioning: complexity, FK limitations, no performance benefit
-- Decision: use indexes, not partitioning
CREATE INDEX idx_products_category ON product_catalog(category_id);
CREATE INDEX idx_products_sku ON product_catalog(sku);
-- These will be faster than any partitioned approach for 2M rows
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handle Partition Key Updates

**Scenario:** Your `orders` table is partitioned monthly by `order_date`. A business rule requires that orders in "pending" status can have their `order_date` corrected. What happens when you UPDATE the `order_date` and it moves the row to a different partition? How do you handle this?

<details>
<summary>💡 Hint</summary>

In PostgreSQL, updating a partition key value that crosses a partition boundary automatically moves the row to the correct partition. But this behavior has important performance implications and version requirements. Consider whether the design allows this or should restrict it.

</details>

<details>
<summary>✅ Solution</summary>

**PostgreSQL behavior (PG11+):**

```sql
-- In PostgreSQL 11+, UPDATE on partition key is supported:
UPDATE orders 
SET order_date = '2024-02-15'  -- Moving from January partition to February
WHERE order_id = 12345 AND status = 'pending';

-- PostgreSQL internally does:
-- 1. DELETE from orders_2024_01 (old partition)
-- 2. INSERT into orders_2024_02 (new partition)
-- This is an implicit DELETE + INSERT under the hood

-- Verify the row moved:
EXPLAIN SELECT * FROM orders WHERE order_id = 12345;
-- Should show orders_2024_02 is scanned (not orders_2024_01)
```

**Performance implications:**

```sql
-- Cross-partition UPDATE = DELETE + INSERT = more WAL, more index maintenance
-- For a table with many indexes: each cross-partition UPDATE updates ALL indexes twice
-- High volume of cross-partition updates: consider whether the design is correct

-- How to track cross-partition updates (pg_stat_user_tables):
SELECT relname, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd
FROM pg_stat_user_tables
WHERE relname LIKE 'orders_%'
ORDER BY relname;
-- High n_tup_ins + n_tup_del relative to n_tup_upd = cross-partition moves happening
```

**Design alternatives if cross-partition updates are frequent:**

```sql
-- Option 1: Don't partition on a mutable column
-- Use an immutable created_at instead of mutable order_date for partitioning
CREATE TABLE orders (...) PARTITION BY RANGE (created_at);
-- order_date can be updated freely without moving partitions
-- Queries filter on created_at for lifecycle, order_date for business queries

-- Option 2: Add a CHECK constraint to prevent cross-partition updates
ALTER TABLE orders_2024_01 ADD CONSTRAINT chk_date_range
    CHECK (order_date >= '2024-01-01' AND order_date < '2024-02-01');
-- Now UPDATE that violates this raises an error, preventing accidental moves
-- Business must explicitly do: move order to right partition if date correction needed

-- Option 3: Soft-delete + re-insert (application handles the move)
UPDATE orders SET status = 'cancelled_for_correction' WHERE order_id = 12345;
INSERT INTO orders (customer_id, amount, order_date, status)
SELECT customer_id, amount, '2024-02-15', 'pending' FROM orders WHERE order_id = 12345;
-- Clear audit trail; explicit operation
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Multi-Tenant Partitioned Architecture with Data Residency Requirements

**Scenario:** You're building a SaaS platform with 10,000 enterprise customers. Requirements:
- EU customers' data must physically reside in EU (GDPR compliance)
- US and rest-of-world customers can share infrastructure
- Queries are always by `customer_id` (not by region)
- Volume: 5 billion rows, growing 500M rows/month
- Per-customer query p99 SLA: <100ms

Design the complete partitioning and isolation strategy.

<details>
<summary>💡 Hint</summary>

This requires two dimensions: (1) data residency by region (list partitioning) for compliance, and (2) distribution by customer_id within each region (hash partitioning) for query performance. Consider how to route queries to the correct region partition when only customer_id is known — you'll need a customer-to-region mapping.

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Architecture: composite partitioning — LIST by region, then HASH by customer_id

-- Step 1: Customer registry (small — 10K rows — not partitioned)
CREATE TABLE customer_registry (
    customer_id  INT PRIMARY KEY,
    company_name TEXT,
    region       TEXT NOT NULL,  -- 'EU', 'US', 'ROW'
    plan         TEXT,
    created_at   TIMESTAMPTZ
);
CREATE INDEX ON customer_registry(region);
-- Application looks up region for each customer_id ONCE (cached in Redis)

-- Step 2: Main data table with composite partitioning
CREATE TABLE customer_data (
    record_id    BIGSERIAL,
    customer_id  INT NOT NULL,
    region       TEXT NOT NULL,  -- Denormalized for partition routing
    data_type    TEXT,
    payload      JSONB,
    created_at   TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY LIST (region);

-- Step 3: Region-level partitions (maps to physical EU vs. US storage)
-- In Postgres tablespace-per-region setup (or separate databases per region):
CREATE TABLE customer_data_eu PARTITION OF customer_data
    FOR VALUES IN ('EU')
    PARTITION BY HASH (customer_id)
    TABLESPACE eu_storage;  -- EU tablespace on EU-located disk

CREATE TABLE customer_data_us_row PARTITION OF customer_data
    FOR VALUES IN ('US', 'ROW')
    PARTITION BY HASH (customer_id);

-- Step 4: Hash sub-partitions within each region (64 shards per region)
DO $$
BEGIN
    FOR i IN 0..63 LOOP
        -- EU sub-partitions
        EXECUTE format(
            'CREATE TABLE customer_data_eu_p%s PARTITION OF customer_data_eu
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
        -- US/ROW sub-partitions
        EXECUTE format(
            'CREATE TABLE customer_data_us_row_p%s PARTITION OF customer_data_us_row
             FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            i, i
        );
    END LOOP;
END $$;

-- Step 5: Covering indexes on each leaf partition (for <100ms p99)
CREATE INDEX idx_customer_data_lookup 
    ON customer_data (customer_id, data_type, created_at DESC)
    INCLUDE (payload);

-- Step 6: Row Level Security for tenant isolation
ALTER TABLE customer_data ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON customer_data
    FOR ALL TO app_role
    USING (customer_id = current_setting('app.customer_id')::INT);

-- Step 7: Application connection pattern
-- Application must:
-- 1. Look up customer's region from registry (cached in Redis, TTL 24h)
-- 2. Set session context before each request:
--    SET LOCAL app.customer_id = '12345';
-- 3. Query normally — partition pruning handles the rest

-- Query execution path for EU customer (customer_id = 12345, 12345 % 64 = 57):
-- EXPLAIN SELECT * FROM customer_data WHERE customer_id = 12345 ORDER BY created_at DESC LIMIT 100;
-- Plan: Seq Scan on customer_data_eu_p57 (RLS + partition pruning)
-- → 1 of 128 leaf partitions scanned, index used, all within EU storage
```

**Data residency verification:**

```sql
-- Verify EU data never appears outside EU partition:
SELECT schemaname, tablename, pg_relation_filepath(quote_ident(tablename)::regclass) AS physical_path
FROM pg_tables
WHERE tablename LIKE 'customer_data_eu%';
-- All EU partition files should map to paths on EU-tablespace storage
-- Auditors can verify this from the OS filesystem perspective

-- Compliance query: confirm no EU customer data in US partition
SELECT COUNT(*) FROM customer_data_us_row WHERE region = 'EU';
-- Must return 0 — if not, trigger incident response
```

**Performance validation:**
- 128 leaf partitions (64 EU + 64 US/ROW) → each has ~40M rows at 5B total
- Per-customer query at 100ms SLA: customer_id lookup hits 1 partition's covering index
- Index size per partition: ~3GB (vs 400GB if no partitioning) → entirely cache-resident for active customers

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is table partitioning in SQL databases and what are its main benefits?**
A: Partitioning divides a large table into smaller physical segments (partitions) based on a column's values. Benefits include partition pruning (queries that filter on the partition key scan only relevant partitions), faster maintenance operations (e.g., dropping an entire partition instead of deleting rows), and improved parallelism.

**Q: What are the main partitioning strategies?**
A: Range partitioning divides data into contiguous value ranges (e.g., by month). List partitioning assigns specific values to each partition (e.g., by country code). Hash partitioning distributes rows across a fixed number of partitions by hashing the key—good for even distribution when there's no natural range or list. Composite partitioning combines strategies (e.g., range by date, hash by ID within each range).

**Q: What is partition pruning and how does it work?**
A: When a query filters on the partition key, the optimizer skips partitions that cannot contain matching rows—this is partition pruning. For example, `WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'` on a monthly-range-partitioned table scans only the January 2024 partition. Pruning only works when the filter column matches the partition key.

**Q: What is a partition key and how do you choose one?**
A: The partition key is the column used to distribute rows across partitions. Choose a column that is: (a) frequently used in WHERE clauses to enable pruning, (b) has high cardinality relative to partition count, and (c) distributes data evenly to avoid hot partitions. Date/timestamp columns are the most common choice in data warehousing.

**Q: What is partition maintenance and why does it matter?**
A: Partition maintenance includes adding new partitions for future data, dropping old partitions (much faster than DELETE for time-series data), and rebuilding statistics per partition. In time-series workloads, automated partition management (e.g., monthly partition creation via a scheduled job) is essential to prevent insert failures when the current partition is full.

**Q: What is sub-partitioning and when is it used?**
A: Sub-partitioning (composite partitioning) adds a second level of partitioning within each partition—e.g., range by year, then hash by customer_id within each year. It provides finer-grained pruning for queries that filter on both dimensions and better load distribution, at the cost of more management complexity.

**Q: How does partitioning differ between PostgreSQL and Snowflake?**
A: PostgreSQL uses declarative partitioning (range, list, hash) with physical partition tables that you create and manage explicitly. Snowflake uses micro-partitions (automatic, immutable columnar blocks) as its storage unit, and clustering keys guide which micro-partitions are co-located—there's no user-created partition DDL. Snowflake's Automatic Clustering maintains physical ordering over time.

**Q: What are the downsides of over-partitioning a table?**
A: Too many partitions (e.g., partitioning by minute instead of month for a daily-query workload) increases metadata overhead, makes partition pruning less effective (more partitions to evaluate), and can cause performance issues in query planning. The partition count should align with actual query access patterns—not be finer than the finest common filter granularity.

---

## 💼 Interview Tips

- Always tie your partitioning strategy to a specific query pattern. "I'd partition by event_date because 95% of queries filter on date ranges" is much stronger than "I'd use range partitioning."
- Bring up partition maintenance proactively—automated partition creation for future periods and partition dropping for expired data are critical operational concerns that many candidates miss.
- Distinguish partitioning (a storage layout strategy) from indexing (a lookup acceleration structure). Both improve query performance but via different mechanisms; combining them is often the right answer for large tables.
- Senior interviewers at Snowflake shops will ask about clustering keys—understand that Snowflake's clustering is logically similar to partitioning but implemented differently (no user-managed DDL partitions).
- Discuss the trade-off between partition granularity and management overhead. A table partitioned by day gives more pruning opportunities than one partitioned by month, but requires 30x as many partitions and more maintenance scripts. Always calibrate to actual query patterns.
