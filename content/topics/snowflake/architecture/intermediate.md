---
title: "Snowflake Architecture - Intermediate"
topic: snowflake
subtopic: architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, architecture, clustering, caching, time-travel, cloning, data-sharing]
---

# Snowflake Architecture — Intermediate Concepts

## Clustering and Partition Pruning

### Natural Clustering

When data is loaded, Snowflake stores rows in insertion order within micro-partitions. If you load data chronologically, it's naturally clustered by date — queries filtering by date automatically prune most partitions.

### Clustering Keys (Manual Optimization)

For tables where the natural order doesn't match query patterns, define a clustering key:

```sql
-- Cluster fact_sales by date and store (most common filter columns)
ALTER TABLE fact_sales CLUSTER BY (sale_date, store_id);
```

**When clustering helps:**

| Scenario | Before Clustering | After Clustering |
|----------|------------------|-----------------|
| Filter on sale_date | 5% partitions scanned | 0.1% partitions scanned |
| Filter on store_id | 30% partitions scanned | 2% partitions scanned |
| Filter on both | 1.5% scanned | 0.05% scanned |

**When NOT to cluster:**
- Tables under 1 TB (not enough partitions to matter)
- Tables with no dominant filter pattern
- Tables where data arrives pre-sorted (already naturally clustered)

> **Cost warning:** Snowflake automatically re-clusters data in the background (Automatic Clustering service). This consumes credits. Only cluster tables with clear, repeated filter patterns on specific columns.

### Checking Clustering Quality

```sql
-- Check clustering depth and overlap
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(sale_date, store_id)');

-- Result shows:
-- average_depth: 1.5 (good: 1-2 means well-clustered)
-- average_overlap: 0.2 (good: low overlap between partitions)
```

---

## Caching Layers

Snowflake has three levels of caching (all automatic, no configuration needed):

### 1. Result Cache (Cloud Services Layer)

If the exact same query runs again and the underlying data hasn't changed, Snowflake returns the cached result **instantly** (no compute used).

```sql
-- Query 1: takes 30 seconds
SELECT region, SUM(revenue) FROM fact_sales GROUP BY region;

-- Query 2 (identical, run 5 minutes later): returns in < 1 second
SELECT region, SUM(revenue) FROM fact_sales GROUP BY region;
-- Uses result cache — zero compute cost!
```

**Cache invalidation:** Results are cached for 24 hours. Invalidated if underlying table data changes (INSERT, UPDATE, DELETE).

### 2. Local Disk Cache (Compute Layer)

Each warehouse node caches micro-partitions it recently read on local SSD. Repeated scans of the same data avoid re-reading from remote storage.

**How it helps:** A dashboard that queries the same table repeatedly benefits because the data is already on local disk after the first query.

### 3. Remote Disk Cache (Storage Layer)

Cloud storage (S3) itself caches frequently accessed objects at the storage layer.

| Cache Level | Scope | Duration | Cost |
|-------------|-------|----------|------|
| Result cache | Per-query (exact match) | 24 hours | Free (no compute) |
| Local disk | Per-warehouse node | Until evicted (LRU) | No extra cost |
| Remote storage | Per-object | Platform-managed | No extra cost |

---

## Time Travel

Query or restore data as it existed at any point in the past (up to 90 days on Enterprise edition).

```sql
-- Query data as it was yesterday
SELECT * FROM fact_sales AT (TIMESTAMP => '2024-01-14 10:00:00');

-- Query data from 30 minutes ago
SELECT * FROM fact_sales AT (OFFSET => -1800);  -- 1800 seconds ago

-- Query data before a specific query modified it
SELECT * FROM fact_sales BEFORE (STATEMENT => '<query_id>');
```

**Use cases:**
- Recover from accidental DELETE/UPDATE
- Audit what data looked like at a point in time
- Compare current vs historical state

```sql
-- Restore accidentally dropped table
DROP TABLE fact_sales;  -- Oops!
UNDROP TABLE fact_sales; -- Restored!

-- Restore from a specific point in time
CREATE TABLE fact_sales_restored CLONE fact_sales
    AT (TIMESTAMP => '2024-01-14 10:00:00');
```

> **Storage cost:** Time Travel retains old micro-partitions for the configured retention period. More changes = more historical storage consumed.

---

## Zero-Copy Cloning

Create an instant copy of a database, schema, or table **without duplicating data**. The clone shares micro-partitions with the original until one side makes changes.

```sql
-- Clone entire database (instant, regardless of size)
CREATE DATABASE analytics_dev CLONE analytics_prod;

-- Clone a single table
CREATE TABLE staging.orders_test CLONE production.orders;

-- Clone at a point in time (Time Travel + Clone)
CREATE TABLE orders_snapshot CLONE orders 
    AT (TIMESTAMP => '2024-01-15 00:00:00');
```

**How it works internally:**

```
Original table: points to micro-partitions [A, B, C, D, E]
Cloned table:   points to micro-partitions [A, B, C, D, E]  ← same pointers!

After INSERT on clone: 
Cloned table:   points to [A, B, C, D, E, F_new]  ← only F is new storage

After DELETE on original:
Original table: points to [A, B, C, E]  ← D removed from original
Cloned table:   still points to [A, B, C, D, E, F]  ← D still accessible
```

> **Cost:** Zero additional storage until modifications are made. Only the changed/new micro-partitions consume storage. A 10 TB clone initially uses 0 bytes of additional storage.

**Use cases:**
- Development/testing environments (full copy of prod data — instant, free)
- Backup before a risky migration
- Creating a snapshot for analysis without impacting production

---

## Secure Data Sharing

Share live data with other Snowflake accounts without copying — the consumer reads directly from your micro-partitions (read-only).

```sql
-- Provider account: create a share
CREATE SHARE sales_data_share;
GRANT USAGE ON DATABASE analytics TO SHARE sales_data_share;
GRANT USAGE ON SCHEMA analytics.curated TO SHARE sales_data_share;
GRANT SELECT ON TABLE analytics.curated.fact_sales TO SHARE sales_data_share;

-- Add consumer account
ALTER SHARE sales_data_share ADD ACCOUNTS = 'consumer_account_xyz';

-- Consumer account: create database from share
CREATE DATABASE shared_sales FROM SHARE provider_account.sales_data_share;
SELECT * FROM shared_sales.curated.fact_sales; -- Live data, always current
```

**Key properties:**
- No data copying (consumer reads provider's storage directly)
- Always current (no ETL needed to sync)
- Provider controls access granularity (table/view level)
- Consumer cannot modify data (read-only)
- Cross-region/cross-cloud sharing available (with replication)

---

## Materialized Views

Pre-computed result sets that Snowflake automatically maintains as underlying data changes:

```sql
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT 
    sale_date,
    store_id,
    SUM(amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM fact_sales
GROUP BY sale_date, store_id;

-- Queries automatically use the MV when the optimizer determines it's beneficial
SELECT * FROM mv_daily_revenue WHERE sale_date = '2024-01-15';
-- Reads from pre-computed MV instead of scanning fact_sales (much faster)
```

**Automatic maintenance:** When fact_sales gets new data, Snowflake incrementally updates the MV in the background. You don't run any refresh commands.

**Limitations:**
- Only supports SELECT with aggregations (no JOINs in the MV query)
- Background refresh consumes credits
- Not suitable for rapidly changing tables (high refresh cost)

---

## Table Types

| Type | Time Travel | Fail-safe | Use Case |
|------|------------|-----------|----------|
| Permanent (default) | Up to 90 days | 7 days | Production data |
| Transient | Up to 1 day | None | Staging/temp data (cheaper) |
| Temporary | Session only | None | Session scratch data (cheapest) |

```sql
-- Transient table: no fail-safe, lower storage cost
CREATE TRANSIENT TABLE staging.raw_events (
    event_id VARCHAR,
    payload VARIANT
);

-- Temporary table: gone when session ends
CREATE TEMPORARY TABLE session_temp AS
SELECT * FROM fact_sales WHERE sale_date = CURRENT_DATE;
```

> **Cost optimization:** Use TRANSIENT for staging tables and intermediate ETL results. You don't need 7-day fail-safe for data that can be re-derived from source.

---

## Interview Tips

> **Tip 1:** "How does Snowflake optimize queries without traditional indexes?" — "Two mechanisms: (1) Partition pruning using min/max metadata per micro-partition — the optimizer skips irrelevant data without scanning. (2) Clustering keys to co-locate data, maximizing prune effectiveness. Additionally, columnar storage means only the needed columns are read."

> **Tip 2:** "Explain zero-copy cloning" — "It creates a new pointer to the same underlying micro-partitions. No data is duplicated. Changes to either copy create new micro-partitions only for the modified data. This enables instant dev/test environments on TB-scale databases at zero additional storage cost."

> **Tip 3:** "How do you manage costs in Snowflake?" — "Auto-suspend warehouses aggressively (60-300 seconds). Use transient tables for staging (no fail-safe cost). Right-size warehouses — bigger is faster but not necessarily more expensive per query. Monitor with ACCOUNT_USAGE views. Set resource monitors to cap spending."
