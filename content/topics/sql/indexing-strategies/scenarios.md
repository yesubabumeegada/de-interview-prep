---
title: "SQL Indexing Strategies - Scenario Questions"
topic: sql
subtopic: indexing-strategies
content_type: scenario_question
tags: [sql, indexing, interview, scenarios, performance, b-tree, composite-index]
---

# Scenario Questions — SQL Indexing Strategies

<article data-difficulty="junior">

## 🟢 Junior: Explain Why a Query Is Slow and Fix It

**Scenario:** A developer reports that this query against a 5-million-row `products` table takes 12 seconds:

```sql
SELECT product_id, name, price
FROM products
WHERE category_id = 42
  AND is_active = TRUE
ORDER BY price ASC
LIMIT 20;
```

Running `EXPLAIN` shows a `Seq Scan` on the products table. The table has no indexes except the primary key on `product_id`. Explain what's happening and add the right index.

<details>
<summary>💡 Hint</summary>

A Sequential Scan means the database reads every row in the table to find the ones matching `category_id = 42 AND is_active = TRUE`. With 5 million rows, this is slow. Add an index that supports the WHERE clause columns and the ORDER BY column so the database can find and sort the rows using the index directly.

</details>

<details>
<summary>✅ Solution</summary>

**What's happening:**
- No index exists on `category_id` or `is_active`
- The database reads all 5 million rows, filters them, then sorts the survivors
- Even if 5,000 rows match, all 5 million must be examined first

**The fix:**

```sql
-- Option 1: Composite index supporting filter + sort
CREATE INDEX idx_products_category_active_price 
    ON products(category_id, is_active, price ASC);

-- Now the query can:
-- 1. Seek to category_id = 42, is_active = TRUE in the index (fast)
-- 2. Rows are already sorted by price (no sort step needed)
-- 3. Read just 20 rows (LIMIT applied immediately)
```

**Why this column order:**
- `category_id` first: equality filter, highly selective
- `is_active` second: equality filter (but low selectivity — only 2 values)
- `price` third: the ORDER BY column — placing it here avoids a sort step

**Verify the fix:**
```sql
EXPLAIN SELECT product_id, name, price
FROM products
WHERE category_id = 42 AND is_active = TRUE
ORDER BY price ASC LIMIT 20;

-- Expected output:
-- Limit (rows=20)
--   -> Index Scan using idx_products_category_active_price on products
--         Index Cond: (category_id = 42 AND is_active = true)
-- Execution time: ~0.5ms instead of 12 seconds
```

**Alternative:** If `is_active = FALSE` rows are rarely needed, use a partial index:
```sql
CREATE INDEX idx_products_active_category_price
    ON products(category_id, price ASC)
    WHERE is_active = TRUE;
-- Smaller index, same performance for the common case
```

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Leftmost Prefix Rule — Which Queries Use the Index?

**Scenario:** A table `orders` has the following composite index:

```sql
CREATE INDEX idx_orders_comp ON orders(customer_id, status, order_date);
```

For each query below, determine whether the index is used (fully, partially, or not at all) and explain why:

```sql
-- Query A:
SELECT * FROM orders WHERE customer_id = 100;

-- Query B:
SELECT * FROM orders WHERE customer_id = 100 AND status = 'shipped';

-- Query C:
SELECT * FROM orders WHERE status = 'shipped';

-- Query D:
SELECT * FROM orders WHERE customer_id = 100 AND order_date >= '2024-01-01';

-- Query E:
SELECT * FROM orders WHERE customer_id = 100 AND status = 'shipped' AND order_date >= '2024-01-01';
```

<details>
<summary>💡 Hint</summary>

The leftmost prefix rule: a composite index on `(A, B, C)` supports queries that filter on `A`, `A+B`, or `A+B+C`. It CANNOT be used if the leftmost column is missing from the WHERE clause. For ranges: the range column should come after all equality columns.

</details>

<details>
<summary>✅ Solution</summary>

| Query | Index Usage | Reason |
|-------|-------------|--------|
| A (`WHERE customer_id = 100`) | ✅ Partial — first column | Uses first prefix of the index |
| B (`customer_id + status`) | ✅ Partial — first two columns | Uses two-column prefix |
| C (`WHERE status = 'shipped'`) | ❌ Not used | Missing the leftmost column (`customer_id`) |
| D (`customer_id + order_date`) | ⚠️ Partial — first column only | `status` is in between and missing — index used for `customer_id` but not `order_date` |
| E (all three columns) | ✅ Full | All three columns used in left-to-right order |

**Explanation for Query D (the tricky one):**
```sql
-- Index: (customer_id, status, order_date)
-- Query: customer_id = 100 AND order_date >= '2024-01-01'
-- The index can navigate to customer_id = 100 rows
-- But to reach order_date, it must also know status (the middle column)
-- Since status is not filtered, the index can only use the customer_id prefix
-- → Scans all rows for customer_id = 100, then filters by date in memory

-- Fix: create an index with order_date right after customer_id:
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);
-- Now Query D uses the index fully
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design an Indexing Strategy for a Reporting API

**Scenario:** You have an `events` table (100 million rows) with this schema:

```sql
CREATE TABLE events (
    event_id    BIGSERIAL PRIMARY KEY,
    user_id     INT,
    event_type  VARCHAR(50),   -- 8 distinct values
    event_ts    TIMESTAMPTZ,
    session_id  UUID,
    value       NUMERIC
);
```

The application runs these queries frequently:

1. **Query A (very frequent — runs per page load):** Find all events for a specific user in the last 7 days, ordered by time
2. **Query B (frequent — analytics dashboard):** Count events per type per day for the last 30 days
3. **Query C (rare — admin only):** Find all events for a specific session

Design the minimal set of indexes and explain the reasoning behind each.

<details>
<summary>💡 Hint</summary>

Think about each query's access pattern: what columns are in the WHERE clause, what columns are in the ORDER BY, and what columns are in the SELECT. For Query B, consider that `event_type` has low cardinality (8 values) — would a full index help? For Query C, consider how often it runs before adding an index.

</details>

<details>
<summary>✅ Solution</summary>

**Query A — User events in last 7 days:**
```sql
-- Access pattern: equality on user_id, range on event_ts, ORDER BY event_ts
-- Composite index with covering columns:
CREATE INDEX idx_events_user_ts 
    ON events(user_id, event_ts DESC)
    INCLUDE (event_type, value, session_id);  -- Cover the SELECT columns

-- Reasoning:
-- user_id first (equality filter), event_ts second (range + sort direction)
-- INCLUDE avoids heap fetches for the SELECT columns → Index-Only Scan possible
-- DESC matches the typical "most recent first" ordering
```

**Query B — Events per type per day:**
```sql
-- Access pattern: range on event_ts, GROUP BY event_type + date
-- event_type has only 8 values → low selectivity → index less useful alone

-- Option 1: Partial index per event_type (if queries often filter by type)
-- Option 2: Materialized view refreshed hourly (better for aggregation at this scale)

-- Practical answer for 100M rows:
CREATE MATERIALIZED VIEW daily_event_counts AS
SELECT 
    DATE_TRUNC('day', event_ts) AS event_day,
    event_type,
    COUNT(*) AS event_count,
    SUM(value) AS total_value
FROM events
WHERE event_ts >= NOW() - INTERVAL '30 days'
GROUP BY 1, 2;

CREATE UNIQUE INDEX ON daily_event_counts(event_day, event_type);
-- Dashboard queries the materialized view (tiny) instead of 100M rows
-- Refresh: REFRESH MATERIALIZED VIEW CONCURRENTLY daily_event_counts;
```

**Query C — Events by session (admin only):**
```sql
-- Runs rarely → weigh benefit against write overhead
-- Decision: add a partial index (sessions are looked up within recent data)

CREATE INDEX idx_events_session 
    ON events(session_id)
    WHERE event_ts >= NOW() - INTERVAL '90 days';  -- Only recent sessions
-- Much smaller than a full-table session_id index
-- Admin queries typically look up recent sessions
```

**Final summary:**
- 2 indexes on `events` table (not 3 — avoid the full aggregation index)
- 1 materialized view for the dashboard query
- Total write overhead: 2 index updates per insert (manageable)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Identify and Remove Redundant Indexes

**Scenario:** You inherit a production database with this `users` table and its indexes:

```sql
CREATE TABLE users (
    user_id    BIGINT PRIMARY KEY,
    email      VARCHAR(255) UNIQUE,
    username   VARCHAR(100),
    country    VARCHAR(2),
    created_at TIMESTAMPTZ,
    is_active  BOOLEAN
);

CREATE INDEX idx_1 ON users(email);
CREATE INDEX idx_2 ON users(username);
CREATE INDEX idx_3 ON users(country, created_at);
CREATE INDEX idx_4 ON users(country);
CREATE INDEX idx_5 ON users(created_at);
CREATE INDEX idx_6 ON users(is_active, country, created_at);
CREATE INDEX idx_7 ON users(is_active, created_at);
```

Identify which indexes are redundant and which to keep. Explain your reasoning.

<details>
<summary>💡 Hint</summary>

A composite index `(A, B)` makes a single-column index on `(A)` redundant — the composite index can handle all queries the single-column index can handle. Also check: does a UNIQUE constraint already create an index? Look for indexes that are exact subsets of other indexes.

</details>

<details>
<summary>✅ Solution</summary>

**Analysis:**

| Index | Columns | Redundant? | Reason |
|-------|---------|-----------|--------|
| PK | `user_id` | Keep | Primary key — required |
| UNIQUE on `email` | `email` | Keep (idx_1 is duplicate!) | UNIQUE constraint creates implicit index |
| `idx_1` | `email` | ❌ DROP | Exact duplicate of the UNIQUE constraint's implicit index |
| `idx_2` | `username` | Keep if used | Check pg_stat_user_indexes — username lookups are common |
| `idx_3` | `(country, created_at)` | Keep | Two-column composite — supports (country) and (country, created_at) queries |
| `idx_4` | `country` | ❌ DROP | Redundant — idx_3 on (country, created_at) starts with country and handles all queries idx_4 handles |
| `idx_5` | `created_at` | Keep | Leading column is different — no other index starts with created_at |
| `idx_6` | `(is_active, country, created_at)` | Keep | Composite — idx_7 is a subset! |
| `idx_7` | `(is_active, created_at)` | ❌ DROP | idx_6 starts with (is_active, ...) — can handle all queries idx_7 handles, PLUS country filtering |

**Actions:**
```sql
-- Drop idx_1: duplicate of UNIQUE email index
DROP INDEX idx_1;

-- Drop idx_4: subset of idx_3
DROP INDEX idx_4;

-- Drop idx_7: subset of idx_6
DROP INDEX idx_7;

-- Savings: 3 fewer indexes to maintain on every INSERT/UPDATE/DELETE
-- Remaining: 5 indexes (PK, UNIQUE email, idx_2, idx_3, idx_5, idx_6) — all non-redundant
```

**Before dropping in production:** verify scan counts:
```sql
SELECT indexrelname, idx_scan FROM pg_stat_user_indexes 
WHERE tablename = 'users' AND indexrelname IN ('idx_1', 'idx_4', 'idx_7');
-- If any show idx_scan > 0 in the current stats window, investigate before dropping
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Index Strategy for a High-Volume Event Ingestion Table

**Scenario:** You're designing the indexing strategy for a `sensor_readings` table that will receive 50,000 inserts per second from IoT devices. The table schema is:

```sql
CREATE TABLE sensor_readings (
    reading_id  BIGSERIAL PRIMARY KEY,
    sensor_id   INT,          -- 100,000 distinct sensors
    device_type VARCHAR(20),  -- 12 distinct values
    recorded_at TIMESTAMPTZ,
    temperature FLOAT,
    humidity    FLOAT,
    pressure    FLOAT
);
```

Query patterns:
- **Query A (latency-sensitive):** Latest 100 readings for a specific sensor in the last hour
- **Query B (batch analytics):** Hourly averages per device_type for the last 24 hours (runs every 5 min)
- **Query C (alerting — real-time):** Find sensors where the last reading had temperature > 100°C (runs every 30 sec)

Design the complete indexing and storage strategy, considering the 50K inserts/sec write pressure.

<details>
<summary>💡 Hint</summary>

At 50K inserts/sec, each additional index costs significant write throughput. Consider: (1) which queries are latency-sensitive enough to justify an index, (2) whether a materialized view or summary table could replace a heavy analytics query, (3) table partitioning to reduce index size. Don't add indexes blindly — calculate the insert amplification.

</details>

<details>
<summary>✅ Solution</summary>

**Write cost analysis first:**
```
50,000 inserts/sec × (1 table write + N index writes) = total I/O
Each index: ~50,000 additional B-tree writes/sec
At 50K/sec: every additional index = 4.3 billion extra writes per day
→ Minimize indexes aggressively
```

**Query A — Latest readings per sensor:**
```sql
-- Partial index: only index the last 1 hour (rolling window)
-- Problem: partial index WHERE clause can't use dynamic NOW() — use time-based partitioning instead

-- Solution: partition by hour, index within each partition
CREATE TABLE sensor_readings (
    reading_id  BIGINT,
    sensor_id   INT,
    device_type VARCHAR(20),
    recorded_at TIMESTAMPTZ,
    temperature FLOAT,
    humidity    FLOAT,
    pressure    FLOAT
) PARTITION BY RANGE (recorded_at);

-- Create partitions (automation in production):
CREATE TABLE sensor_readings_2024_01_15_14 
    PARTITION OF sensor_readings 
    FOR VALUES FROM ('2024-01-15 14:00') TO ('2024-01-15 15:00');

-- Index only within the current partition:
CREATE INDEX ON sensor_readings_2024_01_15_14 (sensor_id, recorded_at DESC);
-- Index is tiny (50K × 3600s = 180M rows/hour), refreshed every hour
-- Old partitions: drop index or detach partition entirely after retention period
```

**Query B — Hourly aggregations per device_type:**
```sql
-- Don't query the raw table at 50K inserts/sec for analytics
-- Use a summary table, updated incrementally:

CREATE TABLE sensor_hourly_summary (
    device_type  VARCHAR(20),
    hour_bucket  TIMESTAMPTZ,
    avg_temp     FLOAT,
    avg_humidity FLOAT,
    avg_pressure FLOAT,
    reading_count INT,
    PRIMARY KEY (device_type, hour_bucket)
);

-- Airflow DAG or pg_cron updates this every 5 minutes:
INSERT INTO sensor_hourly_summary
SELECT 
    device_type,
    DATE_TRUNC('hour', recorded_at),
    AVG(temperature),
    AVG(humidity),
    AVG(pressure),
    COUNT(*)
FROM sensor_readings
WHERE recorded_at >= NOW() - INTERVAL '2 hours'  -- Only recompute recent hours
GROUP BY device_type, DATE_TRUNC('hour', recorded_at)
ON CONFLICT (device_type, hour_bucket) DO UPDATE SET
    avg_temp = EXCLUDED.avg_temp,
    avg_humidity = EXCLUDED.avg_humidity,
    avg_pressure = EXCLUDED.avg_pressure,
    reading_count = EXCLUDED.reading_count;
-- Query B now hits this 12 × 24 = 288 row summary table — sub-millisecond
```

**Query C — Real-time alerting:**
```sql
-- Creating a "hot sensor" materialized table updated by triggers or CDC
-- Avoid full-table scan every 30 seconds at this write rate

CREATE TABLE sensor_latest (
    sensor_id    INT PRIMARY KEY,
    temperature  FLOAT,
    humidity     FLOAT,
    recorded_at  TIMESTAMPTZ
);

-- Upsert on every insert to sensor_readings (via trigger or application):
INSERT INTO sensor_latest VALUES (sensor_id, temperature, humidity, recorded_at)
ON CONFLICT (sensor_id) DO UPDATE SET
    temperature = EXCLUDED.temperature,
    humidity    = EXCLUDED.humidity,
    recorded_at = EXCLUDED.recorded_at
WHERE EXCLUDED.recorded_at > sensor_latest.recorded_at;

-- Alert query every 30 seconds:
SELECT sensor_id, temperature FROM sensor_latest WHERE temperature > 100;
-- 100,000 row table → sub-millisecond, even full scan is fast
```

**Final indexing summary:**
| Table | Indexes | Rationale |
|-------|---------|-----------|
| `sensor_readings` | PK only (per partition + 1 index on active partition) | Minimize write amplification |
| `sensor_hourly_summary` | PK on (device_type, hour_bucket) | Tiny table — fast lookups |
| `sensor_latest` | PK on sensor_id | 100K rows — any access is fast |

</details>

</article>
