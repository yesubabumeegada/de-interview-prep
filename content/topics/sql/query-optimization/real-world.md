---
title: "SQL Query Optimization - Real-World Production Examples"
topic: sql
subtopic: query-optimization
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, optimization, production, performance-tuning, monitoring]
---

# SQL Query Optimization — Real-World Production Examples

## Case Study 1: Dashboard Query — 45 Seconds to 2 Seconds

**The problem:** A BI dashboard query runs 200+ times per day and takes 45 seconds each time.

```sql
-- Original query (45 seconds)
SELECT 
    d.month_name,
    p.category,
    SUM(f.amount) AS revenue,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_id = p.product_id
WHERE d.year = 2024
GROUP BY d.month_name, p.category
ORDER BY d.month_name, revenue DESC;
```

**Diagnosis (execution plan analysis):**
1. `fact_sales`: 2B rows, full scan (no partition pruning — `date_key` is a surrogate integer, not a date)
2. `dim_product`: 100K rows, hash join (fine)
3. `COUNT(DISTINCT customer_id)` on 500M rows (expensive)

**Optimization applied:**

```sql
-- Fix 1: Add date column to fact table for partition pruning
-- (Or filter on date_key range corresponding to 2024)
ALTER TABLE fact_sales ADD sale_date DATE;
-- Backfill: UPDATE fact_sales f SET sale_date = d.full_date FROM dim_date d WHERE f.date_key = d.date_key;
-- Add clustering: ALTER TABLE fact_sales CLUSTER BY (sale_date);

-- Fix 2: Create a materialized view for this exact pattern
CREATE MATERIALIZED VIEW mv_monthly_category_revenue AS
SELECT 
    d.date_key,
    d.month_name,
    d.year,
    p.category,
    SUM(f.amount) AS revenue,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.date_key, d.month_name, d.year, p.category;

-- Now the dashboard query hits the MV (pre-aggregated, tiny table)
SELECT month_name, category, SUM(revenue), SUM(unique_customers)
FROM mv_monthly_category_revenue
WHERE year = 2024
GROUP BY month_name, category
ORDER BY month_name, revenue DESC;
-- Result: 2 seconds (reading ~200 rows from the MV instead of 2B from fact)
```

**Result:** 45 seconds → 2 seconds (22x improvement). MV refresh runs nightly in 3 minutes.

---

## Case Study 2: Nightly ETL Join — 4 Hours to 20 Minutes

**The problem:** Nightly MERGE job joining 500M events with 10M user profiles takes 4 hours.

```sql
-- Original (4 hours)
MERGE INTO user_event_summary t
USING (
    SELECT user_id, event_date, COUNT(*) as event_count, SUM(duration) as total_duration
    FROM raw_events
    WHERE event_date = '2024-01-15'
    GROUP BY user_id, event_date
) s
ON t.user_id = s.user_id AND t.event_date = s.event_date
WHEN MATCHED THEN UPDATE SET event_count = s.event_count, total_duration = s.total_duration
WHEN NOT MATCHED THEN INSERT VALUES (s.user_id, s.event_date, s.event_count, s.total_duration);
```

**Diagnosis:**
1. `raw_events` has no partition on `event_date` → scans ALL 500M rows for one day's data
2. Target table `user_event_summary` has no clustering → MERGE scans entire table to find matches
3. Skew: 1% of users generate 50% of events (power users)

**Optimizations:**

```sql
-- Fix 1: Partition raw_events by event_date
-- (Now filtering to one day reads 5M rows instead of 500M)

-- Fix 2: Cluster target on (user_id, event_date)
ALTER TABLE user_event_summary CLUSTER BY (user_id, event_date);
-- MERGE now finds matching rows by scanning only relevant micro-partitions

-- Fix 3: Use INSERT OVERWRITE instead of MERGE (idempotent, simpler)
DELETE FROM user_event_summary WHERE event_date = '2024-01-15';
INSERT INTO user_event_summary
SELECT user_id, event_date, COUNT(*), SUM(duration)
FROM raw_events
WHERE event_date = '2024-01-15'
GROUP BY user_id, event_date;
-- Simpler execution plan, same result, idempotent

-- Fix 4 (Spark): Handle skew with AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Result:** 4 hours → 20 minutes. Main wins: partition pruning (100x less data read) + DELETE+INSERT instead of MERGE (simpler plan).

---

## Case Study 3: Real-Time API Query — 5 Seconds to 50ms

**The problem:** Customer-facing API endpoint queries recent orders. SLA is 200ms, actual is 5 seconds.

```sql
-- Original (5 seconds)
SELECT order_id, status, amount, created_at
FROM orders
WHERE customer_id = 42
ORDER BY created_at DESC
LIMIT 10;
```

**Diagnosis:**
- `orders` table: 200M rows, no index on `customer_id`
- Full table scan + sort + limit = slow
- Even with index on `customer_id`: 50K matching rows need sorting

**Optimizations:**

```sql
-- Fix: Composite index that supports BOTH the filter AND the sort
CREATE INDEX idx_orders_cust_created ON orders(customer_id, created_at DESC);

-- Now the query:
-- 1. Seeks to customer_id=42 in the index (instant)
-- 2. Reads first 10 entries in reverse (already sorted DESC in index!)
-- 3. No sort needed, no extra rows read
-- Result: 50ms (index seek + 10 row reads)

-- For even faster: covering index (avoids table lookup entirely)
CREATE INDEX idx_orders_cust_created_cover 
ON orders(customer_id, created_at DESC) 
INCLUDE (order_id, status, amount);
-- All columns in SELECT are in the index → zero table access
-- Result: <10ms
```

**Result:** 5 seconds → 50ms (100x improvement from a single composite index).

---

## Case Study 4: Analytical Window Function — Memory Spill

**The problem:** Window function on 1B-row table causes memory spill to disk, taking 45 minutes.

```sql
-- Original (45 minutes, spills to disk)
SELECT 
    user_id,
    event_date,
    page_views,
    SUM(page_views) OVER (
        PARTITION BY user_id 
        ORDER BY event_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_views
FROM user_daily_activity;
-- With 50M distinct users, each partition is small
-- But the GLOBAL sort for PARTITION BY user_id + ORDER BY event_date is massive
```

**Diagnosis (Snowflake/Spark):**
- `PARTITION BY user_id` requires shuffling 1B rows by user_id
- Some users have 100K+ events (skew within partition)
- Total data > available memory → spills to disk

**Optimizations:**

```sql
-- Fix 1 (Snowflake): Cluster table on (user_id, event_date)
ALTER TABLE user_daily_activity CLUSTER BY (user_id, event_date);
-- Now the data is pre-sorted → window function reads sequentially (no shuffle)

-- Fix 2: Process incrementally (only today's data)
-- Instead of recomputing cumulative over ALL history:
INSERT INTO user_cumulative_views
SELECT 
    a.user_id,
    a.event_date,
    a.page_views,
    COALESCE(prev.cumulative_views, 0) + a.page_views AS cumulative_views
FROM user_daily_activity a
LEFT JOIN user_cumulative_views prev 
    ON a.user_id = prev.user_id 
    AND prev.event_date = a.event_date - INTERVAL '1 day'
WHERE a.event_date = CURRENT_DATE;
-- Only processes today's 5M rows instead of recomputing 1B rows

-- Fix 3 (Spark): Increase executor memory + use AQE
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Result:** 45 minutes → 3 minutes (clustering eliminated the shuffle; incremental approach avoids reprocessing history).

---

## Production Optimization Process

| Phase | Activity | Tools |
|-------|----------|-------|
| 1. Identify | Find slowest queries | `pg_stat_statements`, Snowflake Query History, Spark UI |
| 2. Diagnose | Read execution plan | EXPLAIN ANALYZE, Query Profile, Spark DAG |
| 3. Hypothesize | Identify root cause | Full scan? Skew? Spill? Bad stats? |
| 4. Fix | Apply optimization | Index, partition, broadcast, rewrite |
| 5. Verify | Confirm improvement | Before/after timing, plan comparison |
| 6. Monitor | Watch for regression | Alert on >50% slowdown week-over-week |

---

## Optimization Decision Tree

```
Query is slow. What kind of slow?

├── Full table scan on large table?
│   ├── Filter column available? → Add INDEX (row-store) or CLUSTERING (columnar)
│   └── No filter possible? → Materialized View or pre-aggregate
│
├── Join is expensive?
│   ├── Missing index on join key? → Add index
│   ├── One table is small? → BROADCAST hint
│   ├── Data skew on join key? → Salt technique or AQE
│   └── Joining too many rows? → Filter before join
│
├── Sort/GROUP BY is expensive?
│   ├── Can avoid sort? → Index on ORDER BY column
│   ├── COUNT(DISTINCT) on huge data? → Approximate count (HyperLogLog)
│   └── Window function spilling? → Cluster on PARTITION BY column
│
└── Not a query problem?
    ├── Warehouse/cluster undersized? → Scale up compute
    ├── Concurrent queries queuing? → Scale out (multi-cluster)
    └── Network/storage latency? → Check cloud region, caching
```

---

## Interview Tips

> **Tip 1:** "Tell me about a query you optimized" — Use the structure: "The query did [what] in [time]. I found [root cause] by reading the [execution plan/query profile]. I fixed it by [specific action: index, partition, rewrite, broadcast]. The result was [before time] → [after time], a [X]x improvement. The fix also reduced [costs/resource usage]."

> **Tip 2:** "How do you proactively prevent slow queries?" — "Four things: (1) Partition all fact tables on date (most queries filter by time). (2) Ensure all FK/join columns have indexes. (3) Refresh statistics after every major data load. (4) Monitor query execution times and alert on regressions — catch problems before users complain."

> **Tip 3:** "What's your approach to optimization in a data warehouse?" — "Different from OLTP: traditional indexes don't exist (columnar). Instead: cluster/sort keys for filter columns, partition on date, broadcast small dimensions, use materialized views for repeated dashboard queries, and always check partition pruning in the query profile — if >10% of partitions are scanned, the clustering needs improvement."
