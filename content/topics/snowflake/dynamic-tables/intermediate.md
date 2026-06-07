---
title: "Dynamic Tables - Intermediate"
topic: snowflake
subtopic: dynamic-tables
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, dynamic-tables, incremental, joins, optimization, monitoring]
---

# Snowflake Dynamic Tables — Intermediate

## Incremental vs Full Refresh

Snowflake automatically decides whether to refresh incrementally or fully:

```sql
-- INCREMENTAL REFRESH (preferred — processes only changes):
-- Supported for: simple SELECT, filter, JOIN, GROUP BY, UNION ALL
-- Snowflake detects what changed in source → applies only those changes
CREATE DYNAMIC TABLE silver.orders TARGET_LAG = '10 minutes' WAREHOUSE = 'WH' AS
    SELECT order_id, amount, order_date FROM raw.orders WHERE amount > 0;
-- Only new/changed rows from raw.orders are processed each refresh!

-- FULL REFRESH (entire table recomputed):
-- Required for: window functions, non-deterministic functions (CURRENT_TIMESTAMP),
-- complex subqueries, LIMIT, SAMPLE
CREATE DYNAMIC TABLE gold.top_customers TARGET_LAG = '1 hour' WAREHOUSE = 'WH' AS
    SELECT customer_id, total_spend,
           ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS rank
    FROM silver.customer_metrics;
-- Window function → can't do incrementally → full recompute each time
-- Use larger TARGET_LAG for full-refresh DTs (less frequent = less cost)

-- CHECK refresh mode:
SELECT NAME, REFRESH_MODE  -- 'INCREMENTAL' or 'FULL'
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE NAME = 'ORDERS';
```

### What Enables Incremental Refresh

| Pattern | Incremental? | Why |
|---------|-------------|-----|
| Simple SELECT + WHERE | ✅ Yes | New rows only need filter check |
| JOIN (inner, left) | ✅ Yes | New rows from either side can be joined |
| GROUP BY + aggregates | ✅ Yes | Update aggregates for changed groups |
| UNION ALL | ✅ Yes | Append from each source independently |
| Window functions | ❌ No | Rankings depend on all rows |
| LIMIT / TOP | ❌ No | Result depends on entire dataset |
| Non-deterministic functions | ❌ No | Same input → different output |
| Recursive CTE | ❌ No | Requires full traversal |

---

## Multi-Level Dynamic Table Pipelines

```sql
-- Layer 1: Silver (cleaned, typed)
CREATE DYNAMIC TABLE silver.orders
    TARGET_LAG = '5 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        order_id::NUMBER AS order_id,
        customer_id::NUMBER AS customer_id,
        amount::DECIMAL(10,2) AS amount,
        order_date::DATE AS order_date
    FROM raw.orders
    WHERE order_id IS NOT NULL AND amount > 0;

-- Layer 2: Silver enriched (joins dimension)
CREATE DYNAMIC TABLE silver.enriched_orders
    TARGET_LAG = '10 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        o.order_id, o.amount, o.order_date,
        c.customer_name, c.region, c.segment
    FROM silver.orders o
    LEFT JOIN silver.customers c ON o.customer_id = c.customer_id;

-- Layer 3: Gold (aggregated)
CREATE DYNAMIC TABLE gold.revenue_by_region
    TARGET_LAG = '30 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        region, order_date,
        COUNT(*) AS orders,
        SUM(amount) AS revenue,
        AVG(amount) AS avg_order_value
    FROM silver.enriched_orders
    GROUP BY region, order_date;

-- DEPENDENCY CHAIN (automatic):
-- raw.orders changes → silver.orders refreshes (5 min)
-- silver.orders changes → silver.enriched_orders refreshes (10 min)
-- silver.enriched_orders changes → gold.revenue_by_region refreshes (30 min)
-- Total end-to-end: ~45 minutes max from raw change to gold update
-- Snowflake manages ALL of this automatically!
```

---

## Dynamic Tables with Joins

```sql
-- JOIN between Dynamic Table and regular table (dimension)
CREATE DYNAMIC TABLE silver.enriched_events
    TARGET_LAG = '15 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        e.event_id, e.event_type, e.event_time, e.user_id,
        u.user_name, u.email, u.signup_date
    FROM raw.events e
    LEFT JOIN dim.users u ON e.user_id = u.user_id
    WHERE e.event_time >= DATEADD('day', -90, CURRENT_DATE());
-- Incremental: new events joined with users dimension
-- Dimension changes also trigger refresh (new users matched to old events)

-- JOIN between two Dynamic Tables
CREATE DYNAMIC TABLE gold.customer_order_summary
    TARGET_LAG = '30 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        c.customer_id, c.customer_name, c.region,
        COUNT(o.order_id) AS total_orders,
        SUM(o.amount) AS lifetime_value
    FROM silver.customers c
    LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.region;
-- Both silver.customers and silver.orders are DTs
-- This gold DT refreshes when EITHER source DT changes
```

---

## Monitoring and Troubleshooting

```sql
-- Refresh history (when did it refresh, how long, how many rows?)
SELECT 
    NAME,
    REFRESH_TRIGGER,        -- 'INCREMENTAL' or 'FULL'
    STATE,                  -- 'SUCCEEDED', 'FAILED', 'CANCELLED'
    STATE_MESSAGE,          -- Error message if failed
    DATA_TIMESTAMP,         -- Point-in-time the data represents
    REFRESH_START_TIME,
    REFRESH_END_TIME,
    TIMESTAMPDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) AS duration_sec,
    STATISTICS:insertedRowCount::NUMBER AS rows_inserted,
    STATISTICS:deletedRowCount::NUMBER AS rows_deleted
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'SILVER.ORDERS'
))
ORDER BY REFRESH_START_TIME DESC
LIMIT 20;

-- Current status of all Dynamic Tables
SELECT NAME, SCHEMA_NAME, TARGET_LAG, REFRESH_MODE, SCHEDULING_STATE,
       DATA_TIMESTAMP, -- Last refresh data point
       TIMESTAMPDIFF('minute', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) AS actual_lag_minutes
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
ORDER BY actual_lag_minutes DESC;
-- If actual_lag > target_lag → DT is falling behind (investigate!)

-- Dependency graph
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE TARGET_NAME = 'GOLD.REVENUE_BY_REGION';
-- Shows: which tables feed into this DT (the DAG)
```

---

## Suspend and Resume

```sql
-- Pause refreshes (for maintenance, cost control, or debugging)
ALTER DYNAMIC TABLE silver.orders SUSPEND;
-- Table still queryable (shows data from last refresh)
-- No new refreshes happen (saves cost)
-- Use for: planned maintenance, cost reduction overnight

-- Resume refreshes
ALTER DYNAMIC TABLE silver.orders RESUME;
-- Catches up from where it left off (processes accumulated changes)

-- Change TARGET_LAG (adjust freshness without recreation)
ALTER DYNAMIC TABLE silver.orders SET TARGET_LAG = '30 minutes';
-- Less frequent refreshes = lower cost
-- Use for: non-peak hours where freshness can be relaxed
```

---

## Limitations and Workarounds

```sql
-- LIMITATION 1: No DML on Dynamic Tables (INSERT, UPDATE, DELETE)
-- DTs are read-only (managed entirely by Snowflake refresh)
-- Workaround: if you need manual overrides, use regular table + stream + task

-- LIMITATION 2: Window functions trigger full refresh
-- Workaround: compute ranks in a regular table via task, or accept full refresh cost
-- Or: use a QUALIFY (which IS incremental!) instead of a window in a subquery

-- LIMITATION 3: No non-deterministic functions (CURRENT_TIMESTAMP, RANDOM)
-- Workaround: use SYSTEM$CURRENT_TIMESTAMP() in supported contexts
-- Or: accept full refresh mode

-- LIMITATION 4: No CHANGES clause or STREAMS on Dynamic Tables (yet)
-- Can't create a stream ON a Dynamic Table
-- Workaround: downstream DT reads directly from this DT (DT chain)

-- BEST PRACTICE: Keep DT queries simple for incremental refresh
-- Complex? Split into: DT (simple) → view (complex) or DT → DT (simpler each step)
```

---

## Interview Tips

> **Tip 1:** "How does Snowflake decide incremental vs full refresh?" — Based on the SQL query pattern. Simple SELECT + WHERE + JOIN + GROUP BY → incremental (only process changes). Window functions, LIMIT, non-deterministic functions → full refresh (recompute everything). Check REFRESH_MODE in DYNAMIC_TABLES metadata to confirm which mode your DT uses.

> **Tip 2:** "How do you build a multi-table pipeline with Dynamic Tables?" — Chain DTs: each DT's SELECT references the previous DT (or raw table). Set increasing TARGET_LAGs downstream (silver=5 min, gold=30 min). Snowflake automatically detects the dependency chain and refreshes in order. No manual dependency management needed.

> **Tip 3:** "Dynamic Tables vs Materialized Views?" — Dynamic Tables: full transformation (any SELECT), managed refresh, cost proportional to change volume. Materialized Views: limited transformations (no JOIN between tables), automatic transparent query rewriting, background maintenance. Use DT for pipeline transformations; MV for query acceleration on single tables.
