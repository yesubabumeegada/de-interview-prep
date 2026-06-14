---
title: "Snowflake Cost Management - Fundamentals"
topic: snowflake
subtopic: cost-management
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, cost, credits, warehouses, storage, optimization, billing]
---

# Snowflake Cost Management — Fundamentals

## 🎯 Analogy

Snowflake billing is like a cloud electricity meter. **Storage** is your base monthly rate (how much data you're keeping plugged in). **Compute** (virtual warehouses) is like running appliances — you only pay while they're on. Leave the TV on all night = wasted credits. Auto-suspend is your smart power strip.

---

## Two Cost Components

| Component | What You Pay For | Unit | Typical Cost |
|-----------|----------------|------|-------------|
| **Compute** | Virtual warehouse running time | Snowflake Credits | $2–4 per credit |
| **Storage** | Data stored (compressed) | TB per month | ~$23–40 per TB/month |
| **Cloud Services** | Usually free | Included | Free if < 10% of daily compute |
| **Data Transfer** | Cross-region or cross-cloud egress | GB | $0.01–$0.08/GB |

> Compute is almost always the dominant cost — optimize warehouses first.

---

## Credits Explained

1 credit = 1 X-Small warehouse running for 1 hour.

| Warehouse Size | Credits/Hour | Relative Power |
|----------------|-------------|----------------|
| X-Small | 1 | 1x |
| Small | 2 | 2x |
| Medium | 4 | 4x |
| Large | 8 | 8x |
| X-Large | 16 | 16x |
| 2X-Large | 32 | 32x |
| 3X-Large | 64 | 64x |
| 4X-Large | 128 | 128x |

**Key insight:** Bigger warehouse = faster query but same total credits per query. A 1-hour query on XS (1 credit) = a 7.5-minute query on XL (16 credits / 8 = still ~2 credits). Bigger is cheaper for long queries; smaller is fine for interactive queries.

---

## Auto-Suspend: The Most Important Cost Control

```sql
-- Set auto-suspend to 60 seconds (saves credits between queries)
CREATE WAREHOUSE analytics_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60         -- Suspend after 60 seconds idle
    AUTO_RESUME = TRUE;       -- Resume automatically when a query arrives

-- Change existing warehouse
ALTER WAREHOUSE analytics_wh SET AUTO_SUSPEND = 60;

-- Manually suspend immediately
ALTER WAREHOUSE analytics_wh SUSPEND;

-- Manually resume
ALTER WAREHOUSE analytics_wh RESUME;
```

**Minimum billing:** 60 seconds per session. A warehouse that resumes, runs a 5-second query, then suspends costs 60 seconds of credit — not 5. Plan queries accordingly.

---

## Checking Your Costs

```sql
-- Credit usage by warehouse (last 30 days)
SELECT warehouse_name, SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Storage usage (current)
SELECT table_catalog, table_schema, table_name,
       ACTIVE_BYTES / 1e9 AS active_gb,
       TIME_TRAVEL_BYTES / 1e9 AS time_travel_gb,
       FAILSAFE_BYTES / 1e9 AS failsafe_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY ACTIVE_BYTES DESC
LIMIT 20;

-- Most expensive queries (last 7 days)
SELECT query_text, warehouse_name,
       CREDITS_USED_CLOUD_SERVICES,
       TOTAL_ELAPSED_TIME / 1000 AS duration_seconds,
       execution_status
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;
```

---

## Storage Cost Optimization

```sql
-- 1. Transient tables: no time-travel, no fail-safe (staging/temp data)
CREATE TRANSIENT TABLE staging.orders_raw (
    order_id INT,
    raw_json VARIANT,
    loaded_at TIMESTAMP
);
-- Saves: 7-day time-travel + 7-day fail-safe storage cost

-- 2. Temporary tables: auto-dropped at session end
CREATE TEMPORARY TABLE temp_calc AS
SELECT * FROM fact_sales WHERE sale_date = CURRENT_DATE();
-- No storage cost beyond the session

-- 3. Reduce time-travel retention for large tables
ALTER TABLE large_log_table SET DATA_RETENTION_TIME_IN_DAYS = 1;  -- default is 1, max is 90

-- 4. Drop unused tables and schemas
SHOW TABLES IN DATABASE analytics;
-- Review last_altered dates — drop tables not touched in 90+ days
```

---

## Resource Monitors

Set credit limits with automatic actions (warn, suspend):

```sql
-- Account-level monitor: alert when 80% spent, suspend at 100%
CREATE RESOURCE MONITOR monthly_limit
    WITH CREDIT_QUOTA = 5000     -- 5000 credits per month
    TRIGGERS
        ON 80 PERCENT DO NOTIFY            -- email notification at 80%
        ON 100 PERCENT DO SUSPEND;         -- suspend ALL warehouses at 100%

ALTER ACCOUNT SET RESOURCE_MONITOR = monthly_limit;

-- Warehouse-level monitor (overrides account monitor for that warehouse)
CREATE RESOURCE MONITOR dev_limit
    WITH CREDIT_QUOTA = 200
    FREQUENCY = WEEKLY
    TRIGGERS
        ON 90 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE dev_wh SET RESOURCE_MONITOR = dev_limit;
```

---

## Try It Yourself

```sql
-- Find your top 5 most expensive warehouses this week
SELECT
    warehouse_name,
    ROUND(SUM(credits_used), 2) AS credits_this_week,
    ROUND(SUM(credits_used) * 3.0, 2) AS estimated_cost_usd  -- ~$3/credit
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY credits_this_week DESC
LIMIT 5;
```

---

## Interview Tips

> **Tip 1:** "How does Snowflake pricing work?" — "Two main costs: compute (credits per second of warehouse runtime) and storage (per TB/month of compressed data). Cloud services are included unless they exceed 10% of daily compute credits. Credit price depends on edition and region — typically $2–4/credit."

> **Tip 2:** "What's the single most impactful cost optimization?" — "Auto-suspend. A warehouse that stays running idle for 8 hours overnight burns 8 × credits_per_hour with zero benefit. Setting `AUTO_SUSPEND = 60` on all warehouses is the first thing every Snowflake account should do."

> **Tip 3:** "What's a Resource Monitor?" — "A credit quota + trigger policy on a warehouse or account. When usage hits the threshold, Snowflake can notify (email) or suspend the warehouse automatically. It's a guardrail — prevents runaway queries or scripts from blowing the monthly budget."
