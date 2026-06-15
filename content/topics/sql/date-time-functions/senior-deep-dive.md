---
title: "Date & Time Functions - Senior Deep Dive"
topic: sql
subtopic: date-time-functions
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, date-time, late-arriving-data, scd2, partition-pruning, date-dimension, timestamp-precision, timezone-bugs]
---

# Date & Time Functions — Senior Deep Dive

Senior DE interviews probe temporal edge cases that only appear at scale: late data, SCD2 point-in-time queries, partition pruning correctness, and DST bugs that affect one hour per year but are impossible to debug after the fact.

---

## Late-Arriving Data: Event Time vs Process Time

In streaming pipelines there are two clocks:
- **event_time**: when the event actually happened (set on the device/service)
- **process_time**: when the event arrived in your warehouse

```sql
-- Naive query: uses process time — looks fast but wrong for late data
SELECT DATE_TRUNC('hour', process_time) AS hour,
       COUNT(*) AS events
FROM raw_events
GROUP BY 1;

-- Correct: use event_time, accept late arrivals via a watermark
SELECT DATE_TRUNC('hour', event_time) AS hour,
       COUNT(*) AS events
FROM raw_events
WHERE event_time >= '2024-03-01'         -- watermark lower bound
  AND event_time <  '2024-03-02'         -- the window being finalized
  AND process_time >= '2024-03-01'       -- only data processed so far
  AND process_time <  '2024-03-03'       -- 2-day late arrival tolerance
GROUP BY 1;
```

The watermark is the maximum event time the pipeline has seen minus an acceptable lateness threshold. SQL jobs on warehouses typically implement this as a scheduled reprocessing window.

### Handling Reprocessing

```sql
-- Idempotent hourly aggregate that handles late arrivals
-- Run every hour, reprocess last 48 hours (late arrival tolerance)
DELETE FROM hourly_events_agg
WHERE event_hour >= CURRENT_TIMESTAMP - INTERVAL '48 hours';

INSERT INTO hourly_events_agg
SELECT DATE_TRUNC('hour', event_time) AS event_hour,
       COUNT(*)                        AS event_count,
       CURRENT_TIMESTAMP               AS processed_at
FROM raw_events
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
GROUP BY 1;
```

---

## SCD2 Effective/End Date Patterns

Slowly Changing Dimension Type 2 tables track history by adding `effective_date` and `end_date` columns. Querying them correctly requires careful date boundary handling.

```sql
-- SCD2 customer dimension schema
CREATE TABLE dim_customer (
    customer_key    BIGINT,
    customer_id     INT,           -- natural key
    name            VARCHAR,
    tier            VARCHAR,
    effective_date  DATE NOT NULL,
    end_date        DATE,          -- NULL means current record
    is_current      BOOLEAN
);

-- Point-in-time query: what tier was customer 42 on 2023-06-15?
SELECT tier
FROM dim_customer
WHERE customer_id = 42
  AND effective_date <= '2023-06-15'
  AND (end_date > '2023-06-15' OR end_date IS NULL);

-- Join a fact table to SCD2 dimension at event time
SELECT f.order_id,
       f.order_date,
       c.tier AS customer_tier_at_order_time,
       f.revenue
FROM fact_orders f
JOIN dim_customer c
  ON f.customer_id = c.customer_id
  AND f.order_date >= c.effective_date
  AND f.order_date <  COALESCE(c.end_date, '9999-12-31'::date);
```

> Senior tip: the `COALESCE(end_date, '9999-12-31')` sentinel avoids `OR end_date IS NULL` which can break some query planners. Use consistently across your team.

---

## Partition Pruning — What Works and What Doesn't

Date partitioning is the primary cost-control mechanism in BigQuery, Snowflake, and Spark. Bad date predicates silently scan entire tables.

```sql
-- Partition column: event_date DATE

-- PRUNING WORKS: direct comparison on partition column
WHERE event_date = '2024-03-15'
WHERE event_date >= '2024-03-01' AND event_date < '2024-04-01'
WHERE event_date BETWEEN '2024-03-01' AND '2024-03-31'

-- PRUNING BREAKS: applying a function to the partition column
WHERE DATE_TRUNC('month', event_date) = '2024-03-01'  -- full scan!
WHERE YEAR(event_date) = 2024                          -- full scan!
WHERE CAST(event_date AS VARCHAR) LIKE '2024-03%'      -- full scan!

-- Fix: use explicit range instead of function
-- Bad:
WHERE DATE_TRUNC('month', event_date) = '2024-03-01'
-- Good:
WHERE event_date >= '2024-03-01' AND event_date < '2024-04-01'
```

### Timestamp Partition Columns

```sql
-- Partitioned on event_timestamp TIMESTAMP
-- Still works if filter matches partition granularity
WHERE event_timestamp >= '2024-03-01 00:00:00'
  AND event_timestamp <  '2024-04-01 00:00:00'

-- Breaks: casting timestamp to date for comparison
WHERE DATE(event_timestamp) = '2024-03-15'   -- BigQuery: full scan
-- Fix:
WHERE event_timestamp >= '2024-03-15'
  AND event_timestamp <  '2024-03-16'
```

---

## Date Dimension Table Design

A `dim_date` table pre-computes fiscal periods, holiday flags, and business day indicators so downstream queries don't repeat complex CASE WHEN logic.

```sql
CREATE TABLE dim_date (
    date_key          INT PRIMARY KEY,      -- YYYYMMDD integer for fast joins
    date_actual       DATE NOT NULL,
    year_actual       INT,
    quarter_actual    INT,
    month_actual      INT,
    week_of_year      INT,
    day_of_week       INT,                  -- 1=Monday ... 7=Sunday (ISO)
    is_weekend        BOOLEAN,
    is_holiday        BOOLEAN,
    is_business_day   BOOLEAN,
    fiscal_year       INT,
    fiscal_quarter    INT,
    fiscal_week       INT,
    fiscal_month      INT,
    quarter_start     DATE,
    quarter_end       DATE,
    fiscal_year_start DATE,
    fiscal_year_end   DATE
);

-- Usage: join to avoid repeated CASE WHEN
SELECT d.fiscal_year,
       d.fiscal_quarter,
       SUM(f.revenue) AS revenue
FROM fact_orders f
JOIN dim_date d ON f.order_date = d.date_actual
WHERE d.fiscal_year = 2024
GROUP BY 1, 2
ORDER BY 1, 2;
```

Building the date dimension is usually a one-time script using `generate_series`:

```sql
-- Postgres: populate dim_date for 10 years
INSERT INTO dim_date (date_key, date_actual, year_actual, ...)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT           AS date_key,
    d                                      AS date_actual,
    EXTRACT(YEAR FROM d)::INT              AS year_actual,
    EXTRACT(QUARTER FROM d)::INT           AS quarter_actual,
    EXTRACT(MONTH FROM d)::INT             AS month_actual,
    EXTRACT(ISODOW FROM d)::INT            AS day_of_week,  -- ISO: 1=Mon
    EXTRACT(ISODOW FROM d) IN (6, 7)       AS is_weekend,
    CASE WHEN EXTRACT(YEAR FROM d+1) = EXTRACT(YEAR FROM d)
         THEN EXTRACT(YEAR FROM d)
         ELSE EXTRACT(YEAR FROM d) - 1 END AS fiscal_year   -- FY ends Jan 31
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, INTERVAL '1 day') d;
```

---

## Timestamp Precision Issues

Different systems use different timestamp resolutions — mixing them causes silent data loss or comparison failures.

| System | Default precision | Storage |
|---|---|---|
| Postgres TIMESTAMP | microseconds (6) | 8 bytes |
| MySQL DATETIME | microseconds | 8 bytes |
| BigQuery TIMESTAMP | microseconds | — |
| Snowflake TIMESTAMP_NTZ(9) | nanoseconds | — |
| Spark TIMESTAMP | microseconds | — |
| Python datetime | microseconds | — |
| Java Instant | nanoseconds | — |

```sql
-- Snowflake: subsecond precision matters for deduplication
SELECT *
FROM events
WHERE event_ts = '2024-03-15 14:22:00.123456789'::TIMESTAMP_NTZ(9);
-- If you cast to TIMESTAMP_NTZ(6), you truncate the last 3 digits — potential duplicates

-- Comparing across precision boundaries
SELECT COUNT(DISTINCT event_ts::TIMESTAMP(3)) AS ms_distinct,   -- milliseconds
       COUNT(DISTINCT event_ts)               AS us_distinct    -- microseconds
FROM high_freq_events;
-- The ms count may be lower — multiple events sharing the same millisecond bucket
```

---

## Common Timezone Bugs in Production

### DST Gap — Spring Forward Loses Data

When clocks spring forward (e.g., 2:00 AM → 3:00 AM), events logged in the local clock during that hour don't have valid UTC equivalents. If your ETL trusts local timestamps:

```sql
-- Events with local timestamps 02:xx in EST on 2024-03-10 are invalid
-- Any join/comparison against UTC will fail silently
SELECT *
FROM events
WHERE local_time BETWEEN '2024-03-10 02:00:00' AND '2024-03-10 02:59:59';
-- Returns 0 rows in systems that correctly model DST — those times don't exist
```

### DST Fall Back — Ambiguous Hour

When clocks fall back (3:00 AM → 2:00 AM), the hour 2:00–2:59 occurs TWICE. If stored as UTC this is unambiguous; if stored as a naive local timestamp, you can't tell which occurrence it is.

```sql
-- Ambiguous: which 02:30 is this?
'2024-11-03 02:30:00'  -- EDT (UTC-4) or EST (UTC-5)?

-- Unambiguous UTC versions:
'2024-11-03 06:30:00 UTC'   -- first occurrence (EDT)
'2024-11-03 07:30:00 UTC'   -- second occurrence (EST)
```

### Offset vs. IANA Timezone Names

Never hardcode UTC offsets (`-05:00`). DST makes them wrong for half the year.

```sql
-- Wrong: '-05:00' is only correct during EST, not EDT
SELECT ts AT TIME ZONE '-05:00' FROM events;

-- Correct: IANA name handles DST automatically
SELECT ts AT TIME ZONE 'America/New_York' FROM events;
```

---

## Key Takeaways

- Prefer event_time over process_time in aggregations; implement late-data tolerance via reprocessing windows
- SCD2 point-in-time joins require `effective_date <= query_date AND end_date > query_date` — use `COALESCE(end_date, '9999-12-31')` for cleaner predicates
- Never apply functions to partition columns in WHERE clauses — use explicit date ranges
- Build a `dim_date` table to avoid repeated fiscal/holiday logic in every query
- Timestamp precision mismatches cause silent deduplication bugs — normalize to microseconds at ingestion
- Use IANA timezone names, never UTC offsets
