---
title: "Date & Time Functions - Intermediate"
topic: sql
subtopic: date-time-functions
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, date-time, timezone, generate-series, fiscal-year, rolling-window, epoch]
---

# Date & Time Functions — Intermediate

Once you master the basics, interviewers push to timezone correctness, generating date ranges, fiscal calendars, and writing rolling window filters that actually work in production.

---

## Timezone Handling

### AT TIME ZONE — Converting Stored Timestamps

Data engineers frequently store timestamps in UTC then convert on read. The `AT TIME ZONE` operator handles this.

```sql
-- Postgres: convert UTC timestamp to US/Eastern
SELECT created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS eastern_time
FROM events;

-- Snowflake: CONVERT_TIMEZONE(source_tz, target_tz, timestamp)
SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', created_at) AS eastern_time
FROM events;

-- BigQuery
SELECT DATETIME(created_at, 'America/New_York')   AS eastern_dt,
       TIMESTAMP_MICROS(UNIX_MICROS(created_at))  AS utc_ts   -- keep UTC
FROM events;

-- Spark SQL
SELECT from_utc_timestamp(created_at, 'America/New_York') AS eastern_time,
       to_utc_timestamp(local_time, 'America/Chicago')     AS utc_time
FROM events;
```

### UTC Best Practices

Always store in UTC, convert at query time. Never store in local time — you lose the ability to reconstruct the original moment during DST transitions.

```sql
-- Good: filter events in a timezone-aware way
SELECT *
FROM events
WHERE created_at >= '2024-03-10 00:00:00'::timestamptz AT TIME ZONE 'America/New_York'
  AND created_at <  '2024-03-11 00:00:00'::timestamptz AT TIME ZONE 'America/New_York';

-- Bad: comparing a UTC timestamp to a naive local datetime string
WHERE created_at >= '2024-03-10 00:00:00'  -- What timezone is this?
```

### DST Pitfalls

Daylight saving time creates two dangerous moments each year:
- **Spring forward**: 2:00 AM becomes 3:00 AM — one hour of timestamps doesn't exist
- **Fall back**: 2:00 AM occurs twice — one hour of timestamps is ambiguous

```sql
-- Check for DST gap in Postgres: this returns 0 rows during spring-forward gap
SELECT generate_series(
    '2024-03-10 02:00:00 America/New_York'::timestamptz,
    '2024-03-10 02:59:59 America/New_York'::timestamptz,
    INTERVAL '1 minute'
);
-- Returns 0 rows — these timestamps don't exist!
```

---

## Generating Date Series

A date series lets you create a spine of consecutive dates — essential for cohort analysis, filling gaps in time-series, and calendar joins.

### Postgres — generate_series

```sql
-- Generate every day in Q1 2024
SELECT generate_series(
    '2024-01-01'::date,
    '2024-03-31'::date,
    INTERVAL '1 day'
)::date AS day;

-- Generate every month in 2024
SELECT generate_series(
    '2024-01-01'::timestamptz,
    '2024-12-01'::timestamptz,
    INTERVAL '1 month'
)::date AS month_start;
```

### BigQuery — GENERATE_DATE_ARRAY

```sql
-- Every day in Q1
SELECT day
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-03-31', INTERVAL 1 DAY)) AS day;

-- Every Monday in 2024
SELECT day
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31', INTERVAL 1 WEEK)) AS day;
```

### Spark SQL — sequence() + explode()

```sql
-- Every day in Q1
SELECT explode(sequence(DATE '2024-01-01', DATE '2024-03-31', INTERVAL 1 DAY)) AS day;

-- Monthly spine
SELECT explode(sequence(DATE '2024-01-01', DATE '2024-12-01', INTERVAL 1 MONTH)) AS month_start;
```

### Snowflake — GENERATOR

```sql
-- Last 90 days
SELECT DATEADD(day, -SEQ4(), CURRENT_DATE) AS day
FROM TABLE(GENERATOR(ROWCOUNT => 90))
ORDER BY 1;
```

---

## Fiscal Year and Week Calculations

Many companies use fiscal years that don't align with the calendar year (e.g., fiscal year starts Feb 1 or Oct 1).

```sql
-- Postgres: fiscal year starting Feb 1
SELECT order_date,
       CASE
           WHEN EXTRACT(MONTH FROM order_date) >= 2
           THEN EXTRACT(YEAR FROM order_date)
           ELSE EXTRACT(YEAR FROM order_date) - 1
       END AS fiscal_year,
       -- Fiscal quarter (FY starts Feb)
       CASE
           WHEN EXTRACT(MONTH FROM order_date) BETWEEN 2  AND 4  THEN 'Q1'
           WHEN EXTRACT(MONTH FROM order_date) BETWEEN 5  AND 7  THEN 'Q2'
           WHEN EXTRACT(MONTH FROM order_date) BETWEEN 8  AND 10 THEN 'Q3'
           ELSE 'Q4'
       END AS fiscal_quarter
FROM orders;

-- Snowflake: offset months from fiscal start
SELECT order_date,
       YEAR(DATEADD(month, -1, order_date))    AS fiscal_year,  -- FY starts Feb
       CEIL(MONTH(DATEADD(month, -1, order_date)) / 3.0) AS fiscal_quarter
FROM orders;
```

---

## Business Day Calculations

Excluding weekends from date calculations:

```sql
-- Postgres: count business days between two dates
SELECT COUNT(*) AS business_days
FROM generate_series(start_date::date, end_date::date, INTERVAL '1 day') d
WHERE EXTRACT(DOW FROM d) NOT IN (0, 6);  -- 0=Sunday, 6=Saturday

-- Add 5 business days (approximate — for exact, use a calendar table)
WITH RECURSIVE bdays AS (
    SELECT start_date AS d, 0 AS cnt
    UNION ALL
    SELECT d + 1, cnt + CASE WHEN EXTRACT(DOW FROM d+1) NOT IN (0,6) THEN 1 ELSE 0 END
    FROM bdays WHERE cnt < 5
)
SELECT MAX(d) AS five_bdays_later FROM bdays;
```

For production accuracy, join to a `dim_date` table with a `is_business_day` flag.

---

## Rolling Window Date Filters

The most common interview patterns for time-bounded aggregations:

```sql
-- Last 30 days (rolling, relative to today)
SELECT COUNT(*) AS events_30d
FROM events
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days';

-- Month-to-date (MTD)
SELECT SUM(revenue) AS mtd_revenue
FROM orders
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE)
  AND order_date <  CURRENT_DATE + INTERVAL '1 day';

-- Year-to-date (YTD)
SELECT SUM(revenue) AS ytd_revenue
FROM orders
WHERE order_date >= DATE_TRUNC('year', CURRENT_DATE);

-- Same period last year (SPLY)
SELECT SUM(revenue) AS sply_revenue
FROM orders
WHERE order_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
  AND order_date <  CURRENT_DATE - INTERVAL '1 year';

-- Rolling 7-day window using window function (no WHERE clause needed)
SELECT order_date,
       SUM(revenue) OVER (
           ORDER BY order_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS rolling_7d_revenue
FROM daily_revenue;
```

---

## Epoch / Unix Timestamp Conversion

Unix timestamps (seconds since 1970-01-01 UTC) are common in event data from mobile apps and Kafka streams.

```sql
-- Postgres: unix epoch to timestamp
SELECT TO_TIMESTAMP(1710000000)                      AS from_epoch,
       EXTRACT(EPOCH FROM NOW())::bigint             AS to_epoch;

-- BigQuery
SELECT TIMESTAMP_SECONDS(1710000000)                AS from_epoch,
       UNIX_SECONDS(CURRENT_TIMESTAMP())            AS to_epoch;

-- Snowflake
SELECT TO_TIMESTAMP(1710000000)                     AS from_epoch,
       DATE_PART('epoch_second', CURRENT_TIMESTAMP) AS to_epoch;

-- Spark SQL
SELECT TIMESTAMP_SECONDS(1710000000)                AS from_epoch,
       UNIX_TIMESTAMP(CURRENT_TIMESTAMP())          AS to_epoch;

-- Handle milliseconds (divide by 1000 first)
SELECT TO_TIMESTAMP(event_ts_ms / 1000.0) AS event_time
FROM raw_events;  -- event_ts_ms is in milliseconds
```

---

## Key Takeaways

- Always store timestamps in UTC; convert in queries using `AT TIME ZONE` or `CONVERT_TIMEZONE`
- DST means one hour per year doesn't exist (spring forward) and one is ambiguous (fall back) — always use IANA timezone names, not offsets like `-05:00`
- `generate_series` (Postgres), `GENERATE_DATE_ARRAY` (BigQuery), and `sequence()` (Spark) are your spine-building tools for cohort and gap-fill queries
- Rolling date filters should use `>= DATE_TRUNC(...)` not `= EXTRACT(MONTH ...)` — the latter breaks index scans
- Unix epoch milliseconds vs seconds is a common source of off-by-1000x bugs
