---
title: "Date & Time Functions - Fundamentals"
topic: sql
subtopic: date-time-functions
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, date-time, functions, dateadd, datediff, date-trunc, extract]
---

# Date & Time Functions — Fundamentals

Dates and timestamps are everywhere in data engineering: event logs, user signups, order timestamps, SCD2 windows. Interviewers test date functions constantly because bugs here silently corrupt metrics.

---

## Current Date / Timestamp

Getting "now" looks deceptively simple — each dialect has its own spelling.

| Intent | Postgres | BigQuery | Snowflake | Spark SQL |
|---|---|---|---|---|
| Current date | `CURRENT_DATE` | `CURRENT_DATE()` | `CURRENT_DATE` | `CURRENT_DATE` |
| Current timestamp (with TZ) | `NOW()` or `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP()` | `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP` |
| Current timestamp (no TZ) | `LOCALTIMESTAMP` | — | `LOCALTIMESTAMP` | `NOW()` |

```sql
-- Postgres / Snowflake
SELECT CURRENT_DATE,         -- 2026-06-15
       CURRENT_TIMESTAMP,    -- 2026-06-15 14:22:00.123456+00
       NOW();                -- same as CURRENT_TIMESTAMP in Postgres

-- BigQuery
SELECT CURRENT_DATE(),
       CURRENT_TIMESTAMP();

-- Spark SQL
SELECT CURRENT_DATE(), CURRENT_TIMESTAMP();
```

---

## Date Arithmetic — Adding and Subtracting

### DATEADD / DATE_ADD / Interval Math

```sql
-- Postgres: interval syntax
SELECT order_date + INTERVAL '30 days'     AS due_date,
       order_date - INTERVAL '1 year'      AS last_year_date
FROM orders;

-- BigQuery
SELECT DATE_ADD(order_date, INTERVAL 30 DAY)  AS due_date,
       DATE_SUB(order_date, INTERVAL 1 YEAR)  AS last_year_date
FROM orders;

-- Snowflake
SELECT DATEADD(day, 30, order_date)   AS due_date,
       DATEADD(year, -1, order_date)  AS last_year_date
FROM orders;

-- Spark SQL
SELECT DATE_ADD(order_date, 30)       AS due_date,
       ADD_MONTHS(order_date, -12)    AS last_year_date
FROM orders;
```

### DATEDIFF — Days Between Two Dates

```sql
-- Postgres
SELECT (end_date::date - start_date::date) AS days_diff;

-- BigQuery
SELECT DATE_DIFF(end_date, start_date, DAY) AS days_diff;

-- Snowflake
SELECT DATEDIFF('day', start_date, end_date) AS days_diff;

-- Spark SQL
SELECT DATEDIFF(end_date, start_date) AS days_diff;  -- end - start
```

> Interview gotcha: Snowflake `DATEDIFF` argument order is `(unit, start, end)`. BigQuery is `(end, start, unit)`. Mixing these up is a classic production bug.

---

## EXTRACT and DATE_PART

Extract a single component (year, month, day, hour, etc.) from a date or timestamp.

```sql
-- Postgres / Snowflake / BigQuery
SELECT EXTRACT(YEAR  FROM order_date) AS yr,
       EXTRACT(MONTH FROM order_date) AS mo,
       EXTRACT(DOW   FROM order_date) AS day_of_week,   -- 0=Sunday in Postgres
       EXTRACT(EPOCH FROM created_at) AS unix_ts;

-- Snowflake also supports DATE_PART (alias)
SELECT DATE_PART('month', order_date) AS mo;

-- BigQuery alternative
SELECT EXTRACT(DAYOFWEEK FROM order_date);  -- 1=Sunday in BigQuery

-- Spark SQL
SELECT YEAR(order_date), MONTH(order_date), DAYOFWEEK(order_date);
```

---

## DATE_TRUNC — Grouping by Week, Month, Quarter

`DATE_TRUNC` rounds a date/timestamp DOWN to the start of the specified period. This is the foundation of time-series aggregations.

```sql
-- Postgres / Snowflake
SELECT DATE_TRUNC('month',   created_at) AS month_start,
       DATE_TRUNC('week',    created_at) AS week_start,   -- Monday in Postgres
       DATE_TRUNC('quarter', created_at) AS quarter_start,
       DATE_TRUNC('year',    created_at) AS year_start
FROM events;

-- BigQuery
SELECT DATE_TRUNC(created_at, MONTH)   AS month_start,
       DATE_TRUNC(created_at, WEEK)    AS week_start,  -- Sunday in BigQuery
       DATE_TRUNC(created_at, QUARTER) AS quarter_start;

-- Spark SQL
SELECT DATE_TRUNC('MM',  created_at) AS month_start,
       TRUNC(created_at, 'MM')       AS month_start_alt;
```

Typical usage in interview-style aggregation:

```sql
-- Monthly revenue trend
SELECT DATE_TRUNC('month', order_date) AS month,
       SUM(revenue)                     AS total_revenue
FROM orders
GROUP BY 1
ORDER BY 1;
```

---

## Casting Strings to Dates

```sql
-- Postgres
SELECT '2024-03-15'::date,
       CAST('2024-03-15' AS DATE),
       TO_DATE('15/03/2024', 'DD/MM/YYYY');

-- BigQuery
SELECT DATE('2024-03-15'),
       PARSE_DATE('%Y-%m-%d', '2024-03-15');

-- Snowflake
SELECT TO_DATE('2024-03-15'),
       TRY_TO_DATE('bad-value') AS safe_cast;  -- returns NULL instead of error

-- Spark SQL
SELECT TO_DATE('2024-03-15', 'yyyy-MM-dd'),
       CAST('2024-03-15' AS DATE);
```

Use `TRY_TO_DATE` / `SAFE.CAST` / `TRY_CAST` when input data is dirty to avoid pipeline failures.

---

## Format Functions — TO_CHAR and DATE_FORMAT

Convert dates to formatted strings for display.

```sql
-- Postgres
SELECT TO_CHAR(order_date, 'YYYY-MM')    AS year_month,
       TO_CHAR(order_date, 'Month YYYY') AS friendly_date;

-- Snowflake
SELECT TO_CHAR(order_date, 'YYYY-MM') AS year_month;

-- BigQuery
SELECT FORMAT_DATE('%Y-%m', order_date)  AS year_month,
       FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', created_at) AS iso_string;

-- Spark SQL / MySQL
SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS year_month;
```

---

## Common Interview Date Patterns

### Age Calculation

```sql
-- Postgres: exact age in years
SELECT EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date)) AS age_years;

-- Universal approach (any dialect)
SELECT FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS age_years;

-- BigQuery
SELECT DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) AS age_years;

-- Handle NULL birth_date
SELECT COALESCE(
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date)),
    -1
) AS age_years;
```

### First and Last Day of Month

```sql
-- Postgres
SELECT DATE_TRUNC('month', CURRENT_DATE)                             AS first_day,
       (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::date AS last_day;

-- BigQuery
SELECT DATE_TRUNC(CURRENT_DATE(), MONTH)                            AS first_day,
       LAST_DAY(CURRENT_DATE())                                     AS last_day;

-- Snowflake
SELECT DATE_TRUNC('month', CURRENT_DATE)                            AS first_day,
       LAST_DAY(CURRENT_DATE)                                       AS last_day;
```

### Days Between Two Dates

```sql
SELECT user_id,
       signup_date,
       first_purchase_date,
       DATEDIFF(first_purchase_date, signup_date) AS days_to_convert   -- Spark/Snowflake
FROM users
WHERE first_purchase_date IS NOT NULL;
```

---

## Quick Reference: Dialect Cheat Sheet

| Operation | Postgres | BigQuery | Snowflake | Spark SQL |
|---|---|---|---|---|
| Add days | `+ INTERVAL '7 days'` | `DATE_ADD(d, INTERVAL 7 DAY)` | `DATEADD(day,7,d)` | `DATE_ADD(d,7)` |
| Diff in days | `d2-d1` | `DATE_DIFF(d2,d1,DAY)` | `DATEDIFF('day',d1,d2)` | `DATEDIFF(d2,d1)` |
| Truncate month | `DATE_TRUNC('month',t)` | `DATE_TRUNC(t,MONTH)` | `DATE_TRUNC('month',t)` | `TRUNC(t,'MM')` |
| Extract year | `EXTRACT(YEAR FROM d)` | `EXTRACT(YEAR FROM d)` | `YEAR(d)` | `YEAR(d)` |
| Format string | `TO_CHAR(d,'YYYY-MM')` | `FORMAT_DATE('%Y-%m',d)` | `TO_CHAR(d,'YYYY-MM')` | `DATE_FORMAT(d,'yyyy-MM')` |
| Safe cast | `TRY_CAST` | `SAFE.CAST` | `TRY_TO_DATE` | `TRY_CAST` |

---

## Key Takeaways

- Always know which dialect you're in — `DATE_ADD` and `DATEADD` argument orders differ
- `DATE_TRUNC` is your best friend for time-series grouping
- Use `TRY_TO_DATE` / `SAFE.CAST` in production to avoid pipeline crashes on bad dates
- `EXTRACT(DOW ...)` returns 0=Sunday in Postgres but 1=Sunday in BigQuery — verify before using for weekday filters
