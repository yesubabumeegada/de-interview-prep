---
title: "Date & Time Functions - Real-World"
topic: sql
subtopic: date-time-functions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, date-time, production-bugs, timezone, fiscal-year, date-trunc, partition-pruning, interview-tips]
---

# Date & Time Functions — Real-World

These are the date bugs that actually happen in production data pipelines — the kind that show up on your pager at 2 AM, cost the company hours of engineer time to debug, and make great interview stories.

---

## Production Bug #1: NYC → UTC Migration Loses 5 Hours of Data

**Scenario**: A company migrated their event logging from `America/New_York` local time to UTC. The migration script converted all existing timestamps correctly. But the ETL job that backfilled the last 7 days had a one-line bug:

```sql
-- The bug: converting an already-UTC timestamp as if it were Eastern time
UPDATE events
SET event_ts = event_ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'
WHERE event_date = '2024-03-10'
  AND event_ts < '2024-03-10 05:00:00';  -- naive UTC-5 cutoff

-- This double-converts times that were ALREADY in UTC
-- A 2024-03-10 01:00:00 UTC event becomes 2024-03-10 06:00:00 UTC
-- effectively shifting it forward 5 hours
```

**Result**: 5 hours of events appear to happen in the future. Dashboards show a gap in NYC evening data.

**Fix**: Always tag your conversion direction explicitly and validate with `MIN/MAX event_ts` before committing.

```sql
-- Validate before committing migration
SELECT MIN(event_ts), MAX(event_ts), COUNT(*)
FROM events
WHERE event_date = '2024-03-10';
-- Compare to expected range: should be '2024-03-10 00:00:00 UTC' to '2024-03-10 23:59:59 UTC'
```

---

## Production Bug #2: Fiscal Year Offset Causing Wrong YoY Comparisons

**Scenario**: An e-commerce company's fiscal year starts February 1. An analyst wrote a YoY revenue comparison using `EXTRACT(YEAR ...)`:

```sql
-- Buggy: uses calendar year, not fiscal year
SELECT
    EXTRACT(YEAR FROM order_date)           AS year,
    SUM(revenue)                            AS revenue
FROM orders
GROUP BY 1;
-- January 2024 orders are in "2024" but fiscally they belong to FY2023
```

**Result**: FY2024 Q4 (November + December + January) is split across two calendar years. YoY looks wrong because January revenue appears in the wrong year.

**Fix**: Apply fiscal year offset before extracting year.

```sql
-- Correct: shift the date back 1 month so FY Feb=month1, Jan=month12
SELECT
    EXTRACT(YEAR FROM order_date - INTERVAL '1 month')  AS fiscal_year,
    SUM(revenue)                                         AS revenue
FROM orders
GROUP BY 1;
```

Or better: join to `dim_date` with a precomputed `fiscal_year` column.

---

## Production Bug #3: DATE_TRUNC('week') Returns Monday vs Sunday Depending on Dialect

**Scenario**: A Postgres query was migrated to BigQuery as part of a warehouse migration. The weekly cohort report started showing different numbers.

```sql
-- Postgres: DATE_TRUNC('week', ...) returns the MONDAY of the week (ISO 8601)
SELECT DATE_TRUNC('week', '2024-03-13'::date);  -- Returns 2024-03-11 (Monday)

-- BigQuery: DATE_TRUNC(date, WEEK) returns the SUNDAY of the week
SELECT DATE_TRUNC('2024-03-13', WEEK);           -- Returns 2024-03-10 (Sunday)
```

**Result**: Weekly buckets are misaligned by 1 day. Users appear to drop from one cohort to another. Week-over-week trends are skewed.

**Fix**: Be explicit about the week start in BigQuery.

```sql
-- BigQuery: force ISO week (Monday start) to match Postgres behavior
SELECT DATE_TRUNC(event_date, ISOWEEK)  AS week_monday;  -- BigQuery ISOWEEK = Monday

-- Or in Postgres: verify expected week start
SELECT DATE_TRUNC('week', CURRENT_DATE),         -- always Monday
       DATE_TRUNC('week', CURRENT_DATE) + 6      -- always Sunday
```

---

## Production Bug #4: Partition Pruning Breaks When CAST Applied to Partition Column

**Scenario**: A Spark table is partitioned by `event_date DATE`. An analyst writes:

```sql
-- Breaks partition pruning — scans ALL partitions
SELECT COUNT(*)
FROM events
WHERE CAST(event_date AS STRING) LIKE '2024-03%';

-- Also breaks pruning:
WHERE DATE_FORMAT(event_date, 'yyyy-MM') = '2024-03'
WHERE YEAR(event_date) = 2024 AND MONTH(event_date) = 3
```

**Result**: A query expected to scan 31 partitions (31 days of March) scans the entire 3-year table (1000+ partitions). The query takes 45 minutes instead of 45 seconds and costs $200 instead of $4.

**Fix**: Use explicit range comparisons on the raw partition column.

```sql
-- Works: explicit range on partition column
WHERE event_date >= '2024-03-01' AND event_date < '2024-04-01'

-- Also works in BigQuery (partition decorator syntax)
SELECT * FROM `project.dataset.events` WHERE _PARTITIONDATE >= '2024-03-01'
```

---

## Interview Tips for Date Questions

### Framework: SPACE (State, Precision, Arithmetic, Conversion, Edge cases)

When given a date-related interview problem, walk through:

1. **State** — What timezone is the data stored in? Is there a timezone column?
2. **Precision** — Seconds, milliseconds, microseconds? Is it a DATE or TIMESTAMP?
3. **Arithmetic** — What dialect are we in? DATEADD vs DATE_ADD vs interval math?
4. **Conversion** — Do we need to convert timezone before or after comparison?
5. **Edge cases** — NULL dates, end-of-month arithmetic, leap years, DST

### Common Interview Date Questions and What They're Really Testing

| Question | What they're really testing |
|---|---|
| "Find users active in the last 30 days" | Rolling vs fixed window; INDEX usage |
| "Calculate age from birth_date" | NULL handling; floor vs truncation |
| "Week-over-week retention" | DATE_TRUNC week start; timezone |
| "Same period last year" | Leap year handling; fiscal vs calendar |
| "Partition by month and query March" | Partition pruning anti-patterns |

### When to Ask Clarifying Questions

In a real interview, it's a green flag to ask:
- "Is the timestamp stored in UTC or local time?"
- "Does the company use fiscal year or calendar year?"
- "Should 'week' start on Monday or Sunday?"
- "How should I handle NULL dates — exclude or treat as unknown?"

### Syntax to Memorize by Dialect

```sql
-- Quickest safe way to get "first day of current month" in each dialect
-- Postgres:
SELECT DATE_TRUNC('month', CURRENT_DATE);
-- BigQuery:
SELECT DATE_TRUNC(CURRENT_DATE(), MONTH);
-- Snowflake:
SELECT DATE_TRUNC('month', CURRENT_DATE());
-- Spark:
SELECT TRUNC(CURRENT_DATE(), 'MM');
```

---

## Checklist Before Pushing Date Logic to Production

- [ ] All timestamps stored in UTC, not local time
- [ ] IANA timezone names used (not offset strings like `-05:00`)
- [ ] `DATE_TRUNC('week')` behavior verified in target dialect (Monday vs Sunday)
- [ ] Partition column comparisons use raw column values, not function results
- [ ] Fiscal year offset applied before EXTRACT(YEAR) if company uses fiscal year
- [ ] NULL date handling explicit (COALESCE or IS NOT NULL filter)
- [ ] Rolling window uses `>= start AND < end`, not `BETWEEN` (BETWEEN is inclusive on both ends)
- [ ] Late-arriving data tolerance documented and reprocessing window tested
