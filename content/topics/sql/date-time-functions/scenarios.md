---
title: "Date & Time Functions - Scenario Questions"
topic: sql
subtopic: date-time-functions
content_type: scenario_question
tags: [sql, date-time, timezone, rolling-window, fiscal-week]
---

# Date & Time Functions — Scenario Questions

<article data-difficulty="junior">

## Scenario 1 (Junior): Calculate User Age from birth_date

You have a `users` table:

```
users(user_id INT, name VARCHAR, birth_date DATE)
```

Some users have `NULL` in `birth_date`. Write a query that returns each user's age in complete years. For users with a NULL birth_date, return -1.

**What interviewers are looking for**: `EXTRACT(YEAR FROM AGE(...))` or equivalent arithmetic, NULL handling with COALESCE or CASE WHEN, awareness that floor-division on `DATEDIFF/365.25` is more portable.

<details>
<summary>✅ Solution</summary>

```sql
-- Postgres
SELECT
    user_id,
    name,
    birth_date,
    COALESCE(
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date))::INT,
        -1
    ) AS age_years
FROM users;

-- Alternative using DATEDIFF — works in Snowflake and Spark too
SELECT
    user_id,
    name,
    birth_date,
    CASE
        WHEN birth_date IS NULL THEN -1
        ELSE FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25)
    END AS age_years
FROM users;

-- BigQuery
SELECT
    user_id,
    name,
    birth_date,
    CASE
        WHEN birth_date IS NULL THEN -1
        ELSE DATE_DIFF(CURRENT_DATE(), birth_date, YEAR)
    END AS age_years
FROM users;
```

**Key points to mention**:
- `AGE(CURRENT_DATE, birth_date)` gives an interval; wrap with `EXTRACT(YEAR FROM ...)` to get integer years
- `FLOOR(DATEDIFF / 365.25)` handles leap years; using plain 365 can give an off-by-one in leap years
- `COALESCE(..., -1)` or `CASE WHEN birth_date IS NULL` are both fine — be explicit about the NULL strategy
- Don't use `DATEDIFF / 365` (integer arithmetic in some dialects truncates rather than floors)

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2 (Mid): Rolling 7-Day Active Users with Timezone Handling

You have an `events` table where `event_ts` is stored in UTC:

```
events(user_id INT, event_ts TIMESTAMP WITH TIME ZONE, event_type VARCHAR)
```

The business team wants to see **daily active users (DAU) for the last 30 days, counted in US/Pacific time** (so a "day" runs midnight-to-midnight Pacific, not UTC). Rolling 7-day active users = distinct users who had at least one event in the 7 calendar days ending on each date.

Write a query that produces: `pacific_date DATE, rolling_7d_dau INT`

<details>
<summary>✅ Solution</summary>

```sql
-- Postgres
WITH events_pacific AS (
    -- Step 1: convert event_ts to Pacific time and extract the date
    SELECT
        user_id,
        (event_ts AT TIME ZONE 'America/Los_Angeles')::date AS pacific_date
    FROM events
    WHERE event_ts >= NOW() - INTERVAL '37 days'  -- extra buffer for timezone shift
),
date_spine AS (
    -- Step 2: generate the last 30 days in Pacific time
    SELECT generate_series(
        (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date - 29,
        (CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles')::date,
        INTERVAL '1 day'
    )::date AS pacific_date
),
daily_users AS (
    -- Step 3: one row per user per Pacific day
    SELECT DISTINCT user_id, pacific_date
    FROM events_pacific
)
-- Step 4: for each date in the spine, count distinct users in the 7-day window ending that date
SELECT
    ds.pacific_date,
    COUNT(DISTINCT du.user_id) AS rolling_7d_dau
FROM date_spine ds
LEFT JOIN daily_users du
  ON du.pacific_date > ds.pacific_date - 7
  AND du.pacific_date <= ds.pacific_date
GROUP BY 1
ORDER BY 1;

-- Snowflake equivalent (replace generate_series with GENERATOR)
-- BigQuery equivalent (replace generate_series with GENERATE_DATE_ARRAY)
```

**Key points to mention**:
- Convert to Pacific time BEFORE extracting the date — otherwise events near midnight are bucketed to the wrong day
- Use IANA timezone name (`America/Los_Angeles`) not a fixed offset (`-08:00`) — the offset changes with DST
- Generate a date spine so days with 0 active users still appear as 0 (not missing)
- The 7-day window uses `> date - 7 AND <= date` (open-left, closed-right) — this includes exactly 7 days
- Buffer the UTC filter by 37 days, not 30, to account for the timezone shift at the edges

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3 (Senior): Same-Week-Last-Year Revenue Handling Fiscal Weeks and DST

You work at a retail company whose fiscal week starts on Sunday. The fiscal year has 52 or 53 weeks (52 most years, 53 in years when Jan 1 falls on a certain day). The company tracks `weekly_revenue` in a table:

```
weekly_revenue(revenue_week_start DATE, region VARCHAR, revenue DECIMAL)
-- revenue_week_start is always a Sunday (the start of the fiscal week)
```

Stakeholders want a report showing current fiscal year weekly revenue vs the **same fiscal week in the prior fiscal year** (not simply 52 weeks ago — fiscal week numbers must match).

Design a query that produces:
`fiscal_year INT, fiscal_week_num INT, revenue DECIMAL, prior_year_revenue DECIMAL, yoy_growth_pct DECIMAL`

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: build a fiscal week mapping CTE
-- Fiscal year starts on the Sunday on or before Feb 1 of each calendar year
-- (adjust anchor logic per company's actual fiscal calendar)
WITH fiscal_weeks AS (
    SELECT
        d.date_actual                    AS week_start,  -- from dim_date
        d.fiscal_year,
        -- Fiscal week = number of Sundays from FY start through this week
        ROW_NUMBER() OVER (
            PARTITION BY d.fiscal_year
            ORDER BY d.date_actual
        ) AS fiscal_week_num
    FROM dim_date d
    WHERE d.day_of_week = 1           -- 1 = Sunday in your dim_date convention
      AND d.date_actual BETWEEN '2022-01-01' AND '2026-12-31'
),
-- Step 2: join weekly_revenue to fiscal_weeks for current and prior year
current_year AS (
    SELECT
        fw.fiscal_year,
        fw.fiscal_week_num,
        SUM(wr.revenue) AS revenue
    FROM weekly_revenue wr
    JOIN fiscal_weeks fw ON wr.revenue_week_start = fw.week_start
    GROUP BY 1, 2
),
prior_year AS (
    SELECT
        fw.fiscal_year + 1      AS next_fiscal_year,  -- align to current year's perspective
        fw.fiscal_week_num,
        SUM(wr.revenue)         AS revenue
    FROM weekly_revenue wr
    JOIN fiscal_weeks fw ON wr.revenue_week_start = fw.week_start
    GROUP BY fw.fiscal_year, fw.fiscal_week_num
)
-- Step 3: join on fiscal week number
SELECT
    cy.fiscal_year,
    cy.fiscal_week_num,
    cy.revenue,
    py.revenue                                    AS prior_year_revenue,
    ROUND(
        100.0 * (cy.revenue - py.revenue) / NULLIF(py.revenue, 0),
        2
    )                                             AS yoy_growth_pct
FROM current_year cy
LEFT JOIN prior_year py
  ON cy.fiscal_year     = py.next_fiscal_year
  AND cy.fiscal_week_num = py.fiscal_week_num
ORDER BY cy.fiscal_year, cy.fiscal_week_num;
```

**Key design decisions to discuss in interview**:

1. **Fiscal week numbering via ROW_NUMBER**: avoids hard-coding week number math; the `dim_date` table handles the actual calendar logic (including 53-week years)
2. **Week 53 handling**: in 53-week years, week 53 has no prior-year counterpart — the `LEFT JOIN` will produce `NULL` for `prior_year_revenue`, which is correct behavior to surface
3. **DST and week boundaries**: since `revenue_week_start` is already a DATE (not a timestamp), DST doesn't affect the fiscal week boundary. DST would matter if you were grouping raw event timestamps into fiscal weeks — in that case, convert to UTC first
4. **NULLIF(py.revenue, 0)**: prevents division-by-zero if a region had zero revenue last year
5. **Scalability**: the fiscal_weeks CTE can be materialized as a view or loaded into dim_date to avoid recomputing on every run

</details>
</article>
