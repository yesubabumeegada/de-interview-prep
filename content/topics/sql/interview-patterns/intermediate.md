---
title: "SQL Interview Patterns - Intermediate"
topic: sql
subtopic: interview-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, interview-patterns, funnel, cohort, sessionization, streaks, month-over-month, percentile, median]
---

# SQL Interview Patterns — Intermediate

These patterns appear in mid-level DE and analytics engineer interviews: funnel analysis with real timestamps, cohort retention tables, sessionization, consecutive login streaks, and percentile calculations.

---

## Funnel Analysis — Multi-Step Conversion with Timestamps

A funnel tracks users moving through ordered steps. The key details:
- Each step must happen AFTER the previous step for the same user
- We need time-to-convert at each step, not just presence

```sql
-- events(user_id, event_type, event_ts)
-- Steps: signup → onboarding_complete → first_purchase
WITH step_times AS (
    SELECT
        user_id,
        MIN(event_ts) FILTER (WHERE event_type = 'signup')              AS signup_ts,
        MIN(event_ts) FILTER (WHERE event_type = 'onboarding_complete') AS onboard_ts,
        MIN(event_ts) FILTER (WHERE event_type = 'first_purchase')      AS purchase_ts
    FROM events
    GROUP BY user_id
),
funnel AS (
    SELECT
        user_id,
        signup_ts,
        -- Only count onboarding if it happened AFTER signup
        CASE WHEN onboard_ts > signup_ts THEN onboard_ts END AS onboard_ts,
        -- Only count purchase if it happened AFTER onboarding
        CASE WHEN purchase_ts > onboard_ts THEN purchase_ts END AS purchase_ts
    FROM step_times
    WHERE signup_ts IS NOT NULL
)
SELECT
    COUNT(*)                                 AS signed_up,
    COUNT(onboard_ts)                        AS completed_onboarding,
    COUNT(purchase_ts)                       AS made_first_purchase,
    ROUND(100.0 * COUNT(onboard_ts) / COUNT(*), 1) AS signup_to_onboard_pct,
    ROUND(100.0 * COUNT(purchase_ts) / NULLIF(COUNT(onboard_ts), 0), 1) AS onboard_to_purchase_pct,
    -- Median time to convert (Postgres)
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM onboard_ts - signup_ts) / 3600
    ) AS median_hours_to_onboard
FROM funnel;
```

---

## Cohort Retention Table

A retention table shows what percentage of each signup cohort was still active in week 1, week 2, etc.

```sql
-- users(user_id, signup_date)
-- activity(user_id, activity_date)
WITH cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('week', signup_date) AS cohort_week
    FROM users
    WHERE signup_date >= '2024-01-01'
),
weekly_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('week', activity_date) AS activity_week
    FROM activity
),
retention_raw AS (
    SELECT
        c.cohort_week,
        COUNT(DISTINCT c.user_id)          AS cohort_size,
        -- Week 0: active in signup week
        COUNT(DISTINCT CASE WHEN w.activity_week = c.cohort_week           THEN c.user_id END) AS w0,
        -- Week 1: active exactly 1 week later
        COUNT(DISTINCT CASE WHEN w.activity_week = c.cohort_week + INTERVAL '1 week' THEN c.user_id END) AS w1,
        -- Week 2
        COUNT(DISTINCT CASE WHEN w.activity_week = c.cohort_week + INTERVAL '2 week' THEN c.user_id END) AS w2,
        -- Week 4
        COUNT(DISTINCT CASE WHEN w.activity_week = c.cohort_week + INTERVAL '4 week' THEN c.user_id END) AS w4
    FROM cohorts c
    LEFT JOIN weekly_activity w ON c.user_id = w.user_id
    GROUP BY c.cohort_week
)
SELECT
    cohort_week,
    cohort_size,
    ROUND(100.0 * w0 / cohort_size, 1) AS w0_pct,
    ROUND(100.0 * w1 / cohort_size, 1) AS w1_pct,
    ROUND(100.0 * w2 / cohort_size, 1) AS w2_pct,
    ROUND(100.0 * w4 / cohort_size, 1) AS w4_pct
FROM retention_raw
ORDER BY cohort_week;
```

---

## Sessionization — Gap > 30 Minutes = New Session

Sessionization groups consecutive events for a user into sessions based on inactivity gaps.

```sql
-- events(user_id, event_ts, event_type)
-- A new session starts if gap > 30 min since last event
WITH event_gaps AS (
    SELECT
        user_id,
        event_ts,
        event_type,
        LAG(event_ts) OVER (
            PARTITION BY user_id
            ORDER BY event_ts
        ) AS prev_event_ts
    FROM events
),
session_flags AS (
    SELECT
        user_id,
        event_ts,
        event_type,
        -- Flag = 1 when this is the start of a new session
        CASE
            WHEN prev_event_ts IS NULL THEN 1                        -- first event ever
            WHEN event_ts - prev_event_ts > INTERVAL '30 minutes' THEN 1  -- gap > 30m
            ELSE 0
        END AS is_session_start
    FROM event_gaps
),
sessions AS (
    SELECT
        user_id,
        event_ts,
        event_type,
        -- Cumulative sum of session start flags = session number per user
        SUM(is_session_start) OVER (
            PARTITION BY user_id
            ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_num
    FROM session_flags
)
SELECT
    user_id,
    session_num,
    MIN(event_ts) AS session_start,
    MAX(event_ts) AS session_end,
    COUNT(*)      AS events_in_session,
    EXTRACT(EPOCH FROM MAX(event_ts) - MIN(event_ts)) / 60 AS session_duration_min
FROM sessions
GROUP BY user_id, session_num
ORDER BY user_id, session_num;
```

---

## Consecutive Login Streaks — Gaps and Islands Deep Dive

```sql
-- logins(user_id, login_date) — one row per user per login day
WITH daily_logins AS (
    -- deduplicate: one row per user per day
    SELECT DISTINCT user_id, login_date FROM logins
),
numbered AS (
    SELECT
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM daily_logins
),
islands AS (
    -- Subtracting row number from date gives same constant for consecutive days
    SELECT
        user_id,
        login_date,
        login_date - (rn * INTERVAL '1 day') AS island_id   -- Postgres
        -- Snowflake: DATEADD('day', -rn, login_date)
    FROM numbered
),
streaks AS (
    SELECT
        user_id,
        island_id,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end,
        COUNT(*)        AS streak_days
    FROM islands
    GROUP BY user_id, island_id
)
-- Longest streak per user
SELECT user_id, MAX(streak_days) AS longest_streak
FROM streaks
GROUP BY user_id
ORDER BY longest_streak DESC;
```

---

## Month-Over-Month Growth Rate

```sql
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(revenue) AS revenue
    FROM orders
    GROUP BY 1
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month)  AS prev_month_revenue,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
               / NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
        2
    ) AS mom_growth_pct
FROM monthly
ORDER BY month;
```

---

## Percentile Calculation

### Built-in Percentile Functions

```sql
-- Postgres / Snowflake: ordered-set aggregate functions
SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) AS median_revenue,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY revenue) AS p90_revenue,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY revenue) AS median_disc  -- returns an actual value
FROM orders;
-- PERCENTILE_CONT: interpolates between values
-- PERCENTILE_DISC: returns the smallest value >= the percentile position

-- BigQuery: APPROX_QUANTILES for large tables (faster but approximate)
SELECT APPROX_QUANTILES(revenue, 100)[OFFSET(50)] AS approx_median,
       APPROX_QUANTILES(revenue, 100)[OFFSET(90)] AS approx_p90
FROM orders;

-- Spark SQL 3.1+
SELECT PERCENTILE(revenue, 0.5) AS median_revenue,
       PERCENTILE_APPROX(revenue, 0.9) AS p90_approx
FROM orders;
```

### Median Without a Built-in Function

```sql
-- Universal approach using ROW_NUMBER
WITH ranked AS (
    SELECT
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue)        AS rn_asc,
        ROW_NUMBER() OVER (ORDER BY revenue DESC)   AS rn_desc
    FROM orders
)
SELECT AVG(revenue) AS median
FROM ranked
WHERE ABS(rn_asc - rn_desc) <= 1;
-- For odd N: one row where rn_asc = rn_desc (the middle)
-- For even N: two rows where they differ by 1 (the two middle values, AVG them)
```

---

## Key Takeaways

- Funnel analysis requires enforcing step ORDER — use MIN(ts) per step and check `step2_ts > step1_ts`
- Cohort retention: LEFT JOIN activity on week offset from signup week, count distinct users at each offset
- Sessionization: `LAG` + cumulative `SUM(is_new_session)` creates session IDs without a loop
- Gaps & islands: `date - ROW_NUMBER()` produces the same constant for consecutive date sequences
- Always use `NULLIF` in the denominator when computing percentages (avoids division by zero on zero-size cohorts)
- `PERCENTILE_CONT(0.5)` for exact median; `APPROX_QUANTILES` or `PERCENTILE_APPROX` for large tables
