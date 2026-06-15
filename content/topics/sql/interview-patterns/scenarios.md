---
title: "SQL Interview Patterns - Scenario Questions"
topic: sql
subtopic: interview-patterns
content_type: scenario_question
tags: [sql, interview-patterns, dense-rank, retention, funnel, rolling-window]
---

# SQL Interview Patterns — Scenario Questions

<article data-difficulty="junior">

## Scenario 1 (Junior): Second Highest Salary Per Department

You have:

```sql
employees(employee_id INT, name VARCHAR, department VARCHAR, salary DECIMAL)
```

Find the **second highest salary** in each department. If a department has only one distinct salary, do not include it in the results. Use `DENSE_RANK` so that two employees earning the same salary both count as tied for the same rank.

**Expected output columns**: `department`, `second_highest_salary`

<details>
<summary>✅ Solution</summary>

```sql
WITH ranked AS (
    SELECT
        department,
        salary,
        DENSE_RANK() OVER (
            PARTITION BY department
            ORDER BY salary DESC
        ) AS salary_rank
    FROM employees
)
SELECT
    department,
    salary AS second_highest_salary
FROM ranked
WHERE salary_rank = 2
ORDER BY department;
```

**Why DENSE_RANK over ROW_NUMBER?**

If two employees in Engineering both earn $120,000, they share rank 1. The next distinct salary (say $95,000) gets rank 2 — correct. With `ROW_NUMBER`, one of the $120K employees gets rank 1 and the other gets rank 2 — the "second highest" would wrongly return $120,000.

**Edge cases to mention in the interview:**
- Departments with only one distinct salary: `DENSE_RANK = 2` never exists → naturally excluded from results
- Ties at rank 2: if three employees earn $95K, all three rows appear in the output — is that what the interviewer wants? Confirm before writing
- NULL salaries: `DENSE_RANK` puts NULLs at the end in descending order by default; exclude with `WHERE salary IS NOT NULL` if needed

**Alternative using subquery (good to mention but less clean):**
```sql
SELECT department, MAX(salary) AS second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees e2 WHERE e2.department = e.department)
GROUP BY department;
-- Works but does N subquery executions — less efficient, and suboptimal when there are ties
```

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2 (Mid): 7-Day Rolling Retention for a Mobile App

You have a mobile app. "Retention" is defined as: a user returns to the app within 7 calendar days of their signup date.

Tables:
```sql
users(user_id INT, signup_date DATE)
app_sessions(user_id INT, session_date DATE)
```

Write a query that computes, for each signup cohort day, the 7-day retention rate: what % of users who signed up on day D had at least one session between D+1 and D+7 (inclusive).

**Expected output**: `signup_date DATE, cohort_size INT, retained_users INT, retention_rate DECIMAL`

<details>
<summary>✅ Solution</summary>

```sql
WITH cohort_base AS (
    -- All users and their signup date
    SELECT user_id, signup_date
    FROM users
    WHERE signup_date >= '2024-01-01'
),
retention_check AS (
    -- For each user, check if they had a session in the 7-day window after signup
    SELECT
        c.user_id,
        c.signup_date,
        MAX(CASE
            WHEN s.session_date > c.signup_date
             AND s.session_date <= c.signup_date + INTERVAL '7 days'
            THEN 1 ELSE 0
        END) AS was_retained
    FROM cohort_base c
    LEFT JOIN app_sessions s ON c.user_id = s.user_id
    GROUP BY c.user_id, c.signup_date
)
SELECT
    signup_date,
    COUNT(*)                                    AS cohort_size,
    SUM(was_retained)                           AS retained_users,
    ROUND(100.0 * SUM(was_retained) / COUNT(*), 1) AS retention_rate
FROM retention_check
GROUP BY signup_date
ORDER BY signup_date;
```

**Key design decisions to discuss:**

1. **Window: D+1 to D+7 (not D+0 to D+6)**: The session on signup_date itself doesn't count — we want to know if they *came back*. Use `> signup_date` (not `>=`) for the lower bound.

2. **LEFT JOIN + MAX**: Using LEFT JOIN ensures users with zero sessions still appear in the retention_check CTE with `was_retained = 0`. Without LEFT JOIN, they'd be missing and cohort_size would be wrong.

3. **MAX(CASE WHEN ...)**: Collapses multiple sessions per user in the window to a single 0/1 flag per user. Using `COUNT(DISTINCT)` would also work but is slightly more expensive.

4. **Cohort recency filter**: Cohorts from the last 7 days aren't "complete" yet (users still have time to retain). Mention this to the interviewer — you might want to exclude `signup_date > CURRENT_DATE - 7`.

```sql
-- With recency guard:
WHERE signup_date >= '2024-01-01'
  AND signup_date <= CURRENT_DATE - 7   -- only include complete cohorts
```

**BigQuery / Snowflake equivalent**: identical SQL works; replace `INTERVAL '7 days'` with `INTERVAL 7 DAY` (BigQuery) or keep as-is (Snowflake).

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3 (Senior): Complete Funnel Analysis with Drop-Off Rates and Time-to-Convert

You are building a funnel analysis for an e-commerce app. The funnel has four steps (in order):

1. `signup` — user creates an account
2. `onboarding_complete` — user finishes onboarding
3. `first_purchase` — user makes their first purchase
4. `repeat_purchase` — user makes any subsequent purchase

Tables:
```sql
events(user_id INT, event_type VARCHAR, event_ts TIMESTAMP)
-- event_type values: 'signup', 'onboarding_complete', 'first_purchase', 'repeat_purchase'
-- A user can have multiple repeat_purchase events
```

Build a complete funnel report that shows:
- Total users who reached each step
- Drop-off rate from the previous step
- Median time-to-convert between each step (in hours)

The funnel must enforce ordering: each step must occur AFTER the previous step for the same user. A user who has a first_purchase before completing onboarding should NOT be counted in step 3.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: get the FIRST timestamp for each user at each funnel step
WITH step_times AS (
    SELECT
        user_id,
        MIN(event_ts) FILTER (WHERE event_type = 'signup')              AS signup_ts,
        MIN(event_ts) FILTER (WHERE event_type = 'onboarding_complete') AS onboard_ts,
        MIN(event_ts) FILTER (WHERE event_type = 'first_purchase')      AS first_purchase_ts,
        MIN(event_ts) FILTER (WHERE event_type = 'repeat_purchase')     AS repeat_purchase_ts
    FROM events
    GROUP BY user_id
),
-- Step 2: enforce ordering — each step must occur AFTER the previous
ordered_funnel AS (
    SELECT
        user_id,
        signup_ts,
        -- Only count onboarding if it happened after signup
        CASE WHEN onboard_ts > signup_ts
             THEN onboard_ts END AS onboard_ts,
        -- Only count first_purchase if it happened after onboarding
        CASE WHEN first_purchase_ts > onboard_ts AND onboard_ts > signup_ts
             THEN first_purchase_ts END AS first_purchase_ts,
        -- Only count repeat_purchase if it happened after first_purchase
        CASE WHEN repeat_purchase_ts > first_purchase_ts
              AND first_purchase_ts > onboard_ts
              AND onboard_ts > signup_ts
             THEN repeat_purchase_ts END AS repeat_purchase_ts
    FROM step_times
    WHERE signup_ts IS NOT NULL  -- must have signed up to enter the funnel
),
-- Step 3: compute counts and time-to-convert for each step
funnel_metrics AS (
    SELECT
        COUNT(*)                AS users_signed_up,
        COUNT(onboard_ts)       AS users_onboarded,
        COUNT(first_purchase_ts)   AS users_first_purchase,
        COUNT(repeat_purchase_ts)  AS users_repeat_purchase,
        -- Median hours from signup to onboarding (Postgres)
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM onboard_ts - signup_ts) / 3600
        ) AS median_hours_signup_to_onboard,
        -- Median hours from onboarding to first purchase
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM first_purchase_ts - onboard_ts) / 3600
        ) AS median_hours_onboard_to_purchase,
        -- Median hours from first to repeat purchase
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM repeat_purchase_ts - first_purchase_ts) / 3600
        ) AS median_hours_first_to_repeat
    FROM ordered_funnel
)
-- Step 4: format as a readable funnel table
SELECT
    step_num,
    step_name,
    users_reached,
    ROUND(100.0 * users_reached / LAG(users_reached) OVER (ORDER BY step_num), 1) AS step_conversion_pct,
    ROUND(100.0 * users_reached / MAX(users_reached) OVER (), 1)                   AS overall_conversion_pct,
    median_hours_to_convert
FROM (
    SELECT 1 AS step_num, 'signup'           AS step_name, users_signed_up    AS users_reached, NULL                              AS median_hours_to_convert FROM funnel_metrics
    UNION ALL
    SELECT 2, 'onboarding_complete', users_onboarded,   median_hours_signup_to_onboard   FROM funnel_metrics
    UNION ALL
    SELECT 3, 'first_purchase',      users_first_purchase, median_hours_onboard_to_purchase FROM funnel_metrics
    UNION ALL
    SELECT 4, 'repeat_purchase',     users_repeat_purchase, median_hours_first_to_repeat  FROM funnel_metrics
) steps
ORDER BY step_num;
```

**Expected output format:**
```
step | step_name              | users_reached | step_conversion_pct | overall_conversion_pct | median_hours_to_convert
1    | signup                 | 100,000       | NULL                | 100.0%                 | NULL
2    | onboarding_complete    | 72,000        | 72.0%               | 72.0%                  | 2.3 hours
3    | first_purchase         | 31,000        | 43.1%               | 31.0%                  | 18.7 hours
4    | repeat_purchase        | 18,000        | 58.1%               | 18.0%                  | 72.4 hours
```

**Senior-level considerations to mention:**

1. **Ordering enforcement**: `CASE WHEN step2_ts > step1_ts` is critical — without it, a user who bought before completing onboarding inflates the funnel incorrectly.

2. **PERCENTILE_CONT on filtered population**: the median is computed only on users who converted at that step, not the full cohort. Numerically correct — mention whether NULL users (who didn't convert) should be included as infinite time-to-convert.

3. **Dialect notes**: 
   - BigQuery: replace `PERCENTILE_CONT ... WITHIN GROUP` with `APPROX_QUANTILES(value, 100)[OFFSET(50)]`
   - Spark: `PERCENTILE(value, 0.5)` or `PERCENTILE_APPROX(value, 0.5)`
   - Snowflake: same syntax as Postgres

4. **Scalability**: for 100M+ users, consider computing `step_times` incrementally per cohort (partition by signup_week) to avoid a single huge aggregation.

5. **Repeat purchases nuance**: `MIN(repeat_purchase_ts)` gives time to second purchase. For "any repeat purchase" count, using `COUNT(DISTINCT user_id)` where any repeat event exists is equivalent since we're checking existence, not timing.

</details>
</article>
