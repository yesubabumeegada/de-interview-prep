---
title: "SQL Interview Patterns - Fundamentals"
topic: sql
subtopic: interview-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, interview-patterns, ranking, running-total, lag, deduplication, anti-join, pivot, gaps-islands, funnel, retention]
---

# SQL Interview Patterns — Fundamentals

There are 10 archetypal SQL interview problems. Every company's SQL question is a variation on one of these. Master the templates and you can adapt them to any domain.

---

## Pattern 1: Top-N Per Group

**Problem**: "Find the top 3 highest-paid employees in each department."

```sql
-- Template: DENSE_RANK() partitioned by group, filter WHERE rank <= N
WITH ranked AS (
    SELECT
        employee_id,
        department,
        salary,
        DENSE_RANK() OVER (
            PARTITION BY department
            ORDER BY salary DESC
        ) AS rnk
    FROM employees
)
SELECT employee_id, department, salary
FROM ranked
WHERE rnk <= 3;
```

> Use `DENSE_RANK()` (not `ROW_NUMBER()`) when ties should be treated equally. `ROW_NUMBER()` gives unique ranks arbitrarily — use it only when you want exactly N rows regardless of ties.

---

## Pattern 2: Running Total

**Problem**: "Show cumulative revenue by day."

```sql
-- Template: SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM daily_revenue_summary
ORDER BY order_date;

-- Partitioned: cumulative revenue PER product
SELECT
    product_id,
    order_date,
    revenue,
    SUM(revenue) OVER (
        PARTITION BY product_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS product_cumulative_revenue
FROM orders;
```

---

## Pattern 3: Previous Row Comparison (LAG)

**Problem**: "Show day-over-day revenue change."

```sql
-- Template: LAG() to reference the previous row's value
SELECT
    order_date,
    revenue,
    LAG(revenue) OVER (ORDER BY order_date)          AS prev_day_revenue,
    revenue - LAG(revenue) OVER (ORDER BY order_date) AS daily_change,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY order_date))
               / NULLIF(LAG(revenue) OVER (ORDER BY order_date), 0),
        2
    ) AS pct_change
FROM daily_revenue
ORDER BY order_date;
```

> `NULLIF(..., 0)` prevents division by zero when the previous day's revenue was 0.

---

## Pattern 4: Deduplication

**Problem**: "Keep only the most recent record per user."

```sql
-- Template: ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM user_profiles
)
SELECT *
FROM deduped
WHERE rn = 1;

-- Tie-breaking: if updated_at has ties, add a secondary key
ROW_NUMBER() OVER (
    PARTITION BY user_id
    ORDER BY updated_at DESC, record_id DESC  -- record_id breaks ties deterministically
)
```

---

## Pattern 5: Self-Join for Comparisons

**Problem**: "Find all pairs of employees in the same department with a salary difference > $10,000."

```sql
-- Template: self-join with e1.id < e2.id to avoid duplicates
SELECT
    e1.employee_id AS emp1_id,
    e2.employee_id AS emp2_id,
    e1.department,
    ABS(e1.salary - e2.salary) AS salary_diff
FROM employees e1
JOIN employees e2
  ON e1.department = e2.department
  AND e1.employee_id < e2.employee_id    -- avoid (A,B) and (B,A) duplicates
WHERE ABS(e1.salary - e2.salary) > 10000;
```

---

## Pattern 6: Anti-Join (NOT EXISTS vs NOT IN)

**Problem**: "Find customers who have never made a purchase."

```sql
-- Template: NOT EXISTS (preferred — handles NULLs safely)
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- Alternative: LEFT JOIN + IS NULL
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- Avoid: NOT IN — breaks if orders.customer_id has any NULLs
WHERE c.customer_id NOT IN (SELECT customer_id FROM orders)
```

> Always choose `NOT EXISTS` or `LEFT JOIN ... IS NULL` over `NOT IN` with a subquery.

---

## Pattern 7: Pivot — CASE WHEN SUM

**Problem**: "Show revenue by product in columns: Jan, Feb, Mar."

```sql
-- Template: conditional aggregation
SELECT
    product_id,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN revenue ELSE 0 END) AS jan_revenue,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN revenue ELSE 0 END) AS feb_revenue,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN revenue ELSE 0 END) AS mar_revenue
FROM orders
WHERE EXTRACT(YEAR FROM order_date) = 2024
GROUP BY product_id;

-- Snowflake / BigQuery also support PIVOT syntax
SELECT *
FROM orders
PIVOT (SUM(revenue) FOR month_num IN (1 AS jan, 2 AS feb, 3 AS mar));
```

---

## Pattern 8: Consecutive Events — Gaps and Islands

**Problem**: "Find the longest streak of consecutive days a user logged in."

```sql
-- Template: row_number() - row_number() trick creates "island" groups
WITH login_days AS (
    SELECT DISTINCT user_id, login_date FROM logins
),
islands AS (
    SELECT
        user_id,
        login_date,
        login_date - ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY login_date
        ) * INTERVAL '1 day' AS island_group
    FROM login_days
)
SELECT
    user_id,
    island_group,
    MIN(login_date) AS streak_start,
    MAX(login_date) AS streak_end,
    COUNT(*)        AS streak_length
FROM islands
GROUP BY user_id, island_group
ORDER BY streak_length DESC;
```

The trick: if you subtract a row number from a date, consecutive dates produce the same constant value — the "island group."

---

## Pattern 9: Funnel Analysis — Step Conversion

**Problem**: "What % of users who signed up completed onboarding?"

```sql
-- Template: count distinct users at each funnel step
SELECT
    COUNT(DISTINCT CASE WHEN step >= 1 THEN user_id END) AS signup,
    COUNT(DISTINCT CASE WHEN step >= 2 THEN user_id END) AS onboarding,
    COUNT(DISTINCT CASE WHEN step >= 3 THEN user_id END) AS first_purchase,
    ROUND(100.0 *
        COUNT(DISTINCT CASE WHEN step >= 2 THEN user_id END) /
        NULLIF(COUNT(DISTINCT CASE WHEN step >= 1 THEN user_id END), 0), 1
    ) AS signup_to_onboarding_pct
FROM (
    SELECT user_id,
           MAX(CASE event_type
               WHEN 'signup'         THEN 1
               WHEN 'onboarding'     THEN 2
               WHEN 'first_purchase' THEN 3
               ELSE 0
           END) AS step
    FROM events
    GROUP BY user_id
) funnels;
```

---

## Pattern 10: Retention — Cohort Analysis

**Problem**: "What % of users who signed up in week 0 returned in week 1 and week 2?"

```sql
-- Template: cohort join on week offset
WITH cohorts AS (
    SELECT user_id,
           DATE_TRUNC('week', signup_date) AS cohort_week
    FROM users
),
activity AS (
    SELECT DISTINCT user_id,
                    DATE_TRUNC('week', event_date) AS active_week
    FROM events
)
SELECT
    c.cohort_week,
    COUNT(DISTINCT c.user_id)                                         AS cohort_size,
    COUNT(DISTINCT CASE WHEN a.active_week = c.cohort_week THEN c.user_id END) AS week0,
    COUNT(DISTINCT CASE WHEN a.active_week = c.cohort_week + INTERVAL '1 week' THEN c.user_id END) AS week1,
    COUNT(DISTINCT CASE WHEN a.active_week = c.cohort_week + INTERVAL '2 week' THEN c.user_id END) AS week2
FROM cohorts c
LEFT JOIN activity a ON c.user_id = a.user_id
GROUP BY c.cohort_week
ORDER BY c.cohort_week;
```

---

## Quick Reference

| Pattern | Key SQL | Watch Out For |
|---|---|---|
| Top-N per group | `DENSE_RANK()` | Use `DENSE_RANK` not `ROW_NUMBER` for ties |
| Running total | `SUM() OVER (ORDER BY ...)` | `ROWS UNBOUNDED PRECEDING` vs default frame |
| Previous row | `LAG()` | NULLIF denominator for pct change |
| Deduplication | `ROW_NUMBER() = 1` | Add secondary sort key for deterministic ties |
| Self-join pairs | `e1.id < e2.id` | The `<` prevents duplicate pairs |
| Anti-join | `NOT EXISTS` | Never `NOT IN` if subquery can have NULLs |
| Pivot | `SUM(CASE WHEN ...)` | COALESCE to 0 for clean output |
| Gaps & islands | `date - ROW_NUMBER()` | `DISTINCT` dates first if duplicate days possible |
| Funnel | `MAX(step)` per user | `NULLIF` in denominator |
| Retention | `LEFT JOIN` on week offset | `DATE_TRUNC` week start dialect differences |
