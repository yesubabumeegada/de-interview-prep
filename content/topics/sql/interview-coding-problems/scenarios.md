---
title: "SQL Interview Coding Problems — Scenarios"
topic: sql
subtopic: interview-coding-problems
content_type: scenario_question
difficulty_level: mid-level
layer: scenarios
tags: [sql, interview, scenarios, second-highest, streak, scd2, practice]
---

# SQL Interview Coding Problems — Scenarios

Practice these three scenarios as if you're in a live interview. Time yourself: junior scenarios should take under 5 minutes, mid-level under 10, senior under 15.

---

<article data-difficulty="junior">

## 🟢 Junior: Find the Second Highest Salary

**Scenario:** "Write a query to find the second highest salary from an employees table. If there are ties for second place, return all of them. Handle the case where there is no second distinct salary."

```sql
CREATE TABLE employees (
    emp_id INT,
    name   VARCHAR(100),
    salary DECIMAL(10,2)
);

INSERT INTO employees VALUES
(1, 'Alice',   95000),
(2, 'Bob',    110000),
(3, 'Carol',  110000),  -- tie for highest
(4, 'Dave',    85000),
(5, 'Eve',     95000);  -- tie for second
```

**Hint:** Use `DENSE_RANK()` rather than a subquery with `MAX(salary) WHERE salary < MAX(salary)`. The subquery approach breaks on ties and is harder to extend.

**Solution:**

```sql
WITH ranked_salaries AS (
    SELECT
        emp_id,
        name,
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM employees
)
SELECT emp_id, name, salary
FROM ranked_salaries
WHERE salary_rank = 2;
```

**Expected output:**

| emp_id | name | salary |
|---|---|---|
| 1 | Alice | 95000 |
| 5 | Eve | 95000 |

**Why MAX(salary) WHERE salary < MAX(salary) fails:**

```sql
-- WRONG — returns only one row even if there are ties
SELECT MAX(salary)
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
-- Returns 95000, but doesn't return both Alice and Eve
-- To get full rows, you'd need another join — more complex for no benefit

-- Also wrong for finding Nth highest without hardcoding N:
-- DENSE_RANK scales cleanly: change WHERE salary_rank = 2 to any N
```

**Why DENSE_RANK and not RANK or ROW_NUMBER:**
- `ROW_NUMBER` gives Alice rank 3 and Eve rank 4 (or vice versa) — neither would be returned by `WHERE rn = 2`
- `RANK` gives Alice and Eve both rank 3 (because ranks 3 and 4 are "taken" by the two 110k employees) — no row has rank 2
- `DENSE_RANK` gives both 110k employees rank 1, and both 95k employees rank 2 — this is what we want

**Extension questions interviewers often ask:**

1. "What if there's no second distinct salary?" — the query returns 0 rows, which is correct (no second-highest exists)
2. "How would you get the Nth highest?" — change `WHERE salary_rank = 2` to `WHERE salary_rank = :n`
3. "What if you need the second highest per department?" — add `PARTITION BY dept` inside `DENSE_RANK() OVER (...)`

```sql
-- Per-department second highest:
WITH ranked AS (
    SELECT
        emp_id, name, dept, salary,
        DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS salary_rank
    FROM employees
)
SELECT emp_id, name, dept, salary
FROM ranked
WHERE salary_rank = 2;
```

</article>

---

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Gaps and Islands — User Activity Streaks

**Scenario:** "You have a table of daily user logins — one row per user per day they logged in. Find each user's longest streak of consecutive login days. Return the user_id, streak_start, streak_end, and streak_length."

```sql
CREATE TABLE daily_logins (
    user_id    INT,
    login_date DATE
);

INSERT INTO daily_logins VALUES
(1, '2024-01-01'), (1, '2024-01-02'), (1, '2024-01-03'),
(1, '2024-01-05'), (1, '2024-01-06'),  -- gap on Jan 4
(1, '2024-01-10'),
(2, '2024-01-01'), (2, '2024-01-02'), (2, '2024-01-03'), (2, '2024-01-04'),
(2, '2024-01-06'),
(3, '2024-01-15');  -- single day, streak = 1
```

**Hint:** The trick is that `(login_date - ROW_NUMBER() days)` produces a constant value within any consecutive streak. When there's a gap in dates, this value changes — creating a new group. Once you have groups, MAX minus MIN gives the streak length.

**Solution:**

```sql
-- Step 1: Assign island groups using the date-minus-rownumber trick
WITH numbered AS (
    SELECT
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM daily_logins
),
islands AS (
    SELECT
        user_id,
        login_date,
        login_date - (rn * INTERVAL '1 day') AS island_group
        -- PostgreSQL syntax; for MySQL: DATE_SUB(login_date, INTERVAL rn DAY)
    FROM numbered
),
-- Step 2: Aggregate each island into a streak record
streaks AS (
    SELECT
        user_id,
        island_group,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end,
        COUNT(*)        AS streak_length
    FROM islands
    GROUP BY user_id, island_group
),
-- Step 3: Find the longest streak per user
ranked_streaks AS (
    SELECT
        user_id,
        streak_start,
        streak_end,
        streak_length,
        RANK() OVER (PARTITION BY user_id ORDER BY streak_length DESC) AS rnk
    FROM streaks
)
SELECT user_id, streak_start, streak_end, streak_length
FROM ranked_streaks
WHERE rnk = 1
ORDER BY user_id;
```

**Expected output:**

| user_id | streak_start | streak_end | streak_length |
|---|---|---|---|
| 1 | 2024-01-01 | 2024-01-03 | 3 |
| 2 | 2024-01-01 | 2024-01-04 | 4 |
| 3 | 2024-01-15 | 2024-01-15 | 1 |

**Why the trick works — visualized for user_id = 1:**

```
login_date   rn   date - rn * 1 day   → island_group
2024-01-01   1    2023-12-31          ← streak 1
2024-01-02   2    2023-12-31          ← streak 1 (same!)
2024-01-03   3    2023-12-31          ← streak 1 (same!)
2024-01-05   4    2024-01-01          ← streak 2 (gap caused change)
2024-01-06   5    2024-01-01          ← streak 2 (same!)
2024-01-10   6    2024-01-04          ← streak 3 (new group)
```

Within a consecutive run, incrementing the date by 1 and incrementing the row number by 1 cancel out. A gap in dates breaks this symmetry.

**Note:** If using RANK() in step 3, ties in streak_length get the same rank — both streaks are returned for a user who has two equally-long max streaks. Use ROW_NUMBER() if you want exactly one row per user.

**Extension questions:**

1. "What if a user has multiple streaks of the same maximum length?" — use `RANK()` to return all tied max streaks, or `ROW_NUMBER()` with a tiebreaker (e.g., most recent streak) to return exactly one
2. "How would you find streaks of at least 7 days?" — add `WHERE streak_length >= 7` to the streaks CTE
3. "The table has duplicates (same user, same date). How do you handle them?" — add `SELECT DISTINCT user_id, login_date` as the first CTE step

</article>

---

<article data-difficulty="senior">

## 🔴 Senior: SCD2 Point-in-Time Revenue Attribution

**Scenario:** "Your `fact_orders` table records each order with an `order_date` and a `customer_id` (the business key). Your `dim_customer` table is SCD Type 2 — it has multiple rows per customer, with `effective_from` and `effective_to` date columns tracking when each version was active. The `effective_to` is NULL for the currently active record.

Write a query to report total revenue broken down by the customer tier (`bronze`, `silver`, `gold`) that each customer held AT THE TIME of their order — not their current tier. Also include the count of orders per tier."

```sql
CREATE TABLE fact_orders (
    order_id    INT,
    customer_id INT,    -- business key
    order_date  DATE,
    amount      DECIMAL(10,2)
);

CREATE TABLE dim_customer (
    customer_key   INT,  -- surrogate key (auto-increment)
    customer_id    INT,  -- business key (links to fact)
    tier           VARCHAR(20),
    region         VARCHAR(50),
    effective_from DATE,
    effective_to   DATE  -- NULL = currently active
);

INSERT INTO fact_orders VALUES
(1001, 101, '2023-03-15', 200.00),
(1002, 101, '2023-08-20', 350.00),
(1003, 101, '2024-02-10', 500.00),
(1004, 102, '2023-06-01', 180.00),
(1005, 102, '2024-01-15', 420.00),
(1006, 103, '2023-11-01', 800.00);

INSERT INTO dim_customer VALUES
(1, 101, 'bronze', 'West', '2023-01-01', '2023-06-30'),
(2, 101, 'silver', 'West', '2023-07-01', '2023-12-31'),
(3, 101, 'gold',   'West', '2024-01-01', NULL),
(4, 102, 'silver', 'East', '2023-01-01', '2023-09-30'),
(5, 102, 'gold',   'East', '2023-10-01', NULL),
(6, 103, 'bronze', 'West', '2023-01-01', NULL);
```

**Hint:** You need a range join: `fact.order_date BETWEEN dim.effective_from AND dim.effective_to`. But `effective_to = NULL` for current records — BETWEEN NULL evaluates to NULL (false). Use `COALESCE(effective_to, '9999-12-31')` as a sentinel date for current records. A simple `WHERE effective_to IS NULL` join gives wrong answers for historical orders.

**Solution:**

```sql
-- Step 1: Join fact to dimension using the point-in-time range join
WITH orders_with_historical_tier AS (
    SELECT
        f.order_id,
        f.customer_id,
        f.order_date,
        f.amount,
        d.tier,
        d.region
    FROM fact_orders f
    INNER JOIN dim_customer d
        ON  f.customer_id = d.customer_id
        AND f.order_date BETWEEN d.effective_from
                             AND COALESCE(d.effective_to, '9999-12-31')
)
-- Step 2: Aggregate by historical tier
SELECT
    tier,
    COUNT(*)        AS order_count,
    SUM(amount)     AS total_revenue,
    AVG(amount)     AS avg_order_value
FROM orders_with_historical_tier
GROUP BY tier
ORDER BY total_revenue DESC;
```

**Expected output:**

| tier | order_count | total_revenue | avg_order_value |
|---|---|---|---|
| gold | 2 | 920.00 | 460.00 |
| bronze | 2 | 1000.00 | 500.00 |
| silver | 2 | 530.00 | 265.00 |

*(Note: customer 103 is bronze for all their orders; customer 101's gold order in 2024 contributes to gold; customer 102's silver order in 2023 contributes to silver)*

**Why the naive join gives wrong answers:**

```sql
-- WRONG: current-record-only join
SELECT f.order_id, f.amount, d.tier
FROM fact_orders f
JOIN dim_customer d
    ON  f.customer_id = d.customer_id
    AND d.effective_to IS NULL;  -- Only gets current records

-- Result: ALL of customer 101's orders attributed to 'gold'
-- (their current tier), including the 2023-03-15 order when they were bronze
-- Revenue by tier would show gold=$1050, silver=$0, bronze=$980 — WRONG
```

**Extension: include customers with no matching dimension row**

```sql
-- Use LEFT JOIN to catch fact rows that don't join to any dim version
-- (data quality issue: missing SCD2 coverage)
WITH orders_with_tier AS (
    SELECT
        f.order_id,
        f.customer_id,
        f.order_date,
        f.amount,
        d.tier,
        CASE WHEN d.customer_key IS NULL THEN 'UNMATCHED' ELSE 'OK' END AS match_status
    FROM fact_orders f
    LEFT JOIN dim_customer d
        ON  f.customer_id = d.customer_id
        AND f.order_date BETWEEN d.effective_from
                             AND COALESCE(d.effective_to, '9999-12-31')
)
SELECT match_status, COUNT(*) AS count, SUM(amount) AS revenue
FROM orders_with_tier
GROUP BY match_status;
```

**Common follow-up questions:**

1. "What if an order_date falls in a gap between two SCD2 records?" — LEFT JOIN returns NULL for tier. Investigate the dimension for missing date coverage. In practice, add a data quality check: `COUNT(*) FROM fact_orders f LEFT JOIN dim WHERE match IS NULL`.

2. "At scale, this BETWEEN join is slow. How do you optimize it?" — In Spark/BigQuery, range joins are expensive. Options: (a) broadcast the dimension if small enough; (b) add a `year_month` column to both tables for partition pruning; (c) denormalize the tier onto the fact table during ETL load time.

3. "Why not just store the tier in the fact table at load time?" — Valid option for append-only pipelines. Trade-off: loses ability to reconstruct history if the business retroactively changes tier definitions. SCD2 preserves the as-was record; denormalizing bakes in the decision.

</article>

---

## Interview Tips

**1. Think before you type — state your approach out loud**

Before writing a single line of SQL, say: "I'm going to use a window function here because we need per-group ranking while keeping individual rows. I'll use a CTE to filter on the window function result." Interviewers give significant credit for clear problem decomposition, even if your syntax isn't perfect.

**2. Name your test cases explicitly**

Always check your solution against the edge cases out loud: "What happens if there are ties? What if the effective_to is NULL? What if there are no rows matching the condition?" This demonstrates production instincts and catches bugs before they're coded.

**3. Start simple, then refine**

Write a working solution first, even if it's not optimal. Then optimize: "This works, but at 10B rows the BETWEEN join would be slow — I'd add a partition column to limit the scan." Showing that you can iterate is more valuable than writing a perfect query on the first try.
