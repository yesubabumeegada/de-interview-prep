---
title: "SQL CTEs - Fundamentals"
topic: sql
subtopic: ctes
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, ctes, common-table-expressions, with-clause, readability, subqueries]
---

# SQL CTEs — Fundamentals


## 🎯 Analogy

Think of CTEs like named sub-reports you write first, then reference. Instead of nesting subqueries 5 levels deep, you name each step and build on it — same result, dramatically more readable.

---
## What Is a CTE?

A **CTE (Common Table Expression)** is a temporary, named result set that you define at the beginning of a query using the `WITH` keyword. Think of it as creating a temporary view that only exists for the duration of that single query.

**The analogy:** A CTE is like giving a name to a complex sub-calculation so you can reference it by name later — instead of nesting everything into one enormous query.

> **Key Insight:** CTEs don't store data or create tables. They're just a way to organize complex queries into readable, named steps. The optimizer often treats them identically to subqueries.

---

## Basic Syntax

```sql
WITH cte_name AS (
    -- Any valid SELECT query
    SELECT column1, column2, ...
    FROM some_table
    WHERE conditions
)
-- Main query that references the CTE
SELECT *
FROM cte_name
WHERE ...;
```

**How to read it:** "WITH this temporary result called `cte_name` defined as [query], now SELECT from it."

---

## Sample Data

**employees**

| emp_id | name | department | salary | manager_id | hire_date |
|--------|------|-----------|--------|-----------|-----------|
| 1 | Alice | Engineering | 130000 | NULL | 2018-03-01 |
| 2 | Bob | Engineering | 95000 | 1 | 2020-06-15 |
| 3 | Charlie | Engineering | 110000 | 1 | 2019-01-10 |
| 4 | Diana | Marketing | 88000 | 5 | 2021-04-01 |
| 5 | Eve | Marketing | 105000 | NULL | 2017-09-20 |
| 6 | Frank | Sales | 72000 | 5 | 2022-01-15 |
| 7 | Grace | Sales | 82000 | 5 | 2020-11-01 |

---

## Why Use CTEs? — The Readability Problem

**Without CTE (hard to read nested subquery):**

```sql
SELECT name, salary, dept_avg
FROM (
    SELECT name, salary, department,
           AVG(salary) OVER (PARTITION BY department) AS dept_avg
    FROM employees
) sub
WHERE salary > dept_avg;
```

**With CTE (same logic, much clearer):**

```sql
WITH dept_averages AS (
    SELECT name, salary, department,
           AVG(salary) OVER (PARTITION BY department) AS dept_avg
    FROM employees
)
SELECT name, salary, dept_avg
FROM dept_averages
WHERE salary > dept_avg;
```

**Result:**

| name | salary | dept_avg |
|------|--------|----------|
| Alice | 130000 | 111667 |
| Eve | 105000 | 96500 |
| Grace | 82000 | 77000 |

> **Both queries produce identical results.** The CTE version is just easier to read, debug, and modify.

---

## Multiple CTEs — Building Step by Step

You can chain multiple CTEs, each referencing the previous ones:

```sql
WITH 
-- Step 1: Calculate department stats
dept_stats AS (
    SELECT 
        department,
        COUNT(*) AS headcount,
        AVG(salary) AS avg_salary,
        MAX(salary) AS max_salary
    FROM employees
    GROUP BY department
),
-- Step 2: Find departments with average salary above company average
above_avg_depts AS (
    SELECT department, avg_salary, headcount
    FROM dept_stats
    WHERE avg_salary > (SELECT AVG(salary) FROM employees)
)
-- Step 3: Final output
SELECT 
    department,
    headcount,
    ROUND(avg_salary, 0) AS avg_salary
FROM above_avg_depts
ORDER BY avg_salary DESC;
```

**Result:**

| department | headcount | avg_salary |
|-----------|-----------|-----------|
| Engineering | 3 | 111667 |

**How this reads:**
1. First, compute department stats
2. Then, filter to departments above the company average
3. Finally, output the result

> **Rule:** Later CTEs can reference earlier CTEs. The main query at the bottom can reference ANY of the CTEs. Each CTE is separated by a comma (no comma after the last one).

---

## CTE vs Subquery — When to Use Which

| Aspect | CTE | Subquery |
|--------|-----|----------|
| Readability | Named, self-documenting | Nested, harder to follow |
| Reusability | Can reference same CTE multiple times | Must repeat the subquery |
| Recursion | Supports recursive queries | Cannot be recursive |
| Performance | Usually identical to subquery | Usually identical to CTE |
| Scope | Only within the single statement | Only within the single statement |

**Use CTE when:**
- The query has 3+ logical steps
- You need to reference the same derived result more than once
- You want to document your logic with named steps
- You need recursion

**Subquery is fine when:**
- Simple one-off filter or lookup
- Single level of nesting
- Performance-critical and CTE materialization is unwanted

---

## Referencing a CTE Multiple Times

One of the biggest advantages — reusing the same computation:

```sql
WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
)
-- Compare each month to the previous month
SELECT 
    curr.month,
    curr.revenue AS current_month,
    prev.revenue AS previous_month,
    curr.revenue - prev.revenue AS change,
    ROUND((curr.revenue - prev.revenue) / prev.revenue * 100, 1) AS pct_change
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.month = prev.month + INTERVAL '1 month'
ORDER BY curr.month;
```

**Result:**

| month | current_month | previous_month | change | pct_change |
|-------|--------------|---------------|--------|-----------|
| 2024-01 | 150000 | 140000 | 10000 | 7.1% |
| 2024-02 | 162000 | 150000 | 12000 | 8.0% |
| 2024-03 | 148000 | 162000 | -14000 | -8.6% |

> **Without CTE:** You'd have to write the monthly aggregation subquery TWICE (once for current, once for previous) — duplicating logic and making it harder to maintain.

---

## CTE with INSERT, UPDATE, DELETE

CTEs aren't just for SELECT — they work with data modification too:

```sql
-- Delete duplicate records, keeping only the most recent
WITH duplicates AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (
            PARTITION BY email 
            ORDER BY created_at DESC
        ) AS rn
    FROM users
)
DELETE FROM users
WHERE id IN (
    SELECT id FROM duplicates WHERE rn > 1
);
```

```sql
-- Update using a CTE to compute the new values
WITH salary_adjustments AS (
    SELECT 
        emp_id,
        salary * 1.10 AS new_salary  -- 10% raise
    FROM employees
    WHERE department = 'Engineering'
      AND hire_date < '2020-01-01'
)
UPDATE employees
SET salary = sa.new_salary
FROM salary_adjustments sa
WHERE employees.emp_id = sa.emp_id;
```

---

## Common CTE Patterns

### Pattern 1: Top N Per Group

```sql
WITH ranked AS (
    SELECT 
        name, department, salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees
)
SELECT name, department, salary
FROM ranked
WHERE rn <= 2;  -- Top 2 per department
```

### Pattern 2: Running Totals with Filtering

```sql
WITH daily_totals AS (
    SELECT order_date, SUM(amount) AS daily_revenue
    FROM orders
    GROUP BY order_date
),
running AS (
    SELECT 
        order_date,
        daily_revenue,
        SUM(daily_revenue) OVER (ORDER BY order_date) AS cumulative_revenue
    FROM daily_totals
)
SELECT * FROM running
WHERE cumulative_revenue >= 1000000;  -- Days when we crossed $1M total
```

### Pattern 3: Deduplication

```sql
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, order_date 
            ORDER BY updated_at DESC
        ) AS rn
    FROM raw_orders
)
SELECT * FROM deduped WHERE rn = 1;  -- Keep only the latest version
```

---

## CTE Gotchas

| Gotcha | Explanation |
|--------|-------------|
| CTEs are NOT materialized by default | The optimizer may inline them (run the query multiple times) |
| No indexes on CTEs | They're temporary — can't be optimized with indexes |
| Cannot reference a CTE outside its statement | Scope is limited to the single SQL statement |
| Comma placement | Separate CTEs with commas; no comma before final SELECT |
| Naming conflicts | CTE names can shadow real table names (confusing — avoid this) |

---


## ▶️ Try It Yourself

```sql
WITH
daily_revenue AS (
    SELECT DATE(order_date) AS day, SUM(amount) AS revenue
    FROM orders
    GROUP BY 1
),
moving_avg AS (
    SELECT
        day,
        revenue,
        AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d
    FROM daily_revenue
)
SELECT day, revenue, ROUND(ma_7d, 2) AS moving_avg_7d
FROM moving_avg
WHERE day >= '2024-01-01'
ORDER BY day;
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** When asked "write a query to find X," start with a CTE structure even if a subquery would work. It signals you write production-quality, maintainable SQL. Name your CTEs descriptively.

> **Tip 2:** The #1 CTE interview pattern: "Find top N per group." Answer: CTE with ROW_NUMBER() + PARTITION BY, then filter WHERE rn <= N in the main query.

> **Tip 3:** If asked "are CTEs faster than subqueries?" — "Generally no, performance is identical because the optimizer usually treats them the same. CTEs are for readability and reusability. The exception is recursive CTEs, which have no subquery equivalent."
