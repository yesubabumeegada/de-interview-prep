---
title: "SQL Interview Coding Problems — Fundamentals"
topic: sql
subtopic: interview-coding-problems
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, interview, coding, problems, aggregation, joins, groupby]
---

# SQL Interview Coding Problems — Fundamentals

These are the foundational coding problem patterns every Data Engineer must know cold. Each pattern appears repeatedly in DE interviews across FAANG, mid-size tech, and data-heavy startups.

---

## Setup: Sample Tables Used Throughout This File

```sql
-- Orders table used in most examples
CREATE TABLE orders (
    order_id    INT,
    customer_id INT,
    amount      DECIMAL(10,2),
    order_date  DATE,
    status      VARCHAR(20)
);

INSERT INTO orders VALUES
(1,  101, 250.00, '2024-01-05', 'completed'),
(2,  101, 180.00, '2024-01-12', 'completed'),
(3,  101,  90.00, '2024-01-20', 'cancelled'),
(4,  102, 420.00, '2024-01-07', 'completed'),
(5,  102, 310.00, '2024-02-01', 'completed'),
(6,  102, 150.00, '2024-02-15', 'cancelled'),
(7,  103, 500.00, '2024-01-03', 'completed'),
(8,  103, 470.00, '2024-01-18', 'completed'),
(9,  103, 390.00, '2024-02-10', 'completed'),
(10, 103, 200.00, '2024-02-28', 'cancelled');
```

---

## Problem 1: Top-N Per Group

**Interview prompt:** "Find the top 3 orders by amount for each customer."

This is the single most common window function problem in DE interviews.

### Why naive approaches fail

```sql
-- WRONG: This just gives top 3 globally, not per customer
SELECT customer_id, order_id, amount
FROM orders
ORDER BY amount DESC
LIMIT 3;

-- WRONG: GROUP BY loses individual row details
SELECT customer_id, MAX(amount) FROM orders GROUP BY customer_id;
```

### Correct solution using ROW_NUMBER()

```sql
WITH ranked_orders AS (
    SELECT
        order_id,
        customer_id,
        amount,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY amount DESC
        ) AS rn
    FROM orders
)
SELECT
    order_id,
    customer_id,
    amount,
    order_date
FROM ranked_orders
WHERE rn <= 3
ORDER BY customer_id, amount DESC;
```

**Why the CTE is required:** Window functions are evaluated AFTER the WHERE clause but BEFORE the final SELECT. You cannot write `WHERE ROW_NUMBER() OVER (...) <= 3` — the engine hasn't computed the window function yet when it evaluates WHERE. The CTE (or subquery) materializes the window function result first, then the outer query filters on it.

### ROW_NUMBER vs RANK vs DENSE_RANK — know the difference

| Function | Behavior on ties | Example output for (500, 500, 470) |
|---|---|---|
| ROW_NUMBER() | Assigns unique ranks, arbitrary tiebreak | 1, 2, 3 |
| RANK() | Ties share rank, next rank is skipped | 1, 1, 3 |
| DENSE_RANK() | Ties share rank, next rank is consecutive | 1, 1, 2 |

**Interview rule:** Use `ROW_NUMBER` when you need exactly N rows. Use `DENSE_RANK` when ties should count as the same rank.

---

## Problem 2: Running Total / Cumulative Sum

**Interview prompt:** "Show each order alongside a running total of revenue per customer, ordered by date."

### Solution

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM orders
ORDER BY customer_id, order_date;
```

**Expected output (customer 101):**

| order_id | customer_id | order_date | amount | running_total |
|---|---|---|---|---|
| 1 | 101 | 2024-01-05 | 250.00 | 250.00 |
| 2 | 101 | 2024-01-12 | 180.00 | 430.00 |
| 3 | 101 | 2024-01-20 | 90.00 | 520.00 |

### Frame clause explained

```
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

- `UNBOUNDED PRECEDING` — start from the first row in the partition
- `CURRENT ROW` — include up to and including the current row
- This is the default frame when you specify ORDER BY, but being explicit is good practice

### Variation: running total that excludes cancelled orders

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_completed_total
FROM orders
ORDER BY customer_id, order_date;
```

---

## Problem 3: Year-over-Year / Period Comparison

**Interview prompt:** "Compare each month's total revenue to the same month in the prior year."

### Setup: monthly revenue table

```sql
CREATE TABLE monthly_revenue (
    year_num  INT,
    month_num INT,
    revenue   DECIMAL(12,2)
);

INSERT INTO monthly_revenue VALUES
(2023, 1, 12000), (2023, 2, 13500), (2023, 3, 15000),
(2023, 4, 14200), (2023, 5, 16800), (2023, 6, 17500),
(2024, 1, 14000), (2024, 2, 15200), (2024, 3, 16800),
(2024, 4, 13900), (2024, 5, 18200), (2024, 6, 19100);
```

### Solution 1: LAG window function (cleanest)

```sql
SELECT
    year_num,
    month_num,
    revenue,
    LAG(revenue, 12) OVER (ORDER BY year_num, month_num) AS prior_year_revenue,
    revenue - LAG(revenue, 12) OVER (ORDER BY year_num, month_num) AS yoy_change,
    ROUND(
        100.0 * (revenue - LAG(revenue, 12) OVER (ORDER BY year_num, month_num))
        / NULLIF(LAG(revenue, 12) OVER (ORDER BY year_num, month_num), 0),
        2
    ) AS yoy_pct_change
FROM monthly_revenue
ORDER BY year_num, month_num;
```

**Note:** `NULLIF(..., 0)` prevents division by zero when prior year revenue is 0.

### Solution 2: Self-join

```sql
SELECT
    curr.year_num,
    curr.month_num,
    curr.revenue                                     AS current_revenue,
    prior.revenue                                    AS prior_year_revenue,
    curr.revenue - COALESCE(prior.revenue, 0)        AS yoy_change
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prior
    ON curr.month_num = prior.month_num
   AND curr.year_num  = prior.year_num + 1
ORDER BY curr.year_num, curr.month_num;
```

**When to use each:**
- LAG is cleaner for simple comparisons
- Self-join is easier to understand and debug for complex multi-column comparisons
- Self-join works in older SQL dialects without window function support

---

## Problem 4: Deduplication

**Interview prompt:** "The orders table has duplicate rows. Keep only the latest record per customer (by order_date), delete the rest."

### Setup: table with duplicates

```sql
CREATE TABLE customers_raw (
    id          INT,
    customer_id INT,
    name        VARCHAR(100),
    email       VARCHAR(100),
    updated_at  TIMESTAMP
);

INSERT INTO customers_raw VALUES
(1, 101, 'Alice',  'alice@example.com',  '2024-01-01 10:00:00'),
(2, 101, 'Alice',  'alice2@example.com', '2024-03-15 12:00:00'),  -- duplicate, newer
(3, 102, 'Bob',    'bob@example.com',    '2024-02-01 09:00:00'),
(4, 102, 'Bob',    'bob@example.com',    '2024-02-01 09:00:00'),  -- exact duplicate
(5, 103, 'Carol',  'carol@example.com',  '2024-01-20 14:00:00');
```

### Solution 1: SELECT deduplicated rows (non-destructive)

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM customers_raw
)
SELECT id, customer_id, name, email, updated_at
FROM ranked
WHERE rn = 1;
```

### Solution 2: DELETE duplicates using CTE (destructive, use with caution)

```sql
-- Works in PostgreSQL, SQL Server
WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM customers_raw
)
DELETE FROM customers_raw
WHERE id IN (
    SELECT id FROM ranked WHERE rn > 1
);
```

### Solution 3: CREATE TABLE AS SELECT (safest for production)

```sql
-- Step 1: Create clean version
CREATE TABLE customers_clean AS
SELECT id, customer_id, name, email, updated_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM customers_raw
) t
WHERE rn = 1;

-- Step 2: Validate counts match expectations
-- Step 3: Swap tables (rename)
```

> **Production tip:** Never run a DELETE to deduplicate in production without first validating the SELECT returns exactly the rows you expect. The CTAS approach gives you a safety net.

---

## Problem 5: Aggregation with Conditions

**Interview prompt:** "For each customer, count total orders AND count only cancelled orders."

This tests knowledge of conditional aggregation — a pattern used constantly in DE/analytics work.

### Solution 1: CASE WHEN inside SUM (universal)

```sql
SELECT
    customer_id,
    COUNT(*)                                                        AS total_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)         AS cancelled_orders,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END)    AS completed_revenue,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                               AS cancellation_rate_pct
FROM orders
GROUP BY customer_id
ORDER BY customer_id;
```

### Solution 2: COUNT with CASE (alternative)

```sql
SELECT
    customer_id,
    COUNT(*)                                              AS total_orders,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END)     AS cancelled_orders
FROM orders
GROUP BY customer_id;
```

**Why this works:** `COUNT(expr)` counts non-NULL values. When the CASE condition is false, it returns NULL (no ELSE clause), which COUNT ignores. This is slightly more concise than `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`.

### Solution 3: FILTER clause (PostgreSQL, modern SQL)

```sql
SELECT
    customer_id,
    COUNT(*)                                    AS total_orders,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders,
    SUM(amount) FILTER (WHERE status = 'completed') AS completed_revenue
FROM orders
GROUP BY customer_id;
```

**Compatibility note:** FILTER is PostgreSQL-native and supported in BigQuery / DuckDB. Not available in MySQL, SQL Server, or older Redshift versions. Use CASE WHEN for maximum portability.

### Comparison table

| Pattern | Readability | Portability | Performance |
|---|---|---|---|
| SUM(CASE WHEN ... THEN 1 ELSE 0 END) | Good | Universal | Same |
| COUNT(CASE WHEN ... THEN 1 END) | Good | Universal | Same |
| COUNT(*) FILTER (WHERE ...) | Best | PG/BQ/DuckDB only | Same |

---

## Key Concepts Summary

| Pattern | Core technique | Common mistake |
|---|---|---|
| Top-N per group | ROW_NUMBER() + CTE + WHERE rn <= N | Filtering window function in same SELECT |
| Running total | SUM() OVER (PARTITION BY ... ORDER BY ... ROWS ...) | Missing frame clause causing range vs rows behavior |
| YoY comparison | LAG(col, 12) or self-join | Division by zero on prior period = 0 |
| Deduplication | ROW_NUMBER() CTE + DELETE/CTAS | Not validating before destructive delete |
| Conditional agg | SUM/COUNT + CASE WHEN | Forgetting ELSE 0 in SUM (NULLs propagate) |

---

## What Interviewers Are Testing

> **Junior level:** Can you write a ROW_NUMBER() window function without help? Do you know the CTE pattern to filter on it? Do you use NULLIF to handle division by zero?

At the junior level, interviewers want to see:
1. Fluency with GROUP BY and aggregate functions
2. Basic window function awareness (ROW_NUMBER, SUM OVER)
3. Understanding that NULL propagates in SUM (ELSE 0 matters)
4. Ability to use CTEs to chain query steps

They are NOT expecting you to have memorized every edge case. They want to see you reason through the problem.
