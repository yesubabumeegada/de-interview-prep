---
title: "SQL Subqueries - Scenario Questions"
topic: sql
subtopic: subqueries
content_type: scenario_question
tags: [sql, subqueries, interview, scenarios, anti-join, correlated, lateral]
---

# Scenario Questions — SQL Subqueries

<article data-difficulty="junior">

## 🟢 Junior: Find Products Above Category Average Price

**Scenario:** You have a `products` table with `product_id`, `name`, `category_id`, and `price`. Write a query using a subquery to find all products whose price is above the average price within their own category. Show the product name, price, and the category average.

<details>
<summary>💡 Hint</summary>

You need to compare each product's price to the average price of its category. This requires a correlated subquery in the WHERE clause that groups by category, OR a derived table / CTE that computes category averages first and then joins back.

</details>

<details>
<summary>✅ Solution</summary>

**Approach 1: Correlated subquery (simpler, possibly slower on large tables)**

```sql
SELECT 
    p.product_id,
    p.name,
    p.price,
    p.category_id
FROM products p
WHERE p.price > (
    SELECT AVG(price)
    FROM products
    WHERE category_id = p.category_id  -- Correlated: references outer row
);
```

**Approach 2: Derived table (more performant — computes averages once)**

```sql
SELECT 
    p.product_id,
    p.name,
    p.price,
    cat_avg.avg_price AS category_avg_price,
    ROUND(p.price - cat_avg.avg_price, 2) AS above_avg_by
FROM products p
JOIN (
    SELECT category_id, AVG(price) AS avg_price
    FROM products
    GROUP BY category_id
) cat_avg ON p.category_id = cat_avg.category_id
WHERE p.price > cat_avg.avg_price
ORDER BY cat_avg.avg_price DESC, p.price DESC;
```

**Why Approach 2 is better for large tables:**
- The derived table computes category averages ONCE in a single pass
- Approach 1's correlated subquery runs once per product row (O(N) subquery calls)
- On 1 million products across 100 categories, Approach 2 is ~1000× faster

**Alternative — identical result using CTE:**
```sql
WITH cat_averages AS (
    SELECT category_id, AVG(price) AS avg_price
    FROM products GROUP BY category_id
)
SELECT p.product_id, p.name, p.price, ca.avg_price AS category_avg
FROM products p
JOIN cat_averages ca ON p.category_id = ca.category_id
WHERE p.price > ca.avg_price;
```

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Find Customers Who Have Never Placed an Order

**Scenario:** You have a `customers` table and an `orders` table linked by `customer_id`. Write two versions of the query to find customers who have never placed an order: one using NOT IN and one using NOT EXISTS. Explain the important difference between them.

<details>
<summary>💡 Hint</summary>

NOT IN and NOT EXISTS both find rows in the left table with no match in the right table (anti-join). However, they behave very differently when NULLs are present in the subquery result. Think about what happens if ANY `customer_id` in the orders table is NULL.

</details>

<details>
<summary>✅ Solution</summary>

**Version 1: NOT IN**
```sql
SELECT customer_id, name, email
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
    -- ⚠️ The IS NOT NULL is REQUIRED — see explanation below
);
```

**Version 2: NOT EXISTS (preferred)**
```sql
SELECT c.customer_id, c.name, c.email
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);
```

**Version 3: LEFT JOIN + IS NULL (also correct)**
```sql
SELECT c.customer_id, c.name, c.email
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;
```

**The critical NULL difference:**

If ANY row in orders has `customer_id = NULL`:
- `NOT IN (SELECT customer_id FROM orders)` returns **ZERO rows** — because comparing anything to NULL yields UNKNOWN, and `NOT IN` requires all comparisons to be FALSE
- `NOT EXISTS` correctly ignores NULL rows and returns the expected result

```sql
-- Demo of the NULL trap:
-- Suppose orders has these rows: (1, ...), (2, ...), (NULL, ...)
-- This returns NOTHING:
SELECT * FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders);
-- NULL in the list makes every NOT IN comparison UNKNOWN

-- This works correctly:
SELECT * FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

**Rule:** Always use `NOT EXISTS` for anti-joins in production code. Only use `NOT IN` when you can guarantee the subquery will never return NULL.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Refactor a Slow Correlated Subquery

**Scenario:** A data analyst wrote this query to get each product's name, price, and its rank within its category (1 = most expensive). It takes 45 seconds on a 500,000-row products table. Refactor it to run in under 1 second without changing the output.

```sql
-- Slow query:
SELECT 
    product_id,
    name,
    category_id,
    price,
    (SELECT COUNT(*) FROM products p2 
     WHERE p2.category_id = p1.category_id 
       AND p2.price > p1.price) + 1 AS price_rank
FROM products p1
ORDER BY category_id, price_rank;
```

<details>
<summary>💡 Hint</summary>

The correlated subquery runs once per product row — for 500,000 products, that's 500,000 separate COUNT queries. The result (rank within category by price) can be computed in a single pass using a window function. Think `RANK()` or `DENSE_RANK()` with PARTITION BY.

</details>

<details>
<summary>✅ Solution</summary>

**Why the original is slow:**
- Correlated subquery: `SELECT COUNT(*) FROM products WHERE category_id = p1.category_id AND price > p1.price`
- Runs 500,000 times (once per product)
- Each execution scans all products in that category
- For 100 categories × 5,000 products each: 500,000 × 5,000 = 2.5 billion row comparisons

**Fast version using window function:**
```sql
SELECT 
    product_id,
    name,
    category_id,
    price,
    RANK() OVER (PARTITION BY category_id ORDER BY price DESC) AS price_rank
FROM products
ORDER BY category_id, price_rank;
```

**Result is identical:**
- `RANK()` produces the same values as the correlated COUNT+1 pattern
- Window function runs in a single pass over all rows
- ~0.1 seconds vs 45 seconds

**Verify equivalence:**
```sql
-- The original logic: rank = count of products with higher price + 1
-- RANK() does exactly this: ranks rows by price DESC within each category
-- Ties: both produce the same tie-handling (gaps after ties)
-- If you don't want gaps: use DENSE_RANK() instead

-- Edge case: for tied prices, RANK() produces 1,1,3 (gap)
-- The correlated version produces 1,1,3 as well (same behavior)
-- For 1,1,2 (no gap): use DENSE_RANK()
```

**When to use each approach:**
```sql
-- RANK()      → 1,1,3  (same as original correlated subquery)
-- DENSE_RANK() → 1,1,2  (no gaps)
-- ROW_NUMBER() → 1,2,3  (unique, arbitrary tie-breaking)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Find the Second Most Recent Order Per Customer

**Scenario:** Given an `orders` table with `order_id`, `customer_id`, `order_date`, and `amount`, write a query using subqueries to find each customer's second most recent order. Customers with only one order should be excluded from the result.

<details>
<summary>💡 Hint</summary>

There are multiple approaches: (1) a correlated subquery using NOT EXISTS with two conditions, (2) a derived table with ROW_NUMBER(), or (3) a self-join. The cleanest modern solution uses ROW_NUMBER() in a subquery to rank orders per customer, then filters for rank = 2.

</details>

<details>
<summary>✅ Solution</summary>

**Approach 1: Derived table with ROW_NUMBER() (recommended)**
```sql
SELECT order_id, customer_id, order_date, amount
FROM (
    SELECT 
        order_id,
        customer_id,
        order_date,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) ranked
WHERE rn = 2;
```

**Approach 2: Correlated subquery (clever but harder to read)**
```sql
SELECT o1.order_id, o1.customer_id, o1.order_date, o1.amount
FROM orders o1
WHERE (
    -- Exactly one order exists that is more recent than o1 for the same customer
    SELECT COUNT(*) FROM orders o2
    WHERE o2.customer_id = o1.customer_id
      AND o2.order_date > o1.order_date
) = 1;
-- Logic: the 2nd most recent order has exactly 1 order newer than it
```

**Approach 3: Self-join (older style)**
```sql
SELECT o1.order_id, o1.customer_id, o1.order_date, o1.amount
FROM orders o1
LEFT JOIN orders o2 ON o1.customer_id = o2.customer_id AND o2.order_date > o1.order_date
GROUP BY o1.order_id, o1.customer_id, o1.order_date, o1.amount
HAVING COUNT(o2.order_id) = 1;  -- Exactly 1 more recent order exists
```

**Best choice: Approach 1** — it's the most readable, works in all modern databases, and the optimizer handles ROW_NUMBER() efficiently in a single pass.

**Handling ties (same order_date):**
```sql
-- If two orders share the same date, ROW_NUMBER is arbitrary
-- Use DENSE_RANK if you want both tied orders at rank 2:
SELECT order_id, customer_id, order_date, amount
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS dr
    FROM orders
) ranked
WHERE dr = 2;
-- This may return MULTIPLE rows per customer if there are ties at position 2
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Write a Complete User Activation Funnel with Drop-Off Analysis

**Scenario:** You have an `events` table (`event_id`, `user_id`, `event_type`, `event_timestamp`) and a `users` table (`user_id`, `signup_date`). Using subqueries and/or derived tables, build a funnel analysis showing:

1. How many users completed each step of: signup → email_verified → first_purchase → first_review
2. The drop-off rate between each step
3. The median time (in hours) between each step for users who completed it

The funnel must be computed without CTEs (subquery/derived table only) to demonstrate understanding of derived tables.

<details>
<summary>💡 Hint</summary>

Build the funnel as a series of derived tables or subqueries, each computing the count and timing for one step. Use LATERAL or correlated subqueries to get the timestamp of each event per user. Join all steps together, using the previous step's users as the population for the next step (so the denominator is correct for each step's conversion rate).

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 0: All users (the top of the funnel)
-- Each subsequent step is a subset of the previous

SELECT 
    step_name,
    user_count,
    ROUND(user_count * 100.0 / NULLIF(LAG(user_count) OVER (ORDER BY step_num), 0), 1) AS step_conversion_pct,
    ROUND(user_count * 100.0 / NULLIF(FIRST_VALUE(user_count) OVER (ORDER BY step_num), 0), 1) AS overall_pct,
    ROUND(median_hours_to_step, 1) AS median_hours_to_step
FROM (

    -- Step 1: All signed-up users
    SELECT 1 AS step_num, 'signup' AS step_name,
        COUNT(DISTINCT user_id) AS user_count,
        NULL::NUMERIC AS median_hours_to_step
    FROM users

    UNION ALL

    -- Step 2: Users who verified email (timing from signup)
    SELECT 2, 'email_verified',
        COUNT(DISTINCT e.user_id),
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (e.min_event_ts - u.signup_date)) / 3600.0
        )
    FROM users u
    JOIN (
        SELECT user_id, MIN(event_timestamp) AS min_event_ts
        FROM events WHERE event_type = 'email_verified'
        GROUP BY user_id
    ) e ON u.user_id = e.user_id

    UNION ALL

    -- Step 3: Users who made first purchase (must have verified email first)
    SELECT 3, 'first_purchase',
        COUNT(DISTINCT p.user_id),
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (p.min_purchase_ts - v.min_verify_ts)) / 3600.0
        )
    FROM (
        SELECT user_id, MIN(event_timestamp) AS min_verify_ts
        FROM events WHERE event_type = 'email_verified' GROUP BY user_id
    ) v
    JOIN (
        SELECT user_id, MIN(event_timestamp) AS min_purchase_ts
        FROM events WHERE event_type = 'first_purchase' GROUP BY user_id
    ) p ON v.user_id = p.user_id
    WHERE p.min_purchase_ts > v.min_verify_ts  -- Must happen AFTER verification

    UNION ALL

    -- Step 4: Users who wrote first review (must have purchased first)
    SELECT 4, 'first_review',
        COUNT(DISTINCT r.user_id),
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (r.min_review_ts - p.min_purchase_ts)) / 3600.0
        )
    FROM (
        SELECT user_id, MIN(event_timestamp) AS min_purchase_ts
        FROM events WHERE event_type = 'first_purchase' GROUP BY user_id
    ) p
    JOIN (
        SELECT user_id, MIN(event_timestamp) AS min_review_ts
        FROM events WHERE event_type = 'first_review' GROUP BY user_id
    ) r ON p.user_id = r.user_id
    WHERE r.min_review_ts > p.min_purchase_ts

) funnel_steps
ORDER BY step_num;
```

**Sample result:**

| step_name | user_count | step_conversion_pct | overall_pct | median_hours_to_step |
|-----------|-----------|---------------------|-------------|---------------------|
| signup | 50000 | NULL | 100.0% | NULL |
| email_verified | 38000 | 76.0% | 76.0% | 2.3 |
| first_purchase | 21000 | 55.3% | 42.0% | 18.7 |
| first_review | 9450 | 45.0% | 18.9% | 72.4 |

**Key design decisions:**
- Each derived table pre-aggregates to `(user_id, min_timestamp)` before joining — avoiding row explosion
- `WHERE next_step_ts > prev_step_ts` enforces strict ordering (signup must precede verify, verify must precede purchase)
- `PERCENTILE_CONT(0.5)` computes true median (not average — median is more robust for skewed distributions like time-to-event)
- The UNION ALL structure allows each step's population to be the correct denominator for the step conversion rate

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a subquery and what are the main types?**
A: A subquery is a SELECT statement nested inside another SQL statement. Types include: scalar subquery (returns one value, used in SELECT or WHERE), row subquery (returns one row), table subquery/derived table (returns a result set used in FROM), correlated subquery (references the outer query's columns), and EXISTS subquery (tests for row existence).

**Q: What is a correlated subquery and what are its performance implications?**
A: A correlated subquery references columns from the outer query, making it re-execute for every row of the outer query. This creates O(n * m) execution behavior—for a 1M-row outer table, the subquery may execute 1M times. They're often rewritten as JOINs or window functions for better performance.

**Q: When would you use EXISTS instead of IN for a subquery?**
A: EXISTS short-circuits as soon as it finds the first matching row, making it efficient when the subquery could return many rows. IN evaluates the entire subquery and materializes the result. EXISTS is also NULL-safe—IN with NULLs in the subquery result can behave unexpectedly (it will never return rows for the NOT IN case if any NULL is present).

**Q: What is a derived table and how does it differ from a CTE?**
A: A derived table is an inline subquery in the FROM clause that acts as a temporary result set for the outer query. It differs from a CTE in that it's defined inline (harder to read for complex logic), cannot be referenced multiple times in the same query, and cannot be recursive. CTEs are generally preferred for readability.

**Q: What is the NULL trap with NOT IN subqueries?**
A: If the subquery used with NOT IN returns any NULL value, the entire NOT IN expression evaluates to UNKNOWN for every row, returning zero rows. This is a common bug: `WHERE id NOT IN (SELECT id FROM table_with_nulls)` may silently return no results. Use NOT EXISTS or filter NULLs from the subquery to avoid this.

**Q: How do you convert a correlated subquery to a JOIN for performance?**
A: Replace the correlated subquery with an equivalent JOIN. For example, `SELECT * FROM orders o WHERE amount > (SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id)` becomes a JOIN to a pre-aggregated subquery: `SELECT o.* FROM orders o JOIN (SELECT customer_id, AVG(amount) AS avg_amt FROM orders GROUP BY customer_id) a ON o.customer_id = a.customer_id WHERE o.amount > a.avg_amt`.

**Q: What is a lateral join (LATERAL) and how does it relate to correlated subqueries?**
A: LATERAL (PostgreSQL, standard SQL) or CROSS APPLY / OUTER APPLY (SQL Server) allows a subquery in the FROM clause to reference columns from a preceding table in the same FROM clause. It's like a correlated subquery in the FROM clause—useful for "top N per group" queries and functions that return rows per input row.

**Q: When is a subquery more appropriate than a JOIN?**
A: Subqueries are preferable for: EXISTS checks (cleaner than JOIN + DISTINCT), scalar lookups where a NULL result is semantically meaningful, semi-joins and anti-joins (NOT EXISTS), and cases where the subquery result is used multiple times in the SELECT list. JOINs are generally better when you need multiple columns from the related table.

---

## 💼 Interview Tips

- Master the NOT IN / NOT EXISTS NULL trap cold—it appears in interviews at every level and catching this bug in production is a real differentiator. "NOT IN returns zero rows when the subquery includes NULLs" is a must-know fact.
- Be prepared to rewrite a correlated subquery as a JOIN on a whiteboard. Practice this transformation until it's automatic—it's one of the most common query optimization exercises in DE interviews.
- Show nuance when comparing subqueries to JOINs: they're often logically equivalent, but performance can differ significantly. The right choice depends on the database's optimizer, table sizes, and whether the subquery can be pushed down or must be materialized.
- Bring up LATERAL/APPLY as a more expressive alternative to correlated subqueries for certain patterns—it shows awareness of modern SQL features beyond the basics.
- Senior interviewers often ask "how would you optimize this?" after showing a query with a correlated subquery. Have a systematic approach: identify the correlation, convert to a JOIN or window function, check the execution plan, and measure the improvement.
