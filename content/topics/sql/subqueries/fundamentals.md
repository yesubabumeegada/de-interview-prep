---
title: "SQL Subqueries - Fundamentals"
topic: sql
subtopic: subqueries
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, subqueries, correlated-subquery, scalar-subquery, exists, in, derived-table]
---

# SQL Subqueries — Fundamentals

## What Is a Subquery?

A **subquery** (also called an inner query or nested query) is a SELECT statement written inside another SQL statement. It runs first and its result is used by the outer query.

> **Analogy:** A subquery is like a parenthetical in a sentence — "Find all customers (whose total purchases exceed $1000) and send them a coupon." The parenthetical clause is evaluated first, then the outer action uses the result.

---

## Sample Data

**customers**

| customer_id | name | email | country |
|------------|------|-------|---------|
| 1 | Alice | alice@ex.com | US |
| 2 | Bob | bob@ex.com | UK |
| 3 | Carol | carol@ex.com | US |
| 4 | David | david@ex.com | CA |

**orders**

| order_id | customer_id | amount | order_date | status |
|----------|------------|--------|-----------|--------|
| 101 | 1 | 250.00 | 2024-01-10 | shipped |
| 102 | 1 | 890.00 | 2024-01-20 | shipped |
| 103 | 2 | 150.00 | 2024-01-15 | pending |
| 104 | 3 | 420.00 | 2024-01-18 | shipped |
| 105 | 3 | 680.00 | 2024-02-01 | shipped |
| 106 | 4 | 95.00 | 2024-02-03 | pending |

---

## Types of Subqueries

### 1. Scalar Subquery — Returns a Single Value

A scalar subquery returns exactly one row and one column. It can be used anywhere a single value is expected.

```sql
-- Find all orders with amount above the overall average
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders);
```

**Inner query runs first:** `SELECT AVG(amount) FROM orders` → returns `414.17`

**Outer query becomes:** `WHERE amount > 414.17`

**Result:**

| order_id | customer_id | amount |
|----------|------------|--------|
| 102 | 1 | 890.00 |
| 104 | 3 | 420.00 |
| 105 | 3 | 680.00 |

```sql
-- Scalar subquery in SELECT — compare each order to the average
SELECT 
    order_id,
    amount,
    (SELECT AVG(amount) FROM orders) AS avg_order,
    amount - (SELECT AVG(amount) FROM orders) AS diff_from_avg
FROM orders;
```

### 2. Row Subquery — Returns Multiple Columns, One Row

```sql
-- Find customer whose latest order matches a specific profile
SELECT customer_id, name
FROM customers
WHERE (customer_id, country) = (SELECT customer_id, country FROM orders o 
                                 JOIN customers c USING(customer_id)
                                 ORDER BY order_date DESC LIMIT 1);
```

### 3. Table Subquery (Derived Table) — Returns Multiple Rows and Columns

A subquery in the FROM clause acts as a temporary table:

```sql
-- Use a subquery as a derived table
SELECT c.name, order_summary.total_orders, order_summary.total_spent
FROM customers c
JOIN (
    SELECT customer_id, COUNT(*) AS total_orders, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer_id
) AS order_summary ON c.customer_id = order_summary.customer_id;
```

**Result:**

| name | total_orders | total_spent |
|------|-------------|------------|
| Alice | 2 | 1140.00 |
| Bob | 1 | 150.00 |
| Carol | 2 | 1100.00 |
| David | 1 | 95.00 |

> **This is the same as a CTE.** The derived table approach puts the subquery inline in FROM; the CTE approach names it with WITH. Both produce identical results; CTEs are usually more readable.

---

## IN vs EXISTS

Two very different ways to test membership:

### IN Subquery

```sql
-- Find customers who have placed at least one order
SELECT customer_id, name
FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders);
```

**How IN works:** The subquery runs once, returns a list of values `{1, 2, 3, 4}`, and the outer query checks each customer against this list.

**Result:**

| customer_id | name |
|------------|------|
| 1 | Alice |
| 2 | Bob |
| 3 | Carol |
| 4 | David |

```sql
-- NOT IN: customers who have never ordered
SELECT customer_id, name
FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL);
-- ⚠️ Important: if the subquery returns ANY NULL, NOT IN returns NO rows!
-- Always add WHERE column IS NOT NULL when using NOT IN
```

### EXISTS Subquery

```sql
-- Same result using EXISTS
SELECT c.customer_id, c.name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);
```

**How EXISTS works:** For each customer row in the outer query, it checks if the subquery returns at least one row. The subquery is "correlated" — it references the outer query's row.

**The subquery returns `SELECT 1`** — what it returns doesn't matter, only WHETHER it returns anything.

---

## Correlated Subqueries

A **correlated subquery** references columns from the outer query. It re-executes once per row in the outer query.

```sql
-- Find each customer's most recent order
SELECT c.name, o.order_id, o.order_date, o.amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date = (
    -- Correlated: references o.customer_id from the outer query
    SELECT MAX(order_date) 
    FROM orders 
    WHERE customer_id = o.customer_id  -- ← references outer query
);
```

**How it executes:**
1. For each row in orders (outer), run the inner query with that specific `customer_id`
2. Inner query returns the max order_date for THAT customer
3. Keep the outer row only if its date matches

**Result:**

| name | order_id | order_date | amount |
|------|----------|-----------|--------|
| Alice | 102 | 2024-01-20 | 890.00 |
| Bob | 103 | 2024-01-15 | 150.00 |
| Carol | 105 | 2024-02-01 | 680.00 |
| David | 106 | 2024-02-03 | 95.00 |

> **Performance warning:** Correlated subqueries run once per outer row. For a table with 1 million rows, the subquery executes 1 million times. This is often slow — consider replacing with a JOIN or window function.

---

## Subquery Positions

Subqueries can appear in multiple positions in a SQL statement:

```sql
-- In SELECT (scalar subquery):
SELECT 
    name,
    (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) AS order_count
FROM customers c;

-- In FROM (derived table):
SELECT * FROM (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) t
WHERE total > 500;

-- In WHERE (filter subquery):
SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'US');

-- In HAVING (aggregate filter):
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > (SELECT AVG(total) FROM (SELECT SUM(amount) AS total FROM orders GROUP BY customer_id) t);
```

---

## Subquery vs CTE vs JOIN — When to Use Which

| Approach | When to Use | Example |
|----------|------------|---------|
| **Subquery (inline)** | Simple one-off filter; code is self-contained | `WHERE id IN (SELECT ...)` |
| **Subquery (derived table)** | Intermediate aggregation before joining | `FROM (SELECT ... GROUP BY ...) t` |
| **CTE** | Multi-step logic; reuse same result; recursive | `WITH cte AS (...) SELECT ...` |
| **JOIN** | Combine rows from related tables | `JOIN orders ON customer_id` |
| **EXISTS** | Check for existence (especially with correlated check) | `WHERE EXISTS (SELECT 1 ...)` |

```sql
-- These three are equivalent — choose based on readability:

-- Subquery approach:
SELECT name FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders WHERE amount > 500);

-- JOIN approach:
SELECT DISTINCT c.name FROM customers c
JOIN orders o ON c.customer_id = o.customer_id WHERE o.amount > 500;

-- EXISTS approach:
SELECT name FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.amount > 500);
```

---

## Common Pitfalls

| Pitfall | Problem | Fix |
|---------|---------|-----|
| `NOT IN` with NULLs | If subquery returns NULL, `NOT IN` returns no rows | Use `NOT EXISTS` instead |
| Scalar subquery returning multiple rows | Runtime error | Add `LIMIT 1` or ensure subquery is unique |
| Correlated subquery on large table | Runs N times — O(N²) | Replace with JOIN or window function |
| Subquery in SELECT referencing multiple columns | Syntax error | Use a JOIN instead |

```sql
-- NOT IN NULL trap:
-- If any order has customer_id = NULL, this returns NOTHING:
SELECT * FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders);

-- Safe replacement using NOT EXISTS:
SELECT * FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

---

## Interview Tips

> **Tip 1:** "What's the difference between IN and EXISTS?" — "IN executes the subquery once and compares against the full result set. EXISTS executes the subquery once per outer row and stops as soon as it finds one matching row. For large result sets, EXISTS is usually faster because it short-circuits. Also, NOT EXISTS is safer than NOT IN when NULLs might be present in the subquery result."

> **Tip 2:** "When would you use a subquery instead of a JOIN?" — "I use subqueries in the WHERE clause when I need to filter by an aggregate (like WHERE amount > (SELECT AVG...)). I use derived tables in FROM when I need to aggregate before joining. For simple row-matching between tables, a JOIN is usually cleaner and equally fast — the optimizer often converts IN subqueries to JOINs internally anyway."

> **Tip 3:** "What's a correlated subquery and what's the risk?" — "A correlated subquery references the outer query and runs once per outer row — making it O(N×M). On large tables this is extremely slow. I replace them with JOINs or window functions. For example, 'find each customer's most recent order' should use ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date DESC) in a CTE, not a correlated MAX(date) subquery."
