---
title: "SQL Subqueries - Intermediate"
topic: sql
subtopic: subqueries
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, subqueries, lateral-join, any-all, subquery-optimization, semi-join, anti-join]
---

# SQL Subqueries — Intermediate Concepts

## Semi-Joins and Anti-Joins

The optimizer converts certain subquery patterns into efficient join algorithms. Understanding the intent helps you write the right pattern.

### Semi-Join: EXISTS or IN

A semi-join returns rows from the left table where at least one matching row exists in the right table — but doesn't duplicate left rows.

```sql
-- Both produce a semi-join internally:

-- Pattern 1: EXISTS
SELECT c.customer_id, c.name
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.amount > 500);

-- Pattern 2: IN
SELECT customer_id, name
FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders WHERE amount > 500);

-- Pattern 3: DISTINCT JOIN (anti-pattern — more verbose, same result)
SELECT DISTINCT c.customer_id, c.name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.amount > 500;
-- The DISTINCT is needed here because a customer with 3 orders > $500 would appear 3 times without it
```

**EXPLAIN output comparison (PostgreSQL):**
```
-- EXISTS/IN → Hash Semi Join (efficient, stops at first match per customer)
-- DISTINCT JOIN → Hash Join + Unique/Sort (less efficient for this pattern)
```

### Anti-Join: NOT EXISTS or NOT IN (safely)

An anti-join returns rows from the left table with NO matching rows in the right table.

```sql
-- Safe anti-join with NOT EXISTS:
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- Equivalent: LEFT JOIN + IS NULL
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- Dangerous: NOT IN (fails when NULLs present in subquery)
SELECT customer_id, name FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
-- If ANY row in orders has NULL customer_id → returns empty result!
```

**Rule:** Always use `NOT EXISTS` for anti-joins. Never use `NOT IN` on a subquery unless you're certain the subquery result contains no NULLs.

---

## LATERAL Joins — Subqueries That Reference the Outer Table

A `LATERAL` subquery can reference columns from tables to its left in the FROM clause, enabling per-row subqueries without the performance penalty of correlated subqueries in WHERE.

```sql
-- For each customer, get their top 3 most expensive orders
SELECT c.name, orders_top3.order_id, orders_top3.amount
FROM customers c
CROSS JOIN LATERAL (
    SELECT order_id, amount
    FROM orders o
    WHERE o.customer_id = c.customer_id  -- References outer table!
    ORDER BY amount DESC
    LIMIT 3
) AS orders_top3;
```

**Why LATERAL instead of a correlated subquery:**
- LATERAL in FROM returns multiple rows per outer row — a correlated subquery in SELECT can only return one value
- The optimizer can use a proper Nested Loop join plan with good index use

```sql
-- LATERAL with LEFT JOIN (keep customers even if they have no orders):
SELECT c.name, COALESCE(top.amount, 0) AS top_order_amount
FROM customers c
LEFT JOIN LATERAL (
    SELECT amount FROM orders WHERE customer_id = c.customer_id ORDER BY amount DESC LIMIT 1
) AS top ON TRUE;
```

**Database support:**
| Database | Syntax |
|---------|--------|
| PostgreSQL | `LATERAL` |
| MySQL 8.0+ | `LATERAL` |
| SQL Server | `CROSS APPLY` / `OUTER APPLY` |
| Oracle | `LATERAL` or `CROSS APPLY` |
| BigQuery | `CROSS JOIN UNNEST(...)` (limited LATERAL) |
| Snowflake | `LATERAL` |

**SQL Server equivalent:**
```sql
-- SQL Server: CROSS APPLY = LATERAL INNER JOIN, OUTER APPLY = LATERAL LEFT JOIN
SELECT c.Name, t.OrderID, t.Amount
FROM Customers c
CROSS APPLY (
    SELECT TOP 3 OrderID, Amount FROM Orders 
    WHERE CustomerID = c.CustomerID 
    ORDER BY Amount DESC
) AS t;
```

---

## ANY and ALL Operators

`ANY` and `ALL` compare a value against a set returned by a subquery:

```sql
-- ANY: true if comparison is true for at least one value in the subquery
-- Find orders larger than any order from customer 2:
SELECT order_id, amount FROM orders
WHERE amount > ANY (SELECT amount FROM orders WHERE customer_id = 2);
-- customer 2's orders: [150.00]
-- Returns orders with amount > 150.00 (i.e., > ANY of {150.00})

-- ALL: true if comparison is true for ALL values in the subquery
-- Find orders larger than ALL orders from customer 2:
SELECT order_id, amount FROM orders
WHERE amount > ALL (SELECT amount FROM orders WHERE customer_id = 2);
-- Returns orders with amount > 150.00 (i.e., > ALL of {150.00})
-- If customer 2 had orders [150, 300], this would require amount > 300

-- Common equivalences:
-- = ANY(subquery)   ↔   IN (subquery)
-- != ALL(subquery)  ↔   NOT IN (subquery) [with NULL caveat]

-- Use ANY/ALL when the set isn't a literal list:
SELECT * FROM products WHERE price < ALL (SELECT price FROM premium_products);
-- Find products cheaper than every premium product
```

---

## Subqueries in UPDATE and DELETE

Subqueries aren't just for SELECT — they're essential in data modification:

```sql
-- UPDATE: increase price for products in underperfoming categories
UPDATE products
SET price = price * 1.10
WHERE category_id IN (
    SELECT category_id
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category_id
    HAVING SUM(oi.quantity) < 100  -- Low-selling categories
);

-- DELETE: remove old orders for inactive customers
DELETE FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE is_active = FALSE
)
AND order_date < NOW() - INTERVAL '2 years';

-- UPDATE using a derived table (PostgreSQL):
UPDATE orders o
SET status = 'archived'
FROM (
    SELECT order_id FROM orders WHERE order_date < '2022-01-01'
) old_orders
WHERE o.order_id = old_orders.order_id;
```

---

## Subqueries for Complex Filtering Patterns

### Find Rows Missing From Another Table

```sql
-- Customers who exist in the CRM but have no account in the billing system
SELECT c.customer_id, c.email
FROM crm_customers c
WHERE NOT EXISTS (
    SELECT 1 FROM billing_accounts ba WHERE ba.customer_id = c.customer_id
);
```

### Filter by Aggregated Child Data

```sql
-- Find customers whose AVERAGE order is above the company-wide average
SELECT c.customer_id, c.name
FROM customers c
WHERE (
    SELECT AVG(amount) FROM orders WHERE customer_id = c.customer_id
) > (
    SELECT AVG(amount) FROM orders
);

-- Better alternative (avoids correlated subquery):
WITH customer_avgs AS (
    SELECT customer_id, AVG(amount) AS avg_order
    FROM orders GROUP BY customer_id
),
company_avg AS (
    SELECT AVG(amount) AS company_avg FROM orders
)
SELECT c.customer_id, c.name
FROM customers c
JOIN customer_avgs ca ON c.customer_id = ca.customer_id
CROSS JOIN company_avg
WHERE ca.avg_order > company_avg;
```

### Finding Gaps Using Subqueries

```sql
-- Find missing invoice numbers (gaps in a sequence)
SELECT id + 1 AS gap_start
FROM invoices main
WHERE NOT EXISTS (
    SELECT 1 FROM invoices WHERE id = main.id + 1
)
AND id < (SELECT MAX(id) FROM invoices);
```

---

## Performance: When Subqueries Hurt

### The N+1 Problem in SQL

Correlated subqueries in SELECT re-execute per row — this is the SQL equivalent of N+1:

```sql
-- Bad: correlated subquery runs once per customer row
SELECT 
    customer_id,
    name,
    (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) AS order_count,
    (SELECT SUM(amount) FROM orders WHERE customer_id = c.customer_id) AS total_spent
FROM customers c;
-- For 1 million customers: 2 million subquery executions!

-- Good: single aggregation join
SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;
-- One pass — much faster
```

### Subquery in WHERE vs JOIN

```sql
-- IN subquery (executed once, result set stored):
SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM vip_customers);

-- JOIN equivalent (optimizer may choose either plan):
SELECT o.* FROM orders o JOIN vip_customers vc ON o.customer_id = vc.customer_id;

-- For small subquery result sets: IN is fine (optimizer converts to hash join)
-- For large subquery result sets: JOIN gives optimizer more flexibility
-- Modern optimizers (PostgreSQL, SQL Server, Oracle) often convert IN to a JOIN automatically
```

---

## Cross-Dialect Notes

### MySQL Limitations (pre-8.0)

```sql
-- MySQL 5.7 and earlier: cannot reference the same table in a subquery within UPDATE/DELETE
-- This FAILS in MySQL 5.7:
DELETE FROM orders WHERE order_id IN (
    SELECT order_id FROM orders WHERE amount < 10  -- Same table!
);

-- Fix: wrap in another subquery (MySQL trick):
DELETE FROM orders WHERE order_id IN (
    SELECT order_id FROM (
        SELECT order_id FROM orders WHERE amount < 10
    ) tmp
);

-- MySQL 8.0+ supports CTEs, which solve this cleanly:
WITH to_delete AS (SELECT order_id FROM orders WHERE amount < 10)
DELETE FROM orders WHERE order_id IN (SELECT order_id FROM to_delete);
```

### Snowflake: Scalar Subqueries in SELECT

```sql
-- Snowflake fully supports correlated subqueries but may optimize them differently
-- For scalar subqueries that are constant (no correlation), use a WITH clause instead
-- for better readability and potential performance:

-- OK in Snowflake but potentially slow:
SELECT product_id, price, (SELECT AVG(price) FROM products) AS avg_price FROM products;

-- Better:
WITH avg_price AS (SELECT AVG(price) AS avg FROM products)
SELECT p.product_id, p.price, a.avg AS avg_price FROM products p CROSS JOIN avg_price a;
```

---

## Interview Tips

> **Tip 1:** "When is EXISTS more efficient than IN?" — "EXISTS short-circuits — as soon as one matching row is found, it stops searching. This is faster when the subquery would return many rows but we only care whether any match exists. IN loads the entire subquery result into memory (or uses a hash), which can be expensive for large result sets. However, modern optimizers often compile both to hash semi-joins with similar performance — the real difference is with NOT IN vs NOT EXISTS, where NULL handling makes NOT EXISTS the only correct choice."

> **Tip 2:** "What is a LATERAL join and when would you use it?" — "LATERAL allows a subquery in FROM to reference columns from tables to its left, enabling per-row sub-selects that can return multiple rows. It's ideal for 'top N per group' queries — e.g., the top 3 orders per customer. Without LATERAL you'd need a window function + filter. LATERAL is also needed when you want to apply a function that returns a table to each row, like `UNNEST` or a table-valued function."

> **Tip 3:** "How would you refactor a slow correlated subquery?" — "I first check what the subquery computes. If it's an aggregate (COUNT, SUM, MAX), I replace it with a GROUP BY + JOIN. If it's an existence check, I use EXISTS or a semi-join. If it's filtering by a ranked position (like latest record per group), I use ROW_NUMBER() in a CTE. These transformations reduce query complexity from O(N²) to O(N log N) or O(N)."
