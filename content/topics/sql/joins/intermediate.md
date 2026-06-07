---
title: "SQL Joins - Intermediate"
topic: sql
subtopic: joins
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, joins, anti-join, semi-join, lateral-join, inequality-join, performance]
---

# SQL Joins — Intermediate Concepts

## Anti Joins — Find What's Missing

An anti join returns rows from one table that have **no match** in the other. Three approaches:

```sql
-- Method 1: LEFT JOIN + IS NULL (most readable)
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- Method 2: NOT EXISTS (often fastest — stops at first match)
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- Method 3: NOT IN (caution with NULLs!)
SELECT customer_id, name
FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
-- ⚠️ If orders.customer_id has any NULL, this returns EMPTY RESULT SET
```

**Performance hierarchy (typical):**
1. `NOT EXISTS` — short-circuits, handles NULLs correctly
2. `LEFT JOIN + IS NULL` — readable, optimizer usually handles well
3. `NOT IN` — avoid if subquery column can be NULL

## Semi Joins — Existence Check Without Duplicates

A semi join returns rows from the left table where **at least one match exists** in the right table — but doesn't duplicate rows.

```sql
-- Semi join: "Customers who have placed at least one order"
SELECT c.customer_id, c.name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- vs. INNER JOIN (can produce duplicates if customer has many orders)
SELECT DISTINCT c.customer_id, c.name
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id;
-- Needs DISTINCT to deduplicate — less efficient than EXISTS
```

## Inequality Joins — Non-Equality Conditions

```sql
-- Range join: Find which price tier each order falls into
SELECT 
    o.order_id,
    o.amount,
    t.tier_name
FROM orders o
JOIN price_tiers t 
    ON o.amount >= t.min_amount 
    AND o.amount < t.max_amount;

-- Date range join: Find which marketing campaign was active during each sale
SELECT 
    s.sale_id,
    s.sale_date,
    c.campaign_name
FROM sales s
JOIN campaigns c 
    ON s.sale_date BETWEEN c.start_date AND c.end_date;
```

**Performance warning:** Inequality joins can't use hash joins — they typically require nested loops or sort-merge, making them expensive on large tables.

## LATERAL JOIN (CROSS APPLY) — Correlated Subquery Join

`LATERAL` lets the right side reference columns from the left side. Think of it as a "for each row" join.

```sql
-- Top 3 most recent orders per customer (PostgreSQL/Snowflake)
SELECT c.name, recent.*
FROM customers c
CROSS JOIN LATERAL (
    SELECT order_id, order_date, amount
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
    LIMIT 3
) AS recent;

-- SQL Server equivalent uses CROSS APPLY
SELECT c.name, recent.*
FROM customers c
CROSS APPLY (
    SELECT TOP 3 order_id, order_date, amount
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
) AS recent;
```

**When to use LATERAL over window functions:**
- When you need to limit rows per group efficiently
- When the correlated subquery is complex (multiple tables)
- When you need to call a table-valued function per row

## Join Cardinality Problems

### Row Explosion from Many-to-Many

```sql
-- DANGER: If a customer has 5 orders and 3 addresses,
-- this produces 15 rows per customer (5 × 3 = 15)
SELECT c.name, o.order_id, a.address
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN addresses a ON c.id = a.customer_id;

-- FIX 1: Aggregate before joining
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total_spent
    FROM orders GROUP BY customer_id
),
customer_addresses AS (
    SELECT customer_id, STRING_AGG(address, '; ') AS all_addresses
    FROM addresses GROUP BY customer_id
)
SELECT c.name, co.order_count, co.total_spent, ca.all_addresses
FROM customers c
LEFT JOIN customer_orders co ON c.id = co.customer_id
LEFT JOIN customer_addresses ca ON c.id = ca.customer_id;

-- FIX 2: Use DISTINCT or ROW_NUMBER to control output
```

### Detecting Unexpected Fan-Out

```sql
-- Quick check: does your join multiply rows?
SELECT 
    COUNT(*) AS joined_count,
    (SELECT COUNT(*) FROM table_a) AS left_count,
    (SELECT COUNT(*) FROM table_b) AS right_count
FROM table_a a
JOIN table_b b ON a.key = b.key;

-- If joined_count > left_count, you have fan-out
```

## Composite Key Joins

```sql
-- Joining on multiple columns (common in fact/dimension tables)
SELECT f.*, d.region_name, d.country
FROM fact_sales f
JOIN dim_store d 
    ON f.store_id = d.store_id 
    AND f.brand_id = d.brand_id;

-- Date-effective join (SCD Type 2 dimension)
SELECT f.*, d.customer_name, d.segment
FROM fact_orders f
JOIN dim_customer d 
    ON f.customer_id = d.customer_id
    AND f.order_date BETWEEN d.effective_from AND d.effective_to;
```

## Join Optimization Hints

| Technique | When | Example |
|-----------|------|---------|
| Filter before join | Large tables with selective WHERE | `JOIN (SELECT * FROM big_table WHERE date > X) b` |
| Use EXISTS over JOIN | Only need existence, not data | `WHERE EXISTS (...)` |
| Index the join key | Slow joins on unindexed columns | `CREATE INDEX ON orders(customer_id)` |
| Aggregate before join | Prevent row explosion | Pre-aggregate in CTE then join |
| Broadcast hint | Small dim table in distributed SQL | `/*+ BROADCAST(dim) */` |

## Interview Tip 💡

> When asked a join question, before writing SQL: (1) draw the relationship (1:1? 1:many? many:many?), (2) count expected output rows, (3) choose the join type based on what should happen with non-matches. This structured thinking impresses interviewers more than jumping straight to code.
