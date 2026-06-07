---
title: "SQL Indexing Strategies - Fundamentals"
topic: sql
subtopic: indexing-strategies
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, indexing, b-tree, query-optimization, performance, postgresql, mysql]
---

# SQL Indexing Strategies — Fundamentals

## What Is an Index?

An **index** is a separate data structure maintained alongside a table that allows the database to find rows quickly without scanning every row. Think of it like the index in the back of a textbook — instead of reading every page to find "recursion," you jump directly to page 347.

> **Analogy:** Without an index, finding a customer by email in a 10 million row table means the database reads all 10 million rows (a "full table scan"). With an index on the `email` column, it finds the row in microseconds — like looking up a name in a phone book sorted alphabetically.

**The trade-off:** Indexes speed up reads but slow down writes (INSERT/UPDATE/DELETE must update the index) and consume additional disk space.

---

## Sample Data

**orders** (10 million rows in a real scenario)

| order_id | customer_id | order_date | status | amount | region |
|----------|------------|-----------|--------|--------|--------|
| 1 | 1001 | 2024-01-15 | shipped | 149.99 | west |
| 2 | 1002 | 2024-01-15 | pending | 89.00 | east |
| 3 | 1001 | 2024-01-16 | shipped | 299.00 | west |
| 4 | 1003 | 2024-01-17 | cancelled | 45.50 | north |
| 5 | 1002 | 2024-01-18 | shipped | 189.99 | east |

---

## How Indexes Work (B-Tree Basics)

The most common index type is a **B-Tree (Balanced Tree)**. The database organizes indexed values in a sorted tree structure:

```
                    [500]
                   /     \
            [250]            [750]
           /     \          /     \
       [100] [400]      [600] [900]
```

- To find `customer_id = 400`: traverse root → left → right → found (3 comparisons instead of scanning all rows)
- Lookups are O(log n) instead of O(n)

---

## Creating Indexes

### Single-Column Index

```sql
-- Create an index on customer_id
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- Now this query uses the index instead of scanning all rows:
SELECT * FROM orders WHERE customer_id = 1001;

-- Drop an index when no longer needed:
DROP INDEX idx_orders_customer_id;
```

### Composite (Multi-Column) Index

```sql
-- Index on multiple columns — order matters!
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- This query CAN use the index (leading column customer_id is present):
SELECT * FROM orders WHERE customer_id = 1001 AND order_date = '2024-01-15';

-- This query CAN use the index (leading column only):
SELECT * FROM orders WHERE customer_id = 1001;

-- This query CANNOT use the index efficiently (leading column is missing):
SELECT * FROM orders WHERE order_date = '2024-01-15';
```

> **Key Rule:** A composite index on `(A, B, C)` can be used by queries filtering on `A`, `A+B`, or `A+B+C` — but NOT on `B` alone or `C` alone. This is the **leftmost prefix rule**.

### Unique Index

```sql
-- Enforces uniqueness AND creates an index (both at once)
CREATE UNIQUE INDEX idx_users_email ON users(email);

-- A PRIMARY KEY automatically creates a unique index
ALTER TABLE orders ADD PRIMARY KEY (order_id);
```

---

## Types of Scans

When you run a query, the database chooses between several access methods:

| Scan Type | When Used | Cost |
|-----------|-----------|------|
| **Sequential Scan** (Full Table Scan) | No usable index, or fetching >15-20% of rows | O(n) |
| **Index Scan** | Index exists, selective query (few rows) | O(log n + rows) |
| **Index Only Scan** | All needed columns are in the index | O(log n) — no table access |
| **Bitmap Index Scan** | Moderate selectivity (PostgreSQL) | Between sequential and index scan |

```sql
-- See which scan type PostgreSQL chooses:
EXPLAIN SELECT * FROM orders WHERE customer_id = 1001;

-- Output might show:
-- Index Scan using idx_orders_customer_id on orders
--   Index Cond: (customer_id = 1001)

EXPLAIN SELECT * FROM orders;
-- Output:
-- Seq Scan on orders
--   (No WHERE clause — must read everything)
```

---

## Index Selectivity

**Selectivity** = how many unique values an index column has relative to total rows.

| Column | Distinct Values | Rows | Selectivity | Good Index? |
|--------|----------------|------|------------|-------------|
| `customer_id` | 1,000,000 | 10,000,000 | High | Yes |
| `status` | 4 (pending/shipped/etc.) | 10,000,000 | Low | Usually No |
| `region` | 8 | 10,000,000 | Low | No |
| `email` | 1,000,000 | 1,000,000 | Very High (unique) | Excellent |

> **Rule of thumb:** Index columns with high selectivity (many distinct values). Indexing a boolean or a status column with 4 values is usually wasteful — the database may choose a full table scan anyway because it's faster to just read the table than to read the index PLUS the table rows.

---

## Common Index Patterns

### Pattern 1: Primary Key (Always Indexed)

```sql
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,  -- Automatically creates a unique index
    email VARCHAR(255) NOT NULL,
    name VARCHAR(100)
);
```

### Pattern 2: Foreign Key Index

```sql
-- Without index on foreign key — joining orders to customers is slow
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(customer_id)
);

-- Add index on the foreign key column:
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- Now this join is fast (index lookup instead of full scan per customer):
SELECT c.name, o.amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = 1001;
```

### Pattern 3: Covering Index (Index-Only Scan)

```sql
-- Query needs: customer_id (filter) + order_date + amount (SELECT)
SELECT order_date, amount FROM orders WHERE customer_id = 1001;

-- Covering index: includes ALL columns the query needs
CREATE INDEX idx_orders_covering ON orders(customer_id, order_date, amount);

-- Now the query never touches the main table — it reads only the index!
-- EXPLAIN shows: "Index Only Scan" — fastest possible
```

### Pattern 4: Partial Index

```sql
-- Only index rows you actually query — saves space and maintenance cost
CREATE INDEX idx_orders_pending ON orders(customer_id)
WHERE status = 'pending';

-- This index is small (only pending orders) and fast for:
SELECT * FROM orders WHERE status = 'pending' AND customer_id = 1001;
-- But is NOT used for: WHERE status = 'shipped' AND customer_id = 1001
```

---

## When Indexes Are NOT Used

Even with an index, the optimizer may ignore it:

```sql
-- Function wrapping the column defeats the index:
SELECT * FROM orders WHERE YEAR(order_date) = 2024;  -- No index on YEAR(order_date)
-- Fix: use a range instead:
SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01';

-- Implicit type conversion:
SELECT * FROM orders WHERE customer_id = '1001';  -- String vs INT — may not use index

-- LIKE with leading wildcard:
SELECT * FROM users WHERE email LIKE '%gmail.com';  -- Can't use B-tree index
-- Fix: use full-text index or search engine; or LIKE 'john%' (prefix match works)

-- OR conditions across different columns:
SELECT * FROM orders WHERE customer_id = 1001 OR status = 'pending';
-- May not efficiently use a single index — consider two separate indexes + UNION
```

---

## Index Maintenance

Indexes need maintenance — they can become bloated or fragmented over time:

```sql
-- PostgreSQL: VACUUM and ANALYZE (run automatically by autovacuum)
VACUUM ANALYZE orders;

-- PostgreSQL: check index bloat
SELECT 
    tablename, indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size
FROM pg_indexes
WHERE tablename = 'orders';

-- Rebuild a bloated index (PostgreSQL 12+, non-blocking):
REINDEX INDEX CONCURRENTLY idx_orders_customer_id;

-- MySQL: check if index is used
SELECT * FROM sys.schema_unused_indexes WHERE object_schema = 'mydb';

-- SQL Server: check fragmentation
SELECT * FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED')
WHERE avg_fragmentation_in_percent > 30;
```

---

## Index Types Comparison

| Index Type | Best For | Databases |
|-----------|---------|----------|
| **B-Tree** | Equality, range, ORDER BY | All (default) |
| **Hash** | Equality only (`=`) | PostgreSQL, MySQL |
| **GIN** | Full-text, arrays, JSONB | PostgreSQL |
| **GiST** | Geometric data, ranges | PostgreSQL |
| **Bitmap** | Low-cardinality columns, data warehouses | Oracle, Redshift |
| **Clustered** | Physical row order = index order | SQL Server, MySQL (InnoDB) |
| **Columnstore** | Analytics, large aggregations | SQL Server, Redshift |

---

## Interview Tips

> **Tip 1:** "When should you add an index?" — "When a column is frequently used in WHERE, JOIN ON, or ORDER BY clauses, has high selectivity (many distinct values), and the table is large enough that a full scan is noticeably slow. I also check the read/write ratio — a heavily-written table with many indexes pays a high maintenance cost."

> **Tip 2:** "What's the difference between a clustered and non-clustered index?" — "A clustered index determines the physical order of rows on disk — the table IS sorted by the clustered index. SQL Server and MySQL InnoDB have one clustered index per table (usually the primary key). All other indexes are non-clustered — they store pointers back to the actual rows. PostgreSQL doesn't have clustered indexes in the same sense, but you can CLUSTER a table to physically reorder it."

> **Tip 3:** "How do you know if an index is being used?" — "I use EXPLAIN (PostgreSQL/MySQL) or EXPLAIN ANALYZE to see the query execution plan. An Index Scan or Index Only Scan means the index is being used. A Seq Scan means it's not — which is either expected (small table or low selectivity) or a problem (missing index or index not usable due to function wrapping)."
