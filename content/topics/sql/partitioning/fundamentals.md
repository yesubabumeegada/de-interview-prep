---
title: "SQL Partitioning - Fundamentals"
topic: sql
subtopic: partitioning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, partitioning, range-partition, list-partition, hash-partition, partition-pruning]
---

# SQL Partitioning — Fundamentals


## 🎯 Analogy

Think of table partitioning like filing cabinets organized by year: when you query 2024 data, the database opens only the 2024 drawer instead of searching all 10 years of records.

---
## What Is Table Partitioning?

**Partitioning** splits a large table into smaller physical pieces (partitions) while presenting them as a single logical table to queries. Each partition stores a subset of the data based on a partitioning key.

> **Analogy:** Imagine a filing cabinet with thousands of folders. Instead of one massive drawer with all folders mixed together, you use 12 drawers — one per month. When you need files from January, you open only the January drawer instead of searching all 12. That's partitioning: organize data into predictable subsets so queries only touch the relevant subset.

**Key benefits:**
- **Partition pruning:** Queries filtering on the partition key skip irrelevant partitions entirely
- **Parallel operations:** Each partition can be vacuumed, indexed, or backed up independently
- **Data lifecycle:** Drop an old partition (instant) instead of running a massive DELETE
- **Archiving:** Move cold partitions to cheaper storage

---

## Types of Partitioning

### 1. Range Partitioning — by continuous value ranges

Most common for time-series data:

```sql
-- PostgreSQL: partition orders by month
CREATE TABLE orders (
    order_id   BIGSERIAL,
    customer_id INT,
    amount     NUMERIC(10,2),
    order_date DATE NOT NULL,
    status     TEXT
) PARTITION BY RANGE (order_date);

-- Create monthly partitions:
CREATE TABLE orders_2024_01 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE orders_2024_02 PARTITION OF orders
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE orders_2024_03 PARTITION OF orders
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

-- Insert goes to the correct partition automatically:
INSERT INTO orders (customer_id, amount, order_date) VALUES (101, 99.99, '2024-01-15');
-- Row lands in orders_2024_01

-- Query: only scans orders_2024_01 (partition pruning!)
SELECT * FROM orders WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31';
```

**EXPLAIN shows partition pruning:**
```sql
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-01-15';
-- Output:
-- Append
--   -> Seq Scan on orders_2024_01   ← Only this partition scanned
--        Filter: (order_date = '2024-01-15')
-- (orders_2024_02, 03, etc. are NOT mentioned — pruned!)
```

### 2. List Partitioning — by discrete values

```sql
-- Partition customers by country/region
CREATE TABLE customers (
    customer_id INT,
    name        TEXT,
    region      TEXT NOT NULL,
    email       TEXT
) PARTITION BY LIST (region);

CREATE TABLE customers_americas PARTITION OF customers
    FOR VALUES IN ('US', 'CA', 'MX', 'BR');

CREATE TABLE customers_europe PARTITION OF customers
    FOR VALUES IN ('UK', 'DE', 'FR', 'IT', 'ES');

CREATE TABLE customers_apac PARTITION OF customers
    FOR VALUES IN ('JP', 'AU', 'SG', 'IN', 'CN');

CREATE TABLE customers_other PARTITION OF customers DEFAULT;  -- Catch-all

-- Query: only scans customers_americas
SELECT * FROM customers WHERE region = 'US';
```

### 3. Hash Partitioning — by hash of the key

Distributes data evenly when there's no natural range or list:

```sql
-- Partition by hash of customer_id (e.g., for multi-tenant systems)
CREATE TABLE events (
    event_id    BIGSERIAL,
    customer_id INT NOT NULL,
    event_type  TEXT,
    event_ts    TIMESTAMPTZ
) PARTITION BY HASH (customer_id);

-- Create 8 equal-sized partitions (modulus = total partitions, remainder = shard index)
CREATE TABLE events_p0 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE events_p1 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE events_p2 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 2);
-- ... through p7
```

**When to use hash partitioning:**
- Data distribution by customer_id or user_id (no natural order)
- Multi-tenant databases: each partition might correspond to a tenant group
- When you want even distribution for parallel processing

---

## Partition Pruning

Partition pruning is the optimizer's ability to skip partitions that can't contain matching rows:

```sql
-- With 36 monthly partitions (3 years of data):
EXPLAIN ANALYZE
SELECT SUM(amount) FROM orders
WHERE order_date >= '2024-01-01' AND order_date < '2024-04-01';

-- Without partitioning: scans all 3 years of data
-- With partitioning: only scans orders_2024_01, 02, 03 (3 of 36 partitions)
-- → 33 partitions skipped (92% of data not touched)
```

**Pruning works when:**
- The WHERE clause filters on the partition key directly
- The filter uses comparison operators (`=`, `<`, `>`, `BETWEEN`, `IN`)

**Pruning does NOT work when:**
- The partition key is wrapped in a function: `WHERE YEAR(order_date) = 2024` — use `WHERE order_date >= '2024-01-01'` instead
- The filter uses an expression not matching the partition key

---

## Managing Partitions

### Adding a New Partition

```sql
-- Add next month's partition (do this before the month starts):
CREATE TABLE orders_2024_04 PARTITION OF orders
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

-- Create indexes on the new partition (they're inherited from parent in PostgreSQL 11+):
-- Modern PostgreSQL: indexes on parent table automatically apply to new partitions
CREATE INDEX ON orders (customer_id, order_date);  -- Applied to ALL partitions
```

### Dropping Old Partitions

```sql
-- Drop a partition: instant! (no DELETE needed)
-- Compared to DELETE FROM orders WHERE order_date < '2022-01-01' which could take hours
DROP TABLE orders_2022_01;  -- Drops the partition and all its rows instantly

-- Or: detach and archive (keep the data in a separate table)
ALTER TABLE orders DETACH PARTITION orders_2022_01;
-- orders_2022_01 now exists as a standalone table — can be moved to cold storage
ALTER TABLE orders_2022_01 RENAME TO orders_archive_2022_01;
```

### Checking Partition Status

```sql
-- PostgreSQL: list all partitions of a table
SELECT 
    child.relname AS partition_name,
    pg_size_pretty(pg_relation_size(child.oid)) AS partition_size,
    pg_get_expr(child.relpartbound, child.oid) AS partition_range
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'orders'
ORDER BY partition_name;
```

---

## Partitioning in Other Databases

### MySQL Partitioning

```sql
-- MySQL: range partitioning on date
CREATE TABLE orders (
    order_id   INT,
    customer_id INT,
    amount     DECIMAL(10,2),
    order_date DATE
)
PARTITION BY RANGE (YEAR(order_date) * 100 + MONTH(order_date)) (
    PARTITION p2024_01 VALUES LESS THAN (202402),
    PARTITION p2024_02 VALUES LESS THAN (202403),
    PARTITION p2024_03 VALUES LESS THAN (202404),
    PARTITION p_max    VALUES LESS THAN MAXVALUE
);
```

### SQL Server Partitioning

```sql
-- SQL Server: requires partition function + scheme
CREATE PARTITION FUNCTION pf_orders_by_month (DATE)
AS RANGE RIGHT FOR VALUES (
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01'
);

CREATE PARTITION SCHEME ps_orders_by_month
AS PARTITION pf_orders_by_month
ALL TO ([PRIMARY]);

CREATE TABLE Orders (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE
) ON ps_orders_by_month(OrderDate);
```

---

## Partitioning vs Indexing

| Feature | Partitioning | Indexing |
|---------|-------------|---------|
| Best for | Range queries on partition key, large tables | Any selective column |
| Data organization | Physical data split | Separate lookup structure |
| Drop old data | Drop partition (instant) | Run DELETE (slow) |
| Parallel operations | Per-partition | Entire table |
| Adds complexity | Yes (more objects to manage) | Minimal |
| Helps with aggregations | Yes (parallel partition scans) | No |
| Query requirement | Filter on partition key | Filter on indexed column |

**Use both:** An index on a partitioned table applies to each partition independently — the optimizer can both prune partitions AND use the index within the scanned partitions.

---


## ▶️ Try It Yourself

```sql
-- Postgres: create a partitioned table by date range
CREATE TABLE orders (
    id SERIAL,
    order_date DATE NOT NULL,
    amount DECIMAL
) PARTITION BY RANGE (order_date);

-- Create partitions for each year
CREATE TABLE orders_2023 PARTITION OF orders
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Query only hits the 2024 partition (partition pruning)
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-06-15';
-- → only scans orders_2024
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When should you partition a table?" — "Partitioning is most beneficial for tables that: (1) are very large (100GB+), (2) have a natural partition key you always filter on (like order_date), and (3) need lifecycle management — you regularly archive or delete old data. For smaller tables, indexing is sufficient. The key indicator: if your queries always filter on order_date and you're deleting old data frequently, time-based range partitioning is the right choice."

> **Tip 2:** "What is partition pruning and why does it matter?" — "Partition pruning is when the query optimizer skips partitions that can't contain matching rows based on the WHERE clause. For a 3-year table partitioned by month (36 partitions), a query for a single month scans 1/36 of the data instead of all 3 years. This is a 36× reduction in I/O. Pruning only works when the WHERE clause filters directly on the partition key without wrapping it in a function."

> **Tip 3:** "How is dropping a partition better than DELETE for data lifecycle?" — "DROP TABLE for a partition is a metadata operation — it removes the partition's entries from the system catalog and deallocates the storage blocks in constant time, regardless of how many rows are in the partition. DELETE with a WHERE clause must find every row, mark it as deleted, update indexes, and generate transaction log entries — it can take hours on a partition with millions of rows and causes significant WAL bloat."
