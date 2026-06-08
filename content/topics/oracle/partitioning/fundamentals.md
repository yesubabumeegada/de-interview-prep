---
title: "Partitioning — Fundamentals"
topic: oracle
subtopic: partitioning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, partitioning, range-partition, list-partition, hash-partition, pruning]
---

# Partitioning — Fundamentals

## What Is Partitioning?

Partitioning divides a large table (or index) into smaller, physically separate segments called partitions. Each partition is stored separately but appears as one logical table. The database transparently routes queries to only the relevant partitions.

**Benefits:**
- **Partition pruning**: queries scan only matching partitions — dramatic I/O reduction
- **Partition-wise operations**: joins between partitioned tables can be parallelized by partition
- **Manageability**: drop, archive, or move individual partitions instead of the whole table
- **Availability**: maintenance on one partition doesn't affect others

---

## Range Partitioning

Best for date/time columns or any continuously increasing values:

```sql
-- Create a range-partitioned table by year
CREATE TABLE sales (
  sale_id      NUMBER,
  sale_date    DATE NOT NULL,
  customer_id  NUMBER,
  product_id   NUMBER,
  amount_usd   NUMBER(12,2)
)
PARTITION BY RANGE (sale_date) (
  PARTITION p2022 VALUES LESS THAN (DATE '2023-01-01'),
  PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
  PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01'),
  PARTITION p_max VALUES LESS THAN (MAXVALUE)  -- catch-all for future dates
);

-- Monthly partitioning (more granular)
CREATE TABLE order_events (
  event_id   NUMBER,
  event_time DATE NOT NULL,
  order_id   NUMBER,
  event_type VARCHAR2(50)
)
PARTITION BY RANGE (event_time) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))  -- AUTO-add monthly
(
  PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')
);
-- INTERVAL partitioning: Oracle automatically creates new partitions as data arrives!
```

---

## List Partitioning

Best for categorical values with known, discrete set of values:

```sql
CREATE TABLE customers (
  customer_id  NUMBER,
  region       VARCHAR2(20) NOT NULL,
  customer_name VARCHAR2(100),
  signup_date  DATE
)
PARTITION BY LIST (region) (
  PARTITION p_north  VALUES ('NORTHEAST', 'NORTHWEST'),
  PARTITION p_south  VALUES ('SOUTHEAST', 'SOUTHWEST'),
  PARTITION p_west   VALUES ('WEST', 'PACIFIC'),
  PARTITION p_intl   VALUES ('EUROPE', 'APAC', 'LATAM'),
  PARTITION p_other  VALUES (DEFAULT)  -- catches any unlisted values
);
```

---

## Hash Partitioning

Best when no natural range/list grouping exists — distributes rows evenly:

```sql
-- Hash partition: Oracle hashes customer_id to spread rows evenly
CREATE TABLE orders (
  order_id     NUMBER,
  customer_id  NUMBER NOT NULL,
  order_date   DATE,
  amount_usd   NUMBER(12,2)
)
PARTITION BY HASH (customer_id)
PARTITIONS 8  -- 8 evenly distributed partitions
STORE IN (tbs_01, tbs_02, tbs_03, tbs_04, tbs_05, tbs_06, tbs_07, tbs_08);
-- Use power of 2 (2, 4, 8, 16...) for optimal distribution
```

---

## Partition Pruning

Oracle automatically skips partitions that can't contain matching rows:

```sql
-- Query: only needs data from 2024 — Oracle prunes all other year partitions
SELECT SUM(amount_usd) FROM sales
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';

-- Verify partition pruning in explain plan
EXPLAIN PLAN FOR
SELECT SUM(amount_usd) FROM sales
WHERE sale_date >= DATE '2024-06-01' AND sale_date < DATE '2024-07-01';

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
-- Look for: Pstart (partition start) and Pstop (partition stop) columns
-- If they show KEY/KEY → dynamic pruning at runtime (bind variables)
-- If they show partition numbers → static pruning
```

---

## Partition Management

```sql
-- View partitions
SELECT table_name, partition_name, num_rows, blocks, last_analyzed,
       high_value
FROM dba_tab_partitions
WHERE table_name = 'SALES'
ORDER BY partition_position;

-- Add a new partition
ALTER TABLE sales ADD PARTITION p2025 VALUES LESS THAN (DATE '2026-01-01');

-- Drop a partition (instant — no full table scan)
ALTER TABLE sales DROP PARTITION p2022;
-- With UPDATE GLOBAL INDEXES to keep global indexes valid (slower but safe)
ALTER TABLE sales DROP PARTITION p2022 UPDATE GLOBAL INDEXES;

-- Truncate a partition (faster than DELETE)
ALTER TABLE sales TRUNCATE PARTITION p2022;

-- Exchange partition with a non-partitioned table (for bulk load pattern)
ALTER TABLE sales EXCHANGE PARTITION p2024 WITH TABLE sales_2024_staging
INCLUDING INDEXES WITHOUT VALIDATION;
-- Fast: swaps segment pointers, no data movement
```

---

## Composite Partitioning (Subpartitioning)

Combine two partition strategies:

```sql
-- Range-Hash: partition by month, then hash within each month
CREATE TABLE sales_composite (
  sale_id    NUMBER,
  sale_date  DATE NOT NULL,
  region     VARCHAR2(20),
  amount_usd NUMBER(12,2)
)
PARTITION BY RANGE (sale_date)
SUBPARTITION BY HASH (region) SUBPARTITIONS 4
(
  PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
  PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01'),
  PARTITION p_max VALUES LESS THAN (MAXVALUE)
);
-- 3 main partitions × 4 hash subpartitions = 12 total segments
```

---

## Interview Tips

> **Tip 1:** "What is partition pruning and why is it important?" — Partition pruning is Oracle's ability to skip entire partitions that can't contain rows matching a query's WHERE clause. If a sales table is partitioned by month and you query for January 2024, Oracle reads only the January 2024 partition — ignoring all other partitions. For a table with 5 years of monthly data (60 partitions), a monthly query reads 1/60th of the data.

> **Tip 2:** "When would you use range vs list vs hash partitioning?" — Range: date columns, continuously growing values (most common for fact tables). List: categorical columns with fixed known values (region, status, country code). Hash: no natural grouping, want even distribution, or when joining two large tables to enable partition-wise joins. Composite (range-hash, range-list) combines benefits of both.

> **Tip 3:** "What is INTERVAL partitioning?" — INTERVAL partitioning (Oracle 11g+) is an extension of range partitioning where Oracle automatically creates new partitions as data arrives outside existing partition bounds. Define an interval like `INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))` and Oracle adds a new monthly partition when the first row for that month is inserted — no manual partition management needed.
