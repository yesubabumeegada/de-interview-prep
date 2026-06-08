---
title: "Partitioning — Intermediate"
topic: oracle
subtopic: partitioning
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, partitioning, local-indexes, global-indexes, partition-exchange, pwise-joins]
---

# Partitioning — Intermediate

## Local vs Global Indexes on Partitioned Tables

### Local Index
Each partition has its own index segment — index is co-located with data partition:

```sql
-- Local index: one index partition per table partition
CREATE INDEX idx_sales_customer_local ON sales(customer_id) LOCAL;

-- Local prefixed: partition key is leading column of index
-- (enables partition pruning through the index)
CREATE INDEX idx_sales_date_local ON sales(sale_date, amount_usd) LOCAL;

-- Local non-prefixed: partition key is NOT the leading column
-- Still local (one index seg per table partition), but no pruning
CREATE INDEX idx_sales_region_local ON sales(region) LOCAL;
```

### Global Index
One index spans all table partitions — or can be range-partitioned itself:

```sql
-- Global non-partitioned: single index for entire table
CREATE INDEX idx_sales_order_global ON sales(order_id);  -- default: global non-partitioned

-- Global partitioned: index is partitioned independently of the table
CREATE INDEX idx_sales_cust_global_part ON sales(customer_id)
GLOBAL PARTITION BY RANGE (customer_id) (
  PARTITION gp1 VALUES LESS THAN (100000),
  PARTITION gp2 VALUES LESS THAN (500000),
  PARTITION gp3 VALUES LESS THAN (MAXVALUE)
);
```

### Comparison: Local vs Global

| Aspect | Local Index | Global Index |
|---|---|---|
| Partition pruning | Yes (for prefixed) | No |
| Index maintenance when dropping/truncating partition | Automatic — that index partition is dropped too | **Must use UPDATE GLOBAL INDEXES** or index becomes UNUSABLE |
| Uniqueness enforcement | Only within a partition (for non-prefixed) | Across entire table |
| Best for | Range queries on partition key | Point lookups on non-partition key (e.g., order_id) |
| Availability | Partition-level | Table-level |

```sql
-- Drop partition: local index maintains itself
ALTER TABLE sales DROP PARTITION p2022;  -- idx_sales_customer_local auto-maintained

-- Drop partition: global index becomes UNUSABLE unless you use UPDATE GLOBAL INDEXES
ALTER TABLE sales DROP PARTITION p2022 UPDATE GLOBAL INDEXES;  -- expensive but keeps global indexes valid

-- Rebuild a UNUSABLE global index (if you forgot UPDATE GLOBAL INDEXES)
ALTER INDEX idx_sales_order_global REBUILD;
ALTER INDEX idx_sales_cust_global_part REBUILD PARTITION gp1;
```

---

## Partition Exchange — Bulk Load Pattern

Partition exchange is the fastest way to load data into a partitioned table (no row-by-row insert):

```sql
-- Pattern: build data in a staging table, then swap it into the partition
-- Step 1: Create a regular (non-partitioned) staging table matching the partition structure
CREATE TABLE sales_2024_jan_stage AS
SELECT * FROM sales WHERE 1=0;  -- empty, same structure

-- Step 2: Load data into the staging table (can use INSERT /*+ APPEND */ parallel loads)
INSERT /*+ APPEND PARALLEL(8) */ INTO sales_2024_jan_stage
SELECT * FROM external_sales_source WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31';
COMMIT;

-- Step 3: Validate data (row counts, ranges, etc.)
SELECT COUNT(*), MIN(sale_date), MAX(sale_date) FROM sales_2024_jan_stage;

-- Step 4: Exchange — instant partition swap (no data movement)
ALTER TABLE sales EXCHANGE PARTITION p2024_jan 
WITH TABLE sales_2024_jan_stage
INCLUDING INDEXES         -- exchange local index segment too
WITHOUT VALIDATION;       -- trust the data is valid (faster)
-- Note: WITH VALIDATION checks every row has a valid partition key — very slow

-- Step 5: The staging table now has the old partition data (or is empty if exchange)
-- Drop the staging table
DROP TABLE sales_2024_jan_stage PURGE;
```

---

## Partition-Wise Joins

When joining two tables partitioned on the same key, Oracle can join matching partitions independently — enabling full parallelism:

```sql
-- Both tables partitioned on customer_id with same partition strategy
-- → Oracle joins partition 1 of customers with partition 1 of orders in parallel

SELECT /*+ PARALLEL(c, 4) PARALLEL(o, 4) PQ_DISTRIBUTE(o PARTITION NONE) */
  c.region, COUNT(*) orders, SUM(o.amount_usd) revenue
FROM customers c JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.region;

-- Check explain plan for partition-wise join indicator
-- Look for: "PX PARTITION HASH" or "PX PARTITION RANGE" with same partition keys
```

---

## Online Partition Operations (Oracle 12c+)

```sql
-- Move a partition online (no DML lock — live data is accessible)
ALTER TABLE sales MOVE PARTITION p2022 ONLINE
TABLESPACE tbs_archive
UPDATE INDEXES;  -- keep indexes valid

-- Split a partition online (no downtime)
ALTER TABLE sales SPLIT PARTITION p2024 AT (DATE '2024-07-01')
INTO (PARTITION p2024h1, PARTITION p2024h2) ONLINE UPDATE INDEXES;

-- Merge two partitions online
ALTER TABLE sales MERGE PARTITIONS p2021, p2022
INTO PARTITION p2021_2022 ONLINE UPDATE INDEXES;
```

---

## Reference Partitioning

Partition a child table based on the parent table's partition key — no need to store the key in the child:

```sql
-- Parent: orders partitioned by order_date
CREATE TABLE orders (
  order_id   NUMBER PRIMARY KEY,
  order_date DATE NOT NULL,
  customer_id NUMBER
)
PARTITION BY RANGE (order_date) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION p_init VALUES LESS THAN (DATE '2024-01-01'));

-- Child: order_items partitioned BY REFERENCE to orders (inherits same partition scheme)
CREATE TABLE order_items (
  item_id    NUMBER PRIMARY KEY,
  order_id   NUMBER NOT NULL REFERENCES orders(order_id),
  product_id NUMBER,
  quantity   NUMBER,
  unit_price NUMBER
)
PARTITION BY REFERENCE (order_id);  -- order_id FK references orders.order_id
-- order_items is now co-partitioned with orders — same partition layout, no date column needed
```

Benefits: partition-wise joins between orders and order_items are automatic; when you truncate/drop an orders partition, the corresponding order_items partition is also dropped.

---

## Partition Statistics

```sql
-- Gather stats per partition (useful after loading a specific partition)
EXEC DBMS_STATS.GATHER_TABLE_STATS(
  ownname        => 'SALES_SCHEMA',
  tabname        => 'SALES',
  partname       => 'P2024_JAN',  -- only this partition
  granularity    => 'PARTITION',
  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
);

-- Gather incremental statistics (only recalculate partitions that changed)
EXEC DBMS_STATS.SET_TABLE_PREFS('SALES_SCHEMA', 'SALES', 'INCREMENTAL', 'TRUE');
EXEC DBMS_STATS.GATHER_TABLE_STATS('SALES_SCHEMA', 'SALES');
-- Only changed partitions are sampled; global stats are synthesized from partition stats

-- Check partition-level stats
SELECT partition_name, num_rows, blocks, last_analyzed
FROM dba_tab_partitions
WHERE table_name = 'SALES' AND owner = 'SALES_SCHEMA'
ORDER BY partition_position;
```

---

## Interview Tips

> **Tip 1:** "What happens to global indexes when you drop a partition?" — Global indexes become UNUSABLE unless you include `UPDATE GLOBAL INDEXES` in the ALTER TABLE statement. UNUSABLE indexes cause queries that would use them to fail (ORA-01502). Options: (1) `UPDATE GLOBAL INDEXES` — maintains indexes but is slow, (2) drop partition + rebuild global indexes separately during maintenance window, (3) use local indexes instead of global wherever possible.

> **Tip 2:** "Explain the partition exchange pattern." — Instead of inserting millions of rows into a partitioned table (which is slow even with APPEND), you: (1) build and validate the data in a plain staging table, (2) use `ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE staging` to atomically swap the segment pointers. The entire operation is instantaneous — no data movement, just a data dictionary update. It's the standard bulk load pattern for large partitioned tables.

> **Tip 3:** "What is incremental statistics?" — With `INCREMENTAL => TRUE`, Oracle samples only partitions whose data has changed since the last stats gather, then synthesizes new global stats from all partition-level stats. For a 5-year table with monthly partitions, only the current month's partition is re-sampled — not all 60 partitions. Critical for large partitioned tables where full stats gather would take hours.
