---
title: "Hive Performance Tuning - Intermediate"
topic: hadoop
subtopic: hive-performance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, hive, performance, tez, llap]
---

# Hive Performance Tuning — Intermediate

## Cost-Based Optimizer (CBO) and ANALYZE TABLE

Hive's Cost-Based Optimizer (CBO) uses table and column statistics to generate more efficient query plans — choosing better join orders, join types, and aggregation strategies.

### Collecting Statistics

```sql
-- Compute table-level statistics
ANALYZE TABLE sales COMPUTE STATISTICS;

-- Compute column-level statistics (required for CBO join reordering)
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS
  order_id, customer_id, amount, sale_date;

-- For partitioned tables, analyze each partition
ANALYZE TABLE sales PARTITION(sale_date='2024-01') COMPUTE STATISTICS;
ANALYZE TABLE sales PARTITION(sale_date='2024-01') COMPUTE STATISTICS FOR COLUMNS amount;

-- Auto-gather stats on INSERT/LOAD
SET hive.stats.autogather=true;
SET hive.stats.column.autogather=true;
```

### Enabling CBO

```sql
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
```

CBO impacts:
- **Join reordering**: CBO places the smallest filtered table on the build (map) side.
- **Join algorithm selection**: Hash join vs. sort-merge-bucket join vs. map join.
- **Predicate pushdown**: Filters are pushed as close to data sources as possible.

### Verifying Statistics

```sql
DESCRIBE FORMATTED sales;
-- Look for: numRows, totalSize, rawDataSize in Table Parameters
```

---

## Vectorized Query Execution

Vectorization processes 1,024 rows at a time in a single CPU call instead of one row per call, dramatically improving throughput for scan-heavy queries.

```sql
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
```

**Requirements:**
- Table must be stored as ORC (vectorization reads ORC stripe batches natively).
- Supported operations: filters, projections, aggregations, joins, sorts.
- Check if a query is vectorized via `EXPLAIN VECTORIZATION DETAIL`.

```sql
EXPLAIN VECTORIZATION DETAIL
SELECT region, SUM(amount)
FROM   sales
WHERE  sale_date >= '2024-01'
GROUP BY region;
```

Look for `Vectorized execution: true` in the output. If a UDF is not vectorization-compatible, Hive falls back to row-at-a-time mode for that operator.

---

## Skew Join Optimization

Data skew occurs when one or more join keys have vastly more rows than others (e.g., `customer_id = NULL` or a "catch-all" category). A single reducer handles the entire skewed key, becoming the bottleneck.

### Detection

```sql
-- Find skewed keys
SELECT customer_id, COUNT(*) AS cnt
FROM   orders
GROUP BY customer_id
ORDER BY cnt DESC
LIMIT 10;
```

### Hive Skew Join

```sql
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;  -- key is skewed if >100k rows go to one reducer
```

Hive detects skew at runtime and re-routes heavy keys through a separate map-reduce job, while normal keys proceed through the standard join path.

### Manual Salting (alternative)

```sql
-- Add random salt to distribute skewed keys
SELECT /*+ MAPJOIN(b_salted) */
       a.order_id,
       b.customer_name
FROM (
  SELECT order_id,
         customer_id,
         FLOOR(RAND() * 10) AS salt
  FROM   orders
) a
JOIN (
  SELECT customer_id,
         customer_name,
         explode_salt AS salt
  FROM   customers
  LATERAL VIEW explode(array(0,1,2,3,4,5,6,7,8,9)) t AS explode_salt
) b_salted
  ON a.customer_id = b_salted.customer_id
 AND a.salt        = b_salted.salt;
```

---

## EXPLAIN Output Interpretation

`EXPLAIN` reveals the execution plan before running a query — essential for debugging slow queries.

```sql
EXPLAIN
SELECT region, SUM(amount)
FROM   sales
WHERE  sale_date = '2024-01'
GROUP BY region;
```

Key sections to examine:

| Section | What to look for |
|---|---|
| `Stage Dependencies` | Number of MR/Tez stages; fewer is better |
| `Map Operator Tree` | Filter operators close to TableScan = pruning works |
| `Reduce Operator Tree` | Aggregation / join strategy |
| `Statistics` | Row estimates; huge estimates → missing stats |
| `Fetch Operator` | Query answered from metadata alone (best case) |

```sql
-- Extended plan (Tez DAG details)
EXPLAIN EXTENDED SELECT ...;

-- CBO plan showing join reordering
EXPLAIN CBO SELECT ...;
```

Red flags in EXPLAIN output:
- `Cross Join` instead of `Hash Join`
- Huge estimated rows that don't match actual table size (run ANALYZE)
- Multiple `Shuffle` stages where one would suffice

---

## Hive ACID Transactions

Hive ACID (Atomicity, Consistency, Isolation, Durability) supports row-level `INSERT`, `UPDATE`, `DELETE` and `MERGE` on ORC tables.

```sql
-- Enable ACID
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=2;

-- Create ACID table
CREATE TABLE customer_profile (
  customer_id BIGINT,
  name        STRING,
  email       STRING
)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

-- Row-level UPDATE
UPDATE customer_profile
SET    email = 'new@example.com'
WHERE  customer_id = 12345;

-- MERGE (upsert)
MERGE INTO customer_profile AS t
USING staging_updates         AS s
ON (t.customer_id = s.customer_id)
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email
WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.name, s.email);
```

**Performance consideration:** ACID tables accumulate delta files over time. Run compaction to merge deltas:

```sql
ALTER TABLE customer_profile COMPACT 'MAJOR';
```

Minor compaction merges delta files; major compaction merges deltas into the base files, restoring full read performance.
