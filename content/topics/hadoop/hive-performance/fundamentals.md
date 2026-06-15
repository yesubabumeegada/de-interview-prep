---
title: "Hive Performance Tuning - Fundamentals"
topic: hadoop
subtopic: hive-performance
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [hadoop, hive, performance, tez, llap]
---

# Hive Performance Tuning — Fundamentals

## 🎯 Analogy

Tuning Hive is like tuning a car engine: you can swap the carburetor (execution engine), add turbo (vectorization), or pick the right gear (partitioning) — each change compounds. Ignoring these settings means every query burns unnecessary CPU, memory, and time.

---

## Execution Engines: MapReduce vs Tez vs Spark

Hive supports three execution engines. The engine is chosen via `hive.execution.engine`.

| Engine | Speed | Notes |
|---|---|---|
| MapReduce | Slowest | Default historically; writes intermediate data to disk |
| Tez | Fast | DAG-based; keeps data in memory between stages |
| Spark | Fast | Fully in-memory; good for iterative and ML workloads |

```sql
-- Switch engine per session
SET hive.execution.engine=tez;
-- or
SET hive.execution.engine=spark;
```

**Why Tez is preferred for Hive:**
- Tez builds a Directed Acyclic Graph (DAG) of tasks, eliminating unnecessary MapReduce barriers.
- Intermediate results stay in memory rather than being written to HDFS between stages.
- Typical speedup over MapReduce: **2–10×** for multi-stage queries.

---

## Partitioning and Partition Pruning

Partitioning divides table data into directories based on a column value. Hive can skip entire partitions (partition pruning) if the WHERE clause filters on the partition key.

```sql
-- Create partitioned table
CREATE TABLE sales (
  order_id BIGINT,
  amount   DOUBLE
)
PARTITIONED BY (sale_date STRING, region STRING)
STORED AS ORC;

-- Hive prunes all partitions except 2024-01 / US
SELECT SUM(amount)
FROM   sales
WHERE  sale_date = '2024-01'
  AND  region    = 'US';
```

**Anti-pattern:** Querying a partitioned table without a partition filter causes a full table scan — every partition is read.

```sql
-- BAD: no partition filter → full scan
SELECT * FROM sales WHERE amount > 1000;
```

Enable dynamic partition pruning (Tez):
```sql
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.ppd=true;          -- predicate pushdown
SET hive.optimize.index.filter=true;
```

---

## Bucketing for Joins

Bucketing clusters data within each partition into a fixed number of files (buckets) based on a hash of the bucket column.

```sql
CREATE TABLE orders (
  order_id   BIGINT,
  customer_id BIGINT,
  amount     DOUBLE
)
CLUSTERED BY (customer_id) INTO 32 BUCKETS
STORED AS ORC;
```

Benefits for joins:
- When both tables are bucketed on the join key with the same number of buckets, Hive performs a **bucket map join** — joining matching buckets directly without shuffling the entire dataset.
- Enables **sampling** (`TABLESAMPLE(BUCKET 1 OUT OF 32)`).

```sql
SET hive.optimize.bucketmapjoin=true;
SET hive.enforce.bucketing=true;
```

---

## Map-Side Joins (Small Table Broadcast)

When one table in a join is small enough to fit in memory, Hive broadcasts it to every mapper, eliminating the reduce phase entirely.

```sql
-- Explicit MAPJOIN hint
SELECT /*+ MAPJOIN(d) */ f.order_id, d.dept_name
FROM   fact_orders f
JOIN   dim_dept    d ON f.dept_id = d.dept_id;
```

Auto-conversion (recommended):
```sql
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;  -- 25 MB threshold
```

If the small table is ≤ `hive.mapjoin.smalltable.filesize`, Hive automatically converts the join to a map join.

---

## Common Anti-Patterns to Avoid

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `SELECT *` | Reads all columns; defeats columnar pruning | Select only needed columns |
| No partition filter | Full table scan | Always filter on partition key |
| Cartesian join | O(N×M) shuffle | Use proper join conditions |
| Too many small files | High HDFS namenode pressure | Use ORC compaction or `CONCATENATE` |
| Implicit type casting in join | Prevents map join | Match data types explicitly |

---

## Key Configuration Cheat Sheet

```sql
SET hive.execution.engine=tez;
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;
SET hive.optimize.ppd=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
```
