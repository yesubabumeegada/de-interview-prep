---
title: "Hive Performance Tuning - Real World"
topic: hadoop
subtopic: hive-performance
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, hive, performance, tez, llap]
---

# Hive Performance Tuning — Real World

## War Story: The 6-Hour ETL That Became 18 Minutes

**Situation:** A daily ETL loading 2 TB of clickstream data into a reporting table was running 6 hours and blocking downstream SLAs.

**Diagnosis steps:**

```sql
-- Step 1: Check for full table scans
EXPLAIN
SELECT user_id, COUNT(*) AS clicks
FROM   clickstream
WHERE  event_date = '2024-01-15'
GROUP BY user_id;
-- Found: TableScan with no partition filter operator
-- Root cause: event_date column existed but table was NOT partitioned on it

-- Step 2: Check reducer count
SET hive.exec.reducers.bytes.per.reducer=67108864;
-- Input was 2 TB → expected ~30,000 reducers, actual was 200 (hardcoded)
-- Root cause: SET mapreduce.job.reduces=200 in a shared config file

-- Step 3: Check for skew
SELECT user_id, COUNT(*) FROM clickstream GROUP BY user_id ORDER BY 2 DESC LIMIT 5;
-- Top user_id had 40% of all rows (bot traffic)
```

**Fixes applied:**

1. Recreated table partitioned by `event_date` and backfilled.
2. Removed hardcoded reducer count; switched to auto-sizing.
3. Enabled skew join for `user_id` joins.
4. Enabled vectorization + Tez.
5. Added ORC bloom filters on `user_id` and `event_type`.

**Result:** 6 hours → 18 minutes.

---

## Diagnosing Slow Queries: Systematic Approach

```
1. EXPLAIN the query
   → Are statistics available? (run ANALYZE if not)
   → Is the right join type selected?
   → Is partition pruning happening?

2. Check Tez DAG UI (port 8088)
   → Which vertices are slowest?
   → Is one reducer handling disproportionate data? (skew)
   → Are there straggler tasks?

3. Profile data distribution
   → SELECT key, COUNT(*) GROUP BY key ORDER BY 2 DESC LIMIT 20

4. Check file sizes
   → HDFS dfs -ls -h /warehouse/tablespace/...
   → Many small files (< 128 MB) → merge or use CONCATENATE

5. Review GC logs
   → Excessive GC → increase heap or reduce data per container
```

---

## Small File Problem and Remediation

HDFS namenode tracks every file as a metadata object. Tables with millions of small files (common after many incremental loads) degrade both namenode performance and query speed.

**Detection:**

```bash
hdfs dfs -count /warehouse/tablespace/managed/hive/sales/
# FILE_COUNT  DIR_COUNT  CONTENT_SIZE  PATHNAME
# 1,250,000   3,200      2.1 T         /warehouse/.../sales
# If FILE_COUNT >> (CONTENT_SIZE / 128MB), small file problem
```

**Fix 1: ORC CONCATENATE**

```sql
-- Merge small ORC files within each partition
ALTER TABLE sales PARTITION(sale_date='2024-01') CONCATENATE;
```

**Fix 2: INSERT OVERWRITE with merge**

```sql
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;   -- 256 MB target
SET hive.merge.smallfiles.avgsize=64000000; -- trigger if avg < 64 MB

INSERT OVERWRITE TABLE sales PARTITION(sale_date='2024-01')
SELECT * FROM sales WHERE sale_date='2024-01';
```

**Fix 3: Hive compaction for ACID tables**

```sql
ALTER TABLE sales PARTITION(sale_date='2024-01') COMPACT 'MAJOR';
SHOW COMPACTIONS;
```

---

## Production Configuration Baseline

```sql
-- Engine
SET hive.execution.engine=tez;

-- CBO + statistics
SET hive.cbo.enable=true;
SET hive.stats.autogather=true;
SET hive.stats.column.autogather=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;

-- Vectorization
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

-- Join optimization
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000;

-- Skew join
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;

-- Dynamic partition pruning (Tez)
SET hive.tez.dynamic.partition.pruning=true;

-- Predicate pushdown
SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;

-- Tez container
SET hive.tez.container.size=4096;
SET hive.tez.java.opts=-Xmx3686m;

-- Reducer sizing
SET hive.exec.reducers.bytes.per.reducer=67108864;
SET hive.exec.reducers.max=1009;

-- Small file merging
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
```

---

## Interview Gotchas

**Q: Your Hive query has partition pruning in the WHERE clause but still does a full scan. Why?**

A: Common causes:
1. UDF wrapping the partition column: `WHERE UPPER(sale_date) = '2024-01'` — UDF prevents pruning. Solution: `WHERE sale_date = '2024-01'` (apply transform at write time).
2. Implicit type cast: partition column is STRING but filter is INTEGER → cast bypasses pruning.
3. Dynamic partition pruning disabled or sub-query not recognized by CBO (check `EXPLAIN`).

**Q: Map join fails with OutOfMemory. How do you fix it?**

A: Either increase the MAPJOIN hash table memory threshold:
```sql
SET hive.mapjoin.localtask.max.memory.usage=0.90;
SET hive.tez.container.size=8192;
SET hive.tez.java.opts=-Xmx7372m;
```
Or filter/aggregate the small table before the join to reduce its in-memory footprint.

**Q: What does LLAP NOT help with?**

A: LLAP helps with read-heavy interactive queries on cached data. It does NOT help with:
- First-time cold reads (data not yet in IO cache)
- Write-heavy ETL (ACID compaction, INSERT OVERWRITE)
- Large shuffles (network-bound, not IO-bound)
- Out-of-memory LLAP daemons (mis-sized cache evicts useful data)
