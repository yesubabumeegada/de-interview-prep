---
title: "ORC & Parquet Formats - Real World"
topic: hadoop
subtopic: orc-parquet-formats
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, orc, parquet, columnar, storage]
---

# ORC & Parquet Formats — Real World

## War Story: Schema Evolution Disaster with Parquet

**Situation:** A data engineering team renamed a column `user_id` → `uid` in their Spark pipeline that writes Parquet to S3. Old files (2 years of history) used `user_id`; new files used `uid`. The downstream Presto/Trino tables started returning `null` for `uid` on all historical data.

**Root cause:** Parquet uses column names for schema identity. When reading old files with the new schema, Parquet found no column named `uid` in old files and returned `null`. The old `user_id` column was effectively invisible.

**Fixes:**

```python
# Option 1: Rewrite historical Parquet with renamed column (expensive but clean)
df = spark.read.parquet("s3://bucket/data/year=2022/")
df.withColumnRenamed("user_id", "uid") \
  .write.mode("overwrite") \
  .parquet("s3://bucket/data/year=2022/")

# Option 2: Read with alias at query time (temporary bridge)
df = spark.read.parquet("s3://bucket/data/")
# Coerce both column names
from pyspark.sql.functions import col, coalesce
df = df.withColumn("uid", coalesce(col("uid"), col("user_id"))) \
       .drop("user_id")

# Option 3: Use Iceberg (supports column rename as metadata-only operation)
spark.sql("ALTER TABLE catalog.db.events RENAME COLUMN user_id TO uid")
# → Iceberg records the mapping; old files still work transparently
```

**Lesson:** Never rename Parquet columns in place. Use Iceberg/Delta if you need rename support, or manage renames through a view layer.

---

## War Story: 200 GB Parquet File That Killed Athena

**Situation:** An EMR job produced 3 files of 200 GB each for a single partition. AWS Athena queries on that partition timed out; other engines took 40+ minutes for simple aggregations.

**Root cause:** Default Parquet row group size was left at 128 MB. A 200 GB file contained ~1,600 row groups. Each Athena worker reads one row group at a time but must open the file, seek to the right offset, read statistics, and potentially read data — for 1,600 row groups. The overhead dominated query time.

**Fix:**

```python
# Repartition to many files of ~256 MB each
# 200 GB / 256 MB = ~800 files — much better parallelism
df = spark.read.parquet("s3://bucket/big_partition/")

df.repartition(800) \
  .write \
  .option("compression", "snappy") \
  .parquet("s3://bucket/big_partition_fixed/")
```

**Prevention:**

```python
spark.conf.set("spark.sql.files.maxRecordsPerFile", 10_000_000)  # hard cap per file
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
```

---

## Choosing the Right Format: Decision Framework

```
1. Is your primary engine Hive?
   → YES: ORC (native LLAP IO cache, ACID support, better Hive stats)
   → NO: continue

2. Do you need ACID row-level mutations (UPDATE/DELETE/MERGE)?
   → YES + Hive: ORC ACID
   → YES + Spark: Delta Lake or Iceberg (both use Parquet as base format)
   → NO: continue

3. Do multiple engines need to read this data? (Spark + Trino + Athena + Flink)
   → YES: Parquet + Iceberg (widest ecosystem support)
   → NO: continue

4. Is this a streaming / append-only log dataset?
   → YES: Parquet + Snappy or ZSTD (Parquet + Kafka S3 sink is standard)
   → NO: continue

5. Do you need schema evolution flexibility?
   → YES: Avro (for Kafka events) or Iceberg (for Lakehouse)
   → NO: Parquet or ORC based on engine above
```

---

## ORC ACID Production Operations

```sql
-- Check ACID table health
SHOW TRANSACTIONS;
SHOW LOCKS;
SHOW COMPACTIONS;

-- Kill a stuck transaction
ABORT TRANSACTIONS <txnId>;

-- Compact a single partition (common after large backfills)
ALTER TABLE orders PARTITION(sale_date='2024-01') COMPACT 'MAJOR'
  WITH OVERWRITE TBLPROPERTIES ('compaction.mapreduce.map.memory.mb'='8192');

-- Auto-compaction configuration (hive-site.xml)
-- hive.compactor.initiator.on=true
-- hive.compactor.worker.threads=4
-- hive.compactor.delta.num.threshold=10   (minor compaction after 10 deltas)
-- hive.compactor.delta.pct.threshold=0.1  (major compaction if deltas > 10% of base)
-- hive.compactor.abortedtxn.threshold=1000
```

---

## Parquet + ZSTD: Production Configuration for Spark 3

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.parquet.compression.codec", "zstd") \
    .config("spark.io.compression.zstd.level", "3") \          # 1=fast, 22=max ratio
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m") \
    .config("parquet.enable.dictionary", "true") \
    .config("parquet.page.size", str(1024 * 1024)) \           # 1 MB pages
    .config("parquet.block.size", str(128 * 1024 * 1024)) \    # 128 MB row groups
    .getOrCreate()

# Write
df.write \
  .mode("overwrite") \
  .partitionBy("sale_date", "region") \
  .parquet("s3://bucket/sales/")
```

**ZSTD level guidance:**
- Level 1: ~same speed as Snappy, 10–15% better ratio
- Level 3 (default): good balance; ~2× Snappy ratio, 80% of Snappy speed
- Level 9+: diminishing returns; use only for cold archival

---

## Interview Gotchas

**Q: A Parquet query on Athena is not using predicate pushdown even though `filterPushdown=true`. Why?**

A: Common causes:
1. The filter column is not the first column in the row group sort order — statistics exist but may not be useful without sorting.
2. The Parquet files were written without row group statistics (some older writers omit them).
3. Complex filter expression (UDF, LIKE, NOT IN) — Parquet can only push down simple comparison and equality predicates.
4. The filter is on a nested field — Athena's Parquet reader may not push down nested predicates.

**Q: When does ORC's bloom filter NOT help?**

A: Bloom filters help only for **equality predicates** (`col = value` or `col IN (...)`) on high-cardinality columns. They do NOT help for range predicates (`col > 100`, `col BETWEEN 1 AND 10`) — use min/max statistics (which ORC already tracks per row group) for ranges. Bloom filters also have a false positive rate (FPP); with FPP=5%, 5% of stripes that don't contain the value are still read.

**Q: Your Spark job writes 10,000 Parquet files per day due to high partition cardinality. How do you fix it?**

A: Three approaches:
1. `df.repartition(col("partition_key")).write.partitionBy("partition_key").parquet(...)` — one file per partition key value, but may cause skew.
2. AQE coalesce: `spark.sql.adaptive.coalescePartitions.enabled=true` — Spark merges small shuffle partitions before writing.
3. `spark.sql.files.maxRecordsPerFile`: cap rows per file; combined with `coalesce()` or AQE to keep file count manageable.
4. For Iceberg: use `rewrite_data_files` procedure to compact asynchronously after writes.
