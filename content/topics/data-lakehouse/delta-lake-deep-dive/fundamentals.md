---
title: "Delta Lake Deep Dive — Fundamentals"
topic: data-lakehouse
subtopic: delta-lake-deep-dive
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [delta-lake, transaction-log, acid, time-travel, schema-evolution]
---

# Delta Lake Deep Dive — Fundamentals


## 🎯 Analogy

Think of Delta Lake's transaction log like a blockchain for your table: every operation appends a JSON entry to `_delta_log/` listing exactly which files were added or removed — time travel, ACID, and schema enforcement all derive from this single log.

---
## What Is Delta Lake?

Delta Lake is an open-source storage layer created by Databricks that adds ACID transactions, schema enforcement, and time-travel to Apache Parquet files on object storage. It's the table format underlying the Databricks Lakehouse Platform.

```
Delta Lake = Parquet files + Transaction Log (_delta_log/)

Without Delta:
  s3://bucket/orders/year=2024/month=01/part-00000.parquet
  Multiple writers → partial writes, corrupt state, no history

With Delta:
  s3://bucket/orders/
  ├── _delta_log/
  │   ├── 00000000000000000000.json   ← commit 0: schema + initial data
  │   ├── 00000000000000000001.json   ← commit 1: added files
  │   ├── 00000000000000000002.json   ← commit 2: DELETE operation
  │   └── 00000000000000000010.checkpoint.parquet  ← snapshot every 10 commits
  └── year=2024/month=01/
      ├── part-00000.parquet          ← data files
      └── part-00001.parquet
```

---

## The Delta Transaction Log

```
Each commit JSON file records:
  add:    which Parquet files were added (path, size, stats)
  remove: which Parquet files were logically deleted (tombstoned)
  metaData: schema changes, table configuration changes
  protocol: minimum reader/writer version required

Example commit entry:
{
  "add": {
    "path": "year=2024/month=01/part-00001.parquet",
    "partitionValues": {"year": "2024", "month": "01"},
    "size": 134217728,
    "modificationTime": 1705312800000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000000,\"minValues\":{\"order_id\":1},\"maxValues\":{\"order_id\":1000000}}"
  }
}

How Delta reads:
  1. Read all commit JSONs from _delta_log/ (or latest checkpoint)
  2. Reconstruct table state: which files are "live" (added but not removed)
  3. Read those Parquet files → query result

Data skipping:
  Delta stores min/max values per column in the commit stats
  Query: WHERE order_id BETWEEN 100 AND 200
  Delta checks: which files have maxValues.order_id >= 100 AND minValues.order_id <= 200
  Skips files that can't possibly contain matching rows
```

---

## ACID Guarantees in Delta Lake

```
Atomicity: All changes in a transaction are committed or none are
  Example: INSERT of 1M rows → either all visible or none visible
  How: Parquet files written first, then JSON commit entry added
  If writer crashes before JSON commit → files exist but table doesn't see them (orphans)

Consistency: Schema is enforced on every write
  Try to write wrong schema → AnalysisException before any data is written
  Ensures table always has valid data

Isolation: Concurrent reads see consistent snapshots
  Writer adds files → reader sees old snapshot until writer commits JSON
  Optimistic concurrency for writers (conflict detection on commit)

Durability: Committed data is durable
  JSON commit file written to S3 → data is committed
  S3 provides durability (11 nines)

Serializable isolation (Databricks Delta only):
  Full serializability available as table property
  spark.sql("ALTER TABLE orders SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')")
  Default: WriteSerializable (weaker, better performance)
```

---

## Core Delta Operations

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create
df.write.format("delta").partitionBy("order_date").save("s3://bucket/orders")

# Read
df = spark.read.format("delta").load("s3://bucket/orders")

# MERGE (upsert)
delta = DeltaTable.forPath(spark, "s3://bucket/orders")
delta.alias("t").merge(
    updates.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Time travel
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("s3://bucket/orders")

df_at_time = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 12:00:00") \
    .load("s3://bucket/orders")

# History
spark.sql("DESCRIBE HISTORY delta.`s3://bucket/orders`").show()

# VACUUM (remove old files)
spark.sql("VACUUM delta.`s3://bucket/orders` RETAIN 168 HOURS")

# OPTIMIZE + ZORDER
spark.sql("OPTIMIZE delta.`s3://bucket/orders` ZORDER BY (customer_id, order_date)")
```

---


## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.master("local[*]")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

from delta.tables import DeltaTable
import os

path = "/tmp/delta_deep_dive"
data = [(1,"US",100.0),(2,"EU",200.0),(3,"US",150.0)]
df = spark.createDataFrame(data, ["id","region","amount"])
df.write.format("delta").mode("overwrite").save(path)

dt = DeltaTable.forPath(spark, path)

# MERGE (upsert)
updates = spark.createDataFrame([(2,"EU",250.0),(4,"APAC",300.0)], ["id","region","amount"])
dt.alias("t").merge(updates.alias("s"), "t.id = s.id")     .whenMatchedUpdateAll()     .whenNotMatchedInsertAll()     .execute()

# Time travel
spark.read.format("delta").option("versionAsOf", 0).load(path).show()

# History
dt.history().select("version","operation","timestamp").show()
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How does Delta Lake prevent two Spark jobs from corrupting a table simultaneously?" — Optimistic concurrency control. Both jobs write their Parquet files to S3, then race to add their commit JSON to `_delta_log/`. The commit that gets written second checks if the first commit changed any files it also changed. If there's a conflict (both modified the same files), the second commit fails and the job retries from the new table state. If no conflict (different partitions), both commits succeed.

> **Tip 2:** "What happens if you run VACUUM and then try to time-travel?" — VACUUM removes Parquet files that are no longer part of the current table state (tombstoned by `remove` entries). If you VACUUM older than N hours, any snapshot older than N hours may reference files that no longer exist → time-travel query fails with FileNotFoundException. This is why the default is `RETAIN 7 DAYS` — enough buffer for most time-travel needs. Never run `VACUUM RETAIN 0 HOURS` in production.

> **Tip 3:** "What's a Delta checkpoint file and why does it exist?" — Every 10 commits, Delta writes a checkpoint Parquet file that captures the full table state (all live files). When reading the table, Delta reads the latest checkpoint + commits since the checkpoint, rather than replaying all commits from the beginning. Without checkpoints, tables with thousands of commits would be slow to read (replay 10,000 JSON files). Checkpoint every 10 commits = at most 9 JSON files to replay.
