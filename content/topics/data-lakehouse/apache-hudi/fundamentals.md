---
title: "Apache Hudi — Fundamentals"
topic: data-lakehouse
subtopic: apache-hudi
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [hudi, table-format, cow, mor, upsert]
---

# Apache Hudi — Fundamentals


## 🎯 Analogy

Think of Apache Hudi like a Delta Lake alternative with built-in CDC: CoW (Copy-on-Write) rewrites files on update (fast reads, slower writes), MoR (Merge-on-Read) appends delta logs (fast writes, read merges at query time).

---
## What Is Apache Hudi?

Apache Hudi (Hadoop Upserts Deletes and Incrementals) is an open-source table format originally created by Uber for incremental data processing on the Hadoop/S3 data lake. It specializes in low-latency upserts and incremental reads.

```
Hudi was built to solve:
  1. Efficient upserts on S3/HDFS (Uber needed to update trip records)
  2. Incremental data consumption (downstream jobs only read "what changed")
  3. GDPR-compliant data deletion on immutable storage

Created by: Uber Engineering (2016), donated to Apache (2019)
Used by: Uber, Amazon, ByteDance, Robinhood, Walmart
```

---

## Hudi Table Types

```
Copy-on-Write (COW):
  Write: UPDATE rewrites entire Parquet files containing changed rows
  Read: reads plain Parquet files (no merge needed)
  Write performance: slow (full file rewrite per update)
  Read performance: fast (no read-time merge overhead)
  Use case: read-heavy, BI dashboards, infrequent updates
  File layout: base.parquet files only

Merge-on-Read (MOR):
  Write: INSERT/UPDATE → appends to Avro log files (fast)
  Compaction: periodically merges base + log files → new base Parquet
  Read: 
    Snapshot query: reads base + log files merged (latest view)
    Read Optimized query: reads only base Parquet (may be stale)
    Incremental query: reads only changed files since a timestamp/commit
  Write performance: fast (append to log file)
  Read performance: slower (must merge base + delta logs)
  Use case: write-heavy, CDC ingestion, real-time pipelines
```

---

## Hudi Timeline (Commit Log)

```
Hudi maintains a timeline: .hoodie/ directory on S3/HDFS

Timeline structure:
  s3://bucket/hudi_table/
  ├── .hoodie/
  │   ├── hoodie.properties             ← table config
  │   ├── 20240115120000.commit         ← completed commit
  │   ├── 20240115130000.commit         ← completed commit
  │   ├── 20240115140000.inflight       ← in-progress write
  │   └── archived/                    ← old timeline entries
  └── data/
      └── year=2024/month=01/
          ├── file1_base.parquet
          └── file1_20240115.log        ← delta log (MOR only)

Timeline actions:
  commit: insert/update/delete committed
  deltacommit: MOR log file write
  compaction: base + log merge for MOR
  clean: removes old file versions
  rollback: reverses a failed commit
```

---

## Key Hudi Concepts

```
Record Key: unique identifier per record (like primary key)
  Primary key for upsert/delete operations
  Configured per table: hoodie.datasource.write.recordkey.field

Partition Path: how data is partitioned on storage
  hoodie.datasource.write.partitionpath.field = "order_date"

Precombine Field: for deduplication when same key appears multiple times
  hoodie.datasource.write.precombinekey.field = "updated_at"
  Hudi keeps record with highest precombine value

Index Types:
  BLOOM: bloom filter per file (default; efficient for random lookups)
  HBASE: HBase index (fastest for large tables; requires HBase)
  SIMPLE: file-level lookup (simplest; slow for large tables)
  GLOBAL_BLOOM: cross-partition upsert (needed if key can move partitions)
```

---

## Hudi vs Iceberg vs Delta (Quick Reference)

| Feature | Hudi | Iceberg | Delta Lake |
|---|---|---|---|
| Write model | COW or MOR | COW or MOR (V2) | COW + deletion vectors |
| Incremental reads | Native (first-class) | Via changelog views | Via CDF (Change Data Feed) |
| Created by | Uber | Netflix | Databricks |
| Best engine | Spark | Spark/Trino/Flink | Spark (Databricks) |
| Partition evolution | Limited | Full | Limited |
| Schema evolution | Yes | Yes (ID-based) | Yes |
| Time travel | Yes | Yes | Yes |
| Streaming | Yes | Yes | Yes |

---


## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")     .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0")     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")     .appName("hudi").getOrCreate()

data = [(1, "Alice", 300.0, "2024-01-15"), (2, "Bob", 150.0, "2024-01-15")]
df = spark.createDataFrame(data, ["order_id", "name", "amount", "date"])

hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "date",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.enable": "false",
}

df.write.format("hudi").options(**hudi_options).mode("append").save("/tmp/hudi_orders")
spark.read.format("hudi").load("/tmp/hudi_orders").show()
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "Why did Uber create Hudi instead of using plain Parquet?" — Uber needed to update trip records after the fact (driver ratings, surge adjustments). Plain Parquet files are immutable — updating a row requires rewriting the entire file. At Uber's scale (100B+ events/day), this was prohibitively expensive. Hudi's MOR mode appends changes to log files, enabling efficient upserts without full file rewrites.

> **Tip 2:** "What is an incremental query in Hudi and why is it useful?" — An incremental query returns only records that were written after a specific commit time. `beginTime=20240115120000` returns only what changed since noon. This enables downstream pipelines to process only the delta (new/changed rows) rather than scanning the full table. This is Hudi's killer feature — it enables efficient "what changed since last run" patterns that other table formats require additional machinery for.

> **Tip 3:** "What's the difference between COW and MOR in Hudi?" — COW rewrites Parquet files on update (expensive writes, fast reads — good for analytics). MOR appends to log files on update (fast writes, slower reads — good for high-frequency CDC). MOR requires periodic compaction to merge logs into base files. Production: use MOR for CDC ingestion tables, COW for stable analytics tables. Many teams use MOR for Silver (frequent updates from CDC) and COW for Gold (infrequent writes, read-heavy).
