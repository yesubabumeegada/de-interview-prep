---
title: "Apache Hudi — Intermediate"
topic: data-lakehouse
subtopic: apache-hudi
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hudi, spark, upsert, compaction, incremental-query]
---

# Apache Hudi — Intermediate

## Writing to Hudi Tables with Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# ── COW Upsert ────────────────────────────────────────────────────────────────
hudi_options_cow = {
    "hoodie.table.name": "orders_cow",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.precombinekey.field": "updated_at",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.upsert.shuffle.parallelism": "200",
    "hoodie.insert.shuffle.parallelism": "200",
    # Bloom index (default)
    "hoodie.index.type": "BLOOM",
    "hoodie.bloom.index.update.partition.path": "true",  # handle key moving partitions
}

df.write.format("hudi") \
    .options(**hudi_options_cow) \
    .mode("append") \
    .save("s3://bucket/hudi/orders_cow")

# ── MOR Upsert ────────────────────────────────────────────────────────────────
hudi_options_mor = {
    "hoodie.table.name": "orders_mor",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.precombinekey.field": "updated_at",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    # Inline compaction: compact during write when threshold reached
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "5",
}

df.write.format("hudi") \
    .options(**hudi_options_mor) \
    .mode("append") \
    .save("s3://bucket/hudi/orders_mor")
```

---

## Incremental Queries (Hudi's Killer Feature)

```python
# Incremental query: only records changed AFTER a specific commit time
# This is Hudi's unique advantage over Iceberg and Delta for CDC pipelines

# Get the latest processed commit time (stored externally, e.g., in a state table)
last_commit_time = "20240115120000"

incremental_df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", last_commit_time) \
    .load("s3://bucket/hudi/orders_mor")

# This returns ONLY records where the commit time > last_commit_time
# No full table scan — reads only the changed files
# Perfect for: downstream Silver → Gold refresh, notification pipelines

# Update the watermark after successful processing
new_commit_time = incremental_df.select(max("_hoodie_commit_time")).collect()[0][0]
save_watermark("orders_watermark", new_commit_time)

# Incremental with end time (for bounded window)
bounded_incremental = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240115120000") \
    .option("hoodie.datasource.read.end.instanttime", "20240115130000") \
    .load("s3://bucket/hudi/orders_mor")
```

---

## Hudi Metadata Fields

```
Every Hudi record has system metadata columns:
  _hoodie_commit_time:     timestamp of the commit that wrote this record
  _hoodie_commit_seqno:    unique sequence number within the commit
  _hoodie_record_key:      value of the record key field
  _hoodie_partition_path:  partition path (e.g., "order_date=2024-01-15")
  _hoodie_file_name:       Parquet file name containing this record

Usage:
  Track when a record was last modified:
    SELECT order_id, _hoodie_commit_time FROM hudi.orders ORDER BY _hoodie_commit_time DESC LIMIT 10

  Find all records written in the last run:
    SELECT * FROM hudi.orders WHERE _hoodie_commit_time > '20240115120000'
```

---

## Compaction (MOR Tables)

```python
# Inline compaction: triggered automatically during writes
hudi_options = {
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "5",  # compact after 5 delta commits
}

# Async compaction: run as a separate Spark job (avoids blocking writes)
from pyspark.sql import SparkSession

def run_async_compaction(spark, table_path: str):
    spark.sql(f"""
        CALL hudi.system.run_compaction(
            table => '{table_path}',
            op => 'schedule'  -- schedule first, then execute
        )
    """)
    
    spark.sql(f"""
        CALL hudi.system.run_compaction(
            table => '{table_path}',
            op => 'run'
        )
    """)

# Via HoodieSparkJob (original API)
from hoodie.spark.compact import HoodieCompact

HoodieCompact.run(
    spark=spark,
    base_path="s3://bucket/hudi/orders_mor",
    strategy="org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy",
)
```

---

## Hudi Cleaning and Archival

```python
# Clean old file versions (keeps storage bounded)
# By default, Hudi keeps 3 commits of history

# Configure retention
hudi_options = {
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": "10",  # keep 10 commits of history
    # Or time-based:
    "hoodie.cleaner.policy": "KEEP_LATEST_BY_HOURS",
    "hoodie.cleaner.hours.retained": "168",  # 7 days
}

# Manual clean
spark.sql("""
    CALL hudi.system.run_clean(table => 'hudi.orders_mor')
""")

# Archive old timeline entries (keeps .hoodie/ directory size bounded)
hudi_options_archive = {
    "hoodie.archive.automatic": "true",
    "hoodie.keep.max.commits": "30",  # archive commits older than this
    "hoodie.keep.min.commits": "20",
}
```

---

## Interview Tips

> **Tip 1:** "How does Hudi's incremental query differ from Iceberg's changelog?" — Hudi incremental queries are a first-class feature, available since day one. You specify a start commit time, and Hudi returns only changed files — efficient and built into the format. Iceberg added changelog views later (via incremental scan API). Delta Lake has Change Data Feed (CDF). All three work, but Hudi's implementation is the most mature and was the original motivation for building the format.

> **Tip 2:** "When would you choose MOR over COW for a Hudi table?" — Choose MOR when: (1) write latency matters (MOR is near-instant, COW can take seconds for large files), (2) you have high update frequency (CDC from OLTP updating millions of rows), (3) you query tables incrementally (incremental query reads log files efficiently). Choose COW when: (1) you have more reads than writes, (2) query latency is critical (no merge overhead), (3) you need compatibility with non-Hudi readers (COW files are plain Parquet).

> **Tip 3:** "What happens if compaction falls behind in a MOR table?" — Log files accumulate. Read queries must merge more and more log files at read time → query latency degrades. Symptom: queries that took 5 seconds now take 60 seconds. Fix: run compaction immediately (catch up), then reduce `hoodie.compact.inline.max.delta.commits` to compact more frequently. Monitor: check `_hoodie_commit_time` distribution in log files — if you see many distinct commit times, compaction is behind.
