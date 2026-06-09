---
title: "Apache Hudi — Scenarios"
topic: data-lakehouse
subtopic: apache-hudi
content_type: scenario_question
tags: [hudi, scenarios, interview]
---

# Apache Hudi — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Copy-on-Write vs Merge-on-Read

**Scenario:** You're onboarding to a team that uses Apache Hudi for CDC (change data capture) pipelines. Your tech lead asks you to explain the difference between Copy-on-Write (CoW) and Merge-on-Read (MoR) table types. When would you use each?

<details>
<summary>💡 Hint</summary>

Think about read vs write performance trade-offs. CoW rewrites files on every write; MoR appends delta logs and merges on read. Consider workloads: heavy reads vs frequent updates.

</details>

<details>
<summary>✅ Solution</summary>

**Copy-on-Write (CoW):**
- On every write, Hudi rewrites the entire Parquet file containing updated records
- Reads are fast — always read clean Parquet files
- Writes are slower due to file rewriting
- Best for: read-heavy workloads, BI dashboards, infrequent updates

**Merge-on-Read (MoR):**
- Writes go to delta log files (Avro format) first
- Reads merge base Parquet files with delta logs on the fly
- Two read modes:
  - **Read Optimized (RO):** reads only base files (fast, may miss recent deltas)
  - **Realtime (RT):** merges base + log files (slower, always current)
- Best for: high-frequency updates, near-real-time CDC

```python
# Creating a CoW table with PySpark
hudi_options_cow = {
    'hoodie.table.name': 'orders',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field': 'order_id',
    'hoodie.datasource.write.precombine.field': 'updated_at',
    'hoodie.datasource.write.operation': 'upsert',
}

df.write.format('hudi')     .options(**hudi_options_cow)     .mode('append')     .save('s3://bucket/orders/')

# Creating a MoR table
hudi_options_mor = {
    **hudi_options_cow,
    'hoodie.table.name': 'orders_mor',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
}
```

**Decision Matrix:**

| Factor | CoW | MoR |
|--------|-----|-----|
| Write latency | Higher | Lower |
| Read latency | Lower | Higher (RT view) |
| Storage overhead | Lower | Higher (delta logs) |
| Use case | Analytics | CDC, streaming |

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implementing CDC Pipeline with Hudi

**Scenario:** You need to build a CDC pipeline that ingests change events from MySQL (via Debezium on Kafka) into a Hudi lakehouse table on S3. Records can be inserted, updated, or deleted. Design and implement this pipeline.

<details>
<summary>💡 Hint</summary>

Debezium emits events with `op` field: `c` (create), `u` (update), `d` (delete), `r` (read/snapshot). Hudi handles upserts natively; deletes require marking records with a soft-delete flag or using hard deletes. Consider the precombine field for ordering.

</details>

<details>
<summary>✅ Solution</summary>

**Pipeline Architecture:**
```
MySQL → Debezium → Kafka → Spark Structured Streaming → Hudi (S3)
```

**Spark Structured Streaming Job:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType, LongType, IntegerType

spark = SparkSession.builder     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")     .getOrCreate()

# Debezium schema (simplified)
debezium_schema = StructType()     .add("op", StringType())     .add("ts_ms", LongType())     .add("after", StructType()
        .add("id", IntegerType())
        .add("name", StringType())
        .add("amount", StringType())
        .add("updated_at", LongType())
    )     .add("before", StructType()
        .add("id", IntegerType())
    )

# Read from Kafka
raw_stream = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", "kafka:9092")     .option("subscribe", "mysql.mydb.orders")     .load()

# Parse Debezium events
parsed = raw_stream     .selectExpr("CAST(value AS STRING) as json_value")     .select(from_json("json_value", debezium_schema).alias("data"))     .select("data.*")

# Normalize: handle deletes with is_deleted flag
normalized = parsed.select(
    when(col("op") == "d", col("before.id")).otherwise(col("after.id")).alias("id"),
    col("after.name"),
    col("after.amount"),
    col("after.updated_at"),
    when(col("op") == "d", True).otherwise(False).alias("_hoodie_is_deleted"),
    col("ts_ms").alias("kafka_ts")
)

hudi_options = {
    'hoodie.table.name': 'orders',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.precombine.field': 'updated_at',
    'hoodie.datasource.write.payload.class':
        'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
    'hoodie.datasource.write.hive_style_partitioning': 'true',
    # Enable hard deletes
    'hoodie.datasource.write.drop.partition.columns': 'false',
    'hoodie.datasource.write.payload.ordering.field': 'updated_at',
}

def write_batch(batch_df, batch_id):
    batch_df.write.format("hudi")         .options(**hudi_options)         .mode("append")         .save("s3://datalake/orders/")

normalized.writeStream     .foreachBatch(write_batch)     .option("checkpointLocation", "s3://checkpoints/orders/")     .trigger(processingTime="60 seconds")     .start()     .awaitTermination()
```

**Reading the table:**
```python
# Always get latest (includes deletes filtered)
df = spark.read.format("hudi").load("s3://datalake/orders/")

# Read-optimized view (faster, may lag by one compaction)
df_ro = spark.read.format("hudi")     .option("hoodie.datasource.query.type", "read_optimized")     .load("s3://datalake/orders/")
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Hudi Clustering and Indexing Strategy for Large-Scale Upserts

**Scenario:** Your Hudi MoR table has 500 billion records across 10,000 partitions. Upsert performance has degraded significantly — each batch takes 4 hours. Profiling shows most time is spent on index lookups. Design a comprehensive optimization strategy.

<details>
<summary>💡 Hint</summary>

Hudi's default BLOOM index requires scanning all base files to find records. For large tables, consider GLOBAL_BLOOM, HBASE index, or RECORD_LEVEL index. Also look at clustering to co-locate frequently updated records, and async compaction/clustering to reduce write amplification.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause Analysis:**

The bloom filter index scans every partition file to locate records being upserted. With 500B records, this generates massive S3 GET requests.

**Strategy 1: Switch to a Faster Index**

```python
# Option A: Hudi Metadata Table with Record-Level Index (Hudi 0.13+)
optimized_options = {
    # Enable metadata table
    'hoodie.metadata.enable': 'true',
    'hoodie.metadata.index.bloom.filter.enable': 'true',
    'hoodie.metadata.index.column.stats.enable': 'true',
    # Use record-level index (most precise)
    'hoodie.index.type': 'RECORD_INDEX',
    'hoodie.metadata.record.index.enable': 'true',
}

# Option B: HBase Index (best for random upserts across all partitions)
hbase_options = {
    'hoodie.index.type': 'HBASE',
    'hoodie.index.hbase.zkquorum': 'hbase-zk:2181',
    'hoodie.index.hbase.zkport': '2181',
    'hoodie.index.hbase.tablename': 'hudi_orders_index',
    'hoodie.index.hbase.get.batch.size': '1000',
}
```

**Strategy 2: Partitioning Alignment**

If most upserts touch recent partitions, use date partitioning and set `hoodie.bloom.index.filter.dynamic.max.entries` to limit bloom filter scope:

```python
partition_options = {
    'hoodie.datasource.write.partitionpath.field': 'event_date',
    'hoodie.bloom.index.use.caching': 'true',
    # Only check partitions in the upsert's partition path
    'hoodie.bloom.index.prune.by.ranges': 'true',
    # Use non-global bloom (only search within same partition)
    'hoodie.index.type': 'BLOOM',
    'hoodie.bloom.index.filter.type': 'DYNAMIC_V0',
}
```

**Strategy 3: Async Compaction and Clustering**

```python
# Async compaction config (run compaction in separate job)
writer_options = {
    'hoodie.compact.inline': 'false',
    'hoodie.compact.async.enable': 'true',
    'hoodie.compact.schedule.inline': 'true',
}

# Separate compaction job
spark.sql("""
  CALL hudi_helpers.run_compaction(
    op => 'schedule',
    table => 's3://datalake/orders/'
  )
""")

# Clustering: sort by frequently queried columns to reduce scan
clustering_options = {
    'hoodie.clustering.inline': 'false',
    'hoodie.clustering.async.enabled': 'true',
    'hoodie.clustering.plan.strategy.class':
        'org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy',
    'hoodie.clustering.plan.strategy.sort.columns': 'customer_id,event_date',
    'hoodie.clustering.plan.strategy.target.file.max.bytes': '1073741824',  # 1GB
    'hoodie.clustering.plan.strategy.small.file.limit': '629145600',  # 600MB
}
```

**Strategy 4: Resource Tuning**

```python
spark_options = {
    'spark.executor.memory': '16g',
    'spark.executor.cores': '4',
    'spark.sql.shuffle.partitions': '2000',
    # Tune Hudi parallelism
    'hoodie.upsert.shuffle.parallelism': '500',
    'hoodie.insert.shuffle.parallelism': '500',
    'hoodie.bulkinsert.shuffle.parallelism': '500',
}
```

**Expected Outcome:**
- Record-level index: index lookup from O(files) → O(1) per record key
- Async compaction: writer is not blocked by compaction
- Result: upsert time from 4 hours → ~30 minutes

</details>

</article>

---

## Interview Tips

> **Tip 1:** "When would you choose Hudi over Iceberg?" — Hudi has stronger built-in CDC support (Debezium integration, precombine semantics) and a mature record-level index. Iceberg is better for multi-engine environments and open catalog standards.
> **Tip 2:** "How does Hudi handle late-arriving data?" — The `precombine.field` determines which version of a record wins. Records with a higher precombine value overwrite older ones, even if they arrive late.
> **Tip 3:** "What is a timeline in Hudi?" — Hudi's timeline is an ordered log of all actions (commits, compactions, clustering, rollbacks) on a table. It's the source of truth for MVCC and rollback capabilities.
