---
title: "Table Format Comparison — Intermediate"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [delta-lake, iceberg, hudi, comparison, trade-offs]
---

# Table Format Comparison — Intermediate

## Concurrency Model Comparison

```
Delta Lake:
  Optimistic Concurrency Control (OCC)
  Writers commit a JSON entry to _delta_log/
  If two writers conflict (modified same files): one gets TransactionAbortedException
  Second writer retries from new table version
  Isolation levels:
    WriteSerializable: writes serializable, reads may be stale (default)
    Serializable: both reads and writes serializable (correctness > throughput)

Apache Iceberg:
  Optimistic Concurrency Control
  Writers update the metadata pointer (current-snapshot-id in metadata.json)
  Conflict detection: per-operation conflict resolution
  More granular: APPEND never conflicts, MERGE conflicts only on overlapping data
  Catalog-dependent: Glue uses DynamoDB for atomic pointer swap

Apache Hudi:
  Multi-writer: OCC or lock-based (ZooKeeper/DynamoDB lock provider)
  Timeline-based: each writer acquires a timestamp slot
  Conflict detection: check timeline for overlapping commits
  Multi-writer support more explicit in Hudi vs Delta/Iceberg

Production implication:
  Single writer (most common): all three work well
  Multiple writers to different partitions: all three handle without conflicts
  Multiple writers to same partitions: Delta and Iceberg use OCC (retry needed);
    Hudi can use distributed locks for guaranteed ordering
```

---

## Query Performance Comparison

```
Data Skipping (file pruning for analytical queries):
  Delta Lake:
    Column stats (min/max) stored in transaction log JSON
    Data skipping: WHERE order_id BETWEEN 100 AND 200 → skip files
    Bloom filter index: for equality lookups (WHERE customer_id = X)
    Z-order: multi-column co-location for composite predicates
  
  Apache Iceberg:
    Column stats in manifest files (min/max, null count, distinct count)
    Hidden partitioning: auto-derives partition from query predicate
    Partition evolution: old data with old partition, new data with new — both pruned
    Sort-based rewrite: similar to Z-order
  
  Apache Hudi:
    Column stats in metadata table (opt-in, 0.12+)
    Bloom index: per-file bloom filter for record key lookups
    Clustering (Z-order equivalent): SpaceFillingCurveSortPartitioner
    Record index (0.14+): O(1) key lookup without S3 LIST

Metadata read performance (for large tables with many files):
  Delta:    linear commit log replay + checkpoint (good for < 100K files)
  Iceberg:  manifest hierarchy (good for millions of files, better scalability)
  Hudi:     metadata table (if enabled, fast; if not, S3 LIST which is slow)
```

---

## Streaming Support Comparison

```python
# Delta Lake streaming (most mature for Spark)
spark.readStream.format("delta").load(path)   # stream from Delta source
df.writeStream.format("delta").save(path)     # write to Delta sink

# Key: Delta handles micro-batch streaming natively
# Checkpoint tracks last committed Delta version (not Kafka offsets)
# Trigger options: processingTime, once, availableNow, continuous

# Apache Iceberg streaming (via Flink — best engine for Iceberg streaming)
t_env.execute_sql("""
  CREATE TABLE orders (order_id BIGINT, amount DOUBLE, ts TIMESTAMP(3))
  WITH ('connector' = 'iceberg', 'catalog-type' = 'hive', ...)
""")
# Flink commits an Iceberg snapshot per checkpoint
# Latency: determined by Flink checkpoint interval

# Iceberg + Spark streaming (works but less native than Flink)
spark.readStream.format("iceberg").load("catalog.db.orders")

# Apache Hudi streaming
# Spark Structured Streaming writes to Hudi via DeltaStreamer or Spark write
hoodie_options["hoodie.datasource.write.operation"] = "upsert"
df.writeStream.format("hudi").options(**hoodie_options).save(path)

# DeltaStreamer (Hudi's own streaming tool):
# Reads from Kafka/S3, writes to Hudi, handles schema evolution
# HoodieDeltaStreamer --target-base-path ... --source-class KafkaSource

Streaming capability ranking (as of 2024):
  Delta + Spark: best for Spark micro-batch (most mature, best tooling)
  Iceberg + Flink: best for true event-at-a-time streaming (<100ms latency)
  Hudi: solid for Spark streaming; DeltaStreamer adds Kafka-native source
```

---

## Schema Evolution Depth Comparison

```
Adding a column:
  Delta:   option("mergeSchema","true") or ALTER TABLE ADD COLUMNS
  Iceberg: ALTER TABLE ADD COLUMN (schema ID-based, safe)
  Hudi:    automatic with mergeSchema=true, tracked in timeline

Renaming a column:
  Delta:   ALTER TABLE RENAME COLUMN col1 TO col2 (supported)
  Iceberg: ALTER TABLE RENAME COLUMN (safe — ID-based tracking)
  Hudi:    NOT supported (name-based schema, renaming breaks old readers)

Dropping a column:
  Delta:   ALTER TABLE DROP COLUMN (data stays, column removed from schema)
  Iceberg: ALTER TABLE DROP COLUMN (safe — ID-based, no data rewrite)
  Hudi:    Supported but requires schema version management

Changing column type:
  Delta:   Widening only (int→long, float→double via ALTER TABLE)
  Iceberg: Widening only (int→long, float→double, decimal precision increase)
  Hudi:    With Avro schema registry (Confluent) for type evolution

Schema enforcement on write:
  Delta:   Strict enforcement (write rejected if schema mismatch)
  Iceberg: Strict enforcement
  Hudi:    Flexible (with mergeSchema=true, new columns added automatically)

Winner for schema evolution safety: Iceberg (ID-based means renames never break readers)
Winner for enforcement strictness: Delta and Iceberg (tied)
Winner for flexibility: Hudi (more permissive by default)
```

---

## Interview Tips

> **Tip 1:** "Your team writes data from Spark but analysts use Trino for SQL. Which table format?" — Apache Iceberg. Iceberg has native Trino support (Iceberg connector), and Spark writes Iceberg natively. Delta works with Trino via UniForm but requires extra metadata generation configuration. Iceberg was designed as a multi-engine standard; Trino + Iceberg is a first-class, production-proven combination (Airbnb, Netflix, Apple all run this stack).

> **Tip 2:** "A data engineer says 'we need CDC support, so we should use Hudi.' Do you agree?" — Partially. All three formats support CDC workflows. Hudi is best when: your CDC write frequency is very high (millions of rows/hour), you need native incremental query for efficient downstream consumption, or you're deeply invested in Hudi already. Delta and Iceberg V2 both support CDC via MERGE (Delta) or equality deletes (Iceberg V2). For moderate CDC workloads, Delta or Iceberg are simpler to operate. Reserve Hudi for extreme-scale or incremental-query-critical use cases.

> **Tip 3:** "How would you migrate from Delta to Iceberg?" — For COW tables (plain Parquet files): register the existing Parquet files as an Iceberg table (Iceberg snapshot/migration procedure). Delta adds metadata to plain Parquet files, so the underlying files are still readable. For tables with Delta-specific features (deletion vectors, ZORDER): first run OPTIMIZE to materialize everything into clean Parquet, then migrate. New writes go to Iceberg catalog. Validate row counts and queries before cutting over. Migration is easiest when done table-by-table over weeks, not a big-bang cutover.
