---
title: "Delta Lake Deep Dive — Intermediate"
topic: data-lakehouse
subtopic: delta-lake-deep-dive
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [delta-lake, optimize, zorder, cdf, schema-evolution, streaming]
---

# Delta Lake Deep Dive — Intermediate

## OPTIMIZE and Z-Ordering

```python
# OPTIMIZE: compact small files into 128MB target files
spark.sql("OPTIMIZE delta.`s3://bucket/silver/orders`")

# OPTIMIZE + ZORDER: compact AND sort by specified columns
# Z-order interleaves multiple sort dimensions for multi-column predicate pruning
spark.sql("""
  OPTIMIZE delta.`s3://bucket/silver/orders`
  ZORDER BY (customer_id, order_date)
""")
-- After ZORDER: a query filtering WHERE customer_id=123 AND order_date='2024-01-15'
-- can skip most files because customer_id and order_date values are co-located

-- When to run OPTIMIZE:
-- After heavy streaming writes (small file accumulation)
-- When query latency increases (usually means many small files)
-- Schedule: daily or after each major batch load

-- Auto optimize (Databricks Delta):
spark.sql("""
  ALTER TABLE orders
  SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',  -- auto-compact during write
    'delta.autoOptimize.autoCompact' = 'true'     -- background compaction
  )
""")

-- Check file size distribution BEFORE/AFTER
spark.sql("""
  SELECT 
    COUNT(*) AS file_count,
    SUM(size) / 1024 / 1024 / 1024 AS total_gb,
    AVG(size) / 1024 / 1024 AS avg_file_mb,
    MIN(size) / 1024 / 1024 AS min_file_mb,
    MAX(size) / 1024 / 1024 AS max_file_mb
  FROM (
    DESCRIBE DETAIL delta.`s3://bucket/silver/orders`
  )
""")
```

---

## Change Data Feed (CDF)

```python
# Delta CDF: capture changes (inserts, updates, deletes) as a stream
# Enables incremental downstream pipelines from Delta tables

# Enable CDF on a table
spark.sql("""
  ALTER TABLE silver.orders
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read changes since a version
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 42) \
    .table("silver.orders")

# Or by timestamp
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-15 12:00:00") \
    .table("silver.orders")

# CDF schema: original columns + metadata columns
# _change_type: "insert" | "update_preimage" | "update_postimage" | "delete"
# _commit_version: which commit made this change
# _commit_timestamp: when the commit happened

# Process only inserts and new values of updates
new_and_updated = changes.filter(
    col("_change_type").isin(["insert", "update_postimage"])
)

# CDF streaming: continuously process changes from Silver → Gold
cdf_stream = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "latest") \
    .table("silver.orders")

cdf_stream.writeStream \
    .foreachBatch(process_cdf_batch) \
    .option("checkpointLocation", "/checkpoints/cdf_gold") \
    .start()
```

---

## Schema Evolution

```python
# Default: Delta REJECTS writes with different schema
# (schema enforcement — prevents silent schema drift)

# Allow adding new columns
df_with_new_col.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("s3://bucket/orders")
# New column added to table schema; old records have null for new column

# Overwrite with schema change (replaces entire schema)
df_new_schema.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("s3://bucket/orders")
# WARNING: all existing data + schema replaced; use with caution

# Explicit ALTER TABLE (safer for production schema changes)
spark.sql("ALTER TABLE orders ADD COLUMNS (coupon_code STRING, discount_pct DOUBLE)")
spark.sql("ALTER TABLE orders CHANGE COLUMN amount amount DECIMAL(18,4)")  # widen type
spark.sql("ALTER TABLE orders DROP COLUMN legacy_flag")

# Schema enforcement in action:
from pyspark.sql.utils import AnalysisException
try:
    wrong_schema_df.write.format("delta").mode("append").save("s3://bucket/orders")
except AnalysisException as e:
    print(f"Schema mismatch blocked: {e}")
    # "A schema mismatch detected when writing to the Delta table"
```

---

## Delta Streaming (Exactly-Once Semantics)

```python
# Delta as streaming source (reads only new/changed data)
stream_source = spark.readStream.format("delta").load("s3://bucket/bronze/orders")
# Automatically tracks which files have been processed via checkpoint

# Delta as streaming sink
stream_source.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/silver_orders") \
    .option("path", "s3://bucket/silver/orders") \
    .trigger(processingTime="5 minutes") \
    .start()

# Exactly-once guarantee:
# Checkpoint records the latest committed Delta version
# On restart, stream picks up from checkpointed version
# Delta ACID ensures partial writes are not visible

# Trigger options:
# processingTime="5 minutes"  → micro-batch every 5 min
# once=True                   → process available data, stop (batch-like)
# availableNow=True           → process all available, stop (1.4.0+)
# continuous="1 second"       → near-real-time (experimental)

# foreachBatch with MERGE (common pattern for Silver):
def upsert_to_delta(batch_df, batch_id):
    dt = DeltaTable.forPath(spark, "s3://bucket/silver/orders")
    dt.alias("t").merge(
        batch_df.alias("s"),
        "t.order_id = s.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

stream.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoints/silver") \
    .start()
```

---

## Delta Table Properties and Configuration

```python
# Key table properties for production tables

spark.sql("""
  ALTER TABLE orders SET TBLPROPERTIES (
    -- Retention for time travel (VACUUM won't remove files newer than this)
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    
    -- Log retention (keep commit JSONs for this long)
    'delta.logRetentionDuration' = 'interval 30 days',
    
    -- Auto-compaction (Databricks only)
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    
    -- CDF for incremental downstream
    'delta.enableChangeDataFeed' = 'true',
    
    -- Serializable isolation (strongest)
    'delta.isolationLevel' = 'Serializable',
    
    -- Target file size for OPTIMIZE
    'delta.targetFileSize' = '134217728'  -- 128MB
  )
""")

# Check table details
spark.sql("DESCRIBE DETAIL orders").show(vertical=True)
# Returns: format, id, numFiles, sizeInBytes, partitionColumns, etc.
```

---

## Interview Tips

> **Tip 1:** "What's the difference between ZORDER and partitioning?" — Partitioning physically separates data into subdirectories (great for high-cardinality date filters; each partition is a folder). ZORDER sorts data within each file using a space-filling curve across multiple columns (good for multi-dimensional filtering where you can't partition by all columns). Partitioning prunes at the directory level; ZORDER prunes at the file level via column statistics. Use both together: partition by date, ZORDER by customer_id.

> **Tip 2:** "How does Delta CDF differ from Hudi's incremental query?" — Both expose changed rows to downstream consumers. CDF is Delta's mechanism: it captures change events with `_change_type` metadata (insert/update_preimage/update_postimage/delete) written to special CDF files. Hudi incremental uses the commit timeline to return files modified since a timestamp. CDF provides richer change semantics (pre/post images for updates). Hudi incremental is simpler for "give me everything that changed since X." For complex downstream CDC: CDF is more expressive.

> **Tip 3:** "Why would Delta reject a write even though mergeSchema is not set?" — Schema enforcement: Delta compares the incoming DataFrame schema against the table schema. If the incoming schema has a new column, different column type, or different column name — write is rejected. This is intentional: silent schema changes are a major source of data quality issues. Fix: `option("mergeSchema", "true")` for additive changes, or `ALTER TABLE ADD COLUMNS` explicitly. Never disable schema enforcement — it's one of Delta's key reliability features.
