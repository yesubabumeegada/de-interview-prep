---
title: "Delta Lake Integration — Senior Deep Dive"
topic: pyspark
subtopic: delta-lake-integration
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [delta-lake, z-ordering, OPTIMIZE, VACUUM, CDF, transaction-log, compaction]
---

# Delta Lake Integration — Senior Deep Dive

At the senior level you're responsible for table health, query performance, and pipeline architecture. This covers Z-ordering, compaction, VACUUM, Change Data Feed internals, and the transaction log structure that underpins everything.

---

## Z-Ordering: Data Skipping at Scale

Z-ordering is a data layout optimization that co-locates related data in the same files. When you Z-order on column X, Delta stores file-level min/max statistics for X, and when a query filters on X, Delta can skip entire files where X is out of range.

### How Z-Ordering Works

```
Without Z-ordering (row order = insertion order):
File 1: user_id 1, 5, 3, 9, 2  (all mixed up)
File 2: user_id 7, 1, 8, 4, 6

Filter: user_id = 5
→ Must read ALL files (5 could be in any of them)

With Z-ordering on user_id:
File 1: user_id 1, 2, 3, 4, 5  (min=1, max=5)
File 2: user_id 6, 7, 8, 9, 10 (min=6, max=10)

Filter: user_id = 5
→ Only read File 1 (max=5 ≥ 5, and File 2 min=6 > 5 → skip File 2)
→ 50% data skipping
```

### OPTIMIZE + ZORDER

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("delta-senior").getOrCreate()

# Run OPTIMIZE with Z-ordering
spark.sql("""
    OPTIMIZE warehouse.events
    ZORDER BY (user_id, event_type)
""")

# On a partitioned table — run per-partition to be targeted
spark.sql("""
    OPTIMIZE warehouse.events
    WHERE event_date = '2024-01-20'
    ZORDER BY (user_id, event_type)
""")

# What OPTIMIZE does:
# 1. Compacts small files into target size (default 1 GB)
# 2. Re-sorts data according to Z-order curve (multi-dimensional locality)
# 3. Writes new compacted files
# 4. Records old files as "removed" in transaction log (for VACUUM to clean)
```

### Z-Order Column Selection

```python
# Good Z-order candidates:
# - High cardinality columns used in WHERE clauses frequently
# - Columns used in JOIN conditions
# - Columns with range predicates (dates, IDs, numeric ranges)

# Bad Z-order candidates:
# - Partition columns (already handled by partition pruning)
# - Low cardinality columns like boolean flags (no skipping benefit)
# - Columns never filtered on

# Multi-column Z-ordering uses a space-filling curve
# First column gets the most benefit; diminishing returns after 3-4 columns
# Rule: Z-order on the columns in your most frequent query filter

# Check data skipping effectiveness:
spark.sql("""
    DESCRIBE DETAIL warehouse.events
""").show(truncate=False)
# Shows: numFiles, sizeInBytes, partitionColumns, clusteringColumns
```

---

## OPTIMIZE and File Compaction

Delta tables accumulate small files from streaming writes, frequent small appends, and merge operations. OPTIMIZE compacts them.

```python
# Basic OPTIMIZE — compacts to ~1 GB files (default target)
spark.sql("OPTIMIZE warehouse.events")

# Tune target file size
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(128 * 1024 * 1024))
spark.sql("OPTIMIZE warehouse.events")  # Now targets 128 MB files

# Auto-optimize (Databricks): Spark runs compaction automatically after writes
# Good for streaming tables; not available in open-source Delta
spark.sql("""
    ALTER TABLE warehouse.events
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# How to schedule OPTIMIZE in production (run as a maintenance job):
# Daily or weekly — depends on write frequency
# A good pattern: OPTIMIZE after every nightly batch load
def run_daily_optimize(table_name: str, partition_date: str, zorder_cols: list):
    zorder_clause = f"ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
    spark.sql(f"""
        OPTIMIZE {table_name}
        WHERE event_date = '{partition_date}'
        {zorder_clause}
    """)

run_daily_optimize("warehouse.events", "2024-01-20", ["user_id", "event_type"])
```

---

## VACUUM: Removing Old Files

VACUUM removes data files that are no longer referenced by the transaction log AND are older than the retention period.

```python
# Default retention: 7 days
spark.sql("VACUUM warehouse.events")

# Custom retention (minimum: 7 days for safety)
spark.sql("VACUUM warehouse.events RETAIN 168 HOURS")  # 7 days

# DRY RUN first — shows what would be deleted without deleting
spark.sql("VACUUM warehouse.events DRY RUN").show(100, truncate=False)

# DANGER: do NOT run with retention < 7 days
# If a reader is reading version N while you VACUUM files referenced by N,
# the read will fail mid-query
# The 7-day default exists to protect long-running queries

# Disable safety check (NOT recommended, but sometimes done in dev):
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM warehouse.events RETAIN 0 HOURS")  # ← never do this in production

# After VACUUM, time travel to pre-vacuum versions is no longer possible
# Trade-off: storage cost vs time travel window
```

### VACUUM and Storage Cost

```python
# Check how much space VACUUM would free:
from pyspark.sql.functions import col, sum as spark_sum

table_detail = spark.sql("DESCRIBE DETAIL warehouse.events")
current_size = table_detail.select("sizeInBytes").collect()[0][0]

# Files that would be vacuumed (older than retention + no longer referenced):
files_to_vacuum = spark.sql("VACUUM warehouse.events DRY RUN")
vacuum_size = files_to_vacuum.count()  # Number of files, not bytes directly

print(f"Current table size: {current_size / 1e9:.1f} GB")
print(f"Files to vacuum: {vacuum_size}")
```

---

## Change Data Feed (CDF)

CDF records row-level changes (insert, update_preimage, update_postimage, delete) in a special hidden table, enabling efficient incremental processing.

### Enabling CDF

```python
# Enable at table creation
spark.sql("""
    CREATE TABLE warehouse.customers (
        customer_id BIGINT,
        name STRING,
        email STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Enable on existing table
spark.sql("""
    ALTER TABLE warehouse.customers
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
```

### Reading the Change Feed

```python
from pyspark.sql.functions import col

# Read changes between two versions
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 5) \
    .option("endingVersion", 10) \
    .table("warehouse.customers")

# Or by timestamp
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-20 00:00:00") \
    .option("endingTimestamp", "2024-01-20 23:59:59") \
    .table("warehouse.customers")

# CDF adds metadata columns:
changes.printSchema()
# root
#  |-- customer_id: long
#  |-- name: string
#  |-- email: string
#  |-- updated_at: timestamp
#  |-- _change_type: string           ← "insert", "update_preimage", "update_postimage", "delete"
#  |-- _commit_version: long          ← which transaction this change belongs to
#  |-- _commit_timestamp: timestamp   ← when the transaction happened

# Filter to only the latest state of each changed row:
latest_changes = changes \
    .filter(col("_change_type").isin(["insert", "update_postimage"])) \
    .select("customer_id", "name", "email", "updated_at")
```

### Streaming Incremental Processing with CDF

```python
# Use CDF for Bronze → Silver incremental propagation
# Instead of re-processing the full Bronze table, only process changes

def process_silver_incremental(bronze_table: str, silver_table: str, checkpoint_path: str):
    """Process only changed records from bronze → silver using CDF."""
    # Read CDF as a streaming source
    bronze_changes = spark.readStream \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", "latest") \
        .table(bronze_table)

    # Apply silver transformations only on changed rows
    silver_ready = bronze_changes \
        .filter(col("_change_type").isin(["insert", "update_postimage"])) \
        .transform(apply_silver_transformations) \
        .drop("_change_type", "_commit_version", "_commit_timestamp")

    # Write to silver with MERGE to avoid duplicates
    def upsert_to_silver(batch_df, batch_id):
        silver_target = DeltaTable.forName(spark, silver_table)
        silver_target.alias("t").merge(
            batch_df.alias("s"),
            "t.customer_id = s.customer_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    silver_ready.writeStream \
        .foreachBatch(upsert_to_silver) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(availableNow=True) \
        .start() \
        .awaitTermination()
```

---

## Delta Transaction Log Internals

Every Delta table maintains a `_delta_log/` directory. Understanding its structure helps you debug issues and understand performance.

```python
# Structure:
# _delta_log/
# ├── 00000000000000000000.json   ← first commit
# ├── 00000000000000000001.json
# ├── ...
# ├── 00000000000000000009.json
# ├── 00000000000000000010.checkpoint.parquet  ← checkpoint every 10 commits
# ├── 00000000000000000010.json
# └── _last_checkpoint                         ← pointer to latest checkpoint

# Each JSON log file contains one or more of these "actions":
# - commitInfo: metadata about who committed, when, what operation
# - metaData: table schema, partition columns, table properties
# - protocol: min reader/writer version required
# - add: a file was added (includes stats: min/max per column, row count)
# - remove: a file was logically removed (not physically deleted)

# Inspect a log file:
log_df = spark.read.json(
    "s3://lakehouse/tables/customers/_delta_log/00000000000000000005.json"
)
log_df.select("add", "remove", "commitInfo").show(truncate=False)

# Example add action content:
# {
#   "path": "part-00001-abc123.snappy.parquet",
#   "size": 104857600,
#   "stats": "{\"numRecords\": 50000, \"minValues\": {\"customer_id\": 1, ...},
#              \"maxValues\": {\"customer_id\": 99999, ...}}"
# }

# Checkpoints: every 10 commits, Delta writes a Parquet checkpoint
# The checkpoint is the complete table state at that version
# Readers only need to read: last_checkpoint + subsequent JSON files
# Without checkpoints, reading a 10,000-version table would require
# reading 10,000 JSON files!

# How long does it take to read the transaction log?
# = (number of commits since last checkpoint) × (log file read time)
# With checkpoint every 10: max 10 JSON reads
# Default checkpoint interval: configurable
spark.sql("""
    ALTER TABLE warehouse.customers
    SET TBLPROPERTIES ('delta.checkpointInterval' = '20')
""")
```

---

## Table Properties for Production

```python
spark.sql("""
    ALTER TABLE warehouse.events SET TBLPROPERTIES (
        -- Data retention for time travel (default: 7 days)
        'delta.deletedFileRetentionDuration' = 'interval 14 days',

        -- Checkpoint more frequently for high-write tables
        'delta.checkpointInterval' = '10',

        -- Enable CDF for downstream incremental pipelines
        'delta.enableChangeDataFeed' = 'true',

        -- Target file size for OPTIMIZE (128 MB = smaller files, faster reads for point queries)
        'delta.targetFileSize' = '134217728',

        -- Data skipping statistics for top N columns
        'delta.dataSkippingNumIndexedCols' = '10'
    )
""")

# Monitor table health
spark.sql("DESCRIBE DETAIL warehouse.events").show(truncate=False)
# Key fields: numFiles, sizeInBytes, numPartitions, location

# Check for small file problem
spark.sql("""
    SELECT
        count(*) as num_files,
        sum(size) / 1e9 as total_gb,
        avg(size) / 1e6 as avg_file_mb,
        min(size) / 1e6 as min_file_mb,
        max(size) / 1e6 as max_file_mb
    FROM (
        SELECT explode(files) as file
        FROM (SELECT input_file_name() as files)
    )
""")
# avg_file_mb < 64 MB → you need OPTIMIZE
# Many files with avg < 10 MB → serious small file problem
```

---

## Key Takeaways for Senior DEs

1. **Z-ordering + data skipping** is Delta's answer to secondary indexes — works best on high-cardinality filter columns.
2. **OPTIMIZE is maintenance**, not magic — schedule it after every batch load, per-partition on partitioned tables.
3. **VACUUM cleans orphaned files** — never run with < 7-day retention in production (protects concurrent readers).
4. **CDF enables efficient incremental processing** — the right tool for Bronze→Silver→Gold propagation; avoids full-table re-scans.
5. **The transaction log is the table** — understanding JSON add/remove actions and checkpoint behavior explains why Delta reads are fast and how time travel works.
6. **`deletedFileRetentionDuration`** controls how long you can time travel — set based on your audit and recovery SLA, not the default.
