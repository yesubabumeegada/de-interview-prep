---
title: "PySpark Partitioning and Bucketing - Real World Patterns"
topic: pyspark
subtopic: partitioning-and-bucketing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, partitioning, bucketing, parquet, file-optimization, compaction, dynamic-partition, production]
---

# PySpark Partitioning and Bucketing — Real-World Patterns

## Pattern 1: Write Optimally-Sized Parquet Files

**Problem:** An hourly ETL job writes Parquet files that range from 1KB to 2GB per partition, causing performance issues for downstream consumers.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("OptimalFileSize").getOrCreate()

class OptimizedWriter:
    """Write Parquet files with consistent, optimal sizes."""
    
    def __init__(self, target_file_mb=256, min_file_mb=64, max_file_mb=512):
        self.target_file_mb = target_file_mb
        self.min_file_mb = min_file_mb
        self.max_file_mb = max_file_mb
    
    def estimate_row_size_bytes(self, df, sample_size=10000):
        """Estimate bytes per row from a sample."""
        sample = df.limit(sample_size).toPandas()
        return sample.memory_usage(deep=True).sum() / len(sample)
    
    def calculate_files_per_partition(self, df, partition_col):
        """Determine optimal file count per storage partition."""
        row_size = self.estimate_row_size_bytes(df)
        
        partition_counts = (df
            .groupBy(partition_col)
            .count()
            .collect())
        
        results = {}
        for row in partition_counts:
            partition_val = row[partition_col]
            row_count = row["count"]
            partition_size_mb = (row_count * row_size) / 1024 / 1024
            
            files = max(1, round(partition_size_mb / self.target_file_mb))
            results[partition_val] = {
                "rows": row_count,
                "size_mb": partition_size_mb,
                "files": files,
            }
        
        return results
    
    def write(self, df, path, partition_cols, mode="overwrite"):
        """Write with optimal file sizes."""
        row_size = self.estimate_row_size_bytes(df)
        total_rows = df.count()
        total_size_mb = (total_rows * row_size) / 1024 / 1024
        
        # Target file count
        total_files = max(1, int(total_size_mb / self.target_file_mb))
        
        # Repartition to control file count
        writer_df = df.repartition(total_files, *partition_cols)
        
        # Sort within partitions for better compression
        writer_df = writer_df.sortWithinPartitions(*partition_cols, "timestamp")
        
        (writer_df.write
            .mode(mode)
            .partitionBy(*partition_cols)
            .option("maxRecordsPerFile", int(self.target_file_mb * 1024 * 1024 / row_size))
            .parquet(path))
        
        print(f"Wrote {total_size_mb:.0f}MB in ~{total_files} files "
              f"(target: {self.target_file_mb}MB each)")

# Usage
writer = OptimizedWriter(target_file_mb=256)
writer.write(
    df=processed_events,
    path="s3://data-lake/events/",
    partition_cols=["event_date"],
)
```

---

## Pattern 2: Pre-Partition for Join Elimination

**Problem:** A 2TB fact table joins with a 500GB dimension every hour. The shuffle costs 45 minutes per run. Eliminate the shuffle permanently.

```python
# Strategy: Bucket both tables on join key for zero-shuffle joins

# Step 1: Analyze join patterns
# Which columns are joined most frequently?
# Answer: customer_id (80% of joins), product_id (50%), region (30%)

# Step 2: Create bucketed tables (one-time setup)
BUCKET_COUNT = 512  # Power of 2 for efficiency

# Bucket fact table
(fact_df
    .write
    .format("parquet")
    .bucketBy(BUCKET_COUNT, "customer_id")
    .sortBy("customer_id", "event_timestamp")
    .mode("overwrite")
    .saveAsTable("fact_events_bucketed"))

# Bucket dimension table (same bucket count!)
(dim_customer_df
    .write
    .format("parquet")
    .bucketBy(BUCKET_COUNT, "customer_id")
    .sortBy("customer_id")
    .mode("overwrite")
    .saveAsTable("dim_customer_bucketed"))

# Step 3: Verify zero-shuffle join
spark.conf.set("spark.sql.autoBucketedScan.enabled", "true")

fact = spark.table("fact_events_bucketed")
dim = spark.table("dim_customer_bucketed")

result = fact.join(dim, "customer_id")
result.explain()
# SortMergeJoin — WITHOUT Exchange nodes!

# Step 4: Maintain bucketing in daily incremental loads
def daily_incremental_load(new_data_df, table_name):
    """Append new data maintaining bucket structure."""
    (new_data_df
        .write
        .format("parquet")
        .bucketBy(BUCKET_COUNT, "customer_id")
        .sortBy("customer_id", "event_timestamp")
        .mode("append")
        .insertInto(table_name))
```

### Performance Impact

| Metric | Before (Shuffle Join) | After (Bucketed) |
|--------|----------------------|-------------------|
| Shuffle data | 2.5TB per join | 0 |
| Join duration | 45 min | 8 min |
| Network I/O | 2.5TB | ~0 |
| CPU (sort/merge) | High | Low (pre-sorted) |
| Daily cost (24 runs) | 18 hours | 3.2 hours |

---

## Pattern 3: Dynamic Partition Overwrite

**Problem:** An incremental ETL job processes data for the current day, but late-arriving data means yesterday's partition sometimes needs updating too. Overwrite only affected partitions, not the entire table.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Enable dynamic partition overwrite mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Process data that might span multiple dates
raw_events = spark.read.json("s3://raw/events/incoming/")

processed = (raw_events
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .filter(F.col("event_date").isNotNull())
)

# Dynamic overwrite: only replaces partitions present in this batch
# If batch contains dates [2024-01-14, 2024-01-15]:
#   - Overwrites event_date=2024-01-14/ and event_date=2024-01-15/ only
#   - Leaves all other date partitions untouched
(processed
    .repartition("event_date")
    .write
    .mode("overwrite")  # With dynamic mode, only affected partitions overwritten
    .partitionBy("event_date")
    .parquet("s3://data-lake/events/"))

# Verify: check which partitions were affected
affected_dates = processed.select("event_date").distinct().collect()
print(f"Partitions overwritten: {[r.event_date for r in affected_dates]}")
```

### Static vs Dynamic Partition Overwrite

| Mode | Behavior | Risk | Use Case |
|------|----------|------|----------|
| Static (default) | Overwrites ALL partitions | Data loss | Full reload only |
| Dynamic | Overwrites only partitions in batch | Partial load | Incremental ETL |

---

## Pattern 4: Compaction Job

**Problem:** Over time, incremental appends create thousands of small files per partition. Read performance degrades from ~2 seconds to ~45 seconds per query.

```python
from pyspark.sql import SparkSession, functions as F
import os

spark = SparkSession.builder.appName("Compaction").getOrCreate()

def compact_partition(input_path, partition_filter, target_file_mb=256):
    """
    Read a partition, coalesce small files, rewrite with optimal size.
    """
    # Read the specific partition
    df = (spark.read.parquet(input_path)
        .filter(partition_filter))
    
    row_count = df.count()
    if row_count == 0:
        return {"status": "empty", "files_before": 0, "files_after": 0}
    
    # Estimate current file count and sizes
    # (In production, use file listing instead)
    current_partitions = df.rdd.getNumPartitions()
    
    # Calculate optimal file count
    sample_size = df.limit(10000).toPandas().memory_usage(deep=True).sum()
    estimated_size_mb = (sample_size / 10000) * row_count / 1024 / 1024
    optimal_files = max(1, int(estimated_size_mb / target_file_mb))
    
    # Only compact if significant improvement
    if current_partitions <= optimal_files * 1.5:
        return {"status": "skipped", "reason": "already optimal"}
    
    # Rewrite with optimal file count
    (df
        .coalesce(optimal_files)
        .sortWithinPartitions("timestamp")  # Better compression
        .write
        .mode("overwrite")
        .parquet(f"{input_path}_compacted/"))
    
    return {
        "status": "compacted",
        "files_before": current_partitions,
        "files_after": optimal_files,
        "size_mb": estimated_size_mb,
    }

def run_compaction_job(base_path, partition_col, days_to_compact=7):
    """Compact recent partitions that have accumulated small files."""
    from datetime import date, timedelta
    
    results = []
    for i in range(days_to_compact):
        target_date = (date.today() - timedelta(days=i)).isoformat()
        partition_filter = F.col(partition_col) == target_date
        
        result = compact_partition(base_path, partition_filter)
        result["partition"] = target_date
        results.append(result)
        
        if result["status"] == "compacted":
            print(f"  {target_date}: {result['files_before']} → {result['files_after']} files")
    
    return results

# Run compaction
results = run_compaction_job("s3://data-lake/events/", "event_date", days_to_compact=7)

# For Delta Lake: use OPTIMIZE command instead
spark.sql("OPTIMIZE delta.`s3://data-lake/events/` WHERE event_date >= '2024-01-08'")
```

### Compaction Scheduling Strategy

| Data Age | Compaction Frequency | Target File Size |
|----------|---------------------|-----------------|
| Today (hot) | Skip (still receiving writes) | N/A |
| Yesterday | Every 4 hours | 256MB |
| Last 7 days | Daily | 256MB |
| Last 30 days | Weekly | 512MB |
| Older | Monthly | 1GB |

---

## Interview Tips

> **Tip 1:** "How do you control output file sizes?" — "Three approaches: (1) repartition/coalesce to target file count based on estimated data size, (2) maxRecordsPerFile option to cap rows per file, (3) Delta Lake's OPTIMIZE command for automatic compaction. The target depends on the storage system and query patterns — typically 128MB-1GB for Parquet on cloud storage. I always sort within partitions for better compression ratios."

> **Tip 2:** "How would you eliminate shuffle from a frequently-run join?" — "Bucket both tables on the join key with identical bucket counts, sorted by the same column. Spark then performs a SortMergeJoin without any Exchange (shuffle) nodes because data is already co-partitioned. The tradeoff: write time increases for bucketing, and you need Hive metastore. For tables joined 24+ times daily, the one-time bucketing cost pays for itself within hours."

> **Tip 3:** "Explain dynamic partition overwrite and when to use it." — "With spark.sql.sources.partitionOverwriteMode=dynamic, writing in overwrite mode only replaces partitions that appear in the current batch. If my batch has data for Jan 14 and Jan 15, only those two date partitions are overwritten — all others remain untouched. This is essential for incremental ETL with late-arriving data. Without it, overwrite would delete ALL existing partitions."
