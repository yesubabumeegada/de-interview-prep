---
title: "PySpark Performance Tuning - Real-World Production Examples"
topic: pyspark
subtopic: performance-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, performance, production, case-study, optimization, monitoring]
---

# PySpark Performance Tuning — Real-World Production Examples

## Case Study 1: ETL Job — 4 Hours to 25 Minutes

**The problem:** Nightly ETL joins a 2B-row fact table with a 500K-row dimension, aggregates by date, and writes partitioned Parquet. Takes 4 hours.

**Diagnosis (Spark UI findings):**
- Stage 1 (read): 2 minutes — fine
- Stage 2 (join): 3.5 hours — PROBLEM
  - Shuffle write: 180 GB
  - SortMergeJoin between 2B and 500K rows
  - No broadcast despite dimension being small (stats unavailable)

**Fix:**

```python
# Before: SortMergeJoin (shuffles both 2B rows and 500K rows)
result = fact_df.join(dim_df, "product_id").groupBy("date").agg(...)

# After: Broadcast the small dimension (eliminates fact table shuffle)
from pyspark.sql.functions import broadcast

result = fact_df.join(broadcast(dim_df), "product_id").groupBy("date").agg(...)

# Additional: reduce output file count
result.repartition(100, "date") \
    .write.partitionBy("date") \
    .mode("overwrite") \
    .parquet("s3://warehouse/daily_agg/")
```

**Result:** 4 hours → 25 minutes. The 180 GB shuffle was eliminated entirely by broadcasting a 50 MB dimension table.

---

## Case Study 2: Skewed Join — 1 Task Takes 45 Minutes

**The problem:** A join on `customer_id` has one task running 45 minutes while all other 199 tasks complete in 2 minutes. The `customer_id = NULL` partition has 500M rows.

**Diagnosis:**

```python
# Confirmed skew: NULL has 500M rows, second-highest has 50K
df.groupBy("customer_id").count().orderBy(col("count").desc()).show(5)
# NULL: 500,000,000
# C001: 52,000
# C002: 48,000
```

**Fix — Separate hot and cold paths:**

```python
# Hot path: broadcast-join NULL records (after filtering dim to NULL only)
hot = fact_df.filter(col("customer_id").isNull())
cold = fact_df.filter(col("customer_id").isNotNull())

# For NULL customer_id: use a default "Unknown" dimension record
unknown_customer = dim_df.filter("customer_id = 'UNKNOWN'")
result_hot = hot.crossJoin(broadcast(unknown_customer))

# For non-NULL: normal join (now balanced, no skew)
result_cold = cold.join(dim_df.filter("customer_id != 'UNKNOWN'"), "customer_id")

# Combine
result = result_hot.unionByName(result_cold)
```

**Result:** 45 minutes → 4 minutes. The 500M NULL records are handled via broadcast (no shuffle for them), and the remaining data is balanced.

---

## Case Study 3: Small Files Problem — Read Time 30x Slower

**The problem:** A streaming pipeline writes micro-batches to S3 every 30 seconds, creating 2,880 files/day (most under 5 MB). Downstream batch queries that read this data take 30 minutes instead of 1 minute.

**Root cause:** S3 LIST operations + per-file overhead. 100,000 files × 50ms open time = 83 minutes just for file operations.

**Fix — Compaction job:**

```python
# Daily compaction job: merge small files into optimal size
def compact_partition(spark, path, target_size_mb=256):
    """Compact small files in a partition into ~256 MB files."""
    df = spark.read.parquet(path)
    num_rows = df.count()
    
    if num_rows == 0:
        return
    
    # Estimate current avg file size
    # Target: 256 MB files
    current_size = spark._jvm.org.apache.hadoop.fs.FileSystem \
        .get(spark._jsc.hadoopConfiguration()) \
        .getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path)) \
        .getLength()
    
    target_files = max(1, int(current_size / (target_size_mb * 1024 * 1024)))
    
    df.coalesce(target_files) \
        .write.mode("overwrite") \
        .parquet(path)
    
    print(f"Compacted {path}: {target_files} files")

# Run nightly for yesterday's streaming data
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
compact_partition(spark, f"s3://lake/events/date={yesterday}/")
```

**Better approach with Delta Lake:**

```python
# Delta handles this automatically
spark.sql(f"OPTIMIZE delta.`s3://lake/events/` WHERE date = '{yesterday}'")
# Compacts small files into ~1 GB target size automatically
```

**Result:** Read time from 30 minutes → 1 minute after compaction.

---

## Case Study 4: OOM During Large Aggregation

**The problem:** `groupBy("user_id").agg(collect_list("events"))` crashes with OOM. Some users have millions of events — their event lists exceed executor memory.

**Fix — Cap the collection with a window function:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, collect_list, col

# Limit to last 100 events per user (prevents unbounded memory)
window = Window.partitionBy("user_id").orderBy(col("event_time").desc())

limited = events \
    .withColumn("rn", row_number().over(window)) \
    .filter("rn <= 100")  # Keep only latest 100 per user

# Now safe to collect — bounded at 100 per group
result = limited.groupBy("user_id") \
    .agg(collect_list("event_type").alias("recent_events"))
```

**Alternative — Increase memory with proper partitioning:**

```python
# If you need ALL events: increase memory + more partitions
spark.conf.set("spark.executor.memory", "32g")
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # More partitions = smaller each

# The aggregation's memory per partition = total_group_data / num_partitions
# With 2000 partitions: each executor handles fewer groups → less memory pressure
```

---

## Case Study 5: Streaming Pipeline Falling Behind

**The problem:** Structured Streaming job processes Kafka events but consumer lag keeps growing (processing slower than ingestion).

**Diagnosis:**

```python
# Check processing rate in streaming query progress
query.lastProgress
# {
#   "numInputRows": 50000,
#   "processedRowsPerSecond": 5000,  ← Processing speed
#   "inputRowsPerSecond": 10000,     ← Ingestion speed
# }
# Processing (5K/s) < Ingestion (10K/s) → lag grows!
```

**Fixes applied:**

```python
# Fix 1: Increase micro-batch parallelism
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Match Kafka partitions

# Fix 2: Increase trigger interval (process larger batches more efficiently)
query = stream_df.writeStream \
    .trigger(processingTime="30 seconds")  # Was 10 seconds
# Larger batches have better amortization of per-batch overhead

# Fix 3: Remove UDFs (replaced with native functions)
# Before: Python UDF processing each row — 5K rows/sec
# After: Native Spark functions — 50K rows/sec

# Fix 4: Add more executors
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")  # Was 5

# Fix 5: Optimize write (avoid small files overhead)
query = stream_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://cp/") \
    .option("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .trigger(processingTime="30 seconds") \
    .start("s3://lake/events/")
```

**Result:** Processing rate went from 5K/s to 80K/s. Lag resolved within 2 hours.

---

## Production Monitoring Setup

```python
# Custom metrics listener for production Spark jobs
class ETLMetrics:
    def __init__(self, spark, job_name):
        self.spark = spark
        self.job_name = job_name
        self.start_time = time.time()
        self.stage_metrics = {}
    
    def report(self):
        """Generate performance report after job completes."""
        duration = time.time() - self.start_time
        
        # Get metrics from Spark UI programmatically
        sc = self.spark.sparkContext
        status = sc.statusTracker()
        
        metrics = {
            "job_name": self.job_name,
            "duration_seconds": round(duration, 1),
            "total_stages": len(status.getActiveStageIds()) + len(status.getActiveJobIds()),
        }
        
        # Check for common problems
        # (In production: use Spark listener API for detailed metrics)
        
        return metrics

# Usage in ETL pipeline
metrics = ETLMetrics(spark, "daily_orders_etl")
try:
    run_etl_pipeline()
finally:
    report = metrics.report()
    publish_to_datadog(report)  # Send to monitoring system
```

---

## Performance Tuning Decision Tree

```
Job is slow. What's the bottleneck?

├── ONE task much slower than others?
│   ├── Join skew? → Broadcast, salt, or AQE skew handling
│   ├── GroupBy skew? → Two-phase aggregation (partial + final)
│   └── Slow node? → Enable speculation
│
├── ALL tasks are slow?
│   ├── Full table scan? → Add partition filter, push predicate down
│   ├── Large shuffle? → Broadcast small side, reduce data before shuffle
│   ├── Spilling to disk? → Increase memory, add partitions
│   └── Python UDFs? → Replace with native functions or Pandas UDFs
│
├── Too many small tasks?
│   └── Enable AQE coalescing, reduce shuffle.partitions
│
├── Writing too slow?
│   ├── Too many output files? → Coalesce before write
│   └── S3 throttling? → Randomize prefixes, reduce parallelism
│
└── Streaming falling behind?
    ├── Processing < ingestion rate? → More executors, larger batches
    └── State store too large? → Add watermarks, reduce state TTL
```

---

## Interview Tips

> **Tip 1:** "Tell me about a Spark performance issue you solved" — Structure: "The job took [X time]. In Spark UI, I found [specific symptom: skew/shuffle/spill]. Root cause was [explanation]. I fixed it by [broadcast/salt/AQE/repartition]. Result: [X time → Y time], a [N]x improvement."

> **Tip 2:** "How do you prevent the small files problem?" — "Three levels: (1) Write-time: coalesce to target file size before write, use maxRecordsPerFile option. (2) Post-write: run compaction jobs (OPTIMIZE in Delta, custom Spark job for Parquet). (3) Prevention: for streaming, increase trigger interval or enable optimizeWrite in Delta."

> **Tip 3:** "How do you handle OOM in Spark?" — "Depends on what's OOM-ing: (1) Driver OOM → stop using collect() or reduce broadcast size. (2) Executor OOM during shuffle → increase partitions (smaller per-partition data) or increase executor memory. (3) Executor OOM during aggregation → cap unbounded collections (limit per group), or increase memory. Always check Spark UI for which stage/task is failing."
