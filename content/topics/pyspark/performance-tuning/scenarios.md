---
title: "PySpark Performance Tuning - Scenario Questions"
topic: pyspark
subtopic: performance-tuning
content_type: scenario_question
tags: [pyspark, performance, interview, scenarios, optimization]
---

# Scenario Questions — PySpark Performance Tuning

<article data-difficulty="junior">

## 🟢 Junior: Identify the Performance Problem

**Scenario:** This PySpark job takes 2 hours. The tables are: `fact_events` (5B rows) and `dim_user` (200K rows). What's the most likely performance issue and how would you fix it?

```python
result = fact_events.join(dim_user, "user_id") \
    .groupBy("user_segment") \
    .agg(count("*").alias("event_count"))
result.write.parquet("s3://output/segment_counts/")
```

<details>
<summary>✅ Solution</summary>

**Problem:** `dim_user` is only 200K rows (probably <50 MB) but Spark is doing a SortMergeJoin — shuffling ALL 5B rows of `fact_events` across the network. The shuffle of 5B rows is the bottleneck.

**Fix: Broadcast the small dimension table**

```python
from pyspark.sql.functions import broadcast

result = fact_events.join(broadcast(dim_user), "user_id") \
    .groupBy("user_segment") \
    .agg(count("*").alias("event_count"))
result.write.parquet("s3://output/segment_counts/")
```

**Why this fixes it:**
- Broadcasting sends dim_user (200K rows, ~20 MB) to every executor
- Each executor joins its local partition of fact_events with the full dim_user
- Zero shuffle of fact_events (saves moving 5B rows across the network)
- Expected improvement: 2 hours → 10-15 minutes

**Why didn't Spark broadcast automatically?**
- Auto-broadcast threshold is 10 MB by default
- Spark may not have accurate statistics for dim_user's size
- Fix: increase threshold: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")`

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Too Many Small Output Files

**Scenario:** Your pipeline writes daily data partitioned by `region` (50 regions). After the job, you find 10,000 files (50 regions × 200 shuffle partitions = 10,000 tiny files of ~2 MB each). Downstream Athena queries on this data are 10x slower than expected. Fix it.

<details>
<summary>✅ Solution</summary>

**Problem:** Default `spark.sql.shuffle.partitions = 200`. When writing with `partitionBy("region")`, each shuffle partition creates one file per region partition = 200 × 50 = 10,000 files.

**Fix 1: Repartition by the output partition key before writing**

```python
# Repartition to match the physical partition structure
df.repartition(50, "region") \
    .write.partitionBy("region") \
    .mode("overwrite") \
    .parquet("s3://output/events/")
# Result: 50 files total (1 per region, ~400 MB each) ← OPTIMAL
```

**Fix 2: Coalesce per partition (if regions have uneven data)**

```python
# Control files per partition more precisely
df.repartition(100, "region") \
    .write.partitionBy("region") \
    .option("maxRecordsPerFile", 5000000) \
    .mode("overwrite") \
    .parquet("s3://output/events/")
# Each file gets at most 5M records — manageable, predictable sizes
```

**Fix 3: Enable AQE coalescing (Spark 3.0+)**

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
# AQE merges tiny post-shuffle partitions into ~256 MB chunks automatically
```

**Fix 4: Post-write compaction (if can't change the write job)**

```python
# Separate compaction job
for region in regions:
    path = f"s3://output/events/region={region}/"
    spark.read.parquet(path) \
        .coalesce(1) \
        .write.mode("overwrite") \
        .parquet(path)
```

**Target:** 128 MB–1 GB per file for optimal Athena/Spark read performance.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Spark Job OOM During GroupBy

**Scenario:** Your job crashes with `java.lang.OutOfMemoryError: Java heap space` during a `groupBy("user_id").agg(collect_list("event"))` operation. Some users have 5M+ events. Executor memory is 8 GB. Fix the OOM without simply throwing more memory at it.

<details>
<summary>✅ Solution</summary>

**Root cause:** `collect_list` accumulates ALL events for a user into a single array in memory. A user with 5M events × 200 bytes each = 1 GB for ONE group — exceeds executor memory.

**Fix 1: Cap the collection (most practical)**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, collect_list

# Keep only last 1000 events per user
w = Window.partitionBy("user_id").orderBy(col("event_time").desc())
limited = df.withColumn("rn", row_number().over(w)).filter("rn <= 1000").drop("rn")

# Now collect_list is bounded: max 1000 items per group
result = limited.groupBy("user_id").agg(collect_list("event").alias("recent_events"))
```

**Fix 2: Use struct instead of collect_list (if you need aggregate, not list)**

```python
# Instead of collecting all events, aggregate them
result = df.groupBy("user_id").agg(
    count("*").alias("total_events"),
    countDistinct("event_type").alias("unique_event_types"),
    max("event_time").alias("last_event"),
    min("event_time").alias("first_event"),
)
# No unbounded memory — aggregates are O(1) memory regardless of group size
```

**Fix 3: More partitions + more memory (if you truly need all data)**

```python
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # More partitions = less per executor
spark.conf.set("spark.executor.memory", "32g")           # Handle large groups
spark.conf.set("spark.executor.memoryOverhead", "8g")    # Extra for collection overhead
```

**Fix 4: Use mapPartitions for custom large-group handling**

```python
# For truly massive groups: process partition by partition with explicit memory control
def process_partition(iterator):
    """Process events in batches per user, never holding all in memory."""
    current_user = None
    buffer = []
    for row in iterator:
        if row.user_id != current_user:
            if buffer:
                yield summarize(current_user, buffer)
            current_user = row.user_id
            buffer = []
        buffer.append(row)
        if len(buffer) > 10000:  # Flush every 10K events
            yield summarize(current_user, buffer)
            buffer = []
    if buffer:
        yield summarize(current_user, buffer)

result = df.repartition("user_id").rdd.mapPartitions(process_partition)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Optimize a 3-Hour Job with Multiple Skew Problems

**Scenario:** A daily Spark job processes clickstream data (10B rows) with two major joins:
1. Join with `dim_user` (50M rows) on `user_id` — 15% of events have `user_id = NULL`
2. Join with `dim_page` (1M rows) on `page_url` — the homepage "/" accounts for 40% of all events

Both joins are SortMergeJoins. Total runtime: 3 hours. Reduce to under 30 minutes.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**
- Join 1 skew: 15% of 10B = 1.5B rows have user_id = NULL → one partition gets 1.5B rows
- Join 2 skew: 40% of 10B = 4B rows for page "/" → one partition gets 4B rows
- Both cause one executor to be the bottleneck

**Comprehensive fix:**

```python
from pyspark.sql.functions import broadcast, col, when, concat, lit, floor, rand

# Step 1: Handle dim_page with broadcast (1M rows ≈ 100 MB — broadcastable!)
# No skew problem if we broadcast — every executor has the full dim_page
events_with_page = events.join(broadcast(dim_page), "page_url")
# This eliminates the page_url skew entirely (no shuffle on events)

# Step 2: Handle dim_user NULL skew
# Split into NULL path and non-NULL path

# NULL path: these don't match any user anyway → assign "Unknown" user
null_events = events_with_page.filter(col("user_id").isNull())
unknown_user = dim_user.filter("user_id = 'UNKNOWN'").limit(1)
result_null = null_events.crossJoin(broadcast(unknown_user))
# crossJoin with 1-row broadcast = instant, no skew

# Non-NULL path: normal join (now balanced — no NULL skew)
non_null_events = events_with_page.filter(col("user_id").isNotNull())
result_non_null = non_null_events.join(dim_user.filter("user_id != 'UNKNOWN'"), "user_id")

# Combine
final = result_null.unionByName(result_non_null)

# Step 3: Enable AQE for any residual skew
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Step 4: Optimize write (prevent small files)
final.repartition(200, "event_date") \
    .write.partitionBy("event_date") \
    .mode("overwrite") \
    .parquet("s3://warehouse/enriched_events/")
```

**Expected improvement breakdown:**

| Optimization | Time Saved | Why |
|-------------|-----------|-----|
| Broadcast dim_page (eliminate 10B-row shuffle) | 90 min → 10 min | No shuffle of events for page join |
| Split NULL user path (eliminate 1.5B skew) | 60 min → 5 min | NULLs handled via broadcast, not sort-merge |
| AQE for remaining skew | 10 min → 5 min | Automatic partition splitting |
| **Total** | **3 hours → ~25 min** | |

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Performance Testing Framework

**Scenario:** Your team deploys Spark jobs weekly. Sometimes a code change causes a 5x performance regression that isn't caught until production. Design a performance testing framework that catches regressions before deployment.

<details>
<summary>✅ Solution</summary>

```python
import time
from dataclasses import dataclass

@dataclass
class PerformanceBaseline:
    job_name: str
    stage_durations: dict  # stage_name → expected_seconds
    total_duration: float
    shuffle_bytes: int
    peak_memory: int
    output_row_count: int

class SparkPerformanceTest:
    """
    Run Spark jobs against sample data and compare to baseline.
    Fail CI if performance degrades >50%.
    """
    
    def __init__(self, spark, sample_data_path, baseline: PerformanceBaseline):
        self.spark = spark
        self.sample_data_path = sample_data_path
        self.baseline = baseline
        self.tolerance = 0.5  # 50% degradation threshold
    
    def run_and_validate(self, job_function) -> dict:
        """Run the job and compare against baseline."""
        # Run with metrics collection
        start = time.time()
        result = job_function(self.spark, self.sample_data_path)
        duration = time.time() - start
        
        # Collect metrics
        metrics = {
            "duration": duration,
            "output_rows": result.count() if result else 0,
        }
        
        # Validate against baseline
        failures = []
        
        # Duration check
        if duration > self.baseline.total_duration * (1 + self.tolerance):
            failures.append(
                f"Duration regression: {duration:.0f}s vs baseline {self.baseline.total_duration:.0f}s "
                f"(+{((duration/self.baseline.total_duration)-1)*100:.0f}%)"
            )
        
        # Output row count check (logic correctness)
        if metrics["output_rows"] != self.baseline.output_row_count:
            failures.append(
                f"Row count mismatch: {metrics['output_rows']} vs expected {self.baseline.output_row_count}"
            )
        
        return {
            "passed": len(failures) == 0,
            "failures": failures,
            "metrics": metrics,
            "baseline": self.baseline,
        }

# Usage in CI/CD pipeline
def test_daily_etl_performance():
    """Run in CI before merging PRs that touch ETL code."""
    baseline = PerformanceBaseline(
        job_name="daily_orders_etl",
        stage_durations={"read": 5, "join": 30, "write": 10},
        total_duration=45,
        shuffle_bytes=500_000_000,
        peak_memory=4_000_000_000,
        output_row_count=1_000_000,  # Sample data expected output
    )
    
    tester = SparkPerformanceTest(spark, "s3://test-data/sample/", baseline)
    result = tester.run_and_validate(run_daily_etl)
    
    assert result["passed"], f"Performance regression detected: {result['failures']}"

# In CI config (GitHub Actions, Jenkins, etc.):
# 1. Spin up Spark test cluster with fixed resources
# 2. Run against FIXED sample dataset (deterministic)
# 3. Compare duration and output against stored baselines
# 4. Fail the PR if regression detected
# 5. Update baseline when intentional changes are made
```

**Key design decisions:**
- Fixed sample data (not production — deterministic, fast)
- Fixed cluster resources (same instance types every run — comparable results)
- 50% tolerance (accounts for cloud variability without masking real regressions)
- Row count validation (catches logic bugs, not just performance)
- Baseline stored in version control (updated intentionally with PR review)

</details>

</article>
