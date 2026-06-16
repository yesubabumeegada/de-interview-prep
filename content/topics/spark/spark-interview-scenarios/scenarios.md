---
title: "Spark End-to-End Interview Scenarios"
description: "Full system design and debugging scenarios for senior Spark Data Engineering interviews"
content_type: scenario_question
topic: spark
subtopic: spark-interview-scenarios
tags: [spark, system-design, interview, end-to-end, architecture, debugging]
---

<article data-difficulty="junior">

## Scenario: Tracing groupBy().count().show() End-to-End

Walk me through what happens step-by-step when you run `df.groupBy("category").count().show()` on a 100GB DataFrame — from the moment you call `.show()` through driver planning, task execution, shuffle, and result collection.

<details>
<summary>✅ Solution</summary>

### Overview: Two Stages Separated by a Shuffle

```
Stage 1: Partial Aggregation (map-side)
    → Read partitions → count per category per partition
    → Write shuffle files

SHUFFLE (network transfer)

Stage 2: Final Aggregation (reduce-side)
    → Read grouped shuffle data → sum counts per category
    → Return results to driver

Driver: collect top 20 rows → print to console
```

### Step-by-Step Walkthrough

**Step 1: Action Triggers Execution**

`show()` is an action. When you call it, Spark:
1. Submits the job to the DAG scheduler
2. The DAG scheduler breaks the logical plan into stages based on shuffle boundaries
3. The task scheduler assigns tasks to executors

Nothing before this moment has touched data.

**Step 2: Catalyst Analyzes and Optimizes the Plan**

```python
# Your code:
df.groupBy("category").count().show()

# Catalyst resolves the plan:
# 1. Resolve "category" column — exists, type String
# 2. Optimize: HashAggregate is more efficient than SortAggregate for count()
# 3. Physical plan:
#    *(2) HashAggregate(keys=[category], functions=[count(1)])  ← final
#    +- Exchange hashpartitioning(category, 200)               ← shuffle
#       +- *(1) HashAggregate(keys=[category], functions=[partial_count()])  ← partial
#          +- FileScan parquet [category]                       ← read (column pruning applied)
```

**Step 3: Stage 1 — Partial Aggregation (Map Side)**

Say the 100GB DataFrame is split into 800 partitions (each ~128MB).

- Spark launches **800 tasks** in Stage 1
- Each task reads its partition (one Parquet file or file slice)
- **Column pruning**: only the `category` column is read from Parquet (not the full row)
- Each task runs a local `HashAggregate`:
  - Maintain an in-memory hash map: `{category → partial_count}`
  - For each row, increment the counter for that category key
- When the partition is processed, the task **writes shuffle output files** to local disk
  - Each shuffle file is partitioned by `hash(category) % 200` (the number of reduce partitions)

```
Task 1 result (written to shuffle):
  partition 0: {Electronics: 1200, Books: 800}
  partition 1: {Clothing: 950, ...}
  ...
  partition 199: {...}

Task 2 result:
  partition 0: {Electronics: 900, Books: 1100}
  ...
```

**Step 4: Shuffle — Data Redistribution**

The shuffle moves data across the network so all rows with the same `category` end up on the same executor for the final aggregation.

- The **shuffle service** (or executor directly) serves shuffle blocks to requesting executors
- Each Stage 2 task fetches its partition from ALL 800 Stage 1 tasks
  - Task 0 in Stage 2 fetches partition 0 from all 800 shuffle outputs
  - This means 800 network reads just for one Stage 2 task

This is why shuffle is expensive: 200 reducers × 800 mappers = **160,000 network connections** (logically).

**Step 5: Stage 2 — Final Aggregation (Reduce Side)**

- Spark launches **200 tasks** in Stage 2 (one per shuffle partition)
- Each task reads its assigned shuffle partition from all 800 Stage 1 tasks
- Runs another `HashAggregate` — this time the final merge:
  - Combine all partial counts for each category: `{Electronics: 1200 + 900 + ... = 45,000}`

**Step 6: Driver Collects Results for show()**

`show()` by default shows 20 rows. Spark adds an implicit `LIMIT 20` to the plan.

- The driver collects results from Stage 2 executors
- Only enough data to fill 20 rows is transferred to the driver
- The driver formats and prints the table to stdout

### Key Concepts Illustrated

| Concept | Where It Appears |
|---------|-----------------|
| Lazy evaluation | Nothing runs until `show()` |
| Column pruning | Only `category` read from Parquet |
| Partial aggregation | Stage 1 HashAggregate (map-side combine) |
| Shuffle | Between Stage 1 and Stage 2 |
| Final aggregation | Stage 2 HashAggregate |
| Result collection | Driver collects top 20 rows |

### Spark UI After the Job

```
Jobs → Job 0
  └── Stage 0: Partial HashAggregate (800 tasks)
  └── Stage 1: Exchange + Final HashAggregate (200 tasks)
```

</details>

</article>

<article data-difficulty="mid">

## Scenario: Designing a Clickstream Processing Pipeline

Design a Spark pipeline to process 500GB of raw JSON clickstream data daily: parse and validate JSON, deduplicate events (same user+event_id within 24h), sessionize (30-min gap = new session), compute daily session metrics per user, write to Delta Lake partitioned by date. Include schema, transformations, and error handling.

<details>
<summary>✅ Solution</summary>

### Schema Definition

```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

spark = SparkSession.builder \
    .appName("clickstream-daily") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "800") \
    .getOrCreate()

# Raw JSON schema
RAW_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("event_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("page_url", StringType(), nullable=True),
    StructField("timestamp", LongType(), nullable=True),    # Unix ms
    StructField("session_id", StringType(), nullable=True), # Client-side (unreliable)
    StructField("device_type", StringType(), nullable=True),
    StructField("properties", MapType(StringType(), StringType()), nullable=True),
])

# Output schema for Delta Lake
OUTPUT_PATH = "s3://my-data/clickstream/delta/"
QUARANTINE_PATH = "s3://my-data/clickstream/quarantine/"
```

### Step 1: Ingest and Parse JSON

```python
def ingest_raw(date_str: str):
    """Read raw JSON, parse with schema, quarantine bad records."""
    raw_path = f"s3://my-data/raw/clickstream/date={date_str}/"

    # Read with permissive mode — bad records go to _corrupt_record
    raw_df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(RAW_SCHEMA.add("_corrupt_record", StringType())) \
        .json(raw_path)

    # Split valid from corrupt
    corrupt = raw_df.filter(col("_corrupt_record").isNotNull())
    valid = raw_df.filter(col("_corrupt_record").isNull()) \
                  .drop("_corrupt_record")

    # Quarantine corrupt records for investigation
    if corrupt.count() > 0:
        corrupt.withColumn("ingestion_date", lit(date_str)) \
               .write.mode("append") \
               .partitionBy("ingestion_date") \
               .json(QUARANTINE_PATH)

    return valid
```

### Step 2: Validate and Clean

```python
def validate_and_clean(df):
    """Apply business validation rules and standardize fields."""
    # Parse timestamp from Unix millis to timestamp type
    df = df.withColumn("event_ts", (col("timestamp") / 1000).cast("timestamp"))

    # Validation rules
    valid = df.filter(
        col("user_id").isNotNull() &
        col("event_id").isNotNull() &
        col("event_ts").isNotNull() &
        col("event_type").isin("click", "view", "scroll", "purchase", "search") &
        col("user_id").rlike("^[a-zA-Z0-9_-]{1,64}$")  # Format check
    )

    # Reject invalid rows — send to quarantine
    invalid = df.subtract(valid)
    if invalid.count() > 0:
        invalid.withColumn("rejection_reason", lit("validation_failed")) \
               .write.mode("append").json(QUARANTINE_PATH + "/validation/")

    return valid \
        .withColumn("device_type", lower(col("device_type"))) \
        .withColumn("page_url", regexp_replace(col("page_url"), "\\?.*$", ""))  # strip query params
```

### Step 3: Deduplicate Events

```python
def deduplicate(df):
    """Remove duplicate events: same user_id + event_id within the processing day."""
    from pyspark.sql.window import Window

    # Rank duplicates — keep the earliest timestamp for each user+event_id pair
    window = Window.partitionBy("user_id", "event_id").orderBy("event_ts")

    deduped = df.withColumn("rn", row_number().over(window)) \
                .filter(col("rn") == 1) \
                .drop("rn")

    return deduped
```

### Step 4: Sessionize — 30-Minute Gap Rule

```python
def sessionize(df):
    """
    Assign session IDs based on 30-minute inactivity gap.
    A new session starts when the gap between consecutive events > 30 minutes.
    """
    from pyspark.sql.window import Window

    SESSION_GAP_MINUTES = 30

    # Order events per user by time
    user_window = Window.partitionBy("user_id").orderBy("event_ts")

    # Get the previous event timestamp for each user
    df = df.withColumn("prev_event_ts", lag("event_ts", 1).over(user_window))

    # Mark session boundaries: first event ever, or gap > 30 min
    df = df.withColumn(
        "is_new_session",
        when(col("prev_event_ts").isNull(), lit(1))
        .when(
            (unix_timestamp("event_ts") - unix_timestamp("prev_event_ts")) > (SESSION_GAP_MINUTES * 60),
            lit(1)
        )
        .otherwise(lit(0))
    )

    # Cumulative sum of new session flags = session index per user
    df = df.withColumn(
        "session_index",
        sum("is_new_session").over(user_window.rowsBetween(Window.unboundedPreceding, 0))
    )

    # Build a unique session ID: user_id + date + session index
    df = df.withColumn(
        "computed_session_id",
        concat_ws("_", col("user_id"), date_format("event_ts", "yyyyMMdd"), col("session_index"))
    )

    return df.drop("prev_event_ts", "is_new_session", "session_index")
```

### Step 5: Compute Session-Level Metrics Per User

```python
def compute_session_metrics(df, date_str: str):
    """Aggregate events into per-user daily session metrics."""
    session_metrics = df.groupBy("user_id", "computed_session_id") \
        .agg(
            min("event_ts").alias("session_start"),
            max("event_ts").alias("session_end"),
            count("*").alias("event_count"),
            countDistinct("page_url").alias("unique_pages"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            first("device_type").alias("device_type"),
        ) \
        .withColumn(
            "session_duration_minutes",
            (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60
        )

    # Roll up to user-day level
    user_daily_metrics = session_metrics.groupBy("user_id") \
        .agg(
            count("computed_session_id").alias("session_count"),
            sum("event_count").alias("total_events"),
            sum("session_duration_minutes").alias("total_session_minutes"),
            avg("session_duration_minutes").alias("avg_session_minutes"),
            sum("purchase_count").alias("total_purchases"),
            sum("unique_pages").alias("total_unique_pages"),
        ) \
        .withColumn("date", lit(date_str))

    return user_daily_metrics
```

### Step 6: Write to Delta Lake

```python
def write_to_delta(df, date_str: str):
    """Upsert to Delta Lake — idempotent for reruns."""
    from delta.tables import DeltaTable

    # If the table doesn't exist yet, create it
    if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
        df.write \
          .format("delta") \
          .partitionBy("date") \
          .save(OUTPUT_PATH)
        return

    # MERGE for idempotency — safe to rerun
    delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)

    delta_table.alias("existing").merge(
        df.alias("new"),
        "existing.user_id = new.user_id AND existing.date = new.date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
```

### Step 7: Orchestrate the Full Pipeline

```python
def run_pipeline(date_str: str):
    print(f"[Pipeline] Starting for date: {date_str}")

    raw = ingest_raw(date_str)
    print(f"[Pipeline] Raw records: {raw.count()}")

    clean = validate_and_clean(raw)
    deduped = deduplicate(clean)
    print(f"[Pipeline] After dedup: {deduped.count()}")

    sessionized = sessionize(deduped)
    metrics = compute_session_metrics(sessionized, date_str)

    write_to_delta(metrics, date_str)
    print(f"[Pipeline] Done. Records written: {metrics.count()}")

# Entry point
if __name__ == "__main__":
    import sys
    date_str = sys.argv[1]  # e.g., "2024-01-15"
    run_pipeline(date_str)
```

### Error Handling Summary

| Error Type | Handling |
|-----------|---------|
| Corrupt JSON | `PERMISSIVE` mode + quarantine |
| Invalid field values | Validation filter + quarantine |
| Duplicate events | Window dedup (keep earliest) |
| Pipeline rerun | Delta MERGE (upsert) makes it idempotent |
| Schema evolution | Delta Lake schema evolution (`mergeSchema=true`) |

</details>

</article>

<article data-difficulty="senior">

## Scenario: Cutting a 4-Hour EMR Job to 45 Minutes at 60% Cost Reduction

Your company processes 10TB/day of financial transactions using Spark on EMR. The CTO wants to reduce processing time from 4 hours to 45 minutes and cut costs by 40%. You have full authority over architecture. Design a comprehensive optimization strategy covering: data format/layout, job architecture, cluster configuration, cost optimization (Spot), and monitoring. Justify every decision.

<details>
<summary>✅ Solution</summary>

### Baseline Assessment

Before proposing changes, understand the current state:

```python
# Profile the current job
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Key metrics to gather from Spark UI:
# - Input data format (JSON/CSV vs Parquet/ORC)
# - Shuffle read/write sizes per stage
# - Skew indicators (max task time / median task time)
# - GC time percentage
# - Spill to disk amounts
# - Current cluster: instance types, count, Spot vs On-Demand ratio
```

Assuming the baseline is:
- Input: raw CSV or JSON (10TB)
- Cluster: 20x m5.4xlarge On-Demand (80 vCPU, 320GB RAM)
- No partitioning strategy
- No data skipping
- `spark.sql.shuffle.partitions=200` (default)

---

### Pillar 1: Data Format and Layout (Est. 2-3x speedup)

**Convert to Parquet with optimal partitioning and Z-ordering**

CSV/JSON is the single biggest performance killer. Switching to Parquet:
- Column pruning: read only needed columns
- Predicate pushdown: skip row groups where filter predicates can't match
- 5-10x better compression → less I/O

```python
# One-time migration: convert raw to Parquet
raw_df = spark.read.csv("s3://my-data/raw/transactions/")
raw_df.write \
    .format("delta") \
    .partitionBy("transaction_date", "region") \   # Matches common query filters
    .option("dataChange", "false") \
    .save("s3://my-data/transactions/delta/")

# After migration: daily ingestion writes directly to Delta
new_data.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("transaction_date", "region") \
    .save("s3://my-data/transactions/delta/")
```

**Apply Z-ORDER for multi-dimensional filtering (Delta Lake)**

```sql
-- Z-order on columns used together in filters
OPTIMIZE delta.`s3://my-data/transactions/delta/`
ZORDER BY (merchant_id, transaction_type);
-- This colocates rows with similar merchant_id + type values, enabling data skipping
```

**Enable Delta Lake statistics collection:**
```python
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.dataSkippingNumIndexedCols", "10")
```

**Impact:** For queries with selective filters on `merchant_id` or `region`, Delta's data skipping can skip 80-90% of files without reading them. Reading 1TB instead of 10TB at the start cuts job time proportionally.

---

### Pillar 2: Job Architecture Refactoring (Est. 2x speedup)

**Split the monolithic job into focused stages with incremental processing**

The 4-hour job likely processes all 10TB every run. Switch to:

```python
# Pattern 1: Process only today's partition (partition pruning)
daily_data = spark.read.format("delta") \
    .load("s3://my-data/transactions/delta/") \
    .filter(col("transaction_date") == today)   # Reads only today's Parquet files
# 10TB total → ~28GB per day → reads only 28GB, not 10TB

# Pattern 2: Separate raw processing from aggregation
# Job 1 (fast): validate + clean + write to Silver layer (15 min)
# Job 2 (medium): compute aggregates from Silver (20 min)
# Job 3 (fast): update Gold metrics from Job 2 output (5 min)
# Parallelizable, faster to debug, easier to rerun individual stages
```

**Eliminate redundant actions (count() calls in production)**

```python
# Bad: triggers full job execution 3 times
raw_count = raw_df.count()       # Full scan
clean_count = clean_df.count()   # Full scan again
output_count = result.count()    # Full scan again

# Better: persist and count once, or skip counts in production
clean_df.persist()
result = transform(clean_df)
result.write.format("delta").save(output_path)
# Use Delta transaction log for row counts — no extra scan needed
```

**Broadcast dimension tables aggressively:**

```python
# Financial transactions often join to merchant, account, currency tables
# These are typically < 500MB — always broadcast them
from pyspark.sql.functions import broadcast

merchants = spark.table("merchants")        # ~50MB
currencies = spark.table("currency_rates")  # ~1MB

result = transactions \
    .join(broadcast(merchants), "merchant_id") \
    .join(broadcast(currencies), "currency_code") \
    .select(...)
# Eliminates 2 shuffle stages
```

---

### Pillar 3: Cluster Configuration (Est. 1.5x speedup + cost savings)

**Right-size instances and tune parallelism**

```bash
# Current: 20x m5.4xlarge = 80 vCPU, 320GB RAM, $3.20/hr each = $64/hr

# Proposed: storage-optimized + compute-optimized mix
# Driver: 1x m5.2xlarge On-Demand (8 vCPU, 32GB) — always on-demand
# Core executors: 5x m5.4xlarge On-Demand (20 vCPU, 160GB) — stable baseline
# Task executors: 30x m5.4xlarge Spot (120 vCPU, 480GB) — burst capacity

# Spot price ~70% cheaper: 30 × $3.20 × 0.30 = $2.88/hr vs $96/hr on-demand
# Total cluster cost: 5 × $3.20 + 30 × $0.96 + 1 × $1.60 = $46.40/hr
# vs original: 20 × $3.20 = $64/hr → 27% cluster cost savings
# But job finishes in 45 min instead of 4h → total cost: $46.40 × 0.75 = $34.80 vs $256 → 86% savings
```

**EMR-specific configuration:**

```bash
# EMR cluster config (bootstrap or API)
aws emr create-cluster \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.2xlarge,InstanceCount=1,Market=ON_DEMAND \
    InstanceGroupType=CORE,InstanceType=m5.4xlarge,InstanceCount=5,Market=ON_DEMAND \
    InstanceGroupType=TASK,InstanceType=m5.4xlarge,InstanceCount=30,Market=SPOT,BidPrice=ON_DEMAND_PRICE \
  --configurations '[
    {"Classification":"spark-defaults","Properties":{
      "spark.sql.adaptive.enabled":"true",
      "spark.sql.adaptive.coalescePartitions.enabled":"true",
      "spark.sql.adaptive.skewJoin.enabled":"true",
      "spark.sql.shuffle.partitions":"4000",
      "spark.executor.memory":"28g",
      "spark.executor.cores":"4",
      "spark.executor.memoryOverhead":"4g",
      "spark.task.maxFailures":"8",
      "spark.speculation":"true"
    }}
  ]'
```

**AQE configuration — let Spark self-tune:**

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "200")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

---

### Pillar 4: Cost Optimization Details

**EMR Instance Fleet with multiple Spot types (higher availability):**

```json
{
  "InstanceFleetType": "TASK",
  "TargetSpotCapacity": 120,
  "InstanceTypeConfigs": [
    {"InstanceType": "m5.4xlarge",  "WeightedCapacity": 4},
    {"InstanceType": "m5a.4xlarge", "WeightedCapacity": 4},
    {"InstanceType": "m4.4xlarge",  "WeightedCapacity": 4},
    {"InstanceType": "r5.2xlarge",  "WeightedCapacity": 4}
  ],
  "LaunchSpecifications": {
    "SpotSpecification": {"TimeoutAction": "SWITCH_TO_ON_DEMAND", "TimeoutDurationMinutes": 5}
  }
}
```

**Auto-terminate cluster after job completion:**

```bash
aws emr create-cluster --auto-terminate ...
# Or in Airflow:
EmrCreateJobFlowOperator(task_id="create_cluster", ...) >>
EmrAddStepsOperator(task_id="submit_spark_job", ...) >>
EmrJobFlowSensor(task_id="wait_for_job", ...) >>
EmrTerminateJobFlowOperator(task_id="terminate_cluster", ...)
```

**Use EMR Serverless for variable workloads:**

```bash
# Alternative to managed cluster — pay per vCPU-hour, zero idle cost
aws emr-serverless start-job-run \
  --application-id $APP_ID \
  --execution-role-arn $ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://my-code/jobs/transactions.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=28g"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {"logUri": "s3://my-logs/emr-serverless/"}
    }
  }'
```

---

### Pillar 5: Monitoring and Observability

```python
# Application-level metrics to CloudWatch
spark.conf.set("spark.metrics.namespace", "financial-transactions")
spark.conf.set("spark.metrics.conf.driver.sink.cloudwatch.class",
               "com.banzaicloud.spark.metrics.sink.CloudWatchSink")

# Key metrics dashboard (Grafana + CloudWatch):
# - spark_executor_cpuTime_total (CPU utilization)
# - spark_executor_shuffleReadBytes_total (shuffle size — flag if > 2x input)
# - spark_executor_diskBytesSpilled_total (spill — should be 0)
# - spark_executor_gcTime_total / spark_executor_runTime_total (GC ratio — flag if > 5%)
# - Job duration per run (flag if > 50 min — approaching SLA)
```

---

### Expected Outcome Summary

| Optimization | Time Reduction | Cost Impact |
|-------------|---------------|-------------|
| Parquet + Delta + partitioning | 10TB → ~28GB read | -70% I/O time |
| Broadcast joins | Eliminate 2 shuffle stages | -30% shuffle time |
| Incremental processing | Skip historical data | -80% compute |
| AQE + correct shuffle.partitions | Eliminate straggler stages | -40% stage time |
| Spot instances + auto-terminate | Same compute, lower price | -60-70% cost |
| **Combined** | **4h → ~45 min** | **~86% cost reduction** |

The CTO asked for 45 min and 40% cost cut. This architecture delivers ~45 min runtime and ~86% total cost reduction (job runs faster AND uses cheaper instances). The excess savings can be used to run additional data quality checks or add more partitions for better query performance downstream.

</details>

</article>
