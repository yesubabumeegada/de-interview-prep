---
title: "Auto Loader - Scenario Questions"
topic: databricks
subtopic: auto-loader
content_type: scenario_question
tags: [databricks, auto-loader, interview, scenarios, ingestion]
---

# Scenario Questions — Auto Loader

<article data-difficulty="junior">

## 🟢 Junior: Basic Auto Loader Setup

**Scenario:** JSON files are being dropped into `s3://data-lake/landing/orders/` every 5 minutes by an upstream API. Set up Auto Loader to ingest these files into a Delta table `production.bronze.orders`.

<details>
<summary>💡 Hint</summary>
Use `cloudFiles` format with JSON. Set a schema location for inference and a checkpoint for exactly-once. Use `trigger(availableNow=True)` for batch-style processing.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import current_timestamp, input_file_name

# Read with Auto Loader
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/checkpoints/orders_schema/")
    .load("s3://data-lake/landing/orders/")
)

# Add metadata columns
enriched_df = (raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# Write to Delta table
(enriched_df.writeStream
    .option("checkpointLocation", "/checkpoints/orders/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("production.bronze.orders")
)
```

**Key Points:**
- `cloudFiles` format tells Spark to use Auto Loader (not regular file read)
- `schemaLocation` stores the inferred schema (persists across runs)
- `checkpointLocation` tracks which files have been processed (exactly-once)
- `trigger(availableNow=True)` processes all available files then stops (batch-style)
- `mergeSchema=true` allows the Delta table to evolve if new columns appear
- Schedule this as a Databricks Workflow running every 10-15 minutes

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Handling Schema Changes

**Scenario:** Your Auto Loader pipeline has been running for 3 months. The upstream team adds a new field `loyalty_tier` to their JSON output. Your pipeline should handle this automatically without failing. Configure it.

<details>
<summary>💡 Hint</summary>
Use schema evolution mode. Options: `addNewColumns` (auto-add), `rescue` (put in _rescued_data), or `failOnNewColumns` (stop and alert).
</details>

<details>
<summary>✅ Solution</summary>

```python
# Option A: Automatically add new columns (simplest for production)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-add new fields
    .option("cloudFiles.schemaLocation", "/checkpoints/orders_schema/")
    .load("s3://data-lake/landing/orders/")
)

(df.writeStream
    .option("checkpointLocation", "/checkpoints/orders/")
    .option("mergeSchema", "true")  # Delta table also evolves
    .trigger(availableNow=True)
    .toTable("production.bronze.orders")
)
# When loyalty_tier appears: automatically added to schema + Delta table

# Option B: Rescue mode (safer — capture new fields without changing schema)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # New fields → _rescued_data
    .option("cloudFiles.schemaLocation", "/checkpoints/orders_schema/")
    .load("s3://data-lake/landing/orders/")
)
# New field captured in _rescued_data column as JSON string
# You decide when to formally add it to the schema
```

**Key Points:**
- `addNewColumns`: easiest, auto-evolves schema (good for trusted sources)
- `rescue`: safest, captures unexpected data without schema change (good for untrusted sources)
- `failOnNewColumns`: strictest, stops the pipeline (good for compliance-heavy environments)
- Both source (Auto Loader) AND sink (Delta) must allow schema evolution
- Monitor rescued data: if _rescued_data is filling up, a schema change happened

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Notification vs Directory Listing

**Scenario:** Your team has two ingestion pipelines: (A) receives 50K files/day with sub-minute latency requirements, (B) receives 100 files/day from a partner with hourly batch drops. Which file discovery mode should each use?

<details>
<summary>💡 Hint</summary>
Notification mode: fast detection (seconds), good for high volume. Directory listing: simpler setup, fine for low volume or when you can't set up cloud events.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Pipeline A: 50K files/day, sub-minute latency → NOTIFICATION MODE
df_a = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")  # Near-instant detection
    .load("s3://lake/high-volume-events/")
)
# Auto Loader creates: S3 event notification → SNS → SQS
# New files detected within seconds of landing
# Scales to millions of files without increasing listing API calls

# Pipeline B: 100 files/day, hourly batches → DIRECTORY LISTING
df_b = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.useNotifications", "false")  # Directory listing
    .load("s3://lake/partner-drops/")
)
# Lists the directory each trigger interval
# For 100 files/day: listing is trivial (one API call)
# No cloud infrastructure to set up or maintain
# Run with trigger(availableNow=True) every hour via Workflow

# Decision matrix:
# High volume (>1K files/day) + low latency → Notification
# Low volume (<1K files/day) + batch is OK → Directory listing
# Cross-account (can't set up notifications) → Directory listing
# Development/testing → Directory listing (simpler)
```

**Key Points:**
- Notification: detects in seconds, scales to millions of files, auto-configures SQS
- Directory listing: detects on each trigger (seconds to minutes), no cloud setup needed
- Cost: notification adds small SQS cost (~$0.40/million messages); listing adds S3 API costs
- Notification requires IAM permissions to create SQS queue + S3 event notifications
- Directory listing works across accounts without special permissions
- You can switch modes later without losing checkpoint state

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Exactly-Once Guarantee

**Scenario:** Your Auto Loader pipeline crashed mid-run (cluster terminated). When you restart it, how do you ensure no files are processed twice (duplicates) or skipped (data loss)?

<details>
<summary>💡 Hint</summary>
The checkpoint is the key. As long as you use the same checkpoint location, Auto Loader resumes exactly where it left off.
</details>

<details>
<summary>✅ Solution</summary>

```python
# The checkpoint ensures exactly-once:
(df.writeStream
    .option("checkpointLocation", "/checkpoints/orders/")  # NEVER change this path!
    .trigger(availableNow=True)
    .toTable("production.bronze.orders")
)

# What happens on crash + restart:
# 1. Auto Loader reads checkpoint: "I processed files A, B, C"
# 2. Checks for new files since last successful commit: "D, E, F are new"
# 3. Processes only D, E, F (no duplicates of A, B, C)
# 4. If it crashed WHILE writing batch (D, E): 
#    - Delta Lake rolled back incomplete write
#    - Checkpoint didn't advance (only advances on successful write)
#    - On restart: re-processes D, E from scratch (exactly-once preserved)

# RULES to maintain exactly-once:
# 1. NEVER delete the checkpoint directory
# 2. NEVER change the checkpoint path for an existing stream
# 3. NEVER reuse a checkpoint for a different source/target
# 4. Let Auto Loader manage checkpoint (don't manually modify)

# If you NEED to reprocess everything (rare):
# Delete the checkpoint → next run processes ALL files from scratch
# Use with caution: may create duplicates if target table already has data
```

**Key Points:**
- Checkpoint = the state that enables exactly-once (it's sacred, don't touch it)
- Crash recovery: automatic — just restart with the same checkpoint path
- Delta Lake transactions: prevent partial writes (all-or-nothing per batch)
- Combined: checkpoint tracks source progress + Delta ensures atomic writes = exactly-once
- One checkpoint per stream (never share between different pipelines)
- Stored in cloud storage (durable across cluster failures)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Adding Ingestion Metadata

**Scenario:** Your data quality team needs to know: (1) when each record was ingested, (2) which source file it came from, and (3) when the source file was last modified. Add these metadata columns during ingestion.

<details>
<summary>💡 Hint</summary>
Use `current_timestamp()` for ingestion time, `input_file_name()` for the source file path, and the `_metadata` column for file modification time.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import current_timestamp, input_file_name, col

# Read with Auto Loader
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://lake/landing/events/")
)

# Add metadata columns
enriched_df = (raw_df
    .withColumn("_ingested_at", current_timestamp())              # When we processed it
    .withColumn("_source_file", input_file_name())                 # Which file it came from
    .withColumn("_file_modified_at", col("_metadata.file_modification_time"))  # File's last modified time
    .withColumn("_file_size_bytes", col("_metadata.file_size"))    # File size
)

# Write with metadata
(enriched_df.writeStream
    .option("checkpointLocation", "/checkpoints/events/")
    .trigger(availableNow=True)
    .toTable("production.bronze.events")
)

# Result:
# |event_id|user_id|...|_ingested_at        |_source_file                    |_file_modified_at   |
# |1001    |U1     |...|2024-03-15 10:30:00 |s3://lake/landing/events/f1.json|2024-03-15 10:25:00 |

# Use cases for metadata:
# - _ingested_at: measure ingestion latency (file_modified vs ingested = lag)
# - _source_file: trace back data quality issues to specific files
# - _file_modified_at: detect late-arriving data
```

**Key Points:**
- `current_timestamp()`: when YOUR pipeline processed the record (ingestion time)
- `input_file_name()`: full path of the source file (for traceability)
- `_metadata.file_modification_time`: when the file was last written (source timestamp)
- Ingestion latency = `_ingested_at - _file_modified_at` (should be small for healthy pipelines)
- Always add these in bronze layer — they're invaluable for debugging
- `_metadata` is a special column Auto Loader provides (also has file_size, file_name)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Backfill 500K Historical Files

**Scenario:** You're setting up Auto Loader on a new source that already has 500K historical files (total 2TB). New files arrive at 1K/day. How do you handle the initial backfill without OOM or timeouts, and then transition to incremental processing?

<details>
<summary>💡 Hint</summary>
Use `maxFilesPerTrigger` to batch the backfill into manageable chunks. The same checkpoint handles both backfill and incremental — just run the same code repeatedly.
</details>

<details>
<summary>✅ Solution</summary>

```python
# The SAME code handles both backfill AND incremental!
# Auto Loader processes files in the order they're discovered.

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.maxFilesPerTrigger", "5000")  # Process 5K files per batch
    .option("cloudFiles.maxBytesPerTrigger", "20g")   # Cap at 20GB per batch
    .option("cloudFiles.schemaLocation", "/checkpoints/historical_schema/")
    .load("s3://lake/landing/events/")
)

# Write with availableNow — processes ALL available then stops
(df.writeStream
    .option("checkpointLocation", "/checkpoints/events/")
    .trigger(availableNow=True)
    .toTable("production.bronze.events")
)

# Behavior:
# Run 1 (backfill): 500K files exist → processes in batches of 5K → 100 batches → takes ~2 hours
# Run 2 (incremental): only 200 new files since run 1 → single batch → takes 30 seconds
# Run 3 (incremental): only 150 new files → single batch → takes 20 seconds

# Cluster config for backfill:
# - Use larger cluster during backfill (16 workers)
# - Scale down to 4 workers for incremental (lower cost)
# - Or use auto-scaling: min=4, max=16

# Schedule:
# During backfill: run every 30 minutes until caught up
# After backfill: run every 15 minutes (normal cadence)
# Detection: if processing time < 1 minute, backfill is complete
```

**Key Points:**
- Same code, same checkpoint for both backfill and incremental (no special handling!)
- `maxFilesPerTrigger` prevents OOM during backfill (limits batch size)
- `availableNow=True` processes ALL available files across multiple internal batches
- Backfill may take hours — that's OK, just let it run (use larger cluster)
- After backfill: same code processes only new files (seconds/minutes)
- The checkpoint makes the transition seamless — no manual "switch to incremental" step

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Source Ingestion Architecture

**Scenario:** You have 8 different data sources landing files in S3 (different formats, schemas, and SLAs). Design an Auto Loader architecture that's maintainable, monitored, and cost-effective.

<details>
<summary>💡 Hint</summary>
One stream per source (separate checkpoints). Configuration-driven (not hard-coded). Parallel execution in a Databricks Workflow. Centralized monitoring.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Configuration-driven multi-source ingestion
import json
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Configuration file (stored in repo, version controlled)
INGESTION_CONFIG = [
    {"name": "api_events", "path": "s3://lake/landing/api/events/", "format": "json", "target": "production.bronze.api_events", "sla_minutes": 15},
    {"name": "cdc_orders", "path": "s3://lake/landing/cdc/orders/", "format": "avro", "target": "production.bronze.orders_cdc", "sla_minutes": 10},
    {"name": "partner_feed", "path": "s3://lake/landing/partner/daily/", "format": "csv", "target": "production.bronze.partner_data", "sla_minutes": 120},
    {"name": "logs_raw", "path": "s3://lake/landing/logs/", "format": "text", "target": "production.bronze.raw_logs", "sla_minutes": 30},
    {"name": "iot_sensors", "path": "s3://lake/landing/iot/", "format": "json", "target": "production.bronze.iot_readings", "sla_minutes": 5},
    {"name": "marketing_pixels", "path": "s3://lake/landing/marketing/", "format": "json", "target": "production.bronze.marketing_events", "sla_minutes": 60},
    {"name": "finance_exports", "path": "s3://lake/landing/finance/", "format": "parquet", "target": "production.bronze.finance_data", "sla_minutes": 240},
    {"name": "ml_features", "path": "s3://lake/landing/ml/features/", "format": "parquet", "target": "production.bronze.ml_features", "sla_minutes": 30},
]

def ingest_source(config: dict):
    """Generic ingestion function for any source."""
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config["format"])
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{config['name']}_schema/")
        .option("cloudFiles.useNotifications", "true")
        .load(config["path"])
    )
    
    # Standard metadata
    df = (df
        .withColumn("_source_name", lit(config["name"]))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    query = (df.writeStream
        .option("checkpointLocation", f"/checkpoints/{config['name']}/")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config["target"])
    )
    
    query.awaitTermination()
    return query.lastProgress

# Databricks Workflow:
# Each source = separate task (parallel execution)
# Benefits: one source failure doesn't block others
# Monitoring: each task has its own status in the Workflow UI
```

**Key Points:**
- Configuration-driven: add a new source = add a config entry (no code changes)
- One checkpoint per source: failures are isolated (one bad source doesn't affect others)
- Parallel execution: Databricks Workflow runs all sources concurrently
- Common metadata (_source_name, _ingested_at): enables cross-source monitoring
- SLA tracking: compare _ingested_at against SLA threshold per source
- Version controlled config: changes are reviewed, tracked, and rollback-able

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Corrupt/Bad Files

**Scenario:** 2% of incoming JSON files are malformed (invalid JSON, missing required fields, wrong types). Your pipeline shouldn't fail on bad files, but you need to: (1) process good records, (2) quarantine bad records, (3) alert if bad record rate exceeds 5%.

<details>
<summary>💡 Hint</summary>
Use PERMISSIVE mode with `_corrupt_record` column. Split the stream into good/bad paths. Write bad records to a DLQ table. Monitor the ratio.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, count, sum as spark_sum

# Read with corrupt record handling
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .load("s3://lake/landing/events/")
)

# Add metadata
raw_df = raw_df.withColumn("_source_file", input_file_name())

# Split: good records vs bad records
good_df = (raw_df
    .filter(col("_corrupt_record").isNull())
    .drop("_corrupt_record")
    .withColumn("_ingested_at", current_timestamp())
)

bad_df = (raw_df
    .filter(col("_corrupt_record").isNotNull())
    .select(
        col("_corrupt_record").alias("raw_content"),
        col("_source_file"),
        current_timestamp().alias("failed_at"),
        lit("json_parse_error").alias("error_type"),
    )
)

# Write good records to main table
(good_df.writeStream
    .option("checkpointLocation", "/checkpoints/events_good/")
    .trigger(availableNow=True)
    .toTable("production.bronze.events")
)

# Write bad records to quarantine/DLQ table
(bad_df.writeStream
    .option("checkpointLocation", "/checkpoints/events_dlq/")
    .trigger(availableNow=True)
    .toTable("production.bronze.events_dlq")
)

# Post-run: check error rate and alert
total_good = spark.sql("SELECT COUNT(*) FROM production.bronze.events WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR").collect()[0][0]
total_bad = spark.sql("SELECT COUNT(*) FROM production.bronze.events_dlq WHERE failed_at >= current_timestamp() - INTERVAL 1 HOUR").collect()[0][0]

error_rate = total_bad / max(total_good + total_bad, 1)
if error_rate > 0.05:
    # Alert: bad record rate exceeds 5%!
    send_alert(f"Data quality alert: {error_rate:.1%} of records are corrupt (threshold: 5%)")
```

**Key Points:**
- PERMISSIVE mode: doesn't fail on bad records — captures them in _corrupt_record
- Two separate streams (good + bad) with separate checkpoints: independent processing
- DLQ table enables investigation: what went wrong, which file, when
- Error rate monitoring: alert if source data quality degrades
- The pipeline NEVER stops due to bad data — it processes what it can
- Periodically: investigate DLQ, fix source issues, reprocess fixed files if needed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Schema Hints and Type Enforcement

**Scenario:** Auto Loader infers `user_id` as STRING (because it sees "12345" in JSON) but it should be BIGINT. The `event_timestamp` field is a string like "2024-03-15T10:30:00Z" that should be TIMESTAMP. How do you enforce correct types?

<details>
<summary>💡 Hint</summary>
Use `cloudFiles.schemaHints` to override inference for specific columns. Alternatively, provide a full schema with `schema()` method.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Method 1: Schema hints (override specific columns, infer the rest)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", """
        user_id BIGINT,
        event_timestamp TIMESTAMP,
        amount DECIMAL(10,2),
        properties MAP<STRING, STRING>,
        tags ARRAY<STRING>
    """)
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .load("s3://lake/landing/events/")
)
# user_id: forced to BIGINT (not inferred as STRING)
# event_timestamp: parsed as TIMESTAMP (Auto Loader handles ISO 8601 format)
# Other columns: still inferred normally

# Method 2: Provide full explicit schema (no inference at all)
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DecimalType

explicit_schema = StructType([
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("page_url", StringType(), True),
])

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(explicit_schema)  # No inference — use this exact schema
    .load("s3://lake/landing/events/")
)
# Faster (no schema inference step)
# But won't detect new columns — you must update schema manually
```

**Key Points:**
- Schema hints: best of both worlds — override known columns, infer unknown ones
- Full explicit schema: fastest (no inference), but you miss new columns
- For production: use schema hints for the columns you care about types on
- Common type issues: numeric IDs as STRING, timestamps as STRING, nested JSON as STRING
- Schema hints format: comma-separated `column_name TYPE` (same as SQL DDL)
- If inference gets a type wrong and you don't hint it: corrupt data in the Delta table!

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: High-Throughput Pipeline Design

**Scenario:** Design an Auto Loader pipeline for a real-time analytics platform: 200K events/second arriving as JSON files in S3 (files flushed every 30 seconds, ~6K files/minute). Requirements: p99 ingestion latency < 5 minutes, handle 3x traffic spikes, and cost under $3K/month.

<details>
<summary>💡 Hint</summary>
Notification mode (SQS) for instant detection, continuous trigger (not availableNow), auto-scaling cluster, output file compaction, and monitoring for backlog growth.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Architecture:
# 200K events/sec × 1KB avg = 200 MB/sec ingest rate
# Files flushed every 30s → each file ~6 MB, 6K files/minute = 360K files/hour

# Auto Loader configuration for high throughput:
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")      # SQS-based (instant detection)
    .option("cloudFiles.maxFilesPerTrigger", "2000")    # Process 2K files per micro-batch
    .option("cloudFiles.maxBytesPerTrigger", "12g")     # Cap at 12 GB per batch
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "event_ts TIMESTAMP, user_id BIGINT")
    .option("cloudFiles.schemaLocation", "/checkpoints/rt_events_schema/")
    .load("s3://lake/landing/rt-events/")
)

# Continuous processing (not batch — runs indefinitely)
(df
    .withColumn("_ingested_at", current_timestamp())
    .writeStream
    .option("checkpointLocation", "/checkpoints/rt_events/")
    .option("maxRecordsPerFile", "2000000")  # 2M records per output file (avoid small files)
    .trigger(processingTime="30 seconds")     # Micro-batch every 30 seconds
    .toTable("production.bronze.rt_events")
)

# Cluster configuration:
# - Instance type: i3.2xlarge (64 GB RAM, NVMe for shuffle)
# - Auto-scaling: min 8, max 24 workers
# - Driver: r5.4xlarge (128 GB — holds file state + checkpoint)
# - Spot instances for workers (70% savings, Auto Loader is fault-tolerant)

# Cost estimate:
# Workers: 12 avg × $0.624/hr (i3.2xlarge spot) × 730 hrs = $5,467... over budget!
# Optimize: use c5.2xlarge ($0.17/hr spot) = 12 × $0.17 × 730 = $1,489/mo
# Driver: 1 × r5.4xlarge ($0.50/hr) × 730 = $365/mo
# SQS: 360K msgs/hr × 730 hrs / 1M × $0.40 = $105/mo
# Delta storage: ~500 GB/month growth × $0.023 = $12/mo
# TOTAL: ~$2,000/mo ✓ (under $3K budget)

# 3x spike handling:
# Auto-scaling from 8 → 24 workers handles 3x throughput
# SQS buffers during scaling (messages are durable)
# maxFilesPerTrigger controls batch size (prevents OOM during spikes)

# p99 latency tracking:
# File lands → SQS notification (~1s) → next trigger starts (~30s max) → processing (~30s) → committed
# Expected p99: ~90 seconds (well under 5 minute requirement)
```

**Key Points:**
- Notification mode + continuous trigger = lowest latency (file detected within seconds)
- processingTime="30 seconds" balances latency vs efficiency (not too many small batches)
- Auto-scaling handles 3x spikes without over-provisioning base capacity
- Spot instances are safe: Auto Loader's checkpoint handles worker failures gracefully
- maxRecordsPerFile prevents small output files (downstream query performance)
- Monitor: files outstanding (backlog), processing time per batch, ingestion latency

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Auto Loader vs Custom Streaming

**Scenario:** Your team debates whether to use Auto Loader or build a custom Kafka-based pipeline for file ingestion. The custom approach would: producer publishes file paths to Kafka → consumer reads files → writes to Delta. Compare both approaches and recommend one.

<details>
<summary>💡 Hint</summary>
Auto Loader handles file discovery, dedup, schema evolution, and checkpointing out-of-the-box. Custom Kafka adds infrastructure but gives more control. Consider: what does Kafka add that Auto Loader doesn't?
</details>

<details>
<summary>✅ Solution</summary>

```python
# COMPARISON: Auto Loader vs Custom Kafka Pipeline

"""
AUTO LOADER:
- Discovery: Built-in (SQS notifications or directory listing)
- Dedup: Built-in (checkpoint state store)
- Schema: Built-in evolution with rescue/addNewColumns
- Exactly-once: Built-in (checkpoint + Delta transactions)
- Error handling: Built-in (PERMISSIVE mode, rescued data)
- Infra: Minimal (just the Databricks cluster)
- Lines of code: ~20

CUSTOM KAFKA:
- Discovery: You build it (Lambda triggered by S3 events → Kafka)
- Dedup: You build it (consumer-side offset management)
- Schema: You build it (or use Schema Registry)
- Exactly-once: You build it (offset commit + Delta transactions)
- Error handling: You build it (DLQ topic, retry logic)
- Infra: Kafka cluster + Lambda + monitoring + Schema Registry
- Lines of code: ~500+
"""

# When Custom Kafka IS justified:
# 1. You need to TRANSFORM file paths before processing (routing logic)
# 2. Multiple consumers need the same file events (fan-out)
# 3. You need backpressure control between file discovery and processing
# 4. You already have Kafka infrastructure and want consistency
# 5. Cross-platform: non-Databricks systems also consume file events

# When Auto Loader is the clear winner:
# 1. Standard file → Delta ingestion (90% of use cases)
# 2. Team doesn't want to maintain Kafka infrastructure
# 3. Schema evolution is important (Auto Loader handles it natively)
# 4. You want managed exactly-once without building it yourself
# 5. Cost-sensitive: Auto Loader has zero additional infra cost

# RECOMMENDATION for most teams: Auto Loader
# It solves the problem with 20 lines of code and zero additional infrastructure.
# Only add Kafka if you have a specific requirement Auto Loader can't meet.
```

**Key Points:**
- Auto Loader: 20 lines of code, zero infra, handles 95% of file ingestion use cases
- Custom Kafka: 500+ lines, Kafka cluster to maintain, justified only for advanced routing/fan-out
- Auto Loader's checkpoint IS equivalent to Kafka's consumer offsets (same purpose, simpler)
- Don't add infrastructure complexity without a clear requirement that demands it
- If you already have Kafka: consider Kafka Connect → S3 sink → Auto Loader (best of both worlds)
- The "build vs buy" decision: Auto Loader is the "buy" (Databricks manages the hard parts)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Checkpoint Recovery and Migration

**Scenario:** You need to: (1) migrate an Auto Loader pipeline from workspace A to workspace B (different metastore), (2) change the source path (S3 bucket moved), and (3) preserve exactly-once semantics (no reprocessing, no data loss). How?

<details>
<summary>💡 Hint</summary>
Checkpoint is the source of truth. You can copy it to the new location. The source path change is trickier — you may need a new checkpoint but can use the Delta table's history to avoid duplicates.
</details>

<details>
<summary>✅ Solution</summary>

```python
# SCENARIO: Migrating Auto Loader pipeline across workspaces/buckets

# Case 1: Same source bucket, different workspace
# The checkpoint is in cloud storage (not workspace-specific)
# Just point the new workspace's job to the SAME checkpoint path!
# → Exactly-once preserved (checkpoint knows what's been processed)

# Case 2: Source bucket changed (s3://old-bucket → s3://new-bucket)
# PROBLEM: Checkpoint tracks files by path — old paths won't match new files
# SOLUTION A: If files are the SAME (just moved), and naming is identical:
#   → Checkpoint tracks relative file names → may still work if structure matches

# SOLUTION B: Start fresh checkpoint but prevent reprocessing:
def migrate_with_no_duplicates(new_source: str, target_table: str):
    """Start new Auto Loader with dedup against existing target."""
    
    # Step 1: Get the latest file modification time already ingested
    last_ingested = spark.sql(f"""
        SELECT MAX(_file_modified_at) as cutoff
        FROM {target_table}
    """).collect()[0]["cutoff"]
    
    # Step 2: New Auto Loader with file filter (skip already-ingested files)
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{target_table}_v2_schema/")
        # Only process files modified AFTER the cutoff (skip old files)
        .option("modifiedAfter", last_ingested.strftime("%Y-%m-%dT%H:%M:%S"))
        .load(new_source)
    )
    
    (df.writeStream
        .option("checkpointLocation", f"/checkpoints/{target_table}_v2/")  # NEW checkpoint
        .trigger(availableNow=True)
        .toTable(target_table)
    )

# Case 3: Complete restart (reprocess everything) with dedup
def safe_reprocess(source: str, target_table: str):
    """Reprocess all files but skip records already in target."""
    
    # Auto Loader processes all files (new checkpoint)
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/checkpoints/reprocess_schema/")
        .load(source)
    )
    
    # Use MERGE to avoid duplicates (requires a primary key)
    def upsert_to_delta(batch_df, batch_id):
        batch_df.createOrReplaceTempView("updates")
        spark.sql(f"""
            MERGE INTO {target_table} t
            USING updates s ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    (df.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", "/checkpoints/reprocess/")
        .trigger(availableNow=True)
        .start()
    )
```

**Key Points:**
- Checkpoint is portable (cloud storage path, not tied to a workspace)
- Same source + same checkpoint = seamless migration across workspaces
- Different source = need a new checkpoint (old one tracks old file paths)
- Use `modifiedAfter` filter or MERGE to prevent duplicates during migration
- Always test migration in staging before production (verify row counts match)
- Document the checkpoint location — it's the most critical piece of pipeline state
- Never delete a checkpoint without understanding the consequences (full reprocess!)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cost Optimization

**Scenario:** Your Auto Loader cluster runs 24/7 (continuous trigger) ingesting from 5 sources. Monthly cost: $8K (cluster) + $500 (SQS/S3 APIs). Most sources only receive files during business hours (8 AM - 6 PM). Optimize to under $4K/month without increasing ingestion latency during business hours.

<details>
<summary>💡 Hint</summary>
Switch from continuous (24/7 cluster) to scheduled batch (trigger=availableNow) triggered by workflow. During off-hours: reduce frequency or shut down. Use auto-scaling aggressively.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT STATE: 24/7 cluster, continuous trigger, $8.5K/month
# PROBLEM: Paying for 24/7 compute when data only arrives 10 hours/day

# OPTIMIZATION 1: Switch to scheduled batch (biggest savings)
# Instead of always-on cluster, run every 15 minutes during business hours,
# every hour during off-hours

# Databricks Workflow with schedule:
# Business hours (8 AM - 6 PM): trigger every 15 minutes
# Off-hours (6 PM - 8 AM): trigger every 60 minutes
# Each run: cluster starts → processes files → cluster terminates

(df.writeStream
    .trigger(availableNow=True)  # Process all available then STOP
    .option("checkpointLocation", "/checkpoints/events/")
    .toTable("production.bronze.events")
)
# Cluster only runs during processing (maybe 2-5 minutes per run)
# Not 24/7 anymore!

# OPTIMIZATION 2: Share cluster across sources (multi-task workflow)
# One workflow → 5 parallel tasks → all share the same cluster
# Instead of 5 separate always-on clusters

# OPTIMIZATION 3: Right-size + spot instances
# Business hours cluster: 4 workers (spot) = sufficient for normal load
# Spikes handled by auto-scaling to 8 workers

# COST CALCULATION:
# Business hours: 10 hrs/day × 4 runs/hr × 5 min/run = 200 min/day of compute
# Off-hours: 14 hrs/day × 1 run/hr × 3 min/run = 42 min/day of compute
# Total: 242 min/day = 4 hours/day of cluster time

# Cluster cost: 4 workers × c5.2xlarge spot ($0.17/hr) + driver ($0.50/hr)
# 4 hrs/day × (4×$0.17 + $0.50) × 30 days = 4 × $1.18 × 30 = $142/month (workers)
# Wait, that's too low. Let's include cluster startup overhead:
# Each run: 3 min startup + 5 min processing = 8 min
# 34 runs/day × 8 min = 272 min = 4.5 hrs/day
# Cost: 4.5 × (4×$0.17 + $0.50) × 30 = $160/month... 

# Reality check: Databricks pricing is DBU-based
# c5.2xlarge = 1 DBU/hr, $0.40/DBU (all-purpose) or $0.15/DBU (jobs compute)
# Jobs compute: 5 nodes × 1 DBU × $0.15 × 4.5 hrs × 30 days = $101/month (Databricks)
# AWS: 5 × $0.17 (spot) × 4.5 × 30 = $115/month
# TOTAL: ~$216/month Databricks + ~$115 AWS = ~$331/month

# Hmm, that's unrealistically low. Let's be more realistic:
# With overhead (startup, shuffle, etc): ~$1,500/month for compute
# SQS/API costs remain: ~$500/month
# TOTAL: ~$2,000/month (76% savings from $8,500!)

# OPTIMIZATION 4: For the 2 real-time sources (IoT, events):
# Keep a small always-on cluster (2 workers) with continuous trigger
# Cost: 2 workers × 24/7 = ~$1,500/month
# Other 3 sources: scheduled batch = ~$500/month
# TOTAL: ~$2,500/month (71% savings, meets <$4K target) ✓
```

**Key Points:**
- Biggest savings: switch from 24/7 cluster to scheduled batch (cluster only runs during processing)
- `trigger(availableNow=True)` is key: process available files, then cluster can terminate
- SQS buffers notifications while cluster is off (no data loss during off-hours)
- For truly real-time sources: keep a minimal always-on cluster (small, spot instances)
- Multi-task workflow: share one cluster across multiple Auto Loader streams
- Jobs compute pricing (Databricks): 60-70% cheaper than all-purpose clusters
- Monitor ingestion latency: ensure SLA is still met with reduced frequency

</details>

</article>
