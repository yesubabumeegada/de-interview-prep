---
title: "Auto Loader - Intermediate"
topic: databricks
subtopic: auto-loader
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, auto-loader, schema-evolution, partitioning, performance, rescue]
---

# Auto Loader — Intermediate

## Schema Evolution Strategies

### Handling New Columns Automatically

```python
# Production pattern: auto-add new columns without breaking the pipeline
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .option("cloudFiles.schemaHints", "event_timestamp TIMESTAMP, amount DOUBLE")
    .load("s3://bucket/landing/events/")
)

# With mergeSchema on the write side:
(df.writeStream
    .option("checkpointLocation", "/checkpoints/events/")
    .option("mergeSchema", "true")  # Allow schema evolution in Delta table
    .trigger(availableNow=True)
    .toTable("production.raw.events")
)

# Flow when a new field appears in source files:
# 1. Auto Loader detects new column in incoming file
# 2. Adds it to inferred schema (stored in schemaLocation)
# 3. DataFrame includes the new column
# 4. Delta table adds the column on write (mergeSchema=true)
# 5. No job failure, no manual intervention needed!
```

### Schema Hints (Type Override)

```python
# Problem: Auto Loader infers "12345" as STRING, but you know it's a LONG
# Solution: schema hints override inference for specific columns

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", """
        user_id BIGINT,
        event_timestamp TIMESTAMP,
        amount DECIMAL(10,2),
        properties MAP<STRING, STRING>
    """)
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://bucket/landing/")
)
# Hinted columns use your type; others are still inferred
```

### Rescued Data Pattern

```python
# Safest approach: rescue unexpected data instead of failing or losing it
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://bucket/landing/")
)

# Monitor rescued data for schema drift detection:
from pyspark.sql.functions import col, size

# Count rows with rescued data (indicates unexpected fields)
rescued_count = df.filter(col("_rescued_data").isNotNull()).count()
if rescued_count > 0:
    alert(f"{rescued_count} rows have unexpected fields — check source schema")
```

---

## Performance Optimization

### Partition-Based File Discovery

```python
# If files are organized by date partitions, optimize discovery:
# s3://bucket/landing/events/date=2024-03-01/file1.json
# s3://bucket/landing/events/date=2024-03-02/file2.json

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.partitionColumns", "date")  # Treat subdirs as partitions
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://bucket/landing/events/")
)
# Partition columns are automatically added to the DataFrame
# Delta table can also be partitioned by 'date' for aligned output
```

### Max Files Per Trigger

```python
# Control batch size to prevent memory issues with large backlogs
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxFilesPerTrigger", "1000")    # Max 1000 files per batch
    .option("cloudFiles.maxBytesPerTrigger", "10g")     # Max 10GB per batch
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://bucket/landing/")
)

# Use cases for limiting:
# - Initial backfill of 1M files: process in manageable batches
# - Prevent OOM when files are large (video, images)
# - Control processing time for SLA compliance
```

### Backfill vs Incremental

```python
# Scenario: 500K historical files + new files arriving daily

# The FIRST run processes the backfill (all existing files):
(df.writeStream
    .trigger(availableNow=True)  # Process ALL available files
    .option("checkpointLocation", "/checkpoints/events/")
    .option("cloudFiles.maxFilesPerTrigger", "5000")  # Batch size for backfill
    .toTable("production.raw.events")
)
# May run for hours (processes 500K files in batches of 5000)

# SUBSEQUENT runs only process NEW files (incremental):
# Same code, same checkpoint — Auto Loader knows what was already processed
(df.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", "/checkpoints/events/")  # SAME checkpoint!
    .toTable("production.raw.events")
)
# Runs in seconds/minutes (only new files since last run)
```

---

## File Notification Setup

### AWS (S3 + SQS)

```python
# Auto Loader auto-configures S3 event notifications + SQS queue
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    # Auto Loader creates:
    # 1. SQS queue (for receiving S3 events)
    # 2. S3 event notification (on the source bucket)
    # Requires IAM permissions: s3:PutBucketNotification, sqs:CreateQueue
    .load("s3://bucket/landing/events/")
)

# If you can't grant auto-setup permissions, pre-create resources:
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789/my-queue")
    .load("s3://bucket/landing/events/")
)
```

### Azure (ADLS + Event Grid)

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.resourceGroup", "my-resource-group")
    .option("cloudFiles.subscriptionId", "sub-id")
    .load("abfss://container@account.dfs.core.windows.net/landing/events/")
)
```

---

## Handling Bad Records

```python
# Option 1: Permissive mode (default) — bad records get null fields
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load("s3://bucket/landing/")
)

# Filter and route bad records separately:
good_records = df.filter(col("_corrupt_record").isNull())
bad_records = df.filter(col("_corrupt_record").isNotNull())

# Write good records to main table
(good_records.writeStream
    .option("checkpointLocation", "/checkpoints/good/")
    .toTable("production.raw.events")
)

# Write bad records to quarantine table for investigation
(bad_records.writeStream
    .option("checkpointLocation", "/checkpoints/bad/")
    .toTable("production.raw.events_quarantine")
)
```

---

## Transformations During Ingestion

```python
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    from_json, to_timestamp, regexp_extract
)

# Read raw files
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/")
    .load("s3://bucket/landing/events/")
)

# Apply transformations inline
transformed_df = (raw_df
    # Add ingestion metadata
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    
    # Type casting
    .withColumn("event_timestamp", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("amount", col("amount").cast("decimal(10,2)"))
    
    # Extract partition values from file path
    .withColumn("event_date", regexp_extract(input_file_name(), r"date=(\d{4}-\d{2}-\d{2})", 1))
    
    # Drop raw columns we don't need
    .drop("event_ts", "raw_metadata")
)

# Write transformed data
(transformed_df.writeStream
    .option("checkpointLocation", "/checkpoints/events_transformed/")
    .trigger(availableNow=True)
    .toTable("production.curated.events")
)
```

---

## Monitoring Auto Loader

```python
# Check stream progress
stream = (df.writeStream
    .option("checkpointLocation", "/checkpoints/events/")
    .trigger(availableNow=True)
    .toTable("production.raw.events")
)

# After execution: query metrics
metrics = stream.lastProgress
print(f"Files processed: {metrics['sources'][0]['numFilesOutstanding']}")
print(f"Rows processed: {metrics['numInputRows']}")
print(f"Processing time: {metrics['batchDuration']}ms")

# For ongoing streams: monitor via Spark UI or Delta table history
spark.sql("DESCRIBE HISTORY production.raw.events").show()
# Shows each write batch: timestamp, rows written, operation
```

---

## Interview Tips

> **Tip 1:** "How does Auto Loader handle schema evolution?" — Three modes: (1) `addNewColumns` automatically adds new fields to the schema and target table, (2) `rescue` puts unexpected fields in a `_rescued_data` column (safest), (3) `failOnNewColumns` stops the pipeline (strictest). For production, use `rescue` mode with monitoring on the rescued column.

> **Tip 2:** "How do you handle a backfill of 1M existing files?" — Same Auto Loader code with `trigger(availableNow=True)`. The first run processes all existing files (backfill). Set `maxFilesPerTrigger` to batch it (e.g., 5000 files per batch to avoid OOM). Subsequent runs only process new files — the checkpoint tracks what's been processed.

> **Tip 3:** "Notification mode vs directory listing — when to use each?" — Notification: production workloads needing near-instant ingestion (seconds). Auto Loader auto-configures SQS/EventGrid. Directory listing: development, cross-account scenarios where you can't set up notifications, or when files arrive infrequently (hourly batch drops). Notification scales better for millions of files.
