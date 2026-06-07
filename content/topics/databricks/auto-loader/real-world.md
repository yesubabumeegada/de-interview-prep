---
title: "Auto Loader - Real-World Production Examples"
topic: databricks
subtopic: auto-loader
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, auto-loader, production, pipeline, medallion, ingestion]
---

# Auto Loader — Real-World Production Examples

## Pattern 1: Medallion Architecture Ingestion Layer

```python
from pyspark.sql.functions import current_timestamp, input_file_name, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# BRONZE LAYER: Raw ingestion with Auto Loader
# Files land in S3 from multiple sources (APIs, CDC, partners)

def ingest_to_bronze(source_path: str, target_table: str, file_format: str = "json"):
    """Standard bronze ingestion pattern with Auto Loader."""
    
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{target_table.replace('.','/')}_schema/")
        .option("cloudFiles.useNotifications", "true")
        .load(source_path)
    )
    
    # Add bronze metadata columns
    bronze_df = (raw_df
        .withColumn("_bronze_ingested_at", current_timestamp())
        .withColumn("_bronze_source_file", input_file_name())
        .withColumn("_bronze_file_modification_time", col("_metadata.file_modification_time"))
    )
    
    # Write to bronze Delta table
    (bronze_df.writeStream
        .option("checkpointLocation", f"/checkpoints/{target_table.replace('.','/')}/")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

# Deploy for each source:
ingest_to_bronze("s3://lake/landing/orders/", "production.bronze.orders", "json")
ingest_to_bronze("s3://lake/landing/events/", "production.bronze.events", "json")
ingest_to_bronze("s3://lake/landing/partner/", "production.bronze.partner_data", "csv")
```

---

## Pattern 2: CDC Ingestion from Debezium

```python
# Debezium CDC exports Avro files to S3 (one file per Kafka flush)
# Files contain: before, after, op, ts_ms fields

cdc_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.schemaLocation", "/checkpoints/orders_cdc_schema/")
    .load("s3://lake/cdc/debezium/orders/")
)

# Parse CDC payload
from pyspark.sql.functions import col, when, struct

parsed_cdc = (cdc_df
    .select(
        col("after.order_id").alias("order_id"),
        col("after.customer_id").alias("customer_id"),
        col("after.amount").alias("amount"),
        col("after.status").alias("status"),
        col("after.updated_at").alias("updated_at"),
        col("op").alias("cdc_operation"),  # c=create, u=update, d=delete
        current_timestamp().alias("_ingested_at"),
    )
)

# Write to bronze CDC table (append-only log of all changes)
(parsed_cdc.writeStream
    .option("checkpointLocation", "/checkpoints/orders_cdc/")
    .trigger(availableNow=True)
    .toTable("production.bronze.orders_cdc")
)

# Downstream: MERGE into silver table to get latest state
# (Handled by a separate silver job, not Auto Loader)
```

---

## Pattern 3: Multi-Format Landing Zone

```python
# Real scenario: partners send data in different formats
# partner_a: CSV with headers
# partner_b: JSON (nested)
# partner_c: Parquet (clean)

class LandingZoneIngester:
    """Manage Auto Loader streams for multiple source formats."""
    
    def __init__(self):
        self.sources = [
            {
                "name": "partner_a_orders",
                "path": "s3://lake/landing/partner_a/orders/",
                "format": "csv",
                "options": {"header": "true", "delimiter": ",", "inferSchema": "true"},
                "target": "production.bronze.partner_a_orders",
            },
            {
                "name": "partner_b_events",
                "path": "s3://lake/landing/partner_b/events/",
                "format": "json",
                "options": {"multiLine": "true"},
                "target": "production.bronze.partner_b_events",
            },
            {
                "name": "partner_c_products",
                "path": "s3://lake/landing/partner_c/products/",
                "format": "parquet",
                "options": {},
                "target": "production.bronze.partner_c_products",
            },
        ]
    
    def run_all(self):
        """Process all landing zone sources."""
        for source in self.sources:
            reader = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", source["format"])
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("cloudFiles.schemaLocation", f"/checkpoints/{source['name']}_schema/")
            )
            
            for key, value in source["options"].items():
                reader = reader.option(key, value)
            
            df = reader.load(source["path"])
            
            # Standard metadata enrichment
            df = (df
                .withColumn("_source", lit(source["name"]))
                .withColumn("_ingested_at", current_timestamp())
                .withColumn("_source_file", input_file_name())
            )
            
            (df.writeStream
                .option("checkpointLocation", f"/checkpoints/{source['name']}/")
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
                .toTable(source["target"])
            )
            
            print(f"Completed: {source['name']}")

# Run as a Databricks Workflow task
ingester = LandingZoneIngester()
ingester.run_all()
```

---

## Pattern 4: Error Handling and Dead Letter Queue

```python
from pyspark.sql.functions import col, lit, current_timestamp, struct, to_json

def ingest_with_dlq(source_path: str, target_table: str, dlq_table: str):
    """Ingest with automatic dead letter queue for bad records."""
    
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{target_table}_schema/")
        .load(source_path)
    )
    
    # Split good and bad records
    good_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    bad_df = raw_df.filter(col("_corrupt_record").isNotNull())
    
    # Write good records to target
    (good_df
        .withColumn("_ingested_at", current_timestamp())
        .writeStream
        .option("checkpointLocation", f"/checkpoints/{target_table}_good/")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    
    # Write bad records to DLQ table (for investigation)
    (bad_df
        .select(
            col("_corrupt_record"),
            input_file_name().alias("source_file"),
            current_timestamp().alias("failed_at"),
            lit("parse_error").alias("error_type"),
        )
        .writeStream
        .option("checkpointLocation", f"/checkpoints/{target_table}_dlq/")
        .trigger(availableNow=True)
        .toTable(dlq_table)
    )

# Usage:
ingest_with_dlq(
    "s3://lake/landing/events/",
    "production.bronze.events",
    "production.bronze.events_dlq"
)

# Monitor DLQ:
# SELECT COUNT(*) FROM production.bronze.events_dlq WHERE failed_at >= current_date()
# Alert if > 1% of records are failing
```

---

## Pattern 5: Databricks Workflow Integration

```python
# auto_loader_task.py — run as a Databricks Workflow task

import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    
    # Parameters from workflow (passed as widgets or task values)
    source_path = dbutils.widgets.get("source_path")
    target_table = dbutils.widgets.get("target_table")
    file_format = dbutils.widgets.get("file_format")
    
    # Run Auto Loader
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{target_table}_schema/")
        .option("cloudFiles.useNotifications", "true")
        .load(source_path)
    )
    
    enriched = df.withColumn("_ingested_at", current_timestamp())
    
    query = (enriched.writeStream
        .option("checkpointLocation", f"/checkpoints/{target_table}/")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    
    query.awaitTermination()
    
    # Report metrics
    metrics = query.lastProgress
    print(f"Files processed: {metrics.get('numInputRows', 0)} rows")
    
    # Set task value for downstream tasks
    dbutils.jobs.taskValues.set(key="rows_ingested", value=metrics.get("numInputRows", 0))

if __name__ == "__main__":
    main()

# Databricks Workflow configuration:
# Task 1: Auto Loader (this script) — triggered on schedule or file arrival
# Task 2: Silver transformation — depends on Task 1
# Task 3: Gold aggregation — depends on Task 2
# Task 4: Data quality checks — depends on Task 3
```

---

## Interview Tips

> **Tip 1:** "How do you build a production ingestion layer with Auto Loader?" — Medallion pattern: Auto Loader feeds bronze (raw, append-only, schema on read). Separate jobs transform bronze → silver (cleaned, typed, deduped) → gold (business-ready aggregates). Auto Loader handles exactly-once ingestion; downstream jobs handle business logic.

> **Tip 2:** "How do you handle bad/corrupt records?" — PERMISSIVE mode captures corrupt records in a `_corrupt_record` column. Split the stream: good records → main table, bad records → DLQ table. Monitor DLQ rate. Investigate and fix source issues. Reprocess fixed records from DLQ into the main table.

> **Tip 3:** "How do you handle multiple data sources with different formats?" — One Auto Loader stream per source (separate checkpoints, separate schemas). Centralize configuration (source path, format, target table). Manage all streams in a single Databricks Workflow with parallel tasks. Common metadata columns (_ingested_at, _source, _source_file) across all targets.
