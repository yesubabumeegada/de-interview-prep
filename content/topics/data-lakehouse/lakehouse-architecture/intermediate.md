---
title: "Lakehouse Architecture — Intermediate"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [lakehouse, medallion, architecture, patterns, ingestion]
---

# Lakehouse Architecture — Intermediate

## Multi-Hop Medallion Pipeline

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, sha2, concat_ws

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Bronze: Raw ingest (append-only, no transforms) ──────────────────────────
def ingest_bronze(source_path: str, bronze_path: str):
    df = spark.read.json(source_path)
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", col("_metadata.file_path"))
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_path)

# ── Silver: Cleanse, validate, deduplicate ────────────────────────────────────
def bronze_to_silver(bronze_path: str, silver_path: str):
    df = spark.read.format("delta").load(bronze_path)
    
    # Deduplicate: keep latest record per order_id
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    w = Window.partitionBy("order_id").orderBy(desc("_ingested_at"))
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
    
    # Validate: drop records with null business keys
    df = df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
    
    # Type cast
    df = df.withColumn("amount", col("amount").cast("decimal(18,2)")) \
           .withColumn("order_date", col("order_date").cast("date"))
    
    # MERGE into Silver (upsert pattern for idempotency)
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver = DeltaTable.forPath(spark, silver_path)
        silver.alias("existing").merge(
            df.alias("new"),
            "existing.order_id = new.order_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").save(silver_path)

# ── Gold: Business aggregate ──────────────────────────────────────────────────
def silver_to_gold(silver_path: str, gold_path: str):
    df = spark.read.format("delta").load(silver_path)
    gold = df.groupBy("order_date", "product_category") \
             .agg(
                 {"amount": "sum", "order_id": "count"}
             ).withColumnRenamed("sum(amount)", "total_revenue") \
              .withColumnRenamed("count(order_id)", "order_count")
    gold.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"order_date = '{today}'") \
        .save(gold_path)
```

---

## Lakehouse Ingestion Patterns

```
Pattern 1: Batch Load (most common)
  Source → S3 (Bronze) → Spark transform → Silver → Gold
  Trigger: Airflow DAG, cron, Databricks Workflows
  Latency: hours
  Use case: daily ELT, historical backfill

Pattern 2: Micro-batch Streaming
  Kafka → Spark Structured Streaming (trigger=processingTime "5 minutes")
  Writes to Delta (streaming ACID)
  Latency: minutes
  Use case: near-real-time dashboards

Pattern 3: Continuous Streaming
  Kafka → Spark Structured Streaming (trigger=once / continuous)
  Latency: seconds
  Use case: fraud detection, real-time monitoring

Pattern 4: Change Data Capture (CDC)
  Debezium → Kafka → Spark Streaming → Delta MERGE
  Handles inserts, updates, deletes from source OLTP
  Latency: seconds
```

---

## Lakehouse vs Separate Lake + Warehouse (Total Cost Comparison)

```
Scenario: 10TB processed/day, 100TB stored

Two-tier (Data Lake + Snowflake):
  S3 storage: 100TB × $0.023/GB = $2,300/month
  Snowflake storage: 100TB × $23/TB = $2,300/month (compressed ≈ 30TB × $23 = $690)
  Snowflake compute: $5,000/month
  ETL pipeline (EMR/Glue): $1,500/month
  Total: ~$11,500/month

Lakehouse (S3 + Delta + Databricks/EMR):
  S3 storage only: 100TB × $0.023/GB = $2,300/month
  Databricks compute: $4,000/month (covers both ETL and serving)
  Total: ~$6,300/month
  
  Savings: ~$5,200/month = $62,400/year
  Additional benefit: no data staleness (no copy lag)

Note: Snowflake query performance remains superior for pure BI at scale.
Lakehouse wins when ML workloads are significant.
```

---

## Catalog Integration

```
Why a catalog is essential in a Lakehouse:
  Without catalog: Spark reads s3://bucket/raw/orders/2024/01/ — just a path
  With catalog: SELECT * FROM lakehouse.silver.orders → managed, discoverable

Unity Catalog (Databricks) — three-level namespace:
  catalog.schema.table
  lakehouse.silver.orders
  
  Benefits:
    - Column-level access control
    - Data lineage (table reads/writes tracked automatically)
    - Tag-based PII classification
    - Cross-workspace sharing

Glue Catalog (AWS):
  Tables registered: Glue crawler scans S3, registers schema
  Works with: Athena, EMR, Glue ETL, Redshift Spectrum
  
  Register Delta table in Glue:
  spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_db.orders
    USING delta
    LOCATION 's3://my-bucket/silver/orders'
  """)
```

---

## Interview Tips

> **Tip 1:** "How do you ensure idempotency in a medallion pipeline?" — Bronze is append-only (safe to rerun, duplicates handled in Silver). Silver uses MERGE (upsert) keyed on business key — re-running produces same result. Gold uses `replaceWhere` (partition overwrite) — re-running a specific date replaces only that partition. Result: all three layers are safe to rerun at any time.

> **Tip 2:** "How does streaming fit into the medallion pattern?" — The same Bronze→Silver→Gold layers apply to streaming. Bronze is the streaming ingest (Kafka → Delta with append). Silver is a streaming job that reads Bronze delta and does dedup/validation. Gold can be batch (triggered every N minutes on Silver changes) or streaming (aggregated with watermarks). Delta Lake's unified batch+streaming API makes this seamless.

> **Tip 3:** "What's the biggest operational challenge with a lakehouse?" — Small files problem. Every micro-batch streaming job creates new small Parquet files. Without compaction (Delta OPTIMIZE, Iceberg rewrite_data_files), read performance degrades badly. Production lakehouses need scheduled compaction jobs. Also: catalog management (Unity Catalog setup complexity), compute management (autoscaling clusters), and Z-order tuning for specific query patterns.
