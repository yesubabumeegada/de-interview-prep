---
title: "Lakehouse Architecture — Real World"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [lakehouse, production, medallion, migration, patterns]
---

# Lakehouse Architecture — Real World

## Pattern 1: Warehouse-to-Lakehouse Migration

```
Situation: Team runs Snowflake + S3 data lake (separate pipelines).
Goal: Consolidate to lakehouse, reduce cost, unify ML and BI data.

Phase 1 — Foundation (Week 1–2):
  - Provision S3 bucket with lifecycle policies
  - Set up Databricks workspace or EMR cluster
  - Install Delta Lake: pip install delta-spark
  - Create Unity Catalog (Databricks) or Glue Catalog (AWS)
  - Define medallion zones: s3://bucket/{bronze,silver,gold}/

Phase 2 — Replicate (Week 3–4):
  - Run existing Snowflake tables to Delta via Spark export
  - Validate row counts: Snowflake count vs Delta count must match
  - Keep Snowflake running (parallel operation, no cutover yet)

Phase 3 — Redirect Pipelines (Week 5–8):
  - Move ingestion pipelines from "→ Snowflake" to "→ Delta (Silver)"
  - dbt models: point to Delta tables via Databricks SQL or SparkSQL
  - Run parallel validation: same dbt models on Snowflake vs Delta → diff results

Phase 4 — Cut Over BI (Week 9–10):
  - Update Tableau/Looker connections from Snowflake to Databricks SQL
  - Monitor query latency: Gold Delta tables should match Snowflake performance
  - Add OPTIMIZE + ZORDER for Gold tables used by dashboards

Phase 5 — Decommission (Week 11–12):
  - Keep Snowflake read-only for 30 days (rollback safety)
  - Validate no users still connecting to Snowflake
  - Suspend then delete Snowflake warehouse
  - Cost savings realized
```

---

## Pattern 2: Production Streaming Pipeline into Lakehouse

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

order_schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("status", StringType()) \
    .add("event_time", TimestampType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

parsed = kafka_df.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*") \
 .withColumn("_ingested_at", current_timestamp())

# Write to Bronze (append-only, exactly-once via checkpointing)
bronze_query = parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/orders_bronze") \
    .option("path", "s3://bucket/bronze/orders") \
    .trigger(processingTime="1 minute") \
    .start()

# Separate Silver job reads Bronze, applies MERGE
# (run as separate Spark Structured Streaming job)
def upsert_to_silver(batch_df, batch_id):
    from delta.tables import DeltaTable
    silver = DeltaTable.forPath(spark, "s3://bucket/silver/orders")
    silver.alias("existing").merge(
        batch_df.alias("new"),
        "existing.order_id = new.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

bronze_stream = spark.readStream.format("delta").load("s3://bucket/bronze/orders")
silver_query = bronze_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "s3://bucket/checkpoints/orders_silver") \
    .trigger(processingTime="5 minutes") \
    .start()
```

---

## Pattern 3: Monitoring Lakehouse Health

```python
# Check Delta table freshness and row counts across all zones
import boto3
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def lakehouse_health_check(spark):
    zones = {
        "bronze": "s3://bucket/bronze/orders",
        "silver": "s3://bucket/silver/orders",
        "gold":   "s3://bucket/gold/daily_revenue",
    }
    
    results = []
    for zone, path in zones.items():
        dt = DeltaTable.forPath(spark, path)
        history = dt.history(1).collect()[0]
        last_modified = history["timestamp"]
        row_count = spark.read.format("delta").load(path).count()
        
        results.append({
            "zone": zone,
            "last_modified": last_modified,
            "row_count": row_count,
            "version": history["version"],
            "operation": history["operation"],
        })
        print(f"{zone}: {row_count:,} rows, last modified {last_modified}, v{history['version']}")
    
    return results

# Check table file health
def check_small_files(spark, path: str, threshold: int = 128 * 1024 * 1024):
    """Alert if average file size < threshold (default 128MB)"""
    details = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    avg_file_size = details["sizeInBytes"] / max(details["numFiles"], 1)
    if avg_file_size < threshold:
        print(f"WARNING: avg file size {avg_file_size/1024/1024:.1f}MB < 128MB → run OPTIMIZE")
    else:
        print(f"OK: avg file size {avg_file_size/1024/1024:.1f}MB")
```

---

## Interview Tips

> **Tip 1:** "How do you convince leadership to migrate from Snowflake to a Lakehouse?" — Build the cost model: storage duplication × price per TB, pipeline maintenance hours × engineer cost, ML team blocked on data access issues. Then show the migration phases are low-risk (parallel operation, gradual cutover). Quantify: "This saves us $X/month and unblocks ML team's feature engineering." Avoid: "Snowflake is old technology" — frame it as business value, not technology novelty.

> **Tip 2:** "What's your runbook when a Bronze table is corrupted?" — Delta time-travel to the rescue: `RESTORE TABLE bronze.orders TO VERSION AS OF 42`. Bronze is append-only — corruption is usually a bad ingestion job. Check the Delta transaction log (`DESCRIBE HISTORY`) to find when the bad write happened. Roll back to prior version. Re-run the ingestion job after fixing the root cause. This is why Bronze is never deleted — it's your ultimate recovery point.

> **Tip 3:** "How do you handle backfill in a lakehouse?" — Bronze: re-ingest from source (using timestamp range). Silver: re-MERGE the backfilled Bronze records (idempotent). Gold: re-run with `replaceWhere` for the backfilled date range. Backfill should be a scheduled Spark job parameterized by `start_date`/`end_date`, not manual intervention. Delta's transaction log ensures concurrent reads see a consistent state during backfill.
