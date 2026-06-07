---
title: "Delta Lake - Real-World Production Examples"
topic: databricks
subtopic: delta-lake
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, delta-lake, production, medallion, streaming, maintenance, scd]
---

# Delta Lake — Real-World Production Examples

## Pattern 1: Complete Medallion Architecture

```python
# BRONZE: Raw ingestion from Kafka → Delta (append-only, schema-on-read)
spark.readStream.format("kafka") \
    .option("subscribe", "raw.orders") \
    .load() \
    .selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS kafka_timestamp") \
    .writeStream.format("delta") \
    .option("checkpointLocation", "s3://lake/checkpoints/bronze_orders") \
    .trigger(processingTime="1 minute") \
    .start("s3://lake/bronze/orders")

# SILVER: Clean, validate, deduplicate
silver_stream = spark.readStream.format("delta") \
    .load("s3://lake/bronze/orders")

cleaned = silver_stream \
    .select(from_json(col("raw_json"), ORDER_SCHEMA).alias("data")) \
    .select("data.*") \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("processed_at", current_timestamp())

# Deduplicate using watermark + dropDuplicates
cleaned.withWatermark("event_timestamp", "10 minutes") \
    .dropDuplicatesWithinWatermark(["order_id"]) \
    .writeStream.format("delta") \
    .option("checkpointLocation", "s3://lake/checkpoints/silver_orders") \
    .trigger(processingTime="5 minutes") \
    .start("s3://lake/silver/orders")

# GOLD: Business aggregates (scheduled batch, not streaming)
# Run daily via Databricks Workflow
gold_daily = spark.read.format("delta") \
    .load("s3://lake/silver/orders") \
    .filter(col("order_date") == yesterday) \
    .groupBy("region", "product_category") \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers")
    )

gold_daily.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"report_date = '{yesterday}'") \
    .save("s3://lake/gold/daily_sales_summary")
```

---

## Pattern 2: SCD Type 2 with Delta MERGE

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col

def apply_scd_type2(spark, target_path, source_df, business_keys, tracked_columns):
    """
    SCD Type 2: Maintain full history of dimension changes.
    - New records → INSERT with is_current=True
    - Changed records → Close old row + INSERT new version
    - Unchanged records → No action
    """
    target = DeltaTable.forPath(spark, target_path)
    
    # Build conditions
    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
    change_condition = " OR ".join([f"t.{c} <> s.{c}" for c in tracked_columns])
    
    # Step 1: Close existing current rows that have changes
    target.alias("t").merge(
        source_df.alias("s"),
        f"{key_condition} AND t.is_current = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            "is_current": lit(False),
            "effective_to": current_timestamp(),
        }
    ).execute()
    
    # Step 2: Insert new version rows for changed records + brand new records
    # Find records that need insertion
    new_versions = source_df.alias("s").join(
        target.toDF().filter("is_current = false AND effective_to IS NOT NULL").alias("closed"),
        [col(f"s.{k}") == col(f"closed.{k}") for k in business_keys],
        "inner"
    ).select("s.*")
    
    brand_new = source_df.alias("s").join(
        target.toDF().alias("t"),
        [col(f"s.{k}") == col(f"t.{k}") for k in business_keys],
        "left_anti"
    )
    
    all_inserts = new_versions.union(brand_new) \
        .withColumn("is_current", lit(True)) \
        .withColumn("effective_from", current_timestamp()) \
        .withColumn("effective_to", lit(None).cast("timestamp"))
    
    all_inserts.write.format("delta").mode("append").save(target_path)

# Usage
apply_scd_type2(
    spark,
    target_path="s3://lake/silver/dim_customer",
    source_df=daily_customer_extract,
    business_keys=["customer_id"],
    tracked_columns=["name", "email", "segment", "city"]
)
```

---

## Pattern 3: Automated Table Maintenance

```python
# Schedule this as a daily Databricks Workflow job

from delta.tables import DeltaTable

TABLES_CONFIG = [
    {"path": "s3://lake/silver/orders", "zorder_cols": ["customer_id", "order_date"], "vacuum_hours": 168},
    {"path": "s3://lake/silver/events", "zorder_cols": ["user_id", "event_date"], "vacuum_hours": 72},
    {"path": "s3://lake/gold/daily_summary", "zorder_cols": ["report_date"], "vacuum_hours": 720},
]

def run_maintenance(spark, config):
    """Run OPTIMIZE, Z-ORDER, and VACUUM on a Delta table."""
    path = config["path"]
    dt = DeltaTable.forPath(spark, path)
    
    print(f"Maintaining: {path}")
    
    # Step 1: OPTIMIZE + Z-ORDER
    if config.get("zorder_cols"):
        spark.sql(f"""
            OPTIMIZE delta.`{path}` 
            ZORDER BY ({', '.join(config['zorder_cols'])})
        """)
        print(f"  Optimized with Z-ORDER on {config['zorder_cols']}")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        print(f"  Optimized (compaction only)")
    
    # Step 2: VACUUM old files
    dt.vacuum(config.get("vacuum_hours", 168))
    print(f"  Vacuumed (retention: {config.get('vacuum_hours', 168)} hours)")
    
    # Step 3: Collect metrics
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    history = dt.history(1).first()
    
    return {
        "table": path,
        "num_files": detail["numFiles"],
        "size_gb": round(detail["sizeInBytes"] / (1024**3), 2),
        "last_operation": history["operation"],
    }

# Run maintenance on all tables
results = [run_maintenance(spark, cfg) for cfg in TABLES_CONFIG]

# Report
for r in results:
    print(f"{r['table']}: {r['num_files']} files, {r['size_gb']} GB")
```

---

## Pattern 4: Data Quality with Expectations

```python
# Using Delta Live Tables (DLT) for quality-enforced pipelines

import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw orders from source system")
def bronze_orders():
    return spark.readStream.format("kafka").option("subscribe", "orders").load()

@dlt.table(comment="Cleaned and validated orders")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 1000000")
@dlt.expect_or_drop("not_null_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_date", "order_date <= current_date()")  # Warn but keep
def silver_orders():
    return dlt.read_stream("bronze_orders") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

@dlt.table(comment="Business-ready daily summary")
@dlt.expect_all_or_fail({
    "has_data": "order_count > 0",
    "reasonable_avg": "avg_amount BETWEEN 10 AND 10000"
})
def gold_daily_summary():
    return dlt.read("silver_orders") \
        .groupBy("order_date", "region") \
        .agg(count("*").alias("order_count"), avg("amount").alias("avg_amount"))
```

**Expectation behaviors:**

| Decorator | On Violation |
|-----------|-------------|
| `@dlt.expect("name", "condition")` | Record violation in metrics, keep row |
| `@dlt.expect_or_drop(...)` | Drop the bad row silently |
| `@dlt.expect_or_fail(...)` | Fail the entire pipeline (hard gate) |

---

## Pattern 5: Cross-Table Consistency with Multi-Table Transactions

```python
# Databricks Unity Catalog: multi-table atomic operations

# Scenario: Transfer inventory from warehouse A to B
# Must be atomic: deduct from A AND add to B, or neither

spark.sql("""
    BEGIN TRANSACTION;
    
    UPDATE inventory SET quantity = quantity - 100
    WHERE warehouse_id = 'A' AND product_id = 'P001';
    
    UPDATE inventory SET quantity = quantity + 100
    WHERE warehouse_id = 'B' AND product_id = 'P001';
    
    COMMIT;
""")
# If either UPDATE fails, both are rolled back
```

---

## Production Monitoring Queries

```sql
-- Table health overview
SELECT 
    path,
    numFiles,
    ROUND(sizeInBytes / POWER(1024, 3), 2) AS size_gb,
    ROUND(sizeInBytes / numFiles / POWER(1024, 2), 1) AS avg_file_mb,
    lastModified
FROM (DESCRIBE DETAIL delta.`s3://lake/silver/orders`);

-- Identify tables needing OPTIMIZE (avg file size < 128 MB)
-- Run across all tables in a schema

-- Operation history (last 7 days)
SELECT 
    version,
    timestamp,
    operation,
    operationMetrics.numOutputRows,
    operationMetrics.numTargetRowsUpdated,
    operationMetrics.numFiles
FROM (DESCRIBE HISTORY delta.`s3://lake/silver/orders`)
WHERE timestamp > current_date() - 7
ORDER BY version DESC;
```

---

## Interview Tips

> **Tip 1:** "Describe your Delta Lake pipeline architecture" — "Medallion pattern: Bronze (raw, append-only from streaming), Silver (cleaned, validated, deduplicated with schema enforcement), Gold (aggregated for BI). Streaming between Bronze→Silver using Delta-to-Delta streaming. Batch for Silver→Gold with partition overwrites. Nightly maintenance: OPTIMIZE + Z-ORDER + VACUUM."

> **Tip 2:** "How do you handle late-arriving data with Delta?" — "MERGE on the Silver table. Late records get upserted using the business key. Time travel lets me see what the table looked like before and after the late arrival. CDF (Change Data Feed) propagates the change to downstream Gold tables incrementally."

> **Tip 3:** "How do you ensure data quality in a Delta pipeline?" — "Three layers: (1) Schema enforcement rejects malformed data at write time. (2) CHECK constraints (amount > 0) catch business rule violations. (3) DLT Expectations with expect_or_fail for critical assertions (fail the pipeline) and expect_or_drop for non-critical (drop bad rows, log metrics). Post-load quality checks compare row counts against thresholds."
