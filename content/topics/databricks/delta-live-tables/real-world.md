---
title: "Delta Live Tables - Real-World Production Examples"
topic: databricks
subtopic: delta-live-tables
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, dlt, delta-live-tables, production, medallion, patterns]
---

# Delta Live Tables — Real-World Production Examples

## Pattern 1: Complete E-Commerce Pipeline

```python
import dlt
from pyspark.sql.functions import *

# ===== BRONZE LAYER =====

@dlt.table(comment="Raw orders from API landing zone")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/orders/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

@dlt.table(comment="Raw customer CDC from Debezium")
def bronze_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .load("/mnt/landing/cdc/customers/")
    )

# ===== SILVER LAYER =====

@dlt.table(comment="Cleaned orders with quality enforcement")
@dlt.expect_or_drop("valid_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("has_customer", "customer_id IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("bigint"),
            col("customer_id").cast("bigint"),
            col("amount").cast("decimal(10,2)"),
            to_date(col("order_date")).alias("order_date"),
            col("status"),
            col("_ingested_at"),
        )
        .dropDuplicates(["order_id"])
    )

# CDC-based customer dimension (SCD Type 1)
dlt.create_streaming_table("silver_customers")

@dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("op = 'd'"),
    stored_as_scd_type=1,
)

# ===== GOLD LAYER =====

@dlt.table(comment="Daily revenue by region for dashboards")
def gold_daily_revenue():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    
    return (
        orders.join(customers, "customer_id", "left")
        .groupBy("order_date", "region")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("revenue"),
            avg("amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
        )
    )

@dlt.table(comment="Customer lifetime value metrics")
def gold_customer_ltv():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("lifetime_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            datediff(max("order_date"), min("order_date")).alias("customer_age_days"),
        )
    )
```

---

## Pattern 2: IoT Sensor Pipeline (Continuous)

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(comment="Raw sensor readings from Kafka")
def bronze_sensor_readings():
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", spark.conf.get("kafka_brokers"))
        .option("subscribe", "iot.sensor.readings")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr(
            "CAST(key AS STRING) as sensor_id",
            "from_json(CAST(value AS STRING), schema) as data",
            "timestamp as kafka_timestamp"
        )
        .select("sensor_id", "data.*", "kafka_timestamp")
    )

@dlt.table(comment="Validated sensor data with outlier flagging")
@dlt.expect_or_drop("valid_sensor", "sensor_id IS NOT NULL")
@dlt.expect("reasonable_temp", "temperature BETWEEN -50 AND 150")
@dlt.expect("reasonable_humidity", "humidity BETWEEN 0 AND 100")
def silver_sensor_readings():
    return (
        dlt.read_stream("bronze_sensor_readings")
        .withColumn("reading_timestamp", to_timestamp(col("reading_ts")))
        .withColumn("temperature", col("temperature").cast("double"))
        .withColumn("humidity", col("humidity").cast("double"))
        .withColumn("is_outlier", 
            (col("temperature") < -40) | (col("temperature") > 130))
        .withWatermark("reading_timestamp", "5 minutes")
        .dropDuplicatesWithinWatermark(["sensor_id", "reading_timestamp"])
    )

@dlt.table(comment="5-minute aggregated sensor metrics")
def gold_sensor_5min():
    return (
        dlt.read_stream("silver_sensor_readings")
        .withWatermark("reading_timestamp", "10 minutes")
        .groupBy(
            window("reading_timestamp", "5 minutes"),
            "sensor_id"
        )
        .agg(
            avg("temperature").alias("avg_temp"),
            max("temperature").alias("max_temp"),
            min("temperature").alias("min_temp"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("reading_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sensor_id", "avg_temp", "max_temp", "min_temp",
            "avg_humidity", "reading_count",
        )
    )
```

---

## Pattern 3: Multi-Source Data Quality Hub

```python
import dlt
from pyspark.sql.functions import *

# Centralized quality tracking across all sources

@dlt.table(comment="Quality metrics from all pipelines")
def gold_data_quality_metrics():
    """Aggregate DQ metrics from event logs of all DLT pipelines."""
    return (
        spark.read.table("system.pipeline.events")
        .filter(col("event_type") == "flow_progress")
        .select(
            col("pipeline_id"),
            col("timestamp"),
            col("details.flow_definition.output_dataset").alias("table_name"),
            explode(col("details.flow_progress.data_quality.expectations")).alias("expectation"),
        )
        .select(
            "pipeline_id", "timestamp", "table_name",
            col("expectation.name").alias("check_name"),
            col("expectation.passed_records").alias("passed"),
            col("expectation.failed_records").alias("failed"),
        )
        .withColumn("pass_rate", col("passed") / (col("passed") + col("failed")))
    )

# Dashboard query: quality trends per table
# SELECT table_name, DATE(timestamp), AVG(pass_rate) 
# FROM gold_data_quality_metrics
# GROUP BY table_name, DATE(timestamp)
# ORDER BY DATE(timestamp)
```

---

## Pattern 4: DLT with Databricks Workflows

```python
# Orchestrate DLT pipeline as part of a larger workflow:
# Task 1: DLT pipeline (ingestion + transformation)
# Task 2: Data quality validation (Python notebook)
# Task 3: Downstream notifications (if quality passes)

# Task 2: Post-DLT quality validation notebook
def validate_pipeline_output():
    """Run after DLT pipeline completes."""
    
    # Check row counts
    orders_count = spark.table("production.silver.orders").filter(
        col("_ingested_at") >= current_date()
    ).count()
    
    if orders_count == 0:
        raise Exception("No new orders ingested today! Check source system.")
    
    # Check freshness
    latest = spark.sql("""
        SELECT MAX(_ingested_at) as latest FROM production.silver.orders
    """).collect()[0]["latest"]
    
    hours_stale = (datetime.now() - latest).total_seconds() / 3600
    if hours_stale > 2:
        raise Exception(f"Data is {hours_stale:.1f} hours stale (SLA: 2 hours)")
    
    # Check quality metrics from DLT event log
    quality = spark.sql("""
        SELECT AVG(pass_rate) as avg_quality
        FROM production.gold.data_quality_metrics
        WHERE table_name = 'silver_orders' AND DATE(timestamp) = current_date()
    """).collect()[0]["avg_quality"]
    
    if quality < 0.95:
        dbutils.notebook.exit(f"WARNING: Quality at {quality:.1%}, below 95% threshold")
    
    dbutils.notebook.exit(f"OK: {orders_count} orders, {quality:.1%} quality, {hours_stale:.1f}h fresh")
```

---

## Pattern 5: Migration from Manual ETL to DLT

```python
# BEFORE (manual Spark ETL — 200 lines of boilerplate):
"""
raw_df = spark.read.json("s3://...")
cleaned_df = raw_df.filter(...).withColumn(...)
cleaned_df.write.mode("overwrite").format("delta").saveAsTable("silver.orders")
# Manual: scheduling, error handling, checkpointing, quality checks...
"""

# AFTER (DLT — 30 lines of business logic):
import dlt

@dlt.table
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL AND amount > 0")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("bigint"),
            col("amount").cast("decimal(10,2)"),
            col("order_date").cast("date"),
        )
        .dropDuplicates(["order_id"])
    )

# DLT handles: scheduling, incremental processing, error recovery,
# schema evolution, optimization, monitoring — you only write the logic!

# MIGRATION STEPS:
# 1. Convert existing ETL logic into @dlt.table functions
# 2. Add expectations (data quality that was previously unchecked)
# 3. Deploy as DLT pipeline (same cluster sizing)
# 4. Validate: compare outputs between old and new
# 5. Decommission old ETL jobs
# 6. Enjoy: fewer lines, better quality, less maintenance
```

---

## Interview Tips

> **Tip 1:** "Walk me through a production DLT pipeline" — Bronze: Auto Loader ingests raw files (streaming table, append-only, schema evolution). Silver: type casting, dedup, quality expectations, CDC via apply_changes. Gold: business aggregations joined with dimensions. Deployment: Terraform-managed pipeline config, continuous mode for real-time, triggered for batch.

> **Tip 2:** "How do you monitor DLT in production?" — Event log provides: run duration, row counts per table, expectation pass/fail rates, and failure details. Build a gold table aggregating quality metrics across all pipelines. Dashboard shows: quality trends, freshness, and processing duration. Alert if: quality drops below 95%, pipeline fails, or freshness exceeds SLA.

> **Tip 3:** "How do you migrate existing ETL to DLT?" — Incremental approach: (1) Convert one pipeline at a time to DLT, (2) Run old and new in parallel, compare outputs, (3) Add expectations that codify quality rules previously unchecked, (4) Switch consumers to DLT output, (5) Decommission old pipeline. Typical result: 60-80% less code, better quality, automatic optimization.
