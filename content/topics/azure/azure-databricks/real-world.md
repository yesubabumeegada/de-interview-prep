---
title: "Azure Databricks — Real World"
topic: azure
subtopic: azure-databricks
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, databricks, production, delta-live-tables, streaming, medallion]
---

# Azure Databricks — Real World

## Pattern 1: Production Medallion Pipeline with Auto Loader

```python
# notebook: 01_ingest_bronze.py
# Triggered by Databricks Workflow at 2 AM daily

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

spark = SparkSession.builder.appName("IngestBronze").getOrCreate()

STORAGE_ACCOUNT = spark.conf.get("spark.myapp.storage_account")
RUN_DATE = dbutils.widgets.get("run_date")

def ingest_orders_to_bronze():
    """Incrementally ingest new order files from landing zone to Bronze Delta table."""
    
    # Auto Loader: only picks up new files (checkpointed)
    source_path = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/orders/"
    bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/orders/"
    checkpoint  = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze_orders/"
    
    schema = StructType([
        StructField("order_id",    LongType(),    False),
        StructField("customer_id", LongType(),    False),
        StructField("amount",      DecimalType(18,2), True),
        StructField("order_date",  StringType(),  True),
        StructField("region",      StringType(),  True),
        StructField("status",      StringType(),  True),
    ])
    
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("cloudFiles.maxFilesPerTrigger", 2000)
        .schema(schema)
        .load(source_path)
        .withColumn("ingest_date", F.current_date())
        .withColumn("source_file", F.input_file_name())
    )
    
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint}/ckpt")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)   # process all pending files then stop
        .table("prod.bronze.orders")
    )
    query.awaitTermination()
    log.info(f"Bronze ingestion complete for {RUN_DATE}")

ingest_orders_to_bronze()
```

---

## Pattern 2: Silver Transformation with SCD Type 2

```python
# notebook: 02_transform_customers_scd2.py
# Maintain SCD Type 2 history for customers table

from delta.tables import DeltaTable
from pyspark.sql import functions as F

def apply_scd2_customers(run_date: str):
    """Apply SCD Type 2 merge for customer dimension."""
    
    silver_table = DeltaTable.forName(spark, "prod.silver.dim_customers")
    
    # Read new/changed customers from Bronze
    new_customers = (
        spark.table("prod.bronze.customers")
        .filter(F.col("ingest_date") == run_date)
        .select(
            "customer_id", "name", "email", "region", "tier",
            F.lit(run_date).cast("date").alias("effective_from"),
            F.lit(None).cast("date").alias("effective_to"),
            F.lit(True).alias("is_current")
        )
    )
    
    # SCD2 merge:
    # 1. Expire old record if changed (set effective_to, is_current=False)
    # 2. Insert new record (is_current=True)
    silver_table.alias("target").merge(
        source=new_customers.alias("source"),
        condition="""
            target.customer_id = source.customer_id
            AND target.is_current = true
            AND (target.email != source.email 
                 OR target.region != source.region 
                 OR target.tier != source.tier)
        """
    ).whenMatchedUpdate(set={
        "effective_to": F.col("source.effective_from"),
        "is_current":   F.lit(False)
    }).execute()
    
    # Insert new/changed records
    # (all records in new_customers not matching current unchanged rows)
    existing_current = spark.table("prod.silver.dim_customers").filter("is_current = true")
    
    new_records = new_customers.join(
        existing_current.select("customer_id", "email", "region", "tier"),
        on=["customer_id"],
        how="left_anti"  # records not in existing_current → truly new
    ).union(
        new_customers.join(
            existing_current.select("customer_id"),
            on=["customer_id"],
            how="inner"
        ).filter("target.email != source.email OR ...")  # changed records
    )
    
    new_records.write.format("delta").mode("append").saveAsTable("prod.silver.dim_customers")
    
    print(f"SCD2 update complete: {new_records.count()} records inserted/expired")

apply_scd2_customers(RUN_DATE)
```

---

## Pattern 3: Streaming Fraud Detection Pipeline

```python
# Real-time fraud detection: Event Hubs → Databricks Structured Streaming → Delta

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

EVENTHUB_CONNECTION = dbutils.secrets.get("event-hubs", "fraud-connection-string")
CHECKPOINT = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/fraud_detection/"

# Event Hubs source
ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EVENTHUB_CONNECTION),
    'eventhubs.startingPosition': '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
}

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", TimestampType()),
])

# Read from Event Hubs
stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
    .select(F.from_json(F.col("body").cast("string"), transaction_schema).alias("data"))
    .select("data.*")
    .withWatermark("timestamp", "10 minutes")   # tolerate 10-min late data
)

# Fraud detection: window aggregation
fraud_alerts = (
    stream
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "user_id"
    )
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
        F.countDistinct("country").alias("unique_countries"),
        F.max("amount").alias("max_single_amount")
    )
    .filter(
        (F.col("txn_count") >= 10) |           # 10+ transactions in 5 min
        (F.col("unique_countries") >= 3) |     # 3+ countries in 5 min
        (F.col("total_amount") >= 10000)       # $10K+ in 5 min
    )
    .withColumn("alert_type",
        F.when(F.col("unique_countries") >= 3, "MULTI_COUNTRY")
         .when(F.col("txn_count") >= 10, "HIGH_FREQUENCY")
         .otherwise("HIGH_AMOUNT")
    )
    .withColumn("alert_time", F.current_timestamp())
)

# Write fraud alerts to Delta + Event Hubs for downstream action
fraud_alerts.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT) \
    .trigger(processingTime="30 seconds") \
    .table("prod.gold.fraud_alerts")
```

---

## Interview Tips

> **Tip 1:** "How do you handle failures in Databricks streaming jobs?" — Structured Streaming guarantees exactly-once processing via checkpointing. If a job fails mid-batch: on restart, Spark re-reads from the last committed checkpoint offset. For Event Hubs: the `eventhubs.startingPosition` in checkpoint tracks the last processed sequence number. For ADLS Auto Loader: the checkpoint tracks which files have been processed. Key: checkpoint location must be on durable storage (ADLS, not ephemeral cluster storage). Monitor: Databricks UI → Streaming tab shows batches processed, lag, and errors.

> **Tip 2:** "What's `trigger(availableNow=True)` and how is it different from batch mode?" — `trigger(availableNow=True)` runs a streaming job in "micro-batch" mode but processes all available data and stops. Unlike `trigger(once=True)` (deprecated), it processes multiple micro-batches until caught up. Unlike full batch mode: it still benefits from Auto Loader's file tracking and checkpointing (won't reprocess files already committed). Use case: scheduled daily ingestion — run at 2 AM, process all files since last run, stop. Cheaper than always-on streaming (no cluster idle time between runs).

> **Tip 3:** "How would you debug a Databricks job that runs fine in development but fails in production?" — Systematic approach: (1) Compare cluster configs (dev vs job cluster sizes, runtime versions). (2) Check production data volume — prod likely has 100× more data, triggering OOM. (3) Spark UI → Stages → find stage with most I/O or long GC time. (4) Check Delta table stats: `DESCRIBE DETAIL prod.silver.orders` — many small files? Run OPTIMIZE. (5) Check shuffle: if shuffle size in Spark UI shows GBs, increase shuffle partitions. (6) Check skew: Spark UI → Tasks → look for one task taking 10× longer (data skew in join key). Most common prod vs dev failures: OOM (size issue), timeout (skew issue), 403 (permission issue in VNet).
