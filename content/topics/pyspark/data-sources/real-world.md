---
title: "PySpark Data Sources - Real-World Production Examples"
topic: pyspark
subtopic: data-sources
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, data-sources, production, s3, jdbc, multi-format]
---

# PySpark Data Sources — Real-World Production Examples

## Pattern 1: Optimal S3 Parquet Reading

**Problem:** 50,000 small files, full table scan for 2TB daily data.

```python
spark = (
    SparkSession.builder
    .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .config("spark.sql.parquet.filterPushdown", "true")
    .config("spark.sql.parquet.mergeSchema", "false")
    .getOrCreate()
)

# Explicit schema + partition pruning — only reads today's partition
df = (
    spark.read.schema(events_schema)
    .parquet("s3://datalake/events/")
    .filter("year = '2024' AND month = '01' AND day = '15'")
)
```

### File Compaction (Nightly Job)

```python
def compact_partition(spark, path, target_size_mb=256):
    df = spark.read.parquet(path)
    total_bytes = df.count() * 200  # Estimate bytes/row
    num_files = max(1, int(total_bytes / 5 / (target_size_mb * 1024 * 1024)))
    df.repartition(num_files).write.mode("overwrite").option("compression", "zstd").parquet(path)
```

---

## Pattern 2: Parallel JDBC Extraction (500M Rows)

```python
# Step 1: Get bounds
bounds = spark.read.format("jdbc").option("url", url).option(
    "query", "SELECT MIN(id), MAX(id) FROM orders").load().collect()[0]

# Step 2: Parallel extraction (20 connections)
orders_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://prod-db:5432/warehouse")
    .option("dbtable", "orders")
    .option("partitionColumn", "id")
    .option("lowerBound", str(bounds[0]))
    .option("upperBound", str(bounds[1] + 1))
    .option("numPartitions", "20")
    .option("fetchSize", "50000")
    .options(**{"user": "spark_etl", "password": "secret"})
    .load()
)

# Step 3: Write partitioned output
(orders_df
    .withColumn("order_year", orders_df["order_date"].substr(1, 4))
    .write.mode("overwrite")
    .partitionBy("order_year")
    .option("compression", "zstd")
    .parquet("s3://datalake/raw/orders/"))
```

For skewed data, use date-based predicates instead of numeric ranges:
```python
predicates = [f"order_date >= '{y}-{m:02d}-01' AND order_date < '{y}-{m+1:02d}-01'"
              for y in [2023, 2024] for m in range(1, 12)]
df = spark.read.jdbc(url=url, table="orders", predicates=predicates, properties=props)
```

---

## Pattern 3: Multi-Format Pipeline (JSON → Parquet → Delta)

```python
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws

# BRONZE: Raw JSON ingestion
bronze_df = (
    spark.read.schema(raw_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt")
    .json("s3://raw/transactions/2024/01/15/")
    .withColumn("_ingestion_ts", current_timestamp())
)
bronze_df.write.mode("append").parquet("s3://datalake/bronze/transactions/dt=2024-01-15/")

# SILVER: Cleaned Parquet
silver_df = (
    spark.read.parquet("s3://datalake/bronze/transactions/dt=2024-01-15/")
    .filter(col("_corrupt").isNull() & (col("amount") > 0))
    .withColumn("row_hash", sha2(concat_ws("||", "transaction_id", "amount"), 256))
    .drop("_corrupt")
)
silver_df.write.mode("overwrite").partitionBy("currency").parquet("s3://datalake/silver/transactions/")

# GOLD: Delta serving layer
silver_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3://lakehouse/gold/transactions/")
```

---

## Pattern 4: Streaming-to-Batch Bridging

```python
from pyspark.sql.functions import from_json, col, to_date

# Batch read from Kafka (catch-up or backfill)
kafka_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "page_views")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# Deserialize and write to Delta
parsed = (
    kafka_df
    .select(from_json(col("value").cast("string"), page_view_schema).alias("e"))
    .select("e.*")
    .withColumn("event_date", to_date((col("event_time") / 1000).cast("timestamp")))
)
parsed.write.format("delta").mode("append").partitionBy("event_date").save("s3://lakehouse/page_views/")
```

---

## Performance Comparison by Format

| Format | Write Speed | Read Speed | Compression | ACID | Best For |
|--------|-------------|------------|-------------|------|----------|
| CSV | Fast | Slow | 2-3x | No | Data exchange |
| JSON | Fast | Slow | 2-3x | No | API/log data |
| Parquet | Medium | Fast | 5-10x | No | Analytics |
| Delta | Medium | Fast | 5-10x | Yes | Lakehouse |
| Avro | Fast | Medium | 3-5x | No | Streaming/Kafka |

---

## Interview Tips

> **Tip 1:** "Walk me through a multi-format pipeline." — "Raw JSON lands in bronze (Parquet) for durability. Silver applies cleaning and deduplication. Gold uses Delta for ACID, time travel, and serving BI tools. Each layer improves quality and query performance."

> **Tip 2:** "How do you extract a 500M-row table?" — "Parallel JDBC with partitionColumn on an indexed numeric column. Query MIN/MAX for bounds, set numPartitions to 20-40 (respecting DB pool), fetchSize to 50K. For skewed data, use date-based custom predicates."

> **Tip 3:** "How do you handle the small file problem?" — "Prevention: coalesce/repartition before writing targeting 128-256MB files. Cure: nightly compaction job that rewrites partitions with optimal file count. Delta Lake's OPTIMIZE command does this automatically."
