---
title: "Interview Scenarios - Senior Deep Dive"
topic: pyspark
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, interview, skew, scd2, udaf, broadcast, stream-batch-join, optimization]
---

# PySpark Interview Scenarios — Senior Deep Dive

## What Senior Interviews Test

Senior PySpark interviews go beyond "can you write a Window function?" They test whether you can:
- Identify and fix performance problems (skew, unnecessary shuffles, bad join strategies)
- Implement complex transformations without UDFs (SCD2, sessionization at scale)
- Reason about the execution plan and know when the optimizer needs help
- Design data pipelines, not just individual transformations

---

## Pattern 1: Skew Handling with Salt Keys

**The problem:** An orders table has 80% of rows with `customer_id = "GUEST"`. Any groupBy or join on `customer_id` creates a single massive partition that one executor must handle alone.

```python
from pyspark.sql.functions import col, concat_ws, rand, floor, lit, sum as spark_sum

# STEP 1: Identify skew
df.groupBy("customer_id").count().orderBy(col("count").desc()).show(5)
# +------------+----------+
# |customer_id |count     |
# +------------+----------+
# |GUEST       |400000000 |   ← 80% of 500M rows
# |C_10045     |1000      |
# ...

SALT_FACTOR = 50  # number of sub-partitions for skewed key

# STEP 2: Add a salt column to the skewed table
df_salted = df.withColumn(
    "salt",
    when(col("customer_id") == "GUEST",
         floor(rand() * SALT_FACTOR).cast("string"))   # random 0-49
    .otherwise(lit("0"))                                # non-skewed keys get salt=0
)

df_salted = df_salted.withColumn(
    "salted_key",
    concat_ws("_", col("customer_id"), col("salt"))
)

# STEP 3: Aggregate on the salted key
df_agg_partial = (
    df_salted
    .groupBy("salted_key", "customer_id")
    .agg(spark_sum("revenue").alias("revenue_partial"))
)

# STEP 4: Second aggregation to collapse salt (sum of sums = total)
df_agg_final = (
    df_agg_partial
    .groupBy("customer_id")
    .agg(spark_sum("revenue_partial").alias("total_revenue"))
)
```

**Why this works:** "GUEST" rows are distributed across 50 partitions (GUEST_0, GUEST_1, ... GUEST_49) so no single executor gets more than 2% of the data. The second aggregation collapses the partial sums.

**Trade-off:** Two aggregation passes instead of one. For commutative operations (sum, count, max, min) this is always correct. For non-commutative operations (average, median) you need to aggregate raw values first.

---

## Pattern 2: Optimizing a Slow Join

```python
# Scenario: large orders table (500GB) joining large customers table (100GB)
# Naive join causes a massive shuffle
df_result = orders.join(customers, on="customer_id", how="left")

# Optimization 1: Broadcast hint (if one side fits in memory, ~< 8GB)
from pyspark.sql.functions import broadcast
df_result = orders.join(broadcast(customers), on="customer_id", how="left")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 8 * 1024 * 1024 * 1024)  # 8GB

# Optimization 2: AQE skew join (Spark 3.0+, enabled by default)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE detects skewed partitions at runtime and splits them automatically

# Optimization 3: Repartition both sides on join key before joining
# (avoids repeat shuffles if table is used in multiple joins)
orders_repartitioned = orders.repartition(200, "customer_id")
customers_repartitioned = customers.repartition(200, "customer_id")
df_result = orders_repartitioned.join(customers_repartitioned, on="customer_id")
orders_repartitioned.cache()  # cache if used multiple times

# Inspect the physical plan to confirm join strategy
df_result.explain(mode="formatted")
# Look for: BroadcastHashJoin, SortMergeJoin, or ShuffledHashJoin
```

---

## Pattern 3: SCD Type 2 in PySpark (No Delta)

**SCD2:** For each record, track history with `valid_from`, `valid_to`, and `is_current` flag.

```python
from pyspark.sql.functions import col, lit, current_timestamp, lead, coalesce
from pyspark.sql.window import Window

def build_scd2(df_source):
    """
    Input: (customer_id, email, city, updated_at) — one row per change event
    Output: SCD2 table with (customer_id, email, city, valid_from, valid_to, is_current)
    """
    scd_window = Window.partitionBy("customer_id").orderBy("updated_at")
    
    df_scd = (
        df_source
        # valid_from = current record's updated_at
        .withColumn("valid_from", col("updated_at"))
        # valid_to = next record's updated_at, or null if current
        .withColumn(
            "valid_to",
            lead("updated_at").over(scd_window)
        )
        # is_current = True if valid_to is null (most recent record)
        .withColumn("is_current", col("valid_to").isNull())
        .select("customer_id", "email", "city", "valid_from", "valid_to", "is_current")
    )
    
    return df_scd

# To merge new updates with existing SCD2 table:
def merge_scd2(df_existing, df_new_records):
    """Expire changed records and insert new versions."""
    # Find keys that have changes in new records
    changed_keys = df_new_records.select("customer_id").distinct()
    
    # Expire current records for changed customers
    df_to_expire = (
        df_existing
        .join(broadcast(changed_keys), on="customer_id", how="inner")
        .filter(col("is_current") == True)
        .withColumn("is_current", lit(False))
        .withColumn("valid_to", col("updated_at_new"))  # requires join with new data
    )
    
    # Build new SCD2 rows for the changed customers
    df_new_scd2 = build_scd2(df_new_records)
    
    # Keep unchanged existing records + expired records + new records
    df_unchanged = df_existing.join(changed_keys, on="customer_id", how="left_anti")
    
    return df_unchanged.unionByName(df_to_expire).unionByName(df_new_scd2)
```

---

## Pattern 4: Stream-Batch Join (Enriching Streaming Events with Static Dimension)

```python
# Pattern: Kafka stream of events enriched with a slowly-changing dimension table

# Static dimension — reload periodically
customers_dim = spark.read.format("delta").load("/delta/dim/customers")
customers_dim.cache()  # cache in memory — this is read many times

# Streaming source
events_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "user_events")
    .load()
)

# Join stream with static dimension
# Note: batch DataFrame joined to streaming DataFrame — Spark handles this correctly
df_enriched = events_stream.join(
    broadcast(customers_dim),    # broadcast the smaller static side
    on="customer_id",
    how="left"
)

# Refresh the dimension periodically by restarting the query
# OR use foreachBatch to reload inside the micro-batch
def enrich_batch(batch_df, batch_id):
    # Reload dim on each micro-batch (for data freshness)
    dim = spark.read.format("delta").load("/delta/dim/customers")
    enriched = batch_df.join(broadcast(dim), on="customer_id", how="left")
    enriched.write.format("delta").mode("append").saveAsTable("silver.events_enriched")

query = events_stream.writeStream.foreachBatch(enrich_batch).start()
```

---

## Pattern 5: Processing 1TB Efficiently — Full Pipeline Design

```python
from pyspark.sql.functions import broadcast, col
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "2000")  # large for 1TB
    .getOrCreate()

# 1. Read with predicate pushdown (Parquet filter pushdown happens at file level)
df_orders = (
    spark.read
    .format("parquet")
    .load("s3://bucket/orders/")
    .filter(col("order_date") >= "2024-01-01")  # pushed down to Parquet scan
    .select("order_id", "customer_id", "product_id", "amount", "order_date")  # column pruning
)

# 2. Load small dimension tables — broadcast all of them
dim_customers = spark.read.format("delta").load("/delta/dim/customers").cache()
dim_products = spark.read.format("delta").load("/delta/dim/products").cache()

# 3. Join large table with broadcast dims (no shuffle for broadcast side)
df_enriched = (
    df_orders
    .join(broadcast(dim_customers), on="customer_id", how="left")
    .join(broadcast(dim_products), on="product_id", how="left")
)

# 4. Repartition on aggregation key BEFORE aggregation (avoids double shuffle)
df_enriched = df_enriched.repartition(2000, "customer_id")

# 5. Aggregate
df_agg = (
    df_enriched
    .groupBy("customer_id", "customer_segment")
    .agg(spark_sum("amount").alias("total_revenue"))
)

# 6. Persist intermediate result if used in multiple downstream operations
df_agg.cache()
df_agg.count()  # materialize the cache

# 7. Write partitioned output for downstream query efficiency
(
    df_agg
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("customer_segment")
    .option("compression", "zstd")   # better than snappy for analytical workloads
    .saveAsTable("gold.customer_revenue")
)
```

**Design summary for 1TB:**
- Column pruning + predicate pushdown at read time reduces data volume before processing.
- Broadcast all dimension tables (< 8GB) to eliminate shuffle on joins.
- Repartition before groupBy if you know the key distribution is uneven.
- AQE handles residual skew and coalesces small output partitions automatically.
- `zstd` compression gives better compression ratio than snappy with similar speed.

---

## Key Takeaways

- Salt keys are the manual skew fix; AQE's `skewJoin` is the automatic fix — combine both for worst-case skew.
- SCD2 without Delta is pure Window logic: `lead(updated_at)` gives `valid_to`, null `valid_to` = `is_current`.
- Stream-batch joins work best with `foreachBatch` + broadcasting the small dimension.
- For 1TB: predicate pushdown → column pruning → broadcast joins → repartition before agg → AQE.
- Always `explain(mode="formatted")` in an interview to show you know how to read execution plans.
