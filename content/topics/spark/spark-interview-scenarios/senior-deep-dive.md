---
title: "Spark Interview Scenarios — Senior Deep Dive"
topic: spark
subtopic: spark-interview-scenarios
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, interview, design, large-scale, lambda-architecture, feature-engineering, exactly-once]
---

# Spark Interview Scenarios — Senior Deep Dive

## Scenario 1: Design a Feature Engineering Pipeline

**Question:** Design a Spark pipeline to compute ML features for 500M users. Features: 30-day purchase count, 90-day total spend, last purchase date, product category distribution. The pipeline must run daily with fresh features.

```python
from pyspark.sql import functions as F, Window

def compute_user_features(spark, run_date):
    # 1. Read events with partition pruning
    orders = (spark.read.parquet("s3://bucket/orders/")
        .filter(F.col("order_date") >= F.date_sub(F.lit(run_date), 90))
        .select("user_id", "amount", "order_date", "product_category"))

    # 2. Separate windows (avoids scanning 90d data for 30d features)
    orders_30d = orders.filter(F.col("order_date") >= F.date_sub(F.lit(run_date), 30))
    orders_90d = orders

    # 3. 30-day features
    feat_30d = orders_30d.groupBy("user_id").agg(
        F.count("*").alias("purchase_count_30d"),
        F.max("order_date").alias("last_purchase_date"),
    )

    # 4. 90-day features
    feat_90d = orders_90d.groupBy("user_id").agg(
        F.sum("amount").alias("total_spend_90d"),
    )

    # 5. Category distribution (array of top-3 categories)
    category_counts = orders_90d.groupBy("user_id", "product_category") \
        .count() \
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.desc("count")))) \
        .filter("rank <= 3")

    top_categories = category_counts.groupBy("user_id") \
        .agg(F.collect_list("product_category").alias("top_categories"))

    # 6. Join features (use 500M user table as left to preserve all users)
    users = spark.read.parquet("s3://bucket/users/").select("user_id")
    features = (users
        .join(feat_30d, "user_id", "left")
        .join(feat_90d, "user_id", "left")
        .join(top_categories, "user_id", "left")
        .fillna(0, subset=["purchase_count_30d", "total_spend_90d"])
        .fillna([], subset=["top_categories"])
    )

    # 7. Write to Feature Store (Delta with merge for idempotency)
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, "s3://bucket/features/users/"):
        DeltaTable.forPath(spark, "s3://bucket/features/users/") \
            .alias("t").merge(features.alias("s"), "t.user_id = s.user_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        features.write.format("delta").save("s3://bucket/features/users/")

# Performance notes for interviewer:
# - Partition pruning on order_date reduces scan from all-time to 90 days
# - Two separate aggregations avoid re-scanning for different windows
# - Delta MERGE ensures idempotent daily refresh
# - Expected: 500M users × 90d orders (~1B rows) → ~2 hours with 100 executors
```

---

## Scenario 2: Exactly-Once ETL from Kafka to Delta Lake

**Question:** Design an exactly-once streaming pipeline that reads orders from Kafka and writes to Delta Lake. Handle schema evolution and late data.

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Schema (V1)
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("schema_version", IntegerType()),   # for evolution
])

def process_batch(batch_df, batch_id):
    # Handle schema evolution: V2 adds 'discount_pct' field
    if "discount_pct" not in batch_df.columns:
        batch_df = batch_df.withColumn("discount_pct", F.lit(None).cast(DoubleType()))

    # Idempotent write via MERGE (batch_id + order_id as natural key)
    from delta.tables import DeltaTable
    target = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
    (target.alias("t")
        .merge(batch_df.alias("s"),
               "t.order_id = s.order_id AND t.event_time = s.event_time")
        .whenMatchedUpdate(set={
            "amount": "s.amount",
            "discount_pct": "s.discount_pct",
            "_batch_id": F.lit(batch_id)
        })
        .whenNotMatchedInsert(values={
            "order_id": "s.order_id",
            "customer_id": "s.customer_id",
            "amount": "s.amount",
            "discount_pct": "s.discount_pct",
            "event_time": "s.event_time",
            "_batch_id": F.lit(batch_id)
        })
        .execute())

stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "100000")
    .load()
    .select(F.from_json(F.col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "30 minutes"))

query = (stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3://bucket/checkpoints/orders/")
    .trigger(processingTime="1 minute")
    .start())

query.awaitTermination()
```

---

## Scenario 3: Debug a Production Job That's 5× Slower Than Yesterday

**Question:** A daily ETL job took 45 minutes yesterday, 4 hours today. Walk me through your debugging process.

**Systematic approach:**

```python
# Step 1: Check if input data size changed
# Yesterday: 50 GB; Today: check
today_size = spark.read.parquet("s3://bucket/raw/orders/date=2024-06-12/").count()
yesterday_size = spark.read.parquet("s3://bucket/raw/orders/date=2024-06-11/").count()
# If similar: not a data volume issue → proceed to Step 2

# Step 2: Check Spark UI for slow stages
# Open History Server → find today's app → Stages tab
# Question: which stage is slow?
# If stage with Exchange (shuffle) is slow → shuffle issue (skew or too much data)
# If non-shuffle stage is slow → GC, bad plan, or skew

# Step 3: Check for skew in slow stage
# If one task is 10× the median → skew
# Check: did a new customer with huge volume appear today?
orders.groupBy("customer_id").count().orderBy(F.desc("count")).show(5)

# Step 4: Check for plan regression (new data triggered different plan)
df.explain(mode="formatted")
# Look for unexpected join type changes (BHJ → SMJ)
# AQE may have downgraded broadcast if customers table grew > threshold

# Step 5: Check Executor metrics
# Spark UI → Executors tab → sort by GC time desc
# If GC% increased dramatically → executor memory pressure → check for data growth

# Step 6: Check data quality (new nulls/NaN affecting aggregations)
orders.filter(F.col("customer_id").isNull()).count()
# NULL keys all go to one partition → skew!
```

---

## Interview Tips

> **Tip 1:** "How do you structure a system design answer for a large-scale Spark pipeline?" — Cover: (1) data volume and velocity estimates; (2) partitioning strategy (what to partition by and why); (3) join strategy (broadcast vs sort-merge, bucketing); (4) incremental vs full refresh approach; (5) idempotency and fault tolerance; (6) output format and schema evolution; (7) monitoring and SLA. Interviewers want to see you think about edge cases — late data, schema changes, retries.

> **Tip 2:** "For the 'job is slow today' scenario — what's your first question?" — "Did the input data change?" Most slowdowns are input-driven: a new customer with 1M orders, a sudden null flood creating skew on the join key, or a 10× larger dataset hitting a plan that was tuned for yesterday's size. After data, check the plan — AQE's adaptive decisions are data-driven and can produce very different plans on different data volumes.

> **Tip 3:** "How do you make a foreachBatch write exactly-once?" — The checkpoint guarantees Spark replays the same batch_id on recovery. Make the write idempotent for that batch_id: Delta MERGE on natural business key handles duplicate inserts gracefully (second run updates what was inserted). For appends to non-transactional sinks: add a `_batch_id` column, delete records with that batch_id before inserting, then insert — a two-step idempotent upsert.
