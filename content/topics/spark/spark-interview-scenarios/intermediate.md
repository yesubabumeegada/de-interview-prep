---
title: "Spark Interview Scenarios — Intermediate"
topic: spark
subtopic: spark-interview-scenarios
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, interview, scenarios, skew-handling, incremental-etl, scd-type-2, sessionization]
---

# Spark Interview Scenarios — Intermediate

## Scenario 1: Handle Skewed Data in a Join

**Question:** You're joining a 500 GB orders table with a 2 GB customers table. Most orders belong to 5 corporate accounts. Your join is taking 3 hours. Fix it.

**Diagnosis:**
```python
# Step 1: Confirm skew
orders.groupBy("customer_id").count() \
    .orderBy(F.desc("count")).show(10)
# customer_id | count
# CORP001     | 45_000_000   ← this is the problem
# CORP002     | 30_000_000
# C000123     | 42
```

**Solution A: AQE (automatic, Spark 3.0+)**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Spark detects CORP001 partition is 5× median → splits it automatically
result = orders.join(customers, "customer_id")   # no code change needed
```

**Solution B: Salting (pre-3.0 or extreme skew)**
```python
n_salts = 20  # enough to split the hot partition into 20 sub-partitions

# 1. Add salt to the large side (random)
orders_salted = orders.withColumn("_salt", (F.rand() * n_salts).cast("int"))

# 2. Explode the small side (replicate n_salts times)
from pyspark.sql.functions import explode, array, lit
customers_salted = customers.withColumn("_salt",
    explode(array([lit(i) for i in range(n_salts)])))

# 3. Join on original key + salt
result = orders_salted.join(customers_salted,
    (orders_salted.customer_id == customers_salted.customer_id) &
    (orders_salted._salt == customers_salted._salt)
).drop("_salt")
```

**Follow-up: "When would you use salting vs AQE skew join?"**
- Use AQE first (Spark 3.0+) — zero code change, automatic
- Use salting when: Spark < 3.0, skew is extreme (one key has 99% of data), AQE's split-and-duplicate overhead is too high

---

## Scenario 2: Incremental ETL with Watermarking

**Question:** Design an incremental ETL that processes only new/changed orders daily without reprocessing the entire history (500 GB).

```python
from pyspark.sql import functions as F

# Strategy: high-watermark pattern
# Watermark table tracks last processed timestamp per source

def run_incremental_etl(spark):
    # Read last watermark
    last_watermark = spark.sql("""
        SELECT COALESCE(MAX(watermark_ts), '1970-01-01') as last_ts
        FROM etl_watermarks
        WHERE table_name = 'orders'
    """).collect()[0]["last_ts"]

    # Read only new/changed records
    new_orders = (spark.read.parquet("s3://bucket/raw/orders/")
        .filter(F.col("updated_at") > last_watermark))

    if new_orders.count() == 0:
        print("No new data, skipping")
        return

    # Process
    processed = transform(new_orders)

    # Write with dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    processed.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("s3://bucket/processed/orders/")

    # Update watermark (use MAX of processed batch, not current time)
    new_watermark = new_orders.agg(F.max("updated_at")).collect()[0][0]
    spark.sql(f"""
        INSERT INTO etl_watermarks VALUES
        ('orders', TIMESTAMP '{new_watermark}', current_timestamp())
    """)

run_incremental_etl(spark)
```

**Pitfalls to mention:**
1. Using current_time() as watermark instead of max(updated_at) — misses late-arriving records
2. Not handling deletes (use soft deletes with is_deleted flag)
3. Missing `partitionOverwriteMode=dynamic` — overwrites all partitions instead of just changed ones

---

## Scenario 3: SCD Type 2 in Spark

**Question:** Implement a Type 2 Slowly Changing Dimension — when a customer record changes, close the old record and insert a new one.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Source: new/updated customer records
updates = spark.read.parquet("s3://bucket/customer-updates/")

# Target: Delta table with SCD Type 2 columns
# Schema: customer_id, name, email, tier, eff_start, eff_end, is_current

target = DeltaTable.forPath(spark, "s3://bucket/delta/dim_customers/")

# Step 1: Expire current records that have changed
target.alias("t").merge(
    updates.alias("u"),
    "t.customer_id = u.customer_id AND t.is_current = TRUE"
).whenMatchedUpdate(
    condition="""
        t.name != u.name OR t.email != u.email OR t.tier != u.tier
    """,
    set={
        "eff_end": "u.updated_at",
        "is_current": "false"
    }
).execute()

# Step 2: Insert new versions for changed records
changed = updates.join(
    spark.table("dim_customers").filter("is_current = false AND eff_end = updated_at"),
    "customer_id"
)

new_rows = updates.join(changed.select("customer_id"), "customer_id") \
    .withColumn("eff_start", F.col("updated_at")) \
    .withColumn("eff_end", F.lit("9999-12-31").cast("date")) \
    .withColumn("is_current", F.lit(True))

new_rows.write.format("delta").mode("append").save("s3://bucket/delta/dim_customers/")
```

---

## Scenario 4: Sessionize Clickstream Events

**Question:** Given a stream of page view events (user_id, page, event_time), group events into sessions. A session ends after 30 minutes of inactivity.

```python
from pyspark.sql import functions as F, Window

# Batch approach (for historical analysis):
w = Window.partitionBy("user_id").orderBy("event_time")

df = (raw_events
    .withColumn("prev_event_time", F.lag("event_time", 1).over(w))
    .withColumn("gap_minutes",
        (F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time")) / 60)
    .withColumn("is_new_session",
        (F.col("gap_minutes") > 30) | F.col("gap_minutes").isNull())
    .withColumn("session_id",
        F.sum(F.col("is_new_session").cast("int")).over(
            w.rowsBetween(Window.unboundedPreceding, 0)))
    # session_id is now a monotonically increasing integer per user
    .withColumn("session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_id").cast("string")))
)

# Aggregate sessions
sessions = df.groupBy("user_id", "session_id") \
    .agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count("*").alias("page_views"),
        F.collect_list("page").alias("pages_visited")
    ) \
    .withColumn("duration_min",
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60)
```

**Streaming approach:**
```python
# session_window (Spark 3.2+)
from pyspark.sql.functions import session_window

sessions = (raw_stream
    .withWatermark("event_time", "40 minutes")  # watermark > session gap
    .groupBy(F.col("user_id"), session_window(F.col("event_time"), "30 minutes"))
    .agg(F.count("*").alias("page_views")))
```

---

## Interview Tips

> **Tip 1:** "How do you approach a data skew problem in an interview?" — Start by diagnosing: show how to detect skew (groupBy + count on the join key, check Spark UI for one task 10× longer). Then present solutions in order of preference: AQE automatic skew join (Spark 3.0+, zero code change) → salting (manual, more complex, needed for extreme skew or older Spark) → filter skew keys separately (if they're known and can be handled differently).

> **Tip 2:** "What watermark should you use for incremental ETL?" — Use `MAX(source.updated_at)` from the current batch, not `current_timestamp()`. If you use current_time, you risk missing records that were committed to the source just before your watermark update but have `updated_at` slightly earlier. Using the max of actual source timestamps is safe: every record with `updated_at > last_watermark` will be captured in the next run.

> **Tip 3:** "What are the alternatives to implementing SCD Type 2 manually in Spark?" — Delta Lake MERGE handles the core merge logic; you still need to manage the row closing and new-row insertion. dbt snapshots automate the full SCD2 lifecycle — define the snapshot config, run `dbt snapshot`. For Databricks specifically, Delta Live Tables (DLT) has a built-in SCD Type 2 API: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`. Choose the tool based on your stack — dbt for transformation-layer SCD, Delta MERGE for custom ETL jobs.
