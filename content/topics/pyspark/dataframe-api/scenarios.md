---
title: "PySpark DataFrame API - Scenario Questions"
topic: pyspark
subtopic: dataframe-api
content_type: scenario_question
tags: [pyspark, dataframe, spark, interview, scenarios, etl]
---

# Scenario Questions — PySpark DataFrame API

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Read CSV, Filter, and Write Parquet

**Scenario:** You receive a daily CSV dump of sales transactions. Read it into a DataFrame, filter for rows where `amount > 100`, and write the result as a partitioned Parquet file by `region`.

<details>
<summary>💡 Hint</summary>
Use `spark.read.csv` with `header=True` and `inferSchema=True`, then chain `.filter()` and `.write.partitionBy()`.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("sales_filter").getOrCreate()

df = spark.read.csv("s3://bucket/sales/daily.csv", header=True, inferSchema=True)

filtered = df.filter(col("amount") > 100)

filtered.write.partitionBy("region").mode("overwrite").parquet("s3://bucket/output/sales/")
```

**Explanation:**
- `inferSchema=True` avoids all columns being read as strings (Spark samples data to detect types)
- `col("amount") > 100` creates a Column expression evaluated lazily across all partitions
- `partitionBy("region")` creates subdirectories like `region=US/`, `region=EU/` for partition pruning
- `mode("overwrite")` replaces existing data — use `"append"` for incremental loads

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Join Two DataFrames

**Scenario:** You have a `transactions` DataFrame and a `customers` DataFrame. Join them on `customer_id` to enrich transactions with customer names and segments.

<details>
<summary>💡 Hint</summary>
Use `.join()` with a join condition and specify the join type. Watch out for duplicate column names after joining.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("join_example").getOrCreate()

transactions = spark.read.parquet("s3://bucket/transactions/")
customers = spark.read.parquet("s3://bucket/customers/")

enriched = transactions.join(
    customers.select("customer_id", "name", "segment"),
    on="customer_id",
    how="left"
)

enriched.write.parquet("s3://bucket/enriched_transactions/")
```

**Explanation:**
- `on="customer_id"` uses a string when the column name is the same in both DataFrames (avoids duplicate columns)
- `how="left"` keeps all transactions even if no customer match (nulls for unmatched)
- `.select()` on customers limits columns brought in — avoids shuffling unnecessary data
- Left join is standard for enrichment; inner join would silently drop unmatched records

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Group By and Aggregate

**Scenario:** Given an orders DataFrame with columns `product_id`, `quantity`, and `price`, compute the total revenue and order count per product.

<details>
<summary>💡 Hint</summary>
Use `.groupBy()` followed by `.agg()` with multiple aggregate functions from `pyspark.sql.functions`.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder.appName("aggregation").getOrCreate()

orders = spark.read.parquet("s3://bucket/orders/")

summary = orders.groupBy("product_id").agg(
    spark_sum(col("quantity") * col("price")).alias("total_revenue"),
    count("*").alias("order_count")
)

summary.orderBy(col("total_revenue").desc()).show(10)
```

**Explanation:**
- `groupBy("product_id")` triggers a shuffle to co-locate all rows with the same key
- `spark_sum` is aliased to avoid shadowing Python's built-in `sum`
- `.alias()` names the output column — without it you get auto-generated names like `sum((quantity * price))`
- `.orderBy(...desc())` sorts descending to see top products first

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Add Derived Columns with withColumn

**Scenario:** Your raw events DataFrame has a `timestamp` column (string format `"2024-01-15 10:30:00"`). Add columns for `event_date`, `event_hour`, and a boolean `is_weekend`.

<details>
<summary>💡 Hint</summary>
Use `to_timestamp()` to parse the string, then `to_date()`, `hour()`, and `dayofweek()` for extraction.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, hour, dayofweek

spark = SparkSession.builder.appName("derived_columns").getOrCreate()

events = spark.read.parquet("s3://bucket/raw_events/")

enriched = (
    events
    .withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("event_date", to_date(col("ts")))
    .withColumn("event_hour", hour(col("ts")))
    .withColumn("is_weekend", dayofweek(col("ts")).isin(1, 7))
)

enriched.drop("ts").write.parquet("s3://bucket/enriched_events/")
```

**Explanation:**
- `to_timestamp` parses string → TimestampType using a Java SimpleDateFormat pattern
- `dayofweek` returns 1=Sunday, 7=Saturday (Spark convention)
- `.isin(1, 7)` creates a boolean column — True for weekend days
- Chaining `.withColumn()` is readable but each call creates a new DataFrame (Spark optimizes this internally)

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Handle Nulls and Duplicates

**Scenario:** Your user activity DataFrame has null `email` values and duplicate rows from upstream retries. Clean the data by dropping rows where `email` is null and deduplicating by `user_id` keeping the most recent record.

<details>
<summary>💡 Hint</summary>
Use `.dropna()` for null handling and a window function with `row_number()` for deduplication.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("clean_data").getOrCreate()

raw = spark.read.parquet("s3://bucket/user_activity/")

# Drop rows with null email
non_null = raw.filter(col("email").isNotNull())

# Deduplicate: keep most recent per user_id
window = Window.partitionBy("user_id").orderBy(col("event_time").desc())

deduped = (
    non_null
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

deduped.write.parquet("s3://bucket/clean_activity/")
```

**Explanation:**
- `col("email").isNotNull()` filters out nulls without dropping rows missing other fields
- `row_number()` assigns 1 to the most recent row per `user_id` (ordered by `event_time` descending)
- Filtering `rn == 1` keeps only the latest record per user
- This pattern is more flexible than `.dropDuplicates()` which doesn't let you control which row to keep

</details>
</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Sessionization with Window Functions

**Scenario:** Given a clickstream DataFrame with `user_id` and `event_time`, assign a `session_id` to each event. A new session starts when the gap between consecutive events exceeds 30 minutes.

<details>
<summary>💡 Hint</summary>
Use `lag()` to get the previous event time, compute the gap, flag new sessions with a boolean, then `sum()` the flags as a running total to generate session IDs.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, sum as spark_sum, when, concat_ws
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sessionization").getOrCreate()

clicks = spark.read.parquet("s3://bucket/clickstream/")

user_window = Window.partitionBy("user_id").orderBy("event_time")

sessionized = (
    clicks
    .withColumn("prev_time", lag("event_time").over(user_window))
    .withColumn("gap_seconds", unix_timestamp("event_time") - unix_timestamp("prev_time"))
    .withColumn("new_session", when(col("gap_seconds") > 1800, 1).otherwise(0))
    .withColumn("session_num", spark_sum("new_session").over(user_window))
    .withColumn("session_id", concat_ws("_", col("user_id"), col("session_num")))
    .drop("prev_time", "gap_seconds", "new_session", "session_num")
)

sessionized.write.parquet("s3://bucket/sessionized_clicks/")
```

**Explanation:**
- `lag("event_time")` gets the previous row's timestamp within the same user partition
- Gap > 1800 seconds (30 min) flags a new session boundary
- Cumulative `sum` of the flags creates incrementing session numbers per user
- `concat_ws` builds a unique session ID like `"user123_3"` (user's 3rd session)
- This is the standard sessionization pattern used at scale in web analytics

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: SCD Type 2 Merge with DataFrame API

**Scenario:** Implement a Slowly Changing Dimension Type 2 merge. Given a `current` dimension table and incoming `updates`, close expired records and insert new versions while maintaining `effective_date` and `end_date` columns.

<details>
<summary>💡 Hint</summary>
Split logic into: unchanged rows, rows to close (update end_date), and new versions to insert. Union all three.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when

spark = SparkSession.builder.appName("scd2_merge").getOrCreate()

current = spark.read.parquet("s3://bucket/dim_customer/")  # has: id, name, city, effective_date, end_date, is_current
updates = spark.read.parquet("s3://bucket/staging/customer_updates/")

# Rows not affected by updates (no matching id in updates)
unchanged = current.join(updates, on="id", how="left_anti")

# Close existing records that have updates
closed = (
    current.join(updates.select("id"), on="id", how="inner")
    .withColumn("end_date", current_date())
    .withColumn("is_current", lit(False))
)

# New versions from updates
new_versions = (
    updates
    .withColumn("effective_date", current_date())
    .withColumn("end_date", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

result = unchanged.unionByName(closed).unionByName(new_versions)
result.write.mode("overwrite").parquet("s3://bucket/dim_customer/")
```

**Explanation:**
- `left_anti` join finds rows in `current` with no match in `updates` (unchanged records)
- Closed records get today's date as `end_date` and `is_current=False`
- New versions start with today's `effective_date` and null `end_date`
- `unionByName` handles column order differences between DataFrames
- Production systems typically use Delta Lake's `MERGE INTO` for ACID guarantees

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Schema Evolution — Handling New Columns

**Scenario:** Your pipeline reads daily Parquet files, but upstream started adding new columns mid-month. Write code that reads all files with schema merging and fills missing columns with nulls.

<details>
<summary>💡 Hint</summary>
Parquet supports schema merging natively in Spark via the `mergeSchema` option.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name

spark = SparkSession.builder.appName("schema_evolution").getOrCreate()

# Read with schema merging — fills missing columns with null
df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet("s3://bucket/daily_events/2024-01-*")
)

# Optionally: track which file each record came from
df_with_source = df.withColumn("source_file", input_file_name())

# Enforce expected schema — add defaults for known columns that may be missing
expected_cols = ["user_id", "event_type", "timestamp", "platform", "app_version"]
for c in expected_cols:
    if c not in df.columns:
        df = df.withColumn(c, lit(None).cast("string"))

df.select(expected_cols).write.mode("append").parquet("s3://bucket/unified_events/")
```

**Explanation:**
- `mergeSchema=true` unions all file schemas — columns not present in older files become null
- `input_file_name()` is useful for debugging which source file a row came from
- The explicit column check ensures downstream consumers always get a consistent schema
- For Delta Lake: `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Broadcast Join Optimization

**Scenario:** You're joining a 500GB fact table with a 50MB lookup table. The join is slow due to a full shuffle. Optimize it using a broadcast join and verify it's being applied.

<details>
<summary>💡 Hint</summary>
Use `broadcast()` to hint Spark to send the small table to all executors. Check the physical plan with `.explain()`.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder.appName("broadcast_join").getOrCreate()

# Large fact table — 500GB, many partitions
fact = spark.read.parquet("s3://bucket/fact_orders/")

# Small lookup table — 50MB
lookup = spark.read.parquet("s3://bucket/dim_product/")

# Force broadcast of small table — eliminates shuffle on fact table
enriched = fact.join(broadcast(lookup), on="product_id", how="left")

# Verify broadcast is applied (look for BroadcastHashJoin in plan)
enriched.explain(mode="formatted")

# Tune the auto-broadcast threshold (default is 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")

enriched.write.parquet("s3://bucket/enriched_orders/")
```

**Explanation:**
- `broadcast()` sends the entire small table to every executor's memory (no shuffle needed)
- Eliminates the expensive shuffle of the 500GB fact table — only the 50MB table is transferred
- `explain()` should show `BroadcastHashJoin` instead of `SortMergeJoin`
- `autoBroadcastJoinThreshold` controls when Spark auto-broadcasts (set to -1 to disable)
- Rule of thumb: broadcast tables under 100MB; larger causes executor OOM

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Incremental Processing with Watermarks

**Scenario:** Your pipeline runs hourly. Instead of reprocessing all data, read only new files that arrived since the last run. Use a high-water mark pattern stored in a metadata table.

<details>
<summary>💡 Hint</summary>
Store the last processed timestamp, filter for files/records newer than that mark, then update the watermark after successful processing.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit
from datetime import datetime

spark = SparkSession.builder.appName("incremental_load").getOrCreate()

# Read last watermark from metadata
watermark_df = spark.read.parquet("s3://bucket/metadata/watermarks/")
last_watermark = watermark_df.filter(col("pipeline") == "orders").select("last_ts").collect()[0][0]

# Read only new data beyond watermark
new_data = (
    spark.read.parquet("s3://bucket/raw_orders/")
    .filter(col("ingestion_time") > lit(last_watermark))
)

if new_data.head(1):  # Check if there's new data
    # Process new records
    processed = new_data.transform(apply_business_logic)
    processed.write.mode("append").parquet("s3://bucket/processed_orders/")

    # Update watermark to max ingestion_time of processed batch
    new_watermark = new_data.agg(spark_max("ingestion_time")).collect()[0][0]
    (
        spark.createDataFrame([("orders", new_watermark)], ["pipeline", "last_ts"])
        .write.mode("overwrite").parquet("s3://bucket/metadata/watermarks/")
    )
```

**Explanation:**
- High-water mark pattern: store the max timestamp of the last processed batch
- Filter pushdown on `ingestion_time` leverages Parquet/Delta predicate pushdown for efficiency
- `new_data.head(1)` is a cheap existence check (reads minimal data)
- Watermark update happens only after successful write (crash-safe ordering)
- Delta Lake's `DESCRIBE HISTORY` or `_delta_log` can replace manual watermarks

</details>
</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Handling Data Skew in Joins

**Scenario:** Your join on `customer_id` takes 10x longer than expected because 5% of customers generate 80% of orders (skew). The largest partition runs for hours while others finish in minutes. Fix this without losing correctness.

<details>
<summary>💡 Hint</summary>
Use salting: append a random suffix to the skewed key, replicate the small-side table for each salt value, join on the salted key, then drop the salt.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, floor, rand, explode, array

spark = SparkSession.builder.appName("skew_handling").getOrCreate()

SALT_BUCKETS = 10

orders = spark.read.parquet("s3://bucket/orders/")       # Large, skewed on customer_id
customers = spark.read.parquet("s3://bucket/customers/")  # Small dimension

# Salt the large side: append random bucket to key
salted_orders = orders.withColumn(
    "salted_key", concat(col("customer_id"), lit("_"), floor(rand() * SALT_BUCKETS).cast("int"))
)

# Explode the small side: replicate each row for every salt bucket
salted_customers = (
    customers
    .withColumn("salt", explode(array(*[lit(i) for i in range(SALT_BUCKETS)])))
    .withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt")))
    .drop("salt")
)

# Join on salted key — distributes skewed partitions across SALT_BUCKETS
result = salted_orders.join(salted_customers, on="salted_key", how="left").drop("salted_key")

result.write.parquet("s3://bucket/enriched_orders/")
```

**Explanation:**
- Salting splits the hot partition into N smaller partitions (10x reduction in largest partition)
- The small side is replicated N times — acceptable since it's small (10× 50MB = 500MB total)
- `floor(rand() * N)` distributes large-side rows randomly across salt buckets
- Result is identical to a normal join — salt is only used for distribution, then dropped
- Alternative: Spark 3.x AQE (Adaptive Query Execution) with `skewJoin.enabled=true`

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Exactly-Once Semantics with Idempotent Writes

**Scenario:** Your Spark Streaming job writes to a Delta table. Due to executor failures, some micro-batches may retry. Implement exactly-once semantics ensuring no duplicate records even on retries.

<details>
<summary>💡 Hint</summary>
Use Delta Lake's `MERGE` with a composite deduplication key, combined with Structured Streaming's checkpoint-based recovery.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName("exactly_once") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

def upsert_to_delta(micro_batch_df, batch_id):
    """Idempotent write: MERGE ensures exactly-once even on retry."""
    target = DeltaTable.forPath(spark, "s3://bucket/delta/events/")

    (
        target.alias("t")
        .merge(
            micro_batch_df.alias("s"),
            "t.event_id = s.event_id AND t.source_system = s.source_system"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# Structured Streaming with checkpointing + idempotent sink
(
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "s3://bucket/checkpoints/events/")
    .trigger(processingTime="1 minute")
    .start()
)
```

**Explanation:**
- `MERGE` with a composite key (`event_id + source_system`) is idempotent — re-inserting the same row is a no-op
- `whenNotMatchedInsertAll()` only inserts truly new records
- Structured Streaming's checkpoint tracks which offsets are committed — on retry, it replays the exact same micro-batch
- Checkpoint + idempotent MERGE = exactly-once end-to-end
- Without Delta MERGE, you'd need manual dedup with a staging table pattern

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Production ETL Class with Retry and Metrics

**Scenario:** Design a reusable PySpark ETL base class that handles: configuration management, automatic retries with exponential backoff, data quality assertions, and execution metrics logging.

<details>
<summary>💡 Hint</summary>
Use an abstract base class with `extract()`, `transform()`, `load()` hooks. Wrap execution in retry logic and collect metrics via accumulators.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod
import time
import logging

class SparkETLBase(ABC):
    def __init__(self, app_name: str, max_retries: int = 3):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.max_retries = max_retries
        self.metrics = {"rows_read": 0, "rows_written": 0, "duration_s": 0}
        self.logger = logging.getLogger(app_name)

    @abstractmethod
    def extract(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        pass

    def validate(self, df: DataFrame) -> DataFrame:
        """Override for custom quality checks. Raise on failure."""
        assert df.count() > 0, "Empty DataFrame — aborting"
        null_pct = df.select([
            (col(c).isNull().cast("int")).alias(c) for c in df.columns
        ]).agg(*[spark_sum(col(c)).alias(c) for c in df.columns])
        return df

    def run(self):
        start = time.time()
        for attempt in range(1, self.max_retries + 1):
            try:
                raw = self.extract()
                self.metrics["rows_read"] = raw.count()
                transformed = self.transform(raw)
                self.validate(transformed)
                self.load(transformed)
                self.metrics["rows_written"] = transformed.count()
                break
            except Exception as e:
                wait = 2 ** attempt
                self.logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s")
                if attempt == self.max_retries:
                    raise
                time.sleep(wait)
        self.metrics["duration_s"] = round(time.time() - start, 2)
        self.logger.info(f"ETL complete: {self.metrics}")

# Usage
from pyspark.sql.functions import col, spark_sum

class OrdersETL(SparkETLBase):
    def extract(self) -> DataFrame:
        return self.spark.read.parquet("s3://bucket/raw_orders/")
    def transform(self, df: DataFrame) -> DataFrame:
        return df.filter(col("status") == "completed")
    def load(self, df: DataFrame) -> None:
        df.write.mode("append").parquet("s3://bucket/processed_orders/")

OrdersETL("orders_pipeline").run()
```

**Explanation:**
- Abstract base class enforces consistent ETL structure across all pipelines
- Exponential backoff (2^attempt seconds) prevents thundering herd on transient failures
- `validate()` hook is a quality gate — pipeline aborts if data is malformed
- Metrics tracking enables monitoring and alerting on row counts, duration, and failures
- This pattern is the foundation of frameworks like Optimus, Atlan, and custom ETL platforms

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Data Reconciliation Between Source and Target

**Scenario:** After an ETL run, validate that no records were lost or duplicated. Write a reconciliation check comparing source and target by row counts, key uniqueness, and aggregate checksums.

<details>
<summary>💡 Hint</summary>
Compare counts, check for key duplicates, and use hash-based checksums on critical columns to verify data integrity end-to-end.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, md5, concat_ws, sum as spark_sum, lit

spark = SparkSession.builder.appName("reconciliation").getOrCreate()

def reconcile(source: str, target: str, key_col: str, check_cols: list[str]) -> dict:
    src = spark.read.parquet(source)
    tgt = spark.read.parquet(target)

    report = {}

    # 1. Row count comparison
    src_count = src.count()
    tgt_count = tgt.count()
    report["count_match"] = src_count == tgt_count
    report["source_count"] = src_count
    report["target_count"] = tgt_count

    # 2. Key uniqueness check
    tgt_dupes = tgt.groupBy(key_col).agg(count("*").alias("cnt")).filter(col("cnt") > 1).count()
    report["target_duplicate_keys"] = tgt_dupes

    # 3. Missing keys (in source but not in target)
    missing = src.select(key_col).subtract(tgt.select(key_col)).count()
    report["missing_in_target"] = missing

    # 4. Checksum comparison on critical columns
    checksum_expr = md5(concat_ws("|", *[col(c).cast("string") for c in check_cols]))
    src_checksum = src.withColumn("row_hash", checksum_expr).agg(spark_sum(col("row_hash").substr(1, 8).cast("long"))).collect()[0][0]
    tgt_checksum = tgt.withColumn("row_hash", checksum_expr).agg(spark_sum(col("row_hash").substr(1, 8).cast("long"))).collect()[0][0]
    report["checksum_match"] = src_checksum == tgt_checksum

    return report

result = reconcile(
    source="s3://bucket/raw_orders/",
    target="s3://bucket/processed_orders/",
    key_col="order_id",
    check_cols=["order_id", "amount", "customer_id", "status"]
)
# {'count_match': True, 'source_count': 1000000, 'target_count': 1000000,
#  'target_duplicate_keys': 0, 'missing_in_target': 0, 'checksum_match': True}
```

**Explanation:**
- Count mismatch catches dropped or duplicated records at a glance
- Key uniqueness ensures the primary key contract is maintained in the target
- `subtract()` finds keys present in source but missing in target (data loss)
- MD5 checksum on concatenated columns detects value-level corruption without comparing row-by-row
- Production systems run this as a post-ETL step and alert on any `False` values

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Dynamic Schema Processing for Multi-Tenant Data

**Scenario:** Your platform ingests data from 50+ tenants, each with a different schema. Write a generic processor that dynamically applies tenant-specific transformations defined in a configuration table, without writing per-tenant code.

<details>
<summary>💡 Hint</summary>
Store transformation rules (column mappings, type casts, filters) in a config table. Dynamically build DataFrame transformations from these rules at runtime.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, coalesce, when
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from functools import reduce

spark = SparkSession.builder.appName("multi_tenant").getOrCreate()

TYPE_MAP = {"string": StringType(), "int": IntegerType(), "double": DoubleType(), "timestamp": TimestampType()}

def apply_tenant_config(df: DataFrame, config: list[dict]) -> DataFrame:
    """Dynamically transform DataFrame based on config rules."""
    for rule in config:
        action = rule["action"]
        if action == "rename":
            df = df.withColumnRenamed(rule["source"], rule["target"])
        elif action == "cast":
            df = df.withColumn(rule["column"], col(rule["column"]).cast(TYPE_MAP[rule["type"]]))
        elif action == "default":
            df = df.withColumn(rule["column"], coalesce(col(rule["column"]), lit(rule["value"])))
        elif action == "filter":
            df = df.filter(col(rule["column"]) == rule["value"])
        elif action == "drop":
            df = df.drop(rule["column"])
    return df

# Config stored in a table or JSON — one entry per tenant
tenant_configs = {
    "tenant_a": [
        {"action": "rename", "source": "usr_id", "target": "user_id"},
        {"action": "cast", "column": "amount", "type": "double"},
        {"action": "default", "column": "region", "value": "US"},
    ],
    "tenant_b": [
        {"action": "rename", "source": "customer_key", "target": "user_id"},
        {"action": "drop", "column": "internal_flag"},
        {"action": "filter", "column": "status", "value": "active"},
    ],
}

# Process each tenant dynamically
for tenant, config in tenant_configs.items():
    raw = spark.read.parquet(f"s3://bucket/ingest/{tenant}/")
    standardized = apply_tenant_config(raw, config)
    standardized.write.mode("append").parquet("s3://bucket/unified/")
```

**Explanation:**
- Config-driven architecture: adding a new tenant requires only a config entry, no code change
- `apply_tenant_config` maps string rule definitions to DataFrame operations dynamically
- Each rule is applied sequentially — order matters (rename before cast)
- This pattern scales to 100+ tenants and is used in multi-tenant SaaS data platforms
- Production enhancement: store configs in Delta/DynamoDB with versioning for audit trails

</details>
</article>
