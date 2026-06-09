---
title: "PySpark Joins — Real-World Patterns"
topic: pyspark
subtopic: joins
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [pyspark, star-schema, scd, deduplication, lookup-tables, production]
---

# PySpark Joins — Real-World Patterns

Theory is one thing. Here are the patterns you'll actually implement in production pipelines — star schema enrichment, SCD joins, post-join dedup, and handling large lookup tables efficiently.

---

## Pattern 1: Star Schema Fact-Dimension Join

The most common join pattern in a data warehouse. A large fact table (billions of rows) is enriched with multiple small dimension tables.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, coalesce, lit

spark = SparkSession.builder \
    .appName("star-schema-join") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Load tables
sales_fact    = spark.table("warehouse.sales_fact")         # 2 billion rows
product_dim   = spark.table("warehouse.dim_product")        # 500K rows, ~50 MB
customer_dim  = spark.table("warehouse.dim_customer")       # 10M rows, ~1 GB
date_dim      = spark.table("warehouse.dim_date")           # 3650 rows, trivially small
store_dim     = spark.table("warehouse.dim_store")          # 50K rows, ~5 MB

# Strategy:
# - date_dim, store_dim → broadcast (tiny)
# - product_dim → broadcast (50 MB, within threshold)
# - customer_dim → SMJ or raise broadcast threshold (1 GB is borderline)

enriched = sales_fact \
    .join(broadcast(date_dim),    on="date_key",     how="left") \
    .join(broadcast(product_dim), on="product_key",  how="left") \
    .join(broadcast(store_dim),   on="store_key",    how="left") \
    .join(customer_dim,           on="customer_key", how="left")  # SMJ for large dim

# Select only needed columns — avoid SELECT * in production
enriched_final = enriched.select(
    "sale_id",
    "sale_date",
    col("date_dim.fiscal_quarter").alias("fiscal_quarter"),
    col("date_dim.year").alias("sale_year"),
    "product_key",
    col("product_dim.category").alias("product_category"),
    col("product_dim.brand").alias("brand"),
    "customer_key",
    col("customer_dim.segment").alias("customer_segment"),
    col("customer_dim.region").alias("customer_region"),
    "store_key",
    col("store_dim.district").alias("store_district"),
    "revenue",
    "quantity",
    "discount",
)

# Handle nulls from left joins — customers may be deleted/anonymized
enriched_final = enriched_final.withColumn(
    "customer_segment",
    coalesce(col("customer_segment"), lit("Unknown"))
)

enriched_final.write \
    .partitionBy("sale_year", "product_category") \
    .mode("overwrite") \
    .parquet("s3://warehouse/enriched/sales/")
```

---

## Pattern 2: Slowly Changing Dimension (SCD Type 2) Join

SCD Type 2 stores historical states in the dimension. Joining facts to the correct historical snapshot requires a range join — match on ID AND the fact's timestamp falls within the dimension's validity window.

```python
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import DateType

# SCD2 customer dimension:
# Each row is a version. A customer may have multiple rows with different addresses.
# current_flag = True for the active version.
#
# Schema: customer_id, address, city, start_date, end_date (null if current), current_flag

customer_scd2 = spark.table("warehouse.dim_customer_scd2")
# +------------+---------+-------+----------+----------+
# |customer_id |address  |city   |start_date|end_date  |
# +------------+---------+-------+----------+----------+
# |101         |123 Oak  |Austin |2020-01-01|2022-06-30|
# |101         |456 Elm  |Dallas |2022-07-01|null      |  ← current
# |102         |789 Pine |Houston|2019-03-01|null      |  ← current

# Orders fact: order_id, customer_id, order_date, amount
orders = spark.table("warehouse.fact_orders")

# Join: match customer historical record valid at time of order
# Condition: order_date >= start_date AND (end_date IS NULL OR order_date <= end_date)

historical_enriched = orders.join(
    customer_scd2,
    on=(
        (orders.customer_id == customer_scd2.customer_id) &
        (orders.order_date >= customer_scd2.start_date) &
        (
            customer_scd2.end_date.isNull() |
            (orders.order_date <= customer_scd2.end_date)
        )
    ),
    how="left"
)

# Verify correctness: each order should match at most 1 customer version
from pyspark.sql.functions import count
historical_enriched.groupBy("order_id").agg(count("*").alias("n")) \
    .filter(col("n") > 1) \
    .show()
# Should be empty — if not, you have overlapping SCD2 date ranges (data quality issue)

historical_enriched = historical_enriched.select(
    orders["order_id"],
    orders["order_date"],
    orders["amount"],
    customer_scd2["address"].alias("customer_address_at_order_time"),
    customer_scd2["city"].alias("customer_city_at_order_time"),
)
```

**Performance note:** SCD2 range joins are expensive — no equality predicate on the join means Spark can't use hash joins. For very large dimensions, pre-filter to the likely date range or consider denormalizing dates into integer partition keys.

---

## Pattern 3: Deduplication After Join

Joins can introduce duplicate rows — especially when dimension tables have one-to-many relationships you didn't anticipate, or when a left join is followed by a filter that creates duplicates.

```python
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Scenario: events joined with a session dimension
# A session can map to multiple devices (unintended 1-to-many)
# Result: one event × 3 device rows = 3 duplicate event rows

events = spark.table("warehouse.fact_events")
sessions = spark.table("warehouse.dim_sessions")  # has device_type column, not unique per session_id!

raw_joined = events.join(sessions, on="session_id", how="left")

# Option 1: Deduplicate by taking the "best" dimension match
# (e.g., prefer mobile over desktop)
window = Window.partitionBy("event_id").orderBy(
    col("device_priority").asc_nulls_last()
)
deduped = raw_joined.withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Option 2: Aggregate dimension attributes before joining
# Collapse sessions to 1 row per session_id before the join
sessions_deduped = sessions.groupBy("session_id").agg(
    # Keep the most common/primary device type
    {"device_type": "first", "device_priority": "min"}
)
events_enriched = events.join(sessions_deduped, on="session_id", how="left")

# Verify: row count should not exceed original event count
print(f"Events:  {events.count()}")
print(f"After join: {events_enriched.count()}")
# These should match
```

---

## Pattern 4: Large Lookup Table Patterns

Sometimes your "small" lookup table isn't small — think IP geolocation tables (10 GB), product taxonomy hierarchies (500 MB), or ML feature tables (2 GB). You can't broadcast these, but there are alternatives.

```python
# Scenario: IP-to-country lookup (10 GB range table)
# ip_ranges: start_ip_int, end_ip_int, country_code
# events: event_id, ip_int, ...

# Option A: Partial broadcast — pre-filter lookup to relevant IPs
# If you know which IP prefixes appear in today's events:
event_ip_prefixes = events.select(
    (col("ip_int") / 16777216).cast("int").alias("prefix")  # /8 subnet prefix
).distinct()

# Filter the large lookup to only matching prefixes
ip_lookup_filtered = ip_ranges.filter(
    (col("start_ip_int") / 16777216).cast("int").isin(
        [row.prefix for row in event_ip_prefixes.collect()]  # OK if < 1000 prefixes
    )
)
# Now ip_lookup_filtered may be 200 MB instead of 10 GB → broadcastable

# Option B: Pre-bucket the lookup table at write time
ip_ranges.write \
    .bucketBy(512, "ip_prefix") \
    .saveAsTable("warehouse.ip_ranges_bucketed")

# Option C: Bloom filter pre-filtering
# If your lookup is a key-value map (not a range join), use a join with a
# Bloom filter to push down non-matching keys early
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold",
               str(10 * 1024 * 1024))  # 10 MB

result = events.join(feature_lookup, on="user_id", how="left")
# AQE + Bloom filter: fact side is pre-filtered to only rows that have
# a matching user_id in the lookup

# Option D: For truly massive lookups — consider Delta Lake lookup with
# Z-ordering on the join key (covered in delta-lake-integration)
spark.table("warehouse.feature_store") \
    .join(events, on="user_id", how="inner")
# Delta Z-ordering on user_id means only relevant files are read
```

---

## Common Production Mistakes

```python
# MISTAKE 1: Collecting large DataFrame for isin() filter
# BAD — this collects all IDs to the driver
bad_ids = dim_table.select("id").collect()  # OOM if dim_table is large
fact.filter(col("id").isin([r.id for r in bad_ids]))

# GOOD — use left_semi join
fact.join(dim_table.select("id"), on="id", how="left_semi")

# MISTAKE 2: Joining before filtering
# BAD — joins all of history, then filters
result = fact_full_history \
    .join(dim, on="product_id", how="inner") \
    .filter(col("sale_date") >= "2024-01-01")

# GOOD — filter first, then join (Catalyst may do this automatically,
# but explicit is safer and easier to reason about)
result = fact_full_history \
    .filter(col("sale_date") >= "2024-01-01") \
    .join(dim, on="product_id", how="inner")

# MISTAKE 3: Joining on non-partitioned columns of a partitioned table
# BAD — this full-scans the fact table ignoring partition pruning
fact.join(dim, on="user_id")  # user_id is not the partition column

# GOOD — if fact is partitioned by date, filter on date first
today_fact = spark.read.parquet(f"s3://facts/dt={today}/")  # or use partition filter
today_fact.join(dim, on="user_id")

# MISTAKE 4: Not dropping duplicate keys from the join result
# Both tables have "updated_at" — after join, two columns named "updated_at"
joined = fact.join(dim, on="product_id", how="inner")
# This will fail or return wrong column:
joined.select("updated_at")

# GOOD — drop or rename before returning
joined = fact.join(
    dim.withColumnRenamed("updated_at", "dim_updated_at"),
    on="product_id",
    how="inner"
)
```

---

## Key Takeaways

1. **Star schema joins:** broadcast all dimension tables under ~500 MB; use SMJ for larger dims with AQE enabled.
2. **SCD2 joins:** require a range join condition — expensive, verify 1:1 row matching after join.
3. **Post-join dedup:** always verify `count()` before and after joins; use `row_number()` with a window to pick the best match when 1-to-many is unavoidable.
4. **Large lookup tables:** try partial broadcast filtering, bucketing at write time, or Bloom filter optimization before accepting a full SMJ.
5. **Filter before join** — every row you eliminate before the join reduces shuffle cost.
