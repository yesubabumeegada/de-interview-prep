---
title: "Lakehouse Architecture - Intermediate"
topic: databricks
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, lakehouse, medallion, optimization, data-quality, performance]
---

# Lakehouse Architecture — Intermediate

## Medallion Architecture Design Patterns

### Multi-Hop Refinement

```python
# Pattern: Each layer reads from the previous and adds quality

# Bronze → Silver: Cleansing pipeline
def bronze_to_silver_orders():
    """Transform raw orders into clean business entities."""
    bronze_df = spark.readStream.table("production.bronze.orders")
    
    silver_df = (bronze_df
        # Type casting
        .withColumn("order_id", col("order_id").cast("bigint"))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
        .withColumn("order_date", to_date(col("order_date")))
        # Data quality filters
        .filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        # Deduplication (keep latest by event_time)
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["order_id"])
    )
    
    (silver_df.writeStream
        .option("checkpointLocation", "/checkpoints/silver_orders/")
        .trigger(availableNow=True)
        .toTable("production.silver.orders")
    )

# Silver → Gold: Business aggregation
def silver_to_gold_revenue():
    """Aggregate silver orders into daily revenue metrics."""
    spark.sql("""
        INSERT OVERWRITE production.gold.daily_revenue
        PARTITION (revenue_date)
        SELECT 
            o.order_date AS revenue_date,
            c.region,
            COUNT(*) AS order_count,
            SUM(o.amount) AS revenue,
            AVG(o.amount) AS avg_order_value
        FROM production.silver.orders o
        JOIN production.silver.customers c ON o.customer_id = c.customer_id
        WHERE o.order_date = current_date() - 1
        GROUP BY o.order_date, c.region
    """)
```

---

## Table Organization and Optimization

### Z-Ordering (Data Skipping)

```sql
-- Z-order clusters data by specified columns on disk
-- Queries filtering on those columns skip irrelevant files
OPTIMIZE production.silver.orders
ZORDER BY (customer_id, order_date);

-- Before Z-order: query scans all 500 files (full scan)
-- After Z-order: query scans only 12 files (98% data skipped)

-- Choose Z-order columns by query patterns:
-- If queries always filter by customer_id and order_date → Z-order on both
-- Max 4 columns (effectiveness decreases with more)
-- Re-run OPTIMIZE periodically (after significant data changes)
```

### Partitioning Strategy

```sql
-- Partition for coarse-grained data skipping (high cardinality = bad)
-- GOOD: partition by date (365 values/year)
CREATE TABLE production.silver.events (...)
PARTITIONED BY (event_date DATE);

-- BAD: partition by user_id (millions of values → tiny files!)
-- Use Z-ORDER for high-cardinality columns instead

-- Rules of thumb:
-- < 1000 distinct values: OK to partition
-- > 1000 distinct values: use Z-ORDER instead
-- Each partition should have > 1 GB of data
-- Date partitioning: almost always correct for time-series data
```

### Liquid Clustering (Databricks 13.3+)

```sql
-- Liquid clustering: replaces static partitioning + Z-ORDER
-- Automatically reorganizes data as it grows
CREATE TABLE production.silver.orders
CLUSTER BY (order_date, customer_id);

-- Benefits over partition + Z-ORDER:
-- 1. No partition column limitations
-- 2. Automatically rebalances as data grows
-- 3. Works with incremental OPTIMIZE (not full rewrite)
-- 4. Better for high-cardinality columns

-- Incrementally optimize (only new data):
OPTIMIZE production.silver.orders;
-- Liquid clustering only reorganizes files that need it (not full table)
```

---

## Data Quality in the Lakehouse

### Expectations (Delta Live Tables Style)

```python
import dlt

@dlt.table
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("valid_date", "order_date >= '2020-01-01'")  # Warn but keep
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .withColumn("order_id", col("order_id").cast("bigint"))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
    )
```

### Custom Quality Checks

```sql
-- Post-load quality validation
-- Run after each pipeline execution

-- Check 1: Row count sanity
SELECT COUNT(*) as today_count,
       LAG(COUNT(*)) OVER (ORDER BY _loaded_date) as yesterday_count,
       COUNT(*) / NULLIF(LAG(COUNT(*)) OVER (ORDER BY _loaded_date), 0) as ratio
FROM production.silver.orders
GROUP BY _loaded_date
HAVING ratio < 0.5 OR ratio > 2.0;
-- Alert if count dropped by >50% or doubled (likely data issue)

-- Check 2: Null rate monitoring
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amount
FROM production.silver.orders
WHERE _loaded_at >= current_timestamp() - INTERVAL 1 HOUR;

-- Check 3: Freshness
SELECT MAX(_loaded_at) as last_update,
       TIMESTAMPDIFF(HOUR, MAX(_loaded_at), current_timestamp()) as hours_stale
FROM production.silver.orders;
-- Alert if hours_stale > SLA threshold
```

---

## Streaming + Batch Unified

One of the lakehouse's key advantages — same table for both streaming and batch:

```python
# WRITE: Streaming Auto Loader ingests continuously into bronze
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://lake/landing/events/")
    .writeStream
    .trigger(processingTime="1 minute")
    .toTable("production.bronze.events")
)

# READ: Batch job reads from the same table for daily aggregation
daily_df = spark.sql("""
    SELECT event_type, COUNT(*) as event_count
    FROM production.bronze.events
    WHERE event_date = current_date() - 1
    GROUP BY event_type
""")

# READ: Another streaming job reads from the same bronze table
(spark.readStream
    .table("production.bronze.events")  # Stream from a Delta table!
    .filter(col("event_type") == "purchase")
    .writeStream
    .toTable("production.silver.purchases")
)

# All three access patterns work on the SAME Delta table simultaneously
# No data duplication, no separate streaming/batch systems
```

---

## Multi-Tenant Lakehouse Design

```sql
-- Pattern: Shared platform, isolated by catalog/schema

-- Tenant isolation via Unity Catalog
CREATE CATALOG tenant_acme;
CREATE CATALOG tenant_globex;

-- Each tenant has standard medallion layers
CREATE SCHEMA tenant_acme.bronze;
CREATE SCHEMA tenant_acme.silver;
CREATE SCHEMA tenant_acme.gold;

-- Permissions ensure isolation
GRANT ALL PRIVILEGES ON CATALOG tenant_acme TO `acme-team`;
GRANT ALL PRIVILEGES ON CATALOG tenant_globex TO `globex-team`;
-- acme-team CANNOT see tenant_globex data (and vice versa)

-- Shared reference data (accessible to all tenants)
CREATE CATALOG shared_reference;
GRANT USE CATALOG, SELECT ON CATALOG shared_reference TO `all-tenants`;
-- Currency rates, country codes, etc. — shared but read-only
```

---

## Performance Optimization Patterns

### File Compaction

```sql
-- Problem: many small files from frequent writes (streaming inserts)
-- Small files = slow reads (overhead per file)

-- Solution: OPTIMIZE compacts small files into larger ones
OPTIMIZE production.silver.orders;
-- Merges small files into ~1 GB target size

-- Schedule: run OPTIMIZE daily or after heavy write periods
-- Cost: reads and rewrites data (one-time compute cost)
-- Benefit: 5-50x faster reads for downstream queries
```

### Caching Strategies

```sql
-- Cache frequently accessed tables in cluster memory
CACHE TABLE production.gold.daily_revenue;
-- Subsequent queries on this table are served from memory (instant)

-- Delta cache (automatic, SSD-based):
-- Databricks automatically caches hot data on local NVMe SSDs
-- No configuration needed — happens transparently
-- Benefit: avoids repeated S3/ADLS reads for popular tables

-- When to use explicit CACHE:
-- 1. Dashboard tables queried repeatedly
-- 2. Dimension tables joined frequently
-- 3. Only when table fits in memory!
```

---

## Interview Tips

> **Tip 1:** "How do you optimize lakehouse query performance?" — Three layers: (1) Physical layout — partition by date, Z-ORDER by query columns (or use Liquid Clustering), (2) File maintenance — OPTIMIZE to compact small files, VACUUM to clean old versions, (3) Caching — Delta cache on SSDs for hot data, explicit CACHE for dashboard tables.

> **Tip 2:** "How do you handle data quality in a lakehouse?" — At each layer: Bronze accepts everything (append-only, schema-on-read). Silver enforces quality (type casting, null checks, dedup, DQ rules that drop/quarantine bad records). Gold validates aggregation logic (row count checks, freshness monitoring). Use DLT expectations or custom checks at each transition.

> **Tip 3:** "Streaming + batch on the same table — how?" — Delta Lake supports concurrent readers and writers. A streaming job writes to bronze every minute while a batch job reads yesterday's data for daily aggregation — same table, no conflict. Another streaming job can also read FROM a Delta table (table-as-stream pattern). This eliminates the need for separate Lambda/Kappa architectures.
