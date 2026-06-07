---
title: "Delta Lake - Intermediate"
topic: databricks
subtopic: delta-lake
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, delta-lake, optimize, z-order, partitioning, schema-evolution, constraints]
---

# Delta Lake — Intermediate Concepts

## OPTIMIZE and File Compaction

Small files kill query performance. OPTIMIZE compacts them into larger, optimally-sized files:

```sql
-- Compact all small files in the table
OPTIMIZE delta.`s3://lake/tables/fact_events`;

-- Compact only a specific partition
OPTIMIZE delta.`s3://lake/tables/fact_events`
WHERE event_date = '2024-01-15';
```

**Before OPTIMIZE:** 10,000 files averaging 5 MB each (50 GB total)
**After OPTIMIZE:** 50 files averaging 1 GB each (50 GB total)

**When to OPTIMIZE:**
- After streaming ingestion (many small micro-batches)
- After many small appends
- Before large analytical queries
- As a scheduled maintenance job (nightly)

---

## Z-ORDER (Multi-Dimensional Clustering)

Z-ORDER colocates related data within the same files, dramatically improving filter performance on multiple columns:

```sql
-- Optimize with Z-ORDER on commonly filtered columns
OPTIMIZE delta.`s3://lake/tables/fact_events`
ZORDER BY (customer_id, event_date);
```

**How it helps:**

| Query Filter | Without Z-ORDER | With Z-ORDER |
|-------------|----------------|-------------|
| `WHERE event_date = '2024-01-15'` | Scans 30% of files | Scans 2% of files |
| `WHERE customer_id = 'C123'` | Scans 80% of files | Scans 5% of files |
| `WHERE event_date = '2024-01-15' AND customer_id = 'C123'` | Scans 25% of files | Scans 0.5% of files |

**Z-ORDER column selection rules:**
- Pick 2-4 columns maximum (more = diminishing returns)
- Choose columns used in WHERE clauses most frequently
- High-cardinality columns benefit most (customer_id, user_id)
- Don't Z-ORDER the partition column (already physically separated)

> **Z-ORDER vs Partitioning:** Partition by low-cardinality columns (date, region — few distinct values). Z-ORDER by high-cardinality columns within those partitions (customer_id, product_id).

---

## Partitioning Strategy

```python
# Write with Hive-style partitioning
df.write.format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save("s3://lake/tables/fact_events")

# Result on disk:
# s3://lake/tables/fact_events/
#   _delta_log/
#   year=2024/month=01/part-0001.parquet
#   year=2024/month=01/part-0002.parquet
#   year=2024/month=02/part-0001.parquet
```

**Partitioning guidelines:**

| Data Size | Partition Strategy | Why |
|-----------|-------------------|-----|
| < 1 TB | Don't partition (or single column) | Few files, partitioning adds overhead |
| 1-10 TB | Partition by date (daily/monthly) | Good pruning for time-based queries |
| > 10 TB | Partition by date + one more column | Multi-dimensional pruning |

> **Anti-pattern:** Over-partitioning. If your partition column has 100K+ distinct values (like user_id), you get 100K directories with tiny files. Partition on low-cardinality, Z-ORDER on high-cardinality.

---

## Schema Evolution

Delta supports adding, renaming, and reordering columns without rewriting data:

```python
# Adding a new column (existing rows get NULL)
df_with_new_col.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("s3://lake/tables/employees")

# In SQL:
ALTER TABLE employees ADD COLUMNS (hire_date DATE, is_active BOOLEAN DEFAULT true);

# Rename a column (Databricks)
ALTER TABLE employees RENAME COLUMN dept TO department;

# Change column type (must be compatible: int → long, string → varchar)
ALTER TABLE employees ALTER COLUMN salary TYPE DOUBLE;
```

**Schema evolution modes:**

| Mode | Behavior | Use Case |
|------|----------|----------|
| `mergeSchema=true` | Add new columns from incoming data | Gradual schema growth |
| `overwriteSchema=true` | Replace entire schema | Breaking schema change |
| Default (no option) | Reject if schemas don't match | Production safety |

---

## Constraints and Data Quality

```sql
-- NOT NULL constraint
ALTER TABLE employees ALTER COLUMN name SET NOT NULL;

-- CHECK constraint (Databricks Delta)
ALTER TABLE fact_sales ADD CONSTRAINT positive_amount CHECK (amount > 0);
ALTER TABLE fact_sales ADD CONSTRAINT valid_date CHECK (sale_date <= current_date());

-- Violating a constraint fails the write:
-- INSERT INTO fact_sales VALUES (..., -50, ...);
-- Error: CHECK constraint positive_amount violated
```

**Delta Expectations (Databricks DLT):**

```python
# In Delta Live Tables: declare quality expectations
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_fail("not_null_id", "order_id IS NOT NULL")
def clean_orders():
    return spark.read.format("delta").load("raw_orders")
```

---

## Change Data Feed (CDF)

Track row-level changes (inserts, updates, deletes) for downstream consumers:

```sql
-- Enable Change Data Feed on a table
ALTER TABLE fact_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Read changes since version 5
SELECT * FROM table_changes('fact_orders', 5);
-- Returns: _change_type (insert, update_preimage, update_postimage, delete)
```

```python
# Read changes as a stream
changes = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 5) \
    .load("s3://lake/tables/fact_orders")

# Each row has _change_type, _commit_version, _commit_timestamp
changes.filter("_change_type = 'update_postimage'").show()
```

**Use cases for CDF:**
- Propagate changes to downstream tables (incremental ETL)
- Sync Delta table to a real-time serving layer (Redis, Elasticsearch)
- Audit trail: capture exactly what changed and when
- CDC pipeline: Delta as the target, CDF as the outbound stream

---

## Liquid Clustering (Delta 3.0+ / Databricks 2024)

Replaces partitioning + Z-ORDER with a single, auto-managed clustering solution:

```sql
-- Create table with liquid clustering
CREATE TABLE fact_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    event_date DATE,
    amount DOUBLE
) USING DELTA
CLUSTER BY (event_date, user_id);  -- Liquid clustering columns

-- Change clustering columns without rewriting (instant!)
ALTER TABLE fact_events CLUSTER BY (event_date, event_type);

-- OPTIMIZE now handles both compaction and clustering
OPTIMIZE fact_events;
```

**Liquid Clustering vs Traditional:**

| Feature | Partitioning + Z-ORDER | Liquid Clustering |
|---------|----------------------|-------------------|
| Change columns | Requires full rewrite | Instant ALTER |
| File size management | Manual OPTIMIZE scheduling | Automatic (incremental) |
| Over-partitioning risk | Yes (too many directories) | No (no directories) |
| Works with streaming | Requires careful design | Automatic |
| Databricks only? | No (open Delta) | Currently Databricks-only |

---

## Table Properties and Configuration

```sql
-- Set table properties
ALTER TABLE fact_events SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',     -- Auto-compact on write
    'delta.autoOptimize.autoCompact' = 'true',       -- Background compaction
    'delta.logRetentionDuration' = 'interval 30 days', -- Keep 30 days of log
    'delta.deletedFileRetentionDuration' = 'interval 7 days', -- VACUUM after 7 days
    'delta.enableChangeDataFeed' = 'true',           -- Track row-level changes
    'delta.columnMapping.mode' = 'name',             -- Enable column rename/drop
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);
```

---

## Interview Tips

> **Tip 1:** "How do you handle the small files problem with Delta?" — "OPTIMIZE command compacts small files into ~1 GB target size. For streaming tables, enable autoOptimize (optimizeWrite + autoCompact) so files are automatically compacted. Schedule OPTIMIZE as a nightly maintenance job for batch tables."

> **Tip 2:** "Partitioning vs Z-ORDER?" — "Partition by low-cardinality columns (date, region — physically separate directories). Z-ORDER by high-cardinality columns (customer_id, product_id — colocates related data within files). Don't Z-ORDER the partition column — it's already separated."

> **Tip 3:** "How do you handle schema changes?" — "Delta supports schema evolution with `mergeSchema=true` (additive changes like new columns). For breaking changes (remove column, change type incompatibly), use `overwriteSchema=true`. In production, I use column mapping mode for non-breaking renames and drops without data rewrite."
