---
title: "Delta Live Tables - Intermediate"
topic: databricks
subtopic: delta-live-tables
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, dlt, delta-live-tables, cdc, scd, advanced-expectations]
---

# Delta Live Tables — Intermediate

## Change Data Capture (CDC) with DLT

DLT has native support for processing CDC feeds (insert/update/delete operations):

```python
import dlt
from pyspark.sql.functions import col

# Apply CDC changes to maintain a current-state silver table
dlt.create_streaming_table("silver_customers")

@dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers_cdc",
    keys=["customer_id"],                    # Primary key for matching
    sequence_by=col("updated_at"),           # Order changes by this column
    apply_as_deletes=expr("op = 'DELETE'"),   # Which records are deletes
    apply_as_truncates=expr("op = 'TRUNCATE'"),
    column_list=["customer_id", "name", "email", "region", "updated_at"],
    stored_as_scd_type=1,                    # Type 1: overwrite (current state only)
)

# Result: silver_customers always has the LATEST version of each customer
# CDC operations (insert, update, delete) are applied automatically
# No manual MERGE logic needed!
```

### SCD Type 2 (Full History)

```python
# SCD Type 2: keeps full history with valid_from/valid_to dates
dlt.create_streaming_table("silver_customers_history")

@dlt.apply_changes(
    target="silver_customers_history",
    source="bronze_customers_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("op = 'DELETE'"),
    stored_as_scd_type=2,  # Keeps all versions!
)

# Result table has:
# | customer_id | name | email | __START_AT | __END_AT | __IS_CURRENT |
# | 1 | John | john@v1.com | 2024-01-01 | 2024-03-15 | false |
# | 1 | John | john@v2.com | 2024-03-15 | NULL | true |
# Automatically manages valid_from/valid_to and current flag!
```

---

## Advanced Expectations

### Multiple Expectations with Metrics

```python
@dlt.table
@dlt.expect("has_order_id", "order_id IS NOT NULL")
@dlt.expect("has_customer", "customer_id IS NOT NULL")
@dlt.expect("valid_amount", "amount BETWEEN 0.01 AND 1000000")
@dlt.expect("recent_date", "order_date >= '2020-01-01'")
@dlt.expect_or_drop("not_test_data", "customer_id NOT LIKE 'TEST%'")
def silver_orders():
    return dlt.read_stream("bronze_orders")

# Each expectation generates metrics visible in the DLT UI:
# - Pass rate (% of rows meeting the expectation)
# - Fail count (number of rows that violated)
# - Trend over time (is quality improving or degrading?)
```

### Quarantine Pattern

```python
# Route failed records to a quarantine table for investigation
@dlt.table
def silver_orders_quarantine():
    """Records that failed quality checks."""
    return (
        dlt.read_stream("bronze_orders")
        .filter(
            (col("order_id").isNull()) |
            (col("amount") <= 0) |
            (col("customer_id").isNull())
        )
        .withColumn("_quarantine_reason", 
            when(col("order_id").isNull(), "missing_order_id")
            .when(col("amount") <= 0, "invalid_amount")
            .otherwise("missing_customer_id")
        )
        .withColumn("_quarantined_at", current_timestamp())
    )
```

---

## Pipeline Dependencies and Parameterization

### Cross-Pipeline Dependencies

```python
# Pipeline A produces bronze/silver tables
# Pipeline B consumes silver tables and produces gold

# In Pipeline B:
@dlt.table
def gold_revenue():
    """Reads from another pipeline's output table."""
    return (
        spark.read.table("production.silver.orders")  # External table reference
        .groupBy("order_date")
        .agg(sum("amount").alias("revenue"))
    )
```

### Parameterized Pipelines

```python
# Use pipeline settings for configuration
import dlt

# Access pipeline parameters (set in pipeline config)
source_path = spark.conf.get("pipeline.source_path", "s3://default/path/")
target_schema = spark.conf.get("pipeline.target_schema", "production.default")

@dlt.table(name=f"{target_schema}.bronze_events")
def bronze_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(source_path)
    )

# Same pipeline code, different configs:
# Dev: source_path=s3://dev-data/, target_schema=development.test
# Prod: source_path=s3://prod-data/, target_schema=production.events
```

---

## DLT with Auto Loader Integration

```python
@dlt.table(
    comment="Raw events ingested incrementally from S3"
)
def bronze_events():
    """Auto Loader within DLT — best of both worlds."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", 
                f"{spark.conf.get('pipeline.storage')}/schemas/bronze_events/")
        .load(spark.conf.get("pipeline.source_path"))
    )

# DLT manages:
# - Checkpoint (within its own managed storage)
# - Schema evolution (Auto Loader + DLT mergeSchema)
# - Error recovery (automatic retry on transient failures)
# - Monitoring (file counts, rows processed per update)
```

---

## Monitoring and Observability

### Event Log Queries

```sql
-- DLT generates an event log with detailed pipeline metrics
-- Access after pipeline runs:

-- Check data quality expectation results
SELECT 
    details:flow_definition.output_dataset AS table_name,
    details:flow_progress.data_quality.expectations[0].name AS expectation,
    details:flow_progress.data_quality.expectations[0].passed_records AS passed,
    details:flow_progress.data_quality.expectations[0].failed_records AS failed
FROM event_log(TABLE(production.ecommerce.__pipeline_event_log))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;

-- Check pipeline update duration
SELECT 
    id AS update_id,
    timestamp,
    details:update_progress.state AS state,
    details:update_progress.duration_ms / 1000 AS duration_seconds
FROM event_log(TABLE(production.ecommerce.__pipeline_event_log))
WHERE event_type = 'update_progress';
```

---

## Error Handling and Recovery

```python
# DLT handles most errors automatically:
# - Transient failures (network, cloud API): automatic retry
# - Data corruption: expectations catch and quarantine
# - Schema changes: schemaEvolutionMode handles

# For custom error handling:
@dlt.table
def silver_orders():
    """With explicit error handling for complex transformations."""
    raw = dlt.read_stream("bronze_orders")
    
    # Safe type casting (returns NULL instead of failing on bad data)
    return (
        raw
        .withColumn("amount_safe", 
            when(col("amount").rlike(r"^\d+\.?\d*$"), col("amount").cast("decimal(10,2)"))
            .otherwise(None))
        .withColumn("date_safe",
            to_date(col("order_date"), "yyyy-MM-dd"))  # Returns NULL if format doesn't match
        .filter(col("amount_safe").isNotNull())  # Drop unparseable rows
    )
```

---

## DLT vs dbt

| Aspect | DLT | dbt |
|--------|-----|-----|
| Engine | Spark (Python + SQL) | SQL only (warehouse-specific) |
| Streaming | Native support | No (batch only) |
| Data quality | Built-in expectations | Separate tests (post-load) |
| Incremental | Automatic (streaming tables) | Manual `is_incremental()` logic |
| CDC/SCD | Native `apply_changes` | Manual MERGE or package |
| Orchestration | Self-contained pipeline | Needs external scheduler |
| Visualization | Pipeline DAG in UI | DAG in dbt docs |
| Best for | Streaming + batch on Databricks | SQL-centric, multi-warehouse |

---

## Interview Tips

> **Tip 1:** "How does DLT handle CDC?" — Use `@dlt.apply_changes()` with SCD Type 1 (latest state) or Type 2 (full history). You specify: source table, primary keys, sequence column (for ordering), and which records are deletes. DLT handles the MERGE logic, manages valid_from/valid_to for SCD2, and processes incrementally.

> **Tip 2:** "DLT vs dbt?" — DLT: Spark-native, supports streaming + batch, built-in quality expectations, self-orchestrating. dbt: SQL-focused, works across warehouses (Snowflake, BigQuery, Redshift), better for SQL-centric teams. Choose DLT when you need streaming, CDC, or are fully on Databricks. Choose dbt for multi-warehouse or SQL-only teams.

> **Tip 3:** "How do you monitor DLT pipeline quality?" — Expectations provide real-time quality metrics (pass/fail rates per table per run). The event log stores all pipeline metrics queryable via SQL. Build dashboards on the event log for: quality trends, processing duration, row counts per table, and failure rates. Alert if pass rate drops below threshold.
