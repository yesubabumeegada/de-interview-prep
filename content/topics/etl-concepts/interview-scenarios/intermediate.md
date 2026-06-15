---
title: "ETL Interview Questions: Idempotent Pipelines, CDC, and Schema Evolution"
description: "Mid-level ETL interview scenarios covering idempotent pipeline design, CDC patterns, upsert strategies, and schema evolution handling."
content_type: study_material
topic: etl-concepts
subtopic: interview-scenarios
layer: intermediate
difficulty_level: mid-level
tags: [idempotency, CDC, upsert, schema-evolution, merge, pipeline-design, interview-prep]
---

# ETL Interview Questions: Intermediate

## What Interviewers Are Testing at the Mid Level

At mid-level (2–5 years experience), interviewers expect you to go beyond definitions. They want to see that you can:

- **Design** a pipeline that handles real-world failure modes
- **Explain trade-offs** between different implementation approaches
- **Write or describe code** that implements a specific pattern
- **Debug** common pipeline problems from symptoms

---

## Question 1: Design an Idempotent Incremental Pipeline

**Prompt:** "We have an `orders` table in PostgreSQL. Design an incremental ETL pipeline to Snowflake that is safe to re-run if it fails partway through."

**Strong answer structure:**

### Step 1: Choose an Incremental Strategy

Use `updated_at` timestamp for incremental extraction:

```sql
-- Track pipeline state
CREATE TABLE pipeline_watermarks (
  pipeline_id   VARCHAR(100) PRIMARY KEY,
  last_run_at   TIMESTAMP NOT NULL,
  last_max_ts   TIMESTAMP NOT NULL
);
```

### Step 2: Extract Only Changed Records

```python
def extract_incremental(source_conn, last_max_ts: datetime) -> pd.DataFrame:
    """
    Extract records updated since the last successful run.
    Add a small buffer to catch records that may have been
    committed slightly after the watermark (clock skew).
    """
    buffer = timedelta(minutes=5)
    effective_watermark = last_max_ts - buffer
    
    query = """
        SELECT
          order_id, customer_id, amount, status,
          created_at, updated_at
        FROM orders
        WHERE updated_at > %(watermark)s
        ORDER BY updated_at ASC
    """
    return pd.read_sql(query, source_conn, params={"watermark": effective_watermark})
```

### Step 3: Load Idempotently Using MERGE

```sql
-- Snowflake: idempotent upsert
MERGE INTO orders_fact AS target
USING orders_staging AS source
ON target.order_id = source.order_id

WHEN MATCHED
  AND source.updated_at > target.updated_at  -- Only update if source is newer
THEN UPDATE SET
  status     = source.status,
  amount     = source.amount,
  updated_at = source.updated_at,
  _loaded_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  order_id, customer_id, amount, status, created_at, updated_at, _loaded_at
) VALUES (
  source.order_id, source.customer_id, source.amount,
  source.status, source.created_at, source.updated_at, CURRENT_TIMESTAMP()
);
```

### Step 4: Update Watermark Only on Success

```python
def run_incremental_pipeline(pipeline_id: str):
    last_max_ts = get_watermark(pipeline_id)
    
    try:
        df = extract_incremental(source_conn, last_max_ts)
        new_max_ts = df["updated_at"].max()
        
        load_to_staging(df)           # Load to staging table
        merge_to_fact(pipeline_id)    # Idempotent MERGE
        
        # Only advance watermark if everything succeeded
        update_watermark(pipeline_id, new_max_ts)
        
    except Exception as e:
        # Watermark NOT updated — re-run will re-extract same window
        log_error(pipeline_id, str(e))
        raise
```

**Key insight to mention:** The watermark is updated last. If the pipeline fails during MERGE, the watermark stays at the previous value. Re-running re-extracts the same records and re-runs the MERGE — which is idempotent because we're using `ON target.order_id = source.order_id`.

---

## Question 2: Explain CDC Patterns and When to Use Each

**Prompt:** "What is Change Data Capture? Describe the main CDC patterns and when you'd choose each."

### What Is CDC?

CDC (Change Data Capture) is the process of identifying and capturing changes (inserts, updates, deletes) from a source database so they can be replicated to a target system in near-real-time.

### Pattern 1: Timestamp-Based CDC

Read records where `updated_at > last_run_watermark`.

```sql
-- Source: extract changed orders
SELECT * FROM orders
WHERE updated_at > '2024-01-15 10:00:00'
```

**Pros:** Simple, works on any database, no special source setup needed.
**Cons:** Misses hard deletes; `updated_at` must be maintained correctly; clock skew issues.

**Best for:** Sources with reliable `updated_at` timestamps, no hard deletes needed.

### Pattern 2: Log-Based CDC (Debezium)

Reads the database transaction log (binlog for MySQL, WAL for PostgreSQL) to capture every change in real time.

```yaml
# Debezium connector config for PostgreSQL
{
  "name": "orders-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.dbname": "ecommerce",
    "table.include.list": "public.orders",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput"
  }
}
```

Debezium publishes events to Kafka with before/after images:

```json
{
  "op": "u",
  "before": {"order_id": 123, "status": "pending", "amount": 99.99},
  "after":  {"order_id": 123, "status": "shipped", "amount": 99.99},
  "ts_ms": 1705312800000
}
```

**Pros:** Captures all changes including deletes; sub-second latency; minimal source impact.
**Cons:** Requires WAL/binlog access; complex setup; Kafka infrastructure needed.

**Best for:** Near-real-time replication, capturing deletes, high-volume sources.

### Pattern 3: Trigger-Based CDC

Database triggers write change records to an audit table on every insert/update/delete.

```sql
-- Trigger that writes to change log
CREATE OR REPLACE FUNCTION log_order_changes()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO order_change_log
    (order_id, operation, old_status, new_status, changed_at)
  VALUES (
    COALESCE(NEW.order_id, OLD.order_id),
    TG_OP,
    OLD.status,
    NEW.status,
    NOW()
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_audit
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION log_order_changes();
```

**Pros:** Captures deletes; works on any database; no special permissions.
**Cons:** Adds overhead to every write on the source; can slow down the source database.

**Best for:** Low-write-volume tables where you can't access the transaction log.

### CDC Pattern Comparison

| Pattern | Latency | Captures Deletes | Source Impact | Complexity |
|---------|---------|-----------------|---------------|------------|
| Timestamp | Minutes | No | Low | Low |
| Log-based | Seconds | Yes | Very Low | High |
| Trigger | Minutes | Yes | Medium | Medium |

---

## Question 3: How Do You Handle Upserts at Scale?

**Prompt:** "You're loading 50M records per day into a Snowflake table and need to handle both inserts and updates. How do you do it efficiently?"

### Why Naive MERGE Is Slow at Scale

MERGE on 50M records against a large target table is expensive because it needs to scan the target table to find matches.

### Strategy 1: Staged MERGE with Partition Pruning

```sql
-- Load to staging first
CREATE OR REPLACE TABLE orders_staging AS
SELECT * FROM external_stage;  -- Only today's changed records

-- MERGE only touches relevant partitions
MERGE INTO orders_fact AS target
USING (
  -- Deduplicate within the staging batch first
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM orders_staging
  )
  WHERE rn = 1
) AS source
ON target.order_id = source.order_id
   AND target.event_date = source.event_date  -- Partition pruning

WHEN MATCHED AND source.updated_at > target.updated_at
  THEN UPDATE SET target.status = source.status, target.updated_at = source.updated_at
WHEN NOT MATCHED
  THEN INSERT VALUES (source.*);
```

### Strategy 2: Type 2 SCD (Slowly Changing Dimension) for Updates

Instead of overwriting records, close the old version and insert a new one:

```sql
-- Close existing records that are being updated
UPDATE orders_fact
SET is_current = FALSE,
    valid_to   = source.updated_at
FROM orders_staging source
WHERE orders_fact.order_id = source.order_id
  AND orders_fact.is_current = TRUE
  AND source.updated_at > orders_fact.updated_at;

-- Insert new versions
INSERT INTO orders_fact (order_id, status, amount, updated_at, is_current, valid_from, valid_to)
SELECT
  order_id, status, amount, updated_at,
  TRUE AS is_current,
  updated_at AS valid_from,
  NULL AS valid_to
FROM orders_staging
WHERE (order_id, updated_at) NOT IN (
  SELECT order_id, updated_at FROM orders_fact
);
```

### Strategy 3: Delta Lake MERGE (for Databricks / Spark)

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/data/orders_fact")

delta_table.alias("target").merge(
    source=staging_df.alias("source"),
    condition="target.order_id = source.order_id"
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",
    set={
        "status": "source.status",
        "amount": "source.amount",
        "updated_at": "source.updated_at"
    }
).whenNotMatchedInsertAll() \
 .execute()
```

---

## Question 4: How Do You Handle Schema Evolution?

**Prompt:** "A source system adds a new column to the orders table without warning. How do you prevent your pipeline from breaking, and how do you propagate the change?"

### Common Schema Change Types

| Change | Impact | Handling |
|--------|--------|---------|
| New column added | Low | Ignore or capture with schema merge |
| Column dropped | High | Pipeline fails on SELECT |
| Column renamed | High | Pipeline fails, need mapping update |
| Data type widened (INT→BIGINT) | Low | Usually backward compatible |
| Data type narrowed (BIGINT→INT) | High | Data loss risk |
| NOT NULL added to nullable column | High | Existing NULLs break constraint |

### Strategy 1: Schema-on-Read (Land Raw, Transform Later)

The safest approach: land raw data as-is, apply schema enforcement in transformation:

```python
# Spark: read with schema inference (accepts new columns)
df = spark.read.json("s3://raw/orders/2024-01-15/")
# New columns automatically included

# Write to raw layer with schema evolution
df.write \
  .format("delta") \
  .option("mergeSchema", "true") \  # Delta: accept schema changes
  .mode("append") \
  .save("/data/raw/orders")
```

### Strategy 2: Schema Registry with Compatibility Checks

```python
from confluent_kafka.schema_registry import SchemaRegistryClient

client = SchemaRegistryClient({"url": "http://schema-registry:8081"})

def register_schema(subject: str, schema_str: str, compatibility: str = "FORWARD"):
    """
    BACKWARD: new schema can read old data
    FORWARD: old schema can read new data
    FULL: both directions
    """
    client.set_compatibility(subject, compatibility)
    schema_id = client.register_schema(subject, schema_str)
    return schema_id

def validate_schema_change(subject: str, new_schema_str: str) -> bool:
    """Check if a schema change is compatible before deploying."""
    try:
        is_compat = client.test_compatibility(subject, new_schema_str)
        return is_compat
    except Exception as e:
        log_error(f"Schema compatibility check failed: {e}")
        return False
```

### Strategy 3: Explicit Column Mapping

Maintain a mapping table between source and target column names:

```python
COLUMN_MAPPING = {
    "order_id":    "order_id",
    "cust_id":     "customer_id",   # source renamed, we map it back
    "order_amt":   "amount",
    "order_date":  "created_at",
    "upd_ts":      "updated_at",
}

COLUMNS_TO_DROP = {"internal_flag", "legacy_field"}  # Source columns to ignore

def apply_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Apply column mapping and drop unwanted columns."""
    df = df.rename(columns=COLUMN_MAPPING)
    df = df.drop(columns=[c for c in COLUMNS_TO_DROP if c in df.columns])
    return df
```

---

## Question 5: What Are the Differences Between Full Refresh and Incremental in dbt?

**Prompt:** "Explain dbt's `full_refresh` and `incremental` materialization strategies and when you'd use each."

### dbt Incremental Model

```sql
-- models/orders_fact.sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

SELECT
  order_id,
  customer_id,
  amount,
  status,
  event_date,
  updated_at,
  CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ source('raw', 'orders') }}

{% if is_incremental() %}
-- This block runs only during incremental runs (not full refresh)
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### When to Use Each

| Strategy | When to Use |
|----------|-------------|
| `table` (full refresh every run) | Small tables, all data changes, simple logic |
| `view` | Real-time queries on fast-changing sources |
| `incremental` | Large tables, append/upsert patterns, expensive to recompute |
| `ephemeral` | Intermediate CTEs, no table materialized |

### Full Refresh Command

```bash
# Force a full rebuild of an incremental model
dbt run --full-refresh --select orders_fact

# Full refresh all models in a package
dbt run --full-refresh --select package:core
```

---

## Common Mid-Level Mistakes to Avoid in Interviews

1. **Saying "just use MERGE" without explaining deduplication:** Always deduplicate within the staging batch before merging.

2. **Forgetting about deletes in CDC:** Timestamp-based CDC doesn't capture hard deletes. Mention this limitation.

3. **Not mentioning watermark updates:** Explain that watermarks advance only after successful runs, not before.

4. **Ignoring schema evolution:** Even if the interviewer doesn't ask, mentioning that your design handles schema changes shows production experience.

5. **Not quantifying scale:** Don't say "it works for large tables" — say "this pattern scales to 100M+ rows per day because..."
