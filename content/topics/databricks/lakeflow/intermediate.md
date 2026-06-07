---
title: "Lakeflow - Intermediate"
topic: databricks
subtopic: lakeflow
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, lakeflow, connect, cdc, schema-evolution, monitoring]
---

# Lakeflow — Intermediate

## CDC Replication Modes

### Full CDC (Continuous Change Capture)

```python
# Lakeflow Connect CDC mode for databases:
# Captures every INSERT, UPDATE, DELETE in real-time via WAL/binlog

# PostgreSQL: uses logical replication (pgoutput plugin)
# MySQL: uses binlog replication
# SQL Server: uses Change Tracking or CDC tables
# Oracle: uses LogMiner or XStream

# Target Delta table structure (with CDC metadata):
# | order_id | amount | status | _change_type | _commit_version | _commit_timestamp |
# | 1001 | 99.50 | active | insert | 1 | 2024-03-15 10:00 |
# | 1001 | 99.50 | shipped | update_postimage | 2 | 2024-03-15 14:00 |

# CDC modes in Lakeflow Connect:
# "cdc": Full CDC with history (append changes, track all versions)
# "cdc_scd1": CDC applied as SCD Type 1 (latest state only, overwrites)
# "cdc_scd2": CDC applied as SCD Type 2 (full history with valid_from/valid_to)
```

### Incremental Load

```python
# For sources without CDC support: use incremental by timestamp/ID

# Lakeflow Connect incremental configuration:
{
    "ingestion_mode": "incremental",
    "incremental_column": "updated_at",  # Detect new/changed rows by this column
    "initial_load": "full",              # First run: full snapshot
    "subsequent": "where updated_at > last_loaded_at",  # Subsequent: only new rows
}

# Best for:
# - Tables without CDC support
# - APIs that expose "modified since" parameter
# - Systems where full CDC is too expensive/complex
# Trade-off: can miss hard deletes (row deleted from source won't be detected)
```

---

## Schema Evolution in Lakeflow Connect

```python
# Source database schema changes are handled automatically:

# Scenario 1: New column added to source table
# Source: ALTER TABLE orders ADD COLUMN discount DECIMAL(10,2);
# Lakeflow Connect: detects new column → adds to Delta table automatically
# No manual intervention needed!

# Scenario 2: Column type changed (widening)
# Source: ALTER TABLE orders ALTER COLUMN amount TYPE DECIMAL(15,2);
# Lakeflow Connect: updates Delta table schema to accommodate
# Data already written retains original type (Delta handles this)

# Scenario 3: Column renamed
# Source: ALTER TABLE orders RENAME COLUMN amt TO amount;
# Lakeflow Connect: treats as: drop old column + add new column
# May require manual mapping configuration

# Scenario 4: Column dropped
# Source: ALTER TABLE orders DROP COLUMN legacy_field;
# Lakeflow Connect: column becomes NULL in new rows (not deleted from Delta)
# Historical data retains the column

# Schema evolution settings:
{
    "schema_evolution": {
        "add_new_columns": True,        # Auto-add new source columns
        "widen_column_types": True,     # Allow type widening (INT → BIGINT)
        "drop_columns": False,          # Never drop columns from target
        "fail_on_incompatible": True,   # Stop on breaking changes (type narrowing)
    }
}
```

---

## Monitoring Lakeflow Pipelines

### Ingestion Monitoring

```sql
-- Check Lakeflow Connect replication status
-- (via system tables or pipeline events)

-- Replication lag: time between source change and Delta write
SELECT 
    pipeline_name,
    source_table,
    MAX(_commit_timestamp) AS latest_replicated,
    TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), current_timestamp()) AS lag_seconds
FROM system.lakeflow.ingestion_metrics
GROUP BY pipeline_name, source_table
HAVING lag_seconds > 300;  -- Alert if >5 min lag

-- Throughput: records per minute
SELECT 
    DATE_TRUNC('minute', _commit_timestamp) AS minute,
    source_table,
    COUNT(*) AS records_per_minute
FROM production.bronze.orders
WHERE _commit_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY 1, 2
ORDER BY minute DESC;
```

### Pipeline Health Dashboard

```python
# Monitor all Lakeflow components together

MONITORING_QUERIES = {
    "ingestion_lag": """
        SELECT source_name, MAX(lag_seconds) 
        FROM ingestion_metrics 
        WHERE lag_seconds > threshold
    """,
    "pipeline_freshness": """
        SELECT table_name, 
               TIMESTAMPDIFF(MINUTE, MAX(_loaded_at), current_timestamp()) as stale_minutes
        FROM information_schema.tables
        WHERE schema_name = 'silver'
    """,
    "data_quality": """
        SELECT table_name, pass_rate 
        FROM pipeline_quality_metrics 
        WHERE pass_rate < 0.95
    """,
    "pipeline_errors": """
        SELECT pipeline_name, error_message, COUNT(*) 
        FROM pipeline_events 
        WHERE event_type = 'error' AND timestamp >= current_date()
        GROUP BY 1, 2
    """,
}
```

---

## Multi-Source Pipeline Pattern

```python
# Complete Lakeflow pipeline: multiple sources → medallion architecture

# INGESTION LAYER (Lakeflow Connect — no code)
# Source 1: PostgreSQL (orders, customers) → production.bronze.*
# Source 2: Salesforce (opportunities, accounts) → production.bronze.*
# Source 3: S3 files (partner data) → production.bronze.* (Auto Loader)

# TRANSFORMATION LAYER (Lakeflow Pipeline — DLT code)
import dlt

@dlt.table
def silver_unified_customers():
    """Merge customer data from multiple sources."""
    pg_customers = dlt.read_stream("bronze_pg_customers")
    sf_accounts = dlt.read_stream("bronze_sf_accounts")
    
    # Unify schema from different sources
    unified = (
        pg_customers.select(
            col("customer_id").alias("id"),
            col("name"), col("email"),
            lit("internal_db").alias("source")
        )
        .unionByName(
            sf_accounts.select(
                col("account_id").alias("id"),
                col("account_name").alias("name"),
                col("email"),
                lit("salesforce").alias("source")
            ),
            allowMissingColumns=True
        )
    )
    return unified

@dlt.table
def gold_customer_360():
    """Complete customer view from all sources."""
    return (
        dlt.read("silver_unified_customers")
        .groupBy("id")
        .agg(
            first("name").alias("name"),
            first("email").alias("email"),
            collect_set("source").alias("data_sources"),
            count("*").alias("record_count"),
        )
    )
```

---

## Error Handling and Recovery

```python
# Lakeflow Connect error handling:

# 1. Transient errors (network timeout, rate limit):
#    → Automatic retry with exponential backoff
#    → Configurable: max_retries, retry_interval

# 2. Schema conflicts (incompatible type change):
#    → Pipeline pauses + alerts
#    → Manual resolution: update mapping or accept schema change

# 3. Source unavailable (database down):
#    → Pauses ingestion, maintains checkpoint
#    → Resumes automatically when source recovers
#    → No data loss (catches up from last committed position)

# 4. Target write failures (Delta table issues):
#    → Retry the micro-batch
#    → If persistent: pause + alert + manual investigation

# Recovery from extended outage:
# After source comes back online:
# - CDC mode: replays all missed changes from WAL/binlog (automatic)
# - Incremental mode: queries all rows modified since last checkpoint
# - Full refresh: re-snapshots entire source (manual trigger if CDC log expired)
```

---

## Interview Tips

> **Tip 1:** "How does Lakeflow Connect handle CDC?" — Uses database-native change capture (PostgreSQL logical replication, MySQL binlog, SQL Server CDC). Captures INSERT/UPDATE/DELETE in real-time. Writes changes to Delta table with CDC metadata columns. Can maintain current state (SCD1) or full history (SCD2). Handles schema evolution automatically.

> **Tip 2:** "What happens if the source database is down for 6 hours?" — Lakeflow Connect pauses, maintains its checkpoint position. When the source recovers: for CDC, it replays all missed changes from the WAL/binlog (as long as retention is sufficient). For incremental: re-queries all rows modified since last checkpoint. Zero data loss if source retention covers the outage period.

> **Tip 3:** "Lakeflow Connect vs building your own CDC pipeline?" — DIY CDC: Debezium + Kafka + consumer code + monitoring = 2-4 weeks to build, ongoing maintenance. Lakeflow Connect: click-through setup, managed infrastructure, automatic schema evolution, built-in monitoring = 30 minutes to production. Use Lakeflow Connect for supported sources; DIY only for custom/unsupported systems.
