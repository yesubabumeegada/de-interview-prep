---
title: "Delta Live Tables - Scenario Questions"
topic: databricks
subtopic: delta-live-tables
content_type: scenario_question
tags: [databricks, dlt, delta-live-tables, interview, scenarios]
---

# Scenario Questions — Delta Live Tables

<article data-difficulty="junior">

## 🟢 Junior: Basic DLT Pipeline

**Scenario:** Create a DLT pipeline that ingests JSON files from S3 into a bronze table, then cleans and deduplicates into a silver table. The bronze table should accept all data; the silver table should only keep rows with non-null `order_id` and positive `amount`.

<details>
<summary>💡 Hint</summary>
Use `@dlt.table` decorator for each table. Bronze uses Auto Loader (readStream + cloudFiles). Silver uses `dlt.read_stream()` from bronze with expectations for quality.
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name

@dlt.table(comment="Raw orders from landing zone")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://lake/landing/orders/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

@dlt.table(comment="Cleaned, deduplicated orders")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("bigint").alias("order_id"),
            col("customer_id").cast("bigint").alias("customer_id"),
            col("amount").cast("decimal(10,2)").alias("amount"),
            col("order_date").cast("date").alias("order_date"),
            col("status"),
            col("_ingested_at"),
        )
        .dropDuplicates(["order_id"])
    )
```

**Key Points:**
- `@dlt.table` defines a table; DLT manages creation, updates, and dependencies
- Bronze uses `spark.readStream` with cloudFiles (Auto Loader within DLT)
- Silver uses `dlt.read_stream("bronze_orders")` to read from the bronze table
- `@dlt.expect_or_drop` silently removes rows that fail the condition
- DLT automatically detects that silver depends on bronze (execution order)
- No checkpoint management needed — DLT handles it internally

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Understanding Expectations

**Scenario:** Explain the difference between `@dlt.expect`, `@dlt.expect_or_drop`, and `@dlt.expect_or_fail`. When would you use each?

<details>
<summary>💡 Hint</summary>
They differ in what happens when a row violates the condition: log only, drop the row, or stop the pipeline entirely.
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt

# EXPECT: Log violation, KEEP the row (monitoring only)
@dlt.table
@dlt.expect("has_email", "email IS NOT NULL")
def customers_monitoring():
    """Track email fill rate but don't reject records."""
    return dlt.read("raw_customers")
# Use when: field is desirable but not critical (nice-to-have)
# Example: email for marketing (analytics still works without it)

# EXPECT_OR_DROP: Remove the violating row (quarantine)
@dlt.table
@dlt.expect_or_drop("valid_amount", "amount > 0")
def orders_filtered():
    """Drop invalid orders — downstream analytics need positive amounts."""
    return dlt.read("raw_orders")
# Use when: bad rows would corrupt downstream aggregations
# Example: negative amounts would skew revenue calculations

# EXPECT_OR_FAIL: Stop the entire pipeline
@dlt.table
@dlt.expect_or_fail("schema_version_match", "schema_version = 2")
def critical_data():
    """Pipeline stops if source schema changed unexpectedly."""
    return dlt.read("raw_critical_feed")
# Use when: violation indicates a systemic problem (not just bad rows)
# Example: upstream schema change that would corrupt all data
```

| Level | On Violation | Row Count Impact | Pipeline Status | Use Case |
|-------|-------------|-----------------|-----------------|----------|
| `expect` | Keep row, log metric | No change | Continues | Monitor quality trends |
| `expect_or_drop` | Remove row | Decreases | Continues | Filter bad data |
| `expect_or_fail` | Stop pipeline | N/A | FAILS | Critical data integrity |

**Key Points:**
- Start with `expect` (monitoring) → upgrade to `expect_or_drop` once you understand the data
- `expect_or_fail` should be rare — only for conditions where ALL data would be wrong
- DLT tracks pass/fail metrics for ALL expectation types in the pipeline UI
- You can have multiple expectations per table (they all apply independently)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Streaming vs Materialized View

**Scenario:** You have two tables in your DLT pipeline: (A) raw events from Kafka that arrive continuously, (B) daily revenue summary. Should each be a streaming table or a materialized view?

<details>
<summary>💡 Hint</summary>
Streaming tables: for append-only data that arrives incrementally. Materialized views: for transformations that DLT can optimize (may recompute or process incrementally).
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt

# Table A: Raw events from Kafka → STREAMING TABLE
# WHY: Data is append-only, arrives continuously, we process incrementally
@dlt.table
def bronze_events():  # This is a streaming table (uses readStream)
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "user_events")
        .load()
        .selectExpr("CAST(value AS STRING) as raw_json", "timestamp")
    )

# Table B: Daily revenue summary → MATERIALIZED VIEW
# WHY: It's an aggregation that DLT can optimize (incremental or full recompute)
@dlt.table
def gold_daily_revenue():  # This is a materialized view (uses dlt.read, not readStream)
    return (
        dlt.read("silver_orders")  # Batch read (not streaming)
        .groupBy("order_date")
        .agg(
            count("*").alias("order_count"),
            sum("amount").alias("revenue"),
        )
    )
```

| Type | Created By | Data Pattern | Processing | Best For |
|------|-----------|-------------|-----------|----------|
| Streaming table | `spark.readStream` or `dlt.read_stream()` | Append-only | Always incremental | Bronze ingestion, event logs |
| Materialized view | `dlt.read()` (batch) | Any | DLT chooses (incremental or full) | Silver/gold aggregations |

**Key Points:**
- Use streaming tables for append-only sources (Kafka, Auto Loader, CDC)
- Use materialized views for transformations (aggregations, joins, filters)
- DLT optimizes materialized views automatically (may process incrementally)
- You can read FROM a streaming table in a materialized view (streaming → batch bridge)
- For `dlt.read_stream()`: both source and target must be streaming tables

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Pipeline Modes

**Scenario:** Your DLT pipeline runs in "Triggered" mode (manually or on schedule). Users want data refreshed within 5 minutes of arrival. What should you change?

<details>
<summary>💡 Hint</summary>
Switch from Triggered mode to Continuous mode. Continuous mode keeps the cluster running and processes new data as it arrives.
</details>

<details>
<summary>✅ Solution</summary>

```python
# TRIGGERED MODE (current):
# Pipeline starts → processes available data → stops → cluster terminates
# Data latency: depends on schedule (if hourly = up to 60 min stale)
# Cost: only pay while running

# CONTINUOUS MODE (needed for <5 min latency):
# Pipeline starts → runs forever → processes data as it arrives
# Data latency: seconds to minutes (sub-5 min easily achievable)
# Cost: cluster always on (higher cost, lower latency)

# Change in pipeline settings:
PIPELINE_CONFIG = {
    "name": "real_time_etl",
    "continuous": True,  # Changed from False to True!
    "target": "production.events",
    "clusters": [{
        "autoscale": {"min_workers": 2, "max_workers": 8}
    }]
}

# The DLT code itself doesn't change!
# Same @dlt.table functions work in both modes.
# Only the pipeline configuration changes.
```

| Mode | Latency | Cost | Use Case |
|------|---------|------|----------|
| Triggered | Minutes-hours (depends on schedule) | Low (pay per run) | Batch ETL, daily reports |
| Continuous | Seconds-minutes | Higher (always-on) | Real-time dashboards, alerts |

**Key Points:**
- Continuous mode = cluster always running, processes new data immediately
- Triggered mode = cluster starts/stops per run, processes whatever's available
- Same code works in both modes (no code changes, just config)
- Continuous costs more but gives sub-5-minute latency
- Compromise: Triggered mode running every 5 minutes (cluster restarts add ~3 min overhead though)
- For true <5 min latency: Continuous mode is required

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Reading DLT Pipeline Results

**Scenario:** Your DLT pipeline has been running for a week. How do you query the tables it produces? Can you read them from a regular notebook (not inside DLT)?

<details>
<summary>💡 Hint</summary>
DLT tables are regular Delta tables in Unity Catalog. Query them from any notebook, SQL warehouse, or BI tool using their three-level name.
</details>

<details>
<summary>✅ Solution</summary>

```python
# DLT pipeline config has: "target": "production.ecommerce"
# DLT creates tables like: production.ecommerce.bronze_orders
#                           production.ecommerce.silver_orders
#                           production.ecommerce.gold_daily_revenue

# FROM ANY NOTEBOOK (not DLT):
df = spark.table("production.ecommerce.silver_orders")
df.show()

# FROM SQL:
# SELECT * FROM production.ecommerce.gold_daily_revenue WHERE order_date = current_date() - 1;

# FROM BI TOOL (Tableau, Power BI):
# Connect to Databricks SQL Warehouse → production.ecommerce.gold_daily_revenue

# FROM ANOTHER DLT PIPELINE:
# Reference as external table (not using dlt.read):
# spark.read.table("production.ecommerce.silver_orders")

# KEY: DLT tables are NORMAL Delta tables once created.
# They just happen to be managed/updated by the DLT pipeline.
# Anyone with SELECT permission can query them like any other table.
```

**Key Points:**
- DLT output = regular Delta tables in Unity Catalog (fully accessible)
- Query from: notebooks, SQL worksheets, BI tools, other pipelines — anything
- The `target` config determines the catalog.schema where tables are created
- Table names match the function names in your DLT code (`def silver_orders` → `silver_orders` table)
- Permissions: Unity Catalog GRANT controls who can read the DLT output tables
- DLT is the WRITER; everything else is a READER

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: CDC with apply_changes

**Scenario:** Your source PostgreSQL database sends CDC events (via Debezium) with `op` field (`c`=create, `u`=update, `d`=delete). Implement a DLT pipeline that maintains a current-state `silver_customers` table from these CDC events.

<details>
<summary>💡 Hint</summary>
Use `@dlt.apply_changes()` with the `keys`, `sequence_by`, and `apply_as_deletes` parameters. DLT handles the MERGE logic internally.
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt
from pyspark.sql.functions import col, expr

# Step 1: Ingest CDC events into bronze (streaming table)
@dlt.table(comment="Raw CDC events from Debezium/PostgreSQL")
def bronze_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://lake/landing/cdc/customers/")
    )

# Step 2: Create target streaming table (DLT manages this)
dlt.create_streaming_table(
    name="silver_customers",
    comment="Current state of all customers (maintained via CDC)"
)

# Step 3: Apply CDC changes
@dlt.apply_changes(
    target="silver_customers",                      # Target table to maintain
    source="bronze_customers_cdc",                  # Source of CDC events
    keys=["customer_id"],                           # Primary key for matching
    sequence_by=col("updated_at"),                  # Order events by timestamp
    apply_as_deletes=expr("op = 'd'"),              # 'd' operations = delete the row
    except_column_list=["op", "_rescued_data"],     # Don't include these in target
    stored_as_scd_type=1,                           # Type 1: keep only latest (overwrite)
)

# Result: silver_customers always reflects current database state
# INSERT (op='c'): new row added
# UPDATE (op='u'): existing row overwritten with new values  
# DELETE (op='d'): row removed from table
# All handled automatically — no manual MERGE!
```

**Key Points:**
- `@dlt.apply_changes` replaces manual MERGE logic (DLT generates optimal MERGE)
- `keys`: primary key columns for matching source → target rows
- `sequence_by`: ensures out-of-order events are handled correctly (latest wins)
- `apply_as_deletes`: boolean expression identifying delete operations
- `stored_as_scd_type=1`: only keep latest state (Type 2 keeps full history)
- `except_column_list`: columns from source to NOT carry to target (metadata columns)
- The target table looks exactly like the source database's current state

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Table Dependencies

**Scenario:** Your DLT pipeline has: bronze_orders, bronze_customers, silver_orders (joins with customers), and gold_revenue (aggregates silver_orders). DLT should process them in the correct order. How does dependency resolution work?

<details>
<summary>💡 Hint</summary>
DLT automatically detects dependencies from `dlt.read()` / `dlt.read_stream()` calls. If silver reads from bronze, DLT ensures bronze runs first. No manual ordering needed.
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt
from pyspark.sql.functions import col, sum, count

# DLT builds a dependency graph from dlt.read() calls:

@dlt.table
def bronze_orders():       # No dependencies — runs first
    return spark.readStream.format("cloudFiles").load("s3://lake/orders/")

@dlt.table
def bronze_customers():    # No dependencies — runs first (parallel with bronze_orders)
    return spark.readStream.format("cloudFiles").load("s3://lake/customers/")

@dlt.table
def silver_orders():       # Depends on: bronze_orders + bronze_customers
    orders = dlt.read_stream("bronze_orders")       # Dependency 1
    customers = dlt.read("bronze_customers")         # Dependency 2
    return orders.join(customers, "customer_id", "left")

@dlt.table
def gold_revenue():        # Depends on: silver_orders
    return (
        dlt.read("silver_orders")                    # Dependency on silver
        .groupBy("order_date")
        .agg(sum("amount").alias("revenue"), count("*").alias("orders"))
    )

# DLT execution order (automatic):
# 1. bronze_orders + bronze_customers (parallel, no deps)
# 2. silver_orders (waits for both bronze tables)
# 3. gold_revenue (waits for silver_orders)

# The DAG is visible in the DLT pipeline UI as a graph visualization
# You NEVER specify execution order — DLT infers it from data flow
```

**Key Points:**
- Dependencies are implicit: `dlt.read("table_name")` creates a dependency edge
- DLT builds a DAG and executes in topological order automatically
- Parallel execution where possible (independent tables run simultaneously)
- If a dependency fails, downstream tables are NOT processed (fail-fast)
- The pipeline UI shows the full dependency graph visually
- Adding a new table only requires defining it — DLT figures out where it fits

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Schema Evolution

**Scenario:** Your upstream API added 3 new fields to its JSON payload last week. Your DLT bronze table accepted them (schema evolution), but silver table's explicit column selection doesn't include them. How do you handle schema evolution across layers?

<details>
<summary>💡 Hint</summary>
Bronze: use Auto Loader's schemaEvolutionMode to accept new fields. Silver: either use `select("*")` to pass through all columns, or deliberately choose which new columns to include.
</details>

<details>
<summary>✅ Solution</summary>

```python
import dlt
from pyspark.sql.functions import col

# BRONZE: Accept all new columns automatically
@dlt.table(
    table_properties={"delta.autoSchema.enabled": "true"}
)
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Accept new fields
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://lake/landing/events/")
    )

# SILVER OPTION A: Explicit column list (controlled, safe)
@dlt.table
def silver_events_controlled():
    """Only include columns we've validated. New columns ignored until reviewed."""
    return (
        dlt.read_stream("bronze_events")
        .select(
            col("event_id").cast("bigint"),
            col("user_id").cast("bigint"),
            col("event_type"),
            col("event_timestamp").cast("timestamp"),
            col("amount").cast("decimal(10,2)"),
            # New columns NOT included until reviewed and added here
        )
    )

# SILVER OPTION B: Pass-through (all columns, auto-evolving)
@dlt.table(
    table_properties={"delta.autoSchema.enabled": "true"}
)
def silver_events_flexible():
    """Pass through all columns. Schema evolves with source."""
    return (
        dlt.read_stream("bronze_events")
        .filter(col("event_id").isNotNull())
        # select("*") means new columns flow through automatically
    )

# SILVER OPTION C: Rescue unknown columns (safest)
@dlt.table
def silver_events_rescue():
    """Known columns typed explicitly, unknown go to _extra_fields."""
    bronze = dlt.read_stream("bronze_events")
    known_cols = ["event_id", "user_id", "event_type", "event_timestamp", "amount"]
    
    extra_cols = [c for c in bronze.columns if c not in known_cols and not c.startswith("_")]
    
    return (
        bronze
        .select(
            col("event_id").cast("bigint"),
            col("user_id").cast("bigint"),
            col("event_type"),
            col("event_timestamp").cast("timestamp"),
            col("amount").cast("decimal(10,2)"),
            struct(*[col(c) for c in extra_cols]).alias("_extra_fields") if extra_cols else lit(None).alias("_extra_fields"),
        )
    )
```

**Key Points:**
- Bronze: always accept new columns (schemaEvolutionMode=addNewColumns)
- Silver has three strategies: controlled (explicit), flexible (pass-through), or rescue (capture extras)
- Controlled approach: safest for production (new columns reviewed before inclusion)
- Flexible approach: fastest to adapt (but may introduce unvalidated data)
- Best practice: controlled silver + monitor `_rescued_data` or bronze schema changes
- When new columns appear: review, add to silver's explicit select, deploy update

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: DLT Pipeline Testing

**Scenario:** You need to test your DLT pipeline logic before deploying to production. DLT pipelines can't easily be unit-tested since they require the DLT runtime. What's your testing strategy?

<details>
<summary>💡 Hint</summary>
Separate transformation logic from DLT decorators. Test the transformation functions independently. Use DLT development mode for integration tests.
</details>

<details>
<summary>✅ Solution</summary>

```python
# STRATEGY: Separate business logic from DLT framework

# --- transformations.py (testable, no DLT dependency) ---
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_orders(df: DataFrame) -> DataFrame:
    """Business logic: clean and type orders. Testable without DLT."""
    return (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        .select(
            col("order_id").cast("bigint"),
            col("customer_id").cast("bigint"),
            col("amount").cast("decimal(10,2)"),
            col("order_date").cast("date"),
        )
        .dropDuplicates(["order_id"])
    )

def calculate_daily_revenue(df: DataFrame) -> DataFrame:
    """Business logic: aggregate daily revenue. Testable without DLT."""
    return (
        df.groupBy("order_date")
        .agg(
            count("*").alias("order_count"),
            sum("amount").alias("revenue"),
        )
    )

# --- pipeline.py (DLT wrappers, thin layer) ---
import dlt
from transformations import clean_orders, calculate_daily_revenue

@dlt.table
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL AND amount > 0")
def silver_orders():
    return clean_orders(dlt.read_stream("bronze_orders"))

@dlt.table
def gold_revenue():
    return calculate_daily_revenue(dlt.read("silver_orders"))

# --- test_transformations.py (unit tests, run in regular notebook) ---
def test_clean_orders():
    test_data = spark.createDataFrame([
        {"order_id": "1", "customer_id": "C1", "amount": "99.50", "order_date": "2024-01-15"},
        {"order_id": None, "customer_id": "C2", "amount": "50.00", "order_date": "2024-01-15"},  # Null ID
        {"order_id": "2", "customer_id": "C3", "amount": "-10", "order_date": "2024-01-15"},      # Negative
        {"order_id": "1", "customer_id": "C1", "amount": "99.50", "order_date": "2024-01-15"},    # Duplicate
    ])
    
    result = clean_orders(test_data)
    
    assert result.count() == 1, f"Expected 1 row, got {result.count()}"
    assert result.collect()[0]["order_id"] == 1
    assert float(result.collect()[0]["amount"]) == 99.50
    print("test_clean_orders: PASSED ✓")

def test_calculate_daily_revenue():
    test_data = spark.createDataFrame([
        {"order_date": "2024-01-15", "amount": 100.00, "order_id": 1},
        {"order_date": "2024-01-15", "amount": 50.00, "order_id": 2},
        {"order_date": "2024-01-16", "amount": 75.00, "order_id": 3},
    ]).withColumn("order_date", col("order_date").cast("date"))
    
    result = calculate_daily_revenue(test_data)
    
    jan15 = result.filter(col("order_date") == "2024-01-15").collect()[0]
    assert jan15["order_count"] == 2
    assert float(jan15["revenue"]) == 150.00
    print("test_calculate_daily_revenue: PASSED ✓")

# Run tests
test_clean_orders()
test_calculate_daily_revenue()
```

**Key Points:**
- Separate business logic (pure functions on DataFrames) from DLT decorators
- Test the logic independently in regular notebooks (no DLT runtime needed)
- DLT decorators are a thin wrapper — the real logic is in reusable functions
- For integration tests: use DLT "Development" mode (runs pipeline with relaxed settings)
- Development mode: doesn't publish tables, runs on smaller data, faster iteration
- CI/CD: run unit tests on every PR; run DLT development mode nightly

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Pipeline Design

**Scenario:** Design a DLT pipeline for a fintech company processing 10M transactions/day from 5 sources (banking APIs, card processors, ACH network, wire transfers, internal ledger). Requirements: <5 min latency, SOC 2 compliant, complete audit trail, and 99.9% uptime.

<details>
<summary>💡 Hint</summary>
Continuous mode for latency, expectations for compliance, event log for audit trail, multi-cluster for HA, and separate ingestion from transformation pipelines for resilience.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE: Two DLT pipelines (separation of concerns)
# Pipeline 1: Ingestion (continuous, 5 streams → bronze)
# Pipeline 2: Transformation (triggered every 5 min, bronze → silver → gold)

# === PIPELINE 1: INGESTION (continuous) ===
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Banking API transactions")
def bronze_banking():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://lake/landing/banking/")

@dlt.table(comment="Card processor transactions")
def bronze_cards():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "avro").load("s3://lake/landing/cards/")

@dlt.table(comment="ACH transactions")
def bronze_ach():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://lake/landing/ach/")

@dlt.table(comment="Wire transfers")
def bronze_wires():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://lake/landing/wires/")

@dlt.table(comment="Internal ledger entries")
def bronze_ledger():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("s3://lake/landing/ledger/")

# === PIPELINE 2: TRANSFORMATION (triggered) ===

@dlt.table(comment="Unified transactions across all sources")
@dlt.expect_or_fail("valid_txn_id", "transaction_id IS NOT NULL")
@dlt.expect_or_fail("valid_amount", "amount IS NOT NULL AND amount != 0")
@dlt.expect_or_drop("valid_account", "account_id IS NOT NULL")
@dlt.expect("reasonable_amount", "ABS(amount) < 10000000")  # Flag >$10M but keep
def silver_transactions():
    """SOC 2: every transaction must have valid ID and amount (pipeline fails otherwise)."""
    banking = dlt.read_stream("bronze_banking").withColumn("source", lit("banking"))
    cards = dlt.read_stream("bronze_cards").withColumn("source", lit("card"))
    ach = dlt.read_stream("bronze_ach").withColumn("source", lit("ach"))
    wires = dlt.read_stream("bronze_wires").withColumn("source", lit("wire"))
    ledger = dlt.read_stream("bronze_ledger").withColumn("source", lit("ledger"))
    
    unified = banking.unionByName(cards, allowMissingColumns=True) \
        .unionByName(ach, allowMissingColumns=True) \
        .unionByName(wires, allowMissingColumns=True) \
        .unionByName(ledger, allowMissingColumns=True)
    
    return (
        unified
        .select(
            col("transaction_id").cast("string"),
            col("account_id").cast("string"),
            col("amount").cast("decimal(15,2)"),
            col("currency"),
            col("transaction_type"),
            col("timestamp").cast("timestamp").alias("txn_timestamp"),
            col("source"),
            current_timestamp().alias("_processed_at"),
        )
        .dropDuplicates(["transaction_id"])
    )

# Compliance-critical gold tables
@dlt.table(comment="Daily account balances (SOC 2 audit requirement)")
def gold_daily_balances():
    return (
        dlt.read("silver_transactions")
        .groupBy("account_id", date_trunc("day", col("txn_timestamp")).alias("balance_date"))
        .agg(
            sum(when(col("amount") > 0, col("amount")).otherwise(0)).alias("total_credits"),
            sum(when(col("amount") < 0, abs(col("amount"))).otherwise(0)).alias("total_debits"),
            sum("amount").alias("net_change"),
            count("*").alias("transaction_count"),
        )
    )
```

```python
# PIPELINE CONFIGURATION:
INGESTION_PIPELINE = {
    "name": "fintech_ingestion",
    "continuous": True,          # Always running for <5 min latency
    "target": "production.fintech",
    "photon": True,
    "clusters": [{
        "autoscale": {"min_workers": 4, "max_workers": 12},
        "node_type_id": "r5.xlarge",
        "availability": "ON_DEMAND",  # No spot for 99.9% uptime
    }]
}

TRANSFORM_PIPELINE = {
    "name": "fintech_transformation",
    "continuous": False,
    "target": "production.fintech",
    "photon": True,
    "clusters": [{
        "autoscale": {"min_workers": 2, "max_workers": 8},
        "node_type_id": "i3.xlarge",
        "availability": "ON_DEMAND",
    }]
}
# Transform pipeline triggered every 5 minutes by Databricks Workflow
```

**Key Points:**
- Two pipelines: ingestion (continuous, fault-tolerant) + transformation (triggered, can restart independently)
- `expect_or_fail` for SOC 2 critical fields (pipeline stops → team investigates → no bad data in downstream)
- On-demand instances (no spot) for 99.9% uptime requirement
- Audit trail: DLT event log + Delta time travel + Unity Catalog audit logs
- Deduplication by transaction_id across all 5 sources
- Separation: if transformation fails, ingestion continues (bronze keeps accumulating)
- 10M txn/day = ~115 txn/sec = modest volume for continuous DLT pipeline

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: DLT Performance Optimization

**Scenario:** Your DLT pipeline takes 45 minutes per triggered run. It processes 50M rows from bronze (3 sources) through silver (5 tables with joins) to gold (10 aggregation tables). Target: under 15 minutes. Identify bottlenecks and optimize.

<details>
<summary>💡 Hint</summary>
Common bottlenecks: large joins without broadcast hints, too many shuffles, small files in bronze, unoptimized Delta tables (no Z-ORDER), and undersized clusters.
</details>

<details>
<summary>✅ Solution</summary>

```python
# DIAGNOSIS: Where is the 45 minutes spent?
# Check DLT event log for per-table duration:
# bronze tables: 5 min (IO-bound, reading 50M rows from S3)
# silver tables: 25 min (joins + dedup = shuffle-heavy) ← BOTTLENECK
# gold tables: 15 min (aggregations on silver) ← SECONDARY

# OPTIMIZATION 1: Broadcast small dimension tables in joins
@dlt.table
def silver_enriched_orders():
    orders = dlt.read_stream("bronze_orders")  # 50M rows
    customers = broadcast(dlt.read("dim_customers"))  # 500K rows → broadcast!
    products = broadcast(dlt.read("dim_products"))    # 10K rows → broadcast!
    
    return (
        orders
        .join(customers, "customer_id")  # No shuffle (broadcast)
        .join(products, "product_id")    # No shuffle (broadcast)
    )
# Impact: eliminates 2 shuffle stages → saves 10+ minutes

# OPTIMIZATION 2: Enable Photon (vectorized execution)
# Pipeline config: "photon": True
# Impact: 2-5x faster aggregations and joins (C++ engine vs JVM)

# OPTIMIZATION 3: Auto-optimize writes (prevent small files)
@dlt.table(
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",  # Coalesce small files
        "delta.autoOptimize.autoCompact": "true",    # Compact after write
        "pipelines.autoOptimize.zOrderCols": "customer_id,order_date",
    }
)
def silver_orders():
    return dlt.read_stream("bronze_orders").select(...)
# Impact: downstream reads are 3-5x faster (fewer, larger files)

# OPTIMIZATION 4: Increase cluster size for the bottleneck phase
# Pipeline config:
# "clusters": [{"autoscale": {"min_workers": 8, "max_workers": 16}}]
# More workers = more parallelism for shuffle-heavy operations

# OPTIMIZATION 5: Use streaming tables to make silver incremental
# Instead of recomputing silver every run, stream from bronze
@dlt.table
def silver_orders():
    return dlt.read_stream("bronze_orders")  # Only processes NEW rows!
# Impact: instead of 50M rows, processes only ~500K new rows per run

# RESULT:
# Before: 45 minutes (full recompute of 50M rows)
# After: 8 minutes (streaming incremental + broadcast + Photon + optimized writes)
# - Streaming silver: processes 500K new rows instead of 50M (90% less work)
# - Broadcast: eliminates shuffle for dim joins (saves 10 min)
# - Photon: 3x faster aggregations (saves 5 min)
# - Auto-optimize: downstream reads faster
```

**Key Points:**
- #1 optimization: make silver streaming (incremental, not full recompute) — 90% speedup
- #2 optimization: broadcast small tables (eliminate shuffle) — saves 5-10 min
- #3 optimization: enable Photon (vectorized, C++ engine) — 2-5x faster
- #4 optimization: auto-optimize writes (prevent small files for downstream reads)
- Diagnose first: check event log for per-table duration to find the bottleneck
- 45 min → 8 min = 5.6x improvement with these standard optimizations

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Disaster Recovery

**Scenario:** Your production DLT pipeline corrupted the silver_customers table (a bad CDC event overwrote correct data with nulls). 10K customer records are now incorrect. Downstream gold tables were also affected. How do you recover?

<details>
<summary>💡 Hint</summary>
Delta time travel lets you restore tables to previous versions. But DLT pipelines have their own state (checkpoints). You need to: restore the table, reset the pipeline state, and re-process.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- STEP 1: Identify when corruption occurred
DESCRIBE HISTORY production.fintech.silver_customers;
-- Find the version BEFORE the corruption (e.g., version 42 was last good state)

-- STEP 2: Verify the good version
SELECT COUNT(*) FROM production.fintech.silver_customers VERSION AS OF 42;
SELECT * FROM production.fintech.silver_customers VERSION AS OF 42
WHERE customer_id IN (SELECT customer_id FROM known_affected_ids);
-- Confirm version 42 has correct data

-- STEP 3: Restore the table
RESTORE TABLE production.fintech.silver_customers TO VERSION AS OF 42;
-- Table is now back to the good state

-- STEP 4: Fix downstream gold tables (they consumed bad data)
-- Option A: Full refresh of gold tables
-- In DLT pipeline UI: select gold tables → "Full Refresh"

-- Option B: Restore gold tables too (if you know the good version)
RESTORE TABLE production.fintech.gold_daily_balances TO VERSION AS OF 38;
```

```python
# STEP 5: Prevent the bad CDC events from re-corrupting on next run
# The DLT pipeline checkpoint will try to reprocess events after version 42

# Option A: Fix the source (remove bad CDC events from the landing zone)
# Delete or quarantine the bad files from s3://lake/landing/cdc/customers/

# Option B: Add a quality gate to prevent this in future
@dlt.table
@dlt.expect_or_fail("no_null_overwrites", 
    "NOT (op = 'u' AND name IS NULL AND email IS NULL)")  # Block null-overwrite updates
def silver_customers():
    # ...

# STEP 6: Resume the pipeline
# DLT pipeline will process from where it left off (post-restore)
# If bad files are removed: clean processing
# If bad files remain but expectation added: pipeline fails safely on bad events

# STEP 7: Post-mortem
# - Root cause: upstream sent malformed CDC events (nulls for all fields on UPDATE)
# - Fix: added expect_or_fail for null-overwrite pattern
# - Monitoring: alert if silver_customers null rate spikes above 0.1%
```

**Key Points:**
- Delta time travel is your safety net (RESTORE to any previous version)
- Restore is instant (changes metadata pointer, doesn't copy data)
- After restore: fix the SOURCE of corruption before re-running pipeline
- Add expectations to prevent the same corruption pattern in future
- Gold tables may need full refresh (they consumed bad intermediate data)
- DLT checkpoints: after RESTORE, pipeline processes from its last checkpoint
- If checkpoint is ahead of restored version: may need pipeline full refresh
- Post-mortem: always add a quality expectation to catch the failure pattern

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cost Optimization

**Scenario:** Your DLT continuous pipeline costs $8K/month (always-on cluster). The business only needs data freshness <5 minutes during business hours (8 AM - 6 PM) and <1 hour after hours. Optimize costs while meeting SLAs.

<details>
<summary>💡 Hint</summary>
Replace single continuous pipeline with two modes: triggered every 5 min during business hours, triggered every 60 min after hours. Use Databricks Workflows with cron scheduling.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT: Continuous pipeline, 24/7 cluster = $8K/month
# OPTIMIZATION: Triggered mode with variable frequency

# Approach 1: Two Databricks Workflows with different schedules

# Business hours (8 AM - 6 PM, weekdays): every 5 minutes
# Cron: */5 8-17 * * 1-5
# 10 hours × 12 runs/hr = 120 runs/day × 5 weekdays = 600 runs/week
# Each run: ~3 min processing + ~2 min cluster startup = 5 min
# Cluster active: 600 × 5 min = 3000 min/week = 50 hrs/week

# After hours (nights + weekends): every 60 minutes
# Cron: 0 * * * *  (but only outside business hours)
# 14 hours/night × 7 days + weekends = ~130 runs/week
# Each run: ~3 min
# Cluster active: 130 × 5 min = 650 min/week = 11 hrs/week

# TOTAL cluster time: 61 hrs/week × 4.3 weeks = 262 hrs/month
# vs Continuous: 730 hrs/month
# Savings: 64% less compute time!

# Cost calculation:
# Cluster: 8 workers × i3.xlarge ($0.312/hr on-demand + $0.15 DBU/hr)
# Per hour: 8 × ($0.312 + $0.15) = $3.70/hr
# Continuous: $3.70 × 730 = $2,701/month (compute only, + DBU overhead = ~$8K)
# Optimized: $3.70 × 262 = $969/month (compute only, + DBU = ~$2.9K)
# SAVINGS: ~$5K/month (63% reduction)

# PIPELINE CONFIGURATION (same DLT code, different trigger):
BUSINESS_HOURS_CONFIG = {
    "name": "etl_business_hours",
    "continuous": False,  # Triggered mode
    "target": "production.analytics",
    "clusters": [{"autoscale": {"min_workers": 4, "max_workers": 8}}],
}

AFTER_HOURS_CONFIG = {
    "name": "etl_after_hours",
    "continuous": False,
    "target": "production.analytics",
    "clusters": [{"num_workers": 2}],  # Smaller cluster off-hours
}

# Both pipelines use the SAME notebooks/code and SAME checkpoint
# Just different schedules and cluster sizes
```

**Key Points:**
- Continuous mode costs 730 hrs/month; triggered with scheduling costs ~262 hrs/month (64% savings)
- Same DLT code works in both modes (no code changes needed)
- Variable frequency: tight SLA during business hours, relaxed after hours
- Smaller cluster after hours (lower volume, can afford smaller compute)
- SLA met: 5-min freshness during business hours (5 min trigger interval)
- Trade-off: cluster startup adds ~2 min per run (acceptable for 5-min SLA)
- Alternative: Serverless DLT (preview) — auto-scales to zero between runs

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Pipeline Architecture

**Scenario:** Your organization has 15 data sources feeding into a lakehouse. Design the DLT pipeline architecture: should you have 1 monolithic pipeline, 15 separate pipelines, or something in between?

<details>
<summary>💡 Hint</summary>
Consider: failure isolation (one source failing shouldn't block others), different SLAs per source, team ownership, cluster sizing, and operational complexity. Find the right granularity.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ANTI-PATTERN: One monolithic pipeline for everything
# Problems:
# - One bad source file stops ALL processing
# - Can't have different SLAs per source (all or nothing)
# - Hard to debug (15 sources in one pipeline)
# - Can't scale independently (one cluster size for all)

# ANTI-PATTERN: 15 completely separate pipelines
# Problems:
# - 15 clusters to manage (expensive, operational overhead)
# - Hard to coordinate cross-source dependencies
# - Duplicated configuration and code

# RECOMMENDED: Grouped by cadence and domain (3-5 pipelines)

PIPELINE_ARCHITECTURE = {
    "pipeline_1_realtime_ingestion": {
        "mode": "continuous",
        "sources": ["kafka_events", "api_webhooks", "cdc_orders"],
        "output": "bronze tables only",
        "rationale": "Real-time sources grouped together, <1 min latency SLA",
        "cluster": "4-8 workers, always on",
    },
    "pipeline_2_batch_ingestion": {
        "mode": "triggered (every 15 min)",
        "sources": ["partner_files", "sftp_drops", "s3_exports", "email_attachments"],
        "output": "bronze tables only",
        "rationale": "Batch sources with relaxed SLA (15-60 min)",
        "cluster": "2-4 workers, auto-terminates",
    },
    "pipeline_3_silver_transform": {
        "mode": "triggered (every 15 min)",
        "sources": ["reads from all bronze tables"],
        "output": "silver tables (cleaned, typed, deduped)",
        "rationale": "All transformation logic in one place, runs after ingestion",
        "cluster": "4-8 workers, auto-terminates",
    },
    "pipeline_4_gold_analytics": {
        "mode": "triggered (hourly)",
        "sources": ["reads from silver tables"],
        "output": "gold tables (aggregations, metrics)",
        "rationale": "Business metrics refreshed hourly (sufficient for dashboards)",
        "cluster": "2-4 workers, auto-terminates",
    },
    "pipeline_5_ml_features": {
        "mode": "triggered (daily)",
        "sources": ["reads from silver + gold"],
        "output": "ML feature tables",
        "rationale": "Feature computation is expensive, daily is sufficient",
        "cluster": "8-16 workers (large compute for ML features)",
    },
}

# BENEFITS of this architecture:
# 1. Failure isolation: ingestion failure doesn't block transformation
# 2. Different SLAs: real-time (continuous), batch (15 min), analytics (hourly)
# 3. Cost optimization: each pipeline's cluster sized for its workload
# 4. Team ownership: ingestion team owns 1+2, analytics team owns 3+4, ML team owns 5
# 5. Independent scaling: ML features can use big cluster without affecting real-time

# DEPENDENCY MANAGEMENT:
# Pipeline 3 reads from tables produced by Pipeline 1 and 2
# (reads from Unity Catalog tables, not internal dlt.read())
# Workflow dependency: Pipeline 3 runs AFTER Pipelines 1+2 complete
```

**Key Points:**
- Group by: cadence (continuous/hourly/daily) and domain (ingestion/transform/serving)
- 3-5 pipelines is the sweet spot for most organizations (balance isolation vs complexity)
- Separate ingestion from transformation (different failure modes, different SLAs)
- Each pipeline can have different cluster sizes and scaling policies
- Use Databricks Workflows to orchestrate dependencies between pipelines
- Cross-pipeline dependencies use Unity Catalog tables (not internal DLT references)
- Monitor each pipeline independently (different alerting thresholds per SLA)

</details>

</article>
