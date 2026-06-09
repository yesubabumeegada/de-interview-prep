---
title: "Pipeline Design Patterns — Scenarios"
topic: system-design
subtopic: pipeline-design-patterns
content_type: scenario_question
tags: [pipeline, design-patterns, scenarios]
---

# Pipeline Design Patterns — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: ELT vs ETL — When to Use Each

**Scenario:** Your team is debating whether to use ETL (transform before loading) or ELT (load raw, transform in-warehouse) for a new Salesforce-to-Snowflake pipeline. Explain the difference and make a recommendation.

<details>
<summary>💡 Hint</summary>

ETL transforms data before it lands in the destination — compute happens outside the warehouse. ELT loads raw data first, then transforms using the warehouse's compute. Cloud warehouses (Snowflake, BigQuery) have made ELT the default modern pattern.

</details>

<details>
<summary>✅ Solution</summary>

**ETL (Extract → Transform → Load):**
- Transform happens in an external engine (Spark, custom scripts)
- Only clean data enters the warehouse
- Required when: destination can't handle raw data, strict PII rules, data volume reduction needed

**ELT (Extract → Load → Transform):**
- Raw data lands in warehouse/lake first
- Transform using warehouse SQL (dbt, stored procedures)
- Required when: warehouse compute is cheap (Snowflake), need full raw history, rapid iteration on transformations

**Modern ELT Stack:**
```
Salesforce API → Fivetran/Airbyte → Snowflake (raw schema) → dbt → Analytics schema
```

**dbt transformation example:**
```sql
-- models/silver/salesforce_opportunities.sql
WITH raw AS (
    SELECT * FROM {{ source('salesforce', 'opportunity') }}
),
cleaned AS (
    SELECT
        id AS opportunity_id,
        accountid AS account_id,
        UPPER(TRIM(stagename)) AS stage,
        amount::DECIMAL(18,2) AS amount_usd,
        closedate AS close_date,
        _fivetran_synced AS synced_at
    FROM raw
    WHERE isdeleted = FALSE
)
SELECT * FROM cleaned
```

**Recommendation:** ELT with dbt for Salesforce → Snowflake. Fivetran handles the extract+load; dbt handles all transformations in SQL. Raw data is preserved for reprocessing.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Idempotent Pipeline Design

**Scenario:** Your nightly Spark pipeline occasionally re-runs due to infrastructure failures. On re-run, it duplicates data in the output table. How do you redesign the pipeline to be idempotent — safe to run multiple times with the same result?

<details>
<summary>💡 Hint</summary>

Idempotency means running a job N times produces the same result as running it once. Key techniques: partition overwrite (not append), MERGE INTO for upserts, deterministic job IDs, and deduplication on read.

</details>

<details>
<summary>✅ Solution</summary>

**Anti-Pattern (current — not idempotent):**
```python
# BAD: append on every run creates duplicates
df.write.mode("append").parquet("s3://output/orders/")
```

**Pattern 1: Partition Overwrite**
```python
# GOOD: overwrite specific partition on each run
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.withColumn("process_date", lit(run_date))   .write   .partitionBy("process_date")   .mode("overwrite")   .parquet("s3://output/orders/")
# Re-running for same date overwrites only that partition
```

**Pattern 2: MERGE INTO (Upsert)**
```python
# GOOD: upsert on primary key — safe to re-run
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://output/orders/")
source = spark.read.parquet("s3://staging/orders/")

target.alias("t").merge(
    source.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll()  .whenNotMatchedInsertAll()  .execute()
```

**Pattern 3: Write-Audit-Publish**
```python
def run_pipeline(run_id: str, run_date: str):
    # 1. Write to staging
    staging_path = f"s3://staging/orders/{run_id}/"
    df.write.mode("overwrite").parquet(staging_path)

    # 2. Audit: validate row counts, nulls
    staged = spark.read.parquet(staging_path)
    assert staged.count() > 0, "Empty output!"
    assert staged.filter("order_id IS NULL").count() == 0

    # 3. Publish: atomic move to production path
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    staged.write.partitionBy("order_date").mode("overwrite")         .parquet("s3://prod/orders/")

    # 4. Cleanup staging
    cleanup_staging(staging_path)
```

**Pattern 4: Job Deduplication with State Store**
```python
import redis

r = redis.Redis()

def is_already_processed(run_id: str) -> bool:
    return r.exists(f"pipeline:orders:{run_id}") == 1

def mark_processed(run_id: str):
    r.set(f"pipeline:orders:{run_id}", "1", ex=7*86400)  # 7 day TTL

if not is_already_processed(run_id):
    run_pipeline(run_id, run_date)
    mark_processed(run_id)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Self-Healing Data Pipeline

**Scenario:** Your mission-critical pipeline processes $50M of financial transactions daily. It must detect and recover from: schema changes in source, partial write failures, and upstream data delays — all without manual intervention. Design the self-healing architecture.

<details>
<summary>💡 Hint</summary>

Self-healing pipelines need: schema evolution handling (detect + adapt), atomic writes (no partial state), retry with backoff, circuit breakers for cascading failures, and dead letter queues for unprocessable records.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
Source → Schema Registry → Validation Layer → Transform → Write
            |                    |                          |
        Schema drift         Dead Letter Queue          Atomic commit
        detector             (unprocessable)            (Delta/Iceberg)
            |                    |                          |
        Auto-evolve          Alert + retry              Rollback on failure
        or quarantine
```

**Component 1: Schema Drift Detection and Auto-Evolution**

```python
from pyspark.sql.types import StructType
import json

def handle_schema_evolution(new_df, target_table: str):
    current_schema = spark.table(target_table).schema
    new_schema = new_df.schema

    added_cols = set(new_schema.fieldNames()) - set(current_schema.fieldNames())
    removed_cols = set(current_schema.fieldNames()) - set(new_schema.fieldNames())

    if removed_cols:
        # Missing columns — add nulls to maintain compatibility
        from pyspark.sql.functions import lit
        for col_name in removed_cols:
            col_type = current_schema[col_name].dataType
            new_df = new_df.withColumn(col_name, lit(None).cast(col_type))
        print(f"Added null columns for removed source fields: {removed_cols}")

    if added_cols:
        # New columns — use mergeSchema
        new_df.write.format("delta")             .option("mergeSchema", "true")             .mode("append").saveAsTable(target_table)
        alert(f"Schema evolved: new columns added {added_cols}")
        return

    new_df.write.format("delta").mode("append").saveAsTable(target_table)
```

**Component 2: Dead Letter Queue for Bad Records**

```python
from pyspark.sql.functions import col, current_timestamp

def validate_and_route(df):
    valid = df.filter(
        col("transaction_id").isNotNull() &
        col("amount").between(0.01, 1_000_000) &
        col("currency").isin("USD", "EUR", "GBP")
    )

    invalid = df.subtract(valid)         .withColumn("_dlq_reason", lit("Failed validation"))         .withColumn("_dlq_timestamp", current_timestamp())

    # Write invalids to DLQ for inspection/retry
    invalid.write.format("delta").mode("append")         .saveAsTable("prod.dlq.transactions")

    if invalid.count() > df.count() * 0.05:  # >5% bad = circuit breaker
        raise ValueError(f"Too many invalid records: {invalid.count()}")

    return valid
```

**Component 3: Retry with Backoff and Circuit Breaker**

```python
import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    print(f"Attempt {attempt+1} failed: {e}. Retrying in {delay}s")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3, base_delay=60)
def run_pipeline_with_recovery(run_date: str):
    # Check upstream freshness before running
    upstream_max = spark.sql(
        "SELECT max(event_time) FROM prod.bronze.transactions"
    ).collect()[0][0]

    if upstream_max < run_date - timedelta(hours=2):
        raise ValueError(f"Upstream data is stale: latest={upstream_max}")

    process_transactions(run_date)
```

**Component 4: Monitoring and Alerting**

```python
# Airflow SLA miss callback
def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_pagerduty_alert(
        title=f"SLA Miss: {dag.dag_id}",
        details=f"Tasks: {[t.task_id for t in task_list]}",
        severity="critical"
    )

dag = DAG(
    'financial_transactions',
    sla_miss_callback=sla_miss_handler,
    default_args={'sla': timedelta(hours=2)}
)
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What makes a pipeline idempotent?" — Running it N times produces the same result as once. Key: use partition overwrite (not append), MERGE INTO for upserts, and guard with a processed-job registry.
> **Tip 2:** "What is a dead letter queue in data pipelines?" — A sink for records that fail validation or processing. Instead of dropping or crashing, bad records go to DLQ for inspection, manual correction, and replay. Essential for auditability.
> **Tip 3:** "How do you handle schema evolution in production pipelines?" — Detect schema changes at ingestion time. For additive changes (new columns), use `mergeSchema`. For breaking changes (removed/renamed required columns), alert and quarantine until the downstream schema is updated.
