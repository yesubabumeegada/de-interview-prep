---
title: "Pipeline Design Patterns — Real World"
topic: system-design
subtopic: pipeline-design-patterns
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, pipeline, production, airflow, dbt, monitoring]
---

# Pipeline Design Patterns — Real World

## Pattern 1: Production-Grade Daily Batch Pipeline

A typical e-commerce orders pipeline — from raw events to BI-ready gold table:

```
Architecture:
  Source: PostgreSQL (orders, customers, products)
  Ingestion: Fivetran or custom Airbyte → S3 raw zone (Parquet)
  Orchestration: Airflow on MWAA (managed)
  Transformation: dbt on Snowflake
  Output: Snowflake gold tables → Tableau/Looker

DAG structure (orders_pipeline):
  1. s3_sensor           → wait for today's raw file
  2. validate_raw        → row count > 0, no null PKs
  3. load_to_staging     → COPY INTO Snowflake staging
  4. dbt_run             → run dbt models (bronze → silver → gold)
  5. dq_checks           → dbt tests (not_null, unique, accepted_values)
  6. notify_success      → Slack message with row counts

SLA: gold tables updated by 7:00 AM daily
On-call: alert if pipeline not complete by 6:30 AM
```

---

## Pattern 2: Incremental Pipeline with CDC

```python
# Debezium → Kafka → Spark Streaming → Delta Lake

# Kafka consumer reading Debezium CDC events
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("op", StringType()),
    StructField("before", MapType(StringType(), StringType())),
    StructField("after", MapType(StringType(), StringType())),
    StructField("source", StructType([
        StructField("ts_ms", LongType()),
        StructField("table", StringType())
    ]))
])

df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pg.public.orders")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("cdc"))
)

# Apply CDC operations to Delta table
def upsert_to_delta(batch_df, batch_id):
    from delta.tables import DeltaTable
    dt = DeltaTable.forName(spark, "silver.orders")

    # Handle inserts and updates
    updates = (batch_df.filter("cdc.op IN ('c', 'u')")
               .select("cdc.after.*")
               .dropDuplicates(["order_id"]))
    
    (dt.alias("t")
       .merge(updates.alias("s"), "t.order_id = s.order_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())

    # Handle deletes (soft delete pattern)
    deletes = batch_df.filter("cdc.op = 'd'").select("cdc.before.order_id")
    dt.update(deletes.alias("d"), condition="t.order_id = d.order_id",
              set={"is_deleted": "true", "deleted_at": "current_timestamp()"})

(df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "s3://bucket/checkpoints/orders")
    .start())
```

---

## Pattern 3: Self-Healing Pipeline

```python
# Pipeline that automatically retries and self-diagnoses failures

# Airflow retry configuration
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,   # 5, 10, 20 minutes
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': alert_on_failure,
    'on_retry_callback': log_retry,
    'email_on_failure': True,
    'email': ['data-eng@company.com']
}

def alert_on_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    exception = context['exception']
    
    # Diagnose common failures automatically
    if "connection refused" in str(exception).lower():
        message = f"⚠️ {dag_id}/{task_id}: Database connection issue. Check VPC/firewall."
    elif "no space left" in str(exception).lower():
        message = f"🚨 {dag_id}/{task_id}: Disk full. Check S3/temp storage."
    elif "timeout" in str(exception).lower():
        message = f"⏱️ {dag_id}/{task_id}: Query timeout. Consider adding partition filter."
    else:
        message = f"❌ {dag_id}/{task_id} failed on {execution_date}: {exception}"
    
    slack_client.chat_postMessage(channel="#data-alerts", text=message)
```

---

## Common Production Issues and Fixes

| Issue | Symptoms | Root Cause | Fix |
|---|---|---|---|
| Duplicate rows in fact table | Row count increases on re-run | Non-idempotent INSERT | Switch to MERGE or partition overwrite |
| Pipeline slows down over time | Daily job goes from 10→60 min | Full scan as data grows | Add incremental watermark |
| Late data missing from reports | Yesterday's numbers change today | Late-arriving transactions | Re-process last N days' partitions nightly |
| Schema drift breaks pipeline | TypeError, column not found | Source team changed column name | Schema registry + alerting on drift |
| Cascading failures | All downstream DAGs fail | Upstream dependency unclear | Explicit ExternalTaskSensor dependencies |
| Resource contention | Jobs compete for Spark resources | No job priority | Job queues, resource pools in Spark/Airflow |

---

## Interview Tips

> **Tip 1:** "A pipeline that used to run in 10 minutes now takes 2 hours. What happened?" — Most likely: the pipeline is processing the full table instead of incrementally. Check: is the watermark being applied? Has the source data volume grown? Is there a missing partition filter causing a full scan? Run EXPLAIN PLAN on the source query. Also check for data skew (a few large partitions taking all the time) and resource contention (other jobs competing for cluster resources).

> **Tip 2:** "How do you handle a failed pipeline in production at 2am?" — The pipeline should retry automatically (3 retries with backoff). If all retries fail: (1) check the alert message for auto-diagnosed issue, (2) look at Airflow logs for the first error, (3) manually trigger a re-run with the same `execution_date` once fixed. The pipeline must be idempotent — re-running it for the same date should be safe and produce correct results. Document runbooks for the top 5 failure modes.

> **Tip 3:** "How do you manage pipeline dependencies across teams?" — Use explicit dependency declarations: Airflow ExternalTaskSensors or dataset-based triggers. Document data contracts (schema, SLA, quality expectations) between producer and consumer teams. Run schema validation in CI/CD so breaking changes are caught before deployment. Use a data catalog (DataHub, Atlan) to show lineage — so teams can see what depends on their tables before making changes.
