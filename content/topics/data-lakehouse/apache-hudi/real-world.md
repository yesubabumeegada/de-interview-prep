---
title: "Apache Hudi — Real World"
topic: data-lakehouse
subtopic: apache-hudi
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [hudi, production, uber, aws, cdc-pipeline]
---

# Apache Hudi — Real World

## Uber's Original Use Case

```
Problem: Uber's trip data pipeline circa 2016
  500M+ trips/day
  Each trip record updated 20-30 times (status changes: booked→active→completed→rated)
  Stored in HDFS as plain Parquet
  Full table rewrite on every update: 10+ hours for daily batch

Hudi solution:
  Write operation: UPSERT (updates existing record by trip_id)
  Table type: MOR (fast writes, async compaction)
  Result: update latency reduced from hours to minutes

Architecture:
  MySQL (OLTP) → Kafka → Spark Streaming → Hudi (MOR) → Hive/Presto (analytics)
  
  Incremental consumers:
    Downstream ETL jobs use incremental query (only read changed trips)
    Example: billing pipeline reads only trips with status='completed' in last commit
    This reduced downstream job runtime from 4 hours → 15 minutes
```

---

## Pattern: Delta Lake → Hudi Migration

```
Scenario: AWS-first team migrating from Delta to Hudi for better CDC performance

Reason: Delta MERGE at high frequency (10M CDC events/hour) was causing write amplification
Hudi MOR avoids full file rewrites

Migration Plan:
  Phase 1: Run Hudi in parallel (dual write to Delta and Hudi)
    Validate row counts and data quality for 2 weeks
    
  Phase 2: Switch downstream jobs to Hudi incremental reads
    Monitor: query latency, data freshness, compaction lag
    
  Phase 3: Cut off Delta writes, Delta tables become read-only archive
  
  Data migration (initial load):
    spark.read.format("delta").load("s3://bucket/delta/orders") \
        .write.format("hudi") \
        .options(**hudi_upsert_options) \
        .mode("overwrite") \
        .save("s3://bucket/hudi/orders")
  
  Gotcha: Delta doesn't have _hoodie_commit_time
  Solution: use current_timestamp() as initial commit baseline
  All records get commit_time = migration time; start incremental reads from there
```

---

## Pattern: Production Hudi + Airflow

```python
# Airflow DAG for Hudi pipeline maintenance

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from datetime import datetime

HUDI_TABLES = [
    ("s3://bucket/hudi/orders", "hudi_db", "orders"),
    ("s3://bucket/hudi/customers", "hudi_db", "customers"),
    ("s3://bucket/hudi/events", "hudi_db", "events"),
]

with DAG("hudi_maintenance", schedule_interval="0 3 * * *", start_date=datetime(2024,1,1)) as dag:
    
    for table_path, db, table in HUDI_TABLES:
        # Compaction step (MOR tables)
        compaction = EmrAddStepsOperator(
            task_id=f"compact_{table}",
            job_flow_id="{{ var.value.emr_cluster_id }}",
            steps=[{
                "Name": f"Compact {table}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--class", "org.apache.hudi.utilities.HoodieCompactor",
                        "s3://bucket/jars/hudi-utilities.jar",
                        "--base-path", table_path,
                        "--table-name", table,
                        "--schema-file", f"s3://bucket/schemas/{table}.avsc",
                        "--spark-memory", "4g",
                    ]
                }
            }]
        )
        
        # Clean step
        clean = EmrAddStepsOperator(
            task_id=f"clean_{table}",
            job_flow_id="{{ var.value.emr_cluster_id }}",
            steps=[{
                "Name": f"Clean {table}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--class", "org.apache.hudi.utilities.HoodieCleaner",
                        "s3://bucket/jars/hudi-utilities.jar",
                        "--base-path", table_path,
                    ]
                }
            }]
        )
        
        compaction >> clean
```

---

## Pattern: Hudi + Athena for Ad-Hoc Queries

```sql
-- Hudi COW tables are queryable from Athena directly
-- (Athena reads COW as plain Parquet)

-- 1. Hudi sync to Glue catalog (happens during Spark write with sync options)
-- 2. Athena query (snapshot query — latest state)
SELECT 
    order_id,
    customer_id,
    amount,
    status,
    _hoodie_commit_time
FROM hudi_db.orders
WHERE order_date = '2024-01-15'
  AND status = 'delivered'
ORDER BY _hoodie_commit_time DESC;

-- Athena + MOR: reads read-optimized view (base files only, not log deltas)
-- For fully merged view from Athena: run compaction first, then query

-- Incremental (Athena doesn't support natively — use Spark incremental query)
-- Workaround: Athena time filter via _hoodie_commit_time
SELECT * FROM hudi_db.orders
WHERE _hoodie_commit_time > '20240115120000'
  AND order_date = '2024-01-15'  -- still need partition filter for partition pruning
```

---

## Interview Tips

> **Tip 1:** "How do you monitor Hudi pipeline health in production?" — Key metrics: (1) MOR compaction lag — how many delta commits since last compaction (alert if > 10); (2) upsert latency per batch; (3) index lookup latency (HBase/bloom filter); (4) S3 request count (metadata table should reduce this; alert if LIST calls are high); (5) data freshness — `MAX(_hoodie_commit_time)` should be recent. Use AWS CloudWatch or Databricks Metrics for Spark job duration.

> **Tip 2:** "What's the trade-off between inline compaction vs async compaction?" — Inline compaction triggers during the write Spark job (same cluster, adds latency to each write). Async compaction runs as a separate Spark job (separate cluster, does not block writes). Production recommendation: async compaction for streaming pipelines (don't let compaction stall the stream), inline for batch pipelines (simpler, one job to manage). Monitor: if async compaction can't keep up with write velocity → upgrade to larger compaction cluster or increase frequency.

> **Tip 3:** "How does Hudi handle duplicate records in the source stream?" — The `precombinekey.field` is the deduplication mechanism. When multiple records arrive with the same `recordkey`, Hudi keeps the one with the highest `precombinekey` value (usually `updated_at` timestamp). If two records have the same key AND same precombinekey → Hudi keeps the last one seen (non-deterministic). Best practice: always use a monotonically increasing precombinekey (event_time, sequence_number, updated_at with microsecond precision).
