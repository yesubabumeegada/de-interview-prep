---
title: "Query Engines (Trino, Spark, Flink) — Real World"
topic: data-lakehouse
subtopic: query-engines
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [trino, spark, flink, production, tuning]
---

# Query Engines — Real World

## Pattern 1: Multi-Engine Lakehouse with Trino + Spark

```
Production architecture: Spark for ETL, Trino for analytics

Spark (EMR):
  Use cases:
    - Daily Silver → Gold aggregations (heavy transforms)
    - ML feature engineering (Python + Spark ML)
    - Backfill jobs (full table reprocessing)
  Cluster: 1 driver (m5.2xlarge) + 20 workers (m5.xlarge, spot)
  Triggers: Airflow DAGs at 5 AM and 2 PM

Trino (EC2 cluster or Starburst):
  Use cases:
    - Analyst ad-hoc queries
    - Tableau/Looker dashboards
    - Data exploration for new analyses
  Cluster: 1 coordinator (m5.2xlarge) + 5 workers (m5.4xlarge, always-on)
  Access: all analysts use Trino; no direct Spark access for analysts

Handoff pattern:
  Spark writes → Iceberg Silver/Gold tables (Glue catalog)
  Trino reads → same Iceberg tables (Glue catalog)
  No data movement — same S3 files serve both engines

Monitoring:
  Spark: Databricks/EMR job metrics (duration, shuffle bytes, failed tasks)
  Trino: query history (Trino UI), slow query log (> 30 sec alert)
  Alert: if Trino query > 5 min → DM analyst + auto-kill to free resources
```

---

## Pattern 2: Flink Fraud Detection Pipeline

```python
# Real-time fraud detection with Flink
# Requirement: flag transactions > $5,000 from a new device within 1 hour of first use

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.time import Time

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(20)
env.enable_checkpointing(60000)  # checkpoint every 60 seconds

t_env = StreamTableEnvironment.create(env)

# Source: Kafka transaction stream
t_env.execute_sql("""
  CREATE TABLE transactions (
    txn_id        STRING,
    customer_id   BIGINT,
    amount        DOUBLE,
    device_id     STRING,
    event_ts      TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
  ) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
  )
""")

# Stateful device tracking: first-seen timestamp per (customer, device)
# Alert: high-value transaction AND device first seen < 1 hour ago

t_env.execute_sql("""
  INSERT INTO fraud_alerts
  SELECT
    txn_id,
    customer_id,
    amount,
    device_id,
    event_ts,
    'NEW_DEVICE_HIGH_VALUE' AS alert_type
  FROM (
    SELECT
      txn_id,
      customer_id,
      amount,
      device_id,
      event_ts,
      MIN(event_ts) OVER (
        PARTITION BY customer_id, device_id
        ORDER BY event_ts
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS first_device_seen
    FROM transactions
  )
  WHERE amount > 5000
    AND event_ts - first_device_seen < INTERVAL '1' HOUR
""")
```

---

## Pattern 3: Spark Query Tuning Runbook

```python
# When a Spark job is slow, follow this diagnostic process

def diagnose_spark_job(spark, query: str):
    """
    Diagnose slow Spark queries. Run in order until root cause found.
    """
    
    print("=== Step 1: Check execution plan ===")
    spark.sql(f"EXPLAIN EXTENDED {query}").show(truncate=False)
    # Look for: PartitionFilters, DataFilters (pushdown happening?)
    # Red flags: no PartitionFilter on partitioned table = full scan
    
    print("\n=== Step 2: Run with query metrics ===")
    spark.conf.set("spark.sql.queryExecutionListeners", "")
    df = spark.sql(query)
    df.explain("cost")  # shows estimated row counts per stage
    
    # Check Spark UI (programmatic):
    sc = spark.sparkContext
    app_id = sc.applicationId
    print(f"Spark UI: http://driver:4040/jobs/")
    
    # Common issues and fixes:

    # Issue 1: Too many small tasks (thousands of tiny Parquet files)
    # Symptom: Job has 50,000 tasks, each takes 10ms
    # Fix: coalesce or repartition
    # df = df.coalesce(200)
    
    # Issue 2: Data skew in groupBy/join
    # Symptom: 199 tasks finish in 5 sec, 1 task takes 10 min
    # Fix: enable AQE skew join handling
    # spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Or: add salt to skewed key
    
    # Issue 3: Shuffle too large (shuffle write/read bytes in Spark UI)
    # Symptom: huge shuffle stage, executors hitting disk
    # Fix: increase shuffle partitions or use bucket joins
    # spark.conf.set("spark.sql.shuffle.partitions", "400")
    
    # Issue 4: GC overhead (Spark UI: GC time > 10% of task time)
    # Fix: increase executor memory or use Tungsten off-heap
    # --executor-memory 8g --conf spark.memory.fraction=0.8
    
    # Issue 5: Full S3 scan (no partition pruning)
    # Symptom: "Files: 10,000, scanned: 10,000" in query metrics
    # Fix: add WHERE clause with partition column, or add Z-ORDER
```

---

## Interview Tips

> **Tip 1:** "A Trino query on an Iceberg table suddenly got 10× slower after adding new data. What do you check?" — Check: (1) Was `ANALYZE TABLE` run recently? If new data added without updating stats, the cost-based optimizer makes bad join ordering decisions → `ANALYZE iceberg.silver.orders` to refresh; (2) Did the partition file count spike? Streaming jobs may have created thousands of small files → run `rewrite_data_files`; (3) Did the manifest file count grow? → run `rewrite_manifests`; (4) Is the new data in a different partition layout? → check `$partitions` system table.

> **Tip 2:** "How do you size a Trino cluster for 50 analysts running concurrent queries?" — Start with the P95 query resource requirement, not the average. Benchmark top 10 most-used queries, measure peak memory per query. Target: queries should complete in < 10 seconds. Size workers so total memory = (concurrent queries × peak memory per query) × 1.5 safety margin. For 50 analysts: assume 10-15 concurrent at peak. If peak query uses 8GB memory, you need 10 × 8GB × 1.5 = 120GB total worker memory. Trino 5 workers × m5.4xlarge (64GB each) = 320GB — adequate for headroom.

> **Tip 3:** "What's your approach when a Flink job has checkpoint timeouts?" — Checkpoint timeout = taking too long to snapshot state. Causes: (1) state too large (RocksDB checkpoint writes GB of data) → use incremental checkpoints; (2) GC pause blocking checkpoint → tune JVM GC (G1GC) or use off-heap state (RocksDB); (3) S3 write throttling → increase checkpoint interval to reduce write frequency, use S3 VPC endpoint; (4) Slow operators holding back barrier propagation → find the operator with highest network latency in Flink UI → optimize or scale that operator's parallelism.
