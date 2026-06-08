---
title: "Query Engines (Trino, Spark, Flink) — Scenarios"
topic: data-lakehouse
subtopic: query-engines
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [trino, spark, flink, scenarios, interview, design]
---

# Query Engines — Interview Scenarios

## Scenario 1: Choose Query Engines for a New Data Platform

**Question:** A fintech company is building a new data platform on AWS. Requirements: (1) 20 analysts run SQL queries, expecting results in < 5 seconds; (2) 3 data engineers run heavy ETL jobs (100GB+ daily); (3) A fraud detection team needs < 500ms alert latency on transactions; (4) ML team trains models on 1 year of feature data weekly. Design the query engine architecture.

**Answer:**

```
Analysis:
  Analysts (<5 sec SQL): → Trino
  Heavy ETL (100GB+): → Spark
  <500ms fraud alerts: → Flink
  ML training (weekly batch): → Spark

Architecture:

Trino (interactive analytics):
  Cluster: 1 coordinator (m5.2xlarge) + 6 workers (m5.4xlarge)
  Catalog: Iceberg on S3 + Glue
  Access: Tableau, Looker, direct SQL via client
  Scale: auto-scale workers (2–10) based on concurrent query count
  Cost: ~$1,500/month (6 × m5.4xlarge × 730h × $0.288/h × 70% utilization)

Spark (ETL + ML training):
  Cluster: EMR with auto-scaling (1 driver m5.2xlarge + 5–20 workers m5.xlarge spot)
  Trigger: Airflow DAGs (ETL at 5 AM; ML training on Sundays at midnight)
  Spot workers: 70% cost savings vs on-demand
  Cost: ~$800/month (burst ETL compute, not always-on)

Flink (fraud detection):
  Cluster: Kinesis Data Analytics (managed Flink) or EKS-hosted Flink
  Source: Kafka (transaction events)
  Sink: Kinesis Data Streams → Lambda (alert push to fraud team)
  State backend: RocksDB (incremental checkpoints to S3)
  Cost: ~$500/month (2 KPUs Kinesis Data Analytics)

Data flow:
  Kafka transactions → Flink (<500ms fraud alerts) → Kinesis → Lambda
  Kafka transactions → Spark → Bronze/Silver/Gold Iceberg (hourly)
  Silver/Gold Iceberg → Trino → Analysts/Dashboards
  Silver Iceberg → Spark → ML features → Gold Iceberg → ML training

Total compute cost: ~$2,800/month (vs Databricks: ~$5,000/month for same workload)
Trade-off: more ops overhead managing 3 engines vs Databricks all-in-one
Recommendation: use Databricks if team < 5 DE; use this split if team > 5 DE and cost-sensitive
```

---

## Scenario 2: Spark Job OOM — Debug and Fix

**Question:** A Spark ETL job that joins `orders` (500M rows) with `customers` (10M rows) and `products` (1M rows) is failing with OOM (OutOfMemoryError) on executors. The cluster has 20 × 8GB executors (160GB total). The query is: `SELECT o.*, c.name, c.region, p.category FROM orders o JOIN customers c ON o.customer_id=c.id JOIN products p ON o.product_id=p.id WHERE o.order_date='2024-01-15'`

**Answer:**

```
Diagnosis:
  orders: 500M rows × 200 bytes avg = 100GB
  orders for Jan 15: partition pruning → ~270K rows (1/365 × 100GB ≈ 280MB)
  customers: 10M rows × 100 bytes = 1GB
  products: 1M rows × 50 bytes = 50MB

  After filter: 280MB orders × 1GB customers = 1.28GB shuffle
  Should easily fit in 160GB cluster
  
  Root cause hypothesis: filter not being pushed down before join
  Check: df.explain("extended") → look for Filter position relative to Join

Step 1: Verify partition pruning
  val plan = spark.sql(query).queryExecution.executedPlan
  println(plan)
  -- If you see: Join → [orders full scan, customers full scan]
  -- Filter is happening AFTER join (catastrophic: 500M × 10M cross product)
  
  Fix: ensure filter is applied before join
  orders_filtered = spark.read.format("iceberg").load("s3://bucket/orders") \
      .filter("order_date = '2024-01-15'")  # explicit filter before join

Step 2: Broadcast small tables
  from pyspark.sql.functions import broadcast
  
  result = orders_filtered \
      .join(broadcast(customers_df), "customer_id") \  # broadcast 1GB → all executors
      .join(broadcast(products_df), "product_id")     # broadcast 50MB → all executors
  
  -- Broadcast joins eliminate shuffle entirely
  -- No shuffle = no memory pressure from large partitions
  
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2gb")  # auto-broadcast up to 2GB

Step 3: Enable AQE (catch remaining issues)
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  -- AQE will: auto-broadcast smaller-than-expected tables
  --           split skewed partitions
  --           coalesce empty partitions

Result after fixes:
  Before: OOM (full table scan, sort merge join)
  After: ~30 seconds (partition pruning + broadcast join, no shuffle)
```

---

## Scenario 3: Flink Job Is Producing Duplicate Results

**Question:** A Flink streaming job reads from Kafka, aggregates revenue by region in 1-minute tumbling windows, and writes to an Iceberg table. After a failure and restart, the Iceberg table has duplicate rows for the window that was in-progress when the job crashed. How do you fix this?

**Answer:**

```
Root cause:
  Without 2PC (two-phase commit), Flink commits to Iceberg incrementally
  If job crashes after partial write but before checkpoint:
    - Some Iceberg files were written
    - Checkpoint was not committed
    - On restart: Flink re-processes from last checkpoint
    - Duplicate files written to Iceberg → duplicate rows on read

Fix 1: Use Iceberg's exactly-once sink with checkpointing
  The correct pattern: Iceberg sink commits ONLY at checkpoint boundaries
  
  In Flink, the Iceberg sink uses TwoPhaseCommitSinkFunction:
  - Pre-commit: write data files (phase 1)
  - Commit: on checkpoint success, commit Iceberg snapshot (phase 2)
  - On restart: uncommitted pre-commit files are abandoned (orphans)
  
  Ensure correct sink configuration:
  t_env.execute_sql("""
    INSERT INTO iceberg_orders
    SELECT * FROM kafka_orders
  """)
  -- Flink's Iceberg sink implements TwoPhaseCommit automatically
  -- As long as checkpointing is enabled (env.enable_checkpointing(60000))

Fix 2: Deduplicate existing duplicate data
  -- Find duplicate windows
  spark.sql("""
    SELECT window_start, region, COUNT(*) AS cnt
    FROM iceberg.gold.revenue_by_region
    GROUP BY window_start, region
    HAVING COUNT(*) > 1
  """).show()
  
  -- Delete duplicates (keep most recent write)
  spark.sql("""
    DELETE FROM iceberg.gold.revenue_by_region
    WHERE (window_start, region) IN (
      SELECT window_start, region
      FROM iceberg.gold.revenue_by_region
      GROUP BY window_start, region
      HAVING COUNT(*) > 1
    )
  """)
  -- Re-aggregate the affected windows from source data (Kafka or Bronze)

Fix 3: UPSERT instead of INSERT for idempotency
  -- Change Iceberg sink to UPSERT mode (Flink Iceberg sink option)
  -- write.upsert.enabled=true + primary key defined
  -- On re-processing: UPSERT overwrites existing rows instead of appending duplicates
  -- This makes the sink naturally idempotent

Prevention:
  Always use checkpointing + Iceberg transactional sink
  Never use Append-only sink without exactly-once guarantee
  Test recovery: deliberately kill Flink job mid-window, verify no duplicates after restart
```
