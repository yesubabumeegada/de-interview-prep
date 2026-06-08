---
title: "Apache Hudi — Scenarios"
topic: data-lakehouse
subtopic: apache-hudi
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [hudi, scenarios, interview, cdc, compaction]
---

# Apache Hudi — Interview Scenarios

## Scenario 1: Choose Between COW and MOR for a CDC Pipeline

**Question:** Your team needs to replicate a MySQL customer table (10M rows, 50K updates/hour) to a Hudi table for analytics. The analytics queries run every 5 minutes on the full customer table. Should you use COW or MOR?

**Answer:**

```
Analysis:

Updates: 50K/hour = ~14 updates/second
Table size: 10M rows
Query frequency: every 5 minutes (12 queries/hour)

COW Analysis:
  Each update: Hudi must find the file containing the record, rewrite entire 128MB file
  50K updates/hour across 10M rows = ~0.5% of records updated each hour
  File rewrites: if 10M rows × avg 1KB per row = 10GB total, 128MB files = ~80 files
  At 0.5% update rate: ~40 files rewritten per hour
  Each file rewrite: reads 128MB + writes 128MB = 256MB I/O per file
  Total I/O: 40 × 256MB = ~10GB/hour → manageable
  
  Read performance: excellent (plain Parquet, no merge)
  
MOR Analysis:
  Each update: append 14 records/second to delta log → ~50MB delta log per hour
  Read: must merge base Parquet + delta log per file → adds 10-20% read latency
  Compaction needed every few hours to keep reads fast
  
Recommendation: COW
  Reasoning:
  - 10GB/hour write I/O for COW is acceptable (modern S3 handles 5+ GB/s)
  - Query runs every 5 minutes → read latency is critical (COW wins)
  - 50K updates/hour is moderate — not the extreme scale where MOR is essential
  - Simpler to operate (no compaction scheduling needed)
  
  Use MOR if: updates exceed 500K/hour (5%+ of table updated per hour)
  Or if: write latency SLA < 1 second (COW rewrite may take 2-5 seconds per file)
```

---

## Scenario 2: Hudi Compaction Is Behind — Queries Are Slow

**Question:** A Hudi MOR table for order events has been running for 6 months. Queries that used to take 10 seconds now take 4 minutes. The compaction job was supposed to run nightly but has been failing for 2 weeks (OOM errors). How do you fix this?

**Answer:**

```
Diagnosis:
  Check delta commit count:
    SHOW COMMITS IN hudi.`s3://bucket/hudi/orders`;
    -- 14 delta commits without compaction (2 weeks × 1/day writes)
    -- Each query must merge 14 delta log files per file group → slow

  Check compaction queue:
    SHOW COMPACTION ON hudi.`s3://bucket/hudi/orders`;
    -- Pending compaction plan: 500 file groups to compact

  Why OOM: Spark is trying to compact all 500 file groups in one job
    500 files × 128MB each = 64GB in memory → exceeds executor heap

Fix Strategy:
  Step 1: Emergency read workaround
    -- Switch BI queries to read-optimized view (no log merge, stale by 2 weeks but fast)
    spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "read_optimized") \
        .load("s3://bucket/hudi/orders")
    Notify users: "data is ~2 weeks stale while we fix compaction"

  Step 2: Fix compaction OOM — compact in batches
    # Limit compaction to 50 file groups per run
    hudi_compact_options = {
        "hoodie.compact.inline.max.delta.commits": "2",
        "hoodie.compaction.strategy": "org.apache.hudi.table.action.compact.strategy.BoundedIOCompactionStrategy",
        "hoodie.compaction.target.io": str(10 * 1024 * 1024 * 1024),  # 10GB per run
    }
    
    # Run 5 compaction rounds until caught up
    for i in range(5):
        spark.sql(f"CALL hudi.system.run_compaction(table => 'hudi.orders')")
        time.sleep(300)  # wait between rounds

  Step 3: Prevent recurrence
    -- Switch to inline compaction (compact every 3 delta commits)
    hudi_write_options["hoodie.compact.inline"] = "true"
    hudi_write_options["hoodie.compact.inline.max.delta.commits"] = "3"
    
    -- Monitor: alert if pending compaction file groups > 20
    def check_compaction_lag(spark, table_path):
        pending = spark.sql(f"""
            SELECT COUNT(*) FROM hudi.`{table_path}`.pending_compactions
        """).collect()[0][0]
        if pending > 20:
            send_alert(f"WARN: {pending} file groups pending compaction in {table_path}")
```

---

## Scenario 3: Hudi Incremental Pipeline Design

**Question:** You have a Hudi MOR table `orders` that receives 1M new/updated records per hour. You need to build a downstream Gold table `hourly_order_summary` that aggregates orders by region and status, updated every 15 minutes. Design the incremental pipeline.

**Answer:**

```
Design: Incremental Hudi → Gold Aggregation Pipeline

The key insight: don't read the full orders table every 15 min (100M+ rows)
Instead: read ONLY records changed in the last 15 minutes via incremental query

Pipeline:
  Trigger: every 15 minutes (Airflow sensor or Databricks job)

  Step 1: Read state (last processed commit time)
    last_commit = spark.sql("""
        SELECT watermark FROM pipeline_watermarks
        WHERE pipeline = 'orders_to_gold'
    """).collect()[0][0]
    # e.g., "20240115120000"

  Step 2: Incremental read from Hudi
    changed_orders = spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "incremental") \
        .option("hoodie.datasource.read.begin.instanttime", last_commit) \
        .load("s3://bucket/hudi/orders")
    # Returns ONLY records with commit_time > last_commit
    # Efficient: reads only changed files, not full table

  Step 3: Aggregate changed records
    changed_agg = changed_orders.groupBy("region", "status").agg(
        {"amount": "sum", "order_id": "count"}
    )

  Step 4: MERGE into Gold table (upsert aggregation)
    DeltaTable.forPath(spark, "s3://bucket/gold/hourly_order_summary") \
        .alias("t").merge(changed_agg.alias("s"), "t.region=s.region AND t.status=s.status") \
        .whenMatchedUpdate(set={"total_amount": "t.total_amount + s.sum(amount)",
                                "order_count": "t.order_count + s.count(order_id)"}) \
        .whenNotMatchedInsert(values={"region": "s.region", "status": "s.status",
                                      "total_amount": "s.sum(amount)",
                                      "order_count": "s.count(order_id)"}) \
        .execute()

  Step 5: Save new watermark
    new_commit = changed_orders.select(max("_hoodie_commit_time")).collect()[0][0]
    spark.sql(f"""
        UPDATE pipeline_watermarks SET watermark = '{new_commit}'
        WHERE pipeline = 'orders_to_gold'
    """)

Performance: 
  Without incremental: scan 100M rows every 15 min → ~10 min Spark job
  With incremental: scan ~250K changed rows → ~30 sec Spark job
  Throughput improvement: 20× faster, 20× cheaper
```
