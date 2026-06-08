---
title: "Storage Optimization — Scenarios"
topic: data-lakehouse
subtopic: storage-optimization
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [storage, scenarios, interview, optimization, cost]
---

# Storage Optimization — Interview Scenarios

## Scenario 1: Fix a Poorly Partitioned Lakehouse Table

**Question:** A Delta table `events` was partitioned by `(user_id, event_date)`. After 6 months, there are 50M partitions (10M users × 180 days), each containing 1-5 records. Queries like `SELECT COUNT(*) FROM events WHERE event_date = '2024-01-15'` take 45 minutes. Fix this.

**Answer:**

```
Root cause analysis:
  50M partitions = 50M S3 prefix paths
  Query filters by event_date (not user_id) → must scan all 10M user prefixes for one day
  Each partition: 1-5 records × ~200 bytes = ~1KB per partition
  50M × 1KB = 50GB stored as 50M tiny files
  Spark: 50M files × 1 task/file = 50M Spark tasks → massive scheduling overhead

Fix plan:

Step 1: Re-partition by event_date only (drop user_id from partition key)
  new_df = spark.read.format("delta").load("s3://bucket/events")
  new_df.write.format("delta") \
      .partitionBy("event_date") \  # only event_date
      .option("overwriteSchema", "true") \
      .mode("overwrite") \
      .save("s3://bucket/events_v2")

Step 2: OPTIMIZE + ZORDER on new table
  spark.sql("""
    OPTIMIZE delta.`s3://bucket/events_v2`
    ZORDER BY (user_id)
  """)
  -- After OPTIMIZE: 1 day's events (1M × 200 bytes = 200MB) → 2 × 128MB files
  -- Z-order by user_id: queries like WHERE user_id=123 AND event_date='...' are efficient

Step 3: Validate
  old_count = spark.read.format("delta").load("s3://bucket/events").count()
  new_count = spark.read.format("delta").load("s3://bucket/events_v2").count()
  assert old_count == new_count

Step 4: Redirect consumers
  -- Update all downstream jobs and dashboards to s3://bucket/events_v2
  -- Keep v1 read-only for 30 days (rollback safety)
  -- After 30 days: VACUUM v1, delete bucket prefix

Expected improvement:
  Query: SELECT COUNT(*) FROM events WHERE event_date='2024-01-15'
  Before: 45 min (50M S3 GETs, 50M Spark tasks)
  After:  5 seconds (1 partition, 2 files, 2 Spark tasks)
  
  Storage:
  Before: 50MB (data) + 10GB overhead (S3 metadata for 50M objects)
  After:  50MB data + 2 files overhead (negligible)
  
  Cost reduction: 200× fewer S3 requests per query
```

---

## Scenario 2: Reduce a $50K/Month S3 Bill

**Question:** Your company spends $50,000/month on S3 for a 2PB data lake. Breakdown: Bronze 800TB, Silver 600TB, Gold 200TB, checkpoints/temp 400TB. After 2 years, most Bronze data is never accessed. Propose a cost reduction plan.

**Answer:**

```
Current cost breakdown:
  Bronze 800TB × $0.023/GB = $18,400/month
  Silver 600TB × $0.023/GB = $13,800/month
  Gold 200TB × $0.023/GB = $4,600/month
  Temp/checkpoints 400TB × $0.023/GB = $9,200/month
  Total: ~$46,000/month + request/transfer charges ≈ $50,000

Optimization plan:

Action 1: Tiered storage for Bronze (highest impact)
  Bronze > 90 days old → Standard-IA ($0.0125)
  Bronze > 365 days old → Glacier Instant ($0.004)
  Savings estimate:
    800TB Bronze: 200TB recent (Standard), 600TB old (split IA/Glacier)
    300TB → Standard-IA: 300TB × $0.0125 = $3,750 (was $6,900) → save $3,150
    300TB → Glacier IR:  300TB × $0.004 = $1,200 (was $6,900) → save $5,700
  Bronze savings: ~$8,850/month

Action 2: Tiered storage for Silver
  Silver > 180 days → Standard-IA
  Savings: 300TB old × ($0.023 - $0.0125) = $3,150/month

Action 3: Clean up checkpoints and temp (lowest-hanging fruit)
  Temp files: set TTL 7 days (lifecycle rule: Expiration: {Days: 7})
  Old checkpoints: set TTL 30 days
  Savings: 400TB × $0.023 × 80% reduction = $7,360/month (most temp is old)

Action 4: Compress uncompressed Bronze files
  If Bronze ingested as JSON/CSV, convert to Parquet+Zstd
  10× compression: 800TB → 80TB (real data, now stored at lower tier)
  This is a one-time compute cost but permanent storage savings
  Savings after compression: (720TB eliminated) × $0.023 = $16,560/month ← huge

Action 5: VACUUM and orphan cleanup
  Run VACUUM on all Delta tables with RETAIN 168 HOURS
  Removes orphan files from failed jobs, old compaction artifacts
  Estimate: 5-10% reduction in each zone

Total projected savings:
  Actions 1-3: $19,360/month (immediate, no compute needed)
  Action 4: $16,560/month (after compression job, 1-2 month project)
  Action 5: ~$2,300/month (VACUUM cleanup)
  
  Total: ~$38,220/month savings → new bill: ~$11,780/month (76% reduction)
  
  Timeline: Actions 1-3 + 5 deployable in 1 week; Action 4 requires 1-2 months
```

---

## Scenario 3: Design Storage Layout for 10B Events/Day

**Question:** A ride-sharing company generates 10 billion ride events per day. Events include: ride_id, driver_id, rider_id, pickup_lat, pickup_lng, status, amount, timestamp. Design the lakehouse storage layout to support: (1) dashboard queries by city + day, (2) per-driver earnings queries, (3) ML training on full historical data, (4) real-time fraud detection (last 5 minutes).

**Answer:**

```
Scale calculation:
  10B events × 500 bytes avg = 5TB/day
  Annual growth: 5TB × 365 = 1.8PB/year

Storage layout:

Bronze Zone (s3://lake/bronze/rides/):
  Partition by: ingest_date (one day per partition)
  Format: Parquet + Snappy (preserve as-received)
  File size: ~128MB (Flink writes with target size)
  Daily size: 5TB → ~40,000 files per day
  Lifecycle: Standard 90d → Glacier IR 2y → Deep Archive forever
  Compaction: OPTIMIZE weekly on last 7 days

Silver Zone (s3://lake/silver/rides/):
  Partition by: event_date, city_code
  (city_code reduces partition size: 200 cities × 25GB/day = 25GB/city/day → manageable)
  Format: Delta/Iceberg + Zstd
  Compaction: OPTIMIZE daily with ZORDER BY (driver_id, rider_id)
  Why ZORDER: dashboard queries filter city+date (partition), but also driver_id (ZORDER)
  Daily size: 3TB (Zstd compression from 5TB)

Gold Zone (s3://lake/gold/):
  gold/city_daily_metrics/ → partitioned by (city_code, event_date)
    Columns: city, rides, revenue, avg_wait_time, etc.
    Size: small (aggregated), 10GB/day
  
  gold/driver_earnings/ → partitioned by (event_date)
    Columns: driver_id, city, hours_online, rides, earnings
    Z-ordered by driver_id
    Size: 500GB/day
  
  gold/rider_activity/ → partitioned by (event_date)
    Z-ordered by rider_id
    Size: 200GB/day

ML Zone (s3://lake/ml/):
  Point-in-time correct feature snapshots (weekly)
  Partitioned by snapshot_date
  Format: Parquet + Zstd (ML reads don't benefit from Delta overhead)

Real-time (s3://lake/realtime/fraud_window/):
  Last 5 minutes of events only (TTL: 10 minutes)
  Written by Flink (append, 1-sec checkpoint)
  Read by fraud detection Redis cache loader
  Size: 5min × 5TB/day / 1440min = ~17GB
  Lifecycle: Expiration 15 minutes

Query engine mapping:
  City+date dashboard: Trino → gold/city_daily_metrics/ (seconds)
  Driver earnings query: Trino → gold/driver_earnings/ + ZORDER = fast
  ML training: Spark → ml/features/ (weekly batch job)
  Fraud detection: Flink → Bronze stream → Redis
```
