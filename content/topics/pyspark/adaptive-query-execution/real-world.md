---
title: "PySpark AQE - Real-World Production Examples"
topic: pyspark
subtopic: adaptive-query-execution
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, aqe, production, optimization, case-study]
---

# PySpark AQE — Real-World Production Examples

## Case Study 1: AQE Fixing a Skewed Join (45 min → 8 min)

**Problem:** Nightly ETL joins `fact_orders` (2B rows) with `dim_merchant` on `merchant_id`. One merchant ("Amazon") has 500M orders while the median merchant has 2K. The Amazon partition takes 42 of the 45 minutes.

**AQE automatically fixes this:**

```python
# Just enable AQE — no code changes needed!
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Same join code as before:
result = fact_orders.join(dim_merchant, "merchant_id")
result.write.parquet("s3://output/enriched_orders/")
```

**What AQE did at runtime:**
1. Shuffle by `merchant_id` → partition for "Amazon" is 40 GB
2. AQE detects: 40 GB >> median 2 MB × 5 factor → SKEWED
3. Splits the Amazon partition into 160 sub-partitions (40 GB / 256 MB each)
4. Replicates the dim_merchant Amazon row to each sub-partition
5. 160 tasks process in parallel instead of 1 task doing 40 GB

**Result:** 45 minutes → 8 minutes. Zero code changes.

---

## Case Study 2: AQE Coalescing Partitions (Small Files Fix)

**Problem:** Daily aggregation produces 200 tiny output files (200 shuffle partitions × 2 MB each = 400 MB total). Downstream Athena queries are slow due to per-file overhead.

```python
# Before AQE: 200 files × 2 MB each
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default
result = events.groupBy("event_date", "event_type").count()
result.write.parquet("s3://output/daily_counts/")
# Problem: 200 tiny files!

# After AQE: automatically coalesces to ~3 files
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
# Same code — AQE merges 200 × 2 MB partitions into 3 × 133 MB partitions
result.write.parquet("s3://output/daily_counts/")
# Result: 3 optimally-sized files (128 MB target)
```

**Spark UI shows:** "CustomShuffleReader coalesced: 200 → 3 partitions"

---

## Case Study 3: AQE Converting Sort-Merge to Broadcast

**Problem:** Query joins `fact_events` (10B rows) with a filtered view of `dim_campaign` (1M rows total, but filter reduces to 500 rows). Optimizer estimates 1M rows for dim → chooses sort-merge join. Actually only 500 rows after filter.

```python
# Optimizer sees: dim_campaign (1M rows estimated) → sort-merge join
# Reality after WHERE: only 500 rows (easily broadcastable!)

active_campaigns = dim_campaign.filter("status = 'active' AND start_date > '2024-01-01'")
# Estimated size: 1M × row_size = 200 MB (too large to broadcast at compile time)
# Actual size after filter: 500 rows × row_size = 50 KB (tiny!)

result = fact_events.join(active_campaigns, "campaign_id")
# Without AQE: sort-merge join (shuffles 10B fact rows — terrible!)
# With AQE: after the filter stage runs, AQE sees active_campaigns is 50 KB
#            → converts to BroadcastHashJoin (no shuffle of fact_events!)
```

**Before:** Sort-merge join, shuffles 10B rows, takes 30 minutes
**After AQE:** Broadcast join, zero shuffle of fact table, takes 3 minutes (10x faster)

---

## Case Study 4: Production Configuration That Handles All Three

```python
# Production Spark job configuration (covers all AQE optimizations)
spark_session = SparkSession.builder \
    .appName("production_etl") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "4194304") \
    .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "52428800") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456") \
    .config("spark.sql.shuffle.partitions", "auto") \
    .getOrCreate()

# These numeric values:
# advisoryPartitionSizeInBytes = 128 MB
# minPartitionSize = 4 MB
# autoBroadcastJoinThreshold = 50 MB
# skewedPartitionThresholdInBytes = 256 MB
```

---

## Monitoring AQE Decisions in Spark UI

### What to Check After a Job Runs

| Location | What to Look For | Meaning |
|----------|-----------------|---------|
| SQL tab → Plan | "AdaptiveSparkPlan" | AQE was active |
| SQL tab → Plan | "BroadcastHashJoin" (was SortMerge) | Runtime broadcast conversion |
| Stages tab → Stage details | "Coalesced from 200 to X" | Partition coalescing happened |
| Stages tab → Task list | Even task durations | Skew handling worked |
| Stages tab → Task list | One task 10x longer | Skew NOT handled (check threshold) |

### Automated Monitoring

```python
# After job completion, check for AQE effectiveness:
def check_aqe_impact(spark):
    """Log AQE decisions for monitoring."""
    # Get query execution details from Spark listener
    metrics = spark.sparkContext.statusTracker
    
    # In Databricks: check query plan via dbutils
    # In open-source: check event log for AQE events
    
    # Key question: did any stage have extreme task duration skew?
    # If yes despite AQE: skew threshold may need lowering
```

---

## When AQE Isn't Enough — Manual Intervention Still Needed

| Scenario | Why AQE Can't Help | Manual Fix |
|----------|-------------------|-----------|
| Skew in the FIRST stage (read) | No shuffle before first stage → no stats | Pre-partition source data |
| Very large broadcast attempt | AQE's threshold catches some, not all | Explicit `spark.conf.set("...threshold", "-1")` |
| Write amplification from coalescing | AQE optimizes tasks, not output files | Use `df.coalesce(N)` before write explicitly |
| Cross-job optimization | AQE doesn't share stats between jobs | Use Delta Lake statistics or catalog stats |
| Fixed-schema bucketing | AQE may conflict with bucket assumptions | Disable coalescing for bucketed jobs |

---

## Interview Tips

> **Tip 1:** "Give a real example where AQE made a big difference" — "A join between a 10B-row fact table and a filtered dimension. The optimizer estimated 1M rows for the dimension (sort-merge join, shuffled fact table). After the filter actually ran, only 500 rows remained. AQE detected this, converted to broadcast join at runtime, and the query went from 30 minutes to 3 minutes — 10x improvement with zero code changes."

> **Tip 2:** "How do you verify AQE is helping your job?" — "In the Spark UI SQL tab: look for AdaptiveSparkPlan at the plan root. Check if join types changed (SortMerge → Broadcast). In stage details, verify partition coalescing happened. Compare task duration distributions — they should be even (no 100x outlier). For skew: before AQE one task took 42 of 45 minutes; after, all tasks are roughly equal."

> **Tip 3:** "What's your production AQE configuration?" — "Always enabled. Advisory partition size 128 MB (matches my target Parquet file size). Auto-broadcast threshold 50 MB (larger than default 10 MB to catch more opportunities). Skew handling with factor 5 and threshold 256 MB. Shuffle partitions set to 'auto' (Spark 3.2+). This handles 95% of cases without manual tuning."
