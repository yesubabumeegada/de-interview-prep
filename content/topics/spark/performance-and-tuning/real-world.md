---
title: "Spark Performance & Tuning — Real World"
topic: spark
subtopic: performance-and-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, performance, production, tuning-playbook, skew-fix, small-files, war-story]
---

# Spark Performance & Tuning — Real World

## The Tuning Playbook

**Step 1: Establish a baseline**
```python
# Before any tuning, measure and record:
# - Job duration (wall clock)
# - Stage durations (Spark UI → Stages)
# - Data read/written (Spark UI → Stages → Input/Output)
# - Shuffle read/write bytes
# - GC time %
# - Spill (memory and disk)
```

**Step 2: Identify the bottleneck (one at a time)**
```
Question 1: Is one stage > 80% of total time?
  YES → focus all effort on that stage

Question 2: Is the slow stage waiting on one task (skew)?
  YES → fix skew (AQE, salting, null handling)

Question 3: Is GC time > 10%?
  YES → increase executor memory, tune GC, reduce object creation

Question 4: Is there spill?
  YES → increase partitions, increase executor memory

Question 5: Are shuffle bytes >> input bytes?
  YES → can a broadcast join eliminate the shuffle?
```

---

## War Story: 4-Hour Join Reduced to 8 Minutes

**Before:**
```
Job: daily orders joined with customer features
Inputs: orders (800 GB), customer_features (120 GB)
Duration: 4h 20m
Stages: Stage 1 (scan + filter): 12 min, Stage 2 (join): 4h 8m
Spark UI: Stage 2 — one task taking 3h 55m, 199 tasks done in 2 min
```

**Investigation:**
```python
# Check join key distribution:
orders.groupBy("customer_id").count().orderBy(F.desc("count")).show(10)
# customer_id | count
# INTERNAL    | 45_000_000   ← 45M rows!
# C00001      | 12_000
# ...
# "INTERNAL" = internal system orders, not real customers
```

**Fix:**
```python
# Filter out the known skew key BEFORE join
real_orders = orders.filter(F.col("customer_id") != "INTERNAL")
internal_orders = orders.filter(F.col("customer_id") == "INTERNAL")

# Join real orders normally
result_real = real_orders.join(customer_features, "customer_id", "left")

# Handle internal orders separately (no join needed for analytics)
result_internal = internal_orders.withColumn("customer_tier", F.lit("internal"))

# Combine
result = result_real.union(result_internal)
```

**After:** Job: 8 minutes. The skewed partition was eliminated entirely.

---

## War Story: Broadcast Join Causing Driver OOM

**Scenario:** After setting `autoBroadcastJoinThreshold = "500mb"`, jobs started failing with Driver OOM errors for seemingly reasonable joins.

**Root cause:**
```
Broadcast execution:
  1. Spark collects the broadcast side to Driver memory (!)
  2. Driver serializes it
  3. Driver sends serialized bytes to each executor

With 500MB threshold:
  - 500MB table → ~1.5GB after serialization in Driver heap
  - Driver configured with 4GB → multiple concurrent joins → OOM
```

**Fix:**
```python
# Driver needs headroom for broadcasts:
# Rule: spark.driver.memory >= 3 × peak_broadcast_size

spark.conf.set("spark.driver.memory", "12g")

# OR: lower the threshold back to something safe
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# OR: use AQE broadcast (only triggers post-shuffle, when actual size is known)
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE re-evaluates join strategy AFTER the build-side shuffle completes
# and only broadcasts if the actual size fits
```

---

## The Small Files Death Spiral

**Scenario:** A streaming job wrote micro-batches to Parquet — 96 triggers/day × 50 partitions = 4800 files/day. After 6 months: 876,000 files. Next-day batch read took 45 minutes just for the file listing step.

**Fix pipeline:**
```python
# 1. Immediate: compact existing files
spark.conf.set("spark.sql.shuffle.partitions", "200")
(spark.read.parquet("s3://bucket/events/")
    .filter("year = 2024")   # one month at a time
    .coalesce(200)
    .write.mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet("s3://bucket/events-compacted/"))

# 2. Prevent recurrence: use Delta Lake for streaming output
query = stream.writeStream.format("delta") \
    .option("path", "s3://bucket/delta/events/") \
    .trigger(processingTime="10 minutes") \   # less frequent, fewer files
    .start()

# 3. Schedule Delta OPTIMIZE daily
spark.sql("OPTIMIZE delta.`s3://bucket/delta/events/` WHERE day = current_date()")

# 4. Set maxPartitionBytes for read-heavy workloads
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))
# Spark coalesces small files into larger virtual partitions on read
```

---

## Interview Tips

> **Tip 1:** "Describe a real Spark performance problem you've solved." — Strong answers identify the symptom (job duration), isolate to a stage (Spark UI), identify the root cause (skew, spill, bad plan, small files), apply a targeted fix, and measure the improvement. The best answers show a methodical debugging process rather than random config changes.

> **Tip 2:** "What is the broadcast join OOM trap?" — The broadcast side is first collected to the Driver, serialized, and sent to executors. Driver needs ~3× the broadcast table size in memory. Raising `autoBroadcastJoinThreshold` to 500MB with a 4GB Driver heap is dangerous when multiple concurrent jobs each broadcast large tables. Use AQE's runtime broadcast switching instead — it only broadcasts if the actual post-shuffle size fits, and it bypasses the Driver collection step.
