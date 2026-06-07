---
title: "PySpark AQE - Scenario Questions"
topic: pyspark
subtopic: adaptive-query-execution
content_type: scenario_question
tags: [pyspark, aqe, interview, scenarios]
---

# Scenario Questions — PySpark AQE

<article data-difficulty="junior">

## 🟢 Junior: What Problems Does AQE Solve?

**Scenario:** Your team is upgrading from Spark 2.4 to Spark 3.2. A colleague asks "what is AQE and should we enable it?" Explain the three problems it solves with concrete examples.

<details>
<summary>✅ Solution</summary>

**AQE solves three common Spark performance problems automatically:**

| Problem | Without AQE | With AQE |
|---------|------------|----------|
| Too many tiny partitions after shuffle | 200 partitions × 1 MB = slow (scheduling overhead) | Auto-merges to 8 × 25 MB (fewer, faster tasks) |
| Wrong join strategy | Optimizer guesses table is large → sort-merge join (shuffles everything) | Sees actual size at runtime → broadcast if small enough |
| Data skew | One partition 100x larger → one task takes forever | Detects and splits oversized partition into sub-tasks |

**Should you enable it?** Yes, always. The overhead is negligible (~10ms per stage). Benefits are 2-5x speedup for jobs with these issues. Jobs without issues see no meaningful slowdown.

```python
# Enable in spark-defaults.conf or job config:
spark.conf.set("spark.sql.adaptive.enabled", "true")
# That's it! All three features activate automatically.
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: AQE Producing Too Many Small Files

**Scenario:** Despite AQE being enabled, your job still produces 200 tiny output files (2 MB each). You expected AQE to coalesce partitions. Why isn't it working, and how do you fix it?

<details>
<summary>✅ Solution</summary>

**Why AQE coalescing didn't help with OUTPUT files:**

AQE coalesces partitions between shuffle STAGES (for computation efficiency). But `df.write.parquet()` happens AFTER the last shuffle — by then, coalescing decisions are already made for the computation, but the WRITE stage uses whatever partition count exists at that point.

**Three scenarios where AQE doesn't fix small output files:**
1. The last operation before write is a shuffle with 200 partitions, and the total data is small
2. AQE coalesced for the join/aggregation stage, but a subsequent repartition reset the count
3. `partitionBy("date")` splits data further regardless of AQE

**Fix 1: Explicit coalesce before write**
```python
# AQE handles computation partitions; YOU handle output files
result = df.groupBy("region").agg(sum("amount"))
result.coalesce(4).write.parquet("s3://output/")  # Force 4 output files
```

**Fix 2: Use maxRecordsPerFile**
```python
result.write.option("maxRecordsPerFile", 1000000).parquet("s3://output/")
# Each file has at most 1M records — predictable sizing
```

**Fix 3: For Delta Lake, use OPTIMIZE after write**
```python
result.write.format("delta").save("s3://output/")
spark.sql("OPTIMIZE delta.`s3://output/`")  # Compacts small files post-write
```

**Key insight:** AQE optimizes EXECUTION (fewer tasks, better join strategies). It does NOT optimize OUTPUT FILE SIZES. You still need explicit coalesce/repartition before write for optimal file sizes.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: AQE Making a Query Slower

**Scenario:** After enabling AQE, one specific job went from 10 minutes to 25 minutes. Investigation shows AQE is converting a sort-merge join to a broadcast join, but the "small" table is actually 2 GB at runtime (exceeds executor memory). Executors are OOM-ing and retrying. Diagnose and fix without disabling AQE globally.

<details>
<summary>✅ Solution</summary>

**Root cause:** AQE's auto-broadcast threshold is set to 50 MB. But the statistics AQE sees are the COMPRESSED shuffle output size. The actual in-memory size after deserialization is much larger (2 GB). AQE broadcasts based on compressed size → executor receives 2 GB in memory → OOM.

**Diagnosis:**
```python
# Check Spark UI → SQL tab → Physical Plan
# Look for: BroadcastExchange (estimated size: 45 MB)
# But actual memory usage during broadcast is 2 GB (decompression + deserialization)

# Check executor logs:
# "java.lang.OutOfMemoryError: Java heap space"
# during "BroadcastExchange"
```

**Fix 1: Lower the auto-broadcast threshold (prevent this specific conversion)**
```python
# Set threshold below the compressed size of the problematic table
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "20MB")
# Now AQE won't try to broadcast tables > 20 MB compressed
```

**Fix 2: Use a join hint to force sort-merge for this specific join**
```python
# Explicitly prevent broadcast for this join (AQE respects hints)
result = large_df.join(medium_df.hint("merge"), "key")
# hint("merge") forces SortMergeJoin regardless of AQE's runtime decision
```

**Fix 3: Increase executor memory (if broadcast is actually beneficial for other reasons)**
```python
# If the broadcast would help (eliminate shuffle of large side),
# give executors enough memory to hold it:
spark.conf.set("spark.executor.memory", "16g")  # Was 8g
spark.conf.set("spark.executor.memoryOverhead", "4g")
# Now 2 GB broadcast fits comfortably in 16 GB heap
```

**Fix 4: Disable only auto-broadcast conversion (keep other AQE features)**
```python
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
# Disables runtime broadcast conversion
# Coalescing and skew handling still active!
```

**Best approach:** Fix 2 (hint) for the specific problematic join, keeping AQE fully enabled for all other joins. This is surgical — fixes the one problem without losing AQE benefits elsewhere.

**Prevention:** Monitor for broadcast-related OOMs in production. Set up alerts on executor OOM events and correlate with BroadcastExchange in the query plan.

</details>

</article>
