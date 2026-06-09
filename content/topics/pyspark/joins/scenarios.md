---
title: "PySpark Joins — Scenarios"
topic: pyspark
subtopic: joins
content_type: scenario_question
tags: [pyspark, joins, skewed-join, performance, star-schema, interview]
---

# PySpark Joins — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Explain Join Types and When to Use Each

**Scenario:** Your team is building a pipeline that joins a `page_views` table (500M rows) with a `users` table (2M rows). Your manager asks: "Should we use an inner join or a left join here? Does it matter?" How do you answer, and what's your recommendation?

<details>
<summary>💡 Hint</summary>

Think about what happens to rows that don't have a match. Consider: are there page views from anonymous users (no user_id match)? What should happen to those rows in the output? Also think about what join type would change the final row count.

</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import broadcast, col, coalesce, lit

# The key question: what should happen to page views from anonymous users?

# INNER JOIN: drop any page view without a matching user
# Use when: every event MUST have a matching user (enforced FK)
enriched_inner = page_views.join(
    broadcast(users),
    on="user_id",
    how="inner"
)
# Risk: silently drops anonymous/guest views → undercounts total traffic

# LEFT JOIN: keep all page views, nulls where user doesn't exist
# Use when: you want to preserve all events and optionally enrich with user data
enriched_left = page_views.join(
    broadcast(users),
    on="user_id",
    how="left"
)
# Safe default for analytics — you never silently lose events

# Verification pattern — always check row counts:
print(f"Raw page views:   {page_views.count():,}")
print(f"After inner join: {enriched_inner.count():,}")
print(f"After left join:  {enriched_left.count():,}")
# If inner < raw: you have unmatched rows. Left join is safer.

# Handle nulls from left join for downstream consumers:
enriched_left = enriched_left.withColumn(
    "user_segment",
    coalesce(col("users.segment"), lit("anonymous"))
)
```

**The answer:** Use a **left join** as the safe default for event enrichment. Inner join is only correct if you can guarantee every event has a matching user record (which is rarely true in practice — guest checkouts, bot traffic, deleted users). The performance difference is negligible; the semantic difference is significant.

Also: `users` at 2M rows × ~500 bytes = ~1 GB — borderline for broadcast. Check with `.explain()` whether Spark auto-broadcasts it, or raise the threshold / use the `broadcast()` hint explicitly.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnose and Fix a Skewed Join Causing OOM

**Scenario:** A daily ETL joins a 200 GB `transactions` table with a 50 MB `merchants` table on `merchant_id`. The job has been running for 3 hours with one task stuck at 98% — all other tasks finished in under 5 minutes. The Spark UI shows that task is processing 150 GB of data by itself. The job eventually OOMs. Diagnose and fix it.

<details>
<summary>💡 Hint</summary>

Look at the key distribution of `merchant_id` in the transactions table. Why would one task process 150 GB? Think about what value that merchant_id likely is (null? a single mega-merchant?). What are your fix options: AQE skew handling, salting, or something else? Also re-examine whether the `merchants` table should be broadcast at all.

</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import col, count, desc, broadcast, concat, lit, floor, rand, array, explode

# Step 1: Diagnose the skew
transactions.groupBy("merchant_id") \
    .agg(count("*").alias("txn_count")) \
    .orderBy(desc("txn_count")) \
    .limit(10) \
    .show()
# Result:
# +---------------+---------+
# |merchant_id    |txn_count|
# +---------------+---------+
# |null           |85000000 |  ← NULL key = 150GB of data!
# |amazon_us      |12000000 |
# |walmart_us     |8000000  |
# +---------------+---------+

# Root cause: null merchant_id makes up 40% of all transactions.
# All nulls hash to the same partition → one executor processes 150 GB.

# FIX 1: Broadcast the merchants table (50 MB → always broadcast this!)
result = transactions.join(
    broadcast(merchants),
    on="merchant_id",
    how="left"
)
# BHJ eliminates shuffle entirely — null distribution doesn't matter
# This is the CORRECT fix for this specific problem.
# Runtime: 3 hours → ~8 minutes ✓

# FIX 2: If merchants was genuinely too large to broadcast,
# use AQE skew join (Spark 3+):
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
# AQE will detect the null partition and split it across multiple executors

# FIX 3: Manual salting (if AQE and broadcast are both insufficient)
# Replace null keys with a non-null sentinel before salting
SALT_FACTOR = 20

txn_salted = transactions.withColumn(
    "merchant_id_nn",
    # Treat nulls as a special value
    col("merchant_id").cast("string")
).fillna({"merchant_id_nn": "__NULL__"}) \
 .withColumn(
    "salted_key",
    concat(col("merchant_id_nn"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

salt_array = array([lit(i) for i in range(SALT_FACTOR)])
merchants_salted = merchants.withColumn("salt", explode(salt_array)) \
    .withColumn("merchant_id_nn",
                col("merchant_id").cast("string")) \
    .fillna({"merchant_id_nn": "__NULL__"}) \
    .withColumn(
        "salted_key",
        concat(col("merchant_id_nn"), lit("_"), col("salt").cast("string"))
    ).drop("salt", "merchant_id_nn")

result_salted = txn_salted.join(merchants_salted, on="salted_key", how="left") \
    .drop("salted_key", "merchant_id_nn")
```

**Root cause summary:** The 50 MB `merchants` table should have been broadcast from day one — the auto-broadcast threshold of 10 MB was the real bug. A 150 GB task was entirely avoidable. The fix is adding `broadcast()` hint. The deeper lesson: always verify broadcast thresholds for dimension tables in the 10–500 MB range.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Join Strategy for a 10 TB Fact Table with Skewed user_id

**Scenario:** You're designing a new data model. The `events` fact table is 10 TB, partitioned by `event_date`. You need to join it daily with a `user_profiles` dimension (15 GB) on `user_id`. The `user_id` distribution is highly skewed: 2% of users generate 60% of events (power users + bot traffic). The pipeline must complete in under 30 minutes. Design a complete join strategy.

<details>
<summary>💡 Hint</summary>

15 GB is too large to broadcast naively. Think about: bucketed joins (pre-partition both tables on user_id), AQE configuration, handling the skewed keys (bots/power users) separately, incremental processing (you only need today's partition, not 10 TB). Walk through the full architecture decision.

</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE DECISION: Multi-pronged approach
# 1. Bucket both tables on user_id (write-once, join-many-times)
# 2. Process only today's partition (not 10 TB)
# 3. Handle skewed keys with isolation pattern
# 4. Enable AQE for residual skew

# ── Phase 0: One-time table setup (done once, not daily) ──────────────────

# Bucket events table on user_id
spark.sql("""
    CREATE TABLE warehouse.events_bucketed
    USING DELTA
    CLUSTERED BY (user_id) INTO 1024 BUCKETS
    PARTITIONED BY (event_date)
    AS SELECT * FROM warehouse.events_raw
    LIMIT 0
""")

# Bucket user_profiles on user_id with SAME number of buckets
spark.sql("""
    CREATE TABLE warehouse.user_profiles_bucketed
    USING DELTA
    CLUSTERED BY (user_id) INTO 1024 BUCKETS
    AS SELECT * FROM warehouse.user_profiles_raw
    LIMIT 0
""")

# ── Phase 1: Daily incremental processing ─────────────────────────────────

from pyspark.sql.functions import col, broadcast, concat, lit, floor, rand, array, explode
import datetime

today = datetime.date.today().isoformat()

# Load ONLY today's partition — not 10 TB, just today's slice (~30-50 GB)
today_events = spark.table("warehouse.events_bucketed") \
    .filter(col("event_date") == today)
# With bucketing + partition pruning: scans ~30 GB, no shuffle needed

user_profiles = spark.table("warehouse.user_profiles_bucketed")
# 15 GB bucketed on user_id — same 1024 buckets

# AQE configuration for residual skew
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
               str(256 * 1024 * 1024))

# ── Phase 2: Identify and isolate known skewed keys ───────────────────────

# Compute key distribution on today's data (fast with partition pruning)
key_dist = today_events.groupBy("user_id") \
    .agg({"*": "count"}) \
    .withColumnRenamed("count(1)", "event_count") \
    .orderBy(col("event_count").desc())

# Identify keys with > 1M events today (bots + mega power users)
SKEW_THRESHOLD = 1_000_000
skewed_user_ids = [
    row.user_id
    for row in key_dist.filter(col("event_count") > SKEW_THRESHOLD).collect()
]
# Expected: ~50-100 bot/power users

# Split events into skewed and normal
events_skewed = today_events.filter(col("user_id").isin(skewed_user_ids))
events_normal = today_events.filter(~col("user_id").isin(skewed_user_ids))

# ── Phase 3: Join each portion optimally ──────────────────────────────────

# Skewed portion: only a few user_ids → filter profiles to those IDs + broadcast
profiles_skewed = user_profiles.filter(col("user_id").isin(skewed_user_ids))
# profiles_skewed is tiny (<100 rows) → broadcast
result_skewed = events_skewed.join(
    broadcast(profiles_skewed),
    on="user_id",
    how="left"
)

# Normal portion: bucketed join (no shuffle since both tables bucketed on user_id)
result_normal = events_normal.join(
    user_profiles,
    on="user_id",
    how="left"
)
# Plan should show: SortMergeJoin WITHOUT Exchange (no shuffle)

# ── Phase 4: Union and write ───────────────────────────────────────────────

final = result_normal.union(result_skewed)

final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"event_date = '{today}'") \
    .save("s3://warehouse/enriched_events/")

# ── Expected performance ───────────────────────────────────────────────────
# Today's slice: ~30 GB (not 10 TB)
# Bucketed join: no shuffle on user_id
# Skewed keys: isolated + broadcast (trivial)
# AQE: handles any residual skew in normal portion
# Total runtime: ~12-18 minutes ✓ (well under 30 min target)

# ── Monitoring ────────────────────────────────────────────────────────────
# Check Spark UI:
# - Stage with join: zero Exchange operations (confirms bucket join)
# - Task duration distribution: median vs max within 2x (no skew)
# - Total data read: ~45 GB (not 10 TB) — confirms partition pruning works
```

**Design rationale:**
- **Bucketing** is the foundational optimization — it eliminates shuffle on every daily run, not just today's.
- **Partition pruning** reduces the problem from 10 TB to ~30 GB daily.
- **Key isolation** handles the long tail of extreme skew (bots) that AQE alone may struggle with.
- **AQE** is a safety net for unexpected skew in the normal portion.
- **Incremental writes** with `replaceWhere` avoid re-processing historical partitions.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What's the difference between broadcast join and sort-merge join?" — Don't just say "broadcast sends the small table to all nodes." Explain WHY: BHJ avoids shuffle entirely. The shuffle in SMJ is the expensive part — network I/O + disk writes. Quantify: a 10 GB shuffle across 100 executors = 10 GB of network traffic per executor that needs data.

> **Tip 2:** "How do you detect data skew?" — The Spark UI answer is correct but shallow. Also mention: `groupBy(join_key).count().orderBy(desc())` to inspect distribution before running the job, and monitoring task duration variance in the stage view. Interviewers want to see you'd catch skew before it causes an incident.

> **Tip 3:** "When would you NOT use a broadcast join?" — When the "small" table is larger than available executor memory, or when the table is being updated frequently and you don't want to re-broadcast on every run. Also: broadcast creates a full copy per executor — on a 500-node cluster, a 500 MB broadcast = 250 GB of total memory consumed. Be aware of the cluster-wide memory cost.
