---
title: "Performance Tuning - Scenario Questions"
topic: databricks
subtopic: performance-tuning
content_type: scenario_question
tags: [databricks, spark, performance, scenarios, interview, optimization]
---

# Scenario Questions — Performance Tuning

<article data-difficulty="junior">

## 🟢 Junior: Optimize a Slow Daily Report Query

**Scenario:** A daily revenue report query takes 45 minutes on a 100GB orders table. The query is: `SELECT region, SUM(amount) FROM orders WHERE order_date = '2024-01-15' GROUP BY region`. The table is not partitioned or Z-ordered. How would you optimize it?

<details>
<summary>✅ Solution</summary>

**Step 1: Check the query plan**

```python
df = spark.sql("SELECT region, SUM(amount) FROM orders WHERE order_date = '2024-01-15' GROUP BY region")
df.explain()
# FileScan: 100GB read (full scan — no partition pruning or data skipping)
```

**Step 2: Short-term fix — add Z-ordering**

```sql
-- Z-order by the filter and group-by columns
-- This co-locates rows with same order_date and region in same files
-- Enables data skipping on future queries
OPTIMIZE prod.sales.orders ZORDER BY (order_date, region);

-- After Z-ordering: query reads ~2GB instead of 100GB
-- Runtime: 45min → ~3min
```

**Step 3: Better long-term fix — partition by date**

```sql
-- Recreate table partitioned by order_date
CREATE TABLE prod.sales.orders_v2
    PARTITIONED BY (order_date)
AS SELECT * FROM prod.sales.orders;

-- Now filtering by order_date reads only 1 partition (1 day of data)
-- Runtime: ~30 seconds (reads only that day's files, not 100GB)
```

**Step 4: Cache for repeated queries**

```python
# If this report is run multiple times per day, cache the day's data
daily_orders = spark.sql(
    "SELECT * FROM orders WHERE order_date = '2024-01-15'"
).cache()
daily_orders.count()  # materialize cache

# All report queries now serve from cache
revenue_by_region = daily_orders.groupBy("region").agg(F.sum("amount"))
revenue_by_product = daily_orders.groupBy("product_id").agg(F.sum("amount"))
```

**Summary of options by impact:**
1. Z-ordering: moderate improvement (enables data skipping), no table change
2. Date partitioning: large improvement (partition pruning), requires table recreation
3. Caching: best for repeated queries on same day's data

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Debug a Slow Join

**Scenario:** A join between `transactions` (500GB, 2B rows) and `merchant_info` (2MB, 5,000 rows) is taking 40 minutes. You'd expect it to be fast since merchant_info is tiny. Diagnose the problem and fix it.

<details>
<summary>✅ Solution</summary>

**Diagnose:**

```python
from pyspark.sql.functions import broadcast

txn = spark.table("prod.payments.transactions")
merchants = spark.table("prod.dimensions.merchant_info")

# Check auto-broadcast threshold
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# Output: "10485760" (10MB)

# Check merchant_info size
print(merchants.count())       # 5,000 rows
merchants.cache().count()
# Estimated size: 2MB — SHOULD be broadcast

# Look at the query plan
txn.join(merchants, "merchant_id").explain()
# Output shows: SortMergeJoin ← Problem! Not using BroadcastHashJoin

# Why? merchant_info was read from a table with no statistics
# Spark didn't know it was only 2MB → defaulted to sort-merge join
```

**Fix 1: Compute statistics so Spark can auto-broadcast**

```sql
ANALYZE TABLE prod.dimensions.merchant_info COMPUTE STATISTICS;
-- Now Spark knows it's 2MB → auto-broadcasts it
```

**Fix 2: Use broadcast hint (more reliable)**

```python
result = txn.join(broadcast(merchants), "merchant_id")
result.explain()
# Output now shows: BroadcastHashJoin ← correct!

# Verify in Spark UI after running:
# Stage with join → no "Exchange" operator → no shuffle!
result.count()  # 40min → 4min
```

**Fix 3: Raise auto-broadcast threshold for safety**

```python
# Prevent regression if merchant_info grows to 50MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")
```

**Fix 4: Ensure statistics stay fresh**

```python
# Add to the pipeline that updates merchant_info
spark.sql("INSERT OVERWRITE prod.dimensions.merchant_info SELECT * FROM ...")
spark.sql("ANALYZE TABLE prod.dimensions.merchant_info COMPUTE STATISTICS")
# Now stats are always fresh → auto-broadcast works reliably
```

**Root cause summary:** Spark's optimizer needs statistics to estimate table sizes for auto-broadcast. Without ANALYZE, it falls back to sort-merge join. Always run ANALYZE on dimension tables, or use `broadcast()` hint explicitly.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Performance Budget for a Critical Pipeline

**Scenario:** A real-time risk scoring pipeline must complete within 15 minutes of data arrival (SLA). Currently it takes 45 minutes. The pipeline: reads 50GB of transactions, joins to 3 dimension tables, runs 8 aggregations, and writes results to Delta. You have a budget of $50/day for compute. Design the optimization strategy.

<details>
<summary>✅ Solution</summary>

**Step 1: Profile the current pipeline**

```python
# Instrument with timing at each stage
import time

timings = {}

t = time.time()
txn_raw = spark.table("prod.payments.transactions") \
    .filter(f"txn_date >= '{target_date}'")
txn_raw.cache().count()
timings["read_transactions"] = time.time() - t   # baseline: 8 min

t = time.time()
enriched = txn_raw \
    .join(broadcast(merchants), "merchant_id") \
    .join(broadcast(customers), "customer_id") \
    .join(broadcast(risk_tiers), "risk_tier_id")
enriched.cache().count()
timings["joins"] = time.time() - t   # baseline: 18 min (sort-merge joins!)

t = time.time()
# 8 aggregations — each triggers a shuffle
results = []
for agg_fn in aggregation_functions:
    results.append(enriched.agg(agg_fn))
timings["aggregations"] = time.time() - t   # baseline: 15 min

print(timings)
# read: 8min, joins: 18min, aggregations: 15min → total 41min
```

**Step 2: Fix the biggest bottleneck — joins (18 min)**

```python
# All 3 dimension tables were using sort-merge join (no statistics)
# Fix: add statistics + explicit broadcast hints

spark.sql("ANALYZE TABLE prod.dimensions.merchants COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE prod.dimensions.customers COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE prod.risk.risk_tiers COMPUTE STATISTICS")

enriched = txn_raw \
    .join(broadcast(merchants), "merchant_id") \
    .join(broadcast(customers), "customer_id") \
    .join(broadcast(risk_tiers), "risk_tier_id")
# 3 sort-merge joins → 3 broadcast hash joins
# 18 min → 2 min (no shuffle for any join)
```

**Step 3: Reduce read time (8 min)**

```sql
-- Transactions table had no Z-ordering or partitioning
-- Fix: partition by txn_date, Z-order by merchant_id (most join key)
OPTIMIZE prod.payments.transactions ZORDER BY (merchant_id, risk_tier_id);
-- After: read 8GB instead of 50GB (84% skipped via data skipping)
-- Read time: 8min → 1.5min
```

**Step 4: Parallelize aggregations (15 min)**

```python
# 8 sequential aggregations → run in parallel with threads
from concurrent.futures import ThreadPoolExecutor

def run_aggregation(agg_spec):
    return enriched.groupBy(agg_spec["group_by"]).agg(*agg_spec["aggs"])

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(run_aggregation, spec) for spec in aggregation_specs]
    results = [f.result() for f in futures]

# 8 sequential (15min total) → 4 parallel batches (4min total)
```

**Step 5: Cluster right-sizing within budget**

```python
# Budget: $50/day = $50 / (24 DBU-hours/day at 15min cadence)
# 24 runs/day × (15min / 60) = 6 cluster-hours/day
# $50 / 6 = $8.33/cluster-hour
# At $0.22/DBU: $8.33 / 0.22 = ~38 DBUs available

# Configuration: 4 workers × r5.4xlarge (10 DBU each) = 40 DBU
# Slightly over budget — tune to 3 workers × r5.4xlarge = 30 DBU ($6.60/hr)
# Or: use Spot instances (3x i3.2xlarge + 1x on-demand driver) — 40% cheaper
```

**Final result:**

| Stage | Before | After | Method |
|-------|--------|-------|--------|
| Read | 8 min | 1.5 min | Z-ordering → data skipping |
| Joins | 18 min | 2 min | Broadcast hash joins |
| Aggregations | 15 min | 4 min | Parallel execution |
| **Total** | **41 min** | **7.5 min** | |
| Cost/day | ~$65 | ~$42 | Right-sized cluster |

**SLA met (15 min → 7.5 min), budget met ($50 → $42/day).**

</details>
</article>
