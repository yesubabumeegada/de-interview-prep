---
title: "Photon Engine - Real-World Production Examples"
topic: databricks
subtopic: photon-engine
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, photon, production, migration, benchmarks]
---

# Photon Engine — Real-World Production Examples

## Pattern 1: Migration from Standard to Photon

```python
# Step-by-step migration with validation

# Phase 1: Benchmark current performance (standard runtime)
BASELINE = {
    "daily_etl": {"runtime": "58 min", "cluster": "8× i3.xlarge", "monthly_cost": "$3,200"},
    "hourly_ingest": {"runtime": "8 min", "cluster": "4× m5.xlarge", "monthly_cost": "$1,800"},
    "optimize_job": {"runtime": "25 min", "cluster": "4× i3.xlarge", "monthly_cost": "$400"},
}

# Phase 2: Switch to Photon runtime (one job at a time)
# Change: "spark_version": "14.3.x-scala2.12" → "14.3.x-photon-scala2.12"
# No code changes needed!

# Phase 3: Validate results match (critical!)
# Run same job on both runtimes, compare output:
standard_output = spark.table("production.silver.orders_standard")
photon_output = spark.table("production.silver.orders_photon")

# Compare row counts
assert standard_output.count() == photon_output.count()

# Compare checksums (detect any numerical differences)
assert standard_output.selectExpr("md5(concat_ws(',', *))").collect() == \
       photon_output.selectExpr("md5(concat_ws(',', *))").collect()

# Phase 4: Measure improvement
AFTER_PHOTON = {
    "daily_etl": {"runtime": "22 min", "speedup": "2.6x", "new_cost": "$1,500", "savings": "53%"},
    "hourly_ingest": {"runtime": "4 min", "speedup": "2.0x", "new_cost": "$1,000", "savings": "44%"},
    "optimize_job": {"runtime": "12 min", "speedup": "2.1x", "new_cost": "$220", "savings": "45%"},
}
# Total monthly savings: $2,680 (47% reduction!)
```

---

## Pattern 2: Optimizing ETL for Photon

```python
# BEFORE: ETL with Python UDFs (Photon can't accelerate these)
from pyspark.sql.functions import udf

@udf("string")
def parse_user_agent(ua: str) -> str:
    """Python UDF — runs in Python interpreter, not Photon!"""
    if "Chrome" in ua: return "Chrome"
    elif "Firefox" in ua: return "Firefox"
    else: return "Other"

# This stays in Python regardless of Photon → no speedup!
df = df.withColumn("browser", parse_user_agent(col("user_agent")))

# AFTER: Rewrite with native Spark SQL functions (Photon accelerates these!)
df = df.withColumn("browser",
    when(col("user_agent").contains("Chrome"), "Chrome")
    .when(col("user_agent").contains("Firefox"), "Firefox")
    .otherwise("Other")
)
# Now Photon handles this with vectorized C++ string operations (6x faster!)

# More UDF → native rewrites:
# UDF: json.loads(x)["field"] → native: get_json_object(col, "$.field")
# UDF: x.strip().lower() → native: lower(trim(col))
# UDF: re.search(pattern, x) → native: regexp_extract(col, pattern, 1)
# UDF: datetime.strptime(x, fmt) → native: to_timestamp(col, fmt)

# Impact of removing all UDFs:
# Before (3 Python UDFs): 58 minutes (Photon helps non-UDF parts only)
# After (all native functions): 18 minutes (everything runs in Photon!)
```

---

## Pattern 3: Cost Reduction with Photon + Right-Sizing

```python
# Photon is faster → need fewer workers → less cost

# BEFORE: Standard runtime, 16 workers
BEFORE = {
    "runtime": "14.3.x-scala2.12",
    "workers": 16,
    "instance": "i3.xlarge",
    "job_duration_min": 60,
    "monthly_runs": 30,
    "cost_per_run": 16 * 0.312 * 1 + 16 * 0.15 * 1,  # AWS + DBU = $7.39
    "monthly_cost": 7.39 * 30,  # $221.70
}

# AFTER: Photon runtime, 8 workers (half the cluster!)
AFTER = {
    "runtime": "14.3.x-photon-scala2.12",
    "workers": 8,  # Half the workers (Photon is 2-3x faster per worker)
    "instance": "i3.xlarge",
    "job_duration_min": 30,  # 2x faster with half the workers
    "monthly_runs": 30,
    "cost_per_run": 8 * 0.312 * 0.5 + 8 * 0.20 * 0.5,  # AWS + Photon DBU = $2.05
    "monthly_cost": 2.05 * 30,  # $61.50
}

# SAVINGS: $221.70 → $61.50 = 72% cost reduction!
# How: Photon's efficiency allows FEWER workers AND SHORTER runtime
# The higher per-DBU rate ($0.20 vs $0.15) is MORE than offset by:
# 1. Fewer workers (8 vs 16)
# 2. Less runtime (30 min vs 60 min)
```

---

## Pattern 4: Photon for Delta Table Maintenance

```sql
-- OPTIMIZE jobs benefit significantly from Photon:

-- Standard runtime: OPTIMIZE 5TB table with Z-ORDER takes 45 minutes
-- Photon runtime: Same OPTIMIZE takes 20 minutes (2.25x faster)

-- This matters because OPTIMIZE should run frequently (daily or after heavy writes)
-- Faster OPTIMIZE = less cluster time = less cost

-- Schedule daily OPTIMIZE with Photon:
-- Workflow task: runs at 3 AM on Photon job cluster
OPTIMIZE production.silver.orders ZORDER BY (customer_id, order_date);
OPTIMIZE production.silver.events ZORDER BY (event_date, user_id);
OPTIMIZE production.silver.customers;  -- No Z-ORDER needed (small table)

-- Cost: 20 min on 4× i3.xlarge (Photon) = ~$0.70 per run
-- vs: 45 min on 4× i3.xlarge (Standard) = ~$1.25 per run
-- Monthly savings on OPTIMIZE alone: ~$16.50 (small but free optimization)

-- More impactful: faster OPTIMIZE means queries are faster ALL DAY
-- (well-optimized tables have better data skipping → faster reads for everyone)
```

---

## Pattern 5: SQL Warehouse Photon (Default)

```python
# ALL SQL Warehouses run Photon by default (no opt-in needed)
# This is why DBSQL queries are often faster than running SQL on all-purpose clusters

# Comparison: same query on SQL Warehouse vs All-Purpose cluster
# Query: SELECT region, SUM(amount) FROM 500M-row table GROUP BY region

# All-Purpose cluster (Standard runtime): 12 seconds
# All-Purpose cluster (Photon runtime): 4 seconds
# SQL Warehouse (Photon + query-specific optimizations): 2 seconds

# SQL Warehouse additional optimizations on top of Photon:
# 1. Intelligent result caching (repeated queries return instantly)
# 2. Optimized for Delta Lake metadata operations
# 3. Pre-warmed instances (no cold start for queries)
# 4. Query-level resource isolation (one heavy query doesn't starve others)

# For DE: use SQL Warehouses for any SQL-based consumption
# (dashboards, reports, analyst queries, scheduled SQL)
# Use Photon job clusters for ETL (PySpark + SQL transformations)
```

---

## Interview Tips

> **Tip 1:** "How do you migrate to Photon?" — Three steps: (1) Switch runtime version (one-line change), (2) Validate output matches exactly (compare checksums), (3) Right-size cluster (reduce workers since Photon is faster). No code changes needed unless you have Python UDFs — those should be rewritten to native Spark functions to benefit from Photon.

> **Tip 2:** "How much does Photon actually save in production?" — Typical: 40-70% cost reduction for SQL/DataFrame ETL. Breakdown: Photon finishes 2-3x faster (less runtime) + you can use fewer workers (less parallelism needed). The 33% higher DBU rate is more than offset by dramatic runtime reduction. Validate with your own benchmarks before/after.

> **Tip 3:** "What's the #1 thing to do for Photon performance?" — Eliminate Python UDFs. Every UDF forces Spark to fall back from Photon to Python interpretation for those rows. Rewrite with native Spark SQL functions (when/otherwise, regexp_extract, split, trim, etc.). After removing UDFs: typical additional 2-3x speedup on top of base Photon gains.
