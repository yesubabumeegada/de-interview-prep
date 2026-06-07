---
title: "Photon Engine - Scenario Questions"
topic: databricks
subtopic: photon-engine
content_type: scenario_question
tags: [databricks, photon, interview, scenarios, performance]
---

# Scenario Questions — Photon Engine

<article data-difficulty="junior">

## 🟢 Junior: Enabling Photon

**Scenario:** Your ETL job runs on standard runtime and takes 45 minutes. Your manager heard Photon can make it faster. How do you enable Photon, and what speedup should you expect for a job that does aggregations and joins?

<details>
<summary>💡 Hint</summary>
Change the runtime version from standard to photon. For aggregation/join-heavy workloads, expect 2-4x speedup. No code changes needed.
</details>

<details>
<summary>✅ Solution</summary>

```python
# BEFORE: Standard runtime
"spark_version": "14.3.x-scala2.12"  # Standard Spark

# AFTER: Photon runtime (one-line change!)
"spark_version": "14.3.x-photon-scala2.12"  # Photon-enabled

# That's it! No code changes. Same notebooks, same SQL, same DataFrames.

# Expected speedup for aggregation + join workload:
# - Aggregations (GROUP BY): 3-4x faster
# - Joins (hash join): 2-3x faster
# - Overall job: likely 2.5-3x faster
# - 45 minutes → ~15-20 minutes

# Verify Photon is active (after running):
spark.sql("EXPLAIN EXTENDED SELECT ...").show()
# Look for: "PhotonGroupingAgg", "PhotonBroadcastHashJoin"
# If you see these → Photon is working!
```

**Key Points:**
- Enable: change runtime version string (add "photon" to the version)
- No code changes required (same API, same operations)
- Aggregation + join workloads: typically 2-4x speedup
- Cost: Photon DBU rate is slightly higher, but shorter runtime = usually net savings
- Validation: check EXPLAIN plan for "Photon" prefixed operators
- Risk: zero — same results, just faster (validate with checksum comparison if needed)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Photon vs Python UDFs

**Scenario:** Your job has 5 Python UDFs for parsing and transforming data. After switching to Photon, the job only got 1.3x faster (not the expected 3x). Why, and how do you fix it?

<details>
<summary>💡 Hint</summary>
Python UDFs can't be executed by Photon (they run in the Python interpreter). Photon only accelerates the non-UDF parts. To get full Photon benefit, replace UDFs with native Spark functions.
</details>

<details>
<summary>✅ Solution</summary>

```python
# PROBLEM: UDFs force Photon fallback to Python

# UDF 1: Parse JSON field
@udf("string")
def extract_city(json_str):
    import json
    return json.loads(json_str).get("city", "")
# FIX: get_json_object(col("json_str"), "$.city")

# UDF 2: Clean phone number
@udf("string")
def clean_phone(phone):
    return ''.join(c for c in phone if c.isdigit())[-10:]
# FIX: regexp_replace(col("phone"), "[^0-9]", "")
# Then: substr(col("phone_clean"), -10)

# UDF 3: Categorize amount
@udf("string")
def amount_tier(amount):
    if amount > 1000: return "high"
    elif amount > 100: return "medium"
    else: return "low"
# FIX: when(col("amount") > 1000, "high").when(col("amount") > 100, "medium").otherwise("low")

# UDF 4: Format date
@udf("string")
def format_date(dt):
    return dt.strftime("%Y-%m-%d") if dt else None
# FIX: date_format(col("dt"), "yyyy-MM-dd")

# UDF 5: URL domain extraction
@udf("string")
def get_domain(url):
    from urllib.parse import urlparse
    return urlparse(url).netloc
# FIX: regexp_extract(col("url"), "://([^/]+)", 1)

# RESULT after replacing all 5 UDFs:
# Before (with UDFs, Photon): 35 minutes (1.3x speedup — most time in Python)
# After (native functions, Photon): 12 minutes (3.75x speedup from original!)
# The UDFs were the bottleneck preventing Photon from helping!
```

**Key Points:**
- Python UDFs = Photon can't accelerate them (forces fallback to Python interpreter)
- If 80% of job time is in UDFs, Photon only speeds up the remaining 20% (minimal gain)
- Solution: replace UDFs with native Spark SQL functions (when/otherwise, regexp_extract, etc.)
- After removing UDFs: full Photon benefit applies to entire job (2-4x speedup)
- Common UDF patterns ALL have native equivalents (JSON parsing, string ops, conditionals)
- Rule: any time you write `@udf`, first check if a native function exists!

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Cost Comparison

**Scenario:** Your job runs 60 min on standard (8 workers, $0.15/DBU). With Photon it runs 25 min ($0.20/DBU, same workers). Which is cheaper?

<details>
<summary>💡 Hint</summary>
Calculate total cost: workers × DBU rate × hours. Photon's higher rate but shorter time often makes it cheaper overall.
</details>

<details>
<summary>✅ Solution</summary>

```python
# STANDARD RUNTIME:
standard_workers = 8
standard_dbu_rate = 0.15  # $/DBU/hr (Jobs compute)
standard_hours = 60 / 60  # 1 hour
standard_aws_rate = 0.312  # i3.xlarge on-demand $/hr

standard_dbu_cost = standard_workers * 1 * standard_dbu_rate * standard_hours  # $1.20
standard_aws_cost = standard_workers * standard_aws_rate * standard_hours       # $2.50
standard_total = standard_dbu_cost + standard_aws_cost                          # $3.70

# PHOTON RUNTIME:
photon_workers = 8
photon_dbu_rate = 0.20  # $/DBU/hr (Photon Jobs compute)
photon_hours = 25 / 60  # 0.417 hours
photon_aws_rate = 0.312  # Same instance

photon_dbu_cost = photon_workers * 1 * photon_dbu_rate * photon_hours  # $0.67
photon_aws_cost = photon_workers * photon_aws_rate * photon_hours       # $1.04
photon_total = photon_dbu_cost + photon_aws_cost                        # $1.71

# COMPARISON:
# Standard: $3.70 per run
# Photon: $1.71 per run
# Photon is 54% CHEAPER! (despite 33% higher DBU rate)

# Monthly (30 daily runs):
# Standard: $111/month
# Photon: $51/month
# Annual savings: $720

print(f"Standard: ${standard_total:.2f}/run")   # $3.70
print(f"Photon: ${photon_total:.2f}/run")       # $1.71
print(f"Savings: {(1 - photon_total/standard_total)*100:.0f}%")  # 54%
```

**Key Points:**
- Photon costs MORE per DBU ($0.20 vs $0.15) but LESS total per job
- Why: shorter runtime (25 vs 60 min) more than offsets the rate premium
- Break-even: Photon needs to be at least 1.33x faster to be cheaper (it's usually 2-4x)
- For this job: 2.4x faster = 54% cheaper (big net savings)
- Additional bonus: faster execution means better SLA compliance
- ALWAYS benchmark your specific workload — results vary by operation mix

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnosing Photon Fallback

**Scenario:** Your Photon-enabled job shows mixed operators in the query plan: some "PhotonScan" and "PhotonGroupingAgg" but also regular "SortMergeJoin" (without Photon prefix). Why is the join not using Photon, and how do you fix it?

<details>
<summary>💡 Hint</summary>
Photon supports hash joins (broadcast and shuffle hash). If the optimizer chose SortMergeJoin, it's either because the tables are too large for hash join or statistics are outdated. Fix: update statistics or add broadcast hint.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- DIAGNOSIS: Check why SortMergeJoin was chosen over PhotonShuffledHashJoin

-- Step 1: Look at table sizes in the plan
EXPLAIN COST
SELECT o.*, c.region
FROM production.silver.orders o  -- 500M rows
JOIN production.silver.customers c ON o.customer_id = c.customer_id;  -- 5M rows

-- Plan shows: SortMergeJoin (not Photon)
-- Reason: Spark doesn't know customers is only 5M rows (stale/missing stats)
-- Without stats: optimizer defaults to SortMergeJoin (safe but slow)

-- Step 2: Update table statistics
ANALYZE TABLE production.silver.customers COMPUTE STATISTICS;
ANALYZE TABLE production.silver.customers COMPUTE STATISTICS FOR ALL COLUMNS;

-- Now optimizer knows: customers = 5M rows × 200 bytes = ~1 GB
-- 1 GB < autoBroadcastJoinThreshold? Check:
-- Default threshold: 10 MB (too small for 1 GB table)

-- Step 3: Increase broadcast threshold (if table fits in executor memory)
SET spark.sql.autoBroadcastJoinThreshold = 2147483648;  -- 2 GB
-- Now: customers (1 GB) < threshold (2 GB) → BroadcastHashJoin selected!
-- Plan shows: PhotonBroadcastHashJoin ✓

-- Step 4: Or use explicit hint
SELECT /*+ BROADCAST(c) */ o.*, c.region
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id;
-- Forces broadcast regardless of threshold → PhotonBroadcastHashJoin

-- RESULT:
-- Before (SortMergeJoin, partial Photon): 45 seconds
-- After (PhotonBroadcastHashJoin): 8 seconds (5.6x speedup!)
```

**Key Points:**
- Photon supports: BroadcastHashJoin and ShuffledHashJoin (not SortMergeJoin in some cases)
- SortMergeJoin appears when: tables are too large for hash join OR stats are missing
- Fix 1: ANALYZE TABLE (give optimizer accurate size information)
- Fix 2: Increase autoBroadcastJoinThreshold (if the small table fits in memory)
- Fix 3: BROADCAST hint (force broadcast regardless of stats)
- After fixing: the join uses PhotonBroadcastHashJoin (vectorized, much faster)
- Rule: always run ANALYZE TABLE on dimension tables after loading/updating

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Photon for String-Heavy ETL

**Scenario:** Your log parsing pipeline processes 2TB of text logs daily: extracting URLs, parsing user agents, cleaning email addresses, and splitting CSV fields. It takes 90 minutes on standard runtime. Estimate the Photon improvement and explain why strings benefit most.

<details>
<summary>💡 Hint</summary>
String operations show the largest Photon improvement (6-8x) because JVM String objects are extremely inefficient (immutable, UTF-16, GC pressure). Photon uses raw UTF-8 bytes with zero-copy operations.
</details>

<details>
<summary>✅ Solution</summary>

```python
# WHY strings are Photon's biggest win:

# JVM String handling (traditional Spark):
# - Every string = Java object (16 bytes header + pointer + char array)
# - Immutable: every operation creates NEW String object
# - UTF-16 encoding: 2 bytes per character (wasteful for ASCII)
# - GC pressure: millions of tiny String objects → frequent GC pauses
# - Example: LOWER("Hello") creates a new String "hello" → old one becomes garbage

# Photon string handling (C++):
# - Raw UTF-8 byte arrays (1 byte per ASCII char)
# - In-place operations where possible (no allocation)
# - No GC (manual memory management)
# - SIMD: compare/search 32 bytes at once (AVX-256)
# - Result: 6-8x faster for string-heavy operations

# Your pipeline rewritten for maximum Photon benefit:
parsed_logs = (raw_logs
    # URL extraction (Photon: native regexp on raw bytes)
    .withColumn("url", regexp_extract(col("log_line"), r'(https?://\S+)', 1))
    .withColumn("domain", regexp_extract(col("url"), r'://([^/]+)', 1))
    
    # User agent parsing (Photon: vectorized string contains)
    .withColumn("browser", 
        when(col("user_agent").contains("Chrome"), "Chrome")
        .when(col("user_agent").contains("Firefox"), "Firefox")
        .when(col("user_agent").contains("Safari"), "Safari")
        .otherwise("Other"))
    
    # Email cleaning (Photon: native lower + trim)
    .withColumn("email_clean", lower(trim(col("email"))))
    
    # CSV splitting (Photon: native split function)
    .withColumn("fields", split(col("csv_line"), ","))
    .withColumn("field_1", col("fields")[0])
    .withColumn("field_2", col("fields")[1])
)

# EXPECTED PERFORMANCE:
# Standard runtime: 90 minutes (string operations = JVM bottleneck)
# Photon runtime: ~15 minutes (6x speedup for string-heavy workload!)
# This is the BEST CASE for Photon — string ETL is its sweet spot

# Cost comparison:
# Standard: 90 min × 16 workers × $0.312 + DBU = $10.50/run
# Photon: 15 min × 16 workers × $0.312 + Photon DBU = $2.10/run
# Savings: 80% per run!
```

**Key Points:**
- String operations = Photon's biggest advantage (6-8x vs 2-3x for numeric operations)
- Root cause: JVM String is terribly inefficient (immutable objects, GC pressure, UTF-16)
- Photon: raw UTF-8 bytes, SIMD search, zero-copy, no GC → drastically faster
- Log parsing, URL extraction, email processing, text cleaning: all huge Photon wins
- For 2TB text logs: expect 5-7x total speedup (most time is string operations)
- Make sure to use native Spark functions (NOT Python UDFs) to get the benefit

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Full Performance Optimization

**Scenario:** Your critical daily ETL takes 2 hours. After enabling Photon it dropped to 50 minutes (2.4x). Target: under 20 minutes. The remaining bottleneck is a 4-table join on skewed data. Design the complete optimization strategy combining Photon + cluster tuning + query optimization.

<details>
<summary>💡 Hint</summary>
Photon alone won't fix skew. Combine: Photon (execution speed) + AQE skew handling (balanced partitions) + broadcast small tables (eliminate shuffle) + Z-ORDER (data skipping). Attack each bottleneck layer.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT STATE: 50 min with Photon (down from 2 hours on standard)
# BOTTLENECK: 4-table join, one table has skewed customer_id (top key = 20% of data)

# OPTIMIZATION LAYER 1: Fix the skew (biggest remaining bottleneck)
# Enable AQE skew join (splits skewed partitions automatically)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
# Expected impact: the 20-min straggler task → split into 4× 5-min tasks = 5 min
# Savings: ~15 minutes

# OPTIMIZATION LAYER 2: Broadcast small dimension tables
# Table sizes: orders (500M), customers (5M), products (100K), regions (200)
# Broadcast customers + products + regions (all < 2 GB combined)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2147483648")  # 2 GB
# Or use hints:
result = spark.sql("""
    SELECT /*+ BROADCAST(c), BROADCAST(p), BROADCAST(r) */
        o.order_id, o.amount, c.name, p.category, r.region_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    JOIN regions r ON o.region_id = r.region_id
    WHERE o.order_date = '2024-03-15'
""")
# Eliminates 3 shuffle operations! Only orders is shuffled (for the remaining join)
# Expected savings: ~10 minutes (shuffle was 40% of remaining time)

# OPTIMIZATION LAYER 3: Data skipping via Z-ORDER
# Ensure orders table is well-clustered for the WHERE clause
spark.sql("OPTIMIZE production.silver.orders ZORDER BY (order_date, customer_id)")
# Query only scans files relevant to order_date = '2024-03-15'
# Expected savings: ~5 minutes (reads 3% of files instead of 100%)

# OPTIMIZATION LAYER 4: Increase cluster for the heavy phase
# Use 16 workers instead of 8 for the join/aggregation stage
{
    "autoscale": {"min_workers": 8, "max_workers": 16},
}
# More parallelism for the skew-split tasks
# Expected savings: ~5 minutes (more workers = more parallel tasks)

# FINAL RESULT:
# Original (Standard): 120 minutes
# + Photon: 50 minutes (2.4x)
# + AQE skew handling: 35 minutes (-15 min)
# + Broadcast joins: 25 minutes (-10 min)
# + Z-ORDER data skipping: 20 minutes (-5 min)
# + Larger cluster: 15 minutes (-5 min)
# FINAL: 15 minutes ✓ (under 20 min target, 8x total improvement!)
```

**Key Points:**
- Photon alone gives 2-4x (execution speed) but doesn't fix algorithmic issues (skew)
- AQE skew handling: splits hot partitions at runtime (fixes the straggler problem)
- Broadcast: eliminates shuffle for small tables (3 fewer shuffles = huge time savings)
- Z-ORDER: data skipping means reading 3% of data instead of 100%
- Larger cluster: more parallelism for the remaining heavy operations
- Layered approach: each optimization addresses a DIFFERENT bottleneck
- Monitor after each change to confirm expected improvement materialized

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Photon ROI Analysis

**Scenario:** The CFO asks for an ROI analysis on switching your entire platform (50 jobs) to Photon. Current monthly spend: $25K. Estimate savings, implementation effort, and payback period.

<details>
<summary>💡 Hint</summary>
Not all jobs benefit equally. Categorize: SQL-heavy (3-4x speedup), mixed (2x), Python-heavy (1.2x). Weight by current cost to estimate total savings. Implementation: near-zero (runtime version change).
</details>

<details>
<summary>✅ Solution</summary>

```python
# CATEGORIZE 50 JOBS by expected Photon benefit:

JOB_ANALYSIS = {
    "category_1_sql_heavy": {
        "count": 25,
        "current_monthly_cost": "$12,000",
        "workload": "Joins, aggregations, Delta operations",
        "expected_speedup": "3x",
        "projected_cost": "$4,800",  # 60% savings (faster + fewer resources)
        "savings": "$7,200",
    },
    "category_2_mixed": {
        "count": 15,
        "current_monthly_cost": "$8,000",
        "workload": "Some SQL + some Python transforms",
        "expected_speedup": "2x",
        "projected_cost": "$4,800",  # 40% savings
        "savings": "$3,200",
    },
    "category_3_python_heavy": {
        "count": 10,
        "current_monthly_cost": "$5,000",
        "workload": "ML training, pandas operations, Python UDFs",
        "expected_speedup": "1.2x",
        "projected_cost": "$4,500",  # 10% savings (minimal Photon benefit)
        "savings": "$500",
    },
}

# TOTAL PROJECTED SAVINGS:
# $7,200 + $3,200 + $500 = $10,900/month
# Annual: $130,800

# IMPLEMENTATION EFFORT:
# - Change runtime version in 50 job configs: 1 day (trivial, one-line change each)
# - Validate outputs match (checksums): 2-3 days  
# - Replace 15 critical Python UDFs with native functions: 1 week
# - Total: ~2 weeks of engineering time

# PAYBACK PERIOD:
# Implementation cost: 2 weeks × 2 engineers × $80/hr × 80 hrs = $12,800
# Monthly savings: $10,900
# Payback: $12,800 / $10,900 = 1.2 months (pays for itself in 5 weeks!)

ROI_SUMMARY = {
    "current_monthly_spend": "$25,000",
    "projected_monthly_spend": "$14,100",
    "monthly_savings": "$10,900 (44%)",
    "annual_savings": "$130,800",
    "implementation_effort": "2 weeks (2 engineers)",
    "implementation_cost": "$12,800",
    "payback_period": "5 weeks",
    "3_year_roi": "$130,800 × 3 - $12,800 = $379,600",
    "risk": "Near-zero (same code, same results, just faster)",
}

# RECOMMENDATION TO CFO:
# "Switching to Photon saves $10,900/month (44%) with 5-week payback.
#  Implementation is 2 weeks of work with near-zero risk (no code changes 
#  for 85% of jobs, minor refactoring for 15%). Projected 3-year savings: $380K."
```

**Key Points:**
- Not all jobs benefit equally: SQL-heavy (60% savings), mixed (40%), Python-heavy (10%)
- Weight by cost to estimate total: 44% overall savings is realistic
- Implementation: near-zero risk (same API, validated by output comparison)
- Payback: typically 4-6 weeks (very fast ROI)
- The 10 Python-heavy jobs: optionally keep on standard runtime (save the Photon premium)
- UDF refactoring (15 UDFs → native functions): best opportunity for additional savings
- Present to CFO: monthly savings, payback period, risk level, implementation timeline

</details>

</article>
