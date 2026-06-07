---
title: "Pig - Scenario Questions"
topic: hadoop
subtopic: pig
content_type: scenario_question
tags: [hadoop, pig, scenarios, interview, log-parsing, sessionization, optimization]
---

# Scenario Questions — Pig

<article data-difficulty="junior">

## 🟢 Junior: Basic Log Parsing and Aggregation

**Scenario:** You have Apache web server access logs stored on HDFS at `/data/raw/weblogs/2024-01-15/`. Each line follows the Combined Log Format. You need to:
1. Count requests per HTTP status code
2. Find the top 10 most requested URLs
3. Store results to `/data/output/log_analysis/`

<details><summary>💡 Hint</summary>

Use `TextLoader` to read raw log lines, then `REGEX_EXTRACT` to parse fields. Group by the field you want to aggregate, then use `COUNT` in a `FOREACH ... GENERATE`.

</details>

<details><summary>✅ Solution</summary>

```pig
-- log_analysis.pig

-- Step 1: Load raw log lines as text
raw_logs = LOAD '/data/raw/weblogs/2024-01-15/'
           USING TextLoader() AS (line:chararray);

-- Step 2: Parse with regex
-- Apache Combined Log Format:
-- IP - - [timestamp] "METHOD /url HTTP/1.1" STATUS BYTES
parsed = FOREACH raw_logs GENERATE
  REGEX_EXTRACT(line, '"(?:GET|POST|PUT|DELETE|HEAD) (\\S+)', 1) AS url,
  (int)REGEX_EXTRACT(line, '" (\\d{3}) ', 1) AS status_code;

-- Step 3: Filter out unparseable lines
clean = FILTER parsed BY url IS NOT NULL AND status_code IS NOT NULL;

-- Part 1: Count by status code
by_status = GROUP clean BY status_code;
status_counts = FOREACH by_status GENERATE
  group AS status_code,
  COUNT(clean) AS request_count;

sorted_status = ORDER status_counts BY status_code ASC;

-- Part 2: Top 10 URLs
by_url = GROUP clean BY url;
url_counts = FOREACH by_url GENERATE
  group AS url,
  COUNT(clean) AS hits;

sorted_urls = ORDER url_counts BY hits DESC;
top_10 = LIMIT sorted_urls 10;

-- Step 4: Store results
STORE sorted_status INTO '/data/output/log_analysis/status_counts'
      USING PigStorage('\t');

STORE top_10 INTO '/data/output/log_analysis/top_urls'
      USING PigStorage('\t');
```

**Run the script:**
```bash
pig -x tez \
    -logfile /tmp/log_analysis.log \
    log_analysis.pig
```

**Verify output:**
```bash
hdfs dfs -cat /data/output/log_analysis/status_counts/*
# 200    45823
# 301    1203
# 404    892
# 500    45

hdfs dfs -cat /data/output/log_analysis/top_urls/*
# /index.html    12034
# /api/products  8821
```

**Key concepts demonstrated:**
- `TextLoader` for unstructured log files
- `REGEX_EXTRACT` for field parsing
- `FILTER ... IS NOT NULL` for data quality
- `GROUP BY` + `COUNT` for aggregation
- `ORDER BY` + `LIMIT` for top-N results
- Multiple `STORE` statements (Pig optimizes into fewer MR jobs via multi-query)

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Sessionization of Clickstream Data

**Scenario:** You have a clickstream dataset at `/data/raw/clickstream/dt=2024-01-15/` with schema: `user_id, event_type, url, timestamp_ms`. A "session" ends when a user is inactive for more than 30 minutes. Design a Pig script that:
1. Groups events into sessions per user
2. Computes per-session: session duration, page view count, and whether it contained a purchase event
3. Outputs `/data/output/sessions/dt=2024-01-15/`

<details><summary>💡 Hint</summary>

Sessionization requires: (1) sort by `user_id, timestamp_ms`, (2) group by `user_id`, (3) apply a UDF that walks through the sorted bag and assigns session IDs when the gap exceeds 30 minutes. You'll need a custom Java or Python UDF.

</details>

<details><summary>✅ Solution</summary>

**Python UDF for sessionization:**
```python
# sessionize.py
SESSION_GAP_MS = 30 * 60 * 1000  # 30 minutes

@outputSchema('sessions:bag{t:(session_id:chararray, event_type:chararray, url:chararray, ts:long)}')
def sessionize(events):
    """
    Input: bag of (event_type, url, timestamp_ms) sorted by timestamp
    Output: bag with session_id assigned to each event
    """
    if not events:
        return []

    result = []
    session_num = 0
    last_ts = None

    # Sort by timestamp (bag may not be ordered)
    sorted_events = sorted(events, key=lambda x: x[2])  # sort by ts (index 2)

    for event in sorted_events:
        event_type, url, ts = event[0], event[1], event[2]
        if last_ts is not None and (ts - last_ts) > SESSION_GAP_MS:
            session_num += 1
        result.append(('session_{}'.format(session_num), event_type, url, ts))
        last_ts = ts

    return result
```

**Pig sessionization script:**
```pig
-- sessionization.pig
REGISTER 'sessionize.py' USING jython AS session_lib;

-- Load clickstream data
events = LOAD '/data/raw/clickstream/dt=2024-01-15/'
         USING PigStorage('\t')
         AS (user_id:chararray, event_type:chararray, url:chararray, ts:long);

-- Data quality filter
clean_events = FILTER events BY 
  user_id IS NOT NULL AND ts IS NOT NULL AND ts > 0;

-- Group by user (sort inside UDF)
by_user = GROUP clean_events BY user_id;

-- Apply sessionization UDF
sessionized = FOREACH by_user GENERATE
  group AS user_id,
  FLATTEN(session_lib.sessionize(clean_events.(event_type, url, ts)))
    AS (session_id:chararray, event_type:chararray, url:chararray, ts:long);

-- Now group by (user_id, session_id) for session-level aggregation
by_session = GROUP sessionized BY (user_id, session_id);

session_stats = FOREACH by_session {
  page_views = FILTER sessionized BY event_type == 'page_view';
  purchases = FILTER sessionized BY event_type == 'purchase';
  GENERATE
    FLATTEN(group) AS (user_id, session_id),
    MIN(sessionized.ts) AS session_start_ms,
    MAX(sessionized.ts) AS session_end_ms,
    (MAX(sessionized.ts) - MIN(sessionized.ts)) AS duration_ms,
    COUNT(page_views) AS page_view_count,
    COUNT(purchases) AS purchase_count,
    (COUNT(purchases) > 0 ? 'true' : 'false') AS had_purchase;
}

-- Filter out bounce sessions (< 2 page views)
engaged_sessions = FILTER session_stats BY page_view_count >= 2;

-- Store results
STORE engaged_sessions INTO '/data/output/sessions/dt=2024-01-15/'
      USING PigStorage('\t');
```

**Verify output:**
```bash
hdfs dfs -cat /data/output/sessions/dt=2024-01-15/* | head -5
# user_123  session_0  1705276800000  1705279200000  2400000  5  1  true
# user_123  session_1  1705290000000  1705291800000  1800000  3  0  false
# user_456  session_0  1705280000000  1705282000000  2000000  4  0  false
```

**Performance considerations:**
- Sort inside the UDF, not via Pig `ORDER` (avoids extra MR job)
- Use Tez mode for in-memory pipeline between GROUP and FOREACH
- Set sufficient reducer memory if users have large event counts: `SET mapreduce.reduce.memory.mb 8192`

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Design and Optimize a Complex Multi-Join Pig Pipeline Processing 10TB Daily

**Scenario:** Your team has inherited a legacy Pig pipeline that processes 10TB of clickstream + order + customer data daily. The current pipeline takes 6 hours (SLA: 4 hours). It joins 4 datasets, runs sessionization, computes user lifetime value, and outputs 3 aggregated datasets. Profile and optimize this pipeline.

<details><summary>💡 Hint</summary>

Profiling steps: (1) use `EXPLAIN` to analyze the MR plan, (2) check for missing replicated joins, (3) look for data skew, (4) identify unnecessary sorts, (5) check parallelism settings, (6) evaluate migrating to Tez or Spark as the ultimate fix.

</details>

<details><summary>✅ Solution</summary>

**Step 1: Profile the existing pipeline**

```bash
# Get job timeline from YARN
yarn application -list -appStates FINISHED | grep pig | head -20

# Explain the plan
pig -x tez -e "EXPLAIN -script pipeline.pig" 2>&1 | tee plan.txt

# Check for data skew
pig << 'EOF'
clicks = LOAD '/data/raw/clicks' USING PigStorage('\t')
         AS (user_id:chararray, session_id:chararray, ts:long);
by_user = GROUP clicks BY user_id;
key_sizes = FOREACH by_user GENERATE group, COUNT(clicks) AS cnt;
large_keys = FILTER key_sizes BY cnt > 100000;
DUMP large_keys;
EOF
```

**Step 2: Identify bottlenecks**

```
Common issues found in 10TB pipelines:
1. Inner JOIN instead of replicated JOIN for small lookup tables
2. Missing PARALLEL setting (defaulting to 1 reducer)
3. No data skew handling for hot users
4. Multiple full scans of the same 10TB dataset
5. Sorting large datasets unnecessarily
6. No Tez (running on slow MapReduce)
```

**Step 3: Apply optimizations**

```pig
-- BEFORE (slow): Standard join, 10TB + 500MB customer lookup
orders_enriched = JOIN clicks BY user_id, customers BY user_id;

-- AFTER (fast): Replicated join for small dimension table
orders_enriched = JOIN clicks BY user_id, customers BY user_id USING 'replicated';
-- Customers (500MB) broadcast to all mappers, no reduce needed
-- 40% time savings on join step

-- BEFORE: No parallelism setting
by_user = GROUP clicks BY user_id;

-- AFTER: Explicit parallelism
SET default_parallel 500;
by_user = GROUP clicks BY user_id PARALLEL 500;
-- Estimated: 10TB / 200MB per reducer = 50 reducers minimum
-- Use 500 for headroom

-- BEFORE: Separate MR jobs for each STORE (3 jobs reading base data 3x)
STORE result1 INTO '/output/result1';
STORE result2 INTO '/output/result2';
STORE result3 INTO '/output/result3';

-- AFTER: Multi-query (Pig already handles this, but verify it's enabled)
-- Check: pig -optimizer_off MultiQueryOptimizer (test without to confirm savings)

-- BEFORE: Missing skewed join
user_product_join = JOIN clicks BY user_id, user_profiles BY user_id;
-- Hot users (influencers, bots) cause single reducer hotspot

-- AFTER: Skewed join
user_product_join = JOIN clicks BY user_id, user_profiles BY user_id USING 'skewed';
-- 25% improvement for skewed datasets
```

**Step 4: Switch to Tez**

```bash
# Before (MapReduce): 6 hours
pig -x mapreduce pipeline.pig

# After (Tez): ~3.5 hours (eliminates HDFS writes between stages)
pig -x tez \
    -Dtez.am.resource.memory.mb=4096 \
    -Dtez.task.resource.memory.mb=8192 \
    -Dtez.runtime.io.sort.mb=1024 \
    -Dtez.runtime.sort.threads=4 \
    pipeline.pig
```

**Step 5: Consider Spark migration for long-term solution**

```python
# PySpark equivalent of the full pipeline
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder \
    .appName("pipeline_migration") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# Replicated join equivalent: broadcast the small table
customers = spark.table("raw.customers")
clicks = spark.table("raw.clicks")

# Broadcast join (equivalent to Pig replicated join)
enriched = clicks.join(F.broadcast(customers), "user_id")

# Sessionization using window functions (no UDF needed!)
window = Window.partitionBy("user_id").orderBy("ts")
enriched_with_gaps = enriched.withColumn(
    "prev_ts", F.lag("ts").over(window)
).withColumn(
    "gap_ms", F.col("ts") - F.col("prev_ts")
).withColumn(
    "new_session", (F.col("gap_ms") > 1800000) | F.col("prev_ts").isNull()
).withColumn(
    "session_id", F.sum(F.col("new_session").cast("int")).over(
        window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
)

# Aggregate (equivalent to Pig FOREACH on GROUPed bag)
session_stats = enriched_with_gaps.groupBy("user_id", "session_id").agg(
    F.min("ts").alias("session_start"),
    F.max("ts").alias("session_end"),
    (F.max("ts") - F.min("ts")).alias("duration_ms"),
    F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_views"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases")
)

# Write 3 outputs from one scan (equivalent to Pig multi-query)
session_stats.cache()  # cache before multiple writes
session_stats.write.mode("overwrite").parquet("/data/output/sessions")
session_stats.filter("purchases > 0").write.mode("overwrite").parquet("/data/output/converting_sessions")
session_stats.groupBy("user_id").agg(F.sum("purchases").alias("ltv")).write.mode("overwrite").parquet("/data/output/user_ltv")
```

**Results after optimization:**

| Optimization | Time Saved | Cumulative |
|-------------|-----------|-----------|
| Tez execution | 1.5 hrs | 4.5 hrs |
| Replicated joins | 0.5 hrs | 4.0 hrs |
| Parallelism tuning | 0.3 hrs | 3.7 hrs |
| Skewed join fix | 0.2 hrs | 3.5 hrs |
| **Spark migration** | **1.5 hrs additional** | **2.0 hrs** |

SLA of 4 hours met with optimized Pig. Spark migration gets to 2 hours with further headroom.

</details>
</article>
