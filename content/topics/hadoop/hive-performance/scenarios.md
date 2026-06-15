---
title: "Hive Performance Tuning - Scenario Questions"
topic: hadoop
subtopic: hive-performance
content_type: scenario_question
tags: [hadoop, hive, performance, tez, llap]
---

# Hive Performance Tuning — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Slow Query with No Partition Filter

Your team has a 10 TB Hive table `web_events` partitioned by `event_date`. A junior analyst runs the following query every morning and complains it takes 45 minutes:

```sql
SELECT user_id, COUNT(*) AS events
FROM   web_events
WHERE  event_type = 'PAGE_VIEW'
GROUP BY user_id;
```

The analyst says "I need all historical data, so I can't add a date filter." The SLA requires results in under 5 minutes.

1. Explain why this query is slow.
2. What optimization strategies would you apply without changing the analyst's business requirement?
3. Write the optimized query or configuration.

<details>
<summary>✅ Solution</summary>

**Why it's slow:**

The query has no filter on `event_date` (the partition key), so Hive reads all 10 TB across every partition — a full table scan. Even with Tez, reading 10 TB of data is inherently slow.

**Optimization strategies:**

1. **Enable vectorization + CBO** to reduce CPU overhead during the unavoidable scan:
```sql
SET hive.vectorized.execution.enabled=true;
SET hive.cbo.enable=true;
SET hive.stats.fetch.column.stats=true;
```

2. **Use LLAP** so repeated morning runs hit the IO cache instead of HDFS:
```sql
SET hive.llap.execution.mode=all;
```

3. **Add a secondary index / pre-aggregated summary table**: If the business need is "all historical data," build a daily incremental aggregate:
```sql
-- Run nightly: maintain a running aggregate table
INSERT INTO user_event_summary PARTITION(event_date='2024-01-15')
SELECT user_id, COUNT(*) AS events
FROM   web_events
WHERE  event_date = '2024-01-15' AND event_type = 'PAGE_VIEW'
GROUP BY user_id;

-- Analyst query becomes a sub-second aggregation over a small summary table
SELECT user_id, SUM(events) AS total_events
FROM   user_event_summary
GROUP BY user_id;
```

4. **Tune reducer count** to increase parallelism:
```sql
SET hive.exec.reducers.bytes.per.reducer=67108864;  -- 64 MB per reducer → ~156 reducers for 10 TB
```

**Root lesson:** When a full-scan is genuinely required, the answer is a pre-aggregated summary table maintained by incremental nightly jobs, not making the analyst wait 45 minutes.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Skewed Join Causing Single-Reducer Bottleneck

You have two tables:
- `orders` (500 GB, 2 billion rows) with `customer_id`
- `customers` (1 GB, 5 million rows) with `customer_id`

The join query runs in 2 hours, but the Tez UI shows 999 reducers finishing in 20 minutes and one reducer taking the remaining 100 minutes with 800 million rows:

```sql
SELECT o.order_id, c.name, o.amount
FROM   orders    o
JOIN   customers c ON o.customer_id = c.customer_id;
```

Investigation shows `customer_id = -1` (guest orders) accounts for 40% of all order rows.

1. Why is MAPJOIN not helping here?
2. Implement two different solutions with HiveQL.
3. Compare the tradeoffs.

<details>
<summary>✅ Solution</summary>

**Why MAPJOIN doesn't help:**

`customers` is 1 GB, which exceeds the default `hive.mapjoin.smalltable.filesize=25000000` (25 MB). Even after raising the threshold, broadcasting 1 GB per mapper across hundreds of mappers wastes memory. More importantly, the skew is on the `orders` side, not `customers`.

**Solution 1: Hive built-in skew join**

```sql
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;  -- declare skew threshold

-- Explicitly declare the skewed value
CREATE TABLE orders_skewed_keys (customer_id BIGINT)
AS SELECT -1;  -- the known skewed key

SET hive.skewjoin.mapjoin.map.tasks=200;

SELECT o.order_id, c.name, o.amount
FROM   orders    o
JOIN   customers c ON o.customer_id = c.customer_id;
```

Hive splits execution: normal keys use the regular reduce-side join; the skewed key (`-1`) is processed by a separate map join job.

**Solution 2: Manual split + union**

```sql
-- Handle skewed key separately with map join
SELECT /*+ MAPJOIN(c) */
       o.order_id, c.name, o.amount
FROM  (SELECT * FROM orders WHERE customer_id = -1) o
JOIN   customers c ON o.customer_id = c.customer_id

UNION ALL

-- Non-skewed rows use normal join
SELECT o.order_id, c.name, o.amount
FROM  (SELECT * FROM orders WHERE customer_id != -1) o
JOIN   customers c ON o.customer_id = c.customer_id;
```

**Tradeoffs:**

| Approach | Pros | Cons |
|---|---|---|
| Hive `skewjoin` | Automatic; no query rewrite | Requires two MR passes; threshold tuning needed |
| Manual UNION ALL split | Full control; predictable | Requires knowing skewed keys; query is verbose |
| Salting | Handles unknown skew distribution | Requires exploding dimension table; complex |

For known, static skewed keys (like `-1` for guest orders), the manual UNION ALL split is cleanest and most performant.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: LLAP Cluster Performing Worse Than Plain Tez

Your team migrated an interactive reporting cluster from plain Tez to LLAP expecting 5–10× speedup. Instead, p99 query latency got worse (3 minutes → 7 minutes) and users report frequent "LLAP daemon not available" errors.

LLAP configuration:
- 10 nodes, each with 128 GB RAM
- LLAP daemon: `--size 100g --xmx 8g --cache 80g --executors 8`
- Workload: 200 concurrent BI users, queries range from 1 second to 30 minutes

Diagnose the configuration errors and provide a corrected setup with explanation.

<details>
<summary>✅ Solution</summary>

**Diagnosed problems:**

**Problem 1: Heap is too small relative to executors**
- `--xmx 8g` with `--executors 8` means only 1 GB heap per executor thread.
- Each hash join or aggregation operator can easily exceed 1 GB, causing GC pressure and OOM kills → "daemon not available" errors.

**Problem 2: Cache is consuming memory needed for execution**
- `--cache 80g` leaves only `100g - 80g = 20g` for executor heap + overhead.
- With `--xmx 8g`, JVM heap is only 8 GB; remaining 12 GB is overhead (off-heap buffers, OS).
- Cache is so large it starves executor memory.

**Problem 3: Long-running queries (30 minutes) and short queries share the same LLAP pool**
- Long ETL-style queries hold executor threads for 30 minutes, blocking short BI queries.
- LLAP is designed for interactive sub-minute queries; mixing long jobs degrades tail latency.

**Problem 4: Too many concurrent users for executor count**
- 10 nodes × 8 executors = 80 executor slots total.
- 200 concurrent users → 2.5 users per slot → heavy queuing.

**Corrected configuration:**

```bash
# Per node (128 GB RAM)
hive --service llap \
  --name llap0 \
  --instances 10 \
  --size 90g \         # leave 38 GB for OS, YARN NM, other services
  --xmx 20g \          # 20 GB heap: room for aggregations and hash tables
  --cache 60g \        # 60 GB off-heap IO cache (ORC data)
  --executors 12 \     # 12 threads: (90g - 60g cache - 20g heap) ≈ 10g overhead for 12 threads
  --iothreads 8
```

```sql
-- Route long-running queries to Tez instead of LLAP
SET hive.llap.execution.mode=auto;
-- "auto" lets Hive decide: small, selective queries → LLAP; large scans → Tez

-- Set query timeout: queries > 5 min fall back to Tez
SET hive.llap.auto.enforce.vectorized=false;
SET hive.llap.auto.max.input.size=10737418240;  -- 10 GB: queries touching > 10 GB go to Tez
```

```xml
<!-- hive-site.xml: queue-based routing -->
<property>
  <name>hive.server2.tez.default.queues</name>
  <value>interactive,batch</value>
</property>
<!-- BI users → interactive queue (LLAP) -->
<!-- ETL jobs → batch queue (plain Tez) -->
```

**Corrected memory math:**

| Component | Memory |
|---|---|
| OS + YARN NM | 16 GB |
| LLAP daemon heap (`--xmx`) | 20 GB |
| LLAP IO cache (off-heap) | 60 GB |
| Executor overhead (12 threads) | 12 GB |
| **Total** | **108 GB** (< 128 GB ✓) |

**Expected outcome after fix:**
- No more OOM kills on daemon.
- BI queries (< 10 GB input) stay in LLAP, hitting the IO cache after warm-up.
- ETL queries automatically routed to Tez, not competing with BI users.
- p99 latency for BI queries: ~15–30 seconds (from 7 minutes).

</details>
</article>
