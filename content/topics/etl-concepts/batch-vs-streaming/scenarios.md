---
title: "Batch vs Streaming - Scenario Questions"
topic: etl-concepts
subtopic: batch-vs-streaming
content_type: scenario_question
tags: [etl, batch, streaming, interview, scenarios, architecture]
---

# Scenario Questions — Batch vs Streaming

<article data-difficulty="junior">

## 🟢 Junior: Choose the Right Processing Mode

**Scenario:** For each use case below, recommend batch or streaming and explain why:
1. Daily email campaign report showing open rates
2. Detecting credit card fraud in real-time
3. Loading CSV files uploaded by vendors every morning
4. Showing "currently viewing" count on a product page

<details>
<summary>✅ Solution</summary>

| Use Case | Mode | Reasoning |
|----------|------|-----------|
| Daily email report | **Batch** | Daily granularity is fine. No urgency. Schedule at end of day. |
| Credit card fraud | **Streaming** | Must decide in milliseconds before transaction completes. Latency = money lost. |
| Vendor CSV loading | **Batch** | Bounded input (files), no real-time need. Process when file arrives. |
| "Currently viewing" count | **Streaming** | Must reflect current state in real-time. Stale counts = bad UX. |

**Key principle:** Match the processing mode to the **business latency requirement**, not the technical capability. Don't over-engineer.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Explain Exactly-Once to a Non-Technical Stakeholder

**Scenario:** Your product manager asks: "Our pipeline sometimes produces duplicate records in the dashboard. Can you explain why and how we'll fix it?" Explain the concepts of at-least-once delivery and exactly-once processing in business terms.

<details>
<summary>✅ Solution</summary>

**Explanation for PM:**

"Think of our pipeline like a delivery service. When a package (data record) gets lost in transit, the system re-sends it. This means sometimes the same package arrives twice — that's why you see duplicates.

**Why it happens:** Our system prioritizes 'never lose data' over 'never duplicate data.' If there's a network blip or server restart, it resends the last batch just to be safe.

**How we'll fix it:**
1. **Short term:** Add a deduplication step — each record has a unique ID, and we'll ignore any ID we've already processed (like ignoring a re-delivered package you already signed for).
2. **Long term:** Implement exactly-once semantics — the system will track exactly which records have been delivered and won't resend them.

**Impact:** You'll see a ~2-3% reduction in total counts once we deploy the dedup (that was the duplication rate). Reporting will be more accurate."

**Technical implementation:**
```sql
-- Dedup at read time (immediate fix)
SELECT DISTINCT ON (event_id) *
FROM raw_events
ORDER BY event_id, processed_at DESC;

-- Dedup at write time (permanent fix)
MERGE INTO target t
USING staging s ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT ...;
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Near-Real-Time Dashboard Pipeline

**Scenario:** Your company wants a dashboard showing:
- Orders placed in the last hour (refreshed every minute)
- Top 10 products by revenue (last 24 hours)
- Inventory levels (refreshed every 5 minutes)

Current state: Everything is batch (nightly). Design the pipeline architecture.

<details>
<summary>💡 Hint</summary>

Not everything needs the same freshness. Consider which metric needs streaming vs. which can be micro-batch.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
Data Source          Processing              Serving           Dashboard
──────────────────────────────────────────────────────────────────────
Orders DB (CDC) ──→ Spark Streaming ──→ Delta Table ──→ ┐
                    (trigger: 30 sec)    (hot layer)     │
                                                         ├──→ Dashboard
Sales Events ─────→ Spark Micro-batch ──→ Aggregate ──→ │    (Grafana/
                    (trigger: 5 min)      Table          │     Superset)
                                                         │
Inventory API ────→ Scheduled Job ──────→ Snapshot ──→  ┘
                    (every 5 min)         Table
```

**Component details:**

1. **Orders (1-minute freshness):** CDC from PostgreSQL → Kafka → Spark Structured Streaming with 30-second trigger → Delta Lake hot table. Dashboard queries this directly.

2. **Top products (5-minute freshness):** Read from the same Kafka topic, but with a 5-minute tumbling window. Pre-compute "top 10 by revenue per 24-hour rolling window." Store in a materialized view.

3. **Inventory (5-minute freshness):** Pull API every 5 minutes via a scheduled Airflow task. Full snapshot write to Delta. (Inventory changes infrequently enough that polling is fine.)

**Why NOT full streaming for everything:**
- Top-10 products over 24 hours requires large state (all orders for 24h). Micro-batch with periodic recomputation is cheaper and simpler.
- Inventory API is pull-based — can't "stream" it. Polling every 5 min meets the SLA.

**Cost optimization:**
- Single Spark cluster handles both streaming queries (order count) and micro-batch (top products)
- Inventory job runs on a small scheduled instance (no always-on cost)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handle Late-Arriving Data in a Streaming Pipeline

**Scenario:** Your streaming pipeline aggregates clickstream events into 5-minute windows for a real-time dashboard. You notice that ~3% of events arrive 2-15 minutes late due to mobile app offline sync. The dashboard shows dips that correct themselves later. Design a solution.

<details>
<summary>✅ Solution</summary>

**Option 1: Generous watermark (simplest)**

```python
# Allow 15 minutes of lateness
events.withWatermark("event_time", "15 minutes") \
    .groupBy(window("event_time", "5 minutes"), "page") \
    .agg(count("*").alias("views"))
```

**Tradeoff:** Dashboard numbers for a given window aren't "final" until 15 minutes after the window closes. State is kept for 15 minutes longer.

**Option 2: Two-layer approach (dashboard + correction)**

```python
# Layer 1: Low watermark for real-time dashboard (accept ~3% undercounting)
realtime_counts = events \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window("event_time", "5 minutes"), "page") \
    .agg(count("*").alias("views"))

# Layer 2: Batch correction every 30 minutes
# Recompute windows from the raw event log including late arrivals
corrected_counts = spark.read.parquet("s3://raw/clickstream/") \
    .filter(col("event_time") > now() - interval("2 hours")) \
    .groupBy(window("event_time", "5 minutes"), "page") \
    .agg(count("*").alias("views_corrected"))

# Dashboard shows realtime_counts with a "preliminary" badge
# After 30 min, switches to corrected_counts (final)
```

**Option 3: Update mode (if sink supports upserts)**

```python
# Output mode "update" — re-emits windows when late data arrives
events.withWatermark("event_time", "15 minutes") \
    .groupBy(window("event_time", "5 minutes"), "page") \
    .agg(count("*").alias("views")) \
    .writeStream \
    .outputMode("update") \  # Re-emit updated windows
    .format("delta") \       # Delta supports upserts
    .start()
```

**Recommendation:** Option 3 (update mode + Delta) is cleanest. The dashboard always reads from Delta and shows the latest value. Windows automatically get corrected when late data arrives. After watermark expires (15 min past window), state is cleaned up.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Migrate from Lambda to Kappa Architecture

**Scenario:** Your team maintains a Lambda architecture with:
- Batch path: Airflow → Spark → Hive (runs nightly, 6-hour job)
- Speed path: Kafka Streams → Cassandra (real-time, ~2sec latency)
- Serving: Presto queries both, unions results

Problems: Dual codebase (bugs in one path not in other), reconciliation failures, 2x maintenance burden. Design a migration to Kappa architecture.

<details>
<summary>✅ Solution</summary>

**Migration Plan:**

**Phase 1: Unify Processing Logic**
```python
# Single transformation module used by both old paths
# (Bridge phase — validates parity before migration)
from transforms import apply_business_logic

# Old batch path calls:
batch_result = apply_business_logic(spark_batch_df)

# Old streaming path calls:
stream_result = apply_business_logic(streaming_df)

# Run both in parallel, compare outputs for 2 weeks → validate parity
```

**Phase 2: Implement Kappa (Single Streaming Path)**
```python
# Kafka as the source of truth (retain events for 30 days)
# Single Spark Structured Streaming job replaces both paths

# For "batch": trigger=availableNow (process all available, stop)
# For "streaming": trigger=processingTime("30 seconds")

events = spark.readStream.format("kafka") \
    .option("subscribe", "raw.events") \
    .option("startingOffsets", "earliest") \  # Can replay from 30 days ago
    .load()

processed = apply_business_logic(parse_events(events))

# Write to Delta Lake (unified serving layer)
query = processed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://checkpoints/unified/") \
    .trigger(processingTime="30 seconds") \
    .start("s3://warehouse/events_unified/")
```

**Phase 3: Handle Reprocessing**
```python
# When business logic changes, reprocess history:
# 1. Deploy new version of apply_business_logic
# 2. Start new streaming job with fresh checkpoint + "earliest" offset
# 3. Write to new Delta table version (or new path)
# 4. Swap serving layer pointer once caught up

# Reprocessing rate: ~500K records/sec (Spark can backfill 30 days in ~hours)
```

**Phase 4: Decommission Old Infrastructure**
- Remove Hive tables (replaced by Delta Lake)
- Remove Cassandra (replaced by Delta Lake time-travel queries)
- Remove Airflow batch DAG (replaced by always-on streaming job)
- Remove Presto union query (single table now)

**Architecture comparison:**

| Aspect | Lambda (Before) | Kappa (After) |
|--------|----------------|---------------|
| Code paths | 2 (batch + stream) | 1 |
| Correctness | Reconciliation required | Single source of truth |
| Reprocessing | 6-hour batch job | Replay from Kafka (2-3 hours) |
| Serving | Presto union + merge logic | Direct Delta Lake queries |
| Maintenance | High (2 systems) | Low (1 system) |
| Latency | 2 sec (speed) / 24h (batch) | 30 sec (unified) |

**Risks and mitigations:**
- **Risk:** Kafka retention (30 days) insufficient for full reprocess → **Mitigation:** Archive to S3, replay from S3 when needed
- **Risk:** Streaming job failure affects both real-time and historical → **Mitigation:** Separate streaming job (real-time) from backfill job (same code, different trigger)
- **Risk:** Schema evolution in single path → **Mitigation:** Schema registry + backward-compatible changes only

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cost-Optimize a $50K/Month Streaming Pipeline

**Scenario:** Your streaming pipeline costs $50K/month on AWS (EMR Spark Streaming cluster). It processes clickstream events (10B/day) into hourly aggregates for dashboards. The dashboard SLA is "data no older than 15 minutes." Leadership wants to cut costs by 60%. Design the optimization.

<details>
<summary>✅ Solution</summary>

**Current state analysis:**
- 10B events/day = ~115K events/sec average
- Always-on EMR cluster (24/7) = expensive
- Hourly aggregates with 15-min SLA = mismatch (why process continuously for hourly output?)

**Optimization strategy:**

**1. Switch from continuous streaming to triggered micro-batch (-40% cost)**
```python
# Instead of always-on processing, trigger every 5 minutes
# Still meets 15-min SLA (5 min trigger + ~3 min processing = 8 min)

query = events.writeStream \
    .trigger(processingTime="5 minutes") \  # Process every 5 min instead of continuously
    .format("delta") \
    .start()

# Allows cluster auto-scaling between triggers (nodes idle → scale down)
```

**2. Use Spot/Preemptible instances for workers (-30% on compute)**
```
EMR Configuration:
  Core nodes: 4x On-Demand (stability for checkpoints)
  Task nodes: 12x Spot (80% discount, can lose them)
  
  With graceful decommissioning:
  spark.speculation = true  (re-run tasks on lost spot nodes)
```

**3. Partition pruning and early aggregation (-20% processing)**
```python
# Pre-aggregate at ingestion (reduce 10B events → ~100M aggregated rows/day)
# Push aggregation as close to source as possible

# Instead of storing every click, pre-aggregate by minute:
events.groupBy(
    window("event_time", "1 minute"),
    "page_id", "user_segment", "device_type"
).agg(
    count("*").alias("click_count"),
    countDistinct("user_id").alias("unique_users")
).writeStream...

# 10B raw events → ~100M pre-aggregated rows (100x reduction)
# Downstream hourly aggregation is trivial on 100M rows
```

**4. Right-size the cluster based on actual utilization**
```
Before: 20 x m5.2xlarge (24/7) = $46K/month
After:  4 core (on-demand) + 8 task (spot) x m5.xlarge
        with auto-scaling (scale down between triggers)
        = ~$15K/month
```

**Cost breakdown:**

| Component | Before | After | Savings |
|-----------|--------|-------|---------|
| EMR cluster | $46,000 | $15,000 | -67% |
| S3 storage | $2,000 | $2,000 | 0% |
| Kafka (MSK) | $2,000 | $2,000 | 0% |
| **Total** | **$50,000** | **$19,000** | **-62%** |

**SLA validation:**
- Trigger: every 5 minutes
- Processing time: ~2-3 minutes (pre-aggregated data is small)
- End-to-end latency: 7-8 minutes < 15-minute SLA ✅

</details>

</article>
