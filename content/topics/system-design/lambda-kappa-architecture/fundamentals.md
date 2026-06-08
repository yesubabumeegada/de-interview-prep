---
title: "Lambda & Kappa Architecture — Fundamentals"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, lambda-architecture, kappa-architecture, batch, streaming]
---

# Lambda & Kappa Architecture — Fundamentals

## The Problem: Batch Accuracy vs Streaming Speed

Analytics systems face a fundamental tension:
- **Batch processing** is accurate and complete but slow (results available hours later)
- **Streaming processing** is fast (results in seconds) but harder to make accurate (late data, reprocessing)

Lambda and Kappa architectures are two different solutions to this problem.

---

## Lambda Architecture

Nathan Marz's Lambda Architecture (2011): run **both** batch and streaming layers, merge their outputs:

```
              ┌─────────────────────────────────────┐
              │           Batch Layer               │
Sources ──────┤   (Hadoop/Spark, runs on all data) ├──────┐
   │          │   Recomputes accurate results nightly     │   Serving Layer
   │          └─────────────────────────────────────┘   ├──► (merge batch +
   │                                                     │    speed results)
   └──────────┤           Speed Layer               ├────┘
              │   (Spark Streaming/Flink, recent)   │
              │   Fast but possibly imprecise        │
              └─────────────────────────────────────┘
```

### Components

**Batch Layer:**
- Processes all historical data from scratch each run
- Accurate (sees all data, handles late arrivals)
- Slow (hours to complete)
- Output: batch views (pre-computed results)

**Speed Layer:**
- Processes only recent data (last few hours)
- Fast (seconds to minutes latency)
- Compensates for batch layer's high latency
- Output: real-time views (approximate, recent only)

**Serving Layer:**
- Merges batch views + speed views
- Application queries both and combines results
- When batch runs: evicts old speed layer results

### Lambda Example
```python
# Serving layer query: combine batch + speed
def get_revenue_by_region(date_range):
    # Batch view: accurate data up to yesterday
    batch_result = query_batch_layer(
        "SELECT region, SUM(revenue) FROM batch_views.daily_revenue "
        "WHERE date BETWEEN %s AND %s", date_range
    )
    
    # Speed view: today's data (may be slightly inaccurate)
    speed_result = query_speed_layer(
        "SELECT region, SUM(revenue) FROM speed_views.current_day "
        "WHERE date = CURRENT_DATE"
    )
    
    # Merge: batch for historical + speed for today
    return merge_results(batch_result, speed_result)
```

---

## Kappa Architecture

Jay Kreps' Kappa Architecture (2014): **only one layer** — treat everything as a stream:

```
              ┌─────────────────────────────────────┐
Sources ──────► Kafka (long-retention event log)     ├──► Serving Layer
              │                                      │    (materialized views,
              └──────────────────────────────────────┘     key-value store)
                            │
                      Stream Processing
                        (Flink/Spark)
                   ┌─────────────────┐
                   │ Current job     │  (real-time)
                   │ Historical job  │  (reprocessing from beginning of log)
                   └─────────────────┘
```

### Kappa Key Insight
Reprocessing = start a new stream processing job that reads Kafka from offset 0:
- New job: processes historical data quickly (backfill)
- Old job: still serving current requests
- Switch: when new job catches up, point serving layer to new output, stop old job

```
Reprocessing steps:
  1. Code change: fix logic or update model
  2. Deploy new job (v2) reading from Kafka beginning
  3. v2 runs in parallel with v1 (different output table)
  4. v2 catches up to real-time
  5. Serving layer switches from v1 output → v2 output
  6. Decommission v1 job + v1 output table
```

---

## Lambda vs Kappa Comparison

| Dimension | Lambda | Kappa |
|---|---|---|
| Complexity | High (2 code paths to maintain) | Low (one code path) |
| Accuracy | High (batch recomputes everything) | High (if stream processing is correct) |
| Latency | Seconds (speed layer) | Seconds |
| Reprocessing | Full batch rerun (hours) | Replay Kafka (hours, same code) |
| Storage cost | High (store all raw data + batch views) | Lower (Kafka + stream state) |
| Code drift | Risk: batch and speed give different answers | None: same code |
| Modern relevance | Declining | Growing |

---

## Interview Tips

> **Tip 1:** "What problem does Lambda Architecture solve?" — The tension between low-latency and accurate results. The speed layer gives fast answers using recent data; the batch layer gives accurate answers for historical data. The serving layer merges both so users always see current + accurate data. The tradeoff: you must maintain two separate codebases (batch + streaming) that must produce identical results — a significant operational burden.

> **Tip 2:** "What is the main disadvantage of Lambda Architecture?" — You maintain the same business logic in two codebases: one batch (Spark/SQL) and one streaming (Spark Streaming/Flink). These can drift: a bug fix in batch logic must be replicated in streaming logic. This "dual maintenance" problem was the main motivation for Kappa Architecture: one codebase, one processing model.

> **Tip 3:** "Why is Kappa Architecture simpler?" — One code path: streaming logic handles both real-time processing AND historical reprocessing (by replaying Kafka). Reprocessing is just running the same job from the beginning of the Kafka topic. No separate batch system to maintain. The key enabler: Kafka's long-retention log (store months/years of events) makes Kafka the "source of truth" that can be replayed any time.
