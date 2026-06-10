---
title: "Study Roadmaps — The Mid-Level DE Track"
topic: interview-prep
subtopic: study-roadmaps
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [interview-prep, roadmap, mid-level, career]
---

# Study Roadmaps — The Mid-Level Data Engineer Track

You have 1–3 years of DE experience (or finished the junior track and shipped real pipelines). The mid-level interview tests **depth where you claim it and breadth where the team needs it**. This 16-week track builds both.

**Time budget:** 8–12 hours/week alongside a full-time job.

---

## What Changes at Mid-Level Interviews

| Junior loop tests | Mid-level loop tests |
|---|---|
| Can you write the query? | Can you make it fast and explain why? |
| Can you write a script? | Can you structure, test, and productionize it? |
| Do you know what a DAG is? | Have you debugged a 3 a.m. backfill gone wrong? |
| Vocabulary | Trade-offs and war stories |

---

## The Track at a Glance

```mermaid
gantt
    title Mid-Level DE 16-Week Roadmap
    dateFormat  W
    axisFormat Week %W
    section Distributed Compute
    PySpark depth            :a1, 0, 4w
    Lakehouse (Delta/Iceberg):a2, 3, 2w
    section Streaming & Modeling
    Kafka                    :b1, 5, 3w
    dbt                      :b2, 7, 2w
    section Reliability
    Data Quality             :c1, 9, 2w
    System Design intro      :c2, 10, 3w
    section Breadth
    Second cloud             :d1, 13, 2w
    Mock loops               :d2, 15, 1w
```

---

## Block 1 — PySpark Depth (Weeks 1–4)

Study the **pyspark** topic past the surface:

- **Week 1:** DataFrame API fluency, lazy evaluation, the Spark UI
- **Week 2:** joins at scale — broadcast vs sort-merge, skew detection and salting
- **Week 3:** partitioning, `repartition` vs `coalesce`, file sizing, shuffle mechanics
- **Week 4:** caching, memory model basics, common OOM causes, testing PySpark code

**Milestone:** explain, with a diagram, what happens in a shuffle and name two ways to reduce one. Then answer: "your join went from 10 minutes to 4 hours — walk me through your debugging."

```python
# Be able to discuss why this matters, not just write it:
from pyspark.sql import functions as F

big = spark.table("events")          # 2 TB
small = spark.table("dim_country")   # 4 MB

joined = big.join(F.broadcast(small), "country_code")  # avoids shuffling 2 TB
```

Pair this with the **databricks** topic if your target market uses it (most do): job clusters vs all-purpose, Unity Catalog vocabulary, when to use SQL warehouses.

---

## Block 2 — Lakehouse (Weeks 4–5, overlaps PySpark)

Study **data-lakehouse**:

- Delta Lake / Iceberg: ACID on object storage, time travel, `MERGE`
- `OPTIMIZE`/compaction, Z-ordering or clustering — small-file problem and cures
- Medallion architecture (bronze/silver/gold) — be able to defend *and* criticize it

**Milestone:** whiteboard a medallion layout for a clickstream source and state what schema enforcement happens at each layer.

---

## Block 3 — Kafka & Streaming Foundations (Weeks 6–8)

Study **kafka**, with **real-time-streaming** fundamentals alongside:

- Topics, partitions, consumer groups, offsets — mechanically, not just names
- Delivery semantics: at-least-once vs exactly-once, and what idempotent consumers mean in practice
- Ordering guarantees (per-partition only) and why key choice matters
- Lag: how you detect it, what causes it, three remedies

**Milestone:** answer cold: "Orders must be processed in order per customer, and we can't lose any. Design the topic, keys, and consumer behavior."

---

## Block 4 — dbt & Analytics Engineering (Weeks 8–9)

Study **dbt**:

- Models, `ref()`, materializations (view/table/incremental) and when each fits
- Tests (unique, not_null, relationships) and how they slot into CI
- Incremental models: `is_incremental()`, late-arriving data handling
- Documentation and lineage — why analysts love it, when it sprawls

**Milestone:** explain how you'd migrate 40 cron-scheduled SQL scripts into a dbt project, including the order you'd do it in.

---

## Block 5 — Data Quality as a Discipline (Weeks 10–11)

Study **data-quality** beyond null checks:

- Dimensions: freshness, volume, schema, distribution, referential integrity
- Contracts at ingestion vs assertions at transformation (Great Expectations / dbt tests / Soda — concepts over tools)
- Quarantine patterns: dead-letter tables, reject-and-continue vs fail-fast
- Alert fatigue: tiering checks into "page someone" vs "log it"

**Milestone:** for any pipeline you've built, list the 5 checks you'd add first, ranked by incident probability × blast radius.

---

## Block 6 — System Design Intro (Weeks 10–13, overlaps)

Start the **system-design** topic — fundamentals and pipeline-design-patterns first:

- Batch vs streaming decision framework (latency need, volume, cost)
- Idempotency, backfill strategy, and exactly-once vs effectively-once
- Storage layer choices: warehouse vs lake vs lakehouse vs OLTP offload
- Practice 3 classic prompts: clickstream analytics, CDC replication to a warehouse, daily metrics aggregation

**Milestone:** complete a 35-minute mock design of "ingest 50M events/day, dashboard must be fresh within 15 minutes" — covering ingestion, processing, storage, serving, and failure handling without prompting.

You do **not** need senior-level depth here. You need a structured approach and honest trade-off talk. Save mastery for the senior track.

---

## Block 7 — Second Cloud, Strategically (Weeks 14–15)

Don't relearn everything. Map your first cloud onto the second:

| Concept | AWS | Azure | GCP |
|---|---|---|---|
| Object storage | S3 | ADLS Gen2 | GCS |
| Warehouse | Redshift | Synapse / Fabric | BigQuery |
| Spark service | EMR / Glue | Databricks / Synapse Spark | Dataproc |
| Streaming | Kinesis / MSK | Event Hubs | Pub/Sub |
| Orchestration | MWAA / Step Functions | Data Factory | Composer |

Study the **aws-services**, **azure**, or **gcp** topic for your *second* cloud at translation depth: "I'd reach for Event Hubs where I used Kinesis, and here's what differs."

**Milestone:** for one pipeline you know well, describe how you'd rebuild it on the second cloud, naming services and one gotcha.

---

## Block 8 — Mock Loops (Week 16)

- One full mock loop: SQL/Python screen + PySpark deep dive + 35-min system design + behavioral
- Refresh **behavioral-questions → intermediate** and **project-walkthrough → intermediate** in this topic
- Tighten 3 project stories with metrics (rows/day, latency, cost) — see **project-walkthrough**

---

## Continuous Habits (whole 16 weeks)

- 2 SQL problems/week to keep the screen-sharp edge (window functions decay fast)
- 1 incident write-up/month from your real job — these become behavioral gold
- Skim **airflow** intermediate material: dynamic DAGs, pools, deferrable operators — mid-level loops love "how would you scale your orchestration?"

---

## Readiness Checklist

- [ ] Explain shuffle, skew, and broadcast joins with diagrams
- [ ] Design a Kafka topic/key scheme for an ordering-sensitive use case
- [ ] Defend incremental dbt models including late-data handling
- [ ] Name 5 data quality checks ranked by value for a real pipeline
- [ ] Complete a structured 35-minute pipeline design without prompts
- [ ] Translate your home cloud's services to a second cloud
- [ ] Tell 3 quantified project stories and 8 STAR stories

---

## Where to Go Next

- Interview imminent? Jump to **real-world.md** — the 2-week compressed plan
- Self-test: **scenarios.md**, Mid-Level checkpoint
- Targeting senior roles within 18 months: begin **senior-deep-dive.md** habits now, especially the incident-review and cost-awareness practices — they take calendar time, not study time
