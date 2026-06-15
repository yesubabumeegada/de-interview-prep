---
title: "System Design Interview - Intermediate"
topic: interview-prep
subtopic: system-design-interview
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [interview-prep, system-design-interview, system-design, intermediate]
---

# System Design Interview — Intermediate

## Talking Through Architecture Trade-offs

Mid-level and senior system design interviews hinge on your ability to compare architectural patterns and justify your choices. This section covers the most commonly tested trade-off conversations in data engineering interviews.

---

## Lambda vs Kappa Architecture

This is the most frequently tested architectural debate in DE system design.

### Lambda Architecture

Lambda splits processing into two layers that run in parallel:
- **Batch layer**: Reprocesses all historical data periodically, produces accurate views
- **Speed layer**: Processes recent data in real-time for low latency, accepts some inaccuracy
- **Serving layer**: Merges results from both layers for queries

**Pros:**
- Fault-tolerant: if the speed layer produces wrong results, the batch layer corrects them
- Handles both historical and real-time queries
- Mature tooling (Spark for batch, Flink/Kafka Streams for speed)

**Cons:**
- Maintaining two codebases (batch and streaming) is expensive and error-prone
- Business logic must be duplicated and kept in sync
- Operational complexity is high

**When to choose Lambda**: When data correctness for historical data is paramount and the team has capacity to maintain two pipelines. Common in financial reporting and compliance use cases.

### Kappa Architecture

Kappa uses a single streaming pipeline for everything. Historical reprocessing is done by replaying events through the same stream processor.

**Pros:**
- Single codebase for all processing logic
- Simpler to operate and reason about
- Modern stream processors (Flink, Kafka Streams) handle high throughput

**Cons:**
- Requires an event log with long retention (Kafka with 90-day retention is common)
- Large-scale reprocessing is slower than dedicated batch tools
- Not ideal for complex batch aggregations over years of data

**When to choose Kappa**: When the team wants operational simplicity and the event store can hold sufficient history. Common in modern real-time analytics platforms.

**Interview tip**: When asked "which would you choose?", answer with a question first — "What's the reprocessing frequency and how far back do we need to replay?" Let the answer shape your recommendation.

---

## Batch vs Streaming

The batch-vs-streaming decision comes down to latency requirements vs. operational complexity.

| Dimension | Batch | Streaming |
|---|---|---|
| Latency | Minutes to hours | Sub-second to seconds |
| Complexity | Lower | Higher |
| Fault tolerance | Simple retry/rerun | Exactly-once semantics needed |
| Cost | Lower (burst compute) | Higher (always-on) |
| Best for | Reporting, historical analysis | Fraud detection, real-time dashboards |

A nuanced answer acknowledges that **most production systems use both**: streaming for operational dashboards and alerting, batch for the historical data warehouse. Saying "I'd use streaming for everything" is almost always wrong.

---

## Consistency vs Availability (CAP in a DE Context)

The CAP theorem is most relevant when designing systems that span multiple nodes. In data engineering, this surfaces in questions about:

- **Data lake consistency**: S3 provides eventual consistency for list operations (though this improved with S3 strong consistency in 2020). Delta Lake and Iceberg add ACID transactions on top.
- **Distributed database choices**: Cassandra favors availability (AP); traditional RDBMS favors consistency (CP).
- **Streaming sinks**: Writing to multiple destinations can create inconsistency windows.

For most analytics workloads, eventual consistency is acceptable — a dashboard showing data from 5 minutes ago is fine. For operational data products (inventory counts, billing records), you may need stronger guarantees.

---

## Whiteboard Communication Tips

### Draw top-down, left-to-right

Data flows left to right in most diagrams. Start with data sources on the left, end with consumers on the right. This matches how interviewers expect to read a pipeline.

### Use layers, not spaghetti

Group components by function:
- Ingestion layer (Kafka, Kinesis, Firehose)
- Processing layer (Spark, Flink, dbt)
- Storage layer (S3, Delta Lake, Snowflake, BigQuery)
- Serving layer (Trino, Redshift, REST APIs)

### Label data flows with volume and format

Don't just draw arrows. Write "~1M events/hour, JSON" on the ingestion arrow and "~500GB/day, Parquet" on the storage arrow. Numbers make your design feel grounded.

### Use a legend

If you use colors or shapes, explain them. "Red boxes are managed cloud services, blue boxes are self-managed."

### Evolve the diagram, don't replace it

Start with a rough skeleton, then add detail. Don't erase and redraw — cross things out and annotate. The interviewer wants to see you think, not produce a perfect diagram.

---

## Handling Mid-Interview Constraint Changes

A common interview technique is to say "now assume the volume is 100x higher" or "what if the latency requirement drops to 1 second?" mid-design. This is intentional. The interviewer is testing adaptability.

**Good response pattern:**
1. Acknowledge the constraint change
2. Identify what in your current design breaks
3. Propose a targeted modification rather than a complete redesign

Example: "If volume goes 100x to 100 billion events per day, the single Kafka cluster becomes a bottleneck. I'd shard the topic by tenant ID across multiple clusters and use Kafka MirrorMaker to aggregate for cross-tenant analytics. The downstream Flink job would need to scale out to ~50 task managers based on our earlier throughput math."

---

## Estimating Storage and Compute

Being able to do rough estimates quickly is a signal of experience. Memorize a few useful reference points:

**Storage:**
- Raw JSON event (typical): ~1 KB
- Compressed Parquet conversion ratio: ~8x (1 KB JSON → ~125 bytes Parquet)
- S3 at 1 TB/day: ~$23/month storage (at $0.023/GB)

**Compute:**
- A single Spark executor (4 cores, 16 GB RAM) processes ~10 GB/minute of Parquet
- A Kafka partition handles ~10 MB/s throughput
- Flink parallelism of 1 handles ~50K events/second for simple transformations

**Example walkthrough:**
"1 billion events/day at 1 KB each = 1 TB/day raw. After Parquet compression, ~125 GB/day. At $0.023/GB on S3, storage cost is ~$2.90/day or ~$87/month just for new data. For a 2-year retention policy, peak storage would be ~91 TB at ~$2,100/month."

---

## Common DE System Design Interview Topics

These are the most frequently asked system design questions at major tech companies for DE roles:

1. **Design a CDC pipeline** from PostgreSQL to a data warehouse
2. **Design a metrics aggregation system** (think: Datadog-style metrics)
3. **Design a data lake** for a company ingesting 10 TB/day
4. **Design a feature store** for a real-time ML serving system
5. **Design a data quality monitoring system**
6. **Design a multi-tenant analytics platform**

Each of these follows the same 6-step framework, but each has unique deep-dive areas. The CDC question focuses on consistency and ordering guarantees. The feature store question focuses on online vs. offline consistency. Know the distinguishing concerns for each category.
