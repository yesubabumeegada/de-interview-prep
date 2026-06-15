---
title: "System Design Interview - Senior Deep Dive"
topic: interview-prep
subtopic: system-design-interview
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [interview-prep, system-design-interview, system-design, senior, advanced]
---

# System Design Interview — Senior Deep Dive

## What Senior Interviews Actually Test

At the senior level, the interviewer is no longer checking whether you know what Kafka is. They assume you do. What they're testing is:

- **Judgment**: Can you recognize when a simple solution beats a sophisticated one?
- **Failure mode awareness**: Do you proactively address what can go wrong?
- **Cross-functional thinking**: Do you consider operational burden, cost, and team skill alongside technical correctness?
- **Depth under pressure**: When pushed, can you go deeper on any component in your design?

Senior candidates should be able to take any box in their diagram and spend 10 minutes going deeper on it without running out of things to say.

---

## Walkthrough 1: Design a CDC Pipeline

**Prompt**: "Design a change data capture pipeline from a PostgreSQL operational database to a Snowflake data warehouse. The source has ~50 tables, ~5 million rows updated daily, and the analytics team needs data available within 5 minutes of a change."

### Clarifying questions to ask:
- Are deletes meaningful for analytics, or only inserts and updates?
- Is the PostgreSQL instance under our control (can we enable logical replication)?
- What is the Snowflake tier — are we constrained on credits?
- Is exactly-once delivery required, or is at-least-once with deduplication acceptable?

### Architecture:

```
PostgreSQL (logical replication enabled)
    ↓ WAL stream
Debezium (Kafka Connect source connector)
    ↓ per-table Kafka topics
Kafka (5+ partition topics, 7-day retention)
    ↓
Kafka Connect Snowflake Sink (or custom Flink job)
    ↓
Snowflake raw schema (insert-only landing tables)
    ↓ dbt or Snowflake Tasks
Snowflake curated schema (SCD Type 2 or merge-based)
```

### Deep-dive points:

**WAL Slot Management**: Debezium uses PostgreSQL logical replication slots. If Debezium falls behind, the replication slot prevents WAL cleanup and the source disk fills up. Mitigation: set `pg_wal_keep_size`, alert on slot lag > N MB, and have a runbook for slot recreation.

**Schema evolution**: When a column is added to the source, Debezium emits an event with the new schema. The Kafka topic schema (if using Schema Registry) must be evolved. Downstream Snowflake tables must be altered. This needs automated DDL detection and a schema change pipeline.

**Exactly-once vs at-least-once**: Kafka-to-Snowflake is typically at-least-once. Handle duplicates downstream with a deduplication key (usually the primary key + `ts_ms` from the Debezium envelope). Use `MERGE INTO` or `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) = 1` in dbt.

**5-minute SLA**: With Debezium → Kafka → Snowflake sink, typical end-to-end latency is 30–90 seconds. The 5-minute SLA is achievable. Set up lag monitoring on the Kafka consumer group for the Snowflake sink. Alert if consumer lag exceeds the equivalent of 3 minutes of events.

---

## Walkthrough 2: Design a Metrics Aggregation System

**Prompt**: "Design a system to collect application metrics (counters, gauges, histograms) from 10,000 microservices, aggregate them in near-real-time, and serve them to a monitoring dashboard. Target: metrics visible within 30 seconds of emission."

### Architecture:

```
Microservices (StatsD/Prometheus clients)
    ↓ UDP pushes or HTTP scrapes
Metrics Collection Layer (Telegraf agents on each host, or Prometheus federation)
    ↓ pre-aggregated metrics
Kafka (one topic per metric category, compressed)
    ↓
Flink (tumbling window aggregation: sum, p50, p95, p99)
    ↓ per-minute aggregates
Apache Druid or ClickHouse (columnar, time-series optimized)
    ↓
Grafana (dashboard queries over HTTP)
```

### Deep-dive points:

**Push vs Pull**: Prometheus uses pull (scraping). At 10,000 services, a single Prometheus becomes a bottleneck. Solutions: Prometheus federation (hierarchical scraping), remote write to Kafka, or switching to a push model with StatsD/Telegraf.

**Cardinality problem**: High-cardinality labels (e.g., `user_id` as a metric label) explode storage and query cost. At 10,000 services × 100 metrics × 50 label combinations = 50 million unique time series. Set cardinality limits and reject high-cardinality labels at ingestion.

**Window aggregation in Flink**:
```java
DataStream<MetricEvent> metrics = ...;
metrics
    .keyBy(MetricEvent::getMetricName)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new PercentileAggregator())
    .addSink(druidSink);
```
Use event time windowing with a watermark of 30 seconds to handle late-arriving metrics from lagging agents.

**Druid vs ClickHouse**: Druid was built for exactly this use case — sub-second queries over time-series rollups. ClickHouse is simpler to operate and nearly as fast for this workload. Choose ClickHouse for smaller teams; Druid when you need automatic rollup and tiered storage.

---

## Walkthrough 3: Design a Data Lake for 10 TB/Day

**Prompt**: "Design a data lake for a company ingesting 10 TB/day from 200 source systems. Data must be queryable within 4 hours. The company has both data scientists (Python notebooks) and analysts (SQL)."

### Architecture:

```
200 Source Systems
    ↓ various connectors (Airbyte, custom extractors, Firehose)
Landing Zone (S3, raw format, partitioned by source/date)
    ↓ event-driven trigger (S3 event → Lambda → Glue/Spark)
Bronze Layer (Parquet, deduped, schema-enforced, Delta Lake)
    ↓ dbt or Spark jobs
Silver Layer (cleaned, joined, business rules applied)
    ↓ aggregation jobs
Gold Layer (mart-level aggregates, BI-ready)
    ↓
Query Engine (Athena for ad-hoc, Redshift Spectrum for heavy SQL)
ML Platform (SageMaker / notebooks, read from Silver directly)
```

### Deep-dive points:

**Partitioning strategy at 10 TB/day**: Partition by `year/month/day/hour` at the Bronze layer. At 10 TB/day, daily partitions are ~10 TB — too large to scan for most queries. Add a secondary partition on source system or region where query patterns support it. Use `MSCK REPAIR TABLE` or Glue crawlers to register new partitions automatically.

**File size management**: Spark jobs often produce small files (one per partition per task). A 10 TB daily load over 200 sources at 128 MB target file size = ~80,000 files/day. Use a compaction job (Spark with `coalesce`) to merge small files. Delta Lake's `OPTIMIZE` command handles this automatically.

**Table format choice**: At this scale, use Delta Lake or Apache Iceberg over raw Parquet + Hive metastore. Key reasons: ACID transactions for concurrent writes, time travel for debugging, schema evolution without table recreation, and efficient file skipping.

**Governance and lineage**: 200 source systems means data quality varies. Add: schema validation at ingestion (Great Expectations or Soda), PII detection before landing in Silver, and lineage tracking (OpenLineage/Marquez) to trace which Silver tables were built from which Bronze sources.

**Cost optimization**: At 10 TB/day with 7-year retention, total storage = ~25 PB. At $0.023/GB, that's ~$575K/month. Use S3 Intelligent-Tiering for data older than 30 days, and Glacier Deep Archive for data older than 1 year to reduce costs by ~80%.

---

## Failure Mode Awareness

Senior candidates proactively address what can go wrong. For every major component you design, be ready to answer: "What happens when this fails?"

| Component | Failure | Mitigation |
|---|---|---|
| Kafka broker | Broker goes down | 3-replica topics, `acks=all`, producer retry |
| Spark job | OOM on executor | Partition data to target 128MB tasks, tune `spark.executor.memory` |
| Debezium | Replication slot lag | WAL size alerts, slot recreation runbook |
| S3 | Eventual consistency window | Use Delta Lake or Iceberg for atomic commits |
| Snowflake | Warehouse suspension during load | Use dedicated loading warehouse, monitor credit burn |

Volunteering failure modes without being asked is one of the clearest signals of senior-level thinking.
