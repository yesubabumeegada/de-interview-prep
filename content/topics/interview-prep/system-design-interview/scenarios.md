---
title: "System Design Interview - Scenario Questions"
topic: interview-prep
subtopic: system-design-interview
content_type: scenario_question
tags: [interview-prep, system-design-interview, system-design, scenarios]
---

# System Design Interview — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Design a Daily Sales Report Pipeline

**Scenario:** A retail company has transactional data in a PostgreSQL database. The business team needs a daily sales summary report (total revenue, units sold, top 10 products) available by 7:00 AM each morning. The database has ~500,000 orders per day. You have access to any cloud tools. Walk through how you would design this pipeline.

<details>
<summary>✅ Solution</summary>

**Clarifying questions to ask first:**
- What time zone? (affects scheduling)
- Do we need historical backfill or just going-forward?
- Who consumes the report — a dashboard, an email, or an API?
- Is the PostgreSQL instance in AWS, GCP, or on-prem?

**Architecture:**

```
PostgreSQL (source)
    ↓ nightly extract (Airbyte or pg_dump-based)
S3 raw landing (CSV or Parquet, partitioned by date)
    ↓ Glue/Spark or dbt (transformation)
S3 curated or Redshift (daily_sales_summary table)
    ↓
Tableau / Metabase (dashboard) or SES email (PDF report)
```

**Step-by-step design:**

1. **Extraction**: Use Airbyte with a Postgres source connector. Schedule a full-table or incremental (by `created_at`) extract at 4:00 AM. At 500K orders/day, incremental is better to avoid loading 365 days of history nightly.

2. **Storage**: Land raw data to S3 as Parquet, partitioned by `order_date`. File size: 500K orders × ~500 bytes = ~250 MB/day, easily fitting in a few Parquet files.

3. **Transformation**: A dbt job (scheduled via Airflow or dbt Cloud at 4:30 AM) runs three models:
   - `stg_orders` — clean and type-cast the raw data
   - `daily_sales_summary` — aggregate by date
   - `top_products_daily` — rank products by revenue

4. **Serving**: Write results to Redshift or keep in S3 queryable via Athena. Connect Tableau/Metabase to the summary tables.

5. **Scheduling**: Airflow DAG with three tasks: extract → transform → notify. Add email alert on failure. 4:00 AM start gives ample time for the 7:00 AM SLA.

**Trade-offs to mention:**
- Full extract vs. incremental: full is simpler but slower; incremental is efficient but requires a reliable `updated_at` column
- dbt vs. Spark: dbt is simpler for SQL-based transformations at this scale; Spark would be overkill
- Redshift vs. Athena: Athena is cheaper for low-query-frequency reporting; Redshift is better if analysts run ad-hoc queries throughout the day

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Design a CDC Pipeline with 5-Minute Freshness

**Scenario:** An e-commerce company's data team needs product inventory levels and order status changes reflected in the analytics warehouse within 5 minutes. The source is a MySQL database with ~100 tables, receiving ~10,000 updates per minute during peak hours. The downstream system is BigQuery. Design the end-to-end pipeline, addressing schema evolution, ordering guarantees, and failure recovery.

<details>
<summary>✅ Solution</summary>

**Clarifying questions:**
- Is logical replication or binlog access available on MySQL?
- Are deletes business-meaningful (e.g., soft delete vs. hard delete)?
- Is the 5-minute SLA for all 100 tables or just critical ones?
- Is exactly-once required or is at-least-once with idempotent writes acceptable?

**Architecture:**

```
MySQL (binlog enabled, ROW format)
    ↓
Debezium (Kafka Connect Source, via debezium/connector-mysql)
    ↓ per-table Kafka topics (e.g., ecommerce.inventory, ecommerce.orders)
Kafka (3-broker cluster, replication factor 3, 7-day retention)
    ↓
Dataflow / Flink (streaming consumer)
    ↓ MERGE writes (upsert by primary key)
BigQuery (raw_cdc dataset, one table per source table)
    ↓ scheduled dbt jobs (every 15 min)
BigQuery analytics dataset
```

**Handling the key concerns:**

**Schema evolution**: Debezium uses Kafka Schema Registry (Confluent). When MySQL ALTER TABLE runs, Debezium captures the DDL event and updates the schema in the registry. Downstream consumers using the Avro deserializer receive the new schema automatically. For BigQuery, use the BigQuery Storage Write API with schema auto-detection, or pre-apply DDL based on Debezium DDL events.

**Ordering guarantees**: Within a Kafka partition, ordering is guaranteed. Key all events by primary key — this ensures all events for a given row land in the same partition and are processed in order. Cross-row ordering is not guaranteed and generally not needed for analytics.

**Failure recovery**: 
- Debezium stores its offset (binlog position) in Kafka. If Debezium restarts, it resumes from the last committed offset — no events are lost.
- MySQL binlog rotation can delete events before Debezium reads them if Debezium is down for too long. Set `binlog_expire_logs_seconds` ≥ 7 days and alert if Debezium lag exceeds 1 hour.
- For the BigQuery sink, use `MERGE INTO` keyed on primary key to handle at-least-once delivery without duplicates.

**5-minute SLA monitoring**: Set up a Kafka consumer group lag alert. If the BigQuery sink consumer falls > 30 seconds of events behind, trigger a PagerDuty alert. With Debezium's low latency (~100ms capture) and BigQuery Storage Write API (~30 seconds), the realistic end-to-end latency is 1–2 minutes, well within the SLA.

**Trade-offs:**
- Debezium on Kafka vs. AWS DMS: DMS is simpler to operate but has less flexibility for complex transformations and schema evolution handling. For 100 tables with active schema evolution, Debezium gives more control.
- Full table capture vs. CDC: CDC is necessary for 5-minute freshness. Full nightly extracts cannot meet this SLA.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Real-Time Metrics Aggregation System at Scale

**Scenario:** You're designing a metrics platform for a company with 50,000 microservices emitting ~2 million metric data points per second at peak. Each metric has dimensions (labels) that allow filtering (e.g., by service, region, endpoint). The dashboard must show aggregated metrics (sum, avg, p95, p99) updated within 30 seconds. The system must handle 10x traffic spikes (Black Friday scenario) without data loss. Storage must be cost-efficient for 13 months of retention. Design the complete system.

<details>
<summary>✅ Solution</summary>

**Clarifying questions:**
- Push (StatsD) or pull (Prometheus scrape) model at the source?
- What cardinality limit per metric? (# unique label combinations)
- Is backfill required if a node goes down?
- Is the 30-second SLA for pre-aggregated queries or ad-hoc queries?
- Multi-region or single region?

**Scale estimates:**
- 2M data points/second × 8 bytes/point ≈ 16 MB/s raw
- 86,400 seconds/day × 16 MB/s ≈ 1.35 TB/day raw
- After rollup aggregation (1-minute buckets): ~99% reduction → ~13 GB/day stored
- 13 months × 13 GB/day ≈ 5 TB total

**Architecture:**

```
50,000 Microservices
    ↓ StatsD UDP (low overhead, fire-and-forget per service)
Collection Tier: Telegraf agents (one per host, 100 hosts)
    ↓ pre-aggregate to 10-second intervals per host
    ↓ push to Kafka
Kafka (50 partitions, keyed by metric_name+label_hash, 24hr retention)
    ↓
Flink Cluster (20 task managers, 4 slots each)
    ↓ tumbling 1-minute windows + watermark 30s
    ↓ aggregate: sum, count, min, max, sketch (DDSketch for p95/p99)
    ↓
ClickHouse (2-shard, 3-replica, columnar, time-series optimized)
    ↓ TTL: raw 1-min data → 5-min rollup after 7 days → 1-hour rollup after 30 days
    ↓
Grafana (query ClickHouse via HTTP API)
```

**Key design decisions:**

**Collection tier pre-aggregation**: Don't send raw 2M/second to Kafka. Telegraf agents aggregate to 10-second intervals per metric per label set. This reduces Kafka throughput by ~10x with minimal accuracy loss. Acceptable because 10-second pre-aggregation still achieves the 30-second dashboard SLA.

**DDSketch for percentiles**: You cannot compute exact percentiles from pre-aggregated data without storing all raw values. Use a mergeable sketch algorithm (DDSketch or t-digest). Flink's 1-minute windows merge DDSketch objects from multiple Telegraf agents, producing accurate p95/p99 estimates within ±1%.

**Cardinality control**: High-cardinality labels (user_id, request_id) would explode the number of unique time series. Implement a cardinality guard at the Flink layer: reject any metric where the distinct label combination count exceeds 10,000. Return an error to the emitter and log the rejection. Monitor and alert on cardinality violations.

**10x spike handling (Black Friday)**:
- Kafka absorbs the spike; 50 partitions at 10 MB/s each = 500 MB/s capacity, well above 10x of 16 MB/s
- Flink auto-scales via YARN or Kubernetes; Flink's reactive mode scales task managers based on Kafka consumer lag
- ClickHouse write throughput scales with shards; at 10x load, add a third shard pre-provisioned (hot standby)
- Telegraf: UDP is fire-and-forget — during spikes, some metrics may be dropped at the source. This is acceptable for a metrics system (dashboards show approximate load, not exact counts). For critical business metrics, switch those to TCP with buffering.

**Cost optimization with TTL rollups**: ClickHouse TTL policies roll up data automatically:
```sql
TTL toStartOfInterval(timestamp, INTERVAL 5 MINUTE) + INTERVAL 7 DAY,
    toStartOfHour(timestamp) + INTERVAL 30 DAY
```
This keeps 5 TB total with most of it in compressed 1-hour rollups, reducing storage cost by ~95% vs. retaining raw 1-minute data for 13 months.

**Trade-offs to articulate:**
- **ClickHouse vs. Druid**: Druid has more mature rollup features but is significantly more complex to operate (requires Zookeeper, Coordinator, Broker, Historical nodes). ClickHouse is simpler, nearly as fast, and handles this scale with fewer moving parts.
- **Flink vs. Kafka Streams**: Flink has better exactly-once semantics and more mature windowing operators. Kafka Streams is simpler but weaker for complex aggregations like sketch merging.
- **Exact percentiles vs. sketches**: Exact computation requires storing all raw values — at 2M/s, that's 172 billion data points/day. Not feasible. Sketches give 99%+ accuracy at 0.1% of the storage cost.

**What I'd add with more time:**
- Anomaly detection layer (statistical control charts) running on 5-minute aggregates
- Metric metadata store (what does this metric mean, who owns it)
- Quota system to prevent one team from monopolizing cardinality budget

</details>

</article>
