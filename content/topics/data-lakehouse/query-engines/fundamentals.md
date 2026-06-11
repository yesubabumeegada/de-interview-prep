---
title: "Query Engines (Trino, Spark, Flink) — Fundamentals"
topic: data-lakehouse
subtopic: query-engines
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [trino, spark, flink, query-engines, lakehouse]
---

# Query Engines — Fundamentals


## 🎯 Analogy

Think of query engines as different engines for the same road (your object storage): Trino is the sports car (low-latency interactive), Spark is the freight truck (large-scale batch), and Flink is the electric vehicle (continuous streaming).

---
## Why Multiple Query Engines?

In a lakehouse, different workloads have different requirements. No single query engine excels at all of them.

```
Spark:       Distributed batch processing, large-scale ETL, ML, Python DataFrames
Trino:       Interactive SQL analytics, low latency (seconds), federation across sources
Flink:       True real-time streaming, stateful computations, event-at-a-time processing
DuckDB:      In-process analytics, single machine, development/testing
Presto:      Trino's predecessor (Facebook-origin; Trino is the community fork)
```

---

## Spark — The Swiss Army Knife

```
What Spark does well:
  - Large-scale batch ETL (hundreds of TBs)
  - DataFrame transformations in Python/Scala/Java
  - ML pipelines (Spark ML, MLlib, integration with PyTorch/TensorFlow)
  - Streaming (Spark Structured Streaming — micro-batch, 1-5 min latency)
  - Native Delta Lake / Iceberg read-write

Spark architecture:
  Driver: orchestrates execution, creates DAG plan
  Executors: worker nodes that process data partitions
  Shuffle: data redistribution for JOINs and GROUP BY (most expensive operation)

Spark SQL (ANSI SQL on DataFrames):
  spark.sql("SELECT * FROM delta.`s3://bucket/orders` WHERE order_date = '2024-01-15'")
  
When NOT to use Spark:
  - Interactive queries requiring < 1 second response (Spark has startup overhead)
  - True event-at-a-time processing (Flink is better)
  - Small datasets on a single machine (DuckDB is faster)
```

---

## Trino — Interactive SQL at Scale

```
What Trino does well:
  - Ad-hoc SQL analytics with low latency (seconds, not minutes)
  - Query federation: JOIN a Postgres table with an S3 Parquet table in one SQL
  - Scales to PBs of data via distributed execution
  - No data movement: queries data in place (S3, HDFS, databases)

Trino architecture:
  Coordinator: receives queries, creates execution plan, schedules splits
  Workers: execute plan stages, read data from connectors
  Connectors: translate between Trino operators and data sources
    - Iceberg connector: reads Iceberg manifests, fetches Parquet files
    - Hive connector: reads Hive Metastore, fetches HDFS/S3 files
    - TPCH, TPCDS, PostgreSQL, MySQL connectors

Use cases:
  BI dashboards: Tableau/Looker → Trino → Iceberg on S3
  Ad-hoc exploration: analyst runs SELECT in Trino UI (2 sec vs Spark 30 sec)
  Data federation: SELECT * FROM s3_orders JOIN pg.customers ON order_id

When NOT to use Trino:
  - Write-heavy workloads (Trino is read-optimized)
  - Complex ETL transformations (Spark has richer DataFrame API)
  - Streaming (Trino is batch/interactive only)
```

---

## Flink — True Streaming

```
What Flink does well:
  - Event-at-a-time stream processing (< 100ms latency)
  - Stateful computations (exactly-once, fault-tolerant state)
  - Complex event processing (windowing, watermarks, joins on streams)
  - SQL on streams (Flink SQL: CREATE TABLE → SELECT → INSERT)

Flink architecture:
  JobManager: orchestrates job, manages checkpoints, handles failures
  TaskManagers: execute operator chains in task slots
  State backend: stores operator state (RocksDB for large state, heap for small)

Flink SQL example:
  CREATE TABLE kafka_orders (
    order_id BIGINT, amount DOUBLE, event_ts TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
  ) WITH ('connector' = 'kafka', ...);
  
  INSERT INTO iceberg_orders
  SELECT TUMBLE_START(event_ts, INTERVAL '1' MINUTE) AS window_start,
         SUM(amount) AS revenue
  FROM kafka_orders
  GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE);

When NOT to use Flink:
  - Ad-hoc SQL analytics (Trino/Spark SQL are better UX)
  - Simple batch ETL (Spark is more mature with better tooling)
  - Small teams without streaming expertise (operationally complex)
```

---

## Query Engine Decision Matrix

| Workload | Best Engine | Why |
|---|---|---|
| Daily batch ETL (100GB+) | Spark | Mature, integrates with Delta/Iceberg, Python-first |
| Interactive SQL (analysts) | Trino | Low latency, federation, scales on demand |
| Real-time streaming (<100ms) | Flink | True event-at-a-time, stateful |
| Near-real-time (1-5 min) | Spark Streaming | Easier to operate than Flink |
| Local development/testing | DuckDB | In-process, no cluster needed |
| Spark + SQL mixed | Databricks SQL | Managed Spark with SQL warehouse |

---


## ▶️ Try It Yourself

```sql
-- Trino: query Iceberg on S3 interactively (sub-second for small results)
-- Connect: trino --server localhost:8080 --catalog iceberg

SELECT region, SUM(amount) AS revenue
FROM iceberg.silver.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY region
ORDER BY revenue DESC;

-- Trino time travel (Iceberg)
SELECT * FROM iceberg.silver.orders FOR TIMESTAMP AS OF TIMESTAMP '2024-01-14 00:00:00';

-- DuckDB: local lakehouse queries (amazing for dev/testing)
-- pip install duckdb
-- import duckdb
-- duckdb.sql("SELECT region, SUM(amount) FROM read_parquet('data/orders/**/*.parquet') GROUP BY 1").show()
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "Why can't Spark replace Trino for interactive analytics?" — Spark has startup overhead (cluster initialization: 30 seconds to 2 minutes). For interactive analytics, Trino coordinators are always-on with worker pools pre-warmed. A 5-second Trino query would take 45 seconds on Spark (30 sec startup + 15 sec execution). Databricks SQL Serverless reduces this with pre-warmed clusters, but Trino remains the benchmark for interactive latency in open-source setups.

> **Tip 2:** "Why can't Flink replace Spark Streaming for most teams?" — Operational complexity. Flink requires understanding of checkpoints, state backends (RocksDB vs heap), watermarks, and job manager configuration. A Flink production incident is harder to debug. Spark Structured Streaming is easier to operate (familiar PySpark API, Databricks monitoring, Delta integration). Most teams with latency requirements of 1-5 minutes use Spark Streaming. Flink is the right choice when you need < 1 second latency or very large state.

> **Tip 3:** "Can Trino and Spark read from the same Iceberg table simultaneously?" — Yes. Both read from S3 via Iceberg metadata. Trino uses the Iceberg connector (reads manifest files, fetches Parquet). Spark uses the Iceberg Spark extensions (same manifest reading). Since Iceberg provides snapshot isolation, a Trino query in progress sees a consistent snapshot even if Spark commits a new version. This multi-engine read is one of Iceberg's key design goals.
