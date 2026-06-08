---
title: "Pipeline Design Patterns — Fundamentals"
topic: system-design
subtopic: pipeline-design-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, pipeline, etl, batch, streaming, idempotency]
---

# Pipeline Design Patterns — Fundamentals

## What Is a Data Pipeline?

A data pipeline is a sequence of processing steps that move, transform, and load data from sources to destinations. Every pipeline answers three questions:
- **What data?** — source systems, schemas, volumes
- **How often?** — batch (hourly/daily), micro-batch (minutes), streaming (real-time)
- **To where?** — data warehouse, data lake, operational store, downstream service

---

## Core Pipeline Models

| Model | Trigger | Latency | Use Case |
|---|---|---|---|
| **Batch** | Schedule (cron/Airflow) | Minutes–hours | Daily reports, large transforms |
| **Micro-batch** | Time window (Spark Streaming) | Seconds–minutes | Near-real-time dashboards |
| **Streaming** | Event-driven (Kafka consumer) | Milliseconds–seconds | Fraud detection, live metrics |
| **Incremental** | Watermark / CDC | Low (only new rows) | Reducing batch load on large tables |

---

## ETL vs ELT

```
ETL (Extract → Transform → Load):
  Source → [transform in pipeline] → Data Warehouse
  When: warehouse is compute-limited (legacy); transformations are complex pre-load cleansing

ELT (Extract → Load → Transform):
  Source → Data Warehouse (raw) → [transform inside warehouse using SQL]
  When: cloud DW (Snowflake, BigQuery, Redshift) — leverage warehouse compute
  Tool: dbt runs the T inside the warehouse
```

**Modern default:** ELT. Raw data lands in a staging/raw layer first, transformations happen as SQL/dbt models inside the warehouse.

---

## Idempotency — The Most Important Pipeline Property

An idempotent pipeline can run multiple times for the same input and always produce the same output with no side effects.

```sql
-- Non-idempotent (bad): inserts duplicates on re-run
INSERT INTO orders_fact SELECT * FROM orders_staging;

-- Idempotent pattern 1: DELETE + INSERT by partition/date
DELETE FROM orders_fact WHERE order_date = '2024-01-15';
INSERT INTO orders_fact SELECT * FROM orders_staging WHERE order_date = '2024-01-15';

-- Idempotent pattern 2: MERGE (upsert)
MERGE INTO orders_fact t
USING orders_staging s ON (t.order_id = s.order_id)
WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.status = s.status
WHEN NOT MATCHED THEN INSERT (order_id, amount, status, order_date)
                       VALUES (s.order_id, s.amount, s.status, s.order_date);

-- Idempotent pattern 3: partition overwrite (Spark)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("order_date").parquet("s3://bucket/orders/")
-- Only overwrites the partitions present in df, not the whole table
```

---

## Pipeline Stages: Bronze → Silver → Gold (Medallion Architecture)

```
Raw / Bronze Layer:
  - Exact copy of source data, no transformation
  - Append-only, immutable
  - All columns preserved, even bad/null data
  - Format: Parquet, Delta, or Iceberg

Cleaned / Silver Layer:
  - Validated, deduplicated, typed correctly
  - Null handling, schema enforcement
  - Still granular (row-level)

Aggregated / Gold Layer:
  - Business-ready aggregates and joins
  - Pre-built for specific use cases (BI dashboards, ML features)
  - Optimized for query performance (partitioned, clustered)
```

---

## Common Pipeline Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| No idempotency | Duplicate rows on retry | MERGE or partition overwrite |
| No data quality checks | Garbage propagates silently | Assert row counts, null rates per stage |
| Tight coupling | One failure cascades everywhere | Use queues/events between stages |
| Processing whole table each run | Slow as data grows | Incremental / watermark-based loads |
| Hardcoded dates | Pipeline breaks on re-run | Parameterize execution date |
| No schema evolution handling | Pipeline breaks on source schema change | Schema-on-read, schema registry |

---

## Interview Tips

> **Tip 1:** "How do you make a pipeline idempotent?" — Use MERGE/upsert instead of INSERT. For partitioned tables, use partition overwrite (only rewrite the partitions being processed). For append-only streaming, use deduplication with a unique event ID. Test by running the pipeline twice for the same date and verifying the row count doesn't change.

> **Tip 2:** "ETL or ELT — which would you choose?" — ELT for modern cloud data warehouses (Snowflake, BigQuery, Redshift). The warehouse scales compute independently so running transformations inside it is cheaper and simpler. ETL when: you need to apply complex transformations before loading (PII masking, proprietary logic), or the destination can't store raw data.

> **Tip 3:** "Explain medallion architecture." — Bronze = raw, exact copy from source. Silver = cleaned, validated, typed. Gold = aggregated, business-ready. Each layer adds value progressively. Any layer can be rebuilt by reprocessing the layer below — this is the key benefit: full replayability without re-extracting from source.
