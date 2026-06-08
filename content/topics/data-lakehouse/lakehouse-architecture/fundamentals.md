---
title: "Lakehouse Architecture — Fundamentals"
topic: data-lakehouse
subtopic: lakehouse-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [lakehouse, architecture, delta-lake, data-lake, data-warehouse]
---

# Lakehouse Architecture — Fundamentals

## What Is a Data Lakehouse?

A data lakehouse combines the low-cost, flexible storage of a data lake with the ACID transactions, schema enforcement, and query performance of a data warehouse — in a single architecture.

```
Traditional path (two-tier):
  Sources → Data Lake (S3, raw files) → Data Warehouse (Snowflake, structured)
  Problem: two copies of data, two ETL pipelines, two cost centers, staleness

Lakehouse path (unified):
  Sources → Lakehouse (S3 + open table format: Delta/Iceberg/Hudi)
  Same data serves: BI queries, ML training, streaming reads, ad-hoc analysis
```

---

## Data Lake vs Data Warehouse vs Lakehouse

| Property | Data Lake | Data Warehouse | Lakehouse |
|---|---|---|---|
| Storage cost | Low (S3/GCS) | High (proprietary) | Low (S3/GCS) |
| Schema enforcement | Optional (schema-on-read) | Strict (schema-on-write) | Both (enforced on write, flexible to evolve) |
| ACID transactions | No | Yes | Yes |
| Query performance | Slow (raw files) | Fast (optimized storage) | Fast (with optimization) |
| Streaming | Difficult | Limited | Native |
| ML/DS workloads | Yes | Poor (no DataFrames) | Yes |
| Data types | All (unstructured OK) | Structured only | Structured + semi-structured |
| Open format | Yes (CSV, Parquet) | No (proprietary) | Yes (Parquet + transaction log) |

---

## Lakehouse Architecture Layers

```
┌────────────────────────────────────────────────────────┐
│                   Serving Layer                        │
│   BI (Tableau/Looker) │ SQL (Trino/Athena) │ ML/DS     │
├────────────────────────────────────────────────────────┤
│              Semantic / Catalog Layer                  │
│   Unity Catalog │ Glue │ Hive Metastore │ Nessie       │
├────────────────────────────────────────────────────────┤
│               Table Format Layer                       │
│       Delta Lake │ Apache Iceberg │ Apache Hudi         │
├────────────────────────────────────────────────────────┤
│               Storage Layer                            │
│       S3 │ GCS │ ADLS Gen2 │ MinIO (on-prem)           │
└────────────────────────────────────────────────────────┘
```

---

## Medallion Architecture

The standard organizational pattern for Lakehouse data:

```
Bronze (Raw):
  - Exact copy of source data, no transformations
  - Append-only, never deleted
  - Format: JSON, CSV, Avro as-received; or Parquet for efficiency
  - Purpose: replay source, debug, audit
  - Retention: long (1–7 years)

Silver (Cleansed):
  - Validated, deduplicated, typed, joined to reference data
  - Schema enforced, nulls handled, PII masked
  - Format: Delta/Iceberg Parquet
  - Purpose: analytics-ready, single source of truth

Gold (Aggregated):
  - Business-specific aggregations and metrics
  - Pre-joined for performance (denormalized)
  - Format: Delta/Iceberg, optimized with Z-order/clustering
  - Purpose: BI dashboards, ML features, executive KPIs
```

---

## Why Lakehouse Over Traditional Two-Tier?

```
Problems with Data Lake + Data Warehouse:
  1. Data duplication: raw data in S3 AND in Snowflake → double storage cost
  2. Pipeline complexity: two separate ETL pipelines to maintain
  3. Staleness: warehouse copy lags lake by hours
  4. ML/BI gap: ML uses lake (DataFrames), BI uses warehouse (SQL) → different data
  5. Schema drift: lake schema changes don't automatically propagate to warehouse

How Lakehouse solves these:
  1. Single storage (S3) → one cost, no duplication
  2. Single pipeline: write to lakehouse → serves ALL consumers
  3. Streaming writes → BI sees data in seconds/minutes
  4. Open format: Spark DataFrames AND SQL both read same Parquet files
  5. Schema enforcement at write time → upstream drift caught immediately
```

---

## Interview Tips

> **Tip 1:** "What problem does the lakehouse solve?" — The classic answer: it eliminates the "two-tier tax" — storing and pipeling data twice. A lakehouse provides ACID transactions and schema enforcement directly on open-format files in object storage, so you no longer need to copy data into a proprietary warehouse to get reliability.

> **Tip 2:** "Isn't a lakehouse just a data lake with Delta Lake on top?" — Partially correct. Delta/Iceberg/Hudi are table formats that enable ACID on object storage. But a full lakehouse also includes: a catalog (Unity Catalog, Glue), a query engine (Spark, Trino, Flink), and an organizational pattern (medallion). The table format is the foundation, but not the complete architecture.

> **Tip 3:** "When would you NOT use a lakehouse?" — When your team is small and already on Snowflake/BigQuery with no ML workloads. The lakehouse adds operational complexity (catalog, compute management, file optimization). For pure BI on structured data, a managed warehouse is simpler. Lakehouse shines when: you have both BI and ML workloads, you need to keep raw data, or you're storing multi-petabyte scale.
