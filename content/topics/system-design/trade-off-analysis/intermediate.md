---
title: "Trade-off Analysis — Intermediate"
topic: system-design
subtopic: trade-off-analysis
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, trade-offs, storage-formats, orchestration, caching]
---

# Trade-off Analysis — Intermediate

## Storage Format Trade-offs

| Format | Best For | Avoid When | Key Properties |
|---|---|---|---|
| **CSV** | One-time exports, human readability | Analytics, large data | No compression, no schema, slow |
| **JSON** | Semi-structured, nested data, APIs | Columnar analytics | Schema flexible, large, slow for analytics |
| **Parquet** | Columnar analytics, S3 data lake | Many small files | Columnar, compressed, fast for analytics |
| **Avro** | Kafka, row-level streaming | Heavy analytics | Row-based, schema evolution, compact |
| **Delta Lake** | Lakehouse: ACID + analytics | Simple S3 only needs | Parquet + ACID, time-travel, CDC |
| **Iceberg** | Multi-engine (Spark + Trino + Flink) | Delta-only Databricks stack | Open standard, hidden partitioning |
| **ORC** | Hive/legacy Hadoop workloads | New projects | Columnar, older Hive optimization |

### Parquet vs Avro Decision
```
Use Parquet when:
  - Analytics workload (aggregations, column projections)
  - Data lake storage (S3, GCS, ADLS)
  - Reading many columns from many rows → columnar wins
  
Use Avro when:
  - Streaming (Kafka messages)
  - Row-level operations (read full rows one at a time)
  - Schema evolution with backward/forward compatibility (Schema Registry)
  - Record-by-record processing pipelines

Rule of thumb:
  Writing to Kafka → Avro (with Schema Registry)
  Writing to S3/data lake → Parquet (with Snappy or Zstd)
  Intermediate stream state → Avro
  Final analytical tables → Parquet/Delta/Iceberg
```

---

## Orchestration Trade-offs

| Tool | Strengths | Weaknesses | Best For |
|---|---|---|---|
| **Airflow** | Most popular, huge ecosystem, UI, plugins | Complex setup, scheduler bottleneck at scale | General DE pipelines, complex dependencies |
| **Prefect** | Python-first, simpler code, hybrid cloud | Smaller ecosystem | Python teams wanting simpler code |
| **Dagster** | Asset-based model, data-aware, testing | Steeper learning curve | Data-asset centric teams |
| **dbt (as orchestrator)** | Built-in for dbt models | Only for dbt, no general tasks | dbt-first teams |
| **Step Functions** | AWS-native, serverless, reliable | Verbose JSON, AWS-only, expensive per transition | AWS shops, event-driven pipelines |
| **Databricks Workflows** | Tight Spark/Delta integration, auto-scaling | Databricks vendor lock-in | Databricks-heavy platforms |

```
Choice framework:
  If stack is Databricks-heavy → Databricks Workflows (zero ops, native integration)
  If AWS-native + serverless → Step Functions (no server to manage)
  If team knows Python, wants simplicity → Prefect or Dagster
  If enterprise, complex DAGs, existing Airflow → Airflow (with managed MWAA/Composer)
  If 80% of pipeline is dbt → Dagster with dbt integration (asset-based lineage)
```

---

## Caching Trade-offs

```
Why caching in DE?
  1. Speed up repeated queries (dashboard cache)
  2. Reduce load on source system (read replica / materialized view)
  3. Enable real-time lookup (feature store, driver location)

Caching options and trade-offs:

Redis (in-memory key-value):
  Pro: < 1ms latency, flexible data structures (Hash, List, Set, Sorted Set)
  Con: limited by RAM, data loss risk (in-memory), not for analytical queries
  Use: session data, feature store (online), driver locations, rate limiting

Materialized Views (in DW):
  Pro: SQL-queryable, fresh data, no extra infrastructure
  Con: adds load to DW on refresh, not sub-millisecond
  Use: pre-aggregated BI queries, reduce scan cost on large tables

Result Cache (Snowflake/BigQuery):
  Pro: free (repeat query within 24h returns cached result instantly)
  Con: invalidated on any table change, exact query match only
  Use: dashboards that run the same query repeatedly

CDN (for static/pre-built reports):
  Pro: global distribution, sub-100ms anywhere
  Con: static data only, must pre-generate
  Use: public reports, static dashboards

Parquet file caching (Spark cache):
  Pro: keeps hot DataFrames in executor memory for reuse
  Con: evicted on executor restart, uses executor RAM
  Use: DataFrames used 3+ times in the same Spark job

Cache invalidation:
  Hardest problem in caching: when to update the cache?
  TTL-based: expire after N seconds/minutes (simple, may show stale data)
  Write-through: update cache on every write (consistent, adds write latency)
  Event-based: source table change triggers cache refresh (most current)
```

---

## Cloud Provider Trade-offs (AWS vs GCP vs Azure for DE)

| Dimension | AWS | GCP | Azure |
|---|---|---|---|
| Data warehouse | Redshift | BigQuery | Synapse |
| Managed Kafka | MSK | Pub/Sub (different model) | Event Hubs |
| Managed Spark | EMR | Dataproc | HDInsight |
| Object storage | S3 | GCS | ADLS Gen2 |
| Data catalog | Glue | Dataplex | Purview |
| ML platform | SageMaker | Vertex AI | Azure ML |
| Streaming | Kinesis | Dataflow (Beam) | Stream Analytics |

```
Choose based on:
  1. Where is your existing infrastructure?
     Most companies: AWS. Microsoft shops: Azure. ML-heavy: GCP.
  
  2. BigQuery's unique advantage:
     No cluster management; pay per query byte scanned; scales to PBs instantly
     If analytics-only, no streaming: BigQuery is often simplest + cheapest
  
  3. Azure if:
     Microsoft ecosystem (Office 365, Azure AD, Power BI)
     Enterprise license includes significant Azure credits
  
  4. AWS if:
     Broadest service selection; most DE talent available in market
     Best MSK (managed Kafka); best Glue catalog; most mature ecosystem
  
  5. Multi-cloud:
     Avoid for DE if possible — data transfer costs, latency, complexity
     Unless regulatory requirements mandate specific regions per cloud
```

---

## Interview Tips

> **Tip 1:** "Parquet vs Delta Lake — when to use each?" — Parquet is a file format; Delta Lake is Parquet + a transaction log. Use plain Parquet when: simple, read-only data lake, one writer, no ACID needed, tool compatibility is more important than transactions. Use Delta Lake when: multiple concurrent writers, you need ACID (no partial writes), schema evolution without rewriting files, time-travel queries, or CDC/MERGE operations. Delta adds 10-15% write overhead but significantly improves reliability and manageability.

> **Tip 2:** "Airflow vs Prefect — which would you choose for a new project?" — For teams starting fresh in 2024: Prefect if the team is Python-fluent and wants simpler code (no DAG files, just Python decorators). Airflow if: large team already knows it, you need the extensive plugin ecosystem (100+ providers), or you need complex DAG visualization for stakeholders. Dagster if: you want the data asset concept (track data, not just tasks) and care about lineage. For existing Airflow shops: the migration cost usually doesn't justify switching.

> **Tip 3:** "When does caching hurt more than it helps?" — When staleness is unacceptable (real-time financial data, inventory levels). When cache invalidation is complex (multi-table changes affect the cached result — hard to know when to flush). When cache hit rate is low (every query is unique → cache never hit → adds latency without benefit). When memory is constrained (caching large DataFrames causes GC thrashing). Monitor cache hit rate; if <30%, caching is probably not helping.
