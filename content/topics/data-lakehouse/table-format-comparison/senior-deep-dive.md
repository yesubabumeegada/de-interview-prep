---
title: "Table Format Comparison — Senior Deep Dive"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [table-formats, convergence, puffin, iceberg-v3, onetable, ecosystem]
---

# Table Format Comparison — Senior Deep Dive

## The Convergence Trend (2023–2024)

```
The "format wars" are ending through interoperability:

Databricks UniForm:
  Write as Delta, automatically generate Iceberg metadata
  Trino/Flink/Athena can read Delta tables via Iceberg connector
  No data copy needed — same Parquet files, two metadata views

Snowflake Iceberg Tables:
  Snowflake can read AND write Iceberg tables
  External volume points to your S3 bucket
  Iceberg becomes the "exchange format" between cloud systems

Apache XTable (formerly OneTable):
  Open-source translation layer: convert Delta ↔ Iceberg ↔ Hudi metadata
  Run as a background sync job: Delta primary → generate Iceberg + Hudi metadata
  Use case: team writes Delta, partner reads Iceberg, streaming job uses Hudi incremental

Google BigQuery Biglake Metastore:
  Manages Iceberg tables on GCS
  BigQuery, Spark, Trino all read same Iceberg tables on GCS

Convergence implications:
  The choice of "primary" format matters less than before
  Migration is cheaper (metadata translation vs data copy)
  Iceberg is emerging as the interoperability lingua franca
  Delta remains the best format within Databricks ecosystem
```

---

## Iceberg V3 Features (Emerging, 2024)

```
Iceberg V3 introduces (proposed and in-progress):

1. Row lineage:
   Each row gets a unique row_id assigned at write time
   Enables: tracking which source row produced which output
   Use case: data lineage, audit trails, GDPR subject access requests

2. Variant type (semi-structured data):
   JSON-like nested data stored efficiently in Parquet
   Similar to Spark's VariantType or Snowflake VARIANT
   Avoids storing JSON as strings (poor compression, no typing)

3. Default value expressions:
   Columns can have default values computed at write time
   Schema evolution: add column with default = "unknown" 
   Old records get the default; no file rewrite needed

4. Nanosecond timestamps:
   Current Iceberg: microsecond precision
   V3: nanosecond precision (needed for financial tick data)

5. Multi-arg transforms:
   Partition by bucket(32, user_id) AND days(event_ts) in one spec
   V2 required separate partition specs for each transform

Timeline: Iceberg V3 spec in active development (Apache Iceberg community)
Most V3 features are backwards-compatible (V3 reader can read V2 tables)
```

---

## Puffin Files: Advanced Statistics

```
Standard Iceberg column stats (manifest files):
  Per data file: min/max, null count, distinct count
  Good for: range predicates (WHERE order_date BETWEEN ...)
  Limitation: can't answer "is value X in this file?" efficiently

Puffin statistics (Iceberg extension, 0.14+):
  Blob files stored alongside data files
  Contain: theta sketches (cardinality estimates), bloom filters, histograms
  
  Use cases:
  1. Bloom filters: "is customer_id=123 in this file?"
     → Avoids reading files that don't contain the value (exact match speedup)
  2. Theta sketches: "approximately how many distinct customer_ids?"
     → Used by query optimizers for better join planning
  3. Histograms: value distribution per column
     → Better partition pruning decisions by query optimizer

  Support:
    Trino: reads Puffin bloom filters (significant speedup for equality predicates)
    Spark: Iceberg 1.3+ supports Puffin statistics
    
  Write Puffin stats (Spark):
  spark.sql("""
    CALL local.system.rewrite_position_delete_files(
      table => 'db.orders',
      options => map('write-puffin-stats', 'true')
    )
  """)
```

---

## Format Selection Framework for Senior Engineers

```
Decision framework for greenfield lakehouse design:

Step 1: What's your primary compute?
  → Databricks: Delta (native, optimized, managed)
  → EMR + Athena: Iceberg (Glue catalog, Athena native support)
  → Confluent + Flink: Iceberg (Flink native Iceberg sink)
  → Open/agnostic: Iceberg (broadest engine support)

Step 2: Multi-engine requirement?
  → Single engine (Spark only): Delta or Iceberg, doesn't matter
  → Multi-engine (Spark + Trino + Flink): Iceberg strongly preferred
  → Snowflake + Spark: Iceberg (Snowflake Iceberg tables), or Delta+UniForm

Step 3: Write pattern?
  → High-frequency CDC (>1M upserts/hour): Hudi MOR or Iceberg V2 MOR
  → Batch ETL (hourly/daily): any format
  → Streaming append-only: any format, Iceberg+Flink most efficient

Step 4: Incremental downstream?
  → Efficient "what changed since X" queries: Hudi (best native support)
  → Delta CDF or Iceberg changelog views are workable alternatives

Step 5: Governance?
  → Unity Catalog (Databricks): Delta + Unity Catalog (best integration)
  → AWS: Glue + Iceberg (natural fit)
  → Multi-cloud: Iceberg + Nessie (most portable)

Scoring (1-5 per dimension):
  
  | Dimension               | Delta | Iceberg | Hudi |
  |-------------------------|-------|---------|------|
  | Databricks integration  |  5    |    3    |  2   |
  | Multi-engine support    |  3    |    5    |  2   |
  | CDC / high-freq upsert  |  3    |    4    |  5   |
  | Incremental reads       |  3    |    3    |  5   |
  | Open standard           |  3    |    5    |  4   |
  | Community/ecosystem     |  4    |    5    |  3   |
  | Operational maturity    |  5    |    4    |  3   |
```

---

## Interview Tips

> **Tip 1:** "Where do you see table format evolution going in 5 years?" — Convergence around Iceberg as the interoperability standard, with Delta and Hudi remaining as optimized primary formats. Snowflake, BigQuery, Databricks, and cloud vendors will all support Iceberg as the exchange format. The catalog layer (Apache Polaris/Iceberg REST spec) standardizes catalog access. Practical effect: format choice becomes less critical because translation layers (UniForm, XTable) reduce lock-in.

> **Tip 2:** "How do you make a table format recommendation to a VP?" — Avoid technical jargon. Frame as business risk and cost: "Delta gives us the best developer experience on our Databricks platform — our team is already trained, we get Databricks support, and our query performance is optimized. The risk is vendor dependency — if we ever leave Databricks, migrating Delta tables to Iceberg is a month-long effort. Given our 3-year Databricks contract, Delta is the right choice. We'll build in Iceberg read capability via UniForm so external partners can access our data without us sharing credentials."

> **Tip 3:** "A new team member asks: should we start new tables in Delta or Iceberg?" — For a Databricks shop: Delta, because Databricks' managed operations (Liquid Clustering, auto-optimization, Unity Catalog lineage) add significant value that only works with Delta. Add UniForm to any table that needs to be accessed from non-Databricks tools. For a non-Databricks shop: Iceberg, because the multi-engine support and open standard avoid premature lock-in decisions. The worst answer is "let's support both natively" — that doubles operational complexity for no benefit.
