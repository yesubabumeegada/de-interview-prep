---
title: "Azure Synapse Analytics — Fundamentals"
topic: azure
subtopic: azure-synapse
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, synapse, data-warehouse, sql-pool, spark, analytics]
---

# Azure Synapse Analytics — Fundamentals


## 🎯 Analogy

Think of Azure Synapse as a Swiss Army knife for analytics: it combines a dedicated SQL pool (cloud data warehouse), Spark pools (big data processing), serverless SQL on ADLS, and pipelines (ADF-like orchestration) — all in one workspace.

---
## What Is Azure Synapse Analytics?

Azure Synapse Analytics is a **unified analytics platform** that combines enterprise data warehousing, big data processing, and data integration in one service. Think of it as Azure SQL Data Warehouse + Spark + Data Factory combined.

```
Synapse workspace components:
  
  Dedicated SQL Pool    → Traditional MPP data warehouse (formerly SQL DW)
                          Pay per DWU (Data Warehouse Unit)
                          Best for: structured analytics, BI, reporting
  
  Serverless SQL Pool   → Query data in ADLS without provisioning
                          Pay per TB scanned
                          Best for: ad-hoc queries on data lake, ELT preparation
  
  Apache Spark Pool     → Managed Spark clusters inside Synapse
                          Pay per vCore-hour when running
                          Best for: data engineering, ML, complex transformations
  
  Data Integration      → ADF-equivalent pipeline functionality built-in
                          Same UI and concepts as Azure Data Factory
  
  Synapse Link          → Zero-ETL analytical replica of operational databases
                          (Azure Cosmos DB, Azure SQL DB, Dataverse)
  
  Knowledge Center      → Sample datasets and notebooks to get started quickly
```

---

## Dedicated SQL Pool Architecture

```
MPP (Massively Parallel Processing) architecture:

                    ┌──────────────┐
                    │   Control    │
                    │    Node      │  ← receives queries, creates plan
                    └──────┬───────┘
                           │ distributes work
          ┌────────────────┼─────────────────┐
          ▼                ▼                 ▼
    ┌──────────┐     ┌──────────┐      ┌──────────┐
    │ Compute  │     │ Compute  │ ...  │ Compute  │
    │  Node 1  │     │  Node 2  │      │  Node 60 │
    └──────────┘     └──────────┘      └──────────┘
          │                │                 │
    ┌──────────┐     ┌──────────┐      ┌──────────┐
    │ 60 Distrib│    │ 60 Distrib│    │ 60 Distrib│
    └──────────┘     └──────────┘      └──────────┘

60 fixed distributions across compute nodes
Each distribution holds a portion of each table's data
Scale: DW100c (1 compute node) to DW30000c (60 nodes)

Key insight: every table is split across 60 distributions
Distribution columns determine WHERE each row lands
Poor distribution → data movement between nodes → slow queries
```

---

## Distribution Strategies

```
Three distribution options for Dedicated SQL Pool tables:

1. HASH distributed (default recommended for fact tables)
   Syntax: WITH (DISTRIBUTION = HASH(customer_id))
   
   Rows with same customer_id → same distribution
   Joins on customer_id between two hash-distributed tables = co-located (fast)
   
   Choose column:
   ✓ High cardinality (millions of distinct values)
   ✓ Not frequently used in WHERE filters alone
   ✓ Shared by most join operations
   ✗ Avoid NULL-heavy columns (all NULLs go to distribution 1 → skew)
   ✗ Avoid date columns (skew if querying recent dates)

2. ROUND_ROBIN (default for staging tables)
   Syntax: WITH (DISTRIBUTION = ROUND_ROBIN)
   
   Rows distributed evenly in rotating order across 60 distributions
   Fast loads (no hash calculation)
   Slow joins (data must be shuffled before any join)
   Use for: staging tables, temporary tables, tables with no clear join key

3. REPLICATED (for dimension tables)
   Syntax: WITH (DISTRIBUTION = REPLICATE)
   
   Full copy of table on every compute node
   Joins never require data movement (row already on every node)
   Max size: ~2GB (recommended), 10GB (technical limit) — larger tables negate benefit
   Use for: dimension tables, lookup tables, small reference tables

Index options:
  CLUSTERED COLUMNSTORE INDEX (default): best compression + analytics query performance
  HEAP: no index (use for staging, fast inserts, no query performance needed)
  CLUSTERED INDEX (rowstore): use only for frequently point-lookup tables
```

---

## Serverless SQL Pool

```
Serverless SQL Pool: query ADLS Gen2 data with T-SQL, no provisioning

Syntax:
  SELECT *
  FROM OPENROWSET(
    BULK 'https://account.dfs.core.windows.net/silver/orders/*.parquet',
    FORMAT = 'PARQUET'
  ) AS orders

Supported formats: Parquet, CSV, JSON, Delta (via OPENROWSET or external tables)

Create external table for reusable access:
  CREATE EXTERNAL FILE FORMAT parquet_format
  WITH (FORMAT_TYPE = PARQUET, DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec');
  
  CREATE EXTERNAL TABLE silver_orders (
      order_id    BIGINT,
      customer_id INT,
      amount      DECIMAL(18,2),
      order_date  DATE
  )
  WITH (
      LOCATION = 'silver/orders/',
      DATA_SOURCE = adls_source,
      FILE_FORMAT = parquet_format
  );

Pricing: $5.00 per TB scanned (only pay for queries run)
Use cases:
  - Data exploration before loading to Dedicated SQL Pool
  - Power BI reports directly on data lake
  - ELT: transform data in ADLS using T-SQL, write to ADLS via CETAS
```

---


## ▶️ Try It Yourself

```sql
-- Synapse Dedicated SQL Pool: create an external table on ADLS
CREATE EXTERNAL DATA SOURCE adls_orders
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://raw@mydatalake.dfs.core.windows.net',
    CREDENTIAL = managed_identity_cred
);

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (FORMAT_TYPE = PARQUET);

CREATE EXTERNAL TABLE external_orders (
    order_id BIGINT,
    amount DECIMAL(12,2),
    region NVARCHAR(50),
    order_date DATE
)
WITH (
    LOCATION = 'orders/2024/',
    DATA_SOURCE = adls_orders,
    FILE_FORMAT = parquet_format
);

-- Query data directly from ADLS without loading
SELECT region, SUM(amount) FROM external_orders GROUP BY region;
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between Dedicated SQL Pool and Serverless SQL Pool?" — Dedicated SQL Pool: provisioned MPP warehouse with columnar storage, fixed cost per DWU-hour whether running queries or not, data stored in Synapse internal storage (not raw ADLS), supports INSERT/UPDATE/UPSERT, optimized for repeated BI queries. Serverless SQL Pool: no provisioned resources, reads directly from ADLS, pay-per-query ($5/TB), read-only (can CETAS to write), best for ad-hoc exploration and ELT on data lake. Most architectures use both: Serverless for exploration/ELT, Dedicated for serving layer.

> **Tip 2:** "How many distributions does a Dedicated SQL Pool have?" — Always 60 distributions, regardless of DWU level. When you scale up (add compute nodes), Synapse maps those 60 distributions across more nodes — each node handles fewer distributions, improving parallel execution. Scale does not change data layout, which is why you can pause/resume without data migration.

> **Tip 3:** "What table distribution should I use for a 500M row fact table?" — HASH distributed on the most common join key (usually the primary key of the largest dimension — e.g., `customer_id` or `product_id`). Choose the column that: (1) is high-cardinality (will spread rows evenly across 60 distributions), (2) appears in most joins (collocates with related dimension tables), (3) is not nullable. Avoid distribution by date — recent dates are much larger than old ones, causing skew.
