---
title: "Azure Data Lake Analytics - Fundamentals"
topic: azure
subtopic: azure-data-lake-analytics
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, adla, u-sql, data-lake, analytics, batch-processing]
---

# Azure Data Lake Analytics — Fundamentals

## 🎯 Analogy

Think of Azure Data Lake Analytics (ADLA) as a serverless MapReduce engine with a SQL-like language bolted on. You bring your data to Azure Data Lake Storage, write a U-SQL job, and Azure handles spinning up the distributed compute — you only pay for the processing time, not idle infrastructure.

---

## What Is Azure Data Lake Analytics?

Azure Data Lake Analytics is a **cloud-based, on-demand analytics job service** designed to process big data workloads stored in Azure Data Lake Storage (ADLS). Unlike traditional Hadoop clusters or always-on Spark clusters, ADLA is **serverless and job-scoped** — compute is allocated when a job runs and released when it completes.

Key characteristics:
- **No infrastructure to manage**: No cluster provisioning, no VM sizing, no cluster lifecycle.
- **U-SQL language**: A hybrid of SQL (familiar to data engineers) and C# (extensible for complex logic).
- **Pay-per-job model**: Billing is based on Analytics Units (AUs) consumed per job, not idle time.
- **Deep ADLS integration**: Native integration with ADLS Gen1; Gen2 support via compatibility layers.
- **Parallel job execution**: Jobs automatically scale across distributed nodes.

```
ADLA Architecture:

  ADLS Gen1/Gen2              Azure Data Lake Analytics
  ┌──────────────┐            ┌──────────────────────────────┐
  │  Raw Files   │──────────► │  U-SQL Job Submission        │
  │  (CSV, JSON, │            │  ┌────────────────────────┐  │
  │   Parquet,   │            │  │  U-SQL Script          │  │
  │   ORC)       │            │  │  + Analytics Units (AU)│  │
  └──────────────┘            │  └────────────────────────┘  │
                              │  ┌────────────────────────┐  │
                              │  │  Distributed Execution │  │
                              │  │  (parallel vertices)   │  │
                              │  └────────────────────────┘  │
                              └──────────────────────────────┘
                                            │
                                            ▼
                                  Output back to ADLS
```

---

## U-SQL: The Core Language

U-SQL is the query language for ADLA. It combines:
- **SQL SELECT/FROM/WHERE/GROUP BY** syntax for data manipulation
- **C# expressions** for transformations, UDFs, and custom logic
- **Rowsets** as the primary data abstraction (similar to DataFrames)

### Basic U-SQL Script Structure

```sql
// Read input from ADLS
@rawOrders =
    EXTRACT OrderId    int,
            CustomerId int,
            Amount     decimal,
            OrderDate  DateTime
    FROM "/data/orders/2024/*.csv"
    USING Extractors.Csv(skipFirstNRows: 1);

// Filter and transform
@filteredOrders =
    SELECT OrderId,
           CustomerId,
           Amount,
           OrderDate.Year AS OrderYear,
           OrderDate.Month AS OrderMonth
    FROM @rawOrders
    WHERE Amount > 100.0;

// Aggregate
@summary =
    SELECT OrderYear,
           OrderMonth,
           COUNT(*) AS OrderCount,
           SUM(Amount) AS TotalRevenue
    FROM @filteredOrders
    GROUP BY OrderYear, OrderMonth;

// Write output
OUTPUT @summary
TO "/output/order_summary.csv"
USING Outputters.Csv(outputHeader: true);
```

### Key U-SQL Concepts

**Extractors** — read data from files:
- `Extractors.Csv()` — delimited text files
- `Extractors.Tsv()` — tab-separated files
- `Extractors.Text()` — custom delimiters
- Custom extractors written in C# for binary or proprietary formats

**Outputters** — write data:
- `Outputters.Csv()` — write CSV
- `Outputters.Tsv()` — write TSV
- Custom outputters for custom formats

**File Set Patterns** — process multiple files:
```sql
FROM "/logs/{date:yyyy}/{date:MM}/{date:dd}/events.csv"
```

---

## Analytics Units (AUs)

**Analytics Units** are the currency of ADLA compute. Each AU represents a unit of parallelism — roughly one CPU and associated memory.

| Concept | Detail |
|---------|--------|
| Min AUs per job | 1 |
| Max AUs per job | 500 (soft limit, can be raised) |
| Billing | Per AU-second consumed |
| Parallelism | More AUs = more parallel vertices = faster job |
| Account limit | Default 250 AUs per account (shared across concurrent jobs) |

**How AUs affect a job:**
- A job with 10 AUs runs 10 parallel stages simultaneously.
- Doubling AUs roughly halves wall-clock time for data-parallel workloads (until I/O becomes the bottleneck).
- Over-allocating AUs wastes money without speed gains for small datasets.

```
Job with 1 AU:     Stage1 → Stage2 → Stage3 → Stage4   (sequential)
Job with 4 AUs:    Stage1 │ Stage2             (parallel)
                   Stage3 │ Stage4
                   Total time ≈ 2x faster
```

---

## Integration with ADLS Gen2

ADLA was originally built for ADLS Gen1. Integration with ADLS Gen2 is supported but requires configuration:

- **Storage account**: ADLS Gen2 (hierarchical namespace enabled on Azure Blob Storage)
- **Authentication**: Service principal or managed identity
- **Path syntax**: `abfss://<container>@<account>.dfs.core.windows.net/<path>`

> **Interview note:** ADLA's primary native integration is with ADLS Gen1. When asked about ADLS Gen2 + ADLA, mention that the integration works but Microsoft recommends migrating ADLA workloads to Azure Synapse Analytics or Azure Databricks, which have first-class Gen2 support.

---

## Job Lifecycle

```
1. Submit U-SQL Script
        │
        ▼
2. ADLA Compiles U-SQL → Execution Plan (DAG of vertices)
        │
        ▼
3. Job Manager allocates AUs and schedules vertices
        │
        ▼
4. Vertices execute in parallel reading from ADLS
        │
        ▼
5. Intermediate results shuffled between stages
        │
        ▼
6. Final output written to ADLS
        │
        ▼
7. Compute released, billing stops
```

---

## Key Interview Topics at the Fundamentals Level

1. **What is U-SQL?** — SQL + C# hybrid for batch processing on ADLS.
2. **What are Analytics Units?** — Parallelism units; billing is per AU-second.
3. **When would you use ADLA?** — Serverless batch jobs on ADLS data without cluster management overhead.
4. **ADLA vs. Spark?** — ADLA is serverless and U-SQL-based; Spark is more flexible, supports streaming, and is the current Microsoft recommendation for new workloads.
5. **ADLA vs. ADF?** — ADF orchestrates pipelines and moves data; ADLA runs the analytics compute for transformation.
