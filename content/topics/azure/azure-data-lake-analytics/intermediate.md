---
title: "Azure Data Lake Analytics - Intermediate"
topic: azure
subtopic: azure-data-lake-analytics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, adla, u-sql, analytics-units, catalog, udf, cost-optimization]
---

# Azure Data Lake Analytics — Intermediate

## U-SQL Catalog and Database Objects

ADLA has a **U-SQL Catalog** — a metadata store similar to a Hive Metastore or SQL Server catalog. It lets you register persistent objects that can be referenced across jobs.

### Catalog Hierarchy
```
ADLA Account
└── U-SQL Catalog
    ├── Database (master + custom)
    │   ├── Schemas
    │   ├── Tables (managed, stored in ADLS)
    │   ├── Views
    │   ├── Procedures
    │   ├── Table-Valued Functions (TVFs)
    │   └── Assemblies (C# DLLs for UDFs/UDOs)
    └── Credentials (for accessing external data)
```

### Creating and Using Tables

U-SQL supports **managed tables** backed by ADLS storage:

```sql
// Create a database and table
CREATE DATABASE IF NOT EXISTS SalesDB;

USE DATABASE SalesDB;

CREATE TABLE dbo.Orders
(
    OrderId    int,
    CustomerId int,
    Amount     decimal,
    OrderDate  DateTime,
    INDEX idx_customer
    CLUSTERED (CustomerId ASC)
    DISTRIBUTED BY HASH (CustomerId)
);

// Insert from a rowset
INSERT INTO dbo.Orders
SELECT *
FROM @rawOrders;

// Query the table in future jobs
@result =
    SELECT CustomerId, SUM(Amount) AS TotalSpend
    FROM SalesDB.dbo.Orders
    GROUP BY CustomerId;
```

> **Interview note:** U-SQL tables are columnar and partitioned. They perform better for repeated queries than reading raw CSV files every time — similar to why you'd use Parquet over CSV in a data lake.

---

## User-Defined Functions (UDFs) and User-Defined Operators (UDOs)

U-SQL extensibility comes from C# code registered as assemblies in the catalog.

### UDF Example

```csharp
// C# code in a .cs file or inline
using Microsoft.Analytics.Interfaces;
using System;

namespace MyUDFs
{
    public class StringHelpers
    {
        public static string MaskEmail(string email)
        {
            if (string.IsNullOrEmpty(email)) return email;
            int atIndex = email.IndexOf('@');
            if (atIndex <= 1) return email;
            return email.Substring(0, 2) + new string('*', atIndex - 2) + email.Substring(atIndex);
        }
    }
}
```

```sql
// Register assembly in U-SQL
DROP ASSEMBLY IF EXISTS MyUDFs;
CREATE ASSEMBLY MyUDFs FROM @"/assemblies/MyUDFs.dll";

// Reference and use in a script
REFERENCE ASSEMBLY MyUDFs;

@result =
    SELECT OrderId,
           MyUDFs.StringHelpers.MaskEmail(CustomerEmail) AS MaskedEmail
    FROM @rawData;
```

### User-Defined Aggregators (UDAGGs)

For custom aggregations not covered by built-in functions:

```csharp
[SqlUserDefinedAggregator(IsRecursive = true)]
public class MedianAggregator : IAggregate<double, List<double>, double>
{
    // Implement Init(), Accumulate(), Merge(), Terminate()
    // ...
}
```

---

## Partitioning and Performance Optimization

### Table Partitioning

Partition large tables to enable partition pruning (skip reading irrelevant data):

```sql
CREATE TABLE dbo.PartitionedOrders
(
    OrderId   int,
    OrderDate DateTime,
    Amount    decimal,
    INDEX idx
    CLUSTERED (OrderDate ASC)
    DISTRIBUTED BY HASH (OrderId)
)
PARTITIONED BY (OrderYear int, OrderMonth int);

// Insert data into specific partition
INSERT INTO dbo.PartitionedOrders
    PARTITION (OrderYear = 2024, OrderMonth = 3)
SELECT OrderId, OrderDate, Amount
FROM @rawData
WHERE OrderDate.Year == 2024 AND OrderDate.Month == 3;
```

### File Set Optimization

Use virtual column `__filename` and `__filegroup` for metadata:

```sql
@logs =
    EXTRACT UserId  string,
            Event   string,
            __filename string,
            __filegroup string
    FROM "/logs/{*}/{*}/events.csv"
    USING Extractors.Csv();

// Filter on virtual columns — avoids reading irrelevant files
@filtered =
    SELECT *
    FROM @logs
    WHERE __filegroup == "2024/03";
```

### AU Tuning

| Job Characteristic | Recommended AU Strategy |
|-------------------|------------------------|
| Small job (< 1 GB input) | 1–5 AUs; more AUs add overhead |
| Medium job (1–100 GB) | 10–50 AUs; test empirically |
| Large job (100 GB+) | 50–250 AUs; monitor vertex execution |
| Fan-out heavy (many files) | Higher AUs help parallelise extraction |
| Join-heavy jobs | Ensure sufficient AUs to hold intermediate data |

---

## Cost Model Deep Dive

ADLA billing formula:
```
Cost = (Analytics Units) × (Job Duration in seconds) × (AU-second price)
```

AU-second price (approximate as of 2024): **~$0.00025 per AU-second**

### Example Cost Calculation

Job: process 500 GB of logs, runs for 600 seconds with 50 AUs
```
Cost = 50 AUs × 600 seconds × $0.00025
     = 30,000 AU-seconds × $0.00025
     = $7.50 per job run
```

### Cost Optimization Strategies

1. **Right-size AUs**: Profile jobs and find the AU count where adding more AUs yields diminishing returns.
2. **Use U-SQL tables instead of raw files**: Columnar storage means less data scanned per query.
3. **Partition pruning**: Structured partitions skip irrelevant data entirely.
4. **Job prioritization**: Use job priority (0–1000, lower = higher priority) to sequence workloads.
5. **Recurring jobs**: Cache intermediate results to avoid reprocessing upstream data.

---

## Integration Patterns

### ADLA + ADF Orchestration

Azure Data Factory can trigger ADLA jobs via the **Data Lake Analytics U-SQL activity**:

```json
{
  "name": "RunUSQLJob",
  "type": "DataLakeAnalyticsU-SQL",
  "typeProperties": {
    "scriptPath": "/scripts/transform_orders.usql",
    "scriptLinkedService": "ADLSLinkedService",
    "degreeOfParallelism": 10,
    "priority": 100,
    "parameters": {
      "in_path": "/data/raw/2024/03/",
      "out_path": "/data/processed/2024/03/"
    }
  }
}
```

### Parameterising U-SQL Scripts

```sql
DECLARE @inputPath string = "/data/raw/2024/03/";
DECLARE @outputPath string = "/data/processed/2024/03/";

@data =
    EXTRACT OrderId int, Amount decimal
    FROM @inputPath + "orders.csv"
    USING Extractors.Csv();

OUTPUT @data
TO @outputPath + "orders_clean.csv"
USING Outputters.Csv();
```

---

## Monitoring and Diagnostics

ADLA jobs expose detailed execution graphs through the **Azure Portal** and **Visual Studio / VS Code ADLA extensions**:

- **Job Graph**: DAG visualisation of vertex stages, execution times, and data volumes
- **Vertex execution view**: Per-vertex input/output bytes, CPU time, and wall-clock time
- **Failed vertex diagnostics**: Error messages, stack traces from C# UDFs

Key metrics to monitor:
- **Peak memory usage per vertex** — OOM errors indicate you need more AUs or script redesign
- **Skewed vertex execution** — one slow vertex signals data skew in HASH distribution keys
- **I/O wait time vs. CPU time** — I/O-bound jobs benefit less from adding AUs
