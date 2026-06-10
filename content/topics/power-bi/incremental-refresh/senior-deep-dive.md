---
title: "Incremental Refresh — Senior Deep Dive"
topic: power-bi
subtopic: incremental-refresh
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [power-bi, incremental-refresh, interview, senior, advanced]
---

# Incremental Refresh — Senior Deep Dive

## Partition Internals

### How Power BI Creates and Manages Partitions

Behind the scenes, incremental refresh uses the **Tabular Object Model (TOM)** partition system. Each partition is a separate data segment with its own query (defined by the RangeStart/RangeEnd bounds).

```
Table: FactSales
  Partition: FactSales-2021       → SELECT * FROM FactSales WHERE OrderDate >= '2021-01-01' AND OrderDate < '2022-01-01'
  Partition: FactSales-2022       → SELECT * FROM FactSales WHERE OrderDate >= '2022-01-01' AND OrderDate < '2023-01-01'
  Partition: FactSales-2024-11    → SELECT * FROM FactSales WHERE OrderDate >= '2024-11-01' AND OrderDate < '2024-12-01'
  Partition: FactSales-2024-12    → SELECT * FROM FactSales WHERE OrderDate >= '2024-12-01' AND OrderDate < '2025-01-01'
```

When a refresh runs, Power BI:
1. Calculates which partitions are in the refresh window (based on current date)
2. Drops those partitions (DELETE from VertiPaq)
3. Re-queries the source with the date bounds for each partition
4. Creates new partition segments with fresh data
5. All other partitions remain untouched (no I/O, no query)

### Partition Naming Convention

Power BI uses a predictable naming convention:
- `TableName-YYYY` for annual partitions (archive)
- `TableName-YYYYMM` for monthly partitions (recent)
- `TableName-YYYYMMDD` for daily partitions (very recent)
- `TableName-YYYYMMDD_HH` for hourly partitions (real-time)

---

## Advanced Partition Strategy: Custom Partitioning via XMLA

For enterprise scenarios, you can fully control the partitioning strategy by bypassing the built-in incremental refresh policy and managing partitions directly via XMLA.

### Use Case: Non-Date Partitioning

Standard incremental refresh partitions only by date. For some workloads, partitioning by **region** or **product category** is more efficient:

```python
# Python: Create custom partitions via XMLA (using tabular-editor or microsoft-analytics-services)
import json
import requests

xmla_endpoint = "powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName"

# Add partition for North region
add_partition_request = {
    "type": "alter",
    "object": {
        "database": "DatasetName",
        "table": "FactSales",
        "partition": "FactSales-North"
    },
    "definition": {
        "name": "FactSales-North",
        "source": {
            "type": "query",
            "query": "SELECT * FROM dbo.FactSales WHERE Region = 'North'",
            "dataSource": "SqlDataSource"
        }
    }
}
```

### Use Case: Composite Partitioning (Date + Region)

```csharp
// C# via TOM SDK: Create date × region partitions
var table = model.Tables["FactSales"];

var regions = new[] { "North", "South", "East", "West" };
var months = GetLast12Months(); // returns list of (year, month) tuples

foreach (var region in regions)
{
    foreach (var (year, month) in months)
    {
        var startDate = new DateTime(year, month, 1);
        var endDate = startDate.AddMonths(1);
        var partitionName = $"FactSales-{region}-{year:D4}{month:D2}";

        var partition = new Partition
        {
            Name = partitionName,
            Source = new QueryPartitionSource
            {
                Query = $"SELECT * FROM dbo.FactSales WHERE Region = '{region}' AND OrderDate >= '{startDate:yyyy-MM-dd}' AND OrderDate < '{endDate:yyyy-MM-dd}'",
                DataSource = table.Model.DataSources["SqlDS"]
            }
        };
        table.Partitions.Add(partition);
    }
}
model.SaveChanges();
```

---

## Incremental Refresh and Change Data Capture (CDC)

For maximum efficiency, combine Power BI incremental refresh with a CDC-based source:

```
Source Database (SQL Server with CDC enabled)
    ↓
cdc.dbo_FactSales_CT (change table: inserts, updates, deletes)
    ↓
Power Query: merge changes with last state
    ↓
Power BI: incremental refresh on the merged result
```

```powerquery
// Power Query: Apply CDC changes to incremental window
let
    // Get the current state for the refresh window
    CurrentWindow = Sql.Database("server", "db", [
        Query = "
            SELECT s.*
            FROM dbo.FactSales s
            WHERE s.OrderDate >= @RangeStart AND s.OrderDate < @RangeEnd
        ",
        RangeStart = RangeStart, RangeEnd = RangeEnd
    ]),

    // The incremental refresh policy handles partition management
    // Power BI drops and reloads the partition — CDC changes are included
    // because we're re-querying the source state for the window
in
    CurrentWindow
```

For **updates** to historical data (not just new rows), the standard incremental refresh is insufficient because historical partitions are frozen. In this case:

1. Use the **detect data changes** feature with a `ModifiedDate` column
2. Or use XMLA to manually refresh specific historical partitions after backfills

---

## Query Folding Enforcement

For incremental refresh to work correctly, the RangeStart/RangeEnd filter **must fold** to the source. Power BI validates this at publish time for some connectors, but not all.

### Verifying Folding in Power Query Diagnostics

```powerquery
// Start Query Diagnostics in Power Query Editor
// The diagnostic table shows "Data Source Query" — this should contain your date filter

// Example output for SQL Server (folding CONFIRMED):
// Data Source Query: SELECT * FROM [dbo].[FactSales] WHERE [OrderDate] >= '2024-12-01 00:00:00' AND [OrderDate] < '2025-01-01 00:00:00'

// If Data Source Query shows ALL rows (no WHERE clause), folding is BROKEN
```

### Forcing Folding with Value.NativeQuery

When auto-folding fails, use `Value.NativeQuery` explicitly:

```powerquery
let
    Source = Sql.Database("server", "salesdb"),
    // Explicit parameterized query — always folds
    FilteredSales = Value.NativeQuery(
        Source,
        "SELECT OrderID, CustomerKey, ProductKey, OrderDate, SalesAmount
         FROM dbo.FactSales
         WHERE OrderDate >= @StartDate AND OrderDate < @EndDate",
        [StartDate = DateTime.Date(RangeStart), EndDate = DateTime.Date(RangeEnd)]
    )
in
    FilteredSales
```

**Note**: `Value.NativeQuery` with parameters prevents SQL injection while ensuring folding.

---

## Incremental Refresh in Azure Synapse and Fabric

### Azure Synapse Analytics

For Synapse SQL pools, incremental refresh benefits from:
- **Result Set Caching**: Synapse caches query results — repeated partition queries are faster
- **Materialized Views**: Pre-compute aggregations for common partition queries

```sql
-- Synapse: Create materialized view for common aggregation
CREATE MATERIALIZED VIEW dbo.MV_SalesMonthly
AS
SELECT
    DATETRUNC(MONTH, OrderDate) AS MonthDate,
    ProductKey,
    SUM(SalesAmount) AS TotalSales,
    COUNT(*) AS OrderCount
FROM dbo.FactSales
GROUP BY DATETRUNC(MONTH, OrderDate), ProductKey;
```

### Microsoft Fabric Lakehouse

In Fabric, incremental refresh works against Lakehouse SQL endpoints:

```powerquery
// Fabric Lakehouse connection
let
    Source = Lakehouse.Contents(null){[workspaceId="...", itemId="...", itemType="Lakehouse"]}[Data],
    FactSales = Source{[Schema="dbo", Item="FactSales"]}[Data],
    Filtered = Table.SelectRows(FactSales, each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd)
in
    Filtered
```

Fabric Delta tables support **predicate pushdown** — the date filter folds to the Delta scan, enabling partition pruning at the file level (even faster than traditional SQL folding).

---

## Orchestration Patterns

### Triggering Specific Partition Refresh via Pipeline

Azure Data Factory or Fabric Pipelines can trigger targeted partition refreshes:

```json
// ADF: Power BI Refresh Dataset Activity (Enhanced)
{
  "name": "RefreshLatestPartition",
  "type": "PowerBIRefreshDataset",
  "typeProperties": {
    "datasetId": "<dataset-id>",
    "workspaceId": "<workspace-id>",
    "refreshType": "Enhanced",
    "notifyOption": "NoNotification",
    "objects": [
      {
        "table": "FactSales",
        "partition": "FactSales-202412"
      }
    ]
  }
}
```

### Dependency-Aware Refresh

If multiple tables have incremental refresh, refresh them in the correct dependency order:

```
1. DimProduct (full refresh — small table, no incremental needed)
2. DimCustomer (full refresh)
3. FactSales (incremental — depends on Dim tables being current)
4. AggSalesByMonth (full refresh — recomputed from FactSales)
```

---

## Performance Tuning for Large Partitions

### Parallelism Control

The Enhanced Refresh API allows controlling parallelism:

```http
POST /datasets/{id}/refreshes
{
  "type": "Enhanced",
  "maxParallelism": 4,   // Refresh up to 4 partitions simultaneously
  "objects": [...]
}
```

Too high a value can overwhelm the source database. Balance parallelism against source connection limits.

### Partition Size Optimization

Monthly partitions are typically optimal. For very high-volume tables (>100M rows/month), consider **weekly** partitions:

```
Refresh range: 60 days
Granularity: Weekly (Power BI auto-selects based on range)
Weekly partition: ~25M rows → fast individual partition refresh
```

---

## Summary

- Incremental refresh uses **TOM partitions** — each partition is an independent VertiPaq segment
- **Custom partitioning via XMLA** enables non-date partitioning and composite partitioning
- **CDC integration** handles updates; standard incremental refresh only handles new rows naturally
- Use `Value.NativeQuery` with parameters when auto-folding fails
- **Fabric Delta tables** support predicate pushdown — faster than traditional SQL folding
- Control refresh orchestration with **Enhanced Refresh API** and pipeline integration
- Tune **maxParallelism** to balance refresh speed against source database load

## ⚡ Cheat Sheet

**Data model (Import vs DirectQuery vs Composite)**
```
Import:       data loaded into Power BI memory → fastest queries; stale by refresh schedule
DirectQuery:  queries sent live to source → always current; limited DAX; source load
Composite:    Import for large tables + DirectQuery for real-time; best of both
Dual storage: table can serve as Import or DirectQuery depending on query context
```

**DAX essentials**
```dax
-- Measure (always uses filter context)
Total Revenue = SUM(orders[amount])
Revenue YTD = CALCULATE([Total Revenue], DATESYTD(dates[date]))
Revenue LY  = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dates[date]))
MoM Growth  = DIVIDE([Total Revenue] - [Revenue LY], [Revenue LY])

-- CALCULATE: modifies filter context
Revenue US = CALCULATE([Total Revenue], orders[region] = "US")

-- Iterator functions (row context)
Avg Order = AVERAGEX(orders, orders[amount])
Weighted Score = SUMX(products, products[score] * products[weight]) / SUM(products[weight])

-- Variables (performance + readability)
Margin % = VAR revenue = [Total Revenue]
           VAR cost = [Total Cost]
           RETURN DIVIDE(revenue - cost, revenue)
```

**Row-level security (RLS)**
```dax
-- Static role (in Power BI Desktop)
-- Add table filter: [region] = "US"

-- Dynamic RLS (uses logged-in user)
-- Table filter expression:
[user_email] = USERPRINCIPALNAME()

-- Or via mapping table:
[region] IN VALUES(user_region_map[region])
-- where user_region_map is filtered by USERPRINCIPALNAME()
```

**Power Query M patterns**
```m
// Load from Snowflake
Source = Snowflake.Databases("xy12345.snowflakecomputing.com", "PROD"),
gold = Source{[Name="GOLD"]}[Data],
orders = gold{[Schema="PUBLIC",Item="ORDERS"]}[Data],
// Type columns
typed = Table.TransformColumnTypes(orders,{{"amount", type number}})

// Parameterized query (for incremental refresh)
#"Filtered Rows" = Table.SelectRows(orders, each [order_date] >= RangeStart 
                                              and [order_date] < RangeEnd)
```

**Incremental refresh setup**
```
1. Create parameters: RangeStart (Date/Time), RangeEnd (Date/Time)
2. Filter table in Power Query: order_date >= RangeStart AND < RangeEnd
3. Define incremental refresh policy: Archive 3 years, Refresh last 3 days
4. Publish → Power BI manages partitions automatically
```

**Performance optimization**
```
- Use Import mode for large historical tables (DirectQuery = slower)
- Avoid calculated columns; prefer measures (calculated at query time, not stored)
- Avoid bidirectional relationships (use CROSSFILTER sparingly)
- Star schema: fact table has numeric keys + measures only; dimensions separate
- Aggregations: pre-aggregate large tables; DQ falls through to aggregation table
- Performance Analyzer: shows DAX query time + visual render time per visual
```

**Key interview points**
- DAX filter context vs row context: measures use filter context; calculated columns use row context
- CALCULATE is the most powerful function — changes filter context
- Many-to-many relationships: use bridge table or CROSSFILTER(BOTH) with caution
- Composite models: connect Power BI to Fabric/Databricks via DirectQuery + import local dims
