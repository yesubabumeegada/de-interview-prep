---
title: "Performance Analyzer — Senior Deep Dive"
topic: power-bi
subtopic: performance-analyzer
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [power-bi, performance-analyzer, interview, senior, advanced]
---

# Performance Analyzer — Senior Deep Dive

## VertiPaq Engine Query Lifecycle

Understanding the full query lifecycle helps target optimization at the right layer.

### Query Path

```
Report Visual Interaction
        ↓
DAX Query Engine (Formula Engine)
        ↓
Storage Engine Plan Generation
        ↓
VertiPaq Scan (parallel, per segment)
        ↓
Datacache returned to FE
        ↓
FE: Joins, final calculations
        ↓
Visual result
```

### Storage Engine (SE) Internal Operations

```
SE Operations (Fast, parallelizable):
  - Column dictionary lookups
  - Bitmap filter evaluation
  - Segment-level aggregation (SUM, COUNT, MIN, MAX)
  - GroupBy operations on low-cardinality columns
  - Relationship traversal via bitmap indexes

FE Operations (Slow, serial):
  - RANKX iteration
  - TOPN with complex expressions
  - FILTER over large tables row-by-row
  - LOOKUPVALUE inside iterators
  - CALCULATE inside SUMX/AVERAGEX (context transition per row)
  - IF/SWITCH evaluation per row in iterators
```

---

## Advanced SE Query Analysis

### Reading Raw SE Queries in DAX Studio

Enable **Query Plan** and **Server Timings** simultaneously. Each SE query in Server Timings corresponds to a node in the physical query plan.

```
SE Query #1 (120ms):
  xmSQL: SELECT [FactSales].[ProductKey], SUM([FactSales].[SalesAmount]) FROM [FactSales]

SE Query #2 (35ms):
  xmSQL: SELECT [DimProduct].[ProductKey], [DimProduct].[Category] FROM [DimProduct]

FE operation: JOIN datacache1 with datacache2 on ProductKey → 2,100ms FE time
```

The JOIN itself is in FE — this indicates the SE returned two separate datacaches that the FE is joining. For large tables, this FE join is expensive.

**Optimization**: Restructure the DAX measure to allow SE to handle the groupby in a single xmSQL query:

```dax
-- Before: Causes FE JOIN
Revenue by Category =
SUMX(
    DimProduct,
    CALCULATE(SUM(FactSales[SalesAmount]))
)

-- After: SE can solve in one scan
Revenue by Category =
SUMMARIZECOLUMNS(
    DimProduct[Category],
    "Revenue", SUM(FactSales[SalesAmount])
)
```

---

## Column Store Internals

### Dictionary and Value Encoding

VertiPaq stores each column's data in two structures:

1. **Dictionary**: Maps each unique value to an integer ID
   - `{"North": 0, "South": 1, "East": 2, "West": 3}`
2. **Column data**: Stores the integer IDs (not the original values)
   - `[0, 0, 1, 2, 0, 3, 1, ...]`

**Compression quality depends on:**
- Dictionary size (unique value count = cardinality)
- Integer sequence compressibility (RLE for repeated values)

### Optimizing Column Compression

```
-- High cardinality, poor compression:
FactSales[OrderTimestamp]  -- millisecond precision, ~50M distinct values
FactSales[TransactionGUID] -- UUID, exactly 50M distinct values

-- Low cardinality, excellent compression:
FactSales[Status]          -- 5 distinct values ("Pending", "Complete", etc.)
FactSales[Region]          -- 4 distinct values
FactSales[Year]            -- 7 distinct values (2018-2024)
```

**Rule of thumb**: If a column's cardinality exceeds 1% of the table's row count, it's a compression candidate for removal or transformation.

### Segment Sort Order

VertiPaq processes data in segments of 8M rows. Segments with uniform values for the sorted column use RLE most effectively:

```
-- Good sort for a sales table filtered primarily by date:
Sort by DateKey before loading:
  Segment 1: DateKey 20200101 - 20200630 (all rows sorted by date)
  Segment 2: DateKey 20200701 - 20201231
  ...
  → Most date-filtered queries skip entire segments

-- Bad sort: random order
  Segment 1: Mix of all dates → must scan every segment for any date filter
```

**In Power Query**: Sort the fact table by the most-filtered column before the Load step:

```powerquery
SortedForStorage = Table.Sort(CleanedData, {{"DateKey", Order.Ascending}})
```

---

## DirectQuery Performance Optimization

### Measuring SQL Query Performance

When Performance Analyzer shows a slow DAX query on a DirectQuery table, the actual bottleneck is the SQL sent to the source.

**Step 1: Capture the SQL**

In DAX Studio with Server Timings enabled, the xmSQL section shows the SQL generated:

```sql
-- Generated SQL sent to Azure SQL Database:
SELECT [t0].[ProductKey], SUM([t0].[SalesAmount])
FROM [dbo].[FactSales] AS [t0]
WHERE [t0].[OrderDate] >= '2024-01-01' AND [t0].[OrderDate] < '2024-12-31'
  AND [t0].[Region] IN ('North', 'South')
GROUP BY [t0].[ProductKey]
```

**Step 2: Optimize in SSMS/Query Analyzer**

Take this SQL to the source database and analyze with Query Execution Plan:

```sql
-- Check if indexes are used:
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Run the query and check:
-- Physical reads > 0 → missing index
-- Estimated rows vs actual rows large discrepancy → stale statistics
```

**Step 3: Add indexes to the source**

```sql
-- Index for the most common filter combination:
CREATE NONCLUSTERED INDEX IX_FactSales_Date_Region
ON dbo.FactSales (OrderDate, Region)
INCLUDE (ProductKey, SalesAmount);
```

### DirectQuery Connection Settings

```powerquery
// Tune connection settings in Power Query
Source = Sql.Database(
    "server",
    "database",
    [
        CommandTimeout = #duration(0, 0, 10, 0),  // 10 min per query
        ConnectionTimeout = #duration(0, 0, 0, 30)
    ]
)
```

---

## Query Reduction and Cache Architecture

### VertiPaq Cache Layers

```
L1: Formula Engine Result Cache
    → Caches full query results per visual per filter context
    → Cleared on dataset refresh
    → First user sees slow query; subsequent users see cached result

L2: Storage Engine Datacache
    → Caches SE query results (xmSQL results)
    → Shared across different FE queries that generate the same SE query
    → Partially cleared on refresh

L3: Source Database Query Cache
    → Only for DirectQuery
    → Controlled by source (SQL Server plan cache, etc.)
```

### Maximizing Cache Hit Rate

```dax
-- Measures that generate the same SE query regardless of filter context
-- share the SE cache → higher hit rate

-- Consistent aggregation functions (SUM/COUNT) → cacheable
Good = SUM(FactSales[SalesAmount])

-- Volatile functions break caching:
Bad = SUM(FactSales[SalesAmount]) * (1 + RANDBETWEEN(-5, 5) / 100)
-- RANDBETWEEN generates different SE queries each call — never cached

-- NOW() in measures disables caching:
Bad = IF(MAX(FactSales[Date]) = TODAY(), "Current", "Historical")
-- Replace with a pre-computed column or a calculated table refresh flag
```

---

## Profiling with SQL Server Profiler (Analysis Services Events)

For on-premises Analysis Services or XMLA-connected Power BI models, use SQL Server Profiler to capture extended events:

```
Useful events to capture:
- ProgressReportBegin/End     → Track individual refresh operations
- QueryBegin/End              → Full DAX query with duration
- DirectQueryBegin/End        → SQL sent to source, with duration
- VertiPaqSEQueryBegin/End    → Individual SE scans
- QuerySubcube                → Sub-queries within a DAX measure
```

```xml
<!-- SQL Server Profiler trace definition for Power BI analysis -->
<TraceEventClass>
    <QueryBegin/>
    <QueryEnd/>
    <DirectQueryBegin/>
    <DirectQueryEnd/>
    <VertiPaqSEQueryBegin/>
    <VertiPaqSEQueryEnd/>
</TraceEventClass>
```

---

## Benchmark Framework

For systematic performance improvement, establish a repeatable benchmark:

```dax
-- Benchmark query in DAX Studio
// 1. Clear cache: CLEAR CACHE;
// 2. Run multiple times and compare FE vs SE times
// 3. Record before and after optimization

EVALUATE
    ROW(
        "Revenue", [Total Revenue],
        "YTD", [Revenue YTD],
        "SPLY", [Revenue SPLY],
        "Rank", [Product Revenue Rank],
        "Duration", "Run " & TEXT(NOW(), "HH:MM:SS")
    )
```

Document the optimization journey:

| Measure | Before (ms) | After (ms) | Change | Fix Applied |
|---|---|---|---|---|
| Product Revenue Rank | 8,400 | 320 | -96% | Calculated column for static rank |
| Revenue by Territory | 4,200 | 180 | -96% | Aggregation table |
| YoY Growth % | 750 | 120 | -84% | VAR to prevent double evaluation |
| Customer Cohort Count | 3,100 | 890 | -71% | SUMMARIZE pre-aggregation |

---

## Summary

- xmSQL SE queries that JOIN in the FE indicate an opportunity to restructure DAX for single-SE-query resolution
- **Column cardinality > 1% of row count** signals a compression candidate — remove or transform
- **Sort fact table by primary filter column** (DateKey) before loading to improve segment skipping
- DirectQuery performance lives in the source — index the source columns used in common filters
- **Volatile functions** (NOW, RANDBETWEEN) and complex conditions disable query caching
- Use **SQL Server Profiler with Analysis Services events** for on-premises deep profiling
- Maintain a **benchmark document** to measure and prove performance improvements

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
