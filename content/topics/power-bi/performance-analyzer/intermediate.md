---
title: "Performance Analyzer — Intermediate"
topic: power-bi
subtopic: performance-analyzer
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, performance-analyzer, interview, intermediate]
---

# Performance Analyzer — Intermediate

## DAX Studio Integration

DAX Studio is the essential companion tool for deep performance analysis. It connects to Power BI Desktop or the Power BI Service (via XMLA endpoint) and provides:

- **Query execution** with timing
- **Server Timings** tab: FE vs SE breakdown
- **Query Plan** tab: logical and physical execution plan
- **VertiPaq Analyzer**: column-level model statistics
- **All Queries** trace: captures every DAX query sent while you interact with the report

### Connecting DAX Studio to Power BI Desktop

1. Open DAX Studio (free from daxstudio.org)
2. File → Connect → **Power BI Desktop**
3. Select the running Power BI Desktop instance
4. DAX Studio is now connected to the in-memory model

---

## Server Timings in DAX Studio

Enable Server Timings before running a query:

```
Options → Server Timings (checkbox)
→ Run your query
→ Click the "Server Timings" tab at the bottom
```

### What Server Timings Shows

```
Query:           EVALUATE SUMMARIZECOLUMNS(DimProduct[Category], "Revenue", [Total Revenue])

Total Duration:    2,180ms
  FE Duration:     1,940ms   (89% — bottleneck!)
  SE Duration:        240ms   (11%)

SE Calls:              8
SE Cache Hits:         3  (37% hit rate)

Storage Engine Queries:
  #1: 45ms  — Scan DimProduct (Category, ProductKey)
  #2: 180ms — Scan FactSales (ProductKey, SalesAmount)
  #3: 15ms  — Dictionary lookup
```

### Interpreting the Breakdown

| FE% | SE% | Interpretation |
|---|---|---|
| < 20% | > 80% | Well-optimized; SE doing most work |
| 20-50% | 50-80% | Acceptable; room for improvement |
| > 50% | < 50% | FE bottleneck; review DAX for iterators |
| > 90% | < 10% | Severe FE issue; likely FILTER/RANKX/nested iterators |

### SE Cache Hit Rate

- **High hit rate (>80%)**: Most SE queries served from cache — fast
- **Low hit rate (<50%)**: Cache cold or highly filtered — queries going to disk/memory fresh

---

## Vertipaq Analyzer

VertiPaq Analyzer (built into DAX Studio) shows the physical structure of your model:

**Access**: Advanced → VertiPaq Analyzer → Refresh

### Key Metrics to Review

| Metric | What to Look For |
|---|---|
| Table size (MB) | Large tables → check for unnecessary columns |
| Column cardinality | High cardinality (millions of distinct values) → poor compression |
| Dictionary size | Large dictionaries → high-cardinality string columns |
| Segment count | Many small segments → fragmented storage |
| Relationship size | Large → high-cardinality join keys |

### Sample VertiPaq Analyzer Output

```
Table: FactSales
  Total size: 850 MB

  Columns:
  ┌────────────────────┬───────────────┬────────────────┬─────────────┐
  │ Column             │ Cardinality   │ Size (KB)      │ Encoding    │
  ├────────────────────┼───────────────┼────────────────┼─────────────┤
  │ SalesKey           │ 50,000,000    │ 180,000 KB     │ Hash        │  ← High cardinality!
  │ OrderDateKey       │ 1,826         │   3,200 KB     │ Value       │  ✅ Good
  │ ProductKey         │ 50,000        │   9,000 KB     │ Hash        │
  │ CustomerKey        │ 2,000,000     │  28,000 KB     │ Hash        │
  │ SalesAmount        │ 5,200         │   4,100 KB     │ Value       │  ✅ Good
  │ TransactionGUID    │ 50,000,000    │ 420,000 KB     │ Hash        │  ← Remove this!
  └────────────────────┴───────────────┴────────────────┴─────────────┘
```

**Action**: Remove `TransactionGUID` (high cardinality, likely unused) — would save 420 MB.

---

## Query Plan Tab

The Query Plan tab in DAX Studio shows the logical and physical execution plan.

### Logical Query Plan

The logical plan shows the high-level structure of what the DAX engine intends to do.

```
AddColumns
  Summarize(DimProduct, Category)
    Filter(FactSales, ...)
      ScanTable(FactSales)
```

### Physical Query Plan

The physical plan shows how the FE orchestrates SE calls:

```
ProjectionSpool (line 1, col 1, "Revenue")
  GroupSemiJoin (line 1)
    GroupBy (DimProduct[Category])
      SpoolLookupPhysOp (line 2)
        LookupPhysOp: DimProduct[ProductKey -> Category]
    Spool (line 3)
      ScanTable: FactSales
```

**Key nodes to watch:**
- `CrossApply` — row-by-row join (FE-heavy)
- `LookupPhysOp` — per-row lookup (avoid in large iterators)
- `SpoolLookupPhysOp` — spooling with lookup (acceptable if SE-backed)

---

## All Queries Trace (Capturing Live Report Queries)

DAX Studio can capture all DAX queries while you interact with the report in Power BI Desktop.

### Setup

1. DAX Studio → All Queries trace
2. Interact with the report (click slicers, switch pages)
3. DAX Studio records every query automatically
4. Sort by Duration to find the slowest queries

```
All Queries captured:
────────────────────────────────────────────────────────────────
Duration  │ Visual                    │ Query
────────────────────────────────────────────────────────────────
3,450ms   │ Line Chart - Revenue Trend│ EVALUATE SUMMARIZECOLUMNS(...)
2,180ms   │ Matrix - Sales by Product │ EVALUATE SUMMARIZECOLUMNS(...)
1,200ms   │ Card - Total Revenue      │ EVALUATE ROW("Revenue", [Total Revenue])
  850ms   │ Slicer - Category         │ EVALUATE DISTINCT(DimProduct[Category])
   45ms   │ Card - Date Range         │ EVALUATE ROW(...)
────────────────────────────────────────────────────────────────
```

Focus optimization effort on the slowest queries.

---

## Performance Tuning for Aggregations

### Adding Aggregation Tables in the Model

When DirectQuery queries are consistently slow (2+ seconds), add Import-mode aggregation tables:

```
DimDate    ──┐
DimProduct ──┼──→ FactSales (DQ, 100M rows)
DimCustomer ─┘
               ↓
             AggSalesByMonth (Import, 2,000 rows)  [DateKey, ProductKey, SUM(SalesAmount)]
```

After configuring the aggregation mapping:
- Report filtered at month level → hits AggSalesByMonth (Import, instant)
- Report filtered at day/product detail → falls through to FactSales (DQ, slower)

### Measuring the Impact

```
Before agg table:
  Monthly Revenue Chart → DAX query: 4,200ms (DirectQuery)

After agg table:
  Monthly Revenue Chart → DAX query: 45ms (hits Import agg)
  Daily Revenue Drill   → DAX query: 3,800ms (fallback to DQ)
```

---

## Report-Level Performance Best Practices

### Visual Count

| Visuals per Page | Expected Page Load |
|---|---|
| < 8 | < 2 seconds |
| 8-15 | 2-5 seconds |
| 15-30 | 5-15 seconds |
| > 30 | 15+ seconds (redesign needed) |

### Common Report-Level Fixes

```
1. Reduce visual count per page (use bookmarks/drill-through instead)
2. Disable bidirectional cross-filtering (reduces query fan-out)
3. Use "Reduce number of queries" setting (Query Reduction options)
4. Apply page-level filters to reduce data before it reaches visuals
5. Pre-calculate expensive measures as calculated columns (if static)
6. Use summary/aggregated visuals for overviews, drill-through for details
```

---

## Summary

- Use **Server Timings** in DAX Studio to measure FE vs SE work ratio
- **VertiPaq Analyzer** reveals column cardinality and model size opportunities
- **All Queries trace** captures every query during live report interaction
- Add **aggregation tables** for consistently slow DirectQuery visuals
- Limit **visuals per page** — each visual triggers a separate DAX query
- High FE % means slow DAX patterns; optimize FILTER, RANKX, nested iterators
- **Query Reduction settings** reduce re-queries triggered by slicer interactions
