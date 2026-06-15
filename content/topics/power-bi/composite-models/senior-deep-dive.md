---
title: "Power BI Composite Models - Senior Deep Dive"
topic: power-bi
subtopic: composite-models
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [power-bi, composite-models, directquery, aggregations]
---

# Power BI Composite Models — Senior Deep Dive

## Hybrid Tables: Combining Incremental Refresh with Real-Time Partition

A **Hybrid Table** is a single table that has:
- **Historical partitions** in Import mode (compressed, fast for historical queries)
- **A real-time (current period) partition** in DirectQuery mode (always current, no refresh lag)

This is the most advanced storage pattern in Power BI — it eliminates the tradeoff between performance and freshness for time-series fact tables.

### Setting Up a Hybrid Table

Requires Power BI Premium. Configured via incremental refresh policy with **"Get the latest data in real time with DirectQuery"** enabled.

```
// In Power Query: define RangeStart and RangeEnd parameters (DateTime type)
RangeStart = #datetime(2024, 1, 1, 0, 0, 0)
RangeEnd   = #datetime(2024, 12, 31, 23, 59, 59)

// Filter expression applied to the date column
Source = Sql.Database("prod-sql", "FinanceDB"),
Orders = Source{[Schema="dbo",Item="FactOrders"]}[Data],
Filtered = Table.SelectRows(Orders,
    each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd)
```

**Policy settings:**
- Archive data starting: 3 years before today
- Incrementally refresh data: last 1 day
- **Get the latest data in real time with DirectQuery: ON**

**Result:** Power BI creates Import partitions for data older than 1 day (refreshed nightly) and a DirectQuery partition for today's data. Users always see the most current intraday data with zero refresh lag, while historical queries run at full Import speed.

### Partition View (Tabular Editor or XMLA)

```json
// Simplified partition structure after policy application
{
  "partitions": [
    { "name": "FactOrders_2022", "mode": "import", "source": "...where Year=2022" },
    { "name": "FactOrders_2023", "mode": "import", "source": "...where Year=2023" },
    { "name": "FactOrders_2024-Jan", "mode": "import", "source": "...where Month=2024-01" },
    // ... more monthly Import partitions ...
    { "name": "FactOrders_DirectQuery", "mode": "directQuery", "source": "...where OrderDate >= today" }
  ]
}
```

---

## DAX Behavior Differences in DirectQuery Mode

Understanding which DAX patterns work in DirectQuery is critical for composite model design.

### DAX Functions with DirectQuery Limitations

| Function / Pattern | Import | DirectQuery | Notes |
|-------------------|--------|-------------|-------|
| `CALCULATE` with complex filters | ✅ | ⚠️ Limited | Some filter combinations not translatable to SQL |
| `SUMMARIZECOLUMNS` | ✅ | ✅ (restricted) | Used internally by visuals; some options blocked |
| `TOPN` | ✅ | ⚠️ | May be pushed to SQL or evaluated in engine |
| `EARLIER` | ✅ | ❌ | Row context iteration not supported in DQ |
| `RANKX` | ✅ | ❌ | Iterator not supported in DQ |
| `DISTINCTCOUNT` | ✅ | ✅ | Translated to COUNT(DISTINCT …) |
| `USERELATIONSHIP` | ✅ | ✅ | Supported |
| `CROSSFILTER` | ✅ | ⚠️ | Limited in DQ mode |
| Time Intelligence (SAMEPERIODLASTYEAR etc.) | ✅ | ⚠️ | Requires contiguous date table, may fail |

**Implication for composite model design:** Keep complex DAX calculations (iterators, time intelligence) on **Import-mode tables**. Use DirectQuery tables only for simple aggregations (`SUM`, `COUNT`, `AVERAGE`) where the engine can generate efficient SQL.

---

## Aggregation Precedence and Query Routing Logic

Power BI's query engine applies aggregations in this order:

1. **Check if an aggregation table can answer the query:** All grouping columns must map to aggregation "Group By" columns, and all measures must map to aggregation summarizations.
2. **If yes → serve from Import agg table.** No DirectQuery fired.
3. **If no → fall through to the DirectQuery source.**

### Multi-Level Aggregation Stacking

For very large datasets, stack multiple aggregation layers:

```
FactSales (DirectQuery, 10B rows)
    └── AggSales_Daily (Import, group by ProductKey + DateKey, ~50M rows)
            └── AggSales_Monthly (Import, group by ProductKey + YearMonthKey, ~500K rows)
                    └── AggSales_Annual (Import, group by CategoryKey + YearKey, ~5K rows)
```

Dashboard KPI cards (annual grain) → hit AggSales_Annual (microseconds).  
Monthly trend charts → hit AggSales_Monthly (milliseconds).  
Daily detail tables → hit AggSales_Daily (fast milliseconds).  
Individual order drillthrough → hits DirectQuery (seconds, acceptable for low-frequency action).

### Verifying Query Routing with Server Timing

In Power BI Desktop, enable **Server timing** via Performance Analyzer's context menu. This shows the exact DAX query sent to the engine and whether it was served from storage engine (Import) or formula engine with DQ.

```
// Storage engine result (Import agg hit):
SE Cache: 12ms, FE: 3ms, DQ: 0ms   ← ideal

// DirectQuery fallback:
SE: 0ms, FE: 5ms, DQ: 2,340ms      ← agg miss, check mapping
```

---

## Relationship Cardinality Deep Dive in Composite Models

### Many-to-Many Relationships

M:M relationships in composite models are supported but have performance implications in DirectQuery:

```
FactSales [CustomerKey] ←M:M→ DimCustomer [CustomerKey]
// (a customer can appear in multiple sales, a sale can have multiple customers — e.g., joint accounts)
```

In Import mode, M:M uses a bridge table internally.  
In DirectQuery, M:M generates a `GROUP BY + JOIN` that can be expensive. Prefer flattening to M:1 where possible.

### Bidirectional Cross-Filtering Caution

Enabling **bidirectional cross-filtering** on a DQ relationship causes the engine to generate correlated subqueries, which can degrade performance by 10-100x:

```
// Bidirectional: both directions filter
DimProduct ←→ FactSales

// Generated SQL (simplified):
SELECT p.Category, SUM(f.Amount)
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IN (
    SELECT DISTINCT ProductKey FROM FactSales  -- correlated subquery from bidir
    WHERE ...
)
GROUP BY p.Category
```

**Rule:** Always use single-direction filtering in DirectQuery relationships. Use explicit DAX measures with `CROSSFILTER()` only where necessary.

---

## When to Choose Composite vs. Pure Import vs. Pure DirectQuery

| Scenario | Recommended Mode |
|----------|-----------------|
| Dataset < 1GB, refreshes daily are acceptable | Pure Import |
| Always-current data required, dataset fits in budget | Hybrid Table (Import history + DQ real-time) |
| Source system cannot handle many concurrent queries | Pure Import (offload from source) |
| Huge dataset (100GB+), summary queries only | Composite: Import aggs + DQ fact |
| Regulated data that cannot leave source system | DirectQuery only |
| Multiple sources at different grains | Composite model mandatory |
| Dataset shared across many downstream reports | Certified Import dataset (shared semantic layer) |

---

## Performance Troubleshooting Checklist for Composite Models

1. **Run Performance Analyzer** — identify which visuals show high Direct query times.
2. **Check aggregation mappings** — ensure all measure and group-by columns are correctly mapped.
3. **Verify storage mode of dimension tables** — should be Import or Dual, not DirectQuery.
4. **Check for bidirectional relationships** — disable if not strictly necessary.
5. **Review DAX for iterators on DQ tables** — move `RANKX`, `EARLIER`, `ADDCOLUMNS` logic to Import-mode calculated tables.
6. **Check query folding in Power Query** — DQ tables must fold queries fully to the source; non-folding steps force in-engine processing.
7. **Inspect the model with DAX Studio** — use Server Timings to see exact SE vs. FE vs. DQ split.
8. **Look for M:M relationships on DQ tables** — convert to M:1 via bridge tables where possible.
