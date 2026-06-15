---
title: "Power BI Composite Models - Real World"
topic: power-bi
subtopic: composite-models
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [power-bi, composite-models, directquery, aggregations]
---

# Power BI Composite Models — Real-World Patterns

## Real-World Pattern 1: Enterprise Sales Analytics with 5 Billion Rows

**Context:** A retail company has a FactSales table with 5 billion rows in Azure Synapse Analytics. Business users need both real-time daily dashboards (for operations) and historical trend analysis (for strategy).

**Solution: Composite Model with Aggregation Layers**

```
Model Architecture:
──────────────────
FactSales_DQ           → DirectQuery on Synapse (5B rows, real-time)
AggSales_Daily         → Import, group by [ProductKey, StoreKey, DateKey]      (~2M rows)
AggSales_Monthly       → Import, group by [CategoryKey, RegionKey, MonthKey]   (~50K rows)

DimProduct             → Import (80K rows, refreshed nightly)
DimStore               → Import (15K rows)
DimCalendar            → Dual  (3,650 rows — works with both Import and DQ)
DimRegion              → Import (500 rows)
```

**Aggregation mappings for AggSales_Monthly:**

```
Measure         → Summarization  → Detail Table   → Detail Column
SalesAmt_Sum    → Sum            → FactSales_DQ   → SalesAmount
Cost_Sum        → Sum            → FactSales_DQ   → CostAmount
TxnCount        → Count rows     → FactSales_DQ   → —
CategoryKey     → Group by       → FactSales_DQ   → CategoryKey (via DimProduct)
RegionKey       → Group by       → FactSales_DQ   → RegionKey   (via DimStore)
MonthKey        → Group by       → FactSales_DQ   → DateKey     (mapped to month)
```

**Resulting query routing:**

| Visual | Data Source Hit | Typical Duration |
|--------|----------------|-----------------|
| Revenue by Region (annual) | AggSales_Monthly (Import) | 20ms |
| Monthly trend by Category | AggSales_Monthly (Import) | 30ms |
| Daily operations dashboard | AggSales_Daily (Import) | 80ms |
| Today's live transactions | FactSales_DQ (DirectQuery) | 800ms |
| Individual transaction drillthrough | FactSales_DQ (DirectQuery) | 1,200ms |

Executive dashboards and trend reports run at Import speed. Only operations dashboards that need intraday data hit DirectQuery — and those users expect slight latency.

---

## Real-World Pattern 2: Shared Semantic Layer with DirectQuery Chaining

**Context:** A large organization wants a single certified Finance dataset owned by the Data Engineering team, with departmental BI teams adding their own metrics on top without duplicating the core model.

**Architecture:**

```
Certified Finance Dataset (Power BI Premium workspace: "Finance - Certified")
│  Tables: FactGL, DimCostCenter, DimAccount, DimCalendar
│  Core measures: [Total Revenue], [Total Expenses], [Net Income]
│  Mode: Import, refreshed nightly
│
├── FP&A Dataset (Finance Planning workspace)
│   │  Connects via DirectQuery to Certified Finance Dataset
│   │  Adds: [Budget Variance], [Forecast Accuracy], DimBudget (Import)
│   └── Used by: FP&A team reports
│
└── Operations Finance Dataset (Ops workspace)
    │  Connects via DirectQuery to Certified Finance Dataset
    │  Adds: [Cost per Unit], [OPEX Ratio], DimDepartment (Import)
    └── Used by: Operations leadership reports
```

**In FP&A Dataset — adding a local measure that references the upstream certified dataset:**

```dax
// Local measure in FP&A Dataset
Budget Variance =
VAR ActualExpenses = [Total Expenses]    -- from upstream Certified Finance Dataset
VAR BudgetedExpenses = SUM(DimBudget[BudgetAmount])  -- from local Import table
RETURN
    ActualExpenses - BudgetedExpenses

Budget Variance % =
DIVIDE(
    [Budget Variance],
    SUM(DimBudget[BudgetAmount]),
    0
)
```

**Benefits:**
- Data Engineering maintains one certified model — single source of truth for revenue and expense figures.
- Departmental teams cannot accidentally modify core definitions.
- If the Finance team fixes a measure in the certified dataset, all downstream departmental datasets automatically inherit the fix.
- No data duplication — departmental datasets connect live to the certified dataset.

---

## Real-World Pattern 3: Hybrid Table for Operational Reporting

**Context:** A logistics company needs a dashboard showing shipment status updates. Shipments update every few minutes. The team cannot afford a 2-hour refresh lag, but the full 3-year history table has 800M rows and cannot be fully DirectQueried without overwhelming the source.

**Solution: Hybrid Table**

```
FactShipments table:
├── Import partitions (3 years of history, ~798M rows)
│   Refreshed: nightly at 1am for yesterday's completed shipments
│   VertiPaq compressed to ~4GB
│
└── DirectQuery partition (today's shipments, ~500K rows growing)
    Queries Synapse live on each visual interaction
    No refresh needed — always current
```

**Power Query setup (incremental refresh with real-time):**

```m
// RangeStart and RangeEnd parameters are injected by Power BI
let
    Source = Sql.Database(ServerName, "LogisticsDB"),
    Shipments = Source{[Schema="dbo",Item="FactShipments"]}[Data],
    Filtered = Table.SelectRows(Shipments,
        each [ShipDate] >= RangeStart and [ShipDate] < RangeEnd)
in
    Filtered
```

**Incremental refresh policy:**
- Archive: data older than 3 years
- Incrementally refresh: last 1 day
- Real-time DirectQuery: enabled (today's partition stays DQ)

**DAX measure that spans both partitions transparently:**

```dax
// This measure works correctly across both Import and DQ partitions
On-Time Delivery % =
DIVIDE(
    COUNTROWS(FILTER(FactShipments, FactShipments[DeliveredOnTime] = TRUE())),
    COUNTROWS(FactShipments),
    0
)
```

Power BI automatically routes the historical part of this calculation to Import partitions and today's portion to the DirectQuery partition, then combines the results — transparent to both the developer and the end user.

---

## Real-World Pattern 4: Field Parameters for Self-Service Analytics

**Context:** A sales team wants a single report where they can toggle between Revenue, Gross Profit, Units Sold, and Customer Count on any visual — without the BI team creating four separate reports.

```dax
// Field parameter table (auto-generated by Power BI but can be written manually)
KPI Selector = {
    ("Revenue",          NAMEOF(Measures[Total Revenue]),          0),
    ("Gross Profit",     NAMEOF(Measures[Gross Profit]),           1),
    ("Units Sold",       NAMEOF(FactSales[OrderQty]),              2),
    ("Customer Count",   NAMEOF(Measures[Distinct Customer Count]),3)
}

// Supporting measure (auto-generated)
KPI Selector Fields = SELECTEDVALUE('KPI Selector'[KPI Selector Order])
```

**Report setup:**
1. Add a slicer with `KPI Selector[KPI Selector]` — shows Radio button list: Revenue / Gross Profit / Units Sold / Customer Count.
2. In a bar chart, set the value to `KPI Selector[KPI Selector Fields]`.
3. The chart dynamically switches between all four measures based on slicer selection.
4. Works with any visual type — line charts, cards, tables.

**In a composite model context:** Field parameters work with both Import and DirectQuery measures. If the user selects a DirectQuery-backed measure, the visual fires a DirectQuery; if they select an Import-backed measure, it runs at Import speed. The switching is transparent.

---

## Common Pitfalls and Solutions

| Pitfall | Symptom | Solution |
|---------|---------|---------|
| Agg table not being hit | Performance Analyzer shows high DQ times even for summary queries | Re-check agg mappings; all group-by columns must map exactly to the detail table's columns |
| Dual table queried via DQ unnecessarily | Dual dimension table causes DQ calls when used with DQ fact | Verify the relationship chain — a Dual table reverts to DQ if any table in the same query is DQ and from the same source |
| M:M on DQ tables slows dashboard | Complex SQL with subqueries generated | Redesign to M:1 using a bridge Import table |
| Calculated column on DQ table | Column shows blank or errors | Calculated columns on DQ tables are computed at report render time — very expensive. Move to Import table or use a measure instead |
| DAX time intelligence fails on DQ fact | SAMEPERIODLASTYEAR returns blank | Ensure the date table is Import or Dual, not DQ. The date table must be contiguous with no gaps |
| DirectQuery on PBI dataset causes circular dependency | Dataset fails to load | DQ chaining cannot be circular — A→B→A is invalid. Re-architect the shared layer |
