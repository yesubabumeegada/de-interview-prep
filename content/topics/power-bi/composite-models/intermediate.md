---
title: "Power BI Composite Models - Intermediate"
topic: power-bi
subtopic: composite-models
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, composite-models, directquery, aggregations]
---

# Power BI Composite Models — Intermediate

## Aggregation Tables

Aggregation tables are pre-summarized Import-mode tables that Power BI uses to answer queries **without hitting the DirectQuery source**, dramatically improving performance.

### How Aggregations Work

```
User drags [Year] and [SalesAmount] onto a chart
                    │
                    ▼
Power BI query engine checks:
  Can this be answered from the aggregation table (Import)?
                    │
        ┌───────────┴───────────┐
       Yes                      No
        │                       │
        ▼                       ▼
  Return result            Fall through to
  from Import agg          DirectQuery source
  (milliseconds)           (seconds)
```

### Creating a User-Defined Aggregation Table

```m
// In Power Query: create AggSales from the FactSales DirectQuery table
let
    Source   = FactSales,                                  // DirectQuery table
    Grouped  = Table.Group(Source, {"ProductKey","DateKey"}, {
        {"SalesAmount_Sum", each List.Sum([SalesAmount]), type number},
        {"OrderQty_Sum",    each List.Sum([OrderQty]),    type number},
        {"RowCount",        each Table.RowCount(_),       type number}
    })
in
    Grouped
```

Set `AggSales` storage mode to **Import**. Then define the aggregation mapping:

**Home → Manage aggregations (on AggSales table):**

| Aggregation Column | Summarization | Detail Table | Detail Column |
|--------------------|--------------|--------------|---------------|
| SalesAmount_Sum    | Sum           | FactSales    | SalesAmount   |
| OrderQty_Sum       | Sum           | FactSales    | OrderQty      |
| RowCount           | Count rows    | FactSales    | —             |
| ProductKey         | Group by      | FactSales    | ProductKey    |
| DateKey            | Group by      | FactSales    | DateKey       |

**Result:** Any visual that groups by Product or Date and sums SalesAmount hits the Import agg table. Only visuals that drill to individual rows (or use non-aggregated columns) fall through to DirectQuery.

---

## Automatic Aggregations

For Power BI Premium, enable **Automatic aggregations** (AI-driven):

1. In the dataset settings in the Power BI service, turn on **Automatic aggregations**.
2. Power BI monitors query patterns and automatically builds and maintains aggregation tables.
3. No manual agg table creation required.

**When to use manual vs. automatic:**
- **Manual** — full control, predictable query routing, works in all licenses.
- **Automatic** — less maintenance, adapts to changing query patterns, requires Premium.

---

## DirectQuery on Power BI Datasets (Chaining)

Introduced in 2021, this allows a report or dataset to connect to another **published Power BI dataset** as a DirectQuery source, enabling shared semantic layer patterns.

```
Central Certified Dataset (Finance Semantic Model)
        │
        │ DirectQuery connection
        ├── Departmental Dataset A (adds HR-specific measures)
        └── Departmental Dataset B (adds Marketing dimensions)
```

**Key rules:**
- The downstream dataset is in DirectQuery with respect to the upstream dataset.
- You can add new measures and calculated columns on top of the upstream model.
- You **cannot** modify existing measures/columns from the upstream model.
- This is called a **"live connection with local model"** or **"DirectQuery to Power BI datasets"**.

```
// In Desktop: File → Options → Preview features → DirectQuery for Power BI datasets
// Then: Get data → Power BI datasets → select certified dataset → DirectQuery
// After connecting, you can add local measures:

Revenue per Employee =
DIVIDE(
    [Total Revenue],    // from upstream Finance model
    [Headcount],        // local measure in downstream HR dataset
    0
)
```

---

## Field Parameters

Field parameters let report users dynamically switch which field/measure appears in a visual — a lightweight way to make reports self-service without creating dozens of visuals.

```dax
// Create a field parameter: Modeling → New parameter → Fields
Metric Selector = {
    ("Total Sales",    NAMEOF('FactSales'[SalesAmount]),     0),
    ("Total Cost",     NAMEOF('FactSales'[CostAmount]),      1),
    ("Gross Margin",   NAMEOF(Measures[Gross Margin]),       2),
    ("Order Count",    NAMEOF('FactSales'[OrderID]),         3)
}
```

Power BI generates a disconnected table and a parameter measure. Add a slicer on the field parameter, and visuals that reference `[Metric Selector]` dynamically switch between Sales, Cost, Margin, or Order Count based on the user's selection.

---

## Relationships in Composite Models

Composite models add relationship complexity because you can have:

- Import ↔ Import (standard, all cardinalities supported)
- Import ↔ DirectQuery (supported, but the Import side is typically the "one" side)
- DirectQuery ↔ DirectQuery (same source only — cross-source DQ-to-DQ requires one side as Import/Dual)
- Dual ↔ Import or DirectQuery (most flexible, recommended for shared dimensions)

### Limited Relationships

When a composite model has tables from **different DirectQuery sources**, relationships between them become **limited relationships**:

- They do not guarantee referential integrity.
- Blank rows may appear on the "one" side to accommodate unmatched rows from the "many" side.
- The relationship icon shows a small dot indicating it is limited.

**Implication:** DAX functions like `RELATED()` may return unexpected results across limited relationships. Test carefully.

---

## Performance Analyzer for Composite Models

Use the **Performance Analyzer** (View → Performance Analyzer) to distinguish Import vs. DirectQuery query costs:

```
Visual: Sales by Product and Year
├── DAX query: 45ms            ← hit the Import aggregation table
└── Direct query: 0ms          ← no DQ fallback needed

Visual: Sales by Individual Order (drill-through)
├── DAX query: 12ms
└── Direct query: 2,340ms      ← fell through to DQ source (expected for row-level data)
```

If you see unexpectedly high **Direct query** times on visuals that should be served from aggregations, the agg mapping is incorrect or incomplete — re-check the Manage Aggregations configuration.

---

## Common Mid-Level Interview Questions

**Q: What is the purpose of an aggregation table in a composite model?**  
A: An aggregation table is a pre-summarized, Import-mode table that Power BI queries instead of the underlying DirectQuery table when the query can be answered at a higher grain (e.g., daily or monthly totals). This gives Import-speed performance for summary queries while retaining the ability to drill to row-level detail via DirectQuery.

**Q: What is DirectQuery on Power BI datasets, and when would you use it?**  
A: It allows one Power BI dataset to consume another published dataset as a DirectQuery source. You use it to build a shared certified semantic layer (owned by the data team) that departmental teams extend with their own local measures — without duplicating the core model.

**Q: What is a limited relationship?**  
A: A relationship in a composite model between tables from different DirectQuery sources, or where referential integrity cannot be guaranteed. Limited relationships may surface blank rows and can affect DAX calculation correctness. They appear with a dotted line in the model view.
