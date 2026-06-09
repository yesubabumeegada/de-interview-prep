---
title: "Data Modeling — Scenarios"
topic: power-bi
subtopic: data-modeling
content_type: scenario_question
tags: [power-bi, data-modeling, scenarios, interview]
---

# Data Modeling — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Why Is My Sales Total Double-Counting?

**Scenario:** You have two tables in Power BI: `Sales` and `Returns`. You create a relationship from `Sales[OrderID]` to `Returns[OrderID]` and set cross-filter direction to **Both**. Now your total sales figure is double what you expect. Why is this happening, and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about how bidirectional filtering works and whether a direct relationship between two fact tables is a good idea. Also consider the role of filter direction.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause**: You've created a direct relationship between two fact tables with bidirectional cross-filtering. This creates an ambiguous filter path and can cause unexpected row duplication or double-counting because the engine is applying filters in both directions simultaneously.

**The Fix:**

1. **Remove the direct fact-to-fact relationship** — fact tables should never relate directly.
2. Instead, **create a shared dimension** (e.g., `DimOrder`) that both `FactSales` and `FactReturns` relate to.
3. Change cross-filter direction to **Single** by default.

**Correct Model:**
```
DimOrder (1) ──→ FactSales (*)
DimOrder (1) ──→ FactReturns (*)
```

Now you can calculate net revenue correctly:
```dax
Net Revenue =
SUM(FactSales[SalesAmount]) - SUM(FactReturns[ReturnAmount])
```

Both measures use `DimOrder` as a bridge, avoiding double-counting.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Many-to-Many Product Tags

**Scenario:** You are building a product catalog where each product can have multiple tags (e.g., "Eco-Friendly", "Sale", "New Arrival") and each tag can apply to many products. How would you model this in Power BI, and how would you write a measure to count products by tag?

<details>
<summary>💡 Hint</summary>

This is a classic many-to-many scenario. Think about a bridge table and how filter context flows through it.

</details>

<details>
<summary>✅ Solution</summary>

**Model Design:**

Use a bridge table to resolve the many-to-many relationship:

```
DimProduct (1) ──→ BridgeProductTag (*) ←── (1) DimTag
```

**Table Structures:**

```
DimProduct: ProductKey, ProductName, Category
DimTag: TagKey, TagName
BridgeProductTag: ProductKey, TagKey
```

**Relationship Setup:**
- `DimProduct[ProductKey]` → `BridgeProductTag[ProductKey]` (1:*, single direction)
- `DimTag[TagKey]` → `BridgeProductTag[TagKey]` (1:*, single direction)

**DAX Measure — Products per Tag:**

```dax
Products per Tag =
CALCULATE(
    DISTINCTCOUNT(BridgeProductTag[ProductKey])
)
```

When a user slices by `DimTag[TagName] = "Eco-Friendly"`, the filter flows through `BridgeProductTag` and counts distinct products with that tag.

**Products with At Least One Tag:**
```dax
Tagged Products =
DISTINCTCOUNT(BridgeProductTag[ProductKey])
```

**Why not use direct many-to-many?**
- Native M:M in Power BI can produce blank rows in visuals
- Bridge table is more explicit, predictable, and testable

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Composite Model for a 50-Billion-Row Fact Table

**Scenario:** Your company has a 50-billion-row IoT sensor fact table in Azure Synapse Analytics. Analysts need dashboards that slice by device, location, and time. Direct import is impossible; DirectQuery alone is too slow for interactive dashboards. How do you architect the Power BI model?

<details>
<summary>💡 Hint</summary>

Think about composite models, aggregation tables, storage modes (Import vs DirectQuery vs Dual), and how to tier queries so common aggregations hit cached data while drill-through hits the source.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture: Tiered Composite Model**

```
Synapse (DirectQuery)       Power BI (Import)
├── FactSensorRaw (50B)     ├── AggByHourDevice (Import)
├── DimDevice               ├── AggByDayLocation (Import)
└── DimLocation             ├── DimDeviceType (Import)
                            └── DimDate (Import)
```

**Step 1: Define Aggregation Tables in Synapse**

```sql
-- AggByHourDevice: ~50M rows (hourly per device)
CREATE TABLE AggByHourDevice AS
SELECT
    CAST(EventTimestamp AS DATE) AS EventDate,
    DATEPART(HOUR, EventTimestamp) AS EventHour,
    DeviceID,
    AVG(SensorValue) AS AvgSensorValue,
    MAX(SensorValue) AS MaxSensorValue,
    COUNT(*) AS ReadingCount
FROM FactSensorRaw
GROUP BY CAST(EventTimestamp AS DATE), DATEPART(HOUR, EventTimestamp), DeviceID;

-- AggByDayLocation: ~5M rows (daily per location)
CREATE TABLE AggByDayLocation AS
SELECT
    CAST(EventTimestamp AS DATE) AS EventDate,
    LocationID,
    AVG(SensorValue) AS AvgSensorValue,
    COUNT(DISTINCT DeviceID) AS ActiveDevices
FROM FactSensorRaw r JOIN DimDevice d ON r.DeviceID = d.DeviceID
GROUP BY CAST(EventTimestamp AS DATE), d.LocationID;
```

**Step 2: Power BI Storage Mode Configuration**

```
FactSensorRaw   → DirectQuery  (fallback for drill-through)
AggByHourDevice → Import       (50M rows, acceptable)
AggByDayLocation→ Import       (5M rows, fast)
DimDevice       → Dual         (bridges Import and DQ)
DimLocation     → Import       (small)
DimDate         → Import       (small)
```

**Step 3: Configure Aggregations in Power BI**

Right-click `AggByHourDevice` → Manage Aggregations:

| Aggregation Column | Summarization | Detail Table | Detail Column |
|---|---|---|---|
| AvgSensorValue | Average | FactSensorRaw | SensorValue |
| MaxSensorValue | Max | FactSensorRaw | SensorValue |
| ReadingCount | Sum | FactSensorRaw | (count) |
| EventDate | GroupBy | FactSensorRaw | EventDate |
| EventHour | GroupBy | FactSensorRaw | EventHour |
| DeviceID | GroupBy | FactSensorRaw | DeviceID |

**Step 4: DAX Measures (transparent to agg layer)**

```dax
Avg Sensor Reading =
AVERAGE(FactSensorRaw[SensorValue])
-- Power BI automatically routes this to AggByHourDevice for hourly/device slices
-- Falls through to FactSensorRaw only for non-aggregable queries

Alert Rate =
DIVIDE(
    CALCULATE(COUNTROWS(FactSensorRaw), FactSensorRaw[SensorValue] > 95),
    COUNTROWS(FactSensorRaw)
)
-- This filter condition prevents aggregation hit; routes to DirectQuery
-- Consider pre-computing alert flags in the agg table if needed
```

**Result:** 95% of dashboard queries hit Import agg tables (sub-second), while drill-through to raw data hits Synapse only when needed.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a calculated column and a measure?" — Calculated columns are computed at refresh time for each row and stored in the model (using RAM). Measures are computed at query time using filter context and not stored. Use measures for aggregations, calculated columns only when you need to slice or filter by the result.

> **Tip 2:** "Why should you avoid bidirectional relationships?" — Bidirectional filtering can create ambiguous filter paths in complex models and produce unexpected cross-filtering behavior. Always default to single direction and use explicit CROSSFILTER() in DAX when you need bidirectional behavior in a specific measure.

> **Tip 3:** "How would you handle a fact table with 1 billion rows in Power BI?" — Use a composite model: keep the fact table in DirectQuery mode against a cloud data warehouse (Synapse, BigQuery, Snowflake), import dimensions and aggregation tables, configure aggregation tables in Power BI Desktop, and set dimension storage mode to Dual so they serve both Import and DirectQuery queries.
