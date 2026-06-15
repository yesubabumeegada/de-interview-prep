---
title: "Power BI Composite Models - Scenario Questions"
topic: power-bi
subtopic: composite-models
content_type: scenario_question
tags: [power-bi, composite-models, directquery, aggregations]
---

# Power BI Composite Models — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Choosing the Right Storage Mode

**Scenario:** You are building a Power BI report for a retail company. The dataset has the following tables:
- `FactSales` — 800 million rows, updated every 15 minutes in Azure SQL Database
- `DimProduct` — 60,000 rows, updated nightly
- `DimStore` — 2,000 rows, updated monthly
- `DimCalendar` — 3,650 rows (10 years), static

Users want summary dashboards that load in under 2 seconds AND need to see transactions from the last 15 minutes. Which storage mode would you assign to each table, and why?

<details>
<summary>💡 Hint</summary>

Think about which tables are small enough to import and which are too large. Also consider which table needs to join efficiently with both Import-mode and DirectQuery-mode tables.

</details>

<details>
<summary>✅ Solution</summary>

**Recommended storage mode assignment:**

| Table | Storage Mode | Reason |
|-------|-------------|--------|
| FactSales | DirectQuery | 800M rows — too large for Import; updates every 15 min require near-real-time data |
| DimProduct | Import | 60K rows — tiny; refreshed nightly so Import cache is always current enough |
| DimStore | Import | 2K rows — very small; monthly updates are perfectly suited for nightly Import refresh |
| DimCalendar | Dual | Small and static, but needs to join with both Import dimensions and the DirectQuery FactSales — Dual lets it participate efficiently in both contexts |

**Why Dual for DimCalendar and not just Import?**

If DimCalendar is Import and a user builds a visual using DimCalendar[Month] and SUM(FactSales[Amount]), the query spans an Import table and a DirectQuery table. Power BI must decide how to execute the join. With DimCalendar as Dual, the engine can push the calendar filter down to the DirectQuery source as a SQL WHERE clause, making the join happen at the database level. This is faster and avoids shipping large result sets between the database and the Power BI engine.

**What this composite model achieves:**
- Summary dashboards (Sales by Product, by Store, by Month) — Power BI uses the Import dimension tables for filtering, pushes efficient GROUP BY queries to Azure SQL. Fast.
- "Last 15 minutes" transactions — DirectQuery partition serves current data. Users accept 2–4 second load times for that specific visual.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Aggregation Table Not Being Used

**Scenario:** You built a composite model with `FactSales` in DirectQuery (2 billion rows in Snowflake) and `AggSales_Monthly` in Import (100K rows, grouped by ProductCategoryKey and MonthKey). Your executive dashboard has a bar chart showing Revenue by Product Category for the current year. Performance Analyzer shows **Direct query: 4,200ms** for that visual. The aggregation table exists but is not being used. What are the most likely causes and how do you diagnose and fix them?

<details>
<summary>💡 Hint</summary>

For an aggregation table to be used, every column referenced in the query must either be a "Group by" mapping or a measure mapping in the agg configuration. Think about what columns the bar chart is implicitly referencing.

</details>

<details>
<summary>✅ Solution</summary>

**Most likely causes:**

**1. The date filter uses a column not mapped in the aggregation.**

The visual is filtered to "current year." The filter likely comes from `DimCalendar[Year]`. If `DimCalendar` is Import and the agg maps `MonthKey` (an integer key like 202401), but the visual filter applies `DimCalendar[Year]` (a text or integer year value), Power BI may not be able to resolve that the filter translates through `MonthKey` to the agg table. 

**Fix:** Add a `YearKey` column to `AggSales_Monthly` and add a "Group by" mapping for it:

```m
// Add YearKey to the aggregation Power Query
AggSales_Monthly = Table.Group(
    FactSales,
    {"ProductCategoryKey", "MonthKey", "YearKey"},  // ← add YearKey
    {{"Revenue_Sum", each List.Sum([SalesAmount]), type number}}
)
```

Then in Manage Aggregations, add:

| Aggregation Column | Summarization | Detail Table | Detail Column |
|--------------------|--------------|--------------|---------------|
| YearKey            | Group by      | FactSales    | YearKey       |

**2. The ProductCategoryKey in the agg does not directly match the DimProduct column.**

If the visual uses `DimProduct[CategoryName]` (text) but the agg maps `ProductCategoryKey` (integer), Power BI cannot automatically resolve that `CategoryName` filters through `ProductCategoryKey`. 

**Fix:** Ensure the agg group-by column (`ProductCategoryKey`) is the same column that DimProduct uses as its key in the relationship with FactSales. Alternatively, add `CategoryName` directly to the agg table.

**3. DimCalendar is Import and FactSales is DirectQuery from a different source than the agg.**

**Fix:** Set DimCalendar to Dual mode so it participates correctly in agg routing.

**Diagnosis steps:**

```
1. Open Performance Analyzer → record → expand the bar chart result.
2. Click "Copy query" — this gives you the DAX sent to the engine.
3. Paste into DAX Studio with Server Timings enabled.
4. Look at the Storage Engine (SE) vs Formula Engine (FE) vs DirectQuery (DQ) split.
5. In DAX Studio, run the query with SET SERVER TIMINGS — the query plan shows which tables were accessed.
6. In Power BI Desktop: View → Performance Analyzer → select the visual → "View details" sometimes shows "Aggregations" tab indicating which agg was evaluated.
```

After adding YearKey to the agg and mapping it:

**Before fix:** Direct query: 4,200ms  
**After fix:** Direct query: 0ms, DAX query: 35ms (served fully from Import agg)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Composite Model for a Mixed Real-Time and Historical Analytics Platform

**Scenario:** You are the lead BI architect at a manufacturing company. The analytics requirements are:

- **Operations team:** needs live production line defect rates updated every 5 minutes (last 24 hours of data)
- **Quality team:** needs 3-year trend analysis of defect rates by product line, shift, and machine — must run in under 3 seconds
- **Executive team:** needs monthly and quarterly summaries — must run in under 1 second
- **Data source:** A SQL Server database with a `FactDefects` table containing 4 billion rows, plus dimension tables (DimProduct, DimMachine, DimShift, DimCalendar)
- **Constraint:** The SQL Server cannot handle more than 50 concurrent DirectQuery sessions

Design the complete composite model architecture, including storage modes, aggregation layers, and how you would handle the concurrent query constraint.

<details>
<summary>💡 Hint</summary>

Think about a three-layer aggregation strategy. Consider how hybrid tables handle the real-time + historical requirement. For the concurrency constraint, think about what percentage of queries need to actually hit DirectQuery vs. the aggregation layers.

</details>

<details>
<summary>✅ Solution</summary>

**Complete Architecture Design:**

**Layer 1 — Hybrid Table for FactDefects:**

```
FactDefects table:
├── Import partitions (3 years of history, refreshed nightly per month)
│   Last 24h partition: refreshed every 5 minutes via Enhanced Refresh API
│
└── DirectQuery partition (last 5 minutes only)
    Serves the operations real-time requirement
```

The "last 24 hours" partition is Import-mode but refreshed every 5 minutes via the REST API:

```powershell
# Scheduled every 5 minutes via Azure Function or Azure DevOps
$refreshBody = @{
    type = "automatic"
    commitMode = "transactional"
    objects = @(
        @{ table = "FactDefects"; partition = "FactDefects_Last24h" }
    )
} | ConvertTo-Json -Depth 4

Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/refreshes" `
    -Method POST -Headers $headers -Body $refreshBody
```

This means only the 5-minute-current partition uses DirectQuery. The rest of the last 24 hours is Import (fast, no DQ load).

**Layer 2 — Two Aggregation Tables:**

```
AggDefects_Daily (Import):
  Grain: ProductLineKey, MachineKey, ShiftKey, DateKey
  Rows:  ~3M (3 years × 1,095 days × multiple product/machine combos)
  Serves: Quality team 3-year trend analysis

AggDefects_Monthly (Import):
  Grain: ProductLineName, MonthKey, QuarterKey, YearKey
  Rows:  ~50K
  Serves: Executive monthly/quarterly summaries
```

**Layer 3 — Dimension Storage Modes:**

```
DimProduct   → Import  (50K rows)
DimMachine   → Import  (2K rows)
DimShift     → Import  (3 rows — day/evening/night)
DimCalendar  → Dual    (3,650 rows — joins with both Import agg and DQ partition)
```

**Query Routing Result:**

| User Group | Visual | Route | Latency |
|-----------|--------|-------|---------|
| Executive | Revenue by Quarter | AggDefects_Monthly (Import) | <200ms |
| Executive | Defect Rate by Product Line (monthly) | AggDefects_Monthly (Import) | <200ms |
| Quality | 3-year defect trend by Machine | AggDefects_Daily (Import) | <1s |
| Quality | Defect breakdown by Shift | AggDefects_Daily (Import) | <1s |
| Operations | Last 24h defect rate | FactDefects Import partition (5-min refresh) | <500ms |
| Operations | Last 5 minutes live | FactDefects DirectQuery partition | ~800ms |

**Addressing the 50 Concurrent DQ Session Constraint:**

With this architecture:
- ~95% of all report interactions hit Import layers (agg tables or Import partitions) — zero DQ sessions consumed.
- Only the operations team's "live last 5 minutes" visual fires DirectQuery.
- If 50 operations users each refresh the live visual simultaneously, that is exactly 50 DQ sessions — at the limit but manageable.
- **Mitigation:** Cache the DirectQuery partition result in the Power BI service using a 1-minute query cache (configurable in dataset settings for Premium). This means 50 users refreshing at the same minute reuse the same cached DQ result, consuming only 1 DQ session instead of 50.

```
Dataset Settings (Power BI Service):
  Query caching: ON
  Cache duration: 1 minute
```

**DAX Key Measures:**

```dax
// Works correctly across all partition types and agg levels
Defect Rate % =
DIVIDE(
    COUNTROWS(FILTER(FactDefects, FactDefects[IsDefective] = TRUE())),
    COUNTROWS(FactDefects),
    0
)

// For executive summary — designed to always hit AggDefects_Monthly
Monthly Defect Count =
CALCULATE(
    SUM(AggDefects_Monthly[DefectCount_Sum]),
    USERELATIONSHIP(DimCalendar[MonthKey], AggDefects_Monthly[MonthKey])
)
```

**Monitoring:**

Track DQ session utilization via the Power BI Admin API's activity log. Set an alert if concurrent DQ sessions exceed 40 (80% of limit) so the team can tune query caching or add Import refresh frequency before the limit is breached.

</details>

</article>
