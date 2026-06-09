---
title: "Performance Analyzer — Scenarios"
topic: power-bi
subtopic: performance-analyzer
content_type: scenario_question
tags: [power-bi, performance-analyzer, scenarios, interview]
---

# Performance Analyzer — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Using Performance Analyzer for the First Time

**Scenario:** A report your team built shows the correct data, but users are complaining it takes over 30 seconds to load. Your manager asks you to investigate and report back with findings. How do you use Performance Analyzer to find the slow visuals?

<details>
<summary>💡 Hint</summary>

Think about the step-by-step process: where to find the tool, what to click, and what the output means. Focus on the three timing components each visual shows.

</details>

<details>
<summary>✅ Solution</summary>

**Step-by-step investigation:**

1. Open the report in **Power BI Desktop** (not Service — Performance Analyzer is a Desktop feature)
2. Go to the **View** tab → click **Performance Analyzer**
3. In the Performance Analyzer pane, click **Start Recording**
4. Click **Refresh Visuals** (this clears any cache and re-loads all visuals from scratch — gives worst-case timing)
5. Wait for all visuals to finish loading
6. Click **Stop**

**Reading the results:**

Each visual shows three lines:
- `DAX query` — how long the data calculation took
- `Visual display` — how long rendering took
- `Other` — network and framework overhead

**Example output:**

```
Revenue Line Chart
  DAX query:     18,400ms  ← This is the problem!
  Visual display:    240ms
  Other:              30ms

Product Matrix
  DAX query:      6,200ms  ← Also slow
  Visual display:   180ms

Date Slicer
  DAX query:        120ms  ← Fine
  Visual display:     30ms
```

**Report back to manager:**

"The Revenue Line Chart is responsible for 18 seconds of the 30-second load time. Its DAX query is the bottleneck — the measure calculations are slow. The Product Matrix adds another 6 seconds. The slicers load fast. I recommend optimizing the Revenue Chart measure first — that alone would cut load time by more than half."

**Next steps:**
- Click **Copy query** on the Revenue Line Chart to get the DAX query
- Paste into DAX Studio to analyze the FE vs SE breakdown

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Interpreting DAX Studio Server Timings

**Scenario:** Performance Analyzer shows a matrix visual with 4,200ms DAX query time. You copy the query to DAX Studio and run it with Server Timings enabled. The results show: Total 4,180ms, FE 3,950ms (94%), SE 230ms (6%), SE Queries: 48. What does this tell you, and what are the likely causes and fixes?

<details>
<summary>💡 Hint</summary>

Think about what high FE% and many SE queries indicate. Consider which DAX patterns cause the Formula Engine to do many iterations or row-by-row calculations.

</details>

<details>
<summary>✅ Solution</summary>

**Interpretation of the numbers:**

- **94% FE time**: Almost all work is in the Formula Engine (serial, single-threaded). The Storage Engine (parallel, fast) is only used 6% of the time.
- **48 SE queries**: For one visual, this is very high. Each SE query has overhead. Something in the DAX is generating a new SE scan for each slice of data.

**Likely causes (in order of probability):**

1. **RANKX over a large table**: `RANKX(ALL(DimProduct), [Total Revenue])` evaluates `[Total Revenue]` once per product in the FE. If DimProduct has 10,000 products, that's 10,000 context transitions, each generating SE sub-queries.

2. **FILTER iterating a large fact table**: `CALCULATE(SUM(...), FILTER(FactSales, FactSales[Amount] > threshold))` scans every row of FactSales in the FE instead of using a column filter.

3. **LOOKUPVALUE inside SUMX**: Row-by-row lookup inside an iterator forces FE to execute one lookup per row.

4. **Nested CALCULATE inside an iterator**: Context transition overhead multiplied by every row in the iterator table.

**Diagnosis steps:**

```
1. Look at the measure definitions used in the matrix
2. Check for RANKX, FILTER(LargeTable, ...), SUMX/AVERAGEX with complex expressions
3. In DAX Studio Query Plan tab: look for CrossApply, LookupPhysOp nodes
```

**Fixes based on the pattern:**

```dax
-- If FILTER is the issue: replace with column filter
-- Slow:
CALCULATE(SUM(Sales[Amount]), FILTER(Sales, Sales[Amount] > 1000))
-- Fast:
CALCULATE(SUM(Sales[Amount]), Sales[Amount] > 1000)

-- If RANKX is the issue: use a calculated column for static ranks
-- or limit the ranked table to only visible items

-- If LOOKUPVALUE inside SUMX: replace with RELATED
-- Slow:
SUMX(FactSales, FactSales[Qty] * LOOKUPVALUE(DimProduct[Price], DimProduct[ProductKey], FactSales[ProductKey]))
-- Fast:
SUMX(FactSales, FactSales[Qty] * RELATED(DimProduct[Price]))
```

**Target outcome:** After fixing, SE% should rise to > 70% and SE query count should drop to < 10 for this visual.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise Dashboard Performance Remediation

**Scenario:** You're brought in to fix a company's flagship executive dashboard. Current state: 25 visuals on one page, DirectQuery to Azure Synapse Analytics, average load time 90 seconds. The business requirement is < 5 seconds. Walk through your complete performance remediation approach.

<details>
<summary>💡 Hint</summary>

This requires a systematic multi-layer approach: model architecture (aggregation tables, composite model), DAX optimization, report design, and source-side changes. No single fix will get from 90s to 5s — you need all layers.

</details>

<details>
<summary>✅ Solution</summary>

**Assessment Phase (Day 1)**

Run Performance Analyzer and document all 25 visuals:

```
Visual                    DAX Time   Source
───────────────────────────────────────────
Revenue YTD Card          12,400ms   DQ
Monthly Trend Line        18,200ms   DQ
Top 10 Products           9,800ms    DQ
Regional Heatmap           7,400ms    DQ
... (21 more visuals)
Total page load: ~90 seconds
```

Run DAX Studio All Queries trace and Server Timings:
- 90% of visuals show > 85% FE time
- All DirectQuery — every visual hits Synapse
- 25 visuals × 3-4 Synapse queries each = 75-100 SQL queries per page load

**Solution Architecture:**

```
Phase 1: Aggregation Tables (Impact: 70-80% reduction)
Phase 2: DAX Optimization (Impact: 10-15% additional reduction)
Phase 3: Report Redesign (Impact: Final 5-10%)
```

**Phase 1: Aggregation Tables**

```sql
-- In Synapse: create two aggregation tables
-- AggSalesByMonth: for all monthly/quarterly/annual visuals
CREATE TABLE AggSalesByMonth AS
SELECT
    DATETRUNC(MONTH, OrderDate) AS MonthDate,
    ProductCategoryKey,
    RegionKey,
    SUM(SalesAmount) AS SalesAmount,
    COUNT(DISTINCT CustomerKey) AS UniqueCustomers,
    COUNT(*) AS OrderCount
FROM FactSales
GROUP BY DATETRUNC(MONTH, OrderDate), ProductCategoryKey, RegionKey;

-- AggSalesByProduct: for top products visual
CREATE TABLE AggSalesByProduct AS
SELECT
    ProductKey,
    SUM(SalesAmount) AS SalesAmount,
    COUNT(*) AS OrderCount
FROM FactSales
GROUP BY ProductKey;
```

In Power BI Desktop, set:
- `FactSales` → DirectQuery (keep for drill-through)
- `AggSalesByMonth` → Import
- `AggSalesByProduct` → Import
- Configure aggregation mappings in Model view

After Phase 1, most visuals route to Import agg tables → sub-200ms. Synapse is only hit for drill-through.

**Phase 2: DAX Optimization**

Address the top 5 slowest measures:

```dax
-- Fix 1: YTD with many SE queries
-- Before:
Revenue YTD =
CALCULATE(SUM(FactSales[SalesAmount]), DATESYTD(DimDate[Date]))

-- After agg table: same DAX, but now routes to AggSalesByMonth → fast

-- Fix 2: Remove FILTER over fact table
-- Before:
Large Orders Revenue =
CALCULATE(SUM(FactSales[SalesAmount]),
    FILTER(FactSales, FactSales[SalesAmount] > 10000))

-- After:
Large Orders Revenue =
CALCULATE(SUM(FactSales[SalesAmount]), FactSales[SalesAmount] > 10000)
-- Column filter folds better to agg tables

-- Fix 3: Eliminate volatile NOW() in measures
-- Before:
Current Period Flag = IF(MAX(DimDate[Date]) = TODAY(), "Current", "Prior")
-- Breaks SE cache

-- After: Pre-compute in a calculated column or use a date table flag
IsCurrentMonth = DimDate[YearMonth] = FORMAT(TODAY(), "YYYY-MM")
```

**Phase 3: Report Redesign**

```
Current: 25 visuals on 1 page → 25 × 3 = 75 Synapse queries per load
Target design:
  - Executive Summary page: 6 key KPI cards + 1 trend line = 7 queries
  - Drill-through pages: each page for details on demand
  - Bookmark navigation: users click through logical sections

Additional fixes:
  - Query Reduction: Apply buttons on slicers
  - Edit Interactions: disable cross-filtering on KPI cards
  - Pre-filter heavy visuals with report-level filters
```

**Expected Results:**

| Phase | Page Load Time | Synapse Queries |
|---|---|---|
| Baseline | 90 seconds | 75-100 |
| After Phase 1 (Agg tables) | 8 seconds | 2-5 (drill-through only) |
| After Phase 2 (DAX) | 4 seconds | 2-5 |
| After Phase 3 (Report) | 1.5 seconds | 8-12 (6-visual page) |

**Final result**: 90 seconds → 1.5 seconds. Exceeds the 5-second target.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What are the three components shown in Performance Analyzer for each visual?" — DAX query time (measure evaluation), Visual display time (rendering), and Other (network, framework). DAX query time is almost always the bottleneck for data-heavy reports.

> **Tip 2:** "What does high FE% (Formula Engine) vs SE% (Storage Engine) mean in DAX Studio Server Timings?" — High FE% means the DAX measure has patterns that force serial, row-by-row processing in the Formula Engine instead of parallel columnar scanning in the Storage Engine. Common causes: FILTER iterating large tables, RANKX, LOOKUPVALUE inside iterators, or CALCULATE inside SUMX causing thousands of context transitions.

> **Tip 3:** "How would you improve performance for a DirectQuery report that takes 20 seconds to load?" — First diagnose with Performance Analyzer and DAX Studio to find the slowest visuals. Then: (1) add Import-mode aggregation tables for common aggregation patterns, configure agg mapping so Power BI automatically routes queries; (2) add indexes to the source database on commonly filtered columns; (3) reduce visual count using drill-through pages; (4) enable Query Reduction for slicers.
