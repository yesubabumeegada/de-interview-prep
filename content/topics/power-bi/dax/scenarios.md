---
title: "DAX — Scenarios"
topic: power-bi
subtopic: dax
content_type: scenario_question
tags: [power-bi, dax, scenarios, interview]
---

# DAX — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Calculate % of Total Sales per Product

**Scenario:** You have a matrix with products on rows and total sales in the values. You want to add a column showing each product's percentage of the grand total. When a user filters by year, the denominator should update to reflect only the selected year's total. Write the DAX measure.

<details>
<summary>💡 Hint</summary>

You need to remove the product filter from the denominator but keep any date/slicer filters. Think about ALL vs ALLSELECTED.

</details>

<details>
<summary>✅ Solution</summary>

```dax
% of Total Sales =
DIVIDE(
    SUM(FactSales[SalesAmount]),
    CALCULATE(
        SUM(FactSales[SalesAmount]),
        ALLSELECTED(DimProduct)
    ),
    0
)
```

**Explanation:**
- `SUM(FactSales[SalesAmount])` — the numerator uses the current product filter context (each product row)
- `ALLSELECTED(DimProduct)` — removes the product filter so the denominator is the total for all visible products, but respects any slicers the user has applied (e.g., year filter)
- `DIVIDE(..., 0)` — returns 0 instead of an error if the denominator is blank

**Why not ALL(DimProduct)?**
- `ALL` ignores all filters, including user-applied slicers
- If a user filters to "2024", `ALL` would divide by the grand total (all years), not 2024's total
- `ALLSELECTED` respects user-applied external filters while removing the visual filter on Product

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Year-over-Year Growth That Handles Missing Prior Year Data

**Scenario:** Your report shows monthly revenue. The business launched in March 2023. For Jan and Feb 2024, `SAMEPERIODLASTYEAR` returns BLANK because there was no data in Jan/Feb 2023. The % change card shows an error. How do you write a robust YoY measure that handles this gracefully?

<details>
<summary>💡 Hint</summary>

Check whether the prior year value is blank before dividing. Also consider whether you want to show 0%, BLANK, or a "N/A" label in the no-prior-year case.

</details>

<details>
<summary>✅ Solution</summary>

```dax
YoY Growth % =
VAR Current =
    SUM(FactSales[SalesAmount])
VAR Prior =
    CALCULATE(
        SUM(FactSales[SalesAmount]),
        SAMEPERIODLASTYEAR(DimDate[Date])
    )
RETURN
    IF(
        ISBLANK(Prior) || Prior = 0,
        BLANK(),               -- Show blank card instead of error or ∞
        DIVIDE(Current - Prior, Prior, BLANK())
    )
```

**For a card that shows "N/A" text instead of blank:**
```dax
YoY Growth Label =
VAR Growth = [YoY Growth %]
RETURN
    IF(
        ISBLANK(Growth),
        "N/A (no prior year)",
        FORMAT(Growth, "+0.0%;-0.0%;0.0%")
    )
```

**Key design decisions:**
1. Return `BLANK()` rather than 0 — this prevents the card from showing "0%" which would imply flat growth
2. Check both `ISBLANK(Prior)` (no data) and `Prior = 0` (data exists but was zero — would cause divide-by-zero)
3. Use a separate label measure for text display; keep the numeric measure clean for conditional formatting thresholds

**Production pattern — combine into one measure with VAR:**
```dax
YoY Growth % =
VAR Current = [Total Revenue]
VAR Prior = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))
VAR HasPrior = NOT ISBLANK(Prior) && Prior <> 0
RETURN
    IF(HasPrior, DIVIDE(Current - Prior, Prior), BLANK())
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Diagnose and Fix a Slow RANKX Measure

**Scenario:** A product ranking measure using `RANKX(ALL(DimProduct), [Total Revenue])` takes 8 seconds to render on a matrix with 50,000 products. DAX Studio shows 98% of time is in the Formula Engine. Explain why this is slow and provide a faster implementation.

<details>
<summary>💡 Hint</summary>

RANKX is always a Formula Engine operation. Think about how to pre-compute the ranking or reduce the table being ranked. Also consider whether ranking 50,000 products in a single visual is a good UX approach.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause:**

`RANKX(ALL(DimProduct), [Total Revenue])` evaluates `[Total Revenue]` for every one of 50,000 products in the Formula Engine, one row at a time. Since this is an FE iterator:
- 50,000 context transitions occur
- 50,000 separate SE queries are generated (or the FE processes 50,000 datacache lookups)
- No parallelism — all serial FE work
- Result: 8+ seconds

**Fix 1: Pre-aggregate with SUMMARIZE, then RANKX on the smaller table**

```dax
-- Instead of iterating ALL 50K products, first get only products visible in context
Product Rank (Fast) =
VAR ProductRevenues =
    SUMMARIZE(
        CALCULATETABLE(FactSales, ALLSELECTED(DimProduct)),
        DimProduct[ProductKey],
        DimProduct[ProductName],
        "@Revenue", [Total Revenue]
    )
VAR CurrentRevenue = [Total Revenue]
RETURN
    RANKX(
        ProductRevenues,
        [@Revenue],
        CurrentRevenue,
        DESC,
        Dense
    )
```

**Fix 2: Use a calculated column for static rank (if filtering doesn't change the ranking)**

```dax
-- Calculated column in DimProduct (computed at refresh, not at query time)
Static Revenue Rank =
RANKX(
    ALL(DimProduct),
    CALCULATE(SUM(FactSales[SalesAmount])),
    ,
    DESC,
    Dense
)
-- This runs once at refresh and is stored; no query-time cost
-- Limitation: rank doesn't change based on user filters
```

**Fix 3: Limit the ranked set**

```dax
-- Only rank top 1000 by revenue; everything else is "Other"
Product Rank (Limited) =
VAR N = 1000
VAR Top1000 =
    TOPN(N, ALL(DimProduct[ProductKey]), [Total Revenue])
VAR CurrentKey = SELECTEDVALUE(DimProduct[ProductKey])
RETURN
    IF(
        CurrentKey IN Top1000,
        RANKX(Top1000, [Total Revenue], , DESC, Dense),
        N + 1  -- All others get rank N+1
    )
```

**Recommendation for production:**

1. Apply Fix 2 (calculated column) if the rank is used for display and doesn't need to update with filters
2. Apply Fix 1 if dynamic filtering is required, and combine with visual-level Top N filtering (show only top 50) to keep the rendered set small
3. Add pagination or virtual scrolling for 50,000-row visuals — no user actually reads 50,000 ranked rows

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between row context and filter context?" — Row context is the current row being evaluated in an iterator or calculated column. Filter context is the set of active filters from slicers, visuals, or CALCULATE. CALCULATE can convert row context into filter context through context transition.

> **Tip 2:** "When would you use ALLSELECTED vs ALL?" — Use `ALL` when you want to completely ignore filters (grand total comparisons). Use `ALLSELECTED` when you want to respect the user's slicer selections but remove the visual's own inner filters (% of user's visible selection).

> **Tip 3:** "How do you debug a slow DAX measure?" — Open DAX Studio, connect to the Power BI Desktop file, paste the measure wrapped in EVALUATE, and enable Server Timings. Look at FE vs SE time split. If FE >> SE, you have row-by-row iteration in the Formula Engine — look for FILTER iterating large tables, LOOKUPVALUE inside SUMX, or RANKX over large tables. Rewrite to push work to the Storage Engine with column filters instead.
