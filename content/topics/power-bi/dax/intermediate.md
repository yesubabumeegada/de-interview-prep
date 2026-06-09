---
title: "DAX — Intermediate"
topic: power-bi
subtopic: dax
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, dax, interview, intermediate]
---

# DAX — Intermediate

## Context Transition

**Context transition** is when CALCULATE (or any aggregation function) converts the current **row context** into an equivalent **filter context**.

This is a subtle but critical concept. It occurs automatically when CALCULATE is used inside an iterator or a calculated column.

```dax
-- Calculated column example: context transition
-- Each row's ProductKey becomes a filter context filter
Product Revenue =
CALCULATE(SUM(FactSales[SalesAmount]))
-- Row context for ProductKey=101 → filter context: ProductKey = 101
-- Returns total sales for this specific product

-- Without CALCULATE, SUM would return the grand total (no row context awareness)
Wrong Product Revenue = SUM(FactSales[SalesAmount])
-- Returns the same grand total for every row!
```

### Context Transition Inside SUMX

```dax
-- SUMX creates row context; CALCULATE triggers context transition
Weighted Price =
SUMX(
    DimProduct,
    CALCULATE(SUM(FactSales[SalesAmount]))  -- context transition here
    / CALCULATE(SUM(FactSales[Quantity]))
)
-- For each product row, CALCULATE creates a filter for that product
-- Result: weighted average price per product, summed across all products
```

---

## Advanced CALCULATE Patterns

### Boolean Filters vs Table Filters

CALCULATE accepts two kinds of filters:

```dax
-- Boolean filter (fast, single column)
Electronics = CALCULATE(SUM(Sales[Amount]), Product[Category] = "Electronics")

-- Table filter (more flexible, can be multi-column)
Electronics UK =
CALCULATE(
    SUM(Sales[Amount]),
    FILTER(ALL(Product), Product[Category] = "Electronics"),
    FILTER(ALL(Customer), Customer[Country] = "UK")
)
```

### KEEPFILTERS

By default, CALCULATE **replaces** existing filters on the column. `KEEPFILTERS` **intersects** the new filter with the existing one.

```dax
-- Default: replaces any existing Category filter with "Electronics"
CALCULATE(SUM(Sales[Amount]), Product[Category] = "Electronics")
-- If user selects "Furniture" slicer → result still shows Electronics

-- KEEPFILTERS: intersects with existing filter
CALCULATE(
    SUM(Sales[Amount]),
    KEEPFILTERS(Product[Category] = "Electronics")
)
-- If user selects "Furniture" → result is BLANK (Electronics ∩ Furniture = empty)
-- If user selects "Electronics" → result shows Electronics total
-- If no category filter → result shows Electronics total
```

### REMOVEFILTERS (Power BI 2020+)

Explicit alternative to ALL() inside CALCULATE:

```dax
-- Old syntax
CALCULATE(SUM(Sales[Amount]), ALL(Product[Category]))

-- New syntax (clearer intent)
CALCULATE(SUM(Sales[Amount]), REMOVEFILTERS(Product[Category]))
```

---

## Time Intelligence Deep Dive

### Date Intelligence Requirements Checklist

```
✅ A date table with no gaps
✅ Date table marked as "Date Table" in Power BI Desktop
✅ Relationship from fact table DateKey to date table Date column
✅ Dates cover at least the full range of dates in fact tables
```

### Comprehensive Time Intelligence Patterns

```dax
-- Month-to-date
Revenue MTD =
TOTALMTD(SUM(FactSales[SalesAmount]), DimDate[Date])

-- Quarter-to-date
Revenue QTD =
TOTALQTD(SUM(FactSales[SalesAmount]), DimDate[Date])

-- Year-to-date with custom fiscal year end (June 30)
Revenue FYTD =
TOTALYTD(
    SUM(FactSales[SalesAmount]),
    DimDate[Date],
    "06-30"  -- fiscal year ends June 30
)

-- Same period last year
Revenue SPLY =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    SAMEPERIODLASTYEAR(DimDate[Date])
)

-- Previous quarter
Revenue Prev Quarter =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DATEADD(DimDate[Date], -1, QUARTER)
)

-- Trailing 12 months (not YTD — rolling 12)
Revenue TTM =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DATESINPERIOD(
        DimDate[Date],
        LASTDATE(DimDate[Date]),
        -12,
        MONTH
    )
)

-- Year-over-year growth rate
YoY % =
VAR Current = SUM(FactSales[SalesAmount])
VAR Prior = CALCULATE(SUM(FactSales[SalesAmount]), SAMEPERIODLASTYEAR(DimDate[Date]))
RETURN
    DIVIDE(Current - Prior, Prior, BLANK())

-- Cumulative total from the beginning of the dataset
Running Total =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    FILTER(
        ALL(DimDate),
        DimDate[Date] <= MAX(DimDate[Date])
    )
)
```

---

## RANKX

Returns the rank of a value within a list.

```dax
-- Rank products by total revenue (1 = highest)
Product Revenue Rank =
RANKX(
    ALL(DimProduct[ProductName]),
    [Total Revenue],
    ,
    DESC,
    Dense  -- Dense: no gaps in rank (1,2,2,3); Skip: gaps (1,2,2,4)
)

-- Top N flag (useful for conditional formatting)
Is Top 10 Product =
IF([Product Revenue Rank] <= 10, "Top 10", "Other")

-- Top N using TOPN (table of top products)
Top 10 Revenue =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    TOPN(10, ALL(DimProduct), [Total Revenue])
)
```

---

## HASONEVALUE and ISINSCOPE

Used for handling subtotals and totals in matrix visuals.

```dax
-- Show different calculation for individual rows vs. total row
Smart Average =
IF(
    HASONEVALUE(DimProduct[ProductKey]),
    AVERAGEX(FactSales, FactSales[SalesAmount]),  -- row level
    AVERAGE(FactSales[SalesAmount])               -- total row
)

-- ISINSCOPE: true when the column is the current level of hierarchy
Conditional Measure =
IF(
    ISINSCOPE(DimDate[Year]),
    [Revenue YTD],
    [Total Revenue]
)
```

---

## ALLSELECTED

Returns all values in the column, respecting only user-applied slicers (not filters from the visual itself). Used for "% of filtered total" scenarios.

```dax
-- % of what's currently selected/visible to the user
% of Selection =
DIVIDE(
    SUM(FactSales[SalesAmount]),
    CALCULATE(SUM(FactSales[SalesAmount]), ALLSELECTED(DimProduct))
)
-- If user selects category "Electronics" via slicer,
-- denominator = total Electronics sales (not grand total)
```

---

## EARLIER (Legacy Row Context)

`EARLIER` accesses the outer row context when you are inside a nested iterator.

```dax
-- Calculate running total as a calculated column
Running Total Col =
SUMX(
    FILTER(
        FactSales,
        FactSales[OrderDate] <= EARLIER(FactSales[OrderDate])
    ),
    FactSales[SalesAmount]
)
-- EARLIER(FactSales[OrderDate]) refers to the outer row's OrderDate
-- FILTER iterates the whole table in the inner context
```

**Modern alternative**: Use VAR to capture the outer row value, which is clearer:

```dax
Running Total Col =
VAR CurrentDate = FactSales[OrderDate]
RETURN
SUMX(
    FILTER(FactSales, FactSales[OrderDate] <= CurrentDate),
    FactSales[SalesAmount]
)
```

---

## Dynamic Segmentation

Segment customers or products dynamically without storing the segment in the model.

```dax
-- Customer Revenue Tier (dynamic, based on current filter context)
Customer Tier =
VAR CustomerRevenue =
    CALCULATE(
        SUM(FactSales[SalesAmount]),
        ALLEXCEPT(DimCustomer, DimCustomer[CustomerKey])
    )
RETURN
    SWITCH(
        TRUE(),
        CustomerRevenue >= 100000, "Platinum",
        CustomerRevenue >= 50000, "Gold",
        CustomerRevenue >= 10000, "Silver",
        "Bronze"
    )

-- Count of customers per tier
Platinum Customers =
CALCULATE(
    DISTINCTCOUNT(FactSales[CustomerKey]),
    [Customer Tier] = "Platinum"
)
-- Note: this pattern requires the tier measure to work row by row — use SUMX/COUNTX
```

---

## Measure Branching with ISBLANK and IF

```dax
-- Handle periods with no sales gracefully
Revenue with Zero Fill =
VAR Sales = SUM(FactSales[SalesAmount])
RETURN
    IF(ISBLANK(Sales), 0, Sales)

-- Different calculation for actual vs. future dates
Forecast Revenue =
VAR Today = TODAY()
VAR MaxSalesDate = MAX(FactSales[OrderDate])
RETURN
    IF(
        MAX(DimDate[Date]) <= MaxSalesDate,
        SUM(FactSales[SalesAmount]),
        [Forecasted Amount]
    )
```

---

## Troubleshooting Common DAX Issues

| Problem | Cause | Fix |
|---|---|---|
| Measure returns grand total in every row | Using SUM without filter context | Check that the visual axis is set to a column from the model |
| BLANK instead of 0 | No rows in current filter context | Wrap with `IF(ISBLANK(...), 0, ...)` or use `+0` |
| Wrong SPLY values | Date table not marked as Date Table | Mark the date table in Model view |
| Circular dependency | Calculated column references itself indirectly | Restructure: use measure or different columns |
| FILTER slower than expected | FILTER iterates row by row | Use column filters in CALCULATE instead |
| Context transition unexpected result | CALCULATE inside iterator | Add VAR before iterator to freeze context |

---

## Summary

- **Context transition** converts row context to filter context via CALCULATE
- **KEEPFILTERS** intersects new filter with existing; default CALCULATE replaces
- Master **time intelligence** patterns for YTD, MTD, SPLY, and rolling periods
- **ALLSELECTED** gives "% of visible total" relative to user selections
- Use **VAR** instead of EARLIER for readable nested row context
- **RANKX** with TOPN enables ranking and top-N analysis
- **HASONEVALUE / ISINSCOPE** handle subtotal rows in matrix visuals
