---
title: "DAX — Fundamentals"
topic: power-bi
subtopic: dax
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, dax, interview, fundamentals]
---

# DAX — Fundamentals

## What Is DAX?

**DAX (Data Analysis Expressions)** is the formula language used in Power BI, Analysis Services, and Power Pivot. It is used to create:

- **Measures** — dynamic calculations evaluated at query time
- **Calculated columns** — row-level calculations stored in the model
- **Calculated tables** — tables generated entirely from DAX

DAX looks similar to Excel formulas but operates on entire tables and columns, not individual cells.

---

## Basic Syntax

```dax
-- A simple measure
Total Sales = SUM(FactSales[SalesAmount])

-- A calculated column (in DimProduct table)
Full Category = DimProduct[Category] & " - " & DimProduct[SubCategory]

-- Using a variable for clarity
Gross Margin % =
VAR Revenue = SUM(FactSales[SalesAmount])
VAR COGS = SUM(FactSales[Cost])
VAR Margin = Revenue - COGS
RETURN
    DIVIDE(Margin, Revenue, 0)
```

---

## Evaluation Context

The most important concept in DAX. Every DAX expression is evaluated within a **context** that determines which rows are included.

### Row Context

Row context exists when DAX evaluates a formula **row by row** — this happens in:
- Calculated columns
- Iterator functions (SUMX, AVERAGEX, MAXX, etc.)

```dax
-- Calculated column: runs for each row in FactSales
Line Total = FactSales[Quantity] * FactSales[UnitPrice]
-- Row context: [Quantity] and [UnitPrice] refer to the current row
```

### Filter Context

Filter context comes from:
- Slicers in the report
- Filters on visuals
- Row/column headers in a matrix
- CALCULATE function in DAX

```dax
-- This measure uses filter context
-- When placed in a matrix with Year on rows, each cell has a different filter context
Total Sales = SUM(FactSales[SalesAmount])
-- Filter context for "2024" row: Year = 2024
```

---

## CALCULATE — The Most Important DAX Function

`CALCULATE` evaluates an expression in a **modified filter context**. It is the key to almost all advanced DAX.

**Syntax:**
```dax
CALCULATE(<expression>, <filter1>, <filter2>, ...)
```

```dax
-- Sales for Electronics category only
Electronics Sales =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DimProduct[Category] = "Electronics"
)

-- Sales for the year 2024 regardless of report filter
Sales 2024 =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DimDate[Year] = 2024
)

-- Multiple filters (AND logic)
Electronics Sales 2024 =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DimProduct[Category] = "Electronics",
    DimDate[Year] = 2024
)
```

---

## FILTER Function

`FILTER` returns a filtered table. It is an iterator — it evaluates row by row.

```dax
-- FILTER returns a table, not a scalar
-- Often used inside CALCULATE

High Value Sales =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    FILTER(FactSales, FactSales[SalesAmount] > 1000)
)

-- FILTER with multiple conditions
Premium Electronics =
CALCULATE(
    COUNTROWS(FactSales),
    FILTER(
        FactSales,
        FactSales[SalesAmount] > 500 && RELATED(DimProduct[Category]) = "Electronics"
    )
)
```

**Performance note**: Prefer simple column filters in CALCULATE over FILTER when possible — column filters are faster because they operate on the column dictionary, not row by row.

```dax
-- Faster (column filter)
CALCULATE(SUM(...), DimProduct[Category] = "Electronics")

-- Slower (row-level iterator)
CALCULATE(SUM(...), FILTER(DimProduct, DimProduct[Category] = "Electronics"))
```

---

## ALL and ALLEXCEPT

`ALL` removes filters from columns or tables. Used to create totals, percentages, and comparisons.

```dax
-- Total sales ignoring all filters
Total Sales All =
CALCULATE(SUM(FactSales[SalesAmount]), ALL(FactSales))

-- Share of total
% of Total Sales =
DIVIDE(
    SUM(FactSales[SalesAmount]),
    CALCULATE(SUM(FactSales[SalesAmount]), ALL(DimProduct))
)
-- Result: each product row shows its share of the grand total
```

`ALLEXCEPT` removes all filters except the specified columns:

```dax
-- % of category total (keep category filter, remove product filter)
% of Category =
DIVIDE(
    SUM(FactSales[SalesAmount]),
    CALCULATE(
        SUM(FactSales[SalesAmount]),
        ALLEXCEPT(DimProduct, DimProduct[Category])
    )
)
```

---

## Common Aggregation Functions

| Function | Purpose | Example |
|---|---|---|
| SUM | Sum of a column | `SUM(Sales[Amount])` |
| AVERAGE | Average of a column | `AVERAGE(Sales[Amount])` |
| COUNT | Count non-blank rows | `COUNT(Sales[OrderID])` |
| COUNTROWS | Count all rows in a table | `COUNTROWS(Sales)` |
| DISTINCTCOUNT | Count distinct values | `DISTINCTCOUNT(Sales[CustomerID])` |
| MIN / MAX | Minimum / Maximum | `MAX(Sales[Date])` |
| DIVIDE | Safe division (no divide-by-zero error) | `DIVIDE(Profit, Revenue, 0)` |

---

## Iterator Functions (X-functions)

Iterators evaluate an expression for each row of a table and then aggregate the results.

**Syntax:**
```dax
SUMX(<table>, <expression>)
```

```dax
-- SUMX: sum of Quantity * UnitPrice per row
Total Revenue =
SUMX(
    FactSales,
    FactSales[Quantity] * FactSales[UnitPrice]
)

-- AVERAGEX: average discount per order
Avg Discount =
AVERAGEX(
    FactSales,
    FactSales[SalesAmount] - FactSales[DiscountedAmount]
)

-- MAXX: largest single transaction
Largest Transaction =
MAXX(FactSales, FactSales[SalesAmount])

-- COUNTX: count rows meeting a condition
Large Orders =
COUNTX(
    FILTER(FactSales, FactSales[SalesAmount] > 1000),
    FactSales[OrderID]
)
```

---

## Variables (VAR / RETURN)

Variables make DAX readable and prevent double-computation.

```dax
-- Without variables (harder to read, evaluates Revenue twice)
Gross Margin % =
DIVIDE(
    SUM(FactSales[SalesAmount]) - SUM(FactSales[Cost]),
    SUM(FactSales[SalesAmount]),
    0
)

-- With variables (clear and efficient)
Gross Margin % =
VAR Revenue = SUM(FactSales[SalesAmount])
VAR TotalCost = SUM(FactSales[Cost])
VAR Margin = Revenue - TotalCost
RETURN
    DIVIDE(Margin, Revenue, 0)
```

**Important**: Variables capture the filter context **at the point they are defined**, not when they are used. This matters inside iterators.

---

## RELATED and RELATEDTABLE

These functions navigate relationships in DAX.

### RELATED (many-to-one direction)

Used in a calculated column on the "many" side to look up a value from the "one" side.

```dax
-- In FactSales calculated column: get the Category from DimProduct
Product Category =
RELATED(DimProduct[Category])
-- Walks from FactSales (many) to DimProduct (one) via ProductKey relationship
```

### RELATEDTABLE (one-to-many direction)

Used on the "one" side to return all related rows from the "many" side.

```dax
-- In DimCustomer calculated column: count how many orders this customer has
Order Count =
COUNTROWS(RELATEDTABLE(FactSales))
-- Returns a table of all FactSales rows for this customer, then counts them
```

---

## Time Intelligence Functions

These require a proper **date table** marked as "Date Table" in Power BI.

```dax
-- Year-to-date total
Revenue YTD =
TOTALYTD(SUM(FactSales[SalesAmount]), DimDate[Date])

-- Same period last year
Revenue SPLY =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    SAMEPERIODLASTYEAR(DimDate[Date])
)

-- Year-over-year growth %
YoY Growth % =
DIVIDE([Total Revenue] - [Revenue SPLY], [Revenue SPLY], 0)

-- Previous month
Revenue Prev Month =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DATEADD(DimDate[Date], -1, MONTH)
)

-- Last 30 days rolling
Revenue Last 30 Days =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    DATESINPERIOD(DimDate[Date], MAX(DimDate[Date]), -30, DAY)
)
```

---

## SELECTEDVALUE

Returns the value of a column when there is a single value in the current filter context; returns an alternate result otherwise.

```dax
-- Show the selected year, or "All Years" if multiple selected
Selected Year =
SELECTEDVALUE(DimDate[Year], "All Years")

-- Dynamic title for visuals
Chart Title =
"Revenue for " & SELECTEDVALUE(DimDate[Year], "All Years")
```

---

## SWITCH and IF

```dax
-- IF: simple two-branch logic
Performance =
IF([Revenue YoY Growth %] >= 0, "Positive", "Negative")

-- SWITCH: multiple branches
Performance Band =
SWITCH(
    TRUE(),
    [Revenue YoY Growth %] >= 0.2, "Excellent",
    [Revenue YoY Growth %] >= 0.05, "Good",
    [Revenue YoY Growth %] >= 0, "Flat",
    "Declining"
)

-- SWITCH on a value
Day Type =
SWITCH(
    DimDate[DayOfWeek],
    1, "Monday",
    2, "Tuesday",
    3, "Wednesday",
    4, "Thursday",
    5, "Friday",
    "Weekend"
)
```

---

## Quick Reference Table

| Task | DAX Function |
|---|---|
| Sum a column | `SUM(Table[Column])` |
| Sum row-by-row expression | `SUMX(Table, expr)` |
| Modify filter context | `CALCULATE(expr, filters)` |
| Remove all filters | `ALL(Table)` |
| Navigate to related dimension | `RELATED(Dim[Column])` |
| Safe division | `DIVIDE(numerator, denominator, 0)` |
| Year-to-date | `TOTALYTD(expr, DateColumn)` |
| Same period last year | `SAMEPERIODLASTYEAR(DateColumn)` |
| Single value in context | `SELECTEDVALUE(Column, default)` |

---

## Summary

- DAX operates on **evaluation context** (row context and filter context)
- **CALCULATE** is the primary way to modify filter context
- Use **VAR/RETURN** to make measures readable and efficient
- **Iterator functions** (SUMX, AVERAGEX) evaluate row by row
- **ALL/ALLEXCEPT** remove filters for comparisons and percentages
- **Time intelligence** requires a proper date table
- **RELATED** navigates from fact to dimension; **RELATEDTABLE** goes the other direction
