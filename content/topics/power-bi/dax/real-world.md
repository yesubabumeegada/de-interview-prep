---
title: "DAX — Real-World Patterns"
topic: power-bi
subtopic: dax
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, dax, interview, real-world, production]
---

# DAX — Real-World Patterns

## Pattern 1: Executive KPI Card Set

**Scenario**: Finance dashboard needs Revenue, Gross Margin %, YoY Growth, and Month-over-Month trend for the current period.

```dax
-- ============================================================
-- Revenue KPIs
-- ============================================================

Total Revenue =
SUM(FactSales[SalesAmount])

Total COGS =
SUM(FactSales[CostOfGoodsSold])

Gross Profit =
[Total Revenue] - [Total COGS]

Gross Margin % =
DIVIDE([Gross Profit], [Total Revenue], 0)

-- ============================================================
-- Time Comparisons
-- ============================================================

Revenue SPLY =
CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))

YoY Revenue Growth % =
VAR Current = [Total Revenue]
VAR Prior = [Revenue SPLY]
RETURN
    IF(
        NOT ISBLANK(Prior),
        DIVIDE(Current - Prior, Prior, 0),
        BLANK()
    )

Revenue Prev Month =
CALCULATE([Total Revenue], DATEADD(DimDate[Date], -1, MONTH))

MoM Revenue Change % =
VAR Current = [Total Revenue]
VAR Prior = [Revenue Prev Month]
RETURN
    IF(
        NOT ISBLANK(Prior),
        DIVIDE(Current - Prior, Prior, 0),
        BLANK()
    )

-- ============================================================
-- Trend Arrow for KPI Cards (conditional formatting)
-- ============================================================

YoY Trend Icon =
VAR Growth = [YoY Revenue Growth %]
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(Growth), "—",
        Growth > 0.05, "▲ " & FORMAT(Growth, "0.0%"),
        Growth > 0, "↑ " & FORMAT(Growth, "0.0%"),
        Growth > -0.05, "↓ " & FORMAT(Growth, "0.0%"),
        "▼ " & FORMAT(Growth, "0.0%")
    )

-- ============================================================
-- Dynamic period label for card titles
-- ============================================================

Current Period Label =
VAR SelectedMonth = SELECTEDVALUE(DimDate[MonthName], "")
VAR SelectedYear = SELECTEDVALUE(DimDate[Year], "")
RETURN
    IF(
        SelectedMonth <> "" && SelectedYear <> "",
        SelectedMonth & " " & SelectedYear,
        "All Periods"
    )
```

---

## Pattern 2: Customer Cohort and Retention Analysis

**Scenario**: Marketing wants to see new vs returning customers, first-purchase cohorts, and customer retention rate.

```dax
-- ============================================================
-- Customer Classification
-- ============================================================

Customer First Purchase Date =
CALCULATE(
    MIN(FactSales[OrderDate]),
    ALLEXCEPT(FactSales, FactSales[CustomerKey])
)
-- This is a calculated column in FactSales

-- New vs Returning per period
New Customers =
CALCULATE(
    DISTINCTCOUNT(FactSales[CustomerKey]),
    FILTER(
        FactSales,
        YEAR(FactSales[CustomerFirstPurchaseDate]) = YEAR(MAX(DimDate[Date])) &&
        MONTH(FactSales[CustomerFirstPurchaseDate]) = MONTH(MAX(DimDate[Date]))
    )
)

Returning Customers =
DISTINCTCOUNT(FactSales[CustomerKey]) - [New Customers]

-- ============================================================
-- Cohort Revenue
-- ============================================================

-- Revenue from customers who first purchased in a given cohort month
Cohort Revenue =
CALCULATE(
    SUM(FactSales[SalesAmount]),
    FILTER(
        ALL(DimCustomer),
        DimCustomer[CohortYearMonth] = SELECTEDVALUE(CohortTable[YearMonth])
    )
)

-- ============================================================
-- Retention Rate
-- ============================================================

-- Customers active in both this period and the prior period
Retained Customers =
VAR CurrentCustomers =
    CALCULATETABLE(
        VALUES(FactSales[CustomerKey]),
        DimDate[YearMonth] = MAX(DimDate[YearMonth])
    )
VAR PriorCustomers =
    CALCULATETABLE(
        VALUES(FactSales[CustomerKey]),
        DATEADD(DimDate[Date], -1, MONTH)
    )
RETURN
    COUNTROWS(INTERSECT(PriorCustomers, CurrentCustomers))

Retention Rate =
DIVIDE(
    [Retained Customers],
    CALCULATE(
        DISTINCTCOUNT(FactSales[CustomerKey]),
        DATEADD(DimDate[Date], -1, MONTH)
    ),
    0
)

-- ============================================================
-- Customer Lifetime Value (simple)
-- ============================================================

Customer LTV =
DIVIDE(
    AVERAGEX(
        VALUES(FactSales[CustomerKey]),
        CALCULATE(SUM(FactSales[SalesAmount]))
    ),
    AVERAGEX(
        VALUES(FactSales[CustomerKey]),
        DATEDIFF(
            CALCULATE(MIN(FactSales[OrderDate])),
            CALCULATE(MAX(FactSales[OrderDate])),
            MONTH
        ) + 1
    ),
    0
) * 12  -- Annualized
```

---

## Pattern 3: Dynamic Top N with "Others" Grouping

**Scenario**: A product ranking visual should show top N products (user-selected via slicer) and group the rest as "Others".

```dax
-- ============================================================
-- Setup: What-If parameter for N
-- ============================================================
-- Create via Modeling > New Parameter
-- Name: "Top N", range 1-20, increment 1, default 5

Top N Value = SELECTEDVALUE('Top N'[Top N], 5)

-- ============================================================
-- Is Top N Flag (used as visual filter or measure branch)
-- ============================================================

Is Top N Product =
VAR N = [Top N Value]
VAR ProductRank =
    RANKX(
        ALL(DimProduct[ProductName]),
        [Total Revenue],
        ,
        DESC,
        Dense
    )
RETURN
    ProductRank <= N

-- ============================================================
-- Revenue with Others Grouping
-- ============================================================

Top N Label =
VAR N = [Top N Value]
VAR ProductRank =
    RANKX(
        ALL(DimProduct[ProductName]),
        [Total Revenue],
        ,
        DESC,
        Dense
    )
RETURN
    IF(ProductRank <= N, SELECTEDVALUE(DimProduct[ProductName]), "Others")

-- Revenue split: top N vs others
Top N Revenue =
CALCULATE(
    [Total Revenue],
    TOPN([Top N Value], ALL(DimProduct), [Total Revenue])
)

Others Revenue =
[Total Revenue] - [Top N Revenue]

-- ============================================================
-- Rank label for tooltip
-- ============================================================

Rank Label =
"#" & [Product Revenue Rank] & " of " &
COUNTROWS(ALL(DimProduct[ProductName])) & " products"
```

---

## Pattern 4: Waterfall / Bridge Chart Measures

**Scenario**: Finance needs a waterfall chart showing how revenue moved from last year to this year, broken down by region.

```dax
-- ============================================================
-- Waterfall components
-- ============================================================

-- Base: Prior year total (constant across all categories)
Prior Year Total =
CALCULATE(
    [Total Revenue],
    SAMEPERIODLASTYEAR(DimDate[Date]),
    ALL(DimGeography[Region])
)

-- Delta per region
Region Delta =
[Total Revenue] -
CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))

-- Running subtotal for waterfall positioning
Region Running Total =
VAR CurrentRegion = SELECTEDVALUE(DimGeography[Region])
RETURN
CALCULATE(
    [Prior Year Total] +
    SUMX(
        FILTER(
            ALL(DimGeography[Region]),
            RANKX(
                ALL(DimGeography[Region]),
                CALCULATE([Region Delta]),
                ,
                DESC
            ) <=
            RANKX(
                ALL(DimGeography[Region]),
                CALCULATE([Region Delta]),
                CALCULATE([Region Delta], DimGeography[Region] = CurrentRegion),
                DESC
            )
        ),
        CALCULATE([Region Delta])
    )
)

-- ============================================================
-- Conditional formatting colors for waterfall bars
-- ============================================================

Waterfall Color =
IF([Region Delta] >= 0, "#2ECC71", "#E74C3C")

-- ============================================================
-- Summary KPIs for waterfall chart header
-- ============================================================

Total Growth =
[Total Revenue] - [Prior Year Total (All Regions)]

Total Growth % =
DIVIDE([Total Growth], [Prior Year Total (All Regions)], 0)
```
