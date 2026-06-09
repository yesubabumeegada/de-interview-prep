---
title: "Paginated Reports — Intermediate"
topic: power-bi
subtopic: paginated-reports
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, paginated-reports, interview, intermediate]
---

# Paginated Reports — Intermediate

## Tablix Deep Dive

### Row Groups and Column Groups

Tablix supports grouping at both row and column levels, with totals at each group boundary.

```
Sales Report with Row Groups:
┌────────────────────────────────────────────────────────────┐
│ Region     │ Product     │ Month    │ Qty    │ Revenue     │
├────────────┼─────────────┼──────────┼────────┼─────────────┤
│ North      │             │          │        │             │  ← Region group header
│            │ Widget A    │          │        │             │  ← Product group header
│            │             │ Jan 2024 │    100 │  $2,500.00  │  ← Detail row
│            │             │ Feb 2024 │    120 │  $3,000.00  │
│            │             │ [Total]  │    220 │  $5,500.00  │  ← Month group footer
│            │ [Total]     │          │    220 │  $5,500.00  │  ← Product group footer
│ [Total]    │             │          │    220 │  $5,500.00  │  ← Region group footer
└────────────────────────────────────────────────────────────┘
```

**Adding a row group in Report Builder:**
1. Right-click the detail row in Tablix
2. Row Group → Add Group → Parent Group
3. Set Group By = `[Region]`
4. Repeat for `[Product]`

### Sorting Within Groups

```vb
' Sort products within each region by revenue descending
' In Tablix Properties → Sorting:
Sort by: =Sum(Fields!Revenue.Value)
Order: Descending
```

### Toggle Items (Show/Hide)

Allow users to expand/collapse group sections:

1. Add a text box to the group header (e.g., region name)
2. In Properties → Action → Toggle visibility → Target: Detail rows
3. Users click the region name to expand/collapse

---

## Subreports

A **subreport** embeds one report inside another. The parent report passes parameters to the subreport.

**Use case**: An order summary report embeds an order line items subreport, one per order.

```
Parent Report: OrderSummary.rdl
  → Shows one row per order (OrderID, CustomerName, TotalAmount)
  → Subreport item embedded in a detail row

Subreport: OrderLineItems.rdl
  → Parameter: @OrderID
  → Shows all line items for that order

Connection:
  Subreport Properties → Parameter:
    Name: OrderID
    Value: =Fields!OrderID.Value  (from parent report's current row)
```

**Important**: Subreports have a performance cost — each subreport runs a separate database query for each row in the parent. Use only when necessary; consider joined data in a single dataset as an alternative.

---

## Dataset Queries and Stored Procedures

### Inline Query

```sql
-- Basic inline query in Report Builder dataset
SELECT
    o.OrderID,
    o.OrderDate,
    c.CustomerName,
    p.ProductName,
    od.Quantity,
    od.UnitPrice,
    od.Quantity * od.UnitPrice AS LineTotal
FROM dbo.Orders o
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
    JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
    JOIN dbo.Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate BETWEEN @StartDate AND @EndDate
    AND (@Region = 'All' OR c.Region = @Region)
ORDER BY o.OrderDate, o.OrderID, od.ProductID
```

### Stored Procedure

```sql
-- Stored procedure (more maintainable for complex logic)
CREATE PROCEDURE rpt.GetOrderDetails
    @StartDate datetime,
    @EndDate datetime,
    @Region nvarchar(50) = 'All'
AS
    SELECT ... (same as inline query above)
GO

-- In Report Builder:
-- Dataset command type: Stored Procedure
-- Command text: rpt.GetOrderDetails
-- Parameters: @StartDate, @EndDate, @Region (mapped from report parameters)
```

### Dataset with Power BI Semantic Model (DAX Query)

```dax
-- When connecting to a Power BI dataset, use DAX queries:
EVALUATE
SUMMARIZECOLUMNS(
    DimDate[YearMonth],
    DimProduct[Category],
    "Revenue", [Total Revenue],
    "Units", [Total Units Sold]
)
ORDER BY DimDate[YearMonth] ASC
```

---

## Report Layout Techniques

### Page Header and Footer

Every page gets the same header and footer:

```
Header (1 inch):
  [Left] Company Logo (image)
  [Center] "Monthly Sales Report"
  [Right] "Generated: " & Format(Now(), "MMMM dd, yyyy")

Footer (0.5 inch):
  [Left] "Confidential"
  [Center] "Page " & Globals!PageNumber & " of " & Globals!TotalPages
  [Right] "© Company Name 2024"
```

### Conditional Formatting

Highlight rows based on data values:

```vb
' Background color for cells: red if below threshold
=IIF(Fields!Revenue.Value < 1000, "#FFB3B3", "White")

' Bold font for totals
=IIF(Fields!RowType.Value = "Total", "Bold", "Normal")

' Conditional row visibility (hide zero-revenue rows)
=IIF(Fields!Revenue.Value = 0, True, False)
```

### Page Break Control

```
Page break BEFORE a group: show each region on its own page
  → Group Properties → Page Breaks → Before

Page break BETWEEN rows: each invoice on its own page
  → Detail row Properties → Page Break → Between Each Instance

Keep group together: prevent a group from splitting across pages
  → Group Properties → Keep Together = Always
```

---

## Parameters: Advanced Patterns

### Multi-Value Parameter

Allow users to select multiple values:

```sql
-- In SQL, handle multi-value parameter:
WHERE ProductCategory IN (@ProductCategories)
-- Report Builder automatically generates: WHERE ProductCategory IN ('Electronics', 'Furniture')
```

```vb
' Display selected values in report header:
=Join(Parameters!ProductCategories.Value, ", ")
' → "Electronics, Furniture"
```

### Available Values from Dataset

```
Parameter: @Region
  → Available Values: From dataset "RegionList"
    Label Field: RegionName
    Value Field: RegionID
  → Default Value: (All)
```

### Hidden Parameters (Passed by URL)

```
Report URL:
https://powerbi.com/reports/{id}?rp:StartDate=2024-01-01&rp:Region=North

Parameters:
  StartDate = 2024-01-01 (from URL)
  Region = North (from URL)
  → Report opens pre-filtered, no user interaction needed
```

---

## Connecting to a Power BI Semantic Model

Paginated reports can use a Power BI semantic model as a data source (Premium/PPU required).

**Steps:**
1. Report Builder → New Report Wizard → Data Source
2. Choose connection type: **Microsoft Power BI**
3. Enter workspace and dataset name
4. Write DAX queries for datasets

**Benefits:**
- Uses the same measures and business logic defined in the semantic model
- RLS from the semantic model applies automatically
- No need to re-create business logic in the paginated report

```dax
-- Example DAX dataset query for a paginated report
EVALUATE
SUMMARIZECOLUMNS(
    DimCustomer[CustomerName],
    DimDate[Date],
    DimProduct[ProductName],
    FILTER(DimDate, DimDate[Date] >= DATE(2024,1,1) AND DimDate[Date] <= DATE(2024,12,31)),
    "SalesAmount", SUM(FactSales[SalesAmount]),
    "Quantity", SUM(FactSales[Quantity])
)
ORDER BY DimDate[Date], DimCustomer[CustomerName]
```

---

## Report Server vs Power BI Service

| Aspect | Power BI Report Server (On-Premises) | Power BI Service (Cloud) |
|---|---|---|
| Hosting | Your own servers | Microsoft Azure |
| License | SQL Server Enterprise + SA, or Power BI Report Server | Power BI Premium |
| URL sharing | Internal network | Public internet / private with AAD |
| Integration | On-premises sources natively | Gateway for on-premises |
| Updates | Manual patching | Automatic |

---

## Summary

- **Tablix** supports nested row/column groups with group-level totals and subtotals
- **Subreports** embed one report inside another, passing parameters per-row
- Parameters support **multi-value**, **cascading**, and **URL-driven** patterns
- Connect to **Power BI semantic models** via DAX queries to reuse measures
- **Page breaks**, **Keep Together**, and **conditional visibility** control document layout
- Connecting to Power BI Premium datasets enables RLS enforcement in paginated reports
- **Stored procedures** are recommended for complex queries — easier to test and maintain
