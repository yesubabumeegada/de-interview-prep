---
title: "Power Query M — Fundamentals"
topic: power-bi
subtopic: power-query-m
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, power-query-m, interview, fundamentals]
---

# Power Query M — Fundamentals

## What Is Power Query M?

**Power Query M** is the formula language used by Power Query in Power BI, Excel, and other Microsoft tools. It is used to connect to data sources, transform data, and load the results into the data model.

- M is a **functional, case-sensitive, lazy-evaluated** language
- Every transformation step in the Power Query Editor generates M code
- The result of every M expression is a **value**: a table, a list, a record, a text, a number, etc.

---

## M vs DAX

| Aspect | Power Query M | DAX |
|---|---|---|
| Purpose | Data transformation & loading | Calculations & analytics |
| When it runs | At data refresh | At query time (visuals) |
| Operates on | Tables, columns, rows | Aggregations, filter context |
| Output | Loaded tables in the model | Numbers, text, tables |
| Editor | Power Query Editor | DAX editor / Measure editor |

---

## M Language Basics

### The `let...in` Structure

Every M query is structured as a series of named steps inside a `let` expression:

```powerquery
let
    // Step 1: Connect to source
    Source = Csv.Document(File.Contents("C:\data\sales.csv"), [Delimiter=","]),

    // Step 2: Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // Step 3: Change types
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {
        {"OrderDate", type date},
        {"SalesAmount", type number},
        {"CustomerID", Int64.Type}
    }),

    // Step 4: Filter rows
    FilteredRows = Table.SelectRows(ChangedTypes, each [SalesAmount] > 0)

in
    FilteredRows
-- The last expression after "in" is what gets returned
```

### Comments

```powerquery
// Single-line comment

/* Multi-line
   comment */
```

---

## Data Types in M

| M Type | Description | Example |
|---|---|---|
| `type text` | String | `"Hello"` |
| `Int64.Type` | 64-bit integer | `42` |
| `type number` | Decimal number | `3.14` |
| `type date` | Date only | `#date(2024,1,15)` |
| `type datetime` | Date + time | `#datetime(2024,1,15,9,0,0)` |
| `type logical` | Boolean | `true`, `false` |
| `type null` | Missing value | `null` |
| `type list` | List of values | `{1, 2, 3}` |
| `type record` | Key-value pairs | `[Name="Alice", Age=30]` |
| `type table` | Table of rows | Result of most transformations |

---

## Applied Steps (The Backbone of Power Query)

Every transformation in the Power Query Editor creates a new step in the **Applied Steps** pane. Each step is a named M variable.

```powerquery
let
    Source = Excel.Workbook(File.Contents("data.xlsx"), null, true),
    Sheet1_Sheet = Source{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Sheet1_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Date", type date}, {"Amount", type number}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each [Amount] > 0),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows",{"InternalID", "TempFlag"})
in
    #"Removed Columns"
```

**Note**: Steps with spaces in their names use the `#"..."` notation.

---

## Common Table Transformations

### Filtering Rows

```powerquery
// Keep rows where Region = "North"
FilteredRows = Table.SelectRows(Source, each [Region] = "North")

// Multiple conditions (AND)
FilteredRows = Table.SelectRows(Source, each [Region] = "North" and [Amount] > 100)

// Multiple values (OR / List.Contains)
FilteredRegions = Table.SelectRows(Source, each List.Contains({"North", "South"}, [Region]))

// Remove null rows from a column
NonNullRows = Table.SelectRows(Source, each [CustomerID] <> null)
```

### Adding Columns

```powerquery
// Add a custom column
WithFullName = Table.AddColumn(Source, "FullName", each [FirstName] & " " & [LastName], type text)

// Add conditional column
WithBand = Table.AddColumn(Source, "PriceBand",
    each if [Price] < 50 then "Budget"
         else if [Price] < 200 then "Mid-Range"
         else "Premium",
    type text)

// Add index column
WithIndex = Table.AddIndexColumn(Source, "RowID", 1, 1, Int64.Type)
```

### Renaming and Removing Columns

```powerquery
// Rename columns
Renamed = Table.RenameColumns(Source, {{"old_name", "NewName"}, {"sale_amt", "SalesAmount"}})

// Remove specific columns
Removed = Table.RemoveColumns(Source, {"TempColumn", "InternalFlag"})

// Keep only specific columns (remove all others)
KeepOnly = Table.SelectColumns(Source, {"CustomerID", "Name", "Amount", "Date"})
```

### Changing Data Types

```powerquery
// Transform multiple columns at once
TypeChanged = Table.TransformColumnTypes(Source, {
    {"OrderDate", type date},
    {"Quantity", Int64.Type},
    {"UnitPrice", type number},
    {"CustomerName", type text}
})
```

### Sorting

```powerquery
// Sort ascending
Sorted = Table.Sort(Source, {{"Date", Order.Ascending}})

// Multi-column sort
MultiSorted = Table.Sort(Source, {{"Year", Order.Descending}, {"Month", Order.Ascending}})
```

---

## Merging Queries (Joins)

Merging combines two tables based on matching columns.

```powerquery
// Left join: keep all rows from Sales, match from Products
MergedSales = Table.NestedJoin(
    Sales,                          // left table
    {"ProductKey"},                 // left key column(s)
    Products,                       // right table
    {"ProductKey"},                 // right key column(s)
    "ProductDetails",               // name for the expanded column
    JoinKind.LeftOuter              // join type
)

// Expand the nested table to get the columns you need
Expanded = Table.ExpandTableColumn(
    MergedSales,
    "ProductDetails",
    {"ProductName", "Category", "UnitCost"},
    {"ProductName", "Category", "UnitCost"}
)
```

### Join Types

| JoinKind | Description |
|---|---|
| `JoinKind.LeftOuter` | All rows from left, matching from right |
| `JoinKind.RightOuter` | All rows from right, matching from left |
| `JoinKind.Inner` | Only matching rows from both |
| `JoinKind.FullOuter` | All rows from both |
| `JoinKind.LeftAnti` | Rows in left with no match in right |
| `JoinKind.RightAnti` | Rows in right with no match in left |

---

## Appending Queries (UNION)

Appending stacks two tables on top of each other (like SQL UNION).

```powerquery
// Append two tables
AllSales = Table.Combine({SalesNorth, SalesSouth})

// Append multiple tables
AllRegions = Table.Combine({SalesNorth, SalesSouth, SalesEast, SalesWest})
```

---

## Grouping and Aggregating

```powerquery
// Group by Region, sum SalesAmount, count rows
Grouped = Table.Group(Source, {"Region"}, {
    {"TotalSales", each List.Sum([SalesAmount]), type number},
    {"OrderCount", each Table.RowCount(_), Int64.Type},
    {"AvgSales", each List.Average([SalesAmount]), type number}
})
```

---

## Pivoting and Unpivoting

```powerquery
// Unpivot: convert month columns to rows
// Before: | Product | Jan | Feb | Mar |
// After:  | Product | Month | Value |

Unpivoted = Table.UnpivotOtherColumns(
    Source,
    {"Product"},    // columns to keep as-is
    "Month",        // new attribute column name
    "Value"         // new value column name
)

// Pivot: convert rows to columns
// Before: | Product | Month | Value |
// After:  | Product | Jan | Feb | Mar |

Pivoted = Table.Pivot(
    Source,
    List.Distinct(Source[Month]),  // distinct values become column headers
    "Month",                        // the column to pivot from
    "Value",                        // the values column
    List.Sum                        // aggregation function
)
```

---

## Parameters in Power Query

Parameters make queries dynamic and reusable.

```powerquery
// Create a parameter named "StartDate" of type date
// In Power Query: Manage Parameters > New Parameter

// Reference a parameter in a query
FilteredDates = Table.SelectRows(Source, each [OrderDate] >= StartDate and [OrderDate] <= EndDate)
```

Parameters are also required for **incremental refresh** (RangeStart and RangeEnd).

---

## Common String Functions

```powerquery
// Text manipulation
Text.Upper("hello")                    // → "HELLO"
Text.Lower("HELLO")                    // → "hello"
Text.Trim("  hello  ")                 // → "hello"
Text.Start("Hello World", 5)           // → "Hello"
Text.End("Hello World", 5)             // → "World"
Text.Middle("Hello World", 6, 5)       // → "World"
Text.Contains("Hello World", "World")  // → true
Text.Replace("Hello World", "World", "Power BI") // → "Hello Power BI"
Text.Split("North,South,East", ",")    // → {"North","South","East"}
Text.Combine({"North","South"}, ", ")  // → "North, South"
```

---

## Common Date Functions

```powerquery
Date.Year(#date(2024, 6, 15))         // → 2024
Date.Month(#date(2024, 6, 15))        // → 6
Date.Day(#date(2024, 6, 15))          // → 15
Date.DayOfWeek(#date(2024, 6, 15))    // → 5 (Saturday; 0=Sunday)
Date.AddDays(#date(2024, 1, 1), 30)   // → #date(2024, 1, 31)
Date.AddMonths(#date(2024, 1, 31), 1) // → #date(2024, 2, 29)
DateTime.LocalNow()                    // → current date-time
Date.From(DateTime.LocalNow())         // → today's date
```

---

## Summary

- Power Query M uses a **let...in** expression with named steps
- Each Applied Step in the editor corresponds to one M variable
- **Table.SelectRows** filters, **Table.AddColumn** adds, **Table.NestedJoin** merges
- Use **Table.Combine** to union/append tables
- **Parameters** enable dynamic filtering and incremental refresh
- Data types must be explicitly set — M does not auto-detect reliably
- M transforms run **at refresh time**, not at query time like DAX
