---
title: "Power Query M — Intermediate"
topic: power-bi
subtopic: power-query-m
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, power-query-m, interview, intermediate]
---

# Power Query M — Intermediate

## Custom Functions

Custom functions let you encapsulate reusable transformation logic.

### Defining a Custom Function

```powerquery
// A function that cleans phone numbers
CleanPhone = (rawPhone as text) as text =>
let
    Digits = Text.Select(rawPhone, {"0".."9"}),
    Formatted =
        if Text.Length(Digits) = 10
        then "(" & Text.Start(Digits, 3) & ") " & Text.Middle(Digits, 3, 3) & "-" & Text.End(Digits, 4)
        else rawPhone
in
    Formatted

// Usage:
CleanedPhones = Table.TransformColumns(Source, {{"Phone", CleanPhone}})
```

### Multi-Parameter Function

```powerquery
// Function: fiscal year from a date based on configurable FY start month
GetFiscalYear = (d as date, fyStartMonth as number) as number =>
let
    Year = Date.Year(d),
    Month = Date.Month(d),
    FiscalYear = if Month >= fyStartMonth then Year + 1 else Year
in
    FiscalYear

// Apply to a column
WithFiscalYear = Table.AddColumn(Source, "FiscalYear",
    each GetFiscalYear([OrderDate], 7),  // FY starts July
    Int64.Type)
```

### Function as a Separate Query

Create a query named `fnCleanPhone`, and reference it from other queries:

```powerquery
// Query: fnCleanPhone
(rawPhone as text) as text =>
let
    Digits = Text.Select(rawPhone, {"0".."9"}),
    Result = if Text.Length(Digits) = 10
             then "(" & Text.Start(Digits, 3) & ") " & Text.Middle(Digits, 3, 3) & "-" & Text.End(Digits, 4)
             else rawPhone
in
    Result

// Query: Customers
let
    Source = ...,
    Cleaned = Table.TransformColumns(Source, {{"Phone", fnCleanPhone}})
in
    Cleaned
```

---

## Error Handling: try...otherwise

The `try` expression safely evaluates an expression that might fail.

```powerquery
// Try to parse a text as a number; return null if it fails
SafeNumber = (txt as nullable text) as nullable number =>
    let Result = try Number.From(txt) otherwise null
    in Result

// Apply error handling to a column
WithSafeNumbers = Table.TransformColumns(Source, {
    {"Amount", each try Number.From(_) otherwise null, type nullable number}
})

// More complex error handling
WithDetails = Table.AddColumn(Source, "ParseResult", each
    let Attempt = try Number.From([RawAmount])
    in if Attempt[HasError]
       then "Error: " & Attempt[Error][Message]
       else Text.From(Attempt[Value])
)
```

---

## Query Folding

**Query folding** is the ability of Power Query to translate M transformations into native source queries (SQL, OData, etc.). When query folding occurs, the transformation is pushed to the source server, improving performance dramatically.

### Check Query Folding

Right-click a step in Applied Steps:
- **"View Native Query"** is enabled → the step folds
- **"View Native Query"** is greyed out → the step does not fold

### Transformations That Fold (SQL sources)

```powerquery
// These fold to SQL:
Table.SelectRows(...)       -- WHERE clause
Table.SelectColumns(...)    -- SELECT list
Table.Sort(...)              -- ORDER BY
Table.Group(...)             -- GROUP BY
Table.NestedJoin(...)        -- JOIN
Table.RenameColumns(...)     -- Column alias
Table.TransformColumnTypes   -- CAST
```

### Transformations That Break Folding

```powerquery
// These prevent folding for subsequent steps:
Table.AddColumn with custom logic  // Custom logic can't be SQL
Table.Buffer(...)                  // Forces evaluation in M
Table.FromRecords(...)             // In-memory operation
Text.Format(...)                   // M-specific function
List.Generate(...)                 // Generates data in M
```

### Preserving Folding

```powerquery
// Pattern: keep foldable steps first, non-foldable steps last
let
    Source = Sql.Database("server", "db"),
    // These steps fold to SQL:
    Filtered = Table.SelectRows(Source, each [Year] = 2024),     // WHERE Year = 2024
    Selected = Table.SelectColumns(Filtered, {"ID", "Amount"}),  // SELECT
    Sorted = Table.Sort(Selected, {{"Amount", Order.Descending}}), // ORDER BY
    // Non-foldable step last (minimizes data pulled from SQL):
    WithBand = Table.AddColumn(Sorted, "Band",
        each if [Amount] > 1000 then "High" else "Low")
in
    WithBand
```

---

## List Operations

M lists (`{...}`) are flexible and have many built-in functions.

```powerquery
// List literals
Numbers = {1, 2, 3, 4, 5}
Range = {1..100}
Combined = {1..5} & {10..15}

// List functions
List.Sum({10, 20, 30})              // → 60
List.Average({10, 20, 30})          // → 20
List.Max({10, 20, 30})              // → 30
List.Min({10, 20, 30})              // → 10
List.Count({10, 20, 30})            // → 3
List.Distinct({1, 2, 2, 3})         // → {1, 2, 3}
List.Contains({1, 2, 3}, 2)         // → true
List.Select({1,2,3,4,5}, each _ > 3) // → {4, 5}
List.Transform({1,2,3}, each _ * 2)  // → {2, 4, 6}
List.Accumulate({1,2,3,4}, 0, (state, current) => state + current) // → 10
```

---

## Record Operations

Records (`[key=value, ...]`) are like key-value pairs or dictionaries.

```powerquery
// Record literal
Person = [Name = "Alice", Age = 30, City = "New York"]

// Access a field
Name = Person[Name]          // → "Alice"
Age = Person[Age]            // → 30

// Record functions
Record.FieldNames(Person)    // → {"Name", "Age", "City"}
Record.FieldValues(Person)   // → {"Alice", 30, "New York"}
Record.ToTable(Person)       // → table with Name, Value columns
Record.AddField(Person, "Country", "USA") // → adds Country field
Record.RemoveFields(Person, {"Age"})      // → removes Age field
```

---

## Table.TransformColumns vs Table.AddColumn

```powerquery
// Table.TransformColumns: MODIFY existing columns in place
Modified = Table.TransformColumns(Source, {
    {"Name", Text.Upper, type text},
    {"Amount", each _ * 1.1, type number},
    {"Date", Date.Year, Int64.Type}
})

// Table.AddColumn: ADD a new column (original preserved)
Added = Table.AddColumn(Source, "NameUpper", each Text.Upper([Name]), type text)
```

---

## Expanding Nested Tables and Lists

When merging queries or working with JSON/XML, columns often contain nested tables or lists.

```powerquery
// After a nested join, expand the nested table
Expanded = Table.ExpandTableColumn(
    MergedQuery,
    "ProductDetails",                        // column containing nested table
    {"ProductName", "Category"},             // which sub-columns to expand
    {"ProductDetails.ProductName", "ProductDetails.Category"}  // new column names
)

// Expand a list column (one row per list item)
ExpandedList = Table.ExpandListColumn(Source, "Tags")
// {"A", ["x","y"]} → {"A","x"}, {"A","y"}

// Expand a record column
ExpandedRecord = Table.ExpandRecordColumn(
    Source, "Address",
    {"Street", "City", "Zip"},
    {"Address.Street", "Address.City", "Address.Zip"}
)
```

---

## Table.Buffer

`Table.Buffer` forces immediate evaluation of a table into memory. Useful to prevent recalculation when a query is referenced multiple times.

```powerquery
// Without Buffer: BaseQuery is re-evaluated multiple times
let
    Base = Table.SelectRows(Source, each [Year] = 2024),
    // If Base is referenced in multiple downstream steps, it may be re-evaluated
    Count = Table.RowCount(Base),
    TopRows = Table.FirstN(Base, 100)
in ...

// With Buffer: Base is evaluated once and cached
let
    Base = Table.Buffer(Table.SelectRows(Source, each [Year] = 2024)),
    Count = Table.RowCount(Base),
    TopRows = Table.FirstN(Base, 100)
in ...
```

**Trade-off**: Buffer prevents query folding. Only buffer when the query does not fold to a source anyway.

---

## Working with JSON and API Data

```powerquery
// Connect to a REST API
let
    Source = Web.Contents(
        "https://api.example.com/sales",
        [
            Headers = [#"Authorization" = "Bearer " & ApiToken],
            Query = [startDate = "2024-01-01", endDate = "2024-12-31"]
        ]
    ),
    ParsedJson = Json.Document(Source),
    // JSON is typically a list of records
    ToTable = Table.FromList(ParsedJson, Splitter.SplitByNothing()),
    Expanded = Table.ExpandRecordColumn(ToTable, "Column1",
        {"id", "amount", "date", "customer"})
in
    Expanded

// Paginated API (loop through pages)
let
    GetPage = (page as number) =>
        let
            Url = "https://api.example.com/orders?page=" & Text.From(page) & "&pageSize=100",
            Response = Web.Contents(Url),
            Data = Json.Document(Response)[data]
        in
            Table.FromList(Data, Splitter.SplitByNothing()),

    Pages = List.Generate(
        () => [page = 1, data = GetPage(1)],
        each Table.RowCount([data]) > 0,
        each [page = [page] + 1, data = GetPage([page] + 1)],
        each [data]
    ),
    Combined = Table.Combine(Pages)
in
    Combined
```

---

## Summary

- **Custom functions** encapsulate reusable M logic; create as separate queries and reference by name
- **try...otherwise** handles errors gracefully without crashing the refresh
- **Query folding** pushes transformations to the source — keep foldable steps first
- **List.Transform/Select** are functional alternatives to loops
- **Table.Buffer** caches a table in memory; use cautiously as it breaks folding
- Expand **nested tables/records/lists** after JSON parsing or Merge operations
- Keep non-foldable transformations at the **end** of the Applied Steps chain
