---
title: "Power Query M — Senior Deep Dive"
topic: power-bi
subtopic: power-query-m
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [power-bi, power-query-m, interview, senior, advanced]
---

# Power Query M — Senior Deep Dive

## Query Folding Internals

### How Folding Works

When Power Query connects to a relational source (SQL Server, PostgreSQL, Synapse, etc.), each M transformation is attempted to be translated into the source's native query language. Power Query maintains a **query plan** that tracks whether folding is possible for each step.

The folding mechanism works through **abstract syntax tree (AST) translation**:
1. M expression is parsed into an AST
2. Each AST node is mapped to a native query construct
3. If a node has no mapping, folding stops at that point
4. All subsequent steps run in the M engine against an in-memory result

### Forcing Folding with Table.View

For advanced scenarios, you can manually define what the folded query should look like using `Table.View`:

```powerquery
// Custom connector or advanced scenario: define view with explicit native query
CustomSource =
    Table.View(
        null,
        [
            GetType = () => type table [ID = Int64.Type, Name = text, Amount = number],
            GetRows = () =>
                let
                    Sql = "SELECT ID, Name, Amount FROM dbo.Sales WHERE Year = 2024",
                    Result = Value.NativeQuery(
                        Sql.Database("MyServer", "MyDB"),
                        Sql
                    )
                in
                    Result,
            OnSelectRows = (condition) =>
                Table.SelectRows(CustomSource, condition)
        ]
    )
```

### Value.NativeQuery

For cases where M cannot fold automatically, write the native query directly:

```powerquery
// Pass a parameterized native SQL query
let
    Source = Sql.Database("server.database.windows.net", "MyDB"),
    Result = Value.NativeQuery(
        Source,
        "SELECT OrderID, CustomerID, Amount, OrderDate
         FROM dbo.FactSales
         WHERE OrderDate >= @startDate AND OrderDate < @endDate",
        [startDate = RangeStart, endDate = RangeEnd]
    )
in
    Result
// Parameters are passed safely (SQL injection prevention)
// Result folds the date filter to SQL
```

---

## Advanced Custom Function Patterns

### Recursive Functions

M supports recursion through the `@` self-reference operator.

```powerquery
// Factorial
Factorial = (n as number) as number =>
    if n <= 1 then 1 else n * @Factorial(n - 1)

// Flatten a nested list (recursive)
FlattenList = (lst as list) as list =>
    List.Combine(
        List.Transform(lst, each
            if Value.Is(_, type list)
            then @FlattenList(_)
            else {_}
        )
    )

// Usage
FlattenList({1, {2, 3}, {4, {5, 6}}})
// → {1, 2, 3, 4, 5, 6}
```

### Memoization with Table.Buffer

For expensive functions called many times, use a lookup table pattern:

```powerquery
// Pre-compute expensive lookup once, buffer it, then join
let
    // Expensive operation computed once
    TaxRatesBuffered = Table.Buffer(
        Table.FromRecords({
            [State="CA", TaxRate=0.0725],
            [State="TX", TaxRate=0.0625],
            [State="NY", TaxRate=0.08]
        })
    ),

    // Join to main table
    Source = ...,
    WithTax = Table.NestedJoin(Source, {"State"}, TaxRatesBuffered, {"State"}, "Tax", JoinKind.LeftOuter),
    Expanded = Table.ExpandTableColumn(WithTax, "Tax", {"TaxRate"}),
    WithTaxAmount = Table.AddColumn(Expanded, "TaxAmount",
        each [Amount] * (if [TaxRate] = null then 0 else [TaxRate]),
        type number)
in
    WithTaxAmount
```

---

## List.Generate for Looping

`List.Generate` is M's way to implement iterative/loop logic.

```powerquery
// General pattern
List.Generate(
    () => <initial_state>,
    each <condition_to_continue>,
    each <next_state>,
    each <output_value>   // optional selector
)

// Example: Generate dates for the current quarter
let
    StartDate = Date.StartOfQuarter(Date.From(DateTime.LocalNow())),
    EndDate = Date.EndOfQuarter(StartDate),
    DateList = List.Generate(
        () => StartDate,
        each _ <= EndDate,
        each Date.AddDays(_, 1)
    ),
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    TypedTable = Table.TransformColumnTypes(DateTable, {{"Date", type date}})
in
    TypedTable

// Example: Paginated API calls
GetAllPages = (baseUrl as text) as table =>
let
    GetPage = (page as number) =>
        let
            Url = baseUrl & "?page=" & Text.From(page) & "&limit=200",
            Json = Json.Document(Web.Contents(Url)),
            Records = Json[results]
        in
            Records,

    Pages = List.Generate(
        () => [page = 1, results = GetPage(1)],
        each List.Count([results]) > 0,
        each [page = [page] + 1, results = GetPage([page] + 1)],
        each [results]
    ),
    AllRecords = List.Combine(Pages),
    AsTable = Table.FromList(AllRecords, Splitter.SplitByNothing()),
    Expanded = Table.ExpandRecordColumn(AsTable, "Column1", Record.FieldNames(AllRecords{0}))
in
    Expanded
```

---

## Column-Level Type Inference Optimization

Power Query's auto-type detection step (`Changed Type` with "Detect Data Types") can be slow on large datasets. Replace it with explicit type assignment:

```powerquery
// SLOW: Auto-detect (reads all rows to infer types)
#"Detected Type" = Table.TransformColumnTypes(Source, ...)
// Generated step that reads entire dataset

// FAST: Explicit schema definition (no inference needed)
ExplicitTypes = Table.TransformColumnTypes(Source, {
    {"OrderID",       Int64.Type},
    {"OrderDate",     type date},
    {"CustomerKey",   Int64.Type},
    {"ProductKey",    Int64.Type},
    {"Quantity",      Int64.Type},
    {"UnitPrice",     type number},
    {"SalesAmount",   type number},
    {"Region",        type text}
})
```

---

## Schema Enforcement

Protect against source schema changes that would break reports silently:

```powerquery
// Validate that all expected columns exist
ValidateSchema = (tbl as table, requiredColumns as list) as table =>
let
    ActualColumns = Table.ColumnNames(tbl),
    MissingColumns = List.Difference(requiredColumns, ActualColumns),
    Result =
        if List.Count(MissingColumns) > 0
        then error Error.Record(
            "Schema Mismatch",
            "Missing columns: " & Text.Combine(MissingColumns, ", "),
            [Expected = requiredColumns, Actual = ActualColumns]
        )
        else tbl
in
    Result

// Usage
Validated = ValidateSchema(
    Source,
    {"OrderID", "OrderDate", "CustomerKey", "SalesAmount"}
)
```

---

## Advanced Table Operations

### Table.TransformRows (Row-Level Record Manipulation)

```powerquery
// Custom row-level transformation using records
Transformed = Table.FromRecords(
    Table.TransformRows(Source, (row) =>
        Record.TransformFields(row, {
            {"Amount", each _ * 1.1},
            {"Name", Text.Proper}
        })
    )
)
```

### Table.Partition

Split a table into chunks for parallel processing or batched API calls:

```powerquery
// Split into batches of 1000 rows
Partitions = Table.Partition(Source, "BatchID", 10, each Number.Mod([Index], 10))

// Process each partition separately
ProcessBatch = (batchTable as table) as table => ...

ProcessedPartitions = List.Transform(
    {0..9},
    (i) => ProcessBatch(Table.SelectRows(Source, each [BatchID] = i))
)
AllProcessed = Table.Combine(ProcessedPartitions)
```

---

## Performance Diagnostics

### Profiling Query Performance

1. Open **Power Query Editor**
2. View → **Query Diagnostics** → **Start Diagnostics**
3. Refresh the query
4. View → **Query Diagnostics** → **Stop Diagnostics**
5. Two diagnostic tables appear: one for step-level timings, one for folded SQL

Key metrics:
- `Exclusive Duration` — time spent in this step alone
- `Row Count` — rows processed at each step
- `Data Source Query` — the actual SQL/native query sent to the source

### Identifying Slow Steps

```powerquery
// Common causes of slow M evaluation:
// 1. Table.Buffer on large tables (forces full load)
// 2. List.Generate with many iterations
// 3. Nested joins on non-indexed columns
// 4. Custom functions called row-by-row
// 5. Web.Contents in a loop (network latency per call)

// Optimization: batch API calls instead of per-row calls
BatchApiCall = (ids as list) as table =>
let
    IdString = Text.Combine(List.Transform(ids, Text.From), ","),
    Url = "https://api.example.com/items?ids=" & IdString,
    Result = Json.Document(Web.Contents(Url))
in
    Table.FromList(Result, Splitter.SplitByNothing())

// Process in batches of 100
BatchSize = 100,
AllIDs = Table.Column(Source, "ItemID"),
Batches = List.Transform(
    {0..Number.IntegerDivide(List.Count(AllIDs) - 1, BatchSize)},
    (i) => List.Range(AllIDs, i * BatchSize, BatchSize)
),
Results = Table.Combine(List.Transform(Batches, BatchApiCall))
```

---

## Connection Options and Authentication

```powerquery
// SQL with specific query timeout and command timeout
Sql.Database(
    "server",
    "database",
    [
        Query = "SELECT ...",
        CommandTimeout = #duration(0, 0, 5, 0),  // 5 minutes
        ConnectionTimeout = #duration(0, 0, 0, 30)
    ]
)

// Azure Blob Storage with SAS token
AzureStorage.Blobs(
    "https://myaccount.blob.core.windows.net/mycontainer",
    [SharedAccessSignature = "sv=2020-08-04&ss=b&srt=..."]
)

// Web.Contents with retry logic
SafeWebContents = (url as text, retries as number) as binary =>
    let
        Attempt = try Web.Contents(url, [ManualStatusHandling = {429, 500, 503}])
    in
        if Attempt[HasError] || retries <= 0
        then if Attempt[HasError] then error Attempt[Error] else Attempt[Value]
        else @SafeWebContents(url, retries - 1)
```

---

## Summary

- **Value.NativeQuery** lets you write explicit native SQL for full folding control
- **List.Generate** is M's loop mechanism — use for pagination and date generation
- **Recursive functions** with `@` enable tree traversal and nested list flattening
- **Schema validation** protects against silent source changes
- Use **Query Diagnostics** to identify slow steps and confirm query folding
- **Batch API calls** instead of per-row calls to avoid network overhead
- Explicit **type assignment** is faster than auto-detect on large datasets
