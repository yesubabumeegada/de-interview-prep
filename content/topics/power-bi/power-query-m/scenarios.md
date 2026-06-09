---
title: "Power Query M — Scenarios"
topic: power-bi
subtopic: power-query-m
content_type: scenario_question
tags: [power-bi, power-query-m, scenarios, interview]
---

# Power Query M — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Combining Monthly Sales Files Automatically

**Scenario:** Every month, the finance team drops a new Excel file named `Sales_YYYY_MM.xlsx` into a SharePoint folder. You need to build a Power Query that automatically combines all files in that folder into a single table without manually updating the query each month.

<details>
<summary>💡 Hint</summary>

Think about the Folder data connector. Power Query has a built-in way to load all files in a folder. You'll also need to apply the same transformation to each file. Consider using the "Combine Files" button in Power Query UI, then review what it generates.

</details>

<details>
<summary>✅ Solution</summary>

**UI approach**: Connect via **SharePoint Folder** data source → filter to the folder → click **Combine Files** button. Power Query auto-generates a helper function and applies it to all files.

**Manual M approach** (what the UI generates, simplified):

```powerquery
let
    // Connect to SharePoint folder
    Source = SharePoint.Files("https://company.sharepoint.com/sites/Finance/", [ApiVersion=15]),

    // Filter to target folder and Excel files only
    Filtered = Table.SelectRows(Source, each
        Text.Contains([Folder Path], "Monthly Sales/") and
        Text.EndsWith([Name], ".xlsx")
    ),

    // Function to transform each file
    TransformFile = (content as binary) as table =>
    let
        Wb = Excel.Workbook(content, null, true),
        Sheet = Wb{[Item="Sheet1", Kind="Sheet"]}[Data],
        Promoted = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true]),
        Typed = Table.TransformColumnTypes(Promoted, {
            {"Date", type date},
            {"Amount", type number},
            {"Region", type text}
        })
    in Typed,

    // Apply to each file, adding source file name
    WithData = Table.AddColumn(Filtered, "Data",
        each TransformFile([Content])),
    WithSource = Table.AddColumn(WithData, "SourceFile",
        each [Name], type text),

    // Combine all
    ExpandedData = Table.ExpandTableColumn(WithData, "Data",
        {"Date", "Amount", "Region"}),

    // Remove null rows (from blank excel rows)
    Cleaned = Table.SelectRows(ExpandedData, each [Date] <> null)
in
    Cleaned
```

**Key points:**
- The query automatically picks up new files on refresh — no manual updates needed
- Add a `SourceFile` column so you can trace which file each row came from
- Filter early to avoid loading unrelated files

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Folding Breaks After Adding a Custom Column

**Scenario:** You have a Power Query connected to SQL Server. The query was fast, but after your colleague added a "Tax Amount" calculated column (`each [Amount] * 0.08`), the refresh time jumped from 5 seconds to 4 minutes. You suspect query folding broke. How do you diagnose this, and how do you fix it?

<details>
<summary>💡 Hint</summary>

Check the Applied Steps order. The calculated column step prevents folding for all subsequent steps, including any filters or sorts. Think about step ordering and whether the column can be moved to the end.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

1. In Power Query Editor → right-click each step in Applied Steps
2. Find where "View Native Query" becomes greyed out — that's where folding breaks
3. The `Table.AddColumn` with custom M logic breaks folding because M logic can't be translated to SQL
4. Any filters or sorts **after** this step also stop folding and run in M against the full dataset

**The Problem — Bad Step Order:**

```powerquery
let
    Source = Sql.Database("server", "db"),
    AllData = Source{[Schema="dbo", Item="FactSales"]}[Data],
    // Folding works up to here (WHERE/SELECT go to SQL)
    Filtered = Table.SelectRows(AllData, each [Year] = 2024),    // ✅ folds
    WithTax = Table.AddColumn(Filtered, "TaxAmount",             // ❌ breaks folding
        each [Amount] * 0.08, type number),
    // These now run in M against ALL 2024 rows in memory:
    Sorted = Table.Sort(WithTax, {{"Date", Order.Descending}}),  // ❌ no longer folds
    Top1000 = Table.FirstN(Sorted, 1000)                         // ❌ no longer folds
in
    Top1000
```

**Fix 1 — Move non-foldable step to the end (keeps foldable steps first):**

```powerquery
let
    Source = Sql.Database("server", "db"),
    AllData = Source{[Schema="dbo", Item="FactSales"]}[Data],
    Filtered = Table.SelectRows(AllData, each [Year] = 2024),    // ✅ folds to WHERE
    Sorted = Table.Sort(Filtered, {{"Date", Order.Descending}}),  // ✅ folds to ORDER BY
    Top1000 = Table.FirstN(Sorted, 1000),                         // ✅ folds to TOP 1000
    WithTax = Table.AddColumn(Top1000, "TaxAmount",               // ❌ breaks here
        each [Amount] * 0.08, type number)                        // but only 1000 rows now!
in
    WithTax
```

**Fix 2 — Push the calculation to SQL using Value.NativeQuery:**

```powerquery
Result = Value.NativeQuery(
    Sql.Database("server", "db"),
    "SELECT TOP 1000 *, Amount * 0.08 AS TaxAmount
     FROM dbo.FactSales
     WHERE Year = 2024
     ORDER BY Date DESC"
)
```

**Result:** By reordering steps, SQL handles filtering, sorting, and limiting (reducing 50M rows to 1000), then M only adds the calculated column on 1000 rows instead of 50M.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build a Reusable, Parameterized ETL Framework in Power Query

**Scenario:** Your team manages 15 Power BI reports, each connecting to different database tables but applying the same transformations: column renaming by mapping table, data type enforcement, null handling, and audit columns (LoadDate, SourceTable). How do you build a reusable M framework so that adding a new table requires only adding a config row, not writing new M code?

<details>
<summary>💡 Hint</summary>

Think about configuration-driven design. A metadata table can hold table names, column mappings, and type definitions. A master function reads the config and applies transformations dynamically using Record operations and List.Transform.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
Config Table (in Power Query)
    ↓
fnLoadTable (generic function)
    ↓
One query per table (calls fnLoadTable with table name)
```

**Step 1: Config Table**

```powerquery
// Query: Config
let
    Data = #table(
        {"SourceTable", "TargetTable", "ColumnMappings", "DateColumns", "NumericColumns"},
        {
            {
                "dbo.raw_sales",    "FactSales",
                [order_id = "OrderID", sale_amt = "SalesAmount", cust_key = "CustomerKey"],
                {"order_date", "ship_date"},
                {"sale_amt", "discount_amt"}
            },
            {
                "dbo.raw_customers","DimCustomer",
                [cust_id = "CustomerKey", cust_nm = "CustomerName", rgn = "Region"],
                {"signup_date"},
                {}
            }
        }
    )
in
    Data
```

**Step 2: Generic Load Function**

```powerquery
// Query: fnLoadTable
(sourceTable as text, columnMappings as record, dateColumns as list, numericColumns as list) as table =>
let
    // Connect to SQL Server
    DB = Sql.Database(DatabaseServer, DatabaseName),
    RawTable = DB{[Schema = Text.BeforeDelimiter(sourceTable, "."), Item = Text.AfterDelimiter(sourceTable, ".")]}[Data],

    // Rename columns based on mapping record
    MappingList = Record.ToTable(columnMappings),
    RenamePairs = List.Transform(
        Table.ToRecords(MappingList),
        each {[Name], [Value]}
    ),
    // Only rename columns that exist in source
    ExistingRenames = List.Select(
        RenamePairs,
        each List.Contains(Table.ColumnNames(RawTable), _{0})
    ),
    Renamed = Table.RenameColumns(RawTable, ExistingRenames),

    // Apply type transformations
    DateTypeList = List.Transform(dateColumns, each {_, type date}),
    NumericTypeList = List.Transform(numericColumns, each {_, type number}),
    Typed = Table.TransformColumnTypes(
        Renamed,
        List.Combine({DateTypeList, NumericTypeList})
    ),

    // Handle nulls: replace null text with "Unknown", null numbers with 0
    TextCols = List.Select(Table.ColumnNames(Typed),
        each Table.Schema(Typed){[Name=_]}?[Kind]? = "text"),
    NullHandled = Table.ReplaceValue(Typed, null, "Unknown", Replacer.ReplaceValue, TextCols),

    // Add audit columns
    WithLoadDate = Table.AddColumn(NullHandled, "_LoadDate",
        each DateTime.LocalNow(), type datetime),
    WithSource = Table.AddColumn(WithLoadDate, "_SourceTable",
        each sourceTable, type text)
in
    WithSource

// Save this query as fnLoadTable (not loaded to model — function only)
```

**Step 3: Per-Table Queries (minimal code)**

```powerquery
// Query: FactSales
let
    Config = Table.SelectRows(Config, each [SourceTable] = "dbo.raw_sales"){0},
    Result = fnLoadTable(
        Config[SourceTable],
        Config[ColumnMappings],
        Config[DateColumns],
        Config[NumericColumns]
    )
in
    Result

// Query: DimCustomer (identical structure, different config row)
let
    Config = Table.SelectRows(Config, each [SourceTable] = "dbo.raw_customers"){0},
    Result = fnLoadTable(
        Config[SourceTable],
        Config[ColumnMappings],
        Config[DateColumns],
        Config[NumericColumns]
    )
in
    Result
```

**Result:** Adding a new table requires only:
1. One row in the Config table (source name, column mapping, type lists)
2. One new query (5 lines) that references the config row

No ETL logic is duplicated. Changes to cleaning rules are applied to all tables by editing `fnLoadTable` once.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is query folding and why does it matter?" — Query folding is Power Query's ability to translate M transformations into native source queries (SQL, OData, etc.). When steps fold, the source server handles filtering, sorting, and aggregation before sending data to Power BI — dramatically reducing data transfer and transformation time. Broken folding means Power BI loads all data and processes it in M.

> **Tip 2:** "How do you handle errors in Power Query without crashing the refresh?" — Use `try...otherwise` to wrap expressions that might fail. `try expression otherwise fallback` returns the fallback value if the expression errors. For detailed error information, use `try expression` which returns a record with `[HasError]`, `[Value]`, and `[Error]` fields.

> **Tip 3:** "When should you use Table.Buffer?" — Use `Table.Buffer` when a query is referenced multiple times downstream and re-evaluation is expensive. However, `Table.Buffer` breaks query folding, so only use it when the query doesn't fold anyway (e.g., after a custom column step). Never buffer large tables unnecessarily — it forces full in-memory load.
