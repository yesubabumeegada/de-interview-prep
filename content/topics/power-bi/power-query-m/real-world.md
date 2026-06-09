---
title: "Power Query M — Real-World Patterns"
topic: power-bi
subtopic: power-query-m
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, power-query-m, interview, real-world, production]
---

# Power Query M — Real-World Patterns

## Pattern 1: Dynamic Folder Load (All Files in a Folder)

**Scenario**: Reports must load all Excel files from a SharePoint folder, combine them, and add a source file name column. New files added to the folder should be included automatically on refresh.

```powerquery
let
    // Connect to SharePoint folder
    Source = SharePoint.Files(
        "https://company.sharepoint.com/sites/Finance/",
        [ApiVersion = 15]
    ),

    // Filter to only Excel files in the target subfolder
    FilteredFiles = Table.SelectRows(Source, each
        Text.StartsWith([Folder Path], "https://company.sharepoint.com/sites/Finance/Monthly Reports/") and
        Text.EndsWith([Name], ".xlsx")
    ),

    // Keep only the file content and name
    KeepColumns = Table.SelectColumns(FilteredFiles, {"Content", "Name"}),

    // Define the transformation to apply to each file
    TransformFile = (fileContent as binary, fileName as text) as table =>
    let
        Wb = Excel.Workbook(fileContent, null, true),
        Sheet = Wb{[Item="Sales",Kind="Sheet"]}[Data],
        Headers = Table.PromoteHeaders(Sheet, [PromoteAllScalars=true]),
        Typed = Table.TransformColumnTypes(Headers, {
            {"Date", type date},
            {"Amount", type number},
            {"Region", type text}
        }),
        WithSource = Table.AddColumn(Typed, "SourceFile", each fileName, type text)
    in
        WithSource,

    // Apply transformation to each file
    TransformedFiles = Table.AddColumn(
        KeepColumns,
        "Data",
        each TransformFile([Content], [Name])
    ),

    // Combine all transformed tables
    Combined = Table.Combine(TransformedFiles[Data]),

    // Remove rows with null amounts (header rows from some files)
    Cleaned = Table.SelectRows(Combined, each [Amount] <> null)
in
    Cleaned
```

---

## Pattern 2: Incremental API Load with Error Handling

**Scenario**: Pull data from a paginated REST API with error handling for rate limits and schema validation.

```powerquery
let
    // Configuration (use parameters in production)
    BaseUrl = "https://api.crm.example.com/v2/contacts",
    ApiKey = "Bearer " & ApiKeyParameter,
    PageSize = 200,

    // Function: fetch one page
    FetchPage = (page as number) as record =>
    let
        Url = BaseUrl & "?page=" & Text.From(page) & "&pageSize=" & Text.From(PageSize),
        RawResponse = Web.Contents(
            Url,
            [
                Headers = [Authorization = ApiKey, Accept = "application/json"],
                ManualStatusHandling = {429, 500, 503}
            ]
        ),
        Parsed = Json.Document(RawResponse),
        HasData = Record.HasFields(Parsed, "data") and List.Count(Parsed[data]) > 0
    in
        [data = if HasData then Parsed[data] else {}, hasMore = HasData],

    // Paginate through all pages
    AllPages = List.Generate(
        () => [page = 1, result = FetchPage(1)],
        each [result][hasMore],
        each [page = [page] + 1, result = FetchPage([page] + 1)],
        each [result][data]
    ),

    // Flatten all records
    AllRecords = List.Combine(AllPages),

    // Convert to table
    AsTable = Table.FromList(AllRecords, Splitter.SplitByNothing(), {"Record"}),
    Expanded = Table.ExpandRecordColumn(AsTable, "Record",
        {"id", "firstName", "lastName", "email", "company", "createdAt", "updatedAt"}
    ),

    // Type conversions
    Typed = Table.TransformColumnTypes(Expanded, {
        {"id", Int64.Type},
        {"firstName", type text},
        {"lastName", type text},
        {"email", type text},
        {"company", type text},
        {"createdAt", type datetimezone},
        {"updatedAt", type datetimezone}
    }),

    // Add derived columns
    WithFullName = Table.AddColumn(Typed, "FullName",
        each [firstName] & " " & [lastName], type text),
    WithDomain = Table.AddColumn(WithFullName, "EmailDomain",
        each try Text.AfterDelimiter([email], "@") otherwise null, type nullable text)
in
    WithDomain
```

---

## Pattern 3: Dynamic Date Table Generation

**Scenario**: Auto-generate a comprehensive date dimension table in Power Query M, supporting both standard and fiscal year calendars.

```powerquery
let
    // Parameters (set as M parameters or hardcode for simplicity)
    StartYear = 2020,
    EndYear = 2027,
    FYStartMonth = 7,  // Fiscal year starts July 1

    // Generate all dates
    StartDate = #date(StartYear, 1, 1),
    EndDate = #date(EndYear, 12, 31),
    TotalDays = Duration.TotalDays(EndDate - StartDate) + 1,
    DateList = List.Dates(StartDate, (Int32.From(TotalDays)), #duration(1,0,0,0)),

    // Convert list to table
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    TypedDate = Table.TransformColumnTypes(DateTable, {{"Date", type date}}),

    // Add calendar columns
    WithYear = Table.AddColumn(TypedDate, "Year", each Date.Year([Date]), Int64.Type),
    WithQtr = Table.AddColumn(WithYear, "Quarter",
        each "Q" & Text.From(Date.QuarterOfYear([Date])), type text),
    WithQtrNum = Table.AddColumn(WithQtr, "QuarterNum",
        each Date.QuarterOfYear([Date]), Int64.Type),
    WithMonthNum = Table.AddColumn(WithQtrNum, "MonthNum",
        each Date.Month([Date]), Int64.Type),
    WithMonthName = Table.AddColumn(WithMonthNum, "MonthName",
        each Date.ToText([Date], "MMMM"), type text),
    WithMonthShort = Table.AddColumn(WithMonthName, "MonthShort",
        each Date.ToText([Date], "MMM"), type text),
    WithDayNum = Table.AddColumn(WithMonthShort, "DayNum",
        each Date.Day([Date]), Int64.Type),
    WithDayOfWeek = Table.AddColumn(WithDayNum, "DayOfWeek",
        each Date.DayOfWeek([Date], Day.Monday) + 1, Int64.Type),
    WithDayName = Table.AddColumn(WithDayOfWeek, "DayName",
        each Date.ToText([Date], "dddd"), type text),
    WithIsWeekend = Table.AddColumn(WithDayName, "IsWeekend",
        each Date.DayOfWeek([Date], Day.Monday) >= 5, type logical),
    WithWeekNum = Table.AddColumn(WithIsWeekend, "WeekNum",
        each Date.WeekOfYear([Date]), Int64.Type),

    // Fiscal year columns
    WithFYYear = Table.AddColumn(WithWeekNum, "FiscalYear",
        each if Date.Month([Date]) >= FYStartMonth
             then "FY" & Text.From(Date.Year([Date]) + 1)
             else "FY" & Text.From(Date.Year([Date])),
        type text),
    WithFYQuarter = Table.AddColumn(WithFYYear, "FiscalQuarter",
        each
            let FYMonth = Number.Mod(Date.Month([Date]) - FYStartMonth + 12, 12) + 1
            in "FYQ" & Text.From(Number.IntegerDivide(FYMonth - 1, 3) + 1),
        type text),

    // Integer date key (YYYYMMDD)
    WithDateKey = Table.AddColumn(WithFYQuarter, "DateKey",
        each Date.Year([Date]) * 10000 + Date.Month([Date]) * 100 + Date.Day([Date]),
        Int64.Type),

    // YearMonth for slicers
    WithYearMonth = Table.AddColumn(WithDateKey, "YearMonth",
        each Text.From(Date.Year([Date])) & "-" & Text.PadStart(Text.From(Date.Month([Date])), 2, "0"),
        type text),

    // Relative periods (for "last 30 days" type slicers)
    Today = Date.From(DateTime.LocalNow()),
    WithIsToday = Table.AddColumn(WithYearMonth, "IsToday",
        each [Date] = Today, type logical),
    WithRelativeDays = Table.AddColumn(WithIsToday, "RelativeDayOffset",
        each Duration.Days([Date] - Today), Int64.Type)
in
    WithRelativeDays
```

---

## Pattern 4: Self-Service Data Cleaning Pipeline

**Scenario**: Raw sales data from a legacy system has inconsistent formats, duplicate rows, and outlier values. Build a robust M cleaning pipeline.

```powerquery
let
    // Load raw data
    Source = Csv.Document(
        File.Contents("\\server\share\raw_sales.csv"),
        [Delimiter=",", Encoding=1252, QuoteStyle=QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // Step 1: Trim all text columns
    TrimmedText = Table.TransformColumns(Headers,
        List.Transform(
            List.Select(Table.ColumnNames(Headers), each Table.Column(Headers, _){0} is text),
            each {_, Text.Trim}
        )
    ),

    // Step 2: Standardize column names (lower, replace spaces with underscore)
    StandardizeColName = (name as text) as text =>
        Text.Lower(Text.Replace(Text.Trim(name), " ", "_")),
    RenamedCols = Table.RenameColumns(
        TrimmedText,
        List.Transform(Table.ColumnNames(TrimmedText), each {_, StandardizeColName(_)})
    ),

    // Step 3: Parse amounts (handle "$1,234.56" format)
    ParseAmount = (raw as nullable text) as nullable number =>
        if raw = null then null
        else try Number.From(Text.Remove(raw, {"$", ",", " "})) otherwise null,

    ParsedAmounts = Table.TransformColumns(RenamedCols, {
        {"sales_amount", ParseAmount, type nullable number},
        {"discount_amount", ParseAmount, type nullable number}
    }),

    // Step 4: Parse dates (multiple formats in source)
    ParseDate = (raw as nullable text) as nullable date =>
        if raw = null then null
        else
            let Formats = {"M/d/yyyy", "d-MMM-yyyy", "yyyy-MM-dd", "MM/dd/yyyy"}
            in List.First(
                List.RemoveNulls(
                    List.Transform(Formats, each try Date.FromText(raw, [Format=_]) otherwise null)
                ),
                null
            ),

    ParsedDates = Table.TransformColumns(ParsedAmounts, {
        {"order_date", ParseDate, type nullable date}
    }),

    // Step 5: Remove duplicates (keep latest by order_date)
    Sorted = Table.Sort(ParsedDates, {{"order_date", Order.Descending}}),
    Deduped = Table.Distinct(Sorted, {"order_id"}),

    // Step 6: Flag outliers (sales > 3 standard deviations from mean)
    Amounts = List.RemoveNulls(Table.Column(Deduped, "sales_amount")),
    Mean = List.Average(Amounts),
    StdDev = let
        Variance = List.Average(List.Transform(Amounts, each Number.Power(_ - Mean, 2)))
    in Number.Sqrt(Variance),
    WithOutlierFlag = Table.AddColumn(Deduped, "is_outlier",
        each if [sales_amount] = null then null
             else Number.Abs([sales_amount] - Mean) > 3 * StdDev,
        type nullable logical),

    // Step 7: Filter out records with null required fields
    RequiredNotNull = Table.SelectRows(WithOutlierFlag, each
        [order_id] <> null and
        [order_date] <> null and
        [sales_amount] <> null
    )
in
    RequiredNotNull
```
