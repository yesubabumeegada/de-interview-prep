---
title: "Incremental Refresh — Scenarios"
topic: power-bi
subtopic: incremental-refresh
content_type: scenario_question
tags: [power-bi, incremental-refresh, scenarios, interview]
---

# Incremental Refresh — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Setting Up Incremental Refresh for the First Time

**Scenario:** Your company has a sales table with 3 years of daily data (about 10M rows). Full refresh takes 45 minutes each night, but the business only needs the previous day's data to be updated. How would you configure incremental refresh?

<details>
<summary>💡 Hint</summary>

Think about the two required parameters, the refresh policy settings, and what license is needed. Consider what "archive" and "refresh" windows mean.

</details>

<details>
<summary>✅ Solution</summary>

**Prerequisites:**
- Power BI Premium or Premium Per User (PPU) workspace — incremental refresh requires Premium
- Source database supports query folding (standard SQL Server, Azure SQL, etc.)

**Step 1: Create parameters in Power Query**

1. Open Power Query Editor → Home → Manage Parameters → New Parameter
2. Create `RangeStart`: Type = `Date/Time`, Value = `1/1/2024 12:00:00 AM`
3. Create `RangeEnd`: Type = `Date/Time`, Value = `12/31/2024 12:00:00 AM`

**Step 2: Apply the filter to your query**

```powerquery
let
    Source = Sql.Database("server", "db"),
    FactSales = Source{[Schema="dbo",Item="FactSales"]}[Data],
    FilteredRows = Table.SelectRows(
        FactSales,
        each [SaleDate] >= RangeStart and [SaleDate] < RangeEnd
    )
in
    FilteredRows
```

Verify the filter folds: right-click the `FilteredRows` step → "View Native Query" should show a SQL WHERE clause.

**Step 3: Configure the policy**

Right-click `FactSales` in the Fields pane → Incremental Refresh:
```
Archive data starting: 3 Years before refresh date
Refresh data in the last: 2 Days
(Use 2 days to capture any late-arriving data from yesterday)
```

**Step 4: Publish and run**

Publish to a Premium workspace. The first refresh creates all historical partitions (takes longer than usual). Subsequent refreshes only refresh the 2-day window — expected to take 2-3 minutes instead of 45.

**Expected result:**
- 45-minute refresh → 3-minute refresh
- Source receives only: `WHERE SaleDate >= '2024-12-09' AND SaleDate < '2024-12-11'`
- 3 years of historical data preserved without reloading

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Incremental Refresh Stopped Working After a Power Query Change

**Scenario:** Your dataset had incremental refresh working correctly for 6 months. A colleague added a new column with this transformation: `Table.AddColumn(Source, "FiscalYear", each if Date.Month([SaleDate]) >= 7 then Date.Year([SaleDate]) + 1 else Date.Year([SaleDate]))`. Now the nightly refresh imports the entire 3-year history every night (same as full refresh). What went wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about step ordering in Power Query and query folding. When did the non-foldable step get added relative to the RangeStart/RangeEnd filter?

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause: Non-foldable step was inserted BEFORE the date filter**

The colleague added `Table.AddColumn` (which breaks query folding) before the `Table.SelectRows` step that uses `RangeStart`/`RangeEnd`.

The query now looks like:

```powerquery
let
    Source = Sql.Database("server", "db"),
    FactSales = Source{[Schema="dbo",Item="FactSales"]}[Data],
    WithFiscalYear = Table.AddColumn(FactSales, "FiscalYear",    // ❌ breaks folding HERE
        each if Date.Month([SaleDate]) >= 7 then Date.Year([SaleDate]) + 1 else Date.Year([SaleDate])),
    FilteredRows = Table.SelectRows(WithFiscalYear,               // ❌ filter no longer folds
        each [SaleDate] >= RangeStart and [SaleDate] < RangeEnd)
in
    FilteredRows
```

Because `Table.AddColumn` with M logic breaks folding, the `FilteredRows` step can no longer push the WHERE clause to SQL. Power BI downloads ALL rows and filters in M — defeating incremental refresh entirely.

**Fix: Move the non-foldable step AFTER the date filter**

```powerquery
let
    Source = Sql.Database("server", "db"),
    FactSales = Source{[Schema="dbo",Item="FactSales"]}[Data],
    FilteredRows = Table.SelectRows(FactSales,          // ✅ folds to SQL first
        each [SaleDate] >= RangeStart and [SaleDate] < RangeEnd),
    WithFiscalYear = Table.AddColumn(FilteredRows, "FiscalYear",  // ✅ added AFTER filter
        each if Date.Month([SaleDate]) >= 7 then Date.Year([SaleDate]) + 1 else Date.Year([SaleDate]))
in
    WithFiscalYear
```

**Verify the fix:**
1. Right-click `FilteredRows` step → "View Native Query" should show WHERE clause
2. Right-click `WithFiscalYear` step → "View Native Query" will be greyed out (expected — after the non-foldable step)
3. Publish and trigger a manual refresh — confirm only 2 days of data are re-queried (check SQL Server query logs)

**General rule:** All non-foldable M transformations must come AFTER the RangeStart/RangeEnd filter step.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Historical Data Correction After a Source Backfill

**Scenario:** Your source team discovered that 3 months of data (September–November 2023) had incorrect order amounts due to a currency conversion bug. They've corrected the source data. Your Power BI dataset uses incremental refresh with a 2-year archive and 30-day refresh window. The corrupted partitions are frozen and won't be touched by the normal refresh. How do you get the corrected data into Power BI without running a full refresh (which would take 6 hours)?

<details>
<summary>💡 Hint</summary>

You need to target specific historical partitions for refresh without triggering a full dataset refresh. Think about the Enhanced Refresh REST API and XMLA endpoint.

</details>

<details>
<summary>✅ Solution</summary>

**The Problem:**

Standard incremental refresh only refreshes partitions within the configured refresh window (last 30 days). The corrupted months (Sep-Nov 2023) are in frozen historical partitions:
- `FactSales-202309` — frozen
- `FactSales-202310` — frozen
- `FactSales-202311` — frozen

**Solution 1: Enhanced Refresh REST API (Recommended)**

Use the Power BI REST API's Enhanced Refresh to target only the corrupted partitions:

```http
POST https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/refreshes
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "type": "Enhanced",
  "commitMode": "transactional",
  "maxParallelism": 3,
  "retryCount": 2,
  "objects": [
    {
      "table": "FactSales",
      "partition": "FactSales-202309"
    },
    {
      "table": "FactSales",
      "partition": "FactSales-202310"
    },
    {
      "table": "FactSales",
      "partition": "FactSales-202311"
    }
  ]
}
```

This refreshes ONLY the 3 targeted months — Power BI queries the source for Sep–Nov 2023 and replaces those partition segments. All other partitions remain untouched.

**Estimated time:** ~15-20 minutes for 3 months vs. 6 hours for full refresh.

**Solution 2: XMLA Endpoint via PowerShell**

```powershell
# Connect to XMLA endpoint using Analysis Services module
$server = New-Object Microsoft.AnalysisServices.Tabular.Server
$server.Connect("powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName")

$db = $server.Databases["DatasetName"]
$table = $db.Model.Tables["FactSales"]

# Refresh targeted partitions
$partitionsToRefresh = @("FactSales-202309", "FactSales-202310", "FactSales-202311")
foreach ($partitionName in $partitionsToRefresh) {
    $partition = $table.Partitions[$partitionName]
    if ($null -ne $partition) {
        $partition.RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
        Write-Host "Queued: $partitionName"
    }
}

$db.Model.SaveChanges()
Write-Host "Partition refresh submitted"
$server.Disconnect()
```

**Prevention for the future:**

Add the **"Detect data changes"** feature using a `ModifiedAt` column so future corrections are automatically detected:

```
Incremental Refresh Policy:
✅ Detect data changes
    Column: LastModifiedAt
```

With this enabled, Power BI checks `MAX(LastModifiedAt)` for each partition during refresh. When the source team corrects data and the `LastModifiedAt` updates, Power BI will automatically detect the change and re-refresh that partition — even within the normal scheduled refresh cycle.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What are the two required parameters for incremental refresh?" — `RangeStart` and `RangeEnd`, both of type `Date/Time` (not just Date). They must be named exactly this way. Power BI replaces their values at refresh time with the calculated date boundaries for each partition being refreshed.

> **Tip 2:** "What happens if query folding breaks with incremental refresh?" — If the RangeStart/RangeEnd filter doesn't fold to the source, Power BI downloads the entire table for each partition and filters in M. This effectively turns every partition refresh into a full table scan — worse than no incremental refresh at all. Always verify folding by checking "View Native Query" in Power Query.

> **Tip 3:** "Is incremental refresh available in Power BI Pro?" — No. Incremental refresh requires Power BI Premium capacity or Premium Per User (PPU). In a Pro workspace, you can define the incremental refresh policy in Power BI Desktop, but it's ignored when published — the dataset does a full refresh instead. This is a common interview gotcha.
