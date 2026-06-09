---
title: "Incremental Refresh — Fundamentals"
topic: power-bi
subtopic: incremental-refresh
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, incremental-refresh, interview, fundamentals]
---

# Incremental Refresh — Fundamentals

## What Is Incremental Refresh?

By default, Power BI refreshes an entire dataset on every scheduled refresh — even if only the last day's data changed. For large tables, this is inefficient and slow.

**Incremental refresh** lets Power BI refresh only the most recent portion of a table (the "refresh range") while keeping older historical data intact in read-only "frozen" partitions.

```
Full table:  2020 | 2021 | 2022 | 2023 | 2024-Jan...Nov | 2024-Dec
On refresh:  ✅kept | ✅kept | ✅kept | ✅kept | ✅kept        | 🔄refreshed
```

---

## Why Use Incremental Refresh?

| Without Incremental Refresh | With Incremental Refresh |
|---|---|
| Re-imports all 50M rows every night | Imports only last 30 days (~100K rows) |
| 3-hour refresh window | 10-minute refresh window |
| High Azure/SQL cost (full scan) | Low cost (date-filtered scan) |
| Timeout risk for large datasets | Reliable, fast completion |
| No historical partitioning | Historical data in frozen partitions |

---

## The Two Required Parameters

Incremental refresh requires exactly **two Power Query parameters**:

| Parameter | Type | Purpose |
|---|---|---|
| `RangeStart` | Date/Time | Start of the data range to load |
| `RangeEnd` | Date/Time | End of the data range to load |

**Critical rules:**
- Parameters must be exactly named `RangeStart` and `RangeEnd`
- They must be of type `Date/Time` (not Date, not Text)
- The query must use these parameters to filter the date column
- The filter must **fold to the data source** (query folding required)

---

## Setting Up Incremental Refresh — Step by Step

### Step 1: Create RangeStart and RangeEnd Parameters

In Power Query Editor:
1. **Home** → **Manage Parameters** → **New Parameter**
2. Create `RangeStart`:
   - Name: `RangeStart`
   - Type: `Date/Time`
   - Current Value: `1/1/2024 12:00:00 AM` (any past date for testing)
3. Create `RangeEnd`:
   - Name: `RangeEnd`
   - Type: `Date/Time`
   - Current Value: `12/31/2024 12:00:00 AM`

### Step 2: Filter the Table Using Parameters

In your data query, add a filter step using the parameters:

```powerquery
let
    Source = Sql.Database("server", "salesdb"),
    FactSales = Source{[Schema="dbo", Item="FactSales"]}[Data],

    // Filter using RangeStart and RangeEnd parameters
    FilteredRows = Table.SelectRows(
        FactSales,
        each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd
    )
in
    FilteredRows
```

**Important**: The filter on `[OrderDate]` must use the datetime column directly. Calculated column comparisons may break query folding.

### Step 3: Define the Incremental Refresh Policy

In Power BI Desktop:
1. Right-click the table in the **Fields** pane → **Incremental Refresh**
2. Configure:

```
✅ Define incremental refresh for this table

Archive data starting: 3 Year(s) before refresh date
Refresh data in the last: 30 Day(s)
```

### Step 4: Publish to Power BI Service

Publish the report to Power BI Service. The incremental refresh policy is applied automatically during the first full refresh in the Service.

---

## Understanding the Partitioning

After publishing, Power BI creates multiple partitions for the table:

```
FactSales table partitions:
┌─────────────────────┬───────────────────────────────────────┐
│ Partition           │ Content                               │
├─────────────────────┼───────────────────────────────────────┤
│ Historical-2021     │ All 2021 data — frozen, never refreshed│
│ Historical-2022     │ All 2022 data — frozen, never refreshed│
│ Historical-2023     │ All 2023 data — frozen, never refreshed│
│ Recent-2024-Q1      │ Q1 2024 — frozen after Q2 started     │
│ Recent-2024-Q2      │ Q2 2024 — frozen after Q3 started     │
│ Recent-2024-Q3      │ Q3 2024 — frozen after Q4 started     │
│ Recent-2024-Oct     │ October 2024 — frozen last month      │
│ Recent-2024-Nov     │ November 2024 — frozen yesterday      │
│ Recent-2024-Dec     │ December 2024 — REFRESHED each run    │
└─────────────────────┴───────────────────────────────────────┘
```

On each scheduled refresh, only the "Recent" period partitions (within the refresh range) are deleted and re-created. Historical partitions remain untouched.

---

## Refresh Policy Parameters Explained

```
Archive data starting: X [Year/Month/Day/Hour] before refresh date
```
→ How far back total data goes. Data older than this is deleted from the model.

```
Refresh data in the last: Y [Year/Month/Day/Hour]
```
→ The window that is refreshed on every scheduled refresh.

### Example Configurations

| Scenario | Archive | Refresh |
|---|---|---|
| 3 years history, refresh last 30 days | 3 Years | 30 Days |
| 5 years history, refresh last month | 5 Years | 1 Month |
| 1 year history, near-real-time (refresh last hour) | 1 Year | 1 Hour |
| 2 years history, refresh last week | 2 Years | 7 Days |

---

## What Happens on First Refresh?

When the dataset is first refreshed in the Service after publishing, Power BI:

1. Splits the archive range into **monthly** (or **daily**) partitions
2. Queries the source for each partition separately
3. This is the "historical load" — happens once and can take hours
4. Subsequent refreshes only update the most recent partition(s)

**Tip**: Schedule the first refresh during off-hours and expect it to take longer than normal.

---

## Incremental Refresh Requirements

| Requirement | Details |
|---|---|
| Power BI Premium or PPU | Incremental refresh is a Premium feature |
| Query folding | The RangeStart/RangeEnd filter must fold to native SQL |
| Date/Time column in source | The partition column must be a datetime column |
| Stable partition key | The date column used must not change values between refreshes |

**Note**: Power BI Pro workspaces do not support incremental refresh. You need at least a Premium Per User (PPU) license.

---

## Common Issues

| Issue | Cause | Fix |
|---|---|---|
| Parameters not recognized | Not named exactly `RangeStart`/`RangeEnd` | Rename to match exactly |
| No query folding | Custom M steps before the date filter | Move date filter before non-foldable steps |
| Date type mismatch | Parameter is `Date` not `Date/Time` | Change parameter type to `Date/Time` |
| Historical data missing | Archive range too short | Increase archive range |
| Refresh takes as long as before | Filter not folding to source | Check "View Native Query" in Power Query |

---

## Summary

- Incremental refresh avoids reloading historical data on every refresh
- Requires `RangeStart` and `RangeEnd` parameters of type `Date/Time`
- The date filter **must fold** to the source query
- Define the policy in Power BI Desktop; it executes in Power BI Service
- **Premium or PPU** workspace is required
- Historical data is stored in frozen partitions; only the refresh window is updated
