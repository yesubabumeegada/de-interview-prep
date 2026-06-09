---
title: "Incremental Refresh — Real-World Patterns"
topic: power-bi
subtopic: incremental-refresh
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, incremental-refresh, interview, real-world, production]
---

# Incremental Refresh — Real-World Patterns

## Pattern 1: E-Commerce Sales Table with 5 Years of History

**Scenario**: An e-commerce company has a `FactOrders` table in Azure SQL Database with 200M rows going back 5 years. Full refresh takes 4 hours and often times out. Implement incremental refresh to refresh only the last 7 days.

### Step 1: Power Query Setup

```powerquery
let
    Source = Sql.Database(
        "ecommerce-sql.database.windows.net",
        "EcommerceDB",
        [CredentialType = DatabaseCredentialType.UsernamePassword]
    ),
    FactOrders = Source{[Schema="dbo", Item="FactOrders"]}[Data],

    // CRITICAL: This filter must fold to SQL
    // RangeStart and RangeEnd are Power Query parameters of type Date/Time
    FilteredByDate = Table.SelectRows(
        FactOrders,
        each [OrderCreatedAt] >= RangeStart and [OrderCreatedAt] < RangeEnd
    ),

    // Type enforcement (after filter to minimize rows processed)
    Typed = Table.TransformColumnTypes(FilteredByDate, {
        {"OrderID", Int64.Type},
        {"CustomerKey", Int64.Type},
        {"OrderCreatedAt", type datetime},
        {"OrderAmount", type number},
        {"Status", type text}
    })
in
    Typed
```

### Step 2: Incremental Refresh Policy Configuration

```
Table: FactOrders
Archive data starting: 5 Years before refresh date
Refresh data in the last: 7 Days

✅ Detect data changes
    Column: LastModifiedAt
    (SQL query: SELECT MAX(LastModifiedAt) FROM dbo.FactOrders WHERE OrderCreatedAt >= @RangeStart AND OrderCreatedAt < @RangeEnd)
```

### Step 3: Validation DAX Measures

```dax
-- Partition health check: count of rows per month
Rows This Month =
CALCULATE(
    COUNTROWS(FactOrders),
    DATESMTD(DimDate[Date])
)

-- Data latency: hours since newest order
Data Latency (Hours) =
DATEDIFF(
    MAX(FactOrders[OrderCreatedAt]),
    NOW(),
    HOUR
)

-- Alert if data is stale
Data Freshness Status =
IF(
    [Data Latency (Hours)] > 25,
    "⚠️ Stale - " & TEXT([Data Latency (Hours)], "0") & " hours old",
    "✅ Fresh - " & TEXT([Data Latency (Hours)], "0") & " hours old"
)
```

---

## Pattern 2: IoT Sensor Data with Hourly Partitions

**Scenario**: A manufacturing company collects sensor readings every minute — 50M rows/day. They need dashboards with data no more than 1 hour old, using the hybrid DirectQuery real-time feature.

### Power Query Configuration

```powerquery
let
    Source = Sql.Database("iot-warehouse.database.windows.net", "SensorDB"),
    SensorReadings = Source{[Schema="dbo", Item="FactSensorReadings"]}[Data],

    // Date filter for incremental refresh (must fold)
    Filtered = Table.SelectRows(
        SensorReadings,
        each [ReadingTimestamp] >= RangeStart and [ReadingTimestamp] < RangeEnd
    ),

    // Select only needed columns (reduces partition size)
    Selected = Table.SelectColumns(Filtered, {
        "ReadingID", "DeviceID", "SensorType",
        "ReadingTimestamp", "SensorValue", "IsAlert"
    })
in
    Selected
```

### Incremental Refresh + Real-Time Configuration

```
Table: FactSensorReadings
Archive data starting: 1 Year before refresh date
Refresh data in the last: 3 Days
✅ Get the latest data in real time with DirectQuery
    (Enabled for Premium — serves last few hours via DirectQuery)
```

### DAX Measures for Real-Time Dashboard

```dax
-- Current hour readings count (served via DirectQuery)
Current Hour Readings =
CALCULATE(
    COUNTROWS(FactSensorReadings),
    FILTER(
        DimDate,
        DimDate[Date] = TODAY() &&
        HOUR(MAX(FactSensorReadings[ReadingTimestamp])) = HOUR(NOW())
    )
)

-- Alert rate in last 24 hours (uses hybrid: recent import + live DQ)
Alert Rate 24h =
VAR TotalReadings =
    CALCULATE(
        COUNTROWS(FactSensorReadings),
        FILTER(ALL(DimDate), DimDate[Date] >= DATE(YEAR(TODAY()), MONTH(TODAY()), DAY(TODAY())) - 1)
    )
VAR AlertReadings =
    CALCULATE(
        COUNTROWS(FactSensorReadings),
        FactSensorReadings[IsAlert] = TRUE(),
        FILTER(ALL(DimDate), DimDate[Date] >= DATE(YEAR(TODAY()), MONTH(TODAY()), DAY(TODAY())) - 1)
    )
RETURN
    DIVIDE(AlertReadings, TotalReadings, 0)
```

---

## Pattern 3: Multi-Table Incremental Refresh with Dependency Management

**Scenario**: The dataset has three tables that must be refreshed in dependency order: `DimProduct` (full), `FactSales` (incremental), and `AggSalesByMonth` (recomputed from FactSales). Use the Enhanced Refresh API in Azure Data Factory.

### ADF Pipeline Design

```json
// Pipeline: RefreshSalesDashboard
{
  "activities": [
    {
      "name": "RefreshDimProduct",
      "type": "PowerBIRefreshDataset",
      "typeProperties": {
        "datasetId": "<dataset-id>",
        "workspaceId": "<workspace-id>",
        "refreshType": "Enhanced",
        "objects": [{"table": "DimProduct"}],
        "maxParallelism": 1
      }
    },
    {
      "name": "RefreshFactSales",
      "type": "PowerBIRefreshDataset",
      "dependsOn": [{"activity": "RefreshDimProduct", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "datasetId": "<dataset-id>",
        "workspaceId": "<workspace-id>",
        "refreshType": "Enhanced",
        "objects": [
          {"table": "FactSales"}
        ],
        "maxParallelism": 2
      }
    },
    {
      "name": "RefreshAggSalesByMonth",
      "type": "PowerBIRefreshDataset",
      "dependsOn": [{"activity": "RefreshFactSales", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "datasetId": "<dataset-id>",
        "workspaceId": "<workspace-id>",
        "refreshType": "Enhanced",
        "objects": [{"table": "AggSalesByMonth"}],
        "maxParallelism": 1
      }
    }
  ]
}
```

### Force-Refresh a Historical Partition After Backfill

When the source team corrects historical data:

```powershell
# PowerShell: Force refresh a specific month's partition
$body = @{
    type = "Enhanced"
    commitMode = "transactional"
    objects = @(
        @{
            table = "FactSales"
            partition = "FactSales-202311"  # November 2023 backfill
        }
    )
} | ConvertTo-Json

Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/datasets/$datasetId/refreshes" `
    -Method Post `
    -Headers @{Authorization = "Bearer $accessToken"} `
    -ContentType "application/json" `
    -Body $body
```

---

## Pattern 4: Incremental Refresh Monitoring Dashboard

**Scenario**: Build a meta-dashboard that shows the health of incremental refresh across all production datasets — partition counts, last refresh times, and failure alerts.

```powerquery
// Power Query: Pull refresh history via Power BI REST API
let
    AccessToken = PowerBIToken,  // parameter with service principal token

    // Get all datasets in workspace
    DatasetsUrl = "https://api.powerbi.com/v1.0/myorg/groups/" & WorkspaceId & "/datasets",
    DatasetsResponse = Json.Document(
        Web.Contents(DatasetsUrl, [Headers = [Authorization = "Bearer " & AccessToken]])
    ),
    Datasets = Table.FromList(DatasetsResponse[value], Splitter.SplitByNothing()),
    DatasetsExpanded = Table.ExpandRecordColumn(Datasets, "Column1",
        {"id", "name", "isRefreshable", "isOnPremGatewayRequired"}),

    // For each refreshable dataset, get refresh history
    GetRefreshHistory = (datasetId as text) as table =>
    let
        Url = "https://api.powerbi.com/v1.0/myorg/datasets/" & datasetId & "/refreshes?$top=5",
        Response = Json.Document(Web.Contents(Url, [Headers = [Authorization = "Bearer " & AccessToken]])),
        History = Table.FromList(Response[value], Splitter.SplitByNothing()),
        Expanded = Table.ExpandRecordColumn(History, "Column1",
            {"id", "refreshType", "startTime", "endTime", "status", "serviceExceptionJson"}),
        WithDatasetId = Table.AddColumn(Expanded, "DatasetId", each datasetId, type text)
    in
        WithDatasetId,

    RefreshableDatasets = Table.SelectRows(DatasetsExpanded, each [isRefreshable] = true),
    AllRefreshHistories = Table.Combine(
        List.Transform(Table.Column(RefreshableDatasets, "id"), GetRefreshHistory)
    ),

    // Join dataset names
    WithNames = Table.NestedJoin(AllRefreshHistories, {"DatasetId"}, DatasetsExpanded, {"id"}, "DS"),
    FinalExpanded = Table.ExpandTableColumn(WithNames, "DS", {"name"}, {"DatasetName"}),

    // Parse timestamps
    Typed = Table.TransformColumnTypes(FinalExpanded, {
        {"startTime", type datetimezone},
        {"endTime", type nullable datetimezone}
    })
in
    Typed
```

```dax
-- KPI: Datasets with failed refresh in last 24 hours
Failed Refreshes 24h =
CALCULATE(
    DISTINCTCOUNT(RefreshHistory[DatasetId]),
    RefreshHistory[status] = "Failed",
    RefreshHistory[startTime] >= NOW() - 1
)

-- Average refresh duration (minutes)
Avg Refresh Duration =
AVERAGEX(
    FILTER(RefreshHistory, RefreshHistory[status] = "Completed"),
    DATEDIFF(RefreshHistory[startTime], RefreshHistory[endTime], MINUTE)
)
```
