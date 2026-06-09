---
title: "Dataflows — Real-World Patterns"
topic: power-bi
subtopic: dataflows
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [power-bi, dataflows, interview, real-world, production]
---

# Dataflows — Real-World Patterns

## Pattern 1: Foundation Dataflow for Shared Dimensions

**Scenario**: An enterprise has 20 Power BI reports built by 5 different teams. Each team is independently loading customer and product data with slightly different transformations — causing inconsistent numbers across reports. Centralize this with a Foundation Dataflow.

### Foundation Dataflow: Power Query M

```powerquery
// Entity: DimCustomer_Foundation
// Source: Salesforce CRM via API
let
    // Load from Salesforce
    Source = Salesforce.Data(
        "https://mycompany.my.salesforce.com/",
        [ApiVersion = "56.0"]
    ),
    AccountsTable = Source{[Name="Account"]}[Data],

    // Select and standardize columns
    Selected = Table.SelectColumns(AccountsTable, {
        "Id", "Name", "BillingCountry", "BillingState",
        "Industry", "AnnualRevenue", "NumberOfEmployees",
        "Type", "CreatedDate", "LastModifiedDate"
    }),

    // Rename to canonical names
    Renamed = Table.RenameColumns(Selected, {
        {"Id", "CustomerKey"},
        {"Name", "CustomerName"},
        {"BillingCountry", "Country"},
        {"BillingState", "State"},
        {"AnnualRevenue", "AnnualRevenueUSD"},
        {"NumberOfEmployees", "EmployeeCount"},
        {"Type", "AccountType"},
        {"CreatedDate", "CreatedAt"},
        {"LastModifiedDate", "UpdatedAt"}
    }),

    // Standardize country names
    StandardizedCountry = Table.TransformColumns(Renamed, {
        {"Country", each
            if _ = "US" or _ = "United States of America" then "United States"
            else if _ = "UK" or _ = "Great Britain" then "United Kingdom"
            else _, type text}
    }),

    // Segment classification
    WithSegment = Table.AddColumn(StandardizedCountry, "CustomerSegment",
        each if [EmployeeCount] >= 1000 then "Enterprise"
             else if [EmployeeCount] >= 100 then "Mid-Market"
             else "SMB",
        type text),

    // Type enforcement
    Typed = Table.TransformColumnTypes(WithSegment, {
        {"CustomerKey", type text},
        {"CustomerName", type text},
        {"Country", type text},
        {"State", type text},
        {"AnnualRevenueUSD", type nullable number},
        {"EmployeeCount", type nullable number},
        {"AccountType", type text},
        {"CustomerSegment", type text},
        {"CreatedAt", type datetimezone},
        {"UpdatedAt", type datetimezone}
    })
in
    Typed
```

### Analytical Dataflow: Linking to Foundation

```powerquery
// Entity: FactSales_Enriched (in Sales Analytics Dataflow)
// Links to DimCustomer_Foundation from Foundation Dataflow

let
    // Standard entity: load raw sales
    SalesSource = Sql.Database("sales-server", "SalesDB"),
    RawSales = SalesSource{[Schema="dbo", Item="Orders"]}[Data],
    FilteredSales = Table.SelectRows(RawSales,
        each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd),

    // Link to Foundation DimCustomer (Linked Entity reference in Power Query Online)
    // In the dataflow UI, you reference the linked entity by name:
    CustomerDim = #"DimCustomer_Foundation",  // Linked from Foundation Dataflow

    // Join sales with customer dimension
    JoinedData = Table.NestedJoin(
        FilteredSales, {"CustomerID"},
        CustomerDim, {"CustomerKey"},
        "CustomerInfo", JoinKind.LeftOuter
    ),
    ExpandedCustomer = Table.ExpandTableColumn(JoinedData, "CustomerInfo",
        {"CustomerName", "Country", "CustomerSegment"},
        {"CustomerName", "CustomerCountry", "CustomerSegment"}
    )
in
    ExpandedCustomer
```

---

## Pattern 2: Event-Driven Dataflow with Power Automate

**Scenario**: Sales data files arrive in SharePoint every hour. Each file should trigger a dataflow refresh, then a dataset refresh, then send a success/failure Teams notification.

### Power Automate Flow

```json
{
  "name": "HourlySalesRefreshFlow",
  "trigger": {
    "type": "SharePoint.WhenFileCreated",
    "inputs": {
      "siteAddress": "https://company.sharepoint.com/sites/Sales",
      "libraryName": "Hourly Uploads"
    }
  },
  "actions": [
    {
      "name": "RefreshDataflow",
      "type": "PowerBI.RefreshDataflow",
      "inputs": {
        "workspaceId": "<workspace-guid>",
        "dataflowId": "<dataflow-guid>"
      }
    },
    {
      "name": "WaitForDataflow",
      "type": "Until",
      "condition": "@not(equals(variables('DataflowStatus'), 'In Progress'))",
      "timeout": "PT30M",
      "actions": [
        {
          "name": "CheckDataflowStatus",
          "type": "PowerBI.GetDataflowTransactions",
          "inputs": {
            "workspaceId": "<workspace-guid>",
            "dataflowId": "<dataflow-guid>"
          }
        },
        {
          "name": "SetStatus",
          "type": "SetVariable",
          "inputs": {
            "name": "DataflowStatus",
            "value": "@first(outputs('CheckDataflowStatus')['body']['value'])['status']"
          }
        },
        {
          "name": "Delay1Min",
          "type": "Wait",
          "inputs": { "interval": { "count": 1, "unit": "Minute" } }
        }
      ]
    },
    {
      "name": "ConditionalRefreshDataset",
      "type": "If",
      "condition": "@equals(variables('DataflowStatus'), 'Success')",
      "ifTrue": [
        {
          "name": "RefreshDataset",
          "type": "PowerBI.RefreshDataset",
          "inputs": {
            "workspaceId": "<workspace-guid>",
            "datasetId": "<dataset-guid>"
          }
        },
        {
          "name": "NotifySuccess",
          "type": "Teams.SendMessage",
          "inputs": {
            "recipient": "data-team@company.com",
            "message": "✅ Hourly sales refresh completed successfully at @{utcNow()}"
          }
        }
      ],
      "ifFalse": [
        {
          "name": "NotifyFailure",
          "type": "Teams.SendMessage",
          "inputs": {
            "recipient": "data-team@company.com",
            "message": "❌ Dataflow refresh FAILED at @{utcNow()}. Check Power BI Service refresh history."
          }
        }
      ]
    }
  ]
}
```

---

## Pattern 3: Dataflow Gen2 to Fabric Lakehouse

**Scenario**: Migrating from Gen1 dataflows to Fabric. Configure Dataflow Gen2 to load transformed data into a Fabric Lakehouse and enable Direct Lake queries.

### Dataflow Gen2 Configuration

```powerquery
// Dataflow Gen2: Entity SalesOrders
// Output destination: Fabric Lakehouse table
let
    // Source
    Source = Sql.Database("source-server", "SourceDB"),
    RawOrders = Source{[Schema="dbo", Item="Orders"]}[Data],

    // Filter for incremental refresh
    Filtered = Table.SelectRows(RawOrders,
        each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd),

    // Transformations
    WithFiscalYear = Table.AddColumn(Filtered, "FiscalYear",
        each if Date.Month([OrderDate]) >= 7
             then Date.Year([OrderDate]) + 1
             else Date.Year([OrderDate]),
        Int64.Type),

    Typed = Table.TransformColumnTypes(WithFiscalYear, {
        {"OrderID", Int64.Type},
        {"CustomerKey", Int64.Type},
        {"OrderDate", type date},
        {"OrderAmount", type number},
        {"FiscalYear", Int64.Type}
    })
in
    Typed

// Output destination settings (configured in UI):
// Destination: Lakehouse
// Workspace: [Select Fabric workspace]
// Lakehouse: [Select Lakehouse]
// Table: SalesOrders
// Update method: Replace (or Append for incremental)
```

### Semantic Model with Direct Lake

After Dataflow Gen2 loads data to the Lakehouse, create a Fabric semantic model:

```
Fabric Portal → Lakehouse → New semantic model
  → Select tables: SalesOrders, DimProduct, DimCustomer, DimDate
  → Create relationships (same as Power BI Desktop model view)
  → Create measures

Storage mode: Direct Lake (automatic for Fabric semantic models)
  → Reads directly from Delta files in Lakehouse
  → No VertiPaq copy needed
  → Near-Import performance (Delta file caching in Fabric memory)
```

```dax
-- DAX measures work the same in Fabric semantic models
Total Revenue = SUM(SalesOrders[OrderAmount])

Revenue YTD =
TOTALYTD(SUM(SalesOrders[OrderAmount]), DimDate[Date])
```

---

## Pattern 4: Multi-Source Dataflow with Error Handling

**Scenario**: A dataflow consolidates data from 3 sources (SQL Server, SharePoint, REST API). Any single source failure should not block the others from completing. Implement per-entity error handling.

```powerquery
// Entity: ProductMaster (from SQL Server — primary source)
let
    // This entity must succeed — it's the authoritative product list
    Source = Sql.Database("products-server", "ProductDB"),
    Products = Source{[Schema="dbo", Item="Products"]}[Data],
    Selected = Table.SelectColumns(Products, {"ProductKey", "ProductName", "Category", "SubCategory", "Price"})
in
    Selected

// Entity: ProductReviews (from REST API — optional enrichment)
let
    // If API fails, return empty table with correct schema
    ApiResult = try
        let
            Url = "https://reviews-api.company.com/products",
            Response = Web.Contents(Url, [Headers = [Authorization = "Bearer " & ReviewsApiToken]]),
            Parsed = Json.Document(Response),
            AsTable = Table.FromList(Parsed[reviews], Splitter.SplitByNothing()),
            Expanded = Table.ExpandRecordColumn(AsTable, "Column1", {"productId", "avgRating", "reviewCount"})
        in
            Expanded
    otherwise
        // Return empty table with correct schema if API is down
        #table(
            type table [productId = text, avgRating = number, reviewCount = Int64.Type],
            {}  // Empty rows
        )
in
    ApiResult

// Entity: ProductInventory (from SharePoint list — supplemental)
let
    SharePointResult = try
        let
            Source = SharePoint.Tables("https://company.sharepoint.com/sites/Inventory", [ApiVersion=15]),
            InventoryList = Source{[Title="Current Inventory"]}[Items],
            Selected = Table.SelectColumns(InventoryList, {"ProductKey", "StockLevel", "LastUpdated"})
        in
            Selected
    otherwise
        #table(
            type table [ProductKey = text, StockLevel = Int64.Type, LastUpdated = type date],
            {}
        )
in
    SharePointResult

// Computed entity: ProductMaster_Enriched (joins all three)
let
    Base = #"ProductMaster",
    WithReviews = Table.NestedJoin(
        Base, {"ProductKey"},
        #"ProductReviews", {"productId"},
        "Reviews", JoinKind.LeftOuter
    ),
    ExpandedReviews = Table.ExpandTableColumn(WithReviews, "Reviews",
        {"avgRating", "reviewCount"}, {"AvgRating", "ReviewCount"}),
    WithInventory = Table.NestedJoin(
        ExpandedReviews, {"ProductKey"},
        #"ProductInventory", {"ProductKey"},
        "Inventory", JoinKind.LeftOuter
    ),
    ExpandedInventory = Table.ExpandTableColumn(WithInventory, "Inventory",
        {"StockLevel"}, {"StockLevel"}),
    // Fill nulls for products with no reviews or inventory data
    Filled = Table.FillDown(ExpandedInventory, {}),
    WithDefaults = Table.ReplaceValue(
        Table.ReplaceValue(ExpandedInventory, null, 0, Replacer.ReplaceValue, {"AvgRating", "ReviewCount"}),
        null, -1, Replacer.ReplaceValue, {"StockLevel"}
    )
in
    WithDefaults
```
