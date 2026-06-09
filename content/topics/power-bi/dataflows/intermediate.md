---
title: "Dataflows — Intermediate"
topic: power-bi
subtopic: dataflows
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, dataflows, interview, intermediate]
---

# Dataflows — Intermediate

## Dataflow Architecture Patterns

### Pattern 1: Medallion Architecture with Dataflows

Map dataflows to Bronze/Silver/Gold layers:

```
Bronze Dataflow (Raw)
  → Raw entity: SalesOrders_Raw (direct from SQL Server, no transforms)
  → Raw entity: Products_Raw (from API)
  → Raw entity: Customers_Raw (from SharePoint)

Silver Dataflow (Cleaned)
  → Linked entity: SalesOrders_Raw (from Bronze)
  → Computed entity: SalesOrders_Cleaned (type conversions, null handling)
  → Computed entity: Products_Cleaned (standardize category names)

Gold Dataflow (Conformed)
  → Linked entity: SalesOrders_Cleaned (from Silver)
  → Computed entity: FactSales (join + business rules applied)
  → Computed entity: DimProduct (conformed dimension)
```

**Benefits:**
- Bronze stays raw — full audit trail
- Silver transformations applied once
- Gold entities are ready for direct model connection

### Pattern 2: Shared Foundation + Analytical Domains

```
Foundation Dataflow (IT-owned)
  ├── DimDate (canonical date table)
  ├── DimCustomer (from CRM)
  ├── DimEmployee (from HR)
  └── ExchangeRates (from finance API)

Sales Analytics Dataflow (Sales team)
  ├── [Linked] DimCustomer
  ├── [Linked] DimDate
  └── [Standard] FactSales (from sales DB)

Finance Analytics Dataflow (Finance team)
  ├── [Linked] DimEmployee
  ├── [Linked] ExchangeRates
  └── [Standard] FactGL (from ERP)
```

---

## Computed Entities Deep Dive

Computed entities allow in-place transformations after data lands in the dataflow storage. They do NOT re-query the source.

### When to Use Computed Entities

```
Use computed entities when:
- Transformation is expensive (aggregation, join, type conversion)
- Multiple downstream entities need the same cleaned base
- Source queries should be minimized (rate-limited APIs, expensive cloud queries)

Avoid when:
- Simple column selection (use Power Query filtering on standard entity)
- The computed entity adds no value over the standard entity
```

### Computed Entity Examples

```powerquery
// Standard entity: load raw data from source
// Entity: SalesOrders_Raw
let
    Source = Sql.Database("server", "db"),
    RawOrders = Source{[Schema="dbo", Item="SalesOrders"]}[Data],
    DateFiltered = Table.SelectRows(RawOrders, each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd)
in
    DateFiltered

// Computed entity: built from SalesOrders_Raw (no SQL query)
// Entity: SalesOrders_Enriched
let
    // Reference the standard entity — no source query!
    Source = #"SalesOrders_Raw",

    // Apply complex transformations on already-loaded data
    WithFiscalYear = Table.AddColumn(Source, "FiscalYear",
        each if Date.Month([OrderDate]) >= 7
             then Date.Year([OrderDate]) + 1
             else Date.Year([OrderDate]),
        Int64.Type),

    WithOrderBand = Table.AddColumn(WithFiscalYear, "OrderBand",
        each if [OrderAmount] >= 10000 then "Enterprise"
             else if [OrderAmount] >= 1000 then "SMB"
             else "Consumer",
        type text),

    // Join with ExchangeRates entity (also in the same dataflow)
    JoinedRates = Table.NestedJoin(
        WithOrderBand, {"Currency"},
        #"ExchangeRates_Raw", {"FromCurrency"},
        "Rates", JoinKind.LeftOuter
    ),
    ExpandedRates = Table.ExpandTableColumn(JoinedRates, "Rates", {"USDRate"}),

    WithUSDAmount = Table.AddColumn(ExpandedRates, "AmountUSD",
        each [OrderAmount] * (if [USDRate] = null then 1 else [USDRate]),
        type number)
in
    WithUSDAmount
```

---

## AI Functions in Dataflows

Power BI dataflows (Premium) expose AI capabilities directly in Power Query Online:

### Cognitive Services Text Analytics

```powerquery
// Sentiment Analysis on customer feedback
let
    Source = #"CustomerFeedback",
    SentimentResults = Table.AddColumn(Source, "Sentiment",
        each Text.SentimentScore([FeedbackText], "en")  // Returns 0-1 score
    ),
    WithLabel = Table.AddColumn(SentimentResults, "SentimentLabel",
        each if [Sentiment] >= 0.7 then "Positive"
             else if [Sentiment] <= 0.3 then "Negative"
             else "Neutral",
        type text)
in
    WithLabel

// Key Phrase Extraction
KeyPhrases = Table.AddColumn(Source, "KeyPhrases",
    each Text.KeyPhrases([FeedbackText], "en")
)
```

### Azure Machine Learning Integration

```powerquery
// Call a deployed Azure ML model from within a dataflow
let
    Source = #"CustomerData",
    // Score customers with churn propensity model
    ChurnScores = Table.AddColumn(Source, "ChurnProbability",
        each AzureML.Models(
            "https://myml.azureml.net/api/v1/service/churn-model/score",
            [CustomerAge = [Age], Tenure = [TenureMonths], Balance = [Balance]],
            [ApiKey = "Bearer " & MLApiKey]
        )
    )
in
    ChurnScores
```

---

## Incremental Refresh for Dataflows

Incremental refresh for dataflows follows the same pattern as datasets but is configured differently.

### Configuration in Power Query Online

```powerquery
// 1. Create parameters RangeStart and RangeEnd in the dataflow
// 2. Apply to the filter step
let
    Source = Sql.Database("server", "db"),
    SalesOrders = Source{[Schema="dbo", Item="SalesOrders"]}[Data],
    Filtered = Table.SelectRows(
        SalesOrders,
        each [OrderDate] >= RangeStart and [OrderDate] < RangeEnd
    )
in
    Filtered

// 3. In the dataflow entity settings: Incremental Refresh toggle
// Set Archive period and Refresh period
```

### Dataflow vs Dataset Incremental Refresh

| Aspect | Dataflow | Dataset |
|---|---|---|
| Where configured | Power Query Online (entity settings) | Power BI Desktop |
| Parameters required | RangeStart, RangeEnd (same) | RangeStart, RangeEnd (same) |
| License | Premium/Fabric | Premium |
| Partition management | Automatic | Automatic + XMLA control |

---

## Dataflows and Azure Data Lake Integration

### Setting Up ADLS Gen2 for a Dataflow Workspace

```
Power BI Admin Portal:
  Tenant Settings → Dataflows → Allow dataflows to store data in Azure Data Lake Storage Gen2

Workspace Settings:
  Advanced → Azure Data Lake Storage Gen2 connection
  → Enter storage account URL: https://mystorageaccount.dfs.core.windows.net
  → Authenticate with service principal or OAuth
```

### CDM Structure in ADLS

```
Azure Data Lake Gen2 container:
└── powerbi/
    └── WorkspaceName/
        └── DataflowName/
            ├── model.json          (CDM manifest)
            └── entity/
                ├── SalesOrders/
                │   └── SalesOrders.csv (or Parquet partitions)
                └── Customers/
                    └── Customers.csv
```

### Accessing Dataflow Data from External Tools

When ADLS Gen2 is connected, the CDM data is accessible from:
- Azure Databricks (`abfss://powerbi@storageaccount.dfs.core.windows.net/...`)
- Azure Synapse Analytics (as linked storage)
- Azure Data Factory
- Any Azure service with ADLS access

```python
# Databricks: read dataflow entity as Delta/CSV
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv(
    "abfss://powerbi@mystorageaccount.dfs.core.windows.net/WorkspaceName/DataflowName/entity/SalesOrders/",
    header=True,
    inferSchema=True
)
df.show()
```

---

## Orchestration with Power Automate

Power Automate can trigger dataflow refreshes on events:

```
Scenario: When a new file lands in SharePoint → trigger dataflow refresh

Power Automate Flow:
  Trigger: When a file is created in SharePoint folder
  Action: Refresh a dataflow (Power BI connector)
    → Workspace: Select workspace
    → Dataflow: Select dataflow
    → Group ID: (auto-populated)
  Action: Wait for dataflow refresh to complete (loop check)
  Action: Trigger dataset refresh
```

### Power Automate → Dataset Dependency

```
Flow:
  1. Refresh Dataflow (Power BI connector)
  2. Wait until dataflow refresh status = "Succeeded"
     (poll: GET /groups/{id}/dataflows/{id}/transactions)
  3. Refresh Dataset (Power BI connector)
  4. Send notification email
```

---

## Summary

- **Medallion architecture** with dataflows maps naturally to Bronze/Silver/Gold layers
- **Computed entities** transform already-loaded data — no additional source queries
- **AI functions** (Cognitive Services, Azure ML) are available directly in Power Query Online
- **ADLS Gen2 integration** exposes dataflow data to the broader Azure ecosystem
- **Incremental refresh** for dataflows uses the same RangeStart/RangeEnd pattern as datasets
- Use **Power Automate** for event-driven orchestration (file arrives → refresh dataflow → refresh dataset)
- **Linked entities** from Foundation Dataflows prevent data duplication across analytical domains
