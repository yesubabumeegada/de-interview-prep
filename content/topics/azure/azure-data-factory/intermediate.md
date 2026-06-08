---
title: "Azure Data Factory — Intermediate"
topic: azure
subtopic: azure-data-factory
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, adf, pipelines, incremental-load, mapping-data-flow, parameterization]
---

# Azure Data Factory — Intermediate

## Incremental Load with Watermark Pattern

```python
# ADF Incremental Load — SQL watermark pattern
# Implemented via ADF pipeline + SQL Stored Procedure

# Step 1: Create watermark table in Azure SQL DB
CREATE TABLE watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_updated    DATETIME2
);
INSERT INTO watermark VALUES ('orders', '2000-01-01 00:00:00');

# Step 2: ADF Pipeline activities:
#   A) Lookup Activity: read current watermark
#      Query: SELECT last_updated FROM watermark WHERE table_name='orders'
#
#   B) Copy Activity: copy new records
#      Source query (parameterized):
#        SELECT * FROM orders
#        WHERE updated_at > '@{activity('LookupWatermark').output.firstRow.last_updated}'
#        AND updated_at <= '@{pipeline().TriggerTime}'
#      Sink: ADLS Gen2 / Azure SQL (destination)
#
#   C) Stored Procedure Activity: update watermark
#      Stored procedure: usp_update_watermark
#      Parameters: @table_name = 'orders', @new_watermark = '@{pipeline().TriggerTime}'

CREATE PROCEDURE usp_update_watermark
    @table_name  VARCHAR(100),
    @new_watermark DATETIME2
AS
BEGIN
    UPDATE watermark
    SET last_updated = @new_watermark
    WHERE table_name = @table_name
END

# Result: pipeline runs every hour, copies only records modified in last hour
```

---

## Parameterized Pipelines and Metadata-Driven Patterns

```json
// Metadata-driven pipeline: one pipeline handles N tables
// Store table config in Azure SQL control table

// Control table:
// | table_name | source_schema | sink_container | watermark_col | active |
// | orders     | dbo           | silver/orders  | updated_at    | 1      |
// | customers  | dbo           | silver/customers| modified_date | 1     |
// | products   | dbo           | silver/products | last_modified | 1      |

// Pipeline design:
// 1. Lookup Activity: SELECT * FROM pipeline_config WHERE active = 1
// 2. ForEach Activity: iterate over config rows
//    - Inside ForEach: Execute pipeline (child pipeline)
//    - Pass parameters: table_name, source_schema, sink_container, watermark_col
// 3. Child pipeline: parameterized Copy Activity using passed params

// Child pipeline parameters:
{
  "parameters": {
    "tableName":      { "type": "string" },
    "sourceSchema":   { "type": "string" },
    "sinkContainer":  { "type": "string" },
    "watermarkCol":   { "type": "string" }
  }
}

// Child pipeline Copy Activity source query:
// SELECT * FROM @{pipeline().parameters.sourceSchema}.@{pipeline().parameters.tableName}
// WHERE @{pipeline().parameters.watermarkCol} > '@{activity('LookupWM').output.firstRow.last_updated}'

// Benefits:
//   Adding a new table = insert one row in control table
//   No pipeline changes needed
//   All tables share same logic
```

---

## Mapping Data Flow: Complex Transformations

```
# Complete data flow: join orders + customers, aggregate by region

Source1: orders (Azure SQL)
  columns: order_id, customer_id, amount, order_date, region

Source2: customers (Azure SQL)
  columns: customer_id, name, email, country, tier

Step 1: Filter (orders)
  Condition: order_date >= 2024-01-01 AND amount > 0

Step 2: Join (orders LEFT JOIN customers)
  Left:  orders stream
  Right: customers stream
  Join type: LEFT OUTER
  Condition: orders@customer_id == customers@customer_id

Step 3: Derived Column (add enriched columns)
  revenue_tier = iif(amount >= 1000, 'high', iif(amount >= 100, 'medium', 'low'))
  full_country = iif(isNull(country), 'Unknown', country)

Step 4: Aggregate
  Group by: region, full_country, order_date (truncated to month), revenue_tier
  Aggregations:
    total_orders   = count(order_id)
    total_revenue  = sum(amount)
    avg_order_size = avg(amount)

Step 5: Assert (data quality check)
  Rule name: no_negative_revenue
  Condition: total_revenue >= 0
  Behavior: fail row / log error

Step 6: Sink → ADLS Gen2
  Format: Parquet + Snappy
  Partition by: order_date (month partition)
  File name: [partition-based]
  Update method: Truncate and load (full refresh of aggregates)

# Generated Spark code (visible in Data Flow debug tab):
# ADF translates the visual graph to PySpark and submits to Spark cluster
```

---

## Pipeline Monitoring and Error Handling

```python
# ADF Pipeline error handling patterns

# Pattern 1: Activity-level retry
# In each activity JSON:
{
  "policy": {
    "retry": 3,
    "retryIntervalInSeconds": 30,
    "secureOutput": false
  }
}

# Pattern 2: Failure path routing
# Activities connect with three dependency conditions:
#   Succeeded → next happy-path activity
#   Failed    → error handling activity (send alert, log error)
#   Completed → runs regardless (cleanup activity)
#   Skipped   → upstream was skipped

# Pattern 3: Web Activity to send failure alert
{
  "name": "AlertOnFailure",
  "type": "WebActivity",
  "dependsOn": [{"activity": "CopyOrders", "dependencyConditions": ["Failed"]}],
  "typeProperties": {
    "url": "https://prod.logic.azure.com:443/workflows/.../triggers/manual/paths/invoke",
    "method": "POST",
    "body": {
      "pipeline": "@pipeline().Pipeline",
      "runId": "@pipeline().RunId",
      "error": "@activity('CopyOrders').Error.Message",
      "time": "@pipeline().TriggerTime"
    }
  }
}

# Pattern 4: Set Variable activity for accumulating errors in ForEach
# Variables: error_count (Integer), error_messages (Array)
# In ForEach fail path: append to error_messages array
# After ForEach: If Condition — if error_count > 0 → fail pipeline

# Monitoring endpoints:
# Portal: ADF Studio → Monitor → Pipeline Runs
# Programmatic: Azure Monitor + Log Analytics
# Alerts: Azure Monitor Alerts on pipeline failure metric
```

---

## ARM Templates and CI/CD for ADF

```yaml
# ADF CI/CD with Azure DevOps

# ADF Git integration:
#   Connect ADF Studio to Azure Repos (Git)
#   Collaboration branch: main
#   Publish branch: adf_publish (auto-generated ARM templates)

# Pipeline stages:
stages:
  - stage: Build
    jobs:
      - job: ValidateADF
        steps:
          # Install ADF validation tool
          - task: Npm@1
            inputs:
              command: install
              workingDir: build
          # Run ADF pre/post deployment scripts
          - task: Npm@1
            inputs:
              command: custom
              customCommand: run build -- --rootPath "$(Build.Repository.LocalPath)/adf" --clientId "$(ClientId)" --tenantId "$(TenantId)" --subscriptionId "$(SubscriptionId)" --resourceGroup "$(ResourceGroup)" --factory "$(FactoryName)"

  - stage: DeployDev
    jobs:
      - deployment: DeployADF
        environment: dev
        steps:
          # Pre-deployment: stop triggers
          - task: AzurePowerShell@5
            inputs:
              ScriptPath: PrePostDeploymentScript.ps1
              ScriptArguments: -ArmTemplate "$(Build.ArtifactStagingDirectory)/ARMTemplateForFactory.json" -ResourceGroupName "$(ResourceGroup)" -DataFactoryName "$(FactoryName)" -predeployment $true
          # Deploy ARM template
          - task: AzureResourceManagerTemplateDeployment@3
            inputs:
              resourceGroupName: "$(ResourceGroup)"
              templateLocation: "URL of the file"
              csmFileLink: "$(Build.ArtifactStagingDirectory)/ARMTemplateForFactory.json"
              csmParametersFileLink: "$(Build.ArtifactStagingDirectory)/ARMTemplateParametersForFactory.json"
          # Post-deployment: start triggers
          - task: AzurePowerShell@5
            inputs:
              ScriptPath: PrePostDeploymentScript.ps1
              ScriptArguments: -ArmTemplate "$(Build.ArtifactStagingDirectory)/ARMTemplateForFactory.json" -ResourceGroupName "$(ResourceGroup)" -DataFactoryName "$(FactoryName)" -predeployment $false
```

---

## Interview Tips

> **Tip 1:** "How do you handle incremental loads in ADF without CDC?" — The watermark pattern: store `last_loaded_at` in a control table, use a Lookup Activity to read it, pass it as a parameter to Copy Activity's source query (`WHERE updated_at > @last_loaded_at AND updated_at <= @pipeline().TriggerTime`), then update the watermark with Stored Procedure Activity after successful copy. For tables with no `updated_at` column, use `CHECKSUM_AGG` or `HASHBYTES` to detect row changes, or switch to a CDC-enabled source.

> **Tip 2:** "How would you build a metadata-driven pipeline for 100 tables?" — Create a SQL control table with columns like `table_name, source_connection, sink_path, watermark_col, is_active`. Use a Lookup Activity to fetch all active rows, then a ForEach Activity to iterate and call a generic child pipeline with parameters for each table. Adding a new table = one INSERT into the control table, no ADF code changes. This pattern scales to hundreds of tables and is maintained by data engineers without touching pipelines.

> **Tip 3:** "What's the cost model for ADF?" — ADF charges on: (1) **Activity runs**: $0.001 per run for Copy, Data Flow, etc. (2) **DIU-hours for Copy**: $0.25 per DIU-hour (2 DIUs = minimum). (3) **vCore-hours for Mapping Data Flow**: $0.274/vCore/hr (General Compute). (4) **Self-Hosted IR**: you pay the VM cost (ADF is free for SHIR). Major cost driver for large deployments: Data Flow compute hours. Optimize by: caching reusable reference data, minimizing Data Flow cluster TTL (default 10 min), choosing right compute size.
