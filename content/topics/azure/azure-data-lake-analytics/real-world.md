---
title: "Azure Data Lake Analytics - Real-World Patterns"
topic: azure
subtopic: azure-data-lake-analytics
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [azure, adla, u-sql, migration, production, cost-management, orchestration]
---

# Azure Data Lake Analytics — Real-World Patterns

## How ADLA Was Used in Production

While ADLA is now retired, understanding how it was used in production is valuable for:
- Interview conversations about legacy Azure modernisation projects
- Demonstrating knowledge of migration patterns and trade-offs
- Understanding how Microsoft's analytics platform evolved

---

## Production Pattern 1: Daily Batch ETL with ADF Orchestration

A common production pattern was chaining multiple U-SQL jobs via ADF pipelines, where each job processed one layer of the data lakehouse:

```
ADF Pipeline: daily_etl_pipeline
│
├── Activity 1: CopyActivity (source → ADLS raw zone)
│
├── Activity 2: DataLakeAnalyticsU-SQL (raw → cleansed)
│   Script: /scripts/01_cleanse_orders.usql
│   AUs: 20, Priority: 200
│
├── Activity 3: DataLakeAnalyticsU-SQL (cleansed → curated)
│   Script: /scripts/02_aggregate_orders.usql
│   AUs: 10, Priority: 200
│
└── Activity 4: DataLakeAnalyticsU-SQL (curated → reporting)
    Script: /scripts/03_build_sales_summary.usql
    AUs: 5, Priority: 300
```

**ADF ARM template snippet for U-SQL activity:**
```json
{
  "name": "CleanseOrders",
  "type": "DataLakeAnalyticsU-SQL",
  "dependsOn": [
    { "activity": "CopyRawData", "dependencyConditions": ["Succeeded"] }
  ],
  "typeProperties": {
    "scriptLinkedService": { "referenceName": "ADLSLinkedService", "type": "LinkedServiceReference" },
    "scriptPath": "scripts/01_cleanse_orders.usql",
    "degreeOfParallelism": 20,
    "priority": 200,
    "parameters": {
      "processingDate": "@formatDateTime(pipeline().parameters.runDate, 'yyyy/MM/dd')"
    }
  },
  "linkedServiceName": { "referenceName": "ADLALinkedService", "type": "LinkedServiceReference" }
}
```

---

## Production Pattern 2: Cost Management at Scale

Organisations with dozens of ADLA jobs needed governance over AU consumption to avoid unexpected bills.

### AU Budget Monitoring via Azure Monitor

```python
# Python script to query ADLA job costs via Azure Monitor metrics
from azure.monitor.query import MetricsQueryClient
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta

credential = DefaultAzureCredential()
client = MetricsQueryClient(credential)

resource_id = (
    "/subscriptions/{sub_id}/resourceGroups/{rg}/"
    "providers/Microsoft.DataLakeAnalytics/accounts/{adla_account}"
)

# Query AUs consumed in last 24 hours
result = client.query_resource(
    resource_id,
    metric_names=["JobAUEndedCancelled", "JobAUEndedFailure", "JobAUEndedSuccess"],
    timespan=timedelta(hours=24),
    granularity=timedelta(hours=1)
)

total_aus = sum(
    metric.value
    for response in result.metrics
    for ts in response.timeseries
    for metric in ts.data
    if metric.total
)
print(f"Total AU-hours consumed: {total_aus / 3600:.2f}")
```

### AU Account Limit Enforcement

```bash
# Set maximum AU per account via Azure CLI
az dla account update \
  --account "my-adla-account" \
  --max-degree-of-parallelism 250 \
  --max-job-count 20

# Set per-job AU limit via policy
az dla account compute-policy create \
  --account "my-adla-account" \
  --name "MaxAUPerUser" \
  --object-id "<aad-group-object-id>" \
  --object-type Group \
  --max-degree-of-parallelism-per-job 50
```

---

## Production Pattern 3: U-SQL with Custom C# Assemblies

Production environments often registered shared C# assembly libraries for reusable business logic:

```
ADLS Assembly Store:
/assemblies/
├── SharedUtils.dll       # Common string/date helpers
├── GeoProcessing.dll     # Lat/lon parsing and geohashing
├── BusinessRules.dll     # Revenue classification logic
└── SensitiveDataMasker.dll  # PII masking functions
```

```sql
// In every job script that needs shared logic
REFERENCE ASSEMBLY [SharedUtils];
REFERENCE ASSEMBLY [BusinessRules];

@enriched =
    SELECT OrderId,
           BusinessRules.ClassifyRevenue(Amount) AS RevenueCategory,
           SharedUtils.ParseDate(RawDateString, "yyyy-MM-dd") AS ParsedDate
    FROM @rawOrders;
```

**Deployment pipeline for assemblies (Azure DevOps YAML):**
```yaml
- task: DotNetCoreCLI@2
  displayName: Build C# Assembly
  inputs:
    command: build
    projects: '**/*.csproj'

- task: AzureCLI@2
  displayName: Upload Assembly to ADLS
  inputs:
    azureSubscription: $(SERVICE_CONNECTION)
    scriptType: bash
    scriptLocation: inlineScript
    inlineScript: |
      az storage blob upload \
        --account-name $(ADLS_ACCOUNT) \
        --container-name assemblies \
        --name SharedUtils.dll \
        --file $(Build.ArtifactStagingDirectory)/SharedUtils.dll \
        --overwrite

- task: AzurePowerShell@5
  displayName: Register Assembly in ADLA Catalog
  inputs:
    ScriptInline: |
      Submit-AzDataLakeAnalyticsJob `
        -Account "$(ADLA_ACCOUNT)" `
        -Name "RegisterAssembly_$(Build.BuildId)" `
        -Script "DROP ASSEMBLY IF EXISTS SharedUtils; CREATE ASSEMBLY SharedUtils FROM @'/assemblies/SharedUtils.dll';"
```

---

## Production Pattern 4: Migration War Story — ADLA to Databricks

**Scenario**: Retail company with 150 U-SQL jobs processing 50 TB/day, forced to migrate before the Feb 2024 ADLA retirement deadline.

### Discovery Findings

| Category | Count | Migration Complexity |
|----------|-------|---------------------|
| Pure SQL jobs (no C# UDFs) | 82 | Low — SQL transpilation |
| Jobs with inline C# expressions | 43 | Medium — Python equivalent |
| Jobs with registered C# assemblies | 25 | High — full rewrite |
| Total | 150 | |

### Migration Timeline

```
Month 1-2: Assessment & Setup
  - Inventory all U-SQL scripts, identify C# assemblies
  - Provision Databricks workspace + Unity Catalog
  - Set up ADLS Gen2 with Delta Lake foundation
  - Create ADF → Databricks pipeline templates

Month 3-4: Wave 1 (Low Complexity — 82 jobs)
  - Automated U-SQL → PySpark transpilation for pure SQL jobs
  - ADF pipeline updates: swap U-SQL activity for Databricks notebook activity
  - Parallel validation: compare row counts + checksums

Month 5: Wave 2 (Medium Complexity — 43 jobs)
  - Manual PySpark rewrite for jobs with inline C# expressions
  - Unit tests in pytest for each translated function

Month 6: Wave 3 (High Complexity — 25 jobs)
  - Architect C# assemblies as Delta Live Tables expectations + Python libraries
  - Performance testing + AU → DBU cost comparison
  - Cutover and ADLA account decommission
```

### Key Lesson: Data Skew in U-SQL vs. Spark

U-SQL's HASH distribution on a skewed column caused silent performance degradation. The migration was an opportunity to fix it:

```python
# Identify skewed join keys before migration
skew_analysis = (
    orders
    .groupBy("CustomerId")
    .count()
    .orderBy(F.desc("count"))
)
skew_analysis.show(10)

# Fix: salt the join key in Spark to distribute skewed partitions
import pyspark.sql.functions as F

salted_orders = orders.withColumn(
    "salted_key",
    F.concat(F.col("CustomerId"), F.lit("_"), (F.rand() * 10).cast("int"))
)
```

---

## Real-World Interview Discussion Points

**"Tell me about a time you worked with ADLA or migrated away from it."**

Key talking points:
1. **Business driver**: ADLA retirement forced migration; proactive vs. reactive approach.
2. **Risk management**: Parallel run, row-count validation, checksum comparison before cutover.
3. **Cost outcome**: Post-migration Databricks costs vs. ADLA AU costs (often 20–40% reduction at scale with spot instances).
4. **Unexpected challenges**: C# assembly rewrites took 3× longer than estimated; data skew bugs discovered during migration.
5. **Process improvement**: Migration was used to refactor job logic, adopt Delta Lake, and improve observability.

**On cost comparison:**
- ADLA: predictable, linear cost (AU × seconds)
- Databricks: more complex (on-demand vs. spot, cluster type, DBU rate)
- At 50 TB/day scale with optimised Spark code on spot instances, Databricks typically wins on cost — but requires more operational expertise
