---
title: "Azure Data Lake Analytics - Scenario Questions"
topic: azure
subtopic: azure-data-lake-analytics
content_type: scenario_question
tags: [azure, adla, u-sql, analytics-units, migration, cost-optimization]
---

# Azure Data Lake Analytics — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Understanding U-SQL Basics

Your team has a CSV file on ADLS at `/data/sales/2024/orders.csv` with columns `OrderId`, `CustomerId`, `Amount`, and `OrderDate`. You need to write a U-SQL script that reads this file, filters for orders with Amount greater than 500, and outputs the results grouped by `CustomerId` with the total spend, saving to `/output/high_value_customers.csv`.

Write the U-SQL script, and explain what Analytics Units you would request and why.

<details>
<summary>✅ Solution</summary>

### U-SQL Script

```sql
// Declare parameters for reusability
DECLARE @inputPath string = "/data/sales/2024/orders.csv";
DECLARE @outputPath string = "/output/high_value_customers.csv";

// Step 1: Extract data from ADLS CSV
@rawOrders =
    EXTRACT OrderId    int,
            CustomerId int,
            Amount     decimal,
            OrderDate  DateTime
    FROM @inputPath
    USING Extractors.Csv(skipFirstNRows: 1);  // skip header row

// Step 2: Filter high-value orders
@highValueOrders =
    SELECT CustomerId,
           Amount
    FROM @rawOrders
    WHERE Amount > 500.0;

// Step 3: Aggregate by customer
@customerSummary =
    SELECT CustomerId,
           COUNT(*) AS OrderCount,
           SUM(Amount) AS TotalSpend,
           AVG(Amount) AS AvgOrderValue
    FROM @highValueOrders
    GROUP BY CustomerId;

// Step 4: Write output with header
OUTPUT @customerSummary
TO @outputPath
USING Outputters.Csv(outputHeader: true);
```

### Analytics Units Decision

For a single CSV file:
- **1–5 AUs** is appropriate. A single file cannot be read in parallel across many AUs — the extraction phase is inherently sequential per file. Adding more AUs beyond 5 would allocate resources you can't use.
- If the input were a directory with hundreds of files (`/data/sales/2024/*.csv`), you could justify 20–50 AUs to parallelise reading across files.

### Key Points

1. `Extractors.Csv(skipFirstNRows: 1)` skips the header — forgetting this causes schema parsing errors.
2. U-SQL rowsets (`@rawOrders`) are immutable; each transformation creates a new named rowset.
3. `Outputters.Csv(outputHeader: true)` adds the column header to output — important for downstream consumers.
4. ADLA compiles this script to a DAG: Extract → Filter → Aggregate → Output, executing as vertices in parallel where possible.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Cost Spike Investigation

Your ADLA account shows a 400% cost spike this month. The team has 15 daily jobs running. You have access to Azure Monitor metrics and the Azure Portal ADLA job history. Walk through how you diagnose the root cause and implement guardrails to prevent recurrence.

<details>
<summary>✅ Solution</summary>

### Step 1: Identify the Offending Jobs

```bash
# List jobs from last 30 days via Azure CLI, sorted by AU consumption
az dla job list \
  --account "my-adla-account" \
  --query "sort_by([].{Name:name, State:state, AUs:properties.degreeOfParallelism, Duration:properties.endTime, Submitter:properties.submitter}, &AUs)" \
  --output table
```

Alternatively, in the Azure Portal → ADLA account → Jobs → filter by date, sort by Degree of Parallelism descending.

### Step 2: Calculate Actual AU-Hours Per Job

```python
# Python script to compute cost per job from job history API
import requests
from azure.identity import DefaultAzureCredential

def get_job_cost_estimate(account, job_id, au_second_price=0.00025):
    """Estimate cost of a completed ADLA job."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default").token

    response = requests.get(
        f"https://{account}.azuredatalakeanalytics.net/Jobs/{job_id}?api-version=2016-11-01",
        headers={"Authorization": f"Bearer {token}"}
    )
    job = response.json()

    props = job["properties"]
    aus = props["degreeOfParallelism"]

    # Calculate wall clock seconds
    from datetime import datetime
    start = datetime.fromisoformat(props["startTime"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(props["endTime"].replace("Z", "+00:00"))
    duration_seconds = (end - start).total_seconds()

    estimated_cost = aus * duration_seconds * au_second_price
    return {
        "job_name": props["name"],
        "aus": aus,
        "duration_seconds": duration_seconds,
        "estimated_cost_usd": round(estimated_cost, 4)
    }
```

### Step 3: Common Root Causes & Fixes

| Root Cause | Diagnosis Signal | Fix |
|-----------|-----------------|-----|
| Developer submitted test job with 500 AUs | Job list shows 500 AUs, short duration | Compute policy limiting per-user AUs |
| New job added with default 500 AUs | New job name in list, high AUs | Code review for AU parameter |
| Data volume 10× growth, same AUs | Job duration 10× longer, same AUs | AUs OK, investigate data growth |
| Recursive/infinite ADF trigger | Many duplicate jobs in same hour | Fix ADF trigger condition |

### Step 4: Implement Guardrails

```bash
# 1. Set account-level maximum AUs
az dla account update \
  --account "my-adla-account" \
  --max-degree-of-parallelism 250

# 2. Set per-user AU limit via compute policy
az dla account compute-policy create \
  --account "my-adla-account" \
  --name "DataEngTeamLimit" \
  --object-id "<aad-group-id>" \
  --object-type Group \
  --max-degree-of-parallelism-per-job 50

# 3. Set Azure Monitor alert for AU threshold
az monitor metrics alert create \
  --name "ADLA-HighAU-Alert" \
  --resource "/subscriptions/.../providers/Microsoft.DataLakeAnalytics/accounts/my-adla-account" \
  --metric "JobAUEndedSuccess" \
  --operator GreaterThan \
  --threshold 1000 \
  --window-size 1h \
  --evaluation-frequency 15m \
  --action "/subscriptions/.../actionGroups/DataEngAlerting"
```

### Step 5: Ongoing Cost Governance

- Add AU count as a required code review field in all U-SQL job PRs.
- Create a cost dashboard in Azure Monitor workbooks showing daily AU consumption per job.
- Set a monthly budget alert in Azure Cost Management at 120% of baseline.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Architecting the ADLA-to-Databricks Migration

Your organisation has 80 U-SQL jobs that must be migrated to Azure Databricks before ADLA retirement. 20 of those jobs use registered C# assemblies for complex business rule calculations (revenue classification, customer segmentation, PII masking). Describe your end-to-end migration architecture, validation strategy, and how you handle the C# assembly problem.

<details>
<summary>✅ Solution</summary>

### Migration Architecture

```
Phase 0: Foundation (Week 1-2)
  ├── Provision Databricks workspace (Premium tier for Unity Catalog)
  ├── Create Unity Catalog metastore + external location on ADLS Gen2
  ├── Create Delta Lake table schema equivalents for ADLA catalog tables
  └── Create ADF Databricks linked service + job cluster template

Phase 1: Low-Complexity Migration (Weeks 3-6, 60 jobs)
  ├── Automated transpilation for pure-SQL U-SQL jobs
  ├── ADF pipeline swap: U-SQL activity → Databricks notebook activity
  └── Parallel validation: row count + checksum comparison

Phase 2: C# Assembly Migration (Weeks 7-12, 20 jobs)
  ├── Rewrite C# logic as Python wheel package
  ├── Publish to Databricks cluster init script / Unity Catalog volumes
  └── Integration tests against production data sample

Phase 3: Cutover & Decommission (Week 13)
  ├── Final parallel run with sign-off
  ├── Redirect all ADF triggers to Databricks
  └── Archive U-SQL scripts + decommission ADLA account
```

### Handling C# Assembly Migration

The 20 jobs with C# assemblies are the high-risk element. Strategy:

**Step 1: Audit assembly functionality**
```python
# Map each C# assembly to Python equivalents
ASSEMBLY_MAPPING = {
    "BusinessRules.ClassifyRevenue": {
        "complexity": "high",
        "python_approach": "pandas_udf",
        "test_cases": 47,  # existing unit tests to port
    },
    "SensitiveDataMasker.MaskEmail": {
        "complexity": "low",
        "python_approach": "pyspark_udf",
        "test_cases": 12,
    },
    "GeoProcessing.GeohashEncode": {
        "complexity": "medium",
        "python_approach": "python_geohash_library",
        "test_cases": 23,
    }
}
```

**Step 2: Rewrite as Python wheel**
```python
# src/business_rules/revenue.py
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# Use pandas_udf for complex logic — batched execution, much faster than row UDF
@pandas_udf(StringType())
def classify_revenue(amounts: pd.Series) -> pd.Series:
    """Port of BusinessRules.ClassifyRevenue C# logic."""
    def classify(amount):
        if amount >= 100_000:
            return "enterprise"
        elif amount >= 10_000:
            return "mid-market"
        elif amount >= 1_000:
            return "smb"
        else:
            return "micro"
    return amounts.apply(classify)
```

**Step 3: Validation framework**

```python
# validation/compare_outputs.py
def validate_migration(usql_output_path: str, spark_output_path: str, key_columns: list):
    """Compare ADLA and Databricks job outputs for equivalence."""
    usql_df = spark.read.csv(usql_output_path, header=True, inferSchema=True)
    spark_df = spark.read.parquet(spark_output_path)

    # Row count check
    usql_count = usql_df.count()
    spark_count = spark_df.count()
    assert usql_count == spark_count, f"Row count mismatch: {usql_count} vs {spark_count}"

    # Join and compare key columns
    joined = usql_df.alias("a").join(spark_df.alias("b"), key_columns, "full_outer")

    mismatches = joined.filter(
        F.col("a.TotalRevenue") != F.col("b.TotalRevenue")
    ).count()

    # Allow 0.001% tolerance for floating point differences
    tolerance = usql_count * 0.00001
    assert mismatches <= tolerance, f"Value mismatches: {mismatches} (tolerance: {tolerance})"

    print(f"Validation passed: {usql_count} rows, {mismatches} acceptable mismatches")
```

### ADF Pipeline Migration Pattern

```json
// Before: U-SQL activity
{
  "name": "ClassifyRevenue_USQL",
  "type": "DataLakeAnalyticsU-SQL",
  "typeProperties": {
    "scriptPath": "/scripts/classify_revenue.usql",
    "degreeOfParallelism": 30
  }
}

// After: Databricks notebook activity
{
  "name": "ClassifyRevenue_Databricks",
  "type": "DatabricksNotebook",
  "typeProperties": {
    "notebookPath": "/Shared/pipelines/classify_revenue",
    "baseParameters": {
      "input_path": "@pipeline().parameters.inputPath",
      "output_path": "@pipeline().parameters.outputPath",
      "processing_date": "@pipeline().parameters.runDate"
    }
  },
  "linkedServiceName": { "referenceName": "DatabricksLinkedService" }
}
```

### Go/No-Go Criteria for Cutover

- All 80 jobs pass row count validation (exact match)
- All 80 jobs pass value comparison within tolerance (≤ 0.001% mismatch)
- Databricks job P95 duration ≤ ADLA job P95 duration × 1.2 (20% runtime budget)
- 5 consecutive successful parallel runs for each job
- Rollback plan documented: ADF trigger can be reverted to U-SQL activity within 15 minutes

### Cost Outcome Expectation

ADLA at 30 AUs × 600s average × $0.00025 = $4.50/job
Databricks on spot instances with Photon: ~$2.50–$3.00/job
Expected 30–40% cost reduction, plus long-term flexibility for Delta Lake, streaming, and MLflow.

</details>
</article>
