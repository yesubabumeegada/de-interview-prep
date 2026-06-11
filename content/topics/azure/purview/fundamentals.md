---
title: "Microsoft Purview — Fundamentals"
topic: azure
subtopic: purview
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, purview, data-governance, catalog, lineage, classification, compliance]
---

# Microsoft Purview — Fundamentals


## 🎯 Analogy

Think of Azure Purview like a data catalog + governance layer for the Microsoft ecosystem: it automatically scans ADLS, Azure SQL, Synapse, and Power BI to build a unified data map, lineage graph, and classification inventory.

---
## What Is Microsoft Purview?

Microsoft Purview is a **unified data governance and compliance platform** that helps organizations discover, classify, understand, and govern their data across the entire data estate — on-premises, multi-cloud, and SaaS.

```
Purview capabilities:

1. Data Map
   Scan and catalog all data sources (Azure, AWS, on-prem, SaaS)
   Build a searchable inventory of every table, column, file, and report
   Track lineage: where data came from and where it goes

2. Data Catalog
   Business glossary: define what "Customer" or "Revenue" means
   Search: find datasets by name, column, classification, owner
   Data sensitivity labels: mark PII, financial, confidential data

3. Data Insights
   Dashboards: how much data is classified? How much is sensitive?
   Scan coverage: which sources are cataloged vs unknown

4. Data Policy
   Access policies: grant access to data directly from Purview (preview)
   (Currently limited to ADLS Gen2, Azure SQL, Synapse)

5. Information Protection (formerly MIP — Microsoft Information Protection)
   Sensitivity labels applied to Office 365, Teams, SharePoint, Power BI
   Data Loss Prevention (DLP) policies

Compared to other catalogs:
  AWS Glue:    Technical catalog only (no business glossary, no lineage UI)
  DataHub:     Open source, richer lineage, requires self-hosting
  Alation:     Third-party, richer collaboration features
  Purview:     Azure-native, best Azure integration, DLP + compliance built-in
```

---

## Core Concepts: Data Map

```
Data Map: live inventory of your entire data estate

Sources (what you can scan):
  Azure:         ADLS Gen2, Azure SQL DB, Synapse, Databricks, Blob Storage, Cosmos DB
  Multi-cloud:   AWS S3, RDS, Redshift; GCP BigQuery, GCS
  On-premises:   SQL Server, Oracle, SAP, Teradata, HDFS (via self-hosted IR)
  SaaS:          Salesforce, Dynamics 365, Power BI, Office 365

How scanning works:
  1. Register data source (provide connection details + auth)
  2. Create scan (choose auth method, scope, schedule)
  3. Scan runs: connects to source, extracts schema metadata
  4. Classification: applies built-in classifiers (PII detection, custom rules)
  5. Assets appear in Data Catalog with schema + classifications
  
Asset types discovered:
  Table → columns, data types, row count estimate, classifications per column
  File → format (Parquet/CSV), schema, folder path, classifications
  Report → Power BI: report pages, visuals, datasets linked
  
Metadata collected per asset:
  Name, full path, schema (column names + types)
  Owner (from Azure AD if detectable)
  Classifications (SSN, credit card, email, etc.)
  Last scan time, size estimate
  Linked to: lineage upstream/downstream
```

---

## Data Classification

```
Classification: automatically label data based on content patterns

Built-in classifiers (200+):
  PII:        Social Security Number, Passport Number, Driver's License
              Date of Birth, Email Address, Phone Number, IP Address
  Financial:  Credit Card Number (Luhn algorithm), Bank Account Number, SWIFT code
  Health:     Patient ID, Diagnosis Code (ICD-10), Drug Name
  Azure:      Azure Storage Key, Azure Subscription Key
  Geographic: US ZIP Code, UK National Insurance Number

How classification works:
  Pattern matching: regex (e.g., SSN pattern: \d{3}-\d{2}-\d{4})
  Column name matching: column named "ssn", "social_security" → classified as SSN
  Sampling: scans subset of data (100-1,000 rows) to check patterns
  Threshold: default 60% of sampled rows must match → classified

Custom classifiers:
  Create regex or dictionary-based classifiers for proprietary data:
    Employee ID: EMP-\d{6}
    Internal project codes: PRJ-[A-Z]{3}-\d{4}
  Apply to specific source types or scan-wide

Sensitivity labels (from Microsoft Information Protection):
  Confidential, Highly Confidential, Public, Internal
  Applied on top of classifications
  Flow to: Power BI reports, Office files, SharePoint
  Enforce: DLP policies block sharing of "Confidential" data to external users

Classification report (Data Insights):
  % of assets with PII classifications
  By source: Azure SQL has 23% PII columns
  Trend: growing or shrinking PII surface area
```

---

## Lineage Tracking

```
Lineage: visual map of data flow from source to consumer

Automatic lineage sources:
  Azure Data Factory:    pipeline activities → lineage between datasets
  Azure Synapse:         SQL scripts + Pipelines → lineage
  Azure Databricks:      Spark jobs (requires spark.databricks.purview.lineage enabled)
  Azure SQL DB:          view definitions → column-level lineage
  Power BI:              report ← dataset ← SQL DB (auto-captured)

Lineage view:
  Graph: Source → Transform → Sink
  Example:
    ADLS Bronze/orders → [ADF CopyActivity] → ADLS Silver/orders → [Databricks Notebook] → Gold/revenue → Power BI Revenue Dashboard
  
  Column-level lineage:
    Which input column flows into which output column?
    Example: amount (SQL) → order_amount (Gold) → Revenue (Power BI measure)

Why lineage matters:
  Impact analysis: if we change the orders.amount column, what breaks downstream?
  Compliance: prove PII data flows to approved destinations only
  Debugging: where did this incorrect value come from?
  GDPR: trace all copies of customer_id to delete on right-to-erasure request

OpenLineage integration:
  Emit lineage events from custom pipelines to Purview via OpenLineage Spec
  or via Purview Atlas API REST endpoint
```

---


## ▶️ Try It Yourself

```python
from azure.purview.catalog import PurviewCatalogClient
from azure.identity import DefaultAzureCredential

cred = DefaultAzureCredential()
client = PurviewCatalogClient(
    endpoint="https://my-purview.purview.azure.com",
    credential=cred,
)

# Search the catalog for tables with PII classification
results = client.discovery.query(
    body={
        "keywords": "orders",
        "filter": {"classification": "MICROSOFT.PERSONAL.EMAIL"},
        "limit": 10,
    }
)

for entity in results.get("value", []):
    print(entity["qualifiedName"], entity.get("classification", []))
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between a data catalog and a data governance platform?" — A data catalog is primarily a searchable inventory of data assets (what data exists, where, and what schema). A data governance platform adds: policy enforcement (access control managed centrally), compliance management (DLP, sensitivity labels, GDPR), data quality tracking, business glossary with ownership, and lineage tracking. Microsoft Purview is a governance platform that includes catalog functionality. AWS Glue Data Catalog is a technical catalog only (no business glossary, no DLP, no lineage UI). Organizations often start with catalog needs and grow into governance.

> **Tip 2:** "How does Purview discover lineage from Azure Data Factory?" — Purview integrates natively with ADF. When you create a connection from ADF to Purview (in ADF settings: Manage → Purview account), ADF automatically emits lineage events for every pipeline run. Each Copy Activity creates a lineage edge: source dataset → copy activity → sink dataset. Data Flow activities create column-level lineage. The lineage appears in Purview Data Catalog within minutes of the pipeline run. No code changes to ADF pipelines are needed — it's configuration-only at the ADF workspace level.

> **Tip 3:** "What are sensitivity labels and how are they different from classifications?" — Classification identifies what type of data is present (e.g., "this column contains Social Security Numbers"). Sensitivity label is a human-readable tier of confidentiality applied to an asset (e.g., "Highly Confidential - PII"). Classifications automatically trigger sensitivity label suggestions, but you configure which classifications map to which labels. Sensitivity labels then drive policy: a "Highly Confidential" Power BI report cannot be shared externally via DLP policy. Classifications are technical (regex-based); sensitivity labels are business-level and drive enforcement.
