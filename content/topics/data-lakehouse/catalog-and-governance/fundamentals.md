---
title: "Catalog & Governance — Fundamentals"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [catalog, governance, metadata, data-discovery, lineage]
---

# Catalog & Governance — Fundamentals

## What Is a Data Catalog?

A data catalog is a metadata management tool that indexes all data assets in an organization (tables, columns, dashboards, ML models, pipelines) and makes them discoverable, understandable, and trustworthy.

```
Without a catalog:
  - Engineers spend hours searching for "where is the customer table?"
  - Multiple copies of "customer" with different definitions
  - No one knows if a table is deprecated or still used
  - Analysts use wrong column because schema is undocumented

With a catalog:
  - Search "revenue" → find all tables with revenue columns
  - Column descriptions: "order_subtotal: sum of item prices, before tax"
  - Lineage: "this Gold table came from Silver → Bronze → Fivetran → Salesforce"
  - Ownership: "team: finance-analytics, owner: sarah@company.com"
  - Quality score: "this table passed 3/3 data quality checks today"
```

---

## What Is Data Governance?

Data governance is the framework of policies, processes, and controls that ensure data is accurate, secure, accessible to the right people, and compliant with regulations.

```
Governance pillars:
  1. Access control: who can read, write, or delete data
  2. Data quality: rules and monitoring for data accuracy
  3. Privacy/compliance: PII handling, GDPR, HIPAA, SOC2
  4. Lineage: track data from source to consumption
  5. Classification: tag sensitive data (PII, confidential, public)
  6. Stewardship: assign owners responsible for data quality
```

---

## Catalog Components

```
Metadata catalog:
  Technical metadata: schema (column names, types), partition info, file format
  Business metadata: descriptions, glossary terms, ownership
  Operational metadata: last updated, row count, freshness SLA
  
Table/Column lineage:
  Column-level: which source column feeds this target column
  Table-level: upstream tables → transformations → downstream tables
  
Data dictionary / glossary:
  "revenue": gross_revenue (pre-refund) or net_revenue (post-refund)?
  Define once, link to all tables that use the term
  
Search & discovery:
  Full-text search across table names, column names, descriptions
  Tag-based search: find all tables tagged "pii" or "finance"

Profiling:
  Sample data, distribution stats, null rates, distinct counts
  Helps analysts understand a table before writing SQL
```

---

## Common Catalog Tools

| Tool | Type | Best For |
|---|---|---|
| **Unity Catalog** (Databricks) | Managed | Databricks lakehouse |
| **AWS Glue** | Managed (AWS) | AWS data lake |
| **Apache Atlas** | Open-source | Hadoop/HDP ecosystem |
| **DataHub** (LinkedIn) | Open-source | General, multi-cloud |
| **Apache Polaris** | Open-source | Iceberg-native catalog |
| **Atlan** | SaaS | Modern data stack (Snowflake, dbt) |
| **Alation** | SaaS | Enterprise, ML metadata |
| **Google Dataplex** | Managed (GCP) | GCP data lake |
| **Hive Metastore** | Open-source | Spark/Hive technical catalog only |

---

## Access Control Basics

```
RBAC (Role-Based Access Control):
  Assign permissions to roles, not individuals
  User → Role → Permissions on tables/columns
  Example:
    GRANT SELECT ON silver.orders TO ROLE analytics_ro;
    GRANT INSERT,UPDATE ON silver.orders TO ROLE etl_writer;
    REVOKE SELECT ON silver.customers FROM ROLE intern_analyst;

Column-level security:
  Some users see raw PII; others see masked values
  Example: analysts see email as "j***@gmail.com"
  Implemented via: column masking policies (Snowflake, Unity Catalog)

Row-level security (RLS):
  User sees only rows they're permitted to see
  Example: regional analyst sees only orders from their region
  Implemented via: Row Access Policies (Snowflake), Row Filters (Unity Catalog)

Data classification:
  Tag columns: PII, PHI, Confidential, Internal, Public
  Drive access policies from tags automatically
```

---

## Interview Tips

> **Tip 1:** "What's the difference between a technical catalog (Hive Metastore) and a business catalog (DataHub)?" — A technical catalog stores schema metadata (column names, types, partition info, table location) for query engines to function — Spark reads from Hive Metastore to understand what tables exist. A business catalog adds discoverability layers: search, descriptions, ownership, lineage, quality scores. Most mature organizations need both: a technical catalog (Glue/Hive) for compute and a business catalog (Atlan/DataHub) for governance.

> **Tip 2:** "Why does RBAC matter more than just S3 bucket policies?" — S3 bucket policies control access at the file level (you can read this S3 prefix). RBAC in a catalog controls at the table/column/row level (you can read table X, but only the non-PII columns, and only for your region). Analysts using SQL through Athena or Databricks SQL don't know what S3 files exist — they query tables. Column masking and row-level security enforce governance within the SQL layer, not just the storage layer. Defense in depth: use both.

> **Tip 3:** "What's the first step when implementing governance in an existing data lake with no catalog?" — Inventory first. Use a crawler (AWS Glue Crawler, DataHub ingestion connector) to automatically scan all tables and register schema metadata. Then: assign ownership (who is responsible for each domain?). Then: classify sensitive columns (automated PII detection via regex patterns). Then: implement access control based on classification. Don't try to add business descriptions manually first — automated inventory + automated classification scales; manual metadata entry doesn't.
