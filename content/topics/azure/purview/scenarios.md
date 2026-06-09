---
title: "Microsoft Purview — Scenarios"
topic: azure
subtopic: purview
content_type: scenario_question
tags: [azure, purview, scenarios, interview, compliance, data-governance, gdpr]
---

# Microsoft Purview — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Build a Data Governance Program from Scratch

**Scenario:** A 500-person fintech company has 50TB of data across Azure SQL, ADLS Gen2, Synapse, and Power BI. They have no data catalog, no data ownership, and are approaching a SOC 2 audit in 6 months. Design a Purview-based data governance program.

<details>
<summary>💡 Hint</summary>
Design a phased 6-month program: Month 1 foundation (scan all sources), Month 2 classification + ownership, Month 3 business glossary, Month 4 lineage enablement, Month 5 SOC 2 evidence package, Month 6 ongoing operations.
</details>

<details>
<summary>✅ Solution</summary>

```
Program design (6-month rollout):

Month 1: Foundation
  Infrastructure:
    Create Purview account (Premium tier for Power BI + multi-cloud scanning)
    Set up Collections hierarchy:
      Root → Finance → Trading → Risk
             → Operations → ETL → Serving
             → Engineering → Platform → Analytics
    Create Azure AD groups for each collection's data stewards
    Grant Purview MSI: Storage Blob Data Reader on all storage accounts
    Grant Purview MSI: Reader on all SQL Servers

  Source registration and initial scan:
    Register: 3 ADLS Gen2 accounts, 5 Azure SQL DBs, Synapse, Power BI tenant
    Run full scan on all sources (allow 24-48 hours for initial scan)
    Outcome: catalog populated with ~10,000 assets

Month 2: Classification and Ownership
  Configure classification rules:
    Enable all built-in classifiers (SSN, TIN, credit card, account numbers)
    Create custom classifiers:
      - Trade ID: TRD-\d{10}
      - CUSIP: [A-Z0-9]{9}
      - Internal Account Number: ACC-\d{8}
    Re-run scans with enhanced rules
    
  Ownership assignment:
    Data Curators: assign owners to all classified assets
    Rule: any table with PII classification MUST have an owner within 30 days
    Automation: Purview Event Grid → Teams notification when asset lacks owner > 30 days

Month 3: Business Glossary
  Prioritized term list (10 critical terms first):
    - Net Revenue, Gross Revenue, AUM, Trade Date, Settlement Date
    - Customer (retail vs institutional), Position, Exposure, Risk-Weighted Asset, Counterparty
  
  For each term: definition, owner (business lead), linked columns in catalog
  Approval workflow: draft → data owner review → CFO/CRO approval → Published
  
  KPI: 100% of Gold-layer tables have at least 3 linked glossary terms

Month 4: Lineage
  Enable ADF-Purview integration: all 15 ADF pipelines emit lineage automatically
  Enable Databricks lineage (spark.databricks.purview.lineage=true in all clusters)
  Enable Power BI scanning: full workspace lineage (ADLS → Synapse → Power BI)
  Outcome: end-to-end lineage visible for top 20 reporting pipelines

Month 5: SOC 2 Preparation
  SOC 2 requirements addressed by Purview:
    Data inventory ✓ (catalog with all assets)
    Data classification ✓ (PII marked, sensitivity labels applied)
    Access control audit ✓ (Purview + Azure RBAC logs → Log Analytics)
    Data lineage ✓ (who sees financial data and where it flows)
    Owner accountability ✓ (every sensitive asset has a named owner)
  
  Generate compliance reports:
    % assets classified
    % sensitive assets with owner
    All PII data flows to approved destinations (lineage verification)
    Access log: who accessed SSN-classified tables in last 90 days

Month 6: Ongoing Operations
  Automated weekly compliance report → Slack/Teams
  Monthly steering committee review
  Scan schedule: daily incremental (detect new assets quickly)
  SLA: new PII assets must have owner within 5 business days
  
  KPIs for SOC 2:
    Asset coverage: 95% of registered sources scanned
    PII classification coverage: 100% of financial data sources
    Owner coverage: 98% of PII-classified assets
    Lineage coverage: 90% of Gold-layer tables
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Impact Analysis for a Schema Change

**Scenario:** The data engineering team wants to rename the `orders.customer_id` column to `orders.cust_id` in the Azure SQL DB. How do you use Purview to understand the impact before making the change?

<details>
<summary>💡 Hint</summary>
Use Purview's column-level lineage API. Search for the specific column, trace OUTPUT lineage depth=5 to find all downstream systems. For each impacted system, plan a backward-compatible migration: add new column, dual-read period, drop old.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Find the column in Purview catalog
  Search: "customer_id" + filter: source=Azure SQL DB
  Find: dbo.orders#customer_id (qualified name)
  Note the asset GUID for lineage query

Step 2: Get lineage for customer_id column
  In Purview UI: Data Catalog → search "orders" → select orders table
  Click: Lineage tab → Column-level lineage
  
  OR programmatically:
  GET /catalog/api/atlas/v2/lineage/{column_guid}?direction=OUTPUT&depth=5
  
  Lineage graph (what depends on orders.customer_id):
    orders.customer_id →
      [ADF CopyActivity] → silver.orders#customer_id (ADLS Delta)
        [Databricks job: transform_silver] → gold.daily_revenue#customer_id
          [Synapse SQL view: v_revenue] → customer_id (in view)
            [Power BI dataset: Revenue Analytics] → Customer dimension
              [Power BI report: Executive Dashboard] → Customer filter slicer
    
    orders.customer_id →
      [ADF pipeline: customer_merge] → customers.customer_id (Azure SQL)
    
    orders.customer_id →
      [Stored procedure: sp_order_summary] → @customer_id parameter

Step 3: Document all impacted systems
  1. ADF CopyActivity (2 pipelines): update source column mapping
  2. Databricks notebook transform_silver: update column reference
  3. Databricks notebook aggregate_gold: update groupBy column
  4. Synapse SQL view v_revenue: ALTER VIEW to use new column name
  5. Power BI dataset Revenue Analytics: update column mapping in Power Query
  6. Azure SQL stored procedure sp_order_summary: update parameter reference
  7. Application code: any direct SQL queries referencing orders.customer_id

Step 4: Plan backward-compatible migration
  Phase 1: Add new column (no breaking change)
    ALTER TABLE orders ADD cust_id INT;
    UPDATE orders SET cust_id = customer_id;  -- backfill
    Purview re-scan: cust_id appears in catalog

  Phase 2: Dual-read period (2 weeks)
    Update all downstream (identified above) to read from cust_id
    Verify via Purview lineage: new lineage edges from cust_id

  Phase 3: Drop old column (verify no remaining consumers in lineage)
    ALTER TABLE orders DROP COLUMN customer_id;
    Purview re-scan: customer_id removed from catalog, lineage broken edges highlighted

  Impact assessment value:
    Without Purview: 3 days of manual discovery, risk of missing Power BI reports
    With Purview: 30 minutes of lineage review, complete impact list
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: GDPR Right-to-Erasure Request

**Scenario:** A customer submits a GDPR erasure request. Their customer_id is 'CUST_78421'. You have data across ADLS, Azure SQL, Cosmos DB, and Power BI (cached reports). How do you use Purview to locate and erase all their data?

<details>
<summary>💡 Hint</summary>
Use Purview to discover all tables/columns with PII classification across all sources. Then execute erasure per system: DELETE for SQL, Delta DELETE + VACUUM for ADLS, delete_item for Cosmos DB, dataset refresh for Power BI. Document in a compliance audit trail.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Use Purview to find all data stores with customer data

Search in Purview:
  - Query: columns classified as "CUSTOMER_ID" or containing "customer_id" column
  - Filter: classification_names contains PERSONAL_DATA
  - Result: list of all tables/files across all registered sources

Discovery results (example):
  Azure SQL: 
    orders.dbo.orders (customer_id column)
    orders.dbo.customers (customer_id, email, phone, address)
    orders.dbo.billing_history (customer_id)
  
  ADLS Gen2:
    bronze/orders/... (customer_id in JSON events)
    silver/orders/ (Delta table, customer_id column)
    silver/customers/ (Delta table, all PII)
    gold/daily_revenue/ (customer_id aggregated — individual not present)
    checkpoints/ (Spark checkpoints — no customer data)
  
  Cosmos DB:
    ecommerce/orders (customerId partition key)
    ecommerce/sessions (userId linked to customerId)
  
  Power BI:
    Revenue Analytics dataset: customer_id in data model
    Customer 360 report: customer profile page

Step 2: Execute erasure per data store

Azure SQL (immediate DELETE):
  DELETE FROM orders WHERE customer_id = 'CUST_78421'
  DELETE FROM billing_history WHERE customer_id = 'CUST_78421'
  UPDATE customers SET email=NULL, phone=NULL, address=NULL WHERE customer_id='CUST_78421'
  -- Keep customer_id row (referential integrity) with PII nulled

ADLS Silver (Delta soft delete):
  spark.sql("DELETE FROM silver.orders WHERE customer_id = 'CUST_78421'")
  spark.sql("DELETE FROM silver.customers WHERE customer_id = 'CUST_78421'")
  -- Then physical deletion:
  spark.sql("CALL system.rewrite_data_files(table => 'silver.orders', where => 'true')")
  spark.sql("VACUUM silver.orders RETAIN 0 HOURS")  -- WARNING: requires override, test first

ADLS Bronze (raw events — cannot delete individual records easily):
  Strategy: bronze is immutable log — mark customer as deleted in a "erasure_log" table
  Legal: document that bronze files are raw system logs (may be exempt under legal hold exception)
  If must delete: rewrite affected Parquet files without the customer's rows (expensive)

Cosmos DB:
  container.delete_item("CUST_78421", partition_key="CUST_78421")  # orders
  Query and delete all orders: find all docs WHERE customerId='CUST_78421', delete each
  TTL: if TTL-enabled, verify TTL will expire remaining docs

Power BI:
  Refresh dataset after SQL deletion — cached data cleared on next refresh
  For Import mode: schedule immediate refresh
  For Direct Query: SQL deletion takes effect immediately

Step 3: Audit trail (keep this permanently)
  CREATE record in compliance.erasure_requests:
  {
    "request_id": "GDPR-2024-001",
    "customer_id": "CUST_78421",
    "requested_date": "2024-01-15",
    "completed_date": "2024-01-17",
    "systems_erased": ["Azure SQL orders", "Azure SQL customers", "ADLS Silver", "Cosmos DB"],
    "systems_excluded": ["ADLS Bronze (legal log exemption)"],
    "executed_by": "data_privacy_team@company.com"
  }

Step 4: Verify with Purview re-scan
  Run targeted scan on erased containers
  Confirm: no customer data appears in scan results
  Export compliance certificate for GDPR documentation
  
Timeline: < 30 days (GDPR requirement). With Purview, discovery: 1 day. Execution: 2-3 days.
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "How do you prove to auditors that you have control over your data?" — Purview provides the evidence: (a) Data inventory: exported catalog showing all registered sources and asset counts; (b) Classification report: % of assets scanned and classified, with PII breakdown; (c) Access audit: Log Analytics queries showing who accessed sensitive assets in the last 90 days (via Azure Monitor + storage diagnostic logs); (d) Lineage documentation: export lineage graph showing where PII data flows and that it stays within approved systems; (e) Owner accountability: every sensitive asset has a named owner (Purview Data Curator field). This evidence package directly addresses SOC 2 CC6 (Logical Access) and GDPR Article 30 (Records of Processing Activities).

> **Tip 2:** "What are the limitations of Purview that you should be aware of?" — Key limitations: (a) Scan latency: new assets appear in catalog 1-24 hours after creation (not real-time); (b) Classification sampling: samples 1,000 rows (not all rows) — may miss infrequent PII patterns in large tables; (c) Column-level lineage gaps: only available for native integrations (ADF, Synapse, Databricks) — custom pipelines need OpenLineage instrumentation; (d) Deletes are not surfaced in lineage: Purview tracks assets and processes, but if a table is deleted, lineage edges become orphaned (not automatically cleaned up); (e) Power BI certified datasets only: some Power BI metadata only available with Premium workspace licensing.

> **Tip 3:** "How does Purview differ from Azure Policy?" — Purview: data catalog and governance (knows what data exists, classifies it, tracks lineage, assigns ownership). Azure Policy: infrastructure governance (enforces rules on Azure resource deployment — e.g., "all storage accounts must have HTTPS-only", "all VMs must have a specific tag"). They complement each other: Azure Policy ensures your infrastructure meets standards (HTTPS, encryption, tags, regions), Purview ensures your data meets governance standards (classified, owned, lineage tracked). For compliance: Azure Policy = infrastructure compliance evidence; Purview = data compliance evidence.

