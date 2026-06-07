---
title: "Unity Catalog - Real-World Production Examples"
topic: databricks
subtopic: unity-catalog
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, unity-catalog, production, governance, multi-tenant, compliance]
---

# Unity Catalog — Real-World Production Examples

## Pattern 1: Multi-Environment Governance Setup

```sql
-- PRODUCTION ENVIRONMENT SETUP
-- Catalogs by environment (standard pattern)
CREATE CATALOG development;    -- Engineers experiment freely
CREATE CATALOG staging;        -- Pre-production testing
CREATE CATALOG production;     -- Governed, restricted access

-- Schemas by domain
CREATE SCHEMA production.sales;
CREATE SCHEMA production.marketing;
CREATE SCHEMA production.finance;
CREATE SCHEMA production.raw;       -- Raw ingestion zone
CREATE SCHEMA production.curated;   -- Cleaned data
CREATE SCHEMA production.analytics; -- Business-ready

-- Permission model by environment
-- Development: engineers have full access
GRANT ALL PRIVILEGES ON CATALOG development TO `data-engineers`;
GRANT CREATE SCHEMA ON CATALOG development TO `data-scientists`;

-- Staging: engineers can read/write, analysts can read
GRANT ALL PRIVILEGES ON CATALOG staging TO `data-engineers`;
GRANT USE CATALOG, USE SCHEMA ON CATALOG staging TO `data-analysts`;
GRANT SELECT ON CATALOG staging TO `data-analysts`;

-- Production: strict access control
GRANT USE CATALOG ON CATALOG production TO `all-employees`;
GRANT ALL PRIVILEGES ON CATALOG production TO `platform-team`;  -- Only platform team writes
GRANT SELECT ON SCHEMA production.analytics TO `data-analysts`;  -- Analysts read curated only
GRANT SELECT ON SCHEMA production.curated TO `data-scientists`;
-- Raw data: only ETL service principal can access
GRANT ALL PRIVILEGES ON SCHEMA production.raw TO `etl-service-principal`;
```

---

## Pattern 2: Data Mesh with Unity Catalog

```sql
-- Each domain team owns their catalog
CREATE CATALOG domain_sales;
CREATE CATALOG domain_marketing;
CREATE CATALOG domain_finance;

-- Domain teams govern their own data
-- Sales team manages domain_sales
GRANT ALL PRIVILEGES ON CATALOG domain_sales TO `sales-data-team`;

-- They publish curated data products to a shared catalog
CREATE CATALOG data_products;  -- The "marketplace"

-- Sales team publishes their data product
CREATE SCHEMA data_products.sales;
CREATE TABLE data_products.sales.daily_revenue AS
SELECT order_date, SUM(amount) as revenue, COUNT(*) as orders
FROM domain_sales.curated.orders
GROUP BY order_date;

-- Other teams can discover and consume published data products
GRANT USE CATALOG ON CATALOG data_products TO `all-data-teams`;
GRANT SELECT ON SCHEMA data_products.sales TO `all-data-teams`;

-- Lineage shows: domain_sales.curated.orders → data_products.sales.daily_revenue
-- Ownership: sales-data-team owns both source and published product
```

---

## Pattern 3: CI/CD Pipeline Permissions

```python
# Service principal for CI/CD (Terraform/GitHub Actions)
# This SP deploys schema changes and manages table lifecycle

# terraform/unity_catalog.tf (simplified)
"""
resource "databricks_service_principal" "cicd" {
  display_name = "cicd-pipeline"
}

resource "databricks_grants" "cicd_staging" {
  catalog = "staging"
  grant {
    principal  = databricks_service_principal.cicd.application_id
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "cicd_production" {
  catalog = "production"
  grant {
    principal  = databricks_service_principal.cicd.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "MODIFY"]
  }
}
"""

# GitHub Actions workflow uses the SP to deploy DDL changes:
# 1. PR merges to main
# 2. CI runs databricks-cli with SP credentials
# 3. Applies SQL migrations (CREATE/ALTER tables in production)
# 4. Verifies grants are correct
# 5. Runs data quality checks
```

---

## Pattern 4: Audit and Compliance Reporting

```sql
-- Weekly compliance report: who accessed PII data?
CREATE VIEW compliance.reports.pii_access_weekly AS
SELECT 
    user_identity.email AS user_email,
    request_params.full_name_arg AS table_accessed,
    COUNT(*) AS access_count,
    MIN(event_time) AS first_access,
    MAX(event_time) AS last_access
FROM system.access.audit
WHERE event_date >= current_date() - 7
  AND request_params.full_name_arg LIKE '%customers%'  -- PII tables
  AND action_name = 'commandSubmit'
GROUP BY user_identity.email, request_params.full_name_arg
ORDER BY access_count DESC;

-- Alert on privilege escalation
-- Detect: someone granted themselves elevated permissions
SELECT 
    event_time,
    user_identity.email AS granter,
    request_params.principal AS grantee,
    request_params.privilege AS privilege_granted,
    request_params.full_name_arg AS target_object
FROM system.access.audit
WHERE action_name = 'updatePermissions'
  AND event_date = current_date()
  AND request_params.privilege IN ('ALL_PRIVILEGES', 'CREATE_CATALOG')
ORDER BY event_time DESC;

-- Data freshness monitoring
SELECT 
    table_catalog, table_schema, table_name,
    last_altered,
    DATEDIFF(current_timestamp(), last_altered) AS hours_since_update
FROM system.information_schema.tables
WHERE table_catalog = 'production'
  AND DATEDIFF(current_timestamp(), last_altered) > 24  -- Stale tables
ORDER BY hours_since_update DESC;
```

---

## Pattern 5: Cross-Workspace Data Access

```python
# Scenario: 3 workspaces (ETL, Analytics, ML) sharing one metastore
# All connected to the same Unity Catalog metastore

# ETL Workspace: writes data
spark.sql("""
    INSERT INTO production.sales.orders
    SELECT * FROM production.raw.orders_landing
    WHERE load_date = current_date()
""")

# Analytics Workspace: reads same data (different workspace, same metastore)
spark.sql("""
    SELECT region, SUM(amount) as revenue
    FROM production.sales.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY region
""")

# ML Workspace: builds features from same data
spark.sql("""
    CREATE TABLE production.ml.customer_features AS
    SELECT customer_id,
           COUNT(*) as order_count,
           AVG(amount) as avg_order_value,
           MAX(order_date) as last_order
    FROM production.sales.orders
    GROUP BY customer_id
""")

# All three workspaces:
# - See the same tables (one namespace)
# - Respect same permissions (one grant model)
# - Contribute to lineage (cross-workspace lineage tracking)
# - Appear in same audit logs (unified compliance)
```

---

## Pattern 6: Tagging and Classification

```sql
-- Tag tables with sensitivity levels
ALTER TABLE production.sales.customers
SET TAGS ('sensitivity' = 'pii', 'domain' = 'sales', 'sla' = 'tier1');

ALTER TABLE production.analytics.daily_metrics
SET TAGS ('sensitivity' = 'internal', 'domain' = 'analytics', 'sla' = 'tier2');

-- Find all PII tables
SELECT table_catalog, table_schema, table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'sensitivity' AND tag_value = 'pii';

-- Use tags for automated governance
-- Policy: all 'pii' tagged tables must have row filters
-- Policy: all 'tier1' tagged tables must have freshness monitoring
-- Implementation: scan tags nightly, alert if policy violated
```

---

## Interview Tips

> **Tip 1:** "How do you structure catalogs for a medium-sized company?" — Three catalogs by environment (dev/staging/prod), schemas by domain (sales/marketing/finance). Platform team owns production writes, domain teams own their schemas. Service principals for ETL (not user accounts). Grant to groups, never individuals.

> **Tip 2:** "How do you implement a data mesh with Unity Catalog?" — Each domain team owns a catalog (domain_sales, domain_marketing). They govern their own data internally. Published data products go to a shared `data_products` catalog with broader read access. Unity Catalog lineage tracks the flow from domain source to published product.

> **Tip 3:** "How do you handle cross-workspace access?" — All workspaces connect to the same Unity Catalog metastore. Same three-level namespace everywhere. Permissions are metastore-level (not workspace-level), so a GRANT applies regardless of which workspace the user queries from. This eliminates the old problem of duplicating ACLs per workspace.
