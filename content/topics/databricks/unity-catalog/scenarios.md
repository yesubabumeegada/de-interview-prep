---
title: "Unity Catalog - Scenario Questions"
topic: databricks
subtopic: unity-catalog
content_type: scenario_question
tags: [databricks, unity-catalog, interview, scenarios, governance]
---

# Scenario Questions — Unity Catalog

<article data-difficulty="junior">

## 🟢 Junior: Setting Up Basic Permissions

**Scenario:** A new data analyst (Sarah) joins the company. She needs read access to all tables in the `production.analytics` schema but should NOT be able to see raw data in `production.raw`. Set up her permissions.

<details>
<summary>💡 Hint</summary>
Don't grant to Sarah directly — add her to a group. Grant USE CATALOG, USE SCHEMA, and SELECT at the appropriate levels.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Add Sarah to the data-analysts group (done in IdP/SCIM, not SQL)
-- Sarah is now in group: data-analysts

-- Step 2: Grant catalog usage (required to see the catalog)
GRANT USE CATALOG ON CATALOG production TO `data-analysts`;

-- Step 3: Grant schema usage + SELECT on analytics schema
GRANT USE SCHEMA ON SCHEMA production.analytics TO `data-analysts`;
GRANT SELECT ON SCHEMA production.analytics TO `data-analysts`;

-- Step 4: DO NOT grant anything on production.raw
-- Since we didn't grant USE SCHEMA on production.raw, Sarah can't even see it

-- Verify:
SHOW GRANTS ON SCHEMA production.analytics;
-- principal: data-analysts, privilege: USE_SCHEMA, SELECT

-- When Sarah queries:
-- SELECT * FROM production.analytics.daily_revenue;  ✓ Works
-- SELECT * FROM production.raw.events;               ✗ Access denied
```

**Key Points:**
- Always grant to GROUPS, not individual users (Sarah leaves → just remove from group)
- USE CATALOG is required to see the catalog at all
- USE SCHEMA is required to see tables within the schema
- SELECT grants read access to all tables in the schema (current AND future)
- Without explicit grant on `production.raw`, it's invisible to data-analysts

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Creating a Managed Table

**Scenario:** Create a new managed table `production.sales.monthly_revenue` that aggregates daily orders into monthly totals. Ensure it's properly created within Unity Catalog.

<details>
<summary>💡 Hint</summary>
Use CREATE TABLE AS SELECT (CTAS) or CREATE TABLE with explicit schema. Managed tables have their storage fully controlled by Unity Catalog.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS production.sales;

-- Create managed table with aggregation
CREATE OR REPLACE TABLE production.sales.monthly_revenue AS
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM production.sales.orders
WHERE order_date >= '2023-01-01'
GROUP BY DATE_TRUNC('month', order_date), region;

-- Verify table was created
DESCRIBE TABLE EXTENDED production.sales.monthly_revenue;
-- Type: MANAGED
-- Location: (auto-assigned by Unity Catalog, you don't control this)
-- Provider: delta (always Delta format in UC)

-- Add description for discovery
COMMENT ON TABLE production.sales.monthly_revenue IS 
    'Monthly revenue aggregated from daily orders. Updated daily by ETL pipeline.';

COMMENT ON COLUMN production.sales.monthly_revenue.total_revenue IS 
    'Sum of order amounts in USD for the month';
```

**Key Points:**
- Managed tables: UC controls storage location (you don't specify LOCATION)
- Always Delta format in Unity Catalog
- DROP TABLE on managed table DELETES the data (unlike external tables)
- Add COMMENTs for data discovery (visible in Catalog Explorer UI)
- OR REPLACE allows idempotent recreation (useful in ETL pipelines)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Understanding the Three-Level Namespace

**Scenario:** A colleague's notebook queries `SELECT * FROM orders` and gets an error. They say "it worked yesterday." What's likely wrong and how do you fix it?

<details>
<summary>💡 Hint</summary>
Without the full three-level name, Spark uses the default catalog and schema. If defaults changed or the notebook is running on a different cluster, the resolution changes.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Problem: "SELECT * FROM orders" doesn't specify catalog or schema
-- Spark resolves it using:
--   1. Current catalog (spark.sql.defaultCatalog)
--   2. Current schema (USE SCHEMA statement or default)

-- The resolution depends on context:
-- If default catalog = hive_metastore and schema = sales:
--   resolves to: hive_metastore.sales.orders ← might work on old cluster

-- If default catalog = production and schema = default:
--   resolves to: production.default.orders ← TABLE NOT FOUND!

-- FIX 1: Always use fully qualified names (best practice)
SELECT * FROM production.sales.orders;  -- Always works regardless of defaults

-- FIX 2: Set defaults at notebook/cluster level
USE CATALOG production;
USE SCHEMA sales;
SELECT * FROM orders;  -- Now resolves to production.sales.orders

-- FIX 3: Set defaults in cluster configuration
-- spark.sql.defaultCatalog = production
-- This applies to all sessions on that cluster

-- BEST PRACTICE: Always use three-level names in production code
-- Only use short names in interactive exploration
```

**Key Points:**
- `table_name` alone is ambiguous — depends on catalog/schema context
- Production code should ALWAYS use `catalog.schema.table` (no ambiguity)
- Interactive notebooks can use `USE CATALOG` + `USE SCHEMA` for convenience
- After migrating from Hive metastore: old code using two-level names (`schema.table`) may break
- Set `spark.sql.defaultCatalog` on clusters as a migration bridge

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: External vs Managed Tables

**Scenario:** Your data landing zone is S3 (`s3://company-lake/raw/`). Other teams outside Databricks (Athena, Snowflake) also read from this location. Should you register this as a managed or external table in Unity Catalog?

<details>
<summary>💡 Hint</summary>
If non-Databricks tools need to read the same files, you must use an external table. Managed tables put data in UC-controlled storage that other tools can't easily access.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- EXTERNAL TABLE: you control the storage, other tools access same files
-- This is correct for shared storage

-- First: set up external location (one-time)
CREATE EXTERNAL LOCATION raw_landing
URL 's3://company-lake/raw/'
WITH (STORAGE CREDENTIAL aws_lake_credential);

-- Then: create external table pointing to existing data
CREATE TABLE production.raw.events
USING DELTA
LOCATION 's3://company-lake/raw/events/';

-- Behavior:
-- DROP TABLE production.raw.events;
-- → Only removes UC metadata. Files in S3 are UNTOUCHED.
-- → Athena and Snowflake continue working normally.

-- For comparison, a managed table:
CREATE TABLE production.analytics.summary AS SELECT ...;
-- DROP TABLE production.analytics.summary;
-- → Files are DELETED. Data is gone.
```

| Decision Factor | Use Managed | Use External |
|----------------|-------------|-------------|
| Only Databricks accesses it | ✅ | |
| Other tools need the same files | | ✅ |
| You want UC to manage lifecycle | ✅ | |
| DROP should keep data safe | | ✅ |
| Data landing zone (raw) | | ✅ |
| Curated analytics tables | ✅ | |

**Key Points:**
- External tables: UC manages metadata + permissions, you manage storage
- Managed tables: UC manages everything (simpler but less flexible)
- For shared data lakes accessed by Athena/Snowflake/Spark: always external
- For internal Databricks-only analytics: managed is simpler
- You can convert: migrate from external to managed by CTAS into a managed table

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Viewing Data Lineage

**Scenario:** The CEO asks "where does the revenue number in our dashboard come from?" You need to trace the lineage of `production.analytics.daily_revenue` back to its raw sources. How do you do this in Unity Catalog?

<details>
<summary>💡 Hint</summary>
Unity Catalog automatically tracks lineage when tables are created/modified by Spark SQL or DataFrame operations. View it in the UI or query system tables.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Method 1: UI — Catalog Explorer → Table → "Lineage" tab
-- Shows a visual graph: upstream → this table → downstream

-- Method 2: System tables (programmatic)
-- Find upstream sources of daily_revenue
SELECT 
    source_table_full_name,
    source_column_name,
    target_column_name
FROM system.access.table_lineage
WHERE target_table_full_name = 'production.analytics.daily_revenue';

-- Result:
-- source: production.sales.orders → target: production.analytics.daily_revenue
-- source: production.sales.returns → target: production.analytics.daily_revenue

-- Find downstream consumers (what would break if I change this table?)
SELECT 
    target_table_full_name,
    target_type  -- TABLE, VIEW, NOTEBOOK, DASHBOARD
FROM system.access.table_lineage
WHERE source_table_full_name = 'production.sales.orders';

-- Result: 12 tables, 3 views, 5 dashboards depend on production.sales.orders
```

**Key Points:**
- Lineage is captured automatically — no configuration needed
- Works for: SQL queries, DataFrame operations, DLT pipelines
- Shows: table-to-table AND column-to-column lineage
- Use cases: impact analysis (what breaks if I change this?), regulatory compliance (where did this number come from?), debugging (trace data quality issues upstream)
- Limitation: lineage is captured at EXECUTION time, not by parsing SQL (must run the query once)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Row-Level Security Implementation

**Scenario:** Your `production.sales.orders` table contains data from all regions (US, EU, APAC). Regional sales teams should only see their own region's data. Implement row-level security without creating separate tables or views per region.

<details>
<summary>💡 Hint</summary>
Use Unity Catalog row filters. Create a function that checks the user's group membership and filters rows accordingly.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Ensure groups exist mapping to regions
-- Groups: us-sales-team, eu-sales-team, apac-sales-team, global-leadership

-- Step 2: Create row filter function
CREATE OR REPLACE FUNCTION production.security.region_filter(region STRING)
RETURN 
    CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('global-leadership') THEN TRUE  -- Sees everything
        WHEN IS_ACCOUNT_GROUP_MEMBER('us-sales-team') THEN region = 'US'
        WHEN IS_ACCOUNT_GROUP_MEMBER('eu-sales-team') THEN region = 'EU'
        WHEN IS_ACCOUNT_GROUP_MEMBER('apac-sales-team') THEN region = 'APAC'
        ELSE FALSE  -- Default deny (users not in any team see nothing)
    END;

-- Step 3: Apply filter to table
ALTER TABLE production.sales.orders
SET ROW FILTER production.security.region_filter ON (region);

-- Step 4: Verify
-- As a US team member:
SELECT COUNT(*) FROM production.sales.orders;
-- Returns: 150,000 (only US orders)

-- As global leadership:
SELECT COUNT(*) FROM production.sales.orders;
-- Returns: 500,000 (all orders)

-- The filter is TRANSPARENT — same query, different results per user
-- No code changes needed in downstream notebooks/dashboards!
```

**Key Points:**
- Row filters are applied server-side — users can't bypass them
- Transparent to applications: same SQL, filtered results based on identity
- The function runs PER ROW — keep it simple to avoid performance impact
- Order matters: check the broadest group first (leadership → specific teams)
- Default DENY (return FALSE) is safest — explicitly allow, don't exclude
- Performance: row filters add latency proportional to table size (filter evaluates per row)
- Alternative: if performance is critical, partition by region and use schema-level permissions

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Delta Sharing Setup

**Scenario:** Your company needs to share daily sales aggregates with a partner company that uses Snowflake (not Databricks). They need read access to `production.analytics.partner_sales_summary` without you copying data to their system. Set up Delta Sharing.

<details>
<summary>💡 Hint</summary>
Create a Share, add the table, create a Recipient for the partner, and provide them with the sharing credentials file. They use the open Delta Sharing protocol to read from their Snowflake/Python/pandas.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create a share (logical container for shared tables)
CREATE SHARE partner_analytics;
COMMENT ON SHARE partner_analytics IS 'Sales aggregates shared with Partner Corp';

-- Step 2: Add table to the share
ALTER SHARE partner_analytics
ADD TABLE production.analytics.partner_sales_summary;

-- Optional: share a filtered view (they only see their data)
CREATE VIEW production.analytics.partner_filtered AS
SELECT sale_date, product_category, SUM(revenue) as revenue, SUM(units) as units
FROM production.analytics.daily_sales
WHERE partner_id = 'PARTNER_CORP'
GROUP BY sale_date, product_category;

ALTER SHARE partner_analytics
ADD TABLE production.analytics.partner_filtered;

-- Step 3: Create recipient (represents the partner organization)
CREATE RECIPIENT partner_corp
COMMENT 'Partner Corporation - analytics team';

-- Step 4: Get the activation link / credentials file
-- The partner receives a .share file or activation link
-- They use it to configure their client (Snowflake, Python, etc.)

-- Step 5: Grant share to recipient
GRANT SELECT ON SHARE partner_analytics TO RECIPIENT partner_corp;
```

```python
# PARTNER SIDE (Snowflake, Python, or any Delta Sharing client):

# Python/pandas:
import delta_sharing

profile = "partner_credentials.share"  # File received from provider
client = delta_sharing.SharingClient(profile)

# List available tables
tables = client.list_all_tables()
# [Table(share='partner_analytics', schema='default', name='partner_sales_summary')]

# Read into pandas
df = delta_sharing.load_as_pandas(f"{profile}#partner_analytics.default.partner_sales_summary")
print(df.head())
# Data is read DIRECTLY from provider's storage — no copy!
```

**Key Points:**
- Zero data duplication — partner reads directly from your Delta tables
- Open protocol — works with any Delta Sharing client (Python, Spark, Snowflake, Power BI)
- You control access: revoke anytime, audit who accessed what
- Share views to filter/aggregate data (partners see only what you expose)
- Works cross-cloud (your data on AWS, partner on Azure → still works)
- Automatic: when you update the source table, partner sees new data immediately

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Column Masking for PII

**Scenario:** The `production.sales.customers` table has PII columns (email, phone, address). Data analysts need to query this table for analytics but should NOT see actual PII values unless they're in the `pii-authorized` group. Implement column masking.

<details>
<summary>💡 Hint</summary>
Create masking functions for each sensitive column. Apply them with ALTER TABLE ALTER COLUMN SET MASK. Authorized users see real values; others see masked versions.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Create masking functions
CREATE OR REPLACE FUNCTION production.security.mask_email(val STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-authorized') THEN val
    ELSE CONCAT(LEFT(val, 2), '****@', SUBSTRING_INDEX(val, '@', -1))
END;

CREATE OR REPLACE FUNCTION production.security.mask_phone(val STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-authorized') THEN val
    ELSE CONCAT('***-***-', RIGHT(val, 4))
END;

CREATE OR REPLACE FUNCTION production.security.mask_address(val STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-authorized') THEN val
    ELSE '*** Redacted ***'
END;

-- Apply masks to columns
ALTER TABLE production.sales.customers
ALTER COLUMN email SET MASK production.security.mask_email;

ALTER TABLE production.sales.customers
ALTER COLUMN phone SET MASK production.security.mask_phone;

ALTER TABLE production.sales.customers
ALTER COLUMN address SET MASK production.security.mask_address;

-- Result for non-authorized user:
-- SELECT customer_id, name, email, phone FROM production.sales.customers LIMIT 3;
-- | 1001 | John Smith | jo****@gmail.com | ***-***-4567 |
-- | 1002 | Jane Doe   | ja****@company.com | ***-***-8901 |

-- Result for pii-authorized user (same query):
-- | 1001 | John Smith | john.smith@gmail.com | 555-123-4567 |
-- | 1002 | Jane Doe   | jane.doe@company.com | 555-789-8901 |
```

**Key Points:**
- Column masks are transparent — same SQL returns different values per user
- Non-PII columns (customer_id, name) remain visible to everyone
- Masked values are still queryable: analysts can GROUP BY email domain, count by region, etc.
- Masks apply to ALL access paths (SQL, DataFrame, JDBC, dashboards)
- Performance: negligible overhead (simple CASE expression per row)
- Combine with row filters for complete data governance

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Storage Credentials and External Locations

**Scenario:** Your company has three S3 buckets (`s3://raw-data/`, `s3://curated-data/`, `s3://ml-artifacts/`). Different teams need access to different buckets. Set up the storage credential and external locations hierarchy.

<details>
<summary>💡 Hint</summary>
One storage credential (IAM role) can access all buckets. Then create separate external locations per bucket and grant access to different teams per location.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Storage credential (wraps IAM role that can access all 3 buckets)
CREATE STORAGE CREDENTIAL lake_access_credential
WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/unity-catalog-lake-role');
-- The IAM role has S3 access to all three buckets

-- Step 2: Create external locations (one per bucket/team boundary)
CREATE EXTERNAL LOCATION raw_data_location
URL 's3://raw-data/'
WITH (STORAGE CREDENTIAL lake_access_credential);

CREATE EXTERNAL LOCATION curated_data_location
URL 's3://curated-data/'
WITH (STORAGE CREDENTIAL lake_access_credential);

CREATE EXTERNAL LOCATION ml_artifacts_location
URL 's3://ml-artifacts/'
WITH (STORAGE CREDENTIAL lake_access_credential);

-- Step 3: Grant permissions per team
-- Data engineers: can read/write raw and curated
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES
ON EXTERNAL LOCATION raw_data_location TO `data-engineers`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES
ON EXTERNAL LOCATION curated_data_location TO `data-engineers`;

-- Data scientists: read curated, read/write ML artifacts
GRANT READ FILES ON EXTERNAL LOCATION curated_data_location TO `data-scientists`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ml_artifacts_location TO `data-scientists`;

-- Analysts: read curated only
GRANT READ FILES ON EXTERNAL LOCATION curated_data_location TO `data-analysts`;

-- Step 4: Teams can now create external tables in their permitted locations
CREATE TABLE production.raw.events
LOCATION 's3://raw-data/events/2024/';  -- Works for data-engineers
-- Would FAIL for data-analysts (no permission on raw_data_location)
```

**Key Points:**
- Storage credential: the cloud-level access (IAM role / service principal)
- External location: maps a URL path to a credential + controls who can use it
- Separation of concerns: one credential, multiple locations with different access rules
- Teams can only create external tables in locations they have access to
- This replaces the old pattern of sharing AWS keys or instance profiles directly
- Audit: system.access.audit tracks all file reads/writes through external locations

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Migrating from Hive Metastore

**Scenario:** Your workspace has 200 tables in the `hive_metastore` with cluster-level ACLs. You need to migrate to Unity Catalog with minimal disruption. Users are running daily jobs that reference tables by two-level names (`schema.table`). Plan the migration.

<details>
<summary>💡 Hint</summary>
Use a phased approach: (1) set up UC structure, (2) sync/migrate tables, (3) set up grants, (4) update code to use three-level names (or set defaults), (5) deprecate hive_metastore.
</details>

<details>
<summary>✅ Solution</summary>

```python
# PHASED MIGRATION PLAN (4 weeks)

# WEEK 1: Setup and assessment
# 1a: Create UC catalog/schema structure matching current Hive databases
spark.sql("CREATE CATALOG production")
for db in spark.sql("SHOW DATABASES IN hive_metastore").collect():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS production.{db.databaseName}")

# 1b: Assess tables
assessment = spark.sql("""
    SELECT database, tableName, tableType, 
           CASE WHEN location LIKE 'dbfs:%' THEN 'managed' ELSE 'external' END as storage_type
    FROM hive_metastore.information_schema.tables
""")
assessment.show()
# Managed DBFS tables: need data movement (CTAS)
# External tables: just register (no data movement)

# WEEK 2: Migrate tables
# External tables (fast — just metadata):
for row in assessment.filter("storage_type = 'external'").collect():
    spark.sql(f"""
        CREATE TABLE production.{row.database}.{row.tableName}
        LOCATION '{row.location}'
    """)

# Managed tables (slower — copies data):
for row in assessment.filter("storage_type = 'managed'").collect():
    spark.sql(f"""
        CREATE TABLE production.{row.database}.{row.tableName}
        AS SELECT * FROM hive_metastore.{row.database}.{row.tableName}
    """)

# WEEK 3: Set up permissions + update code
# Map old ACLs to UC grants
spark.sql("GRANT SELECT ON SCHEMA production.sales TO `data-analysts`")
spark.sql("GRANT ALL PRIVILEGES ON SCHEMA production.raw TO `data-engineers`")

# Bridge: set default catalog so old code still works
# Cluster config: spark.sql.defaultCatalog = production
# Now "SELECT * FROM sales.orders" resolves to production.sales.orders

# WEEK 4: Validate and cut over
# Run all jobs pointing to UC (with default catalog set)
# Validate row counts match between hive_metastore and production catalog
# Deprecate hive_metastore access:
spark.sql("REVOKE ALL PRIVILEGES ON CATALOG hive_metastore FROM `all-users`")
```

**Key Points:**
- External tables migrate instantly (just register the existing path — no data copy)
- Managed tables require CTAS (copies data to UC-managed storage — can take hours for large tables)
- Set `spark.sql.defaultCatalog = production` as a bridge (old code keeps working)
- Migrate permissions: map cluster ACLs → UC group grants (often a simplification)
- Validate: compare row counts between old and new tables before cutting over
- Keep hive_metastore readable for 2 weeks as rollback safety net

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Cloud Governance Design

**Scenario:** Your company has Databricks workspaces on both AWS (us-east-1) and Azure (westeurope). Data lives in both clouds. Design a unified governance architecture using Unity Catalog that enables cross-cloud data access with consistent permissions.

<details>
<summary>💡 Hint</summary>
Unity Catalog metastore is regional. You need one metastore per region, with Delta Sharing for cross-cloud access. Permissions can be managed centrally through Terraform/automation.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE:
# AWS (us-east-1): Metastore A + Workspaces (ETL, Analytics)
# Azure (westeurope): Metastore B + Workspaces (ML, EU Analytics)
# Challenge: same permission model across both, cross-cloud data access

# SOLUTION: Central permission management + Delta Sharing for cross-cloud

# terraform/main.tf (manages both clouds)
"""
# AWS metastore
resource "databricks_metastore" "aws_us_east" {
  provider = databricks.aws
  name     = "aws-us-east-1"
  storage_root = "s3://uc-metastore-us-east-1/"
  region   = "us-east-1"
}

# Azure metastore
resource "databricks_metastore" "azure_westeurope" {
  provider = databricks.azure
  name     = "azure-westeurope"
  storage_root = "abfss://uc-metastore@storageaccount.dfs.core.windows.net/"
  region   = "westeurope"
}

# Same catalog structure on both
resource "databricks_catalog" "production_aws" {
  provider = databricks.aws
  name     = "production"
  metastore_id = databricks_metastore.aws_us_east.id
}

resource "databricks_catalog" "production_azure" {
  provider = databricks.azure
  name     = "production"
  metastore_id = databricks_metastore.azure_westeurope.id
}

# Same grants on both (managed by Terraform for consistency)
resource "databricks_grants" "analysts_aws" {
  provider = databricks.aws
  catalog  = databricks_catalog.production_aws.name
  grant { principal = "data-analysts"; privileges = ["USE_CATALOG", "USE_SCHEMA"] }
}

resource "databricks_grants" "analysts_azure" {
  provider = databricks.azure
  catalog  = databricks_catalog.production_azure.name
  grant { principal = "data-analysts"; privileges = ["USE_CATALOG", "USE_SCHEMA"] }
}
"""

# CROSS-CLOUD DATA ACCESS via Delta Sharing:
# AWS tables shared to Azure workspaces (and vice versa)
```

```sql
-- On AWS: share tables that Azure needs to read
CREATE SHARE cross_cloud_share;
ALTER SHARE cross_cloud_share ADD TABLE production.sales.orders;  -- AWS data

-- Create recipient for Azure workspace
CREATE RECIPIENT azure_workspace
USING ID 'azure-workspace-sharing-id';
GRANT SELECT ON SHARE cross_cloud_share TO RECIPIENT azure_workspace;

-- On Azure: mount the share as a catalog
CREATE CATALOG aws_shared_data
USING SHARE aws_provider.cross_cloud_share;

-- Azure users query AWS data seamlessly:
SELECT * FROM aws_shared_data.default.orders WHERE region = 'EU';
```

**Key Points:**
- One metastore per cloud region (can't span clouds within a single metastore)
- Terraform ensures identical permission structure across both environments
- Delta Sharing bridges the gap for cross-cloud data access (no data duplication)
- SCIM/identity provider syncs groups to both metastores (same group names)
- Audit logs are per-metastore — aggregate for cross-cloud compliance reporting
- GDPR: EU data stays in Azure (westeurope), Delta Sharing allows US teams to query it without copying to US

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: GDPR Compliance Implementation

**Scenario:** A customer exercises their GDPR "right to erasure" — you must delete ALL their data across your entire lakehouse within 30 days. Your lakehouse has 50+ tables, some with nested/denormalized customer data. Design the deletion process.

<details>
<summary>💡 Hint</summary>
Use Unity Catalog lineage to find all tables containing customer data. Delta Lake supports DELETE and VACUUM for physical deletion. Challenge: time travel retains old data until VACUUM.
</details>

<details>
<summary>✅ Solution</summary>

```python
class GDPRErasurePipeline:
    """Delete customer data from all tables in the lakehouse."""
    
    def process_erasure_request(self, customer_id: int):
        """Complete GDPR erasure for a customer."""
        
        # Step 1: Find all tables containing customer data (using lineage)
        affected_tables = self.find_customer_tables(customer_id)
        print(f"Found {len(affected_tables)} tables with customer data")
        
        # Step 2: Delete from each table
        deletion_log = []
        for table_name in affected_tables:
            rows_deleted = self.delete_from_table(table_name, customer_id)
            deletion_log.append({"table": table_name, "rows_deleted": rows_deleted})
        
        # Step 3: VACUUM to physically remove data (critical for GDPR!)
        for table_name in affected_tables:
            spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")
            # Note: requires setting spark.databricks.delta.retentionDurationCheck.enabled = false
        
        # Step 4: Audit trail (prove deletion happened)
        self.log_erasure(customer_id, deletion_log)
        
        return deletion_log
    
    def find_customer_tables(self, customer_id: int) -> list[str]:
        """Use UC lineage + schema inspection to find all tables with customer data."""
        # Method 1: Column lineage from the customers table
        lineage = spark.sql(f"""
            SELECT DISTINCT target_table_full_name 
            FROM system.access.column_lineage
            WHERE source_table_full_name = 'production.sales.customers'
        """).collect()
        
        # Method 2: Search for columns named 'customer_id' across all tables
        tables_with_customer_col = spark.sql("""
            SELECT table_catalog || '.' || table_schema || '.' || table_name as full_name
            FROM system.information_schema.columns
            WHERE column_name = 'customer_id'
              AND table_catalog = 'production'
        """).collect()
        
        all_tables = set([r.target_table_full_name for r in lineage] + 
                        [r.full_name for r in tables_with_customer_col])
        
        return list(all_tables)
    
    def delete_from_table(self, table_name: str, customer_id: int) -> int:
        """Delete customer's rows from a specific table."""
        # Check which column contains customer reference
        columns = spark.sql(f"DESCRIBE {table_name}").collect()
        customer_cols = [c.col_name for c in columns if 'customer' in c.col_name.lower()]
        
        if not customer_cols:
            return 0
        
        col = customer_cols[0]
        result = spark.sql(f"DELETE FROM {table_name} WHERE {col} = {customer_id}")
        rows_affected = result.collect()[0]["num_affected_rows"]
        return rows_affected
    
    def log_erasure(self, customer_id: int, log: list[dict]):
        """Create audit record of the erasure."""
        spark.sql(f"""
            INSERT INTO production.compliance.erasure_log
            VALUES ({customer_id}, current_timestamp(), '{json.dumps(log)}', 'completed')
        """)

# CRITICAL: VACUUM is what actually deletes data from storage
# Without VACUUM: old file versions still contain the customer's data (Delta time travel)
# RETAIN 0 HOURS: immediately remove old versions (normally 7-day retention)
# This must be done table-by-table after DELETE
```

**Key Points:**
- Unity Catalog lineage finds WHERE customer data lives (column-level lineage)
- Delta DELETE marks rows as deleted but doesn't remove files (just adds new file version)
- VACUUM RETAIN 0 HOURS physically removes old file versions (actual erasure)
- Must disable retention check: `spark.databricks.delta.retentionDurationCheck.enabled = false`
- After VACUUM: time travel to before the deletion is no longer possible (data truly gone)
- Audit trail: log every deletion for compliance proof (who, what, when)
- Challenge: denormalized tables where customer_id is embedded in arrays/structs

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Performance-Optimized Permission Design

**Scenario:** Your Unity Catalog has 10,000 tables across 50 schemas. Queries are taking 200-500ms extra due to permission checks (row filters on 20 tables, column masks on 15 tables). How do you optimize for performance while maintaining governance?

<details>
<summary>💡 Hint</summary>
Simplify row filter functions (avoid subqueries), use pre-computed security tables, consider materialized views for filtered data, and restructure permissions to minimize per-row evaluation.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- DIAGNOSIS: Row filters with subqueries are the performance killer
-- BAD (slow): subquery executes PER ROW
CREATE FUNCTION slow_filter(region STRING)
RETURN region IN (
    SELECT allowed_region FROM security.user_permissions  -- Subquery per row!
    WHERE user_email = CURRENT_USER()
);

-- GOOD (fast): use IS_ACCOUNT_GROUP_MEMBER (cached, O(1))
CREATE OR REPLACE FUNCTION fast_filter(region STRING)
RETURN 
    IS_ACCOUNT_GROUP_MEMBER('global-access') OR  -- Short-circuit if global
    (IS_ACCOUNT_GROUP_MEMBER('us-team') AND region = 'US') OR
    (IS_ACCOUNT_GROUP_MEMBER('eu-team') AND region = 'EU');
-- Group membership is cached — no per-row lookup!

-- OPTIMIZATION 2: Partition by the filter column
-- If filtering by region, partition the table by region
-- Delta skips entire partitions that don't match the filter → huge speedup
ALTER TABLE production.sales.orders
SET TBLPROPERTIES ('delta.partitionColumns' = 'region');

-- Row filter + partition pruning: filter evaluates region = 'US',
-- Spark reads ONLY the 'US' partition files (instead of scanning all)

-- OPTIMIZATION 3: Materialized security views (for complex filters)
-- Pre-compute filtered datasets for each team
CREATE TABLE production.sales.orders_us AS
SELECT * FROM production.sales.orders WHERE region = 'US';
-- Grant team direct access to pre-filtered table (no row filter overhead)
GRANT SELECT ON TABLE production.sales.orders_us TO `us-sales-team`;

-- OPTIMIZATION 4: Minimize masked columns
-- Only mask columns that truly need it (email, phone)
-- Don't mask: customer_id, region, status (not PII)
-- Each masked column adds function evaluation per row

-- MEASUREMENT: Track permission check latency
SELECT action_name, 
       percentile(response_time_ms, 0.50) as p50,
       percentile(response_time_ms, 0.99) as p99
FROM system.access.audit
WHERE event_date >= current_date() - 7
  AND action_name = 'commandSubmit'
GROUP BY action_name;
```

**Key Points:**
- Row filters with subqueries are O(n × query) — devastating for large tables
- `IS_ACCOUNT_GROUP_MEMBER()` is O(1) — cached, extremely fast
- Partition tables by the filter column — Delta skips irrelevant partitions entirely
- For complex filters: pre-materialize filtered views per team (trade storage for speed)
- Column masks: simple CASE WHEN is fast; avoid calling external functions
- Measure: compare query times with/without filters to quantify overhead
- Budget: <50ms overhead per query is acceptable; >200ms needs optimization

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Disaster Recovery and Metastore HA

**Scenario:** Your Unity Catalog metastore becomes unavailable (regional outage). All workspaces in the region can't resolve table names or check permissions. Design a disaster recovery plan.

<details>
<summary>💡 Hint</summary>
UC metastore is managed by Databricks (you can't replicate it yourself). DR strategy: secondary region with its own metastore, Delta Sharing for critical data, and well-documented manual failover procedures.
</details>

<details>
<summary>✅ Solution</summary>

```python
# UNITY CATALOG DR ARCHITECTURE:
# Primary: us-east-1 (Metastore A, all workspaces)
# Secondary: us-west-2 (Metastore B, standby workspaces)

# STRATEGY 1: Warm standby with Delta Sharing
# Critical tables shared from primary to secondary metastore
# On failover: secondary reads from shared data + has its own permissions

DR_CONFIG = {
    "primary_region": "us-east-1",
    "secondary_region": "us-west-2",
    "rpo": "1 hour (data) / 24 hours (permissions)",
    "rto": "30 minutes (if pre-configured) / 4 hours (cold start)",
    "shared_tables": [
        "production.sales.orders",
        "production.analytics.daily_revenue",
        "production.ml.model_registry",
    ],
}

# STRATEGY 2: Terraform state for permission recreation
# All UC objects (catalogs, schemas, grants) defined in Terraform
# On failover: terraform apply against secondary metastore recreates structure
"""
# terraform apply -target=module.dr_metastore
# Recreates: catalogs, schemas, grants, external locations
# Does NOT recreate: data (already on S3, accessible from any region)
"""

# STRATEGY 3: Cross-region S3 replication for data
# S3 data replicated to us-west-2 (standard S3 CRR)
# External locations in secondary metastore point to replicated bucket
# On failover: tables already have data (RPO = replication lag)

# FAILOVER RUNBOOK:
failover_steps = [
    "1. Confirm primary metastore is unreachable (not just a blip)",
    "2. Activate secondary workspaces (pre-configured, warm standby)",
    "3. Run Terraform to ensure permissions are current in secondary metastore",
    "4. Verify S3 replication is caught up (check replication metrics)",
    "5. Switch DNS/routing for user access to secondary workspaces",
    "6. Notify teams: primary is down, use secondary workspaces",
    "7. Monitor: run health checks on secondary",
    "8. After primary recovery: reverse-replicate any data created during outage",
]
```

**Key Points:**
- UC metastore is Databricks-managed — you can't replicate it yourself (unlike self-hosted HMS)
- Data (in S3/ADLS) is YOUR responsibility to replicate cross-region
- Permissions: store as Infrastructure-as-Code (Terraform) for fast recreation
- Delta Sharing provides read access to critical tables from secondary region
- RTO target: 30 min for pre-configured standby, 4 hours for cold start
- Test DR quarterly: actually fail over, run critical jobs, fail back
- Cost of warm standby: secondary workspace + S3 CRR = ~$500-2000/month depending on data size

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Information Schema and Automation

**Scenario:** You manage Unity Catalog for a 200-person data team. Build automated governance: (1) detect tables without owners, (2) find over-privileged users, (3) alert on stale tables, (4) enforce naming conventions. Use UC system tables.

<details>
<summary>💡 Hint</summary>
Query system.information_schema and system.access tables programmatically. Run as a nightly Databricks job. Alert via Slack/email on policy violations.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- AUTOMATED GOVERNANCE CHECKS (run nightly as a Databricks job)

-- Check 1: Tables without owners (compliance risk)
SELECT table_catalog, table_schema, table_name, table_owner
FROM system.information_schema.tables
WHERE table_catalog = 'production'
  AND (table_owner IS NULL OR table_owner = '');
-- Action: assign owner or flag for review

-- Check 2: Over-privileged users (users with ALL_PRIVILEGES on production)
SELECT grantee, privilege_type, table_catalog, table_schema
FROM system.information_schema.table_privileges
WHERE table_catalog = 'production'
  AND privilege_type = 'ALL_PRIVILEGES'
  AND grantee NOT IN ('platform-team', 'etl-service-principal');
-- Action: alert security team, reduce to minimum necessary privileges

-- Check 3: Stale tables (not updated in 90 days)
SELECT table_catalog, table_schema, table_name, last_altered,
       DATEDIFF(current_date(), last_altered) as days_stale
FROM system.information_schema.tables
WHERE table_catalog = 'production'
  AND DATEDIFF(current_date(), last_altered) > 90
  AND table_schema NOT IN ('archive', 'reference')
ORDER BY days_stale DESC;
-- Action: notify table owner, consider archiving

-- Check 4: Naming convention violations
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_catalog = 'production'
  AND (
    table_name RLIKE '[A-Z]'             -- No uppercase
    OR table_name RLIKE '\\s'             -- No spaces
    OR table_name NOT RLIKE '^[a-z]'      -- Must start with lowercase letter
    OR LENGTH(table_name) > 60            -- Not too long
  );
-- Action: flag for renaming in next sprint

-- Check 5: Unused tables (no queries in 30 days)
SELECT t.table_catalog, t.table_schema, t.table_name
FROM system.information_schema.tables t
LEFT JOIN (
    SELECT DISTINCT request_params.full_name_arg as table_accessed
    FROM system.access.audit
    WHERE event_date >= current_date() - 30
      AND action_name = 'commandSubmit'
) a ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = a.table_accessed
WHERE t.table_catalog = 'production'
  AND a.table_accessed IS NULL;
-- Action: candidate for archiving/deletion
```

```python
# Wrap in a scheduled job with alerting
class GovernanceAutomation:
    def run_nightly_checks(self):
        violations = {}
        
        # Run each check
        violations["no_owner"] = spark.sql(no_owner_query).count()
        violations["over_privileged"] = spark.sql(over_privileged_query).count()
        violations["stale_tables"] = spark.sql(stale_query).count()
        violations["naming_violations"] = spark.sql(naming_query).count()
        
        # Alert if violations exceed threshold
        if violations["over_privileged"] > 0:
            self.alert_security_team(violations["over_privileged"])
        
        if violations["no_owner"] > 10:
            self.alert_platform_team(f"{violations['no_owner']} tables without owners")
        
        # Store results for trending
        spark.sql(f"""
            INSERT INTO production.governance.check_results
            VALUES (current_timestamp(), '{json.dumps(violations)}')
        """)
        
        return violations
```

**Key Points:**
- System tables (`system.information_schema.*`, `system.access.*`) are your governance API
- Run checks nightly as a Databricks workflow (low cost, high value)
- Track violations over time — trending UP = governance is degrading
- Automate alerts: over-privilege and stale data are the highest-risk findings
- Naming conventions: enforce early (hard to rename tables with downstream dependencies)
- Connect to your ticketing system (JIRA) to create remediation tickets automatically
- Cost: essentially free (queries on system tables, one small job per night)

</details>

</article>
