---
title: "Unity Catalog - Intermediate"
topic: databricks
subtopic: unity-catalog
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, unity-catalog, row-filters, column-masks, data-sharing, volumes]
---

# Unity Catalog — Intermediate

## Row-Level Security (Row Filters)

Restrict which rows a user can see without creating separate views:

```sql
-- Create a function that defines the filter logic
CREATE FUNCTION production.sales.region_filter(region_col STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('us-team'), region_col = 'US',
       IF(IS_ACCOUNT_GROUP_MEMBER('eu-team'), region_col = 'EU', FALSE));

-- Apply row filter to table
ALTER TABLE production.sales.orders
SET ROW FILTER production.sales.region_filter ON (region);

-- Now:
-- US team members only see rows where region = 'US'
-- EU team members only see rows where region = 'EU'
-- Others see no rows (filter returns FALSE)

-- The filter is transparent — users query normally:
SELECT * FROM production.sales.orders;
-- Each user sees only their permitted rows
```

---

## Column Masking

Hide sensitive column values based on user group:

```sql
-- Create masking function
CREATE FUNCTION production.sales.mask_email(email_col STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('pii-authorized'), email_col, 
       CONCAT(LEFT(email_col, 2), '***@***.com'));

-- Apply mask to column
ALTER TABLE production.sales.customers
ALTER COLUMN email SET MASK production.sales.mask_email;

-- Result:
-- PII-authorized users see: john.smith@company.com
-- Other users see: jo***@***.com
```

---

## Volumes (Unstructured Data Governance)

Volumes govern access to files (images, PDFs, logs, ML artifacts):

```sql
-- Create managed volume (UC manages storage)
CREATE VOLUME production.ml.model_artifacts;

-- Create external volume (you control storage)
CREATE EXTERNAL VOLUME production.raw.landing_files
LOCATION 's3://data-lake/landing/';

-- Grant access
GRANT READ VOLUME ON VOLUME production.raw.landing_files TO `data-engineers`;

-- Use in code
-- Read files from volume
df = spark.read.format("csv").load("/Volumes/production/raw/landing_files/daily/")

-- Write to volume
dbutils.fs.cp("/tmp/report.pdf", "/Volumes/production/reports/monthly/report_2024_03.pdf")
```

---

## Delta Sharing (Cross-Organization Data Sharing)

Share data externally without copying:

```sql
-- Provider side: create share and add tables
CREATE SHARE customer_analytics_share;

ALTER SHARE customer_analytics_share
ADD TABLE production.analytics.customer_segments;

-- Create recipient (external org)
CREATE RECIPIENT partner_company
USING ID 'partner-databricks-sharing-id';

-- Grant share to recipient
GRANT SELECT ON SHARE customer_analytics_share TO RECIPIENT partner_company;

-- Recipient side: create catalog from share
CREATE CATALOG partner_data
USING SHARE provider_org.customer_analytics_share;

-- Query shared data (reads directly from provider's storage — no copy!)
SELECT * FROM partner_data.default.customer_segments;
```

**Key benefits:**
- No data duplication (reader accesses provider's storage directly)
- Works across clouds (AWS ↔ Azure ↔ GCP)
- Works with non-Databricks recipients (open protocol)
- Provider controls access (revoke anytime)

---

## Identity Federation and SCIM

Sync users and groups from your identity provider:

```python
# SCIM provisioning (Azure AD, Okta, etc.)
# Groups synced automatically:
# IdP Group "data-engineers" → Databricks Group "data-engineers"
# Any user added/removed in IdP → automatically reflected in Databricks

# Service principals for automation
# Create SP for CI/CD pipelines:
# databricks service-principals create --display-name "cicd-pipeline"
# Grant permissions to SP just like a user:
```

```sql
GRANT ALL PRIVILEGES ON CATALOG staging TO `cicd-pipeline-sp`;
GRANT USAGE ON CATALOG production TO `cicd-pipeline-sp`;
GRANT SELECT ON SCHEMA production.sales TO `cicd-pipeline-sp`;
```

---

## Metastore Administration

```sql
-- View metastore info
SELECT * FROM system.information_schema.catalogs;

-- View all grants in the metastore
SELECT * FROM system.information_schema.table_privileges
WHERE grantee = 'data-analysts';

-- System tables for monitoring
SELECT * FROM system.access.audit WHERE event_date = current_date();
SELECT * FROM system.billing.usage WHERE usage_date >= '2024-01-01';
SELECT * FROM system.compute.clusters WHERE state = 'RUNNING';
```

---

## Best Practices

| Practice | Recommendation |
|----------|---------------|
| Catalog naming | `production`, `staging`, `development` (by environment) |
| Schema naming | By domain: `sales`, `marketing`, `finance` |
| Permission model | Grant to GROUPS, never individual users |
| External locations | One per team/domain (not per table) |
| Service principals | One per pipeline/application |
| Audit | Review access logs weekly, alert on privilege escalation |

```sql
-- Anti-pattern: granting to individual users
GRANT SELECT ON TABLE orders TO `john@company.com`;  -- BAD

-- Best practice: grant to groups
GRANT SELECT ON TABLE orders TO `sales-analysts`;     -- GOOD
-- When John leaves, just remove him from the group — no permission cleanup needed
```

---

## Interview Tips

> **Tip 1:** "How do you implement row-level security in Databricks?" — Use Unity Catalog row filters: create a function that returns TRUE/FALSE based on the user's group membership, then apply it to the table with ALTER TABLE SET ROW FILTER. Users query the table normally and only see their permitted rows — completely transparent.

> **Tip 2:** "How does Delta Sharing work?" — The provider creates a Share, adds tables to it, and grants access to Recipients. Recipients mount the share as a catalog and query it directly. Data is NOT copied — reads go to the provider's storage. It's an open protocol that works across clouds and with non-Databricks tools.

> **Tip 3:** "What's the permission model?" — Three-level inheritance: Metastore → Catalog → Schema → Table. Grant to groups (not users). Permission cascades downward: GRANT on schema = access to all current and future tables. Use DENY for explicit exceptions. Always use IS_ACCOUNT_GROUP_MEMBER() for dynamic access control.
