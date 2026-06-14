---
title: "Data Governance & Lineage - Fundamentals"
topic: databricks
subtopic: data-governance-lineage
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, unity-catalog, governance, lineage, data-quality, access-control]
---

# Data Governance & Lineage — Fundamentals

## 🎯 Analogy

Data governance is like an organization's compliance and records management department. Someone decides what data exists (catalog), who can see it (access control), what it means (data dictionary), and where it came from (lineage). Without governance, data becomes a liability — GDPR fines, wrong business decisions, nobody knowing which "revenue" table is the right one.

---

## What Is Data Governance?

Data governance = the policies, processes, and technology that ensure data is accurate, secure, discoverable, and used appropriately.

**Four pillars in Databricks:**

| Pillar | Tool | Purpose |
|--------|------|---------|
| **Discovery** | Unity Catalog | Find and understand what data exists |
| **Access Control** | UC Permissions | Control who can read, write, or manage data |
| **Lineage** | UC System Tables | Track where data came from and flows to |
| **Quality** | Delta constraints + DLT | Enforce data quality rules |

---

## Unity Catalog: The Governance Layer

Unity Catalog (UC) is Databricks' central metadata and governance layer. Three-level namespace:

```
Catalog → Schema → Table/View/Function/Volume
   ↑          ↑           ↑
 prod     marketing    customers
```

```sql
-- Create catalog → schema → table
CREATE CATALOG IF NOT EXISTS prod;
CREATE SCHEMA IF NOT EXISTS prod.marketing;

CREATE TABLE prod.marketing.customers (
    customer_id  STRING NOT NULL,
    email        STRING,
    signup_date  DATE,
    ltv          DOUBLE
);
```

**Unity Catalog stores:**
- Table metadata (schema, location, properties)
- Ownership and permissions
- Data lineage (automatically captured)
- Tags and comments for discovery

---

## Column and Row-Level Access Control

```sql
-- Grant table access
GRANT SELECT ON TABLE prod.marketing.customers TO `analyst-group`;
GRANT ALL PRIVILEGES ON SCHEMA prod.analytics TO `data-engineers`;

-- Column masking — hide PII from non-privileged users
CREATE OR REPLACE FUNCTION prod.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii-access') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@***.com')
END;

ALTER TABLE prod.marketing.customers
    ALTER COLUMN email
    SET MASK prod.security.mask_email;

-- Row filter — data engineers only see their region's data
CREATE OR REPLACE FUNCTION prod.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN region = CURRENT_USER() OR IS_ACCOUNT_GROUP_MEMBER('global-access');

ALTER TABLE prod.marketing.customers
    SET ROW FILTER prod.security.region_filter ON (region);
```

---

## Data Lineage

Unity Catalog automatically captures column-level lineage — no instrumentation needed:

```sql
-- View lineage in system tables
SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name,
    created_by
FROM system.lineage.column_lineage
WHERE target_table_full_name = 'prod.marketing.customer_segments'
ORDER BY created_at DESC;
```

**What lineage captures automatically:**
- Which tables/views feed into which tables
- Column-level: which source columns are used to compute each target column
- Which notebooks, jobs, and users created each transformation

---

## Tags and Comments for Discovery

```sql
-- Add descriptions so others know what tables mean
COMMENT ON TABLE prod.marketing.customers IS
    'Canonical customer table. Source of truth for customer profiles. Updated daily at 3am UTC. Owner: data-platform-team.';

COMMENT ON COLUMN prod.marketing.customers.ltv IS
    'Predicted customer lifetime value in USD. 90-day horizon. See model docs at /models/ltv-v3.';

-- Add tags for classification
ALTER TABLE prod.marketing.customers
    SET TAGS ('contains_pii' = 'true',
              'data_classification' = 'confidential',
              'owner' = 'data-platform',
              'gdpr_subject' = 'true');
```

---

## Data Quality Constraints

```sql
-- Add constraints to catch bad data at write time
ALTER TABLE prod.marketing.customers
    ADD CONSTRAINT customer_id_not_null CHECK (customer_id IS NOT NULL);

ALTER TABLE prod.marketing.customers
    ADD CONSTRAINT valid_ltv CHECK (ltv >= 0);

ALTER TABLE prod.marketing.orders
    ADD CONSTRAINT positive_amount CHECK (amount > 0);

-- Constraint violations cause writes to fail — never silently accept bad data
```

---

## Interview Tips

> **Tip 1:** "What is Unity Catalog and what problems does it solve?" — "Unity Catalog is Databricks' central governance layer — a single metastore for all workspaces that provides (1) three-level namespace (catalog.schema.table), (2) fine-grained access control (table, column, row level), (3) automatic data lineage, and (4) tags/comments for discovery. Before UC, each workspace had a separate Hive metastore with no cross-workspace governance."

> **Tip 2:** "What's the difference between row filters and column masks?" — "Column masks hide or transform specific column values based on who's querying — e.g., show full email to PII-access group, masked email to everyone else. Row filters restrict which rows are visible — e.g., each regional analyst only sees their region's data. Both are functions attached to the table and applied transparently at query time."

> **Tip 3:** "How is data lineage captured in Unity Catalog?" — "Automatically — no code instrumentation required. Every time a Spark job or SQL query reads from table A and writes to table B, UC records that lineage in `system.lineage.column_lineage`. Column-level: if you compute `revenue = price * quantity`, UC records that `revenue` in the target table depends on `price` and `quantity` in the source."
