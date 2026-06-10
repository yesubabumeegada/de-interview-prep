---
title: "Unity Catalog Governance — Scenarios"
topic: data-governance
subtopic: unity-catalog-governance
content_type: scenario_question
tags: [unity-catalog, databricks, interview, scenarios, governance]
---

# Unity Catalog Governance — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Setting Up Access for a New Analyst

**Scenario:** A new data analyst joins. They need read access to gold tables in Databricks but must NOT see raw customer emails (PII). How do you set this up in Unity Catalog?

<details>
<summary>💡 Hint</summary>

In Unity Catalog, access is managed through groups synced from your IdP (Azure AD / Okta via SCIM). Add the analyst to the appropriate group — if the group already has `SELECT ON prod.gold.*`, the user inherits access in 5–10 minutes. For PII masking, Unity Catalog uses *column masking functions* applied to the table: non-privileged users see `NULL` or a masked value for PII columns automatically, no application changes needed. Verify both: the group grant exists, and the masking policy is applied to `email`.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Add analyst to the correct IdP group**
```
In Azure AD / Okta: add user to group 'data-analysts'
(UC syncs groups via SCIM — takes ~5-10 minutes)
```

**Step 2: Verify existing group grants (should already be set)**
```sql
-- Check what data-analysts group can access
SHOW GRANTS TO `data-analysts`;
-- Should show: USE CATALOG prod, USE SCHEMA prod.gold, SELECT ON prod.gold.*
```

**Step 3: Verify PII column masking is in place**
```sql
-- Check masking policy on customer_email column
DESCRIBE EXTENDED prod.gold.orders customer_email;
-- Should show: MASK FUNCTION: prod.gold.mask_email

-- Test as the new user (impersonate)
-- As analyst: SELECT customer_email FROM prod.gold.orders LIMIT 5
-- → Should return hashed values, not real emails
```

**Step 4: If PII masking NOT in place, apply it**
```sql
-- Create masking function (if not exists)
CREATE OR REPLACE FUNCTION prod.gold.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data-pii-approved') THEN email
  ELSE sha2(lower(coalesce(email, '')), 256)
END;

-- Apply to column
ALTER TABLE prod.gold.orders
  ALTER COLUMN customer_email
  SET MASK prod.gold.mask_email;
```

**Step 5: Validate end-to-end**
```sql
-- Run as analyst to verify
SELECT order_id, customer_email, amount_usd
FROM prod.gold.orders
LIMIT 5;
-- customer_email should show hashes, not real emails
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Migrating from Hive Metastore to Unity Catalog

**Scenario:** Your team has 50 Delta tables in the Hive Metastore. You need to migrate them to Unity Catalog while maintaining all access and adding governance. How do you approach this?

<details>
<summary>💡 Hint</summary>

Migration has three phases: inventory first (which tables exist, are they managed or external, who uses them), then migrate in batches (start with non-critical, use `CREATE TABLE ... LOCATION` for external or `DEEP CLONE` for managed), then cut over access. The big decision is managed vs external: managed tables move their data to UC storage; external tables just register a new pointer. Run both catalogs in parallel during migration so consumers don't break.

</details>

<details>
<summary>✅ Solution</summary>

**Phase 1: Assessment (Week 1)**
```python
# Inventory all Hive tables
tables = spark.sql("SHOW TABLES IN hive_metastore.gold").collect()
print(f"Found {len(tables)} tables to migrate")

# Assess each table: size, type (managed vs external), last modified
for table in tables:
    info = spark.sql(f"DESCRIBE EXTENDED hive_metastore.gold.{table.tableName}").collect()
    # Extract: location, type (MANAGED/EXTERNAL), rows
```

**Phase 2: Migration (Week 2-3)**
```sql
-- For each table: DEEP CLONE to UC (preserves Delta history)
CREATE OR REPLACE TABLE prod.gold.orders
DEEP CLONE hive_metastore.gold.orders;

-- Verify
SELECT COUNT(*) FROM prod.gold.orders;
-- Must match: SELECT COUNT(*) FROM hive_metastore.gold.orders;
```

**Phase 3: Apply Governance (Week 3-4)**
```sql
-- Apply tags
ALTER TABLE prod.gold.orders SET TAGS ('sensitivity' = 'restricted', 'owner' = 'revenue-team');

-- Apply column masking for PII tables
ALTER TABLE prod.gold.orders ALTER COLUMN customer_email SET MASK prod.gold.mask_email;

-- Apply RBAC (replaces Hive GRANT)
GRANT SELECT ON TABLE prod.gold.orders TO `data-analysts`;
GRANT SELECT ON TABLE prod.gold.orders TO `data-engineers`;
```

**Phase 4: Cutover**
```python
# Update all notebooks/jobs to use UC table names
# Old: spark.read.table("hive_metastore.gold.orders")
# New: spark.read.table("prod.gold.orders")  OR  spark.table("orders") with UC default catalog

# After validation: deprecate Hive tables
spark.sql("ALTER TABLE hive_metastore.gold.orders SET TBLPROPERTIES ('deprecated' = 'true', 'migrated_to' = 'prod.gold.orders')")
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Unity Catalog Governance Design for 500 Tables

**Scenario:** Your company is adopting Databricks as the primary data platform. Design the Unity Catalog governance architecture for 500 tables, 15 domain teams, and GDPR compliance.

<details>
<summary>💡 Hint</summary>

Design top-down: catalog (one per environment: prod/dev/staging) → schema per team or domain → tables. For 15 teams with GDPR, the key governance decisions are: row-level security (each team sees only their data), column masking for PII (applied at the catalog level, not the application), and data lineage (Unity Catalog captures it automatically for Databricks SQL, but you need OpenLineage for external pipelines). For GDPR, map `customer_email` and other PII columns to a classification tag and attach masking functions — this ensures any new table using those columns inherits the mask automatically.

</details>

<details>
<summary>✅ Solution</summary>

**Catalog structure:**
```
prod               → Production data (all governance enforced)
  ├── bronze       → Raw ingestion (engineers only)
  ├── silver       → Cleaned (engineers + ML)
  ├── gold         → Curated SOT (analysts + engineers + ML)
  └── features     → ML feature store (data scientists)

dev                → Development (no real PII — masked copy of prod)
  ├── bronze, silver, gold → Mirror of prod structure, masked data

ml                 → ML artifacts
  ├── models       → Registered MLflow models
  ├── experiments  → MLflow experiment tracking
  └── training     → Training datasets

eu_prod            → EU-specific catalog (GDPR residency requirement)
  ├── bronze, silver, gold → EU customer data only, region-restricted
```

**Access model:**
```python
# Group hierarchy (synced from Azure AD)
GROUPS = {
    "data-admin":           ["ALL PRIVILEGES"],
    "data-engineers":       ["SELECT", "CREATE TABLE", "MODIFY"],
    "data-analysts":        ["SELECT"],
    "data-scientists":      ["SELECT", "CREATE MODEL"],
    "data-pii-approved":    ["SELECT on unmasked PII columns"],
    "eu-data-team":         ["SELECT on eu_prod catalog"],
    "analyst-<domain>":     ["SELECT on <domain>-specific tables"],
}
```

**GDPR controls:**
```sql
-- EU data: row filter ensures EU analysts only see EU data
CREATE FUNCTION prod.gold.eu_data_filter(user_region STRING) RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('data-admin') THEN TRUE
  WHEN user_region = 'EU' AND is_member('eu-data-team') THEN TRUE
  WHEN user_region != 'EU' THEN TRUE  -- Non-EU data: unrestricted
  ELSE FALSE
END;

-- Applied to all tables with EU customer data
ALTER TABLE prod.gold.customers SET ROW FILTER prod.gold.eu_data_filter ON (customer_region);
```

**Compliance monitoring:**
```python
# Daily audit job using UC system tables
daily_governance_audit()  # From real-world pattern

# Weekly report to DPO: PII access summary, anomalies, unused grants
# Monthly: full access review by domain leads
```

**Key design principle:** Unity Catalog handles the technical enforcement — column masking, row filters, audit logs. The governance program (policies, workflows, glossary) lives outside UC. UC is the enforcement layer; governance is the policy layer.

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is Unity Catalog and what problem does it solve?**
A: Unity Catalog is Databricks' unified governance layer for all data and AI assets — tables, files, ML models, and dashboards — across multiple Databricks workspaces. It solves the challenge of managing access control, lineage, and auditing consistently across a multi-workspace Lakehouse environment.

**Q: What is the three-level namespace in Unity Catalog?**
A: Unity Catalog organizes assets as `catalog.schema.table`. The catalog is the top-level container (typically representing a domain, environment, or business unit), the schema groups related tables, and the table is the leaf asset. This hierarchy maps cleanly to access control policies applied at each level.

**Q: How does Unity Catalog handle access control differently from legacy Hive metastore?**
A: Legacy Hive metastore is workspace-scoped with inconsistent permissions. Unity Catalog provides account-level governance — permissions are defined once and apply across all attached workspaces. It supports fine-grained GRANT/REVOKE SQL syntax, column masking, and row filters natively.

**Q: What are column masks and row filters in Unity Catalog?**
A: Column masks are policies that dynamically replace a column's value based on the querying user's role — for example, returning the real SSN for privileged users and a masked value for others. Row filters restrict which rows a user sees, enabling multi-tenant or regional data isolation without separate tables.

**Q: How does Unity Catalog capture data lineage?**
A: Unity Catalog automatically captures column-level lineage for queries run in Databricks SQL and notebooks — no manual instrumentation required. Lineage is viewable in the Catalog Explorer UI and queryable via the system tables (`system.access.column_lineage`), enabling impact analysis and compliance reporting.

**Q: What are Unity Catalog system tables and what can you do with them?**
A: System tables are built-in tables in the `system` catalog that expose audit logs, lineage events, billing data, and access history. You can query them with SQL to build compliance reports, detect anomalous access patterns, track compute costs by team, and audit who accessed sensitive data and when.

**Q: How do you implement data isolation between environments (dev/staging/prod) in Unity Catalog?**
A: Use separate catalogs per environment (e.g., `dev`, `staging`, `prod`) within the same metastore. Apply environment-specific permissions at the catalog level — dev engineers can write to `dev`, read-only access to `staging`, and only CI/CD service accounts can write to `prod`.

---

## 💼 Interview Tips

- Demonstrate the three-level namespace fluently — catalog.schema.table — and explain the governance implications at each level; it shows you have used Unity Catalog in practice, not just read the docs.
- Mention column masks and row filters as the modern alternative to managing separate views per user group — it shows awareness of Unity Catalog's advanced features beyond basic GRANT statements.
- Connect system tables to observability: explaining how you would use `system.access.audit` to detect unauthorized access or `system.billing` to chargeback teams by compute usage signals operational maturity.
- For senior roles, discuss the account-metastore-workspace hierarchy and when you would use multiple metastores versus multiple catalogs within one — this is a real architectural decision in large Databricks deployments.
- Bring up Delta Sharing in the context of Unity Catalog — the ability to share data externally across clouds and organizations without copying it is a differentiating governance capability.
- Avoid describing Unity Catalog as just "Databricks permissions" — frame it as a complete governance platform covering access, lineage, audit, and data sharing to show breadth of understanding.
