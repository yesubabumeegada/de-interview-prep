---
title: "Unity Catalog Governance — Scenarios"
topic: data-governance
subtopic: unity-catalog-governance
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [unity-catalog, databricks, interview, scenarios, governance]
---

# Unity Catalog Governance — Interview Scenarios

## Scenario 1 (Junior): Setting Up Access for a New Analyst

**Question:** A new data analyst joins. They need read access to gold tables in Databricks but must NOT see raw customer emails (PII). How do you set this up in Unity Catalog?

**Answer:**

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

---

## Scenario 2 (Mid-level): Migrating from Hive Metastore to Unity Catalog

**Question:** Your team has 50 Delta tables in the Hive Metastore. You need to migrate them to Unity Catalog while maintaining all access and adding governance. How do you approach this?

**Answer:**

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

---

## Scenario 3 (Senior): Unity Catalog Governance Design for 500 Tables

**Question:** Your company is adopting Databricks as the primary data platform. Design the Unity Catalog governance architecture for 500 tables, 15 domain teams, and GDPR compliance.

**Answer:**

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
