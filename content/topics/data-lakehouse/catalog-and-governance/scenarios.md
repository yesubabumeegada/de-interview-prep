---
title: "Catalog & Governance — Scenarios"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [governance, catalog, scenarios, pii, access-control]
---

# Catalog & Governance — Interview Scenarios

## Scenario 1: Implement Data Governance for a Multi-Tenant SaaS Company

**Question:** Your company is a B2B SaaS with 50 enterprise customers. You store all customer data in a single Snowflake data warehouse. Analysts from each customer company will have read access. Design the governance architecture to ensure: (1) Customer A's analysts cannot see Customer B's data, (2) PII is masked for junior analysts, (3) Full audit trail of all data access.

**Answer:**

```
Architecture:

1. Row-Level Security (tenant isolation):
   -- Create row access policy in Snowflake
   CREATE OR REPLACE ROW ACCESS POLICY tenant_isolation
   AS (tenant_id STRING) RETURNS BOOLEAN ->
     CURRENT_ROLE() LIKE CONCAT('TENANT_', tenant_id, '%')
     OR CURRENT_ROLE() = 'INTERNAL_ADMIN';
   
   -- Apply to all tenant-scoped tables
   ALTER TABLE silver.orders ADD ROW FILTER tenant_isolation ON (tenant_id);
   ALTER TABLE silver.customers ADD ROW FILTER tenant_isolation ON (tenant_id);
   ALTER TABLE silver.events ADD ROW FILTER tenant_isolation ON (tenant_id);
   
   -- Each customer gets a dedicated role:
   CREATE ROLE TENANT_ACME_ANALYST;  -- Acme Corp analysts
   CREATE ROLE TENANT_GLOBEX_ANALYST; -- Globex Corp analysts
   GRANT ROLE TENANT_ACME_ANALYST TO USER jane_doe_acme;

2. Column Masking (PII for junior analysts):
   -- Masking policy based on role
   CREATE OR REPLACE MASKING POLICY mask_pii_email
   AS (email STRING) RETURNS STRING ->
     CASE
       WHEN CURRENT_ROLE() LIKE '%_ADMIN%' THEN email
       WHEN CURRENT_ROLE() LIKE '%_SENIOR_ANALYST' THEN email
       ELSE REGEXP_REPLACE(email, '(.).*@', '$1***@')
     END;
   
   ALTER TABLE silver.customers MODIFY COLUMN email
     SET MASKING POLICY mask_pii_email;
   
   -- Senior analysts: TENANT_ACME_SENIOR_ANALYST (see raw PII)
   -- Junior analysts: TENANT_ACME_ANALYST (see masked PII)

3. Audit Trail:
   -- Snowflake: all queries auto-logged in QUERY_HISTORY
   -- Create governance view for compliance team
   CREATE OR REPLACE VIEW governance.data_access_audit AS
   SELECT
     query_id,
     user_name,
     role_name,
     query_text,
     database_name || '.' || schema_name AS schema,
     start_time,
     end_time,
     rows_produced,
     execution_status
   FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE database_name NOT IN ('SNOWFLAKE')
     AND start_time >= DATEADD('days', -90, CURRENT_TIMESTAMP());
   
   -- Alert on anomalies: one user reading more rows than usual
   CREATE ALERT unusual_data_access
     WAREHOUSE = 'ADMIN_WH'
     SCHEDULE = '15 MINUTES'
     IF (EXISTS (
       SELECT user_name, SUM(rows_produced)
       FROM governance.data_access_audit
       WHERE start_time > DATEADD('hours', -1, CURRENT_TIMESTAMP())
       GROUP BY user_name
       HAVING SUM(rows_produced) > 1000000  -- alert if > 1M rows in 1 hour
     ))
     THEN CALL notify_security_team();

4. Catalog Integration:
   -- Register tables in Atlan/DataHub with tenant ownership metadata
   -- Tag all tenant columns: tenant_id → classification=TENANT_SCOPE
   -- Analysts see only their tenant's tables in catalog (filtered by role)
```

---

## Scenario 2: Respond to a GDPR Erasure Request

**Question:** A customer submits a GDPR Article 17 (right to erasure) request for customer_id=99999. You have data in S3 + Delta Lake tables across Bronze, Silver, and Gold zones. How do you handle this?

**Answer:**

```
Process:

Step 1: Identify all tables containing customer_id=99999
  -- Query the catalog lineage: what tables reference customer_id?
  -- Or: full scan of all tables for customer_id=99999 (use catalog tags)
  
  Affected tables:
    bronze.orders (raw events — append only)
    bronze.clickstream (raw events)
    silver.orders (upserted by order_id, contains customer_id FK)
    silver.customers (customer record — direct PII)
    gold.customer_360 (pre-joined, contains PII)
    gold.customer_features (ML features — may contain PII indirectly)

Step 2: Delete from Silver and Gold (these have MERGEs, customer data exists)
  spark.sql("DELETE FROM silver.customers WHERE customer_id = 99999")
  spark.sql("DELETE FROM silver.orders WHERE customer_id = 99999")
  spark.sql("DELETE FROM gold.customer_360 WHERE customer_id = 99999")
  spark.sql("DELETE FROM gold.customer_features WHERE customer_id = 99999")
  
  -- Delta/Iceberg: this is a logical delete (soft delete in format)
  -- Physical deletion requires OPTIMIZE/rewrite_data_files

Step 3: Physical deletion via compaction
  -- Delta:   OPTIMIZE silver.customers / VACUUM (waits for retention period)
  -- Iceberg: CALL system.rewrite_data_files(table => 'silver.customers')
  --          CALL system.expire_snapshots(older_than => ...)

Step 4: Handle Bronze (append-only, raw data)
  -- Bronze "true erasure" is controversial: Bronze is raw log, not primary store
  -- Options:
  --   Option A: Pseudonymize customer_id in Bronze (hash with erasure key)
  --             When erasure key is deleted, customer is de-identified
  --   Option B: Accept: Bronze is raw log, Silver/Gold are "personal data stores"
  --             Under GDPR, erasure applies to personal data processed for purposes
  --   Option C: Re-write Bronze partition excluding customer 99999 (expensive)
  -- Common choice: Option A (cryptographic erasure via key deletion)

Step 5: Audit log
  Log the erasure request, tables affected, date completed, confirmed by whom
  Retain erasure log for 7 years (compliance audit trail)

Step 6: Confirm to customer
  "Your data has been deleted from all production systems within 30 days as required by GDPR."
  
Timeline: erasure must complete within 30 days of request (GDPR requirement)
```

---

## Scenario 3: Build a Data Catalog for a 20-Table Lakehouse

**Question:** Your team has 20 Delta tables (Bronze, Silver, Gold) and no catalog. Analysts constantly ask "what does this column mean?" and "is this table up to date?" Design and implement a basic catalog with minimal tooling.

**Answer:**

```
Minimum viable catalog using dbt + docs:

1. dbt YAML descriptions (business metadata + schema docs):

# models/silver/schema.yml
version: 2
models:
  - name: silver_orders
    description: "Cleansed order records. Updated hourly via CDC from Postgres orders table.
                  Source of truth for order data in analytics. PII: customer_id only."
    meta:
      owner: "data-engineering@company.com"
      freshness_sla: "1 hour"
      tier: "silver"
      tags: ["finance", "orders", "internal"]
    columns:
      - name: order_id
        description: "Globally unique order identifier. Matches Postgres public.orders.id."
        tests: [not_null, unique]
      - name: amount
        description: "Order total in USD, exclusive of tax. Inclusive of discounts. NULL if order cancelled."
      - name: status
        description: "Current order status. Values: pending, processing, shipped, delivered, cancelled."
        tests:
          - accepted_values:
              values: [pending, processing, shipped, delivered, cancelled]

2. Generate and serve dbt docs:
   dbt docs generate  # creates target/catalog.json
   dbt docs serve     # serves at localhost:8080
   
   # Deploy to S3 as static site for team access
   aws s3 sync target/docs s3://bucket/data-catalog/
   # CloudFront distribution → internal URL: https://catalog.internal.company.com

3. Freshness monitoring (augments catalog):
   # dbt source freshness check
   version: 2
   sources:
     - name: bronze
       description: "Raw ingest tables"
       freshness:
         warn_after: {count: 2, period: hour}
         error_after: {count: 6, period: hour}
       loaded_at_field: "_ingested_at"
       tables:
         - name: orders
         - name: clickstream

4. Data Dictionary (Google Sheet or Notion for simple start):
   - Term: "revenue" → Definition: gross_revenue includes refunds; net_revenue excludes refunds
   - Term: "active_customer" → Definition: made a purchase in the last 90 days
   - Link from dbt model descriptions to glossary terms

Result: 
  - dbt docs = searchable catalog for 20 tables
  - Schema tests = data quality documentation
  - Freshness checks = SLA monitoring
  - Total setup time: 1 sprint (2 weeks)
  - Upgrade path: import dbt catalog into Atlan/DataHub when team grows
```
