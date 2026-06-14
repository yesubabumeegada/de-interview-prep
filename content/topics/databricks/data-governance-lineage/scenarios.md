---
title: "Data Governance & Lineage - Scenario Questions"
topic: databricks
subtopic: data-governance-lineage
content_type: scenario_question
tags: [databricks, unity-catalog, governance, lineage, scenarios, interview]
---

# Scenario Questions — Data Governance & Lineage

<article data-difficulty="junior">

## 🟢 Junior: Set Up Access Control for a Multi-Team Project

**Scenario:** You're setting up a new Databricks project with three groups: `data-engineers` (need to write data), `analysts` (need to read all tables), and `data-scientists` (need to read feature tables only). Set up Unity Catalog permissions correctly.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create the catalog and schema structure
CREATE CATALOG IF NOT EXISTS prod;

CREATE SCHEMA IF NOT EXISTS prod.raw;        -- raw ingested data
CREATE SCHEMA IF NOT EXISTS prod.analytics;  -- cleaned/transformed data
CREATE SCHEMA IF NOT EXISTS prod.features;   -- ML feature tables

-- Step 2: Data engineers — full control over all schemas
GRANT USE CATALOG ON CATALOG prod TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.raw TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.analytics TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.features TO `data-engineers`;

-- Step 3: Analysts — read access to analytics tables
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA ON SCHEMA prod.analytics TO `analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA prod.analytics TO `analysts`;

-- Future tables automatically included:
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod.analytics TO `analysts`;

-- Step 4: Data scientists — read access to features only
GRANT USE CATALOG ON CATALOG prod TO `data-scientists`;
GRANT USE SCHEMA ON SCHEMA prod.features TO `data-scientists`;
GRANT SELECT ON ALL TABLES IN SCHEMA prod.features TO `data-scientists`;
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod.features TO `data-scientists`;

-- Step 5: Verify permissions
SHOW GRANTS ON SCHEMA prod.analytics;
SHOW GRANTS ON SCHEMA prod.features;
```

**Key points:**
- `USE CATALOG` and `USE SCHEMA` are required before any table-level permissions work — they're the "namespace unlock" grants
- `FUTURE TABLES` ensures new tables are automatically accessible without re-granting
- Data engineers have `ALL PRIVILEGES` which includes CREATE, SELECT, MODIFY, and DROP
- Least privilege: analysts can't see raw data; data scientists can't see analytics tables

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Investigate a Data Quality Incident Using Lineage

**Scenario:** The Finance team reports that the `executive_dashboard` is showing incorrect revenue numbers since this morning. You need to trace the bad data to its source using lineage. The `executive_dashboard` reads from `prod.reporting.monthly_revenue`. Describe your investigation approach and write the queries.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Find all upstream tables feeding monthly_revenue
WITH RECURSIVE upstream AS (
    -- Direct parents
    SELECT
        source_table_full_name AS table_name,
        target_table_full_name AS child_table,
        1 AS depth
    FROM system.lineage.table_lineage
    WHERE target_table_full_name = 'prod.reporting.monthly_revenue'

    UNION ALL

    -- Parents of parents (up to depth 5)
    SELECT
        tl.source_table_full_name,
        tl.target_table_full_name,
        u.depth + 1
    FROM system.lineage.table_lineage tl
    JOIN upstream u ON tl.target_table_full_name = u.table_name
    WHERE u.depth < 5
)
SELECT DISTINCT table_name, MIN(depth) AS hops_from_target
FROM upstream
GROUP BY 1
ORDER BY 2;

-- Result shows the full dependency chain:
-- prod.sales.orders (1 hop)
-- prod.sales.transactions (2 hops)
-- prod.raw.payment_events (3 hops)
-- prod.raw.order_events (3 hops)

-- Step 2: Find who modified upstream tables today
SELECT
    request_params.full_name_arg AS table_name,
    user_identity.email AS user,
    action_name,
    event_time
FROM system.access.audit
WHERE action_name IN ('createTable', 'overwriteTable', 'writeTable', 'deleteTable')
  AND event_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP())
  AND request_params.full_name_arg IN (
    'prod.sales.orders',
    'prod.sales.transactions',
    'prod.raw.payment_events',
    'prod.raw.order_events'
  )
ORDER BY event_time DESC;

-- Step 3: Check which pipeline last wrote to each suspicious table
SELECT
    entity_name AS pipeline_name,
    last_modified_at,
    user_identity.email AS run_by
FROM system.lineage.table_lineage tl
JOIN system.access.audit a
    ON tl.source_entity_name = a.request_params.full_name_arg
WHERE tl.target_table_full_name = 'prod.reporting.monthly_revenue'
ORDER BY last_modified_at DESC;

-- Step 4: Check Delta history of the suspect table
DESCRIBE HISTORY prod.sales.orders;
-- Look for: unexpected OVERWRITE or large DELETE operations today

-- Step 5: Time travel to compare today vs yesterday
SELECT
    SUM(amount) AS total_revenue_today
FROM prod.sales.orders

UNION ALL

SELECT
    SUM(amount) AS total_revenue_yesterday
FROM prod.sales.orders TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 1 DAY);
-- If revenue_today is dramatically different, orders table is the source
```

**Investigation checklist:**
1. Lineage graph → find all upstream tables
2. Audit log → who wrote to upstream tables today
3. Delta DESCRIBE HISTORY → check for accidental OVERWRITE
4. Time travel comparison → quantify when/how much data changed
5. If needed: restore with `RESTORE TABLE ... TO TIMESTAMP AS OF ...`

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Data Governance Framework for a Regulated Industry

**Scenario:** You're joining a healthcare analytics company as a senior data engineer. They have patient data in Databricks but no governance framework. Regulators require HIPAA compliance: PHI must be masked for most users, all PHI access must be auditable, and patient data must be deleted within 30 days of a deletion request. Design the full governance architecture.

<details>
<summary>✅ Solution</summary>

**Architecture overview:**

```
Data Classification Layer:
  All PHI tables tagged: 'hipaa_phi' = 'true', 'retention_days' = '2555' (7 years)
  Column sensitivity: SSN='high', DOB='medium', MRN='high', diagnosis='high'

Access Control Layer:
  Roles: phi-stewards, authorized-researchers, de-identified-analysts, data-engineers
  Column masks: PHI columns → hash/truncate for non-stewards
  Row filters: researchers see only their approved study cohorts

Audit Layer:
  system.access.audit → all PHI table access logged
  Weekly compliance report to HIPAA officer
  Automated alerts on bulk access (>10K rows)

Deletion Layer:
  Erasure requests → automated deletion → vacuum queue → audit log
```

**Implementation:**

```sql
-- 1. Tag all PHI tables (one-time, run on all existing tables)
ALTER TABLE prod.clinical.patient_records
    SET TAGS ('hipaa_phi' = 'true',
              'data_classification' = 'phi_high',
              'retention_years' = '7',
              'owner' = 'clinical-data-team');

-- 2. Column masking for PHI
CREATE OR REPLACE FUNCTION prod.security.mask_phi_high(value STRING)
RETURNS STRING
LANGUAGE SQL
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('phi-stewards')
      OR IS_ACCOUNT_GROUP_MEMBER('hipaa-authorized-researchers')
    THEN value
    ELSE '***REDACTED***'
END;

CREATE OR REPLACE FUNCTION prod.security.mask_dob(dob DATE)
RETURNS DATE
LANGUAGE SQL
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('phi-stewards') THEN dob
    ELSE DATE_TRUNC('year', dob)  -- year only for de-identified analytics
END;

ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN ssn SET MASK prod.security.mask_phi_high;
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN mrn SET MASK prod.security.mask_phi_high;
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN date_of_birth SET MASK prod.security.mask_dob;

-- 3. Row filter: researchers see only their approved study patients
CREATE OR REPLACE FUNCTION prod.security.study_patient_filter(patient_id STRING)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN (
    IS_ACCOUNT_GROUP_MEMBER('phi-stewards')
    OR patient_id IN (
        SELECT patient_id
        FROM prod.compliance.study_patient_enrollments spe
        JOIN prod.compliance.researcher_approvals ra
            ON spe.study_id = ra.study_id
        WHERE ra.researcher_email = CURRENT_USER()
          AND ra.approval_status = 'active'
          AND ra.expiry_date > CURRENT_DATE()
    )
);

ALTER TABLE prod.clinical.patient_records
    SET ROW FILTER prod.security.study_patient_filter ON (patient_id);

-- 4. Automated compliance report (daily job)
SELECT
    DATE_TRUNC('day', event_time) AS access_date,
    user_identity.email AS user,
    COUNT(*) AS query_count,
    MAX(response.numRows) AS max_rows_returned
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'prod.clinical.%'
  AND event_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
HAVING MAX(response.numRows) > 10000   -- alert on bulk PHI access
ORDER BY max_rows_returned DESC;
```

**Erasure workflow (30-day SLA):**

```python
# Triggered when deletion request arrives (via API or workflow)
# 1. Delete from all PHI-tagged tables where patient_id matches
# 2. Log to immutable audit table (cannot delete this log — regulatory requirement)
# 3. Schedule VACUUM after standard 7-day Delta retention window
# 4. Send confirmation to patient within 30 days
# (Same pattern as the GDPR erasure code in the real-world examples)
```

**Governance gaps this covers:**
- Access control: ✅ Column masks + row filters prevent unauthorized PHI access
- Audit trail: ✅ `system.access.audit` + automated reports
- Erasure: ✅ Automated deletion pipeline with audit proof
- Data classification: ✅ Tags on all PHI tables, queryable for compliance
- Retention: ✅ Tag-driven retention enforcement job

**What this doesn't cover (would need additional work):**
- Network security (private endpoints, IP allowlisting)
- Encryption at rest/in transit (handled by Databricks platform)
- Workforce training (HIPAA requires documented training)
- Business associate agreements with Databricks (contractual, not technical)

</details>
</article>
