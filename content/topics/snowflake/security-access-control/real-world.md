---
title: "Snowflake Security & Access Control - Real-World Examples"
topic: snowflake
subtopic: security-access-control
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, security, production, compliance, pii, governance]
---

# Snowflake Security & Access Control — Real-World Production Examples

## Production Pattern: Zero-Trust Data Access Model

A fintech company serving 50+ internal teams implements zero-trust:

```sql
-- No PUBLIC access to any database
REVOKE ALL PRIVILEGES ON DATABASE prod_analytics FROM ROLE PUBLIC;

-- Managed access on all sensitive schemas
CREATE SCHEMA prod_analytics.pii_data WITH MANAGED ACCESS;
CREATE SCHEMA prod_analytics.financial WITH MANAGED ACCESS;

-- Dedicated roles per data domain
CREATE ROLE pii_read;        -- Customer PII (masked by default)
CREATE ROLE pii_admin;       -- Can see unmasked PII (very few users)
CREATE ROLE financial_read;
CREATE ROLE sre_monitoring;  -- Query history / warehouse monitoring only

-- Masking policy on PII by default
CREATE MASKING POLICY mask_pii_email AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('PII_ADMIN', 'COMPLIANCE_AUDITOR') THEN val
       ELSE CONCAT(LEFT(val, 2), '****@****.***') END;

-- Apply to all PII columns (automated via Terraform)
ALTER TABLE customers MODIFY COLUMN email SET MASKING POLICY mask_pii_email;
ALTER TABLE customers MODIFY COLUMN phone SET MASKING POLICY mask_pii_phone;
ALTER TABLE customers MODIFY COLUMN ssn   SET MASKING POLICY mask_pii_ssn;
```

**Result:** Analysts can join on customer data without ever seeing raw PII. Compliance auditors have temporary elevated access with full session logging.

---

## Production Pattern: Service Account Security for dbt Cloud

```sql
-- dbt service account: minimal privilege, no console login
CREATE USER dbt_prod_sa
    DEFAULT_ROLE = dbt_prod_role
    DEFAULT_WAREHOUSE = dbt_wh
    DISABLED = FALSE;

-- Disable password auth — key-pair only
ALTER USER dbt_prod_sa SET RSA_PUBLIC_KEY = '<public_key_from_vault>';

-- Role has only what dbt needs
CREATE ROLE dbt_prod_role;
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_prod_role;
GRANT USAGE ON DATABASE analytics TO ROLE dbt_prod_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE analytics TO ROLE dbt_prod_role;
GRANT CREATE TABLE ON SCHEMA analytics.dbt_prod TO ROLE dbt_prod_role;
GRANT CREATE VIEW  ON SCHEMA analytics.dbt_prod TO ROLE dbt_prod_role;
GRANT SELECT ON ALL TABLES IN DATABASE analytics TO ROLE dbt_prod_role;

-- Network policy: dbt Cloud runs from fixed IP ranges
CREATE NETWORK POLICY dbt_cloud_policy
    ALLOWED_IP_LIST = ('34.98.127.0/24', '35.227.135.0/24');  -- dbt Cloud IPs
ALTER USER dbt_prod_sa SET NETWORK_POLICY = dbt_cloud_policy;
```

---

## Production Incident: Over-Privileged Analyst Leaked Revenue Data

**What happened:** A junior analyst had `SYSADMIN` role (granted "temporarily" for a project). They ran a query that returned unreleased revenue forecasts and shared it in Slack before earnings.

**Root cause analysis:**
1. `SYSADMIN` had access to ALL databases including `finance_restricted`
2. No row access policy on the forecasts table
3. No audit alert for queries on sensitive tables
4. "Temporary" access was never revoked

**Remediation applied:**
```sql
-- 1. Revoke SYSADMIN immediately
REVOKE ROLE sysadmin FROM USER analyst_bob;

-- 2. Apply row access policy to finance tables
CREATE ROW ACCESS POLICY finance_restricted_policy AS (dept VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('FINANCE_TEAM', 'CFO_OFFICE', 'COMPLIANCE_AUDITOR');

ALTER TABLE finance_restricted.forecasts
    ADD ROW ACCESS POLICY finance_restricted_policy ON (department);

-- 3. Set up alert for suspicious access
-- (External: Snowflake → CloudWatch/Datadog alert on ACCESS_HISTORY for finance_restricted)

-- 4. Quarterly privilege review query
SELECT grantee_name, privilege, object_name, created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE created_on < DATEADD('month', -3, CURRENT_TIMESTAMP())
ORDER BY created_on;
```

**Process change:** All elevated access requests go through Jira + 30-day auto-expiry enforced via automation.

---

## Production Pattern: Multi-Account Environment Isolation

```
snowflake-prod-account        snowflake-dev-account         snowflake-sandbox-account
├── analytics_db              ├── analytics_dev_db          ├── analytics_sandbox_db
├── Production data           ├── Anonymized prod sample    ├── Synthetic data only
├── Network: private link     ├── Network: office IPs only  ├── No restrictions
├── MFA required              ├── SSO required              ├── Password OK
└── All PII fully live        └── PII masked at all times   └── No real PII
```

```sql
-- Dev account: global masking on ALL PII, no exceptions
-- Enforced by policy applied at account level
ALTER ACCOUNT SET REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_CREATION = TRUE;

-- Automated data sampling from prod to dev (masked)
-- dbt + Snowflake: pull 1% sample, apply masking during copy
INSERT INTO dev_analytics.staging.customers
SELECT
    customer_id,
    SHA2(email, 256)                        AS email,      -- hashed
    REGEXP_REPLACE(phone, '\\d', 'X')       AS phone,      -- masked
    FLOOR(DATEDIFF('year', dob, CURRENT_DATE()) / 10) * 10 AS age_group,  -- bucketed
    region
FROM PROD_LINK.analytics.customers
SAMPLE (1 PERCENT);
```

---

## Production Monitoring: Access Anomaly Detection

Weekly query run by the security team:

```sql
-- Users who accessed sensitive tables for the first time this week
SELECT a.user_name, a.query_start_time, a.query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a,
LATERAL FLATTEN(a.base_objects_accessed) f
WHERE f.value:objectName::STRING LIKE 'ANALYTICS.PII_DATA.%'
  AND a.query_start_time >= DATEADD('week', -1, CURRENT_TIMESTAMP())
  AND a.user_name NOT IN (
      SELECT DISTINCT user_name
      FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY, LATERAL FLATTEN(base_objects_accessed)
      WHERE value:objectName::STRING LIKE 'ANALYTICS.PII_DATA.%'
        AND query_start_time < DATEADD('week', -1, CURRENT_TIMESTAMP())
  )
ORDER BY a.query_start_time;

-- Privilege changes in last 24h (alert on unexpected grants)
SELECT grantee_name, privilege, object_name, granted_by, created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE created_on >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY created_on DESC;
```

---

## Terraform-Managed Snowflake Security (IaC)

Production teams manage Snowflake RBAC via Terraform to prevent drift:

```hcl
resource "snowflake_role" "analyst_read" {
  name = "ANALYTICS_READ"
}

resource "snowflake_database_grant" "analytics_usage" {
  database_name = snowflake_database.analytics.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_read.name]
}

resource "snowflake_schema_grant" "curated_usage" {
  database_name = snowflake_database.analytics.name
  schema_name   = "CURATED"
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_read.name]
}

resource "snowflake_table_grant" "curated_select" {
  database_name = snowflake_database.analytics.name
  schema_name   = "CURATED"
  privilege     = "SELECT"
  roles         = [snowflake_role.analyst_read.name]
  on_future     = true  # FUTURE GRANTS
}
```

**Benefits:** Code review for access changes, audit trail in git, automatic cleanup when roles are removed from Terraform state.
