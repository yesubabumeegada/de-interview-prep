---
title: "Snowflake Security & Access Control - Scenario Questions"
topic: snowflake
subtopic: security-access-control
content_type: scenario_question
tags: [snowflake, security, rbac, scenarios, interview]
---

# Scenario Questions — Snowflake Security & Access Control

<article data-difficulty="junior">

## 🟢 Junior: Set Up Read-Only Access for a New Analyst

**Scenario:** A new data analyst joins your team and needs read access to the `analytics.curated` schema. They should be able to query all current and future tables but cannot write anything. Set this up.

<details>
<summary>✅ Solution</summary>

```sql
USE ROLE securityadmin;

-- Create the role
CREATE ROLE data_analyst_read;

USE ROLE sysadmin;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE data_analyst_read;

-- Grant database and schema navigation
GRANT USAGE ON DATABASE analytics TO ROLE data_analyst_read;
GRANT USAGE ON SCHEMA analytics.curated TO ROLE data_analyst_read;

-- Grant read on existing and future tables
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.curated TO ROLE data_analyst_read;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.curated TO ROLE data_analyst_read;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics.curated TO ROLE data_analyst_read;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA analytics.curated TO ROLE data_analyst_read;

USE ROLE securityadmin;

-- Create user and assign role
CREATE USER alice_analyst
    PASSWORD = 'TempPass@2024!'
    DEFAULT_ROLE = data_analyst_read
    DEFAULT_WAREHOUSE = analytics_wh
    MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE data_analyst_read TO USER alice_analyst;
```

**Key points:**
- `FUTURE TABLES` ensures new tables are automatically accessible — no need to re-run grants
- Must chain USAGE on database → schema → objects (all three required)
- `MUST_CHANGE_PASSWORD = TRUE` enforces password rotation on first login

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Mask PII Based on Role

**Scenario:** Your `customers` table has `email`, `phone`, and `ssn` columns. Compliance requires:
- Analysts see masked email (only domain visible) and fully masked phone/SSN
- Compliance officers see all data unmasked
- Everyone else sees nothing (fully redacted)

Design and implement the masking policies.

<details>
<summary>✅ Solution</summary>

```sql
-- Email: show domain only for analysts
CREATE MASKING POLICY mask_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('COMPLIANCE_OFFICER', 'DATA_STEWARD') THEN val
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST_READ', 'DATA_ANALYST_WRITE') 
         THEN CONCAT('****@', SPLIT_PART(val, '@', 2))
    ELSE '***REDACTED***'
  END;

-- Phone: fully masked for analysts
CREATE MASKING POLICY mask_phone AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('COMPLIANCE_OFFICER', 'DATA_STEWARD') THEN val
    ELSE 'XXX-XXX-XXXX'
  END;

-- SSN: fully masked for everyone except compliance
CREATE MASKING POLICY mask_ssn AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('COMPLIANCE_OFFICER') THEN val
    ELSE 'XXX-XX-XXXX'
  END;

-- Apply policies to columns
ALTER TABLE customers MODIFY COLUMN email SET MASKING POLICY mask_email;
ALTER TABLE customers MODIFY COLUMN phone SET MASKING POLICY mask_phone;
ALTER TABLE customers MODIFY COLUMN ssn   SET MASKING POLICY mask_ssn;

-- Verify
USE ROLE data_analyst_read;
SELECT email, phone, ssn FROM customers LIMIT 3;
-- email: ****@example.com | phone: XXX-XXX-XXXX | ssn: XXX-XX-XXXX

USE ROLE compliance_officer;
SELECT email, phone, ssn FROM customers LIMIT 3;
-- email: alice@example.com | phone: 555-1234 | ssn: 123-45-6789
```

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Row-Level Access for Multi-Tenant SaaS Analytics

**Scenario:** You're building a shared analytics platform for a multi-tenant SaaS product. All tenants' data is in one Snowflake table (`fact_events`), but each tenant must only see their own rows. You have 500+ tenants. Design a scalable row access solution.

<details>
<summary>✅ Solution</summary>

**Approach: Session context + row access policy (avoids creating 500+ roles)**

```sql
-- 1. Application login flow sets session context with tenant_id
-- (Done in application layer on connection open)
ALTER SESSION SET SESSION_CONTEXT = '{"tenant_id": "acme_corp"}';

-- 2. Create row access policy reading session context
CREATE ROW ACCESS POLICY tenant_isolation AS (row_tenant_id VARCHAR) RETURNS BOOLEAN ->
    -- Match tenant ID from session context
    row_tenant_id = CURRENT_SESSION_CONTEXT('tenant_id')
    -- Internal analysts see everything
    OR CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'SUPPORT_ENGINEER_L3', 'COMPLIANCE_AUDITOR');

-- 3. Apply to the shared table
ALTER TABLE fact_events ADD ROW ACCESS POLICY tenant_isolation ON (tenant_id);

-- 4. Verify: tenant A cannot see tenant B's data
ALTER SESSION SET SESSION_CONTEXT = '{"tenant_id": "acme_corp"}';
SELECT COUNT(*) FROM fact_events;  -- Only ACME's rows

ALTER SESSION SET SESSION_CONTEXT = '{"tenant_id": "other_corp"}';
SELECT COUNT(*) FROM fact_events;  -- Only OTHER's rows

-- 5. For extra security: audit cross-tenant access attempts
-- Snowflake logs all queries — any admin role query on fact_events is logged in ACCESS_HISTORY
```

**Why not one role per tenant?**
- 500+ roles becomes unmanageable
- Session context scales to millions of tenants with zero role changes
- The row access policy is a single policy applied once

**Trade-off:** The application must reliably set `SESSION_CONTEXT` before executing queries. Use a connection pool wrapper that enforces this.

</details>
</article>
