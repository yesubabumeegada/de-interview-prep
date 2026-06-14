---
title: "Snowflake Security & Access Control - Fundamentals"
topic: snowflake
subtopic: security-access-control
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, security, rbac, roles, privileges, access-control]
---

# Snowflake Security & Access Control — Fundamentals

## 🎯 Analogy

Think of Snowflake's security model like a company org chart. **Users** are employees. **Roles** are job titles (Manager, Analyst, Intern). **Privileges** are key-card access to rooms. You give the key card to the job title, not to individual people — when someone gets promoted, they inherit all the new title's access automatically.

---

## Role-Based Access Control (RBAC)

Snowflake uses **RBAC** — you never grant privileges directly to users. Instead:

1. Grant privileges → **Roles**
2. Grant roles → **Users** (or other roles)

```
Privilege (SELECT on table)
       ↓
   Role (ANALYST_ROLE)
       ↓
   User (alice@company.com)
```

This means when you add a new analyst, you just assign the `ANALYST_ROLE` — they instantly get all the right access. No per-user privilege management.

---

## System-Defined Roles

Snowflake ships with five built-in roles:

| Role | What It Can Do |
|------|---------------|
| `ACCOUNTADMIN` | Everything — manage account settings, billing, users |
| `SYSADMIN` | Create and manage databases, warehouses, schemas |
| `SECURITYADMIN` | Create and manage users and roles |
| `USERADMIN` | Create users and roles only (subset of SECURITYADMIN) |
| `PUBLIC` | Auto-granted to every user — minimal access |

> **Best practice:** Never use `ACCOUNTADMIN` for day-to-day work. Create custom roles and use `SYSADMIN` for object creation.

```
ACCOUNTADMIN
    ├── SECURITYADMIN
    │       └── USERADMIN
    └── SYSADMIN
            └── (custom roles you create)
                    └── PUBLIC
```

---

## Creating and Granting Roles

```sql
-- 1. Create roles
CREATE ROLE analyst_role;
CREATE ROLE etl_role;
CREATE ROLE admin_role;

-- 2. Grant privileges to roles
GRANT USAGE ON DATABASE analytics TO ROLE analyst_role;
GRANT USAGE ON SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;

-- 3. Grant warehouse usage
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE analyst_role;

-- 4. Create user and assign role
CREATE USER alice
    PASSWORD = 'TempPassword123!'
    DEFAULT_ROLE = analyst_role
    DEFAULT_WAREHOUSE = analytics_wh
    MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE analyst_role TO USER alice;

-- 5. Role hierarchy — grant a role to another role
GRANT ROLE analyst_role TO ROLE admin_role;  -- admin_role inherits analyst_role's privileges
```

---

## Key Privilege Types

| Privilege | Applied To | What It Allows |
|-----------|-----------|----------------|
| `USAGE` | Database, Schema, Warehouse | Navigate/connect to the object |
| `SELECT` | Table, View | Read data |
| `INSERT`, `UPDATE`, `DELETE` | Table | Write data |
| `CREATE TABLE` | Schema | Create new tables |
| `OWNERSHIP` | Any object | Full control, can grant/revoke others |
| `MONITOR` | Warehouse | See query history and usage |
| `OPERATE` | Warehouse | Start/stop/resize |

> **USAGE is a prerequisite.** To query a table, a user needs USAGE on the database AND schema AND SELECT on the table. Missing any one of these blocks access.

---

## Granting Access — Common Patterns

```sql
-- Read-only analyst: database + schema + tables
GRANT USAGE ON DATABASE analytics TO ROLE analyst_role;
GRANT USAGE ON SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.curated TO ROLE analyst_role;  -- covers new tables

-- ETL writer role: can insert/update but not delete
GRANT USAGE ON DATABASE analytics TO ROLE etl_role;
GRANT USAGE ON SCHEMA analytics.staging TO ROLE etl_role;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA analytics.staging TO ROLE etl_role;
GRANT FUTURE INSERT, UPDATE ON TABLES IN SCHEMA analytics.staging TO ROLE etl_role;

-- Check what privileges a role has
SHOW GRANTS TO ROLE analyst_role;

-- Check what a user has access to
SHOW GRANTS TO USER alice;
```

---

## Authentication Options

| Method | Use Case |
|--------|---------|
| Username + Password | Basic; use MFA in production |
| Multi-Factor Auth (MFA) | Add Duo/TOTP layer on top of password |
| SSO / SAML 2.0 | Enterprise identity providers (Okta, Azure AD, Google) |
| Key Pair Authentication | Service accounts, CI/CD pipelines (no password) |
| OAuth | Third-party tool integration (Tableau, dbt Cloud) |
| Snowflake-managed credentials | Programmatic access via private link |

```sql
-- Enforce MFA for a user
ALTER USER alice SET MINS_TO_BYPASS_MFA = 0;  -- MFA required every session

-- Key pair auth — set RSA public key on user (private key stays with client)
ALTER USER svc_account SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhk...';
```

---

## Network Policies

Control which IP addresses can connect:

```sql
-- Create a policy restricting access to office + VPN IPs
CREATE NETWORK POLICY corp_network_policy
    ALLOWED_IP_LIST = ('203.0.113.0/24', '198.51.100.10')
    BLOCKED_IP_LIST = ('198.51.100.99');

-- Apply to entire account
ALTER ACCOUNT SET NETWORK_POLICY = corp_network_policy;

-- Apply to specific user only
ALTER USER alice SET NETWORK_POLICY = corp_network_policy;
```

---

## Try It Yourself

```sql
-- Full setup for a new analyst
USE ROLE securityadmin;

CREATE ROLE data_analyst;
GRANT ROLE data_analyst TO ROLE sysadmin;  -- sysadmin can also manage this role

USE ROLE sysadmin;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE data_analyst;
GRANT USAGE ON DATABASE analytics TO ROLE data_analyst;
GRANT USAGE ON ALL SCHEMAS IN DATABASE analytics TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN DATABASE analytics TO ROLE data_analyst;

USE ROLE securityadmin;
CREATE USER new_analyst
    PASSWORD = 'Temp@12345'
    DEFAULT_ROLE = data_analyst
    DEFAULT_WAREHOUSE = compute_wh
    MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE data_analyst TO USER new_analyst;
```

---

## Interview Tips

> **Tip 1:** "How does Snowflake handle permissions?" — "RBAC: privileges are granted to roles, roles are granted to users. You never grant directly to users. This makes access management scalable — onboard a new analyst by assigning an existing role."

> **Tip 2:** "What's the difference between SECURITYADMIN and SYSADMIN?" — "SECURITYADMIN manages users and roles. SYSADMIN manages database objects (databases, schemas, warehouses). They're kept separate to enforce least-privilege — your DBA who creates tables shouldn't also control who has access."

> **Tip 3:** "What's FUTURE GRANTS?" — "Instead of re-running grants every time a new table is created, GRANT SELECT ON FUTURE TABLES automatically applies the privilege to any new table added to the schema. Essential for dynamic schemas."
