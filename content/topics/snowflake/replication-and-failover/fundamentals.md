---
title: "Snowflake Replication & Failover Fundamentals"
description: "Core concepts of Snowflake replication, primary/secondary accounts, and replication groups"
content_type: study_material
topic: snowflake
subtopic: replication-and-failover
layer: fundamentals
difficulty_level: junior
tags: [snowflake, replication, failover, disaster-recovery, business-continuity, multi-region]
---

# Snowflake Replication & Failover Fundamentals

## What Is Snowflake Replication?

Snowflake replication is the process of copying data, objects, and configurations from one Snowflake account (the **primary**) to one or more other Snowflake accounts (the **secondaries**). Replication enables:

- **Disaster recovery (DR)**: Keep a warm standby in a different region or cloud
- **Business continuity**: Minimize downtime if a region goes offline
- **Read scalability**: Offload read-heavy workloads to secondary accounts
- **Geo-compliance**: Store copies of data in specific regions to meet regulatory requirements

Snowflake replication is **asynchronous** by default, meaning data is copied with some lag (the replication lag / RPO window). All replication happens at the **storage layer**, leveraging Snowflake's cloud-provider object storage (S3, ADLS, GCS).

---

## Key Terminology

| Term | Definition |
|------|------------|
| **Primary account** | The source account that owns the original data; all writes happen here |
| **Secondary account** | A read-only copy of the primary; cannot accept writes until a failover |
| **Replication group** | A named group of objects (databases, shares, etc.) that replicate together |
| **Failover group** | A replication group that also supports failover/failback operations |
| **RPO** | Recovery Point Objective — maximum acceptable data loss (how old can the secondary be?) |
| **RTO** | Recovery Time Objective — maximum acceptable downtime during failover |
| **Replication lag** | The delay between a change in the primary and its appearance in the secondary |

---

## Database Replication vs. Account Replication

Snowflake has evolved its replication features over time. Understanding the difference is important for interviews.

### Database Replication (legacy)
- Replicates **individual databases** from primary to secondary
- Object: `CREATE DATABASE ... AS REPLICA OF ...`
- Limited to databases only; shares, users, roles, and other objects are **not** replicated
- Still available but considered older approach

```sql
-- Create a replica of a database in another account
CREATE DATABASE my_db_replica
  AS REPLICA OF my_org.primary_account.my_db
  DATA_RETENTION_TIME_IN_DAYS = 1;

-- Refresh the replica manually
ALTER DATABASE my_db_replica REFRESH;
```

### Replication Groups (modern)
- Replicates a **collection of objects** across accounts
- Can include databases, shares, resource monitors, warehouses, users, roles, and more
- Supports scheduled automatic refresh
- The recommended approach for most DR setups

```sql
-- Create a replication group on the primary
CREATE REPLICATION GROUP my_rep_group
  OBJECT_TYPES = DATABASES, SHARES
  ALLOWED_DATABASES = sales_db, product_db
  ALLOWED_ACCOUNTS = my_org.dr_account
  REPLICATION_SCHEDULE = '10 MINUTE';
```

### Failover Groups (DR-ready)
- A **superset** of replication groups
- Adds the ability to **fail over** (promote secondary to primary)
- Includes connection URLs for transparent client redirect
- Object types can include: databases, shares, users, roles, resource monitors, warehouses, integrations, network policies

```sql
-- Create a failover group on the primary
CREATE FAILOVER GROUP my_failover_group
  OBJECT_TYPES = DATABASES, SHARES, USERS, ROLES, WAREHOUSES
  ALLOWED_DATABASES = sales_db, product_db
  ALLOWED_INTEGRATION_TYPES = SECURITY INTEGRATIONS
  ALLOWED_ACCOUNTS = my_org.dr_account
  REPLICATION_SCHEDULE = '5 MINUTE';
```

---

## Primary and Secondary Accounts

### Primary Account Characteristics
- Fully operational: reads, writes, DDL, and DML all work
- Owns the replication group or failover group
- Pushes changes to secondary accounts on schedule

### Secondary Account Characteristics
- **Read-only**: queries are allowed, but no DML or DDL on replicated objects
- Must manually or automatically refresh from primary
- Can be promoted to primary during failover (for failover groups only)

```sql
-- On the secondary account: create a secondary replication group
CREATE REPLICATION GROUP my_rep_group
  AS REPLICA OF my_org.primary_account.my_rep_group;

-- Manually trigger a refresh on the secondary
ALTER REPLICATION GROUP my_rep_group REFRESH;
```

---

## What Objects Can Be Replicated?

When using failover groups, Snowflake can replicate:

| Object Category | Objects |
|-----------------|---------|
| **Databases** | Tables, views, stages, pipes, tasks, streams, UDFs, procedures |
| **Shares** | Inbound and outbound data shares |
| **Users & Roles** | User accounts, role hierarchy, grants |
| **Warehouses** | Virtual warehouse definitions (not compute itself) |
| **Resource monitors** | Spending limits and alerts |
| **Integrations** | Security integrations (SSO/SAML), API integrations, notification integrations |
| **Network policies** | IP allowlists/blocklists |

> **Note**: Some objects are NOT replicated by default, including external stages pointing to cloud storage (credentials may not transfer), Snowpipe status, and tasks in running state.

---

## Replication Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                 Primary Account (us-east-1)              │
│   ┌──────────────┐    ┌──────────────┐                  │
│   │  sales_db    │    │  product_db  │  ← DML/DDL here  │
│   └──────────────┘    └──────────────┘                  │
│            │                │                            │
│            └────────┬───────┘                            │
│                     │                                    │
│           FAILOVER GROUP: my_fg                          │
│           Schedule: every 5 min                          │
└─────────────────────┼───────────────────────────────────┘
                      │ async replication
                      ▼
┌─────────────────────────────────────────────────────────┐
│              Secondary Account (us-west-2)               │
│   ┌──────────────┐    ┌──────────────┐                  │
│   │  sales_db    │    │  product_db  │  ← READ ONLY     │
│   └──────────────┘    └──────────────┘                  │
│                                                          │
│           FAILOVER GROUP: my_fg (replica)                │
└─────────────────────────────────────────────────────────┘
```

---

## Replication Costs

Snowflake replication is **not free**. Costs include:

1. **Compute cost**: Snowflake uses a background virtual warehouse to perform refresh operations. You are charged for the credits used during the refresh.
2. **Storage cost**: The secondary account stores a full copy of the replicated data, billed at standard Snowflake storage rates.
3. **Data transfer cost**: Cross-region or cross-cloud data transfer incurs cloud provider egress fees.

> **Interview tip**: Candidates sometimes forget data transfer costs. Cross-cloud replication (e.g., AWS → Azure) is significantly more expensive than same-cloud cross-region replication.

---

## Monitoring Replication

```sql
-- Check replication group status and last refresh time
SHOW REPLICATION GROUPS;

-- View replication history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME = 'MY_FAILOVER_GROUP'
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

-- Check replication lag
SELECT
  REPLICATION_GROUP_NAME,
  PHASE_NAME,
  START_TIME,
  END_TIME,
  DATEDIFF('minute', END_TIME, CURRENT_TIMESTAMP()) AS lag_minutes
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME = 'MY_FAILOVER_GROUP'
  AND PHASE_NAME = 'PRIMARY_UPLOADING_METADATA'
ORDER BY START_TIME DESC
LIMIT 5;
```

---

## Common Interview Questions at This Level

**Q: What is the difference between a replication group and a failover group?**

A replication group copies objects from primary to secondary but does **not** support promoting the secondary to primary. A failover group is a superset — it replicates AND allows failover/failback. For DR purposes, you almost always want a failover group.

**Q: Is Snowflake replication synchronous or asynchronous?**

Asynchronous. Changes in the primary are batched and pushed to the secondary on a schedule (e.g., every 5 or 10 minutes). This means there is always some replication lag.

**Q: Can you write to a secondary account?**

No. Secondary accounts are read-only for replicated objects. To write, you must either fail over (promoting the secondary to primary) or write to the original primary.

**Q: What happens to replication during a Snowflake regional outage?**

If the primary account's region goes down, the replication schedule will stop. The secondary account will have data up to the last successful refresh. You can then initiate a failover to make the secondary the new primary.

---

## Summary

- Snowflake replication is asynchronous and operates at the storage layer
- **Replication groups** copy objects; **failover groups** copy objects AND support DR failover
- Secondary accounts are read-only until a failover occurs
- Costs include compute (for refresh), storage, and cross-region data transfer
- Modern best practice is to use failover groups rather than legacy database replication
