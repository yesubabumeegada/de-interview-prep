---
title: "Snowflake Replication Configuration and Failover Groups"
description: "Setting up replication groups, failover groups, RPO/RTO planning, and replication cost management"
content_type: study_material
topic: snowflake
subtopic: replication-and-failover
layer: intermediate
difficulty_level: mid-level
tags: [snowflake, replication-groups, failover-groups, RPO, RTO, BCDR, replication-schedule, cost]
---

# Snowflake Replication Configuration and Failover Groups

## Setting Up a Complete Replication Topology

A production replication setup involves steps on both the primary and secondary accounts, plus org-level configuration.

### Step 1: Enable Replication at the Organization Level

Before creating any replication or failover groups, an Org Admin must enable replication for the accounts involved.

```sql
-- Run as ORGADMIN on any account in the org
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER(
  'my_org.dr_account',
  'ENABLE_ACCOUNT_DATABASE_REPLICATION',
  'true'
);
```

### Step 2: Create the Failover Group on the Primary

```sql
-- Run as SYSADMIN or ACCOUNTADMIN on the PRIMARY account
CREATE FAILOVER GROUP prod_failover_group
  OBJECT_TYPES =
    DATABASES,
    SHARES,
    USERS,
    ROLES,
    WAREHOUSES,
    RESOURCE MONITORS,
    INTEGRATIONS,
    NETWORK POLICIES
  ALLOWED_DATABASES = analytics_db, raw_db, transformed_db
  ALLOWED_INTEGRATION_TYPES =
    SECURITY INTEGRATIONS,
    API INTEGRATIONS,
    NOTIFICATION INTEGRATIONS
  ALLOWED_ACCOUNTS = my_org.dr_west_account
  REPLICATION_SCHEDULE = '5 MINUTE';
```

### Step 3: Create the Secondary on the DR Account

```sql
-- Run as SYSADMIN or ACCOUNTADMIN on the SECONDARY (DR) account
CREATE FAILOVER GROUP prod_failover_group
  AS REPLICA OF my_org.primary_east_account.prod_failover_group;
```

### Step 4: Trigger the Initial Refresh

The first refresh copies all data. It may take significant time for large datasets.

```sql
-- On the secondary account
ALTER FAILOVER GROUP prod_failover_group REFRESH;

-- Monitor progress
SELECT *
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('PROD_FAILOVER_GROUP'));
```

---

## Replication Schedules

The `REPLICATION_SCHEDULE` parameter controls how often the secondary is refreshed.

### CRON Expression Format

```sql
-- Every 10 minutes
REPLICATION_SCHEDULE = '10 MINUTE'

-- Every hour at minute 0 (using CRON)
REPLICATION_SCHEDULE = 'USING CRON 0 * * * * UTC'

-- Every day at 2 AM UTC
REPLICATION_SCHEDULE = 'USING CRON 0 2 * * * UTC'

-- Every 15 minutes during business hours (Mon-Fri, 8am-6pm UTC)
REPLICATION_SCHEDULE = 'USING CRON */15 8-18 * * 1-5 UTC'
```

### Modifying the Schedule

```sql
-- Update the schedule on the primary
ALTER FAILOVER GROUP prod_failover_group
  SET REPLICATION_SCHEDULE = '15 MINUTE';

-- Disable automatic scheduling (manual refreshes only)
ALTER FAILOVER GROUP prod_failover_group
  UNSET REPLICATION_SCHEDULE;
```

### Scheduling Considerations

| Factor | Recommendation |
|--------|----------------|
| RPO requirement of < 5 min | Use 5-minute schedule; evaluate continuous replication alternatives |
| Cost-sensitive environment | Longer interval (30–60 min) to reduce compute and transfer costs |
| Large data volumes | Test refresh duration; ensure schedule interval > typical refresh duration |
| Compliance (SOC 2, HIPAA) | Document RPO/RTO; schedule must guarantee RPO commitment |

> **Warning**: If your refresh takes 8 minutes and your schedule is every 5 minutes, refreshes will queue up. Monitor refresh durations and set schedules accordingly.

---

## Understanding RPO and RTO in Snowflake

### Recovery Point Objective (RPO)

RPO is the maximum amount of data loss acceptable after an incident.

- In Snowflake, RPO ≈ your replication schedule interval + any lag in that cycle
- Example: 5-minute schedule with a 3-minute average refresh = ~8 minutes RPO worst case
- For stricter RPO, use more frequent schedules but accept higher cost

```sql
-- Query to assess actual RPO achieved
SELECT
  REPLICATION_GROUP_NAME,
  REFRESH_END_TIME,
  DATEDIFF('second', REFRESH_END_TIME, CURRENT_TIMESTAMP()) AS seconds_since_last_refresh,
  DATEDIFF('second', REFRESH_END_TIME, CURRENT_TIMESTAMP()) / 60.0 AS minutes_rpo
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME = 'PROD_FAILOVER_GROUP'
  AND REFRESH_END_TIME IS NOT NULL
ORDER BY REFRESH_END_TIME DESC
LIMIT 1;
```

### Recovery Time Objective (RTO)

RTO is the maximum acceptable downtime — from incident detection to full restoration of service.

Snowflake failover RTO components:
1. **Detection time**: How long until the team knows there is an outage (~minutes for monitoring alerts)
2. **Decision time**: Approval process to initiate failover (~minutes to hours depending on runbook)
3. **Failover execution**: The actual `ALTER FAILOVER GROUP ... PRIMARY` command (~seconds to minutes)
4. **DNS/connection redirect**: Client reconnection after the failover URL updates (~1–5 minutes)
5. **Validation**: Smoke tests to confirm secondary is working correctly (~minutes)

Total Snowflake RTO: typically **15 minutes to 2 hours** depending on the organization's runbook maturity.

---

## Connection URLs and Client Redirect

Snowflake provides **connection URLs** that automatically redirect clients to the current primary account during a failover. This is a critical piece of the DR design.

### Types of URLs

| URL Type | Format | Behavior |
|----------|--------|----------|
| Account URL | `<account>.snowflakecomputing.com` | Points to a specific account; does NOT redirect |
| Connection URL | `<org>-<connection>.snowflakecomputing.com` | Points to the active primary; redirects on failover |

### Setting Up Connection URLs

```sql
-- Create a connection on the primary account
CREATE CONNECTION my_prod_connection;

-- Enable it as the primary connection for the failover group
ALTER FAILOVER GROUP prod_failover_group
  ADD CONNECTION my_prod_connection;

-- On the secondary, the connection is created automatically when FG is replicated
-- View connections
SHOW CONNECTIONS;
```

### Updating Application Connection Strings

Applications should use the connection URL, not the account URL:

```
# Before (bad for DR):
SNOWFLAKE_ACCOUNT=mycompany-prod.us-east-1

# After (DR-safe connection URL):
SNOWFLAKE_ACCOUNT=myorg-my_prod_connection.snowflakecomputing.com
```

After a failover, Snowflake updates the DNS record behind the connection URL to point to the new primary. Applications reconnect automatically within the DNS TTL window (typically 60 seconds).

---

## Replication Cost Deep Dive

### Cost Components

```
Total Replication Cost = Compute Cost + Storage Cost + Data Transfer Cost
```

**1. Compute Cost**
- Snowflake uses a background serverless compute for replication
- Billed per credit consumed during refresh operations
- Typically 1.5–3x credits compared to a manual COPY INTO of the same data volume
- Billed at the serverless credit rate (usually higher than warehouse credits)

**2. Storage Cost**
- Full copy of all replicated objects stored in the secondary account
- Billed at standard Snowflake storage rates
- Time Travel and Fail Safe on secondary also consume storage

**3. Data Transfer Cost**
- Same cloud, same region: typically free or minimal
- Same cloud, cross-region: ~$0.01–$0.08 per GB (varies by cloud)
- Cross-cloud (AWS ↔ Azure ↔ GCP): ~$0.09–$0.19 per GB

### Estimating Replication Costs

```sql
-- Query replication cost from Account Usage
SELECT
  DATE_TRUNC('day', START_TIME) AS replication_date,
  REPLICATION_GROUP_NAME,
  SUM(CREDITS_USED) AS total_credits,
  SUM(BYTES_TRANSFERRED) / POWER(1024, 3) AS gb_transferred
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### Cost Optimization Strategies

1. **Replicate only what you need**: Use separate failover groups with different schedules for critical vs. non-critical databases
2. **Choose same-cloud DR**: Co-locate primary and secondary on the same cloud provider to minimize transfer costs
3. **Schedule during off-peak hours**: For non-critical data, schedule less frequently
4. **Monitor data change rate**: High-churn tables drive up transfer costs; consider partitioning strategies

```sql
-- Use separate failover groups with different schedules
CREATE FAILOVER GROUP critical_data_fg
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = transactions_db, orders_db
  ALLOWED_ACCOUNTS = my_org.dr_account
  REPLICATION_SCHEDULE = '5 MINUTE';    -- tight RPO

CREATE FAILOVER GROUP analytics_data_fg
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = reporting_db, ml_features_db
  ALLOWED_ACCOUNTS = my_org.dr_account
  REPLICATION_SCHEDULE = '60 MINUTE';   -- looser RPO, lower cost
```

---

## Refresh Phases and Monitoring

A replication refresh goes through several phases. Understanding them helps diagnose slow or failed refreshes.

| Phase | Description |
|-------|-------------|
| `SECONDARY_SYNCHRONIZING_MEMBERSHIP` | Secondary syncs metadata about what needs to transfer |
| `PRIMARY_UPLOADING_METADATA` | Primary uploads changed metadata to cloud storage |
| `PRIMARY_UPLOADING_DATA` | Primary uploads changed data files |
| `SECONDARY_DOWNLOADING_METADATA` | Secondary downloads metadata from staging area |
| `SECONDARY_DOWNLOADING_DATA` | Secondary downloads data files |
| `SECONDARY_APPLYING_CHANGES` | Secondary applies the changes to its local storage |
| `COMPLETED` | Refresh is complete |

```sql
-- Real-time progress of an in-progress refresh
SELECT
  PHASE_NAME,
  RESULT,
  START_TIME,
  END_TIME,
  DETAILS
FROM TABLE(
  SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('PROD_FAILOVER_GROUP')
);
```

---

## Handling Schema Changes During Replication

DDL changes on the primary (ALTER TABLE, CREATE TABLE, DROP TABLE) are replicated along with data. However, there are edge cases:

- **External stages**: Replication copies the stage object but NOT the cloud credentials. You must re-configure credentials on the secondary.
- **Snowpipe**: Pipe objects replicate, but they are paused on the secondary and not actively ingesting.
- **Tasks**: Task objects replicate, but tasks are suspended on the secondary.
- **Streams**: Streams replicate, but the offset may not be accurate on the secondary.

```sql
-- After failover, resume tasks and pipes manually (or via automation)
ALTER TASK my_etl_task RESUME;
ALTER PIPE my_snowpipe_pipe REFRESH;
```

---

## Common Intermediate Interview Questions

**Q: How do you choose the replication schedule interval?**

Start with your RPO requirement and work backward. If RPO = 15 minutes, set a 10-minute schedule (buffer for refresh duration). Measure actual refresh times and adjust. Also factor in cost — tighter schedules cost more.

**Q: What is the difference between a connection URL and an account URL?**

An account URL (`company.snowflakecomputing.com`) always points to a specific account and does not redirect. A connection URL (`org-connection.snowflakecomputing.com`) is managed by Snowflake and automatically redirects to whichever account is currently primary. DR setups should use connection URLs so applications automatically reconnect after failover.

**Q: What objects are NOT replicated by failover groups?**

External stage credentials, active Snowpipe ingestion state, running tasks, and certain types of integrations require manual reconfiguration after failover. Object definitions replicate, but their operational state does not.

**Q: How do you monitor replication health?**

Use `SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY` for historical monitoring and `SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS` for real-time progress. Set up alerts in your observability tool to fire if the last successful refresh is older than 2× the schedule interval.
