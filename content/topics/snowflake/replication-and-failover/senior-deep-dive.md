---
title: "Snowflake BCDR Architecture: Failover, Failback, and Cross-Cloud DR"
description: "Business continuity planning, failover/failback procedures, cross-cloud replication, and enterprise DR architecture patterns"
content_type: study_material
topic: snowflake
subtopic: replication-and-failover
layer: senior-deep-dive
difficulty_level: senior
tags: [snowflake, BCDR, failover, failback, cross-cloud, disaster-recovery, architecture, multi-region, connection-redirect]
---

# Snowflake BCDR Architecture: Failover, Failback, and Cross-Cloud DR

## Business Continuity Planning (BCP) with Snowflake

Business continuity planning requires understanding what can fail and designing for each failure mode. In Snowflake, the key failure scenarios are:

| Failure Scenario | Impact | Snowflake Feature |
|------------------|--------|-------------------|
| Single AZ outage | Cloud provider mitigates; Snowflake auto-recovers | Built-in HA (multi-AZ by default) |
| Regional outage | Primary account inaccessible | Failover Groups → secondary account |
| Cloud provider outage | All accounts on that cloud unavailable | Cross-cloud replication (AWS↔Azure↔GCP) |
| Data corruption / accidental deletion | Data loss within the primary | Time Travel + Fail Safe |
| Configuration corruption | Bad DDL propagated to secondary | Replication lag window; Time Travel on secondary |

### BCP Decision Framework

```
Is data loss of >0 acceptable?
  ├── YES → What is the RPO?
  │     ├── < 5 min → Minimum schedule interval; evaluate near-real-time alternatives
  │     ├── 5–30 min → Standard failover group with tight schedule
  │     └── > 30 min → Standard failover group; optimize for cost
  └── NO → Snowflake currently does not offer synchronous (zero-RPO) replication
            Consider: application-level dual-write with eventual reconciliation
```

---

## Failover Procedures

A failover promotes the secondary account to become the new primary. This is a significant operational event that should be controlled by a tested runbook.

### Pre-Failover Checklist

1. Confirm the primary is truly inaccessible (avoid split-brain)
2. Check replication lag — know how much data may be lost
3. Notify stakeholders of impending failover and expected RPO
4. Confirm secondary account is healthy and last refresh succeeded
5. Identify all applications using Snowflake; confirm they use connection URLs

### Executing Failover

```sql
-- Run on the SECONDARY account to promote it to primary
ALTER FAILOVER GROUP prod_failover_group PRIMARY;

-- Snowflake performs:
-- 1. Marks the secondary as the new primary
-- 2. Updates the connection URL DNS record to point here
-- 3. Marks the old primary (if accessible) as secondary
-- 4. Enables writes on this account

-- Verify the failover
SHOW FAILOVER GROUPS;
-- Look for IS_PRIMARY = TRUE on this account
```

### Post-Failover Validation

```sql
-- Verify data freshness — when was the last refresh before failover?
SELECT MAX(REFRESH_END_TIME) AS last_refresh
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME = 'PROD_FAILOVER_GROUP';

-- Verify key row counts (compare with known baseline)
SELECT COUNT(*) FROM analytics_db.public.orders;
SELECT COUNT(*) FROM analytics_db.public.customers;

-- Re-enable tasks that were suspended
SHOW TASKS IN DATABASE analytics_db;
-- Resume any that need to run

-- Re-enable pipes
SHOW PIPES IN DATABASE analytics_db;
ALTER PIPE my_pipe REFRESH;

-- Validate external integrations (SSO, API integrations, notifications)
SHOW INTEGRATIONS;
```

---

## Failback Procedures

Failback is the process of returning to the original primary account after the incident is resolved. It is NOT automatic and must be explicitly executed.

### Why Failback Is Necessary

After failover, your "original primary" is now a secondary (or possibly offline). When it comes back online, it will:
1. Detect it is no longer primary
2. Begin syncing from the new primary (your old secondary)
3. Appear as a secondary with stale data

### Failback Steps

```sql
-- Step 1: Ensure original primary account is back online and synced
-- Run on the CURRENT PRIMARY (your DR account post-failover):
ALTER FAILOVER GROUP prod_failover_group REFRESH;
-- Wait for this to complete successfully

-- Step 2: On the ORIGINAL PRIMARY (now secondary):
-- Confirm it's caught up
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME = 'PROD_FAILOVER_GROUP'
ORDER BY REFRESH_END_TIME DESC
LIMIT 5;

-- Step 3: Execute failback — on ORIGINAL PRIMARY account
-- This promotes original primary back to primary status
ALTER FAILOVER GROUP prod_failover_group PRIMARY;

-- Step 4: Post-failback validation (same as post-failover)
-- Re-enable tasks, pipes, integrations as needed
-- Verify connection URLs are resolving correctly
```

### Failback Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Data written to DR account during failover not present in original primary | Sync DR→original before failback; verify row counts |
| Applications not reconnecting after DNS update | Use connection URLs; verify DNS TTL settings |
| Tasks/pipes not restarted | Include in post-failback runbook |
| External table credentials not configured | Pre-configure credentials on all accounts |

---

## Testing Disaster Recovery

A DR plan that has never been tested is not a DR plan. Snowflake DR tests should be run at least quarterly.

### Non-Disruptive DR Test

Test the secondary WITHOUT actually failing over. This validates data freshness and secondary readiness.

```sql
-- On the secondary account (read-only mode)
-- Test queries against replicated data
SELECT COUNT(*) FROM analytics_db.public.orders
WHERE order_date >= DATEADD('day', -1, CURRENT_DATE());

-- Compare with primary (must be within RPO window)
-- If results match within expected RPO delta → test passes

-- Validate all expected databases and schemas are present
SELECT TABLE_CATALOG, TABLE_SCHEMA, COUNT(*) AS table_count
FROM INFORMATION_SCHEMA.TABLES
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Disruptive DR Test (Scheduled Maintenance Window)

A full failover/failback test during a maintenance window.

```
1. [T-30min] Notify all stakeholders; freeze non-critical jobs
2. [T-0] Execute failover: ALTER FAILOVER GROUP prod_failover_group PRIMARY (on DR account)
3. [T+2min] Validate connection URL DNS resolution
4. [T+5min] Run application smoke tests against DR endpoint
5. [T+15min] Execute failback: ALTER FAILOVER GROUP prod_failover_group PRIMARY (on original account)
6. [T+20min] Validate applications reconnected to original primary
7. [T+30min] Resume all suspended jobs
8. Document: actual RTO achieved, any issues encountered
```

---

## Cross-Cloud Replication (AWS ↔ Azure ↔ GCP)

Cross-cloud replication protects against an entire cloud provider failure — the most extreme scenario.

### Architecture

```
┌──────────────────────────────┐    ┌──────────────────────────────┐
│   PRIMARY: AWS us-east-1      │    │  SECONDARY: Azure East US    │
│   ┌─────────────────────┐    │    │  ┌─────────────────────┐    │
│   │   prod_failover_fg   │────┼────┼──│   prod_failover_fg   │    │
│   │   (AWS S3 storage)   │    │    │  │   (Azure ADLS)       │    │
│   └─────────────────────┘    │    │  └─────────────────────┘    │
└──────────────────────────────┘    └──────────────────────────────┘
                │                                    │
                └──────── cross-cloud transfer ──────┘
                          (high egress cost)
```

### Creating Cross-Cloud Failover Groups

```sql
-- Primary on AWS; secondary on Azure
CREATE FAILOVER GROUP cross_cloud_fg
  OBJECT_TYPES = DATABASES, SHARES, USERS, ROLES, WAREHOUSES
  ALLOWED_DATABASES = critical_db
  ALLOWED_ACCOUNTS = my_org.azure_dr_account   -- Azure account
  REPLICATION_SCHEDULE = '30 MINUTE';           -- Less frequent due to cost

-- On Azure secondary account:
CREATE FAILOVER GROUP cross_cloud_fg
  AS REPLICA OF my_org.aws_primary_account.cross_cloud_fg;
```

### Cross-Cloud Cost Considerations

Cross-cloud data transfer costs are significantly higher than same-cloud:

| Scenario | Estimated Transfer Cost |
|----------|------------------------|
| AWS us-east-1 → AWS us-west-2 | ~$0.02/GB |
| AWS us-east-1 → Azure East US | ~$0.09–$0.19/GB |
| AWS us-east-1 → GCP us-central1 | ~$0.09–$0.19/GB |

**Cost mitigation for cross-cloud**:
- Replicate only the most critical databases cross-cloud; use same-cloud DR for everything else
- Use longer replication intervals for cross-cloud (30–60 min)
- Consider a tiered DR strategy: same-cloud secondary (tight RPO) + cross-cloud tertiary (loose RPO)

---

## BCDR Architecture Patterns

### Pattern 1: Active-Passive (Single Secondary)

Simple and cost-effective. One primary, one secondary.

```
AWS Primary ──(5 min replication)──► AWS Secondary
              RPO: ~5 min | RTO: ~20 min
```

Best for: Most production workloads, cost-sensitive environments

### Pattern 2: Active-Passive with Cross-Cloud Tertiary

Protects against cloud provider failure.

```
AWS Primary ──(5 min)──► AWS Secondary (same-cloud DR)
     │
     └──(30 min)──► Azure Tertiary (cross-cloud DR for cloud failure)
```

Best for: Regulated industries (banking, healthcare) with strict BCDR requirements

### Pattern 3: Active-Active with Read Scaling

Both accounts receive reads; one is the write primary.

```
AWS Primary (writes + reads) ──(5 min)──► AWS Secondary (reads only)
                                          ▲ Read workload split here
                                          └─ BI tools, heavy analytics
```

Best for: High-read environments where secondary can offload reporting queries

### Pattern 4: Multi-Region Active-Active (Application-Level)

Snowflake does not natively support active-active writes. This requires application-level architecture:

```
Region A App ──writes──► Snowflake Primary (AWS)
Region B App ──writes──► Regional DB (interim) ──sync──► Snowflake Primary
                                                          ▼ replication
                                                    Snowflake Secondary
```

Best for: Global applications with write latency requirements; complex to maintain

---

## Connection URL Architecture for Zero-Touch Failover

The connection URL is the key to transparent failover. Proper setup means applications never need to change their connection strings.

```
Application Connection String: myorg-prod.snowflakecomputing.com
                                         │
                              DNS lookup (TTL: 60s)
                                         │
                              ┌──────────▼──────────┐
                              │  Snowflake DNS Layer  │
                              │                       │
                              │  if PRIMARY=AWS:      │
                              │    → aws_account.sf.com│
                              │                       │
                              │  if FAILOVER executed: │
                              │    → azure_account.sf.com│
                              └───────────────────────┘
```

```sql
-- Create connection and associate with failover group
CREATE CONNECTION prod_connection;

ALTER FAILOVER GROUP prod_failover_group
  ADD CONNECTION prod_connection;

-- Check connection status
SHOW CONNECTIONS;
-- IS_PRIMARY column tells you which account is primary for this connection
```

---

## Advanced Monitoring and Alerting

```sql
-- Alert: Replication lag exceeds threshold
CREATE ALERT replication_lag_alert
  WAREHOUSE = monitoring_wh
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
    WHERE REPLICATION_GROUP_NAME = 'PROD_FAILOVER_GROUP'
      AND REFRESH_END_TIME < DATEADD('minute', -15, CURRENT_TIMESTAMP())
      AND PHASE_NAME = 'COMPLETED'
  ))
  THEN
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
        'CRITICAL: Replication lag for PROD_FAILOVER_GROUP exceeds 15 minutes'
      ),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('slack_alerts')
    );

-- Dashboard query: Replication health overview
SELECT
  REPLICATION_GROUP_NAME,
  MAX(REFRESH_END_TIME) AS last_successful_refresh,
  DATEDIFF('minute', MAX(REFRESH_END_TIME), CURRENT_TIMESTAMP()) AS lag_minutes,
  AVG(DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME)) AS avg_refresh_duration_sec,
  COUNT_IF(RESULT = 'SUCCESS') AS successful_refreshes_last_24h,
  COUNT_IF(RESULT != 'SUCCESS') AS failed_refreshes_last_24h
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REFRESH_START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1;
```

---

## Senior-Level Interview Questions

**Q: How would you design a DR strategy for a Snowflake environment with an RPO of 5 minutes and RTO of 15 minutes?**

Use a failover group with a 4-minute replication schedule (leaving buffer for refresh duration). Use connection URLs for all application connections. Build an automated failover runbook that can be triggered with a single command. Pre-validate the secondary monthly via non-disruptive tests. Invest in monitoring alerts that fire within 2 minutes of a missed refresh.

**Q: What are the tradeoffs between cross-cloud and same-cloud DR?**

Same-cloud is cheaper (lower egress), faster (lower latency for replication), but doesn't protect against cloud-provider-wide outages (rare but possible). Cross-cloud protects against provider failures but costs significantly more in data transfer and introduces higher replication latency. The decision depends on the organization's risk appetite and compliance requirements.

**Q: How do you handle a failover when external stages and integrations are involved?**

External stages copy their configuration (URL, file format) but NOT credentials. After failover, you must reconfigure cloud storage credentials (IAM roles for S3, managed identities for ADLS). Security integrations replicate if included in the failover group, but OAuth tokens may need to be re-issued. Build a post-failover automation script that reconfigures known-external dependencies.

**Q: Explain the split-brain risk in Snowflake failover and how to mitigate it.**

Split-brain occurs when both accounts believe they are primary — for example, if a network partition makes the primary appear down to the DR team but it's actually still up. Mitigation: (1) confirm primary is truly inaccessible before initiating failover, (2) use Snowflake's automatic fencing — once failover is executed, the old primary becomes read-only for replicated objects, preventing conflicting writes, (3) use connection URLs so applications automatically redirect.
