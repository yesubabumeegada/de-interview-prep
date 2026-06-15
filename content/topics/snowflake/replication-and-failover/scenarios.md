---
title: "Snowflake Replication & Failover Scenario Questions"
description: "Interview scenarios covering replication groups, cross-region failover configuration, and multi-cloud DR strategy design"
content_type: scenario_question
topic: snowflake
subtopic: replication-and-failover
tags: [snowflake, replication, failover, disaster-recovery, BCDR, multi-region, cross-cloud, scenarios]
---

<article data-difficulty="junior">

## Scenario: Replication Groups vs. Failover Groups

Your team is setting up Snowflake for a new SaaS product. Your manager asks you to "set up replication so we can recover if AWS goes down." A colleague suggests using a replication group. Another suggests a failover group. When you look at the Snowflake docs, you see both options.

**Question**: What is the difference between a replication group and a failover group, and which would you recommend here? What are the key limitations of each?

<details>
<summary>✅ Solution</summary>

**Replication Group**

A replication group copies a defined set of objects (databases, shares, etc.) from a primary account to one or more secondary accounts on a schedule. Key characteristics:

- Objects in the secondary are **read-only**
- If the primary goes down, you **cannot promote** the secondary to accept writes
- Useful for read offloading or data distribution, NOT for true disaster recovery
- Cannot be used for failover — the secondary is a permanent read replica

```sql
CREATE REPLICATION GROUP my_read_replica
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = analytics_db
  ALLOWED_ACCOUNTS = my_org.bi_account
  REPLICATION_SCHEDULE = '15 MINUTE';
```

**Failover Group**

A failover group is a superset of a replication group. It copies objects AND adds the ability to promote the secondary to primary.

- Secondary starts as read-only (same as replication group)
- During an outage, you can run `ALTER FAILOVER GROUP ... PRIMARY` on the secondary to promote it
- After promotion, the secondary becomes writable and the old primary becomes read-only (if it comes back online)
- Supports connection URLs for transparent client redirect

```sql
CREATE FAILOVER GROUP prod_fg
  OBJECT_TYPES = DATABASES, USERS, ROLES, WAREHOUSES, INTEGRATIONS
  ALLOWED_DATABASES = prod_db
  ALLOWED_ACCOUNTS = my_org.dr_account
  REPLICATION_SCHEDULE = '10 MINUTE';
```

**Recommendation for this scenario**: Use a **failover group**. The goal is recovery if AWS goes down, which means you need to be able to WRITE to the secondary after an outage. A replication group would only allow reads on the secondary — you could not run your SaaS product from it. A failover group gives you all the replication benefits plus the ability to actually fail over.

**Key takeaway summary**:

| Feature | Replication Group | Failover Group |
|---------|------------------|----------------|
| Copies objects to secondary | ✅ | ✅ |
| Secondary is read-only | ✅ | ✅ (until failover) |
| Can promote secondary to primary | ❌ | ✅ |
| Supports connection URLs | ❌ | ✅ |
| Good for DR? | ❌ | ✅ |
| Good for read offloading? | ✅ | ✅ |

</details>

</article>

<article data-difficulty="mid">

## Scenario: Configuring Cross-Region Failover for a Critical Snowflake Account

You work at a healthcare analytics company. The CISO has mandated that the Snowflake environment must achieve an RPO of 10 minutes and an RTO of 30 minutes in the event of an AWS us-east-1 regional outage. You have a secondary Snowflake account available in AWS us-west-2.

Your Snowflake environment includes:
- `clinical_trials_db` — the most critical database (PHI data)
- `reporting_db` — BI dashboards (medium criticality)
- `dev_sandbox_db` — developer experimentation (low criticality)
- SSO via SAML 2.0 (Okta integration)
- 15 Snowflake Tasks running ETL pipelines
- 3 Snowpipe ingestion pipelines
- All applications currently use hardcoded account URLs

**Question**: Walk through the complete setup to achieve this RPO/RTO. What specific steps are needed, and what are the gotchas?

<details>
<summary>✅ Solution</summary>

**Step 1: Design the failover group structure**

Given the mixed criticality, use two failover groups with different schedules:

```sql
-- On PRIMARY account (us-east-1)

-- Critical: 10-minute RPO (satisfies CISO requirement)
CREATE FAILOVER GROUP healthcare_critical_fg
  OBJECT_TYPES =
    DATABASES,
    USERS,
    ROLES,
    WAREHOUSES,
    RESOURCE MONITORS,
    INTEGRATIONS,        -- includes Okta SAML integration
    NETWORK POLICIES
  ALLOWED_DATABASES = clinical_trials_db
  ALLOWED_INTEGRATION_TYPES = SECURITY INTEGRATIONS  -- SSO/SAML
  ALLOWED_ACCOUNTS = my_org.prod_dr_west
  REPLICATION_SCHEDULE = '8 MINUTE';  -- 8 min schedule + ~2 min refresh = ~10 min RPO

-- Medium priority: 30-minute schedule
CREATE FAILOVER GROUP healthcare_reporting_fg
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = reporting_db
  ALLOWED_ACCOUNTS = my_org.prod_dr_west
  REPLICATION_SCHEDULE = '30 MINUTE';

-- Dev sandbox: nightly manual refresh (no DR requirement)
CREATE REPLICATION GROUP dev_rg
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = dev_sandbox_db
  ALLOWED_ACCOUNTS = my_org.prod_dr_west;
-- No schedule set — manual refresh as needed
```

**Step 2: Create secondaries on DR account**

```sql
-- On SECONDARY account (us-west-2)
CREATE FAILOVER GROUP healthcare_critical_fg
  AS REPLICA OF my_org.prod_east.healthcare_critical_fg;

CREATE FAILOVER GROUP healthcare_reporting_fg
  AS REPLICA OF my_org.prod_east.healthcare_reporting_fg;

-- Initial full refresh (may take hours for clinical_trials_db)
ALTER FAILOVER GROUP healthcare_critical_fg REFRESH;
ALTER FAILOVER GROUP healthcare_reporting_fg REFRESH;
```

**Step 3: Set up connection URLs (CRITICAL for RTO)**

Without connection URLs, every application must be manually reconfigured after failover — destroying the RTO target.

```sql
-- On PRIMARY account
CREATE CONNECTION clinical_prod_connection;
ALTER FAILOVER GROUP healthcare_critical_fg
  ADD CONNECTION clinical_prod_connection;

SHOW CONNECTIONS;
-- Note the connection URL: my_org-clinical_prod_connection.snowflakecomputing.com
```

**Step 4: Migrate all applications from account URLs to connection URLs**

This is usually the most time-consuming step in production:

```
# Before (breaks during failover):
SNOWFLAKE_ACCOUNT=mycompany.us-east-1

# After (auto-redirects on failover):
SNOWFLAKE_ACCOUNT=my_org-clinical_prod_connection.snowflakecomputing.com
```

Update all application configs, dbt profiles, Airflow connections, Tableau data sources, etc.

**Step 5: Handle post-failover recovery of tasks and pipes**

This is a major gotcha. Snowflake Tasks are suspended on the secondary and Snowpipe ingestion is paused.

Create a post-failover runbook script:

```sql
-- After failover completes, run on the new primary (old DR account):

-- 1. Re-enable Snowpipe (refresh clears the pipe queue)
ALTER PIPE clinical_trials_pipe REFRESH;
ALTER PIPE lab_results_pipe REFRESH;
ALTER PIPE adverse_events_pipe REFRESH;

-- 2. Resume ETL tasks (in dependency order — start with root tasks)
ALTER TASK etl_orchestrator_task RESUME;
-- (Child tasks auto-resume if ALLOW_OVERLAPPING_EXECUTION is set)

-- 3. Verify Okta SSO integration works (test login)
-- SAML integration replicates, but may need SAML metadata refresh from Okta
```

**Step 6: Build monitoring to validate RPO**

```sql
-- Alert if last refresh is older than RPO threshold
CREATE ALERT rpo_breach_alert
  WAREHOUSE = monitoring_wh
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
    WHERE REPLICATION_GROUP_NAME = 'HEALTHCARE_CRITICAL_FG'
      AND RESULT = 'SUCCESS'
      AND REFRESH_END_TIME < DATEADD('minute', -12, CURRENT_TIMESTAMP())
  ))
  THEN CALL notify_security_team('RPO breach: clinical_trials replication lag > 12 minutes');
```

**Gotchas summary**:

1. **Okta SAML metadata**: The security integration replicates, but if Okta's metadata URL isn't accessible from us-west-2 at time of failover, SSO will fail. Test this in a DR drill.
2. **External stages**: If any stages reference S3 buckets with account-specific IAM roles, these won't work on the DR account. Use cross-account IAM roles or bucket policies.
3. **Tasks and pipes do NOT auto-start**: Must be manually resumed post-failover.
4. **Hardcoded account URLs**: Any application still using the old account URL won't auto-redirect. The application migration in Step 4 is critical.
5. **Dev sandbox**: Explicitly excluded from critical failover group — document this so the dev team knows dev data may be weeks old in a DR scenario.

**RTO calculation**:
- Alert fires: ~5 min
- Decision/approval: ~5 min (pre-approved for this runbook)
- Failover execution: ~2 min (single SQL command)
- DNS propagation: ~2 min (connection URL TTL)
- Post-failover automation: ~5 min
- Smoke test validation: ~5 min
- **Total RTO: ~24 minutes** ✅ within the 30-minute target

</details>

</article>

<article data-difficulty="senior">

## Scenario: Designing a Multi-Cloud DR Strategy with Snowflake

You are the principal data architect at a global financial services firm. The CTO has given you the following requirements for the data platform:

1. **Primary workload** runs on Snowflake (AWS us-east-1)
2. **RPO**: 5 minutes for trading data; 30 minutes for analytics data
3. **RTO**: 15 minutes maximum
4. **Multi-cloud resilience**: The platform must survive an entire AWS outage (however unlikely)
5. **Geo-compliance**: European customer data must remain in EU data centers
6. **Cost constraint**: Total DR spend must not exceed 20% of primary Snowflake spend
7. **Regulatory**: SOC 2 Type II audit requires documented and tested DR procedures

Design the complete multi-cloud DR architecture for Snowflake, including account topology, failover group design, connection strategy, testing plan, and cost justification.

<details>
<summary>✅ Solution</summary>

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ORGANIZATION: global_finance_org                      │
│                                                                          │
│  ┌─────────────────────────┐    ┌─────────────────────────────────────┐ │
│  │  PRIMARY                │    │  SAME-CLOUD DR (Tier 1)             │ │
│  │  AWS us-east-1          │    │  AWS us-west-2                      │ │
│  │  trading_db    ◄────────┤    │  (replica of critical data)         │ │
│  │  eu_data_db    ──5min──►│    │  RPO: 5 min | RTO: 15 min          │ │
│  │  analytics_db  ──30min─►│    └─────────────────────────────────────┘ │
│  │  reference_db           │                                            │
│  └──────────┬──────────────┘    ┌─────────────────────────────────────┐ │
│             │                   │  CROSS-CLOUD DR (Tier 2)            │ │
│             └──60min (Azure)───►│  Azure East US                      │ │
│             └──(EU: Azure NE)──►│  Cloud-failure resilience           │ │
│                                  │  RPO: 60 min | RTO: 30 min         │ │
│                                  └─────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## Account Topology

```
global_finance_org
├── aws_primary        (us-east-1) ← WRITE PRIMARY
├── aws_dr_west        (us-west-2) ← Tier 1 DR (same cloud)
├── azure_dr_east      (Azure East US) ← Tier 2 DR (cross-cloud)
├── azure_eu_dr        (Azure North Europe) ← EU data compliance DR
└── eu_primary         (Azure West Europe) ← EU primary for EU data
```

## Failover Group Design

### Tier 1: Trading Data (5-minute RPO, same-cloud DR)

```sql
-- On aws_primary:
CREATE FAILOVER GROUP trading_critical_fg
  OBJECT_TYPES = DATABASES, USERS, ROLES, WAREHOUSES,
                 INTEGRATIONS, NETWORK POLICIES, RESOURCE MONITORS
  ALLOWED_DATABASES = trading_db, positions_db, risk_db, market_data_db
  ALLOWED_INTEGRATION_TYPES = SECURITY INTEGRATIONS, NOTIFICATION INTEGRATIONS
  ALLOWED_ACCOUNTS = global_finance_org.aws_dr_west
  REPLICATION_SCHEDULE = '4 MINUTE';

-- Same-cloud DR secondary:
-- On aws_dr_west:
CREATE FAILOVER GROUP trading_critical_fg
  AS REPLICA OF global_finance_org.aws_primary.trading_critical_fg;
```

### Tier 2: Analytics Data (30-minute RPO, same-cloud DR)

```sql
-- On aws_primary:
CREATE FAILOVER GROUP analytics_fg
  OBJECT_TYPES = DATABASES
  ALLOWED_DATABASES = analytics_db, reporting_db, ml_features_db, data_science_db
  ALLOWED_ACCOUNTS = global_finance_org.aws_dr_west
  REPLICATION_SCHEDULE = '30 MINUTE';
```

### Tier 3: Cross-Cloud Resilience (60-minute RPO, Azure)

```sql
-- On aws_primary (cross-cloud replication to Azure):
CREATE FAILOVER GROUP cross_cloud_fg
  OBJECT_TYPES = DATABASES, USERS, ROLES, WAREHOUSES
  ALLOWED_DATABASES = trading_db, positions_db   -- critical only; minimize transfer cost
  ALLOWED_ACCOUNTS = global_finance_org.azure_dr_east
  REPLICATION_SCHEDULE = '60 MINUTE';
-- Less frequent due to high cross-cloud transfer cost
```

### EU Data Compliance: Separate EU Primary

```sql
-- EU primary on Azure West Europe manages all EU customer data
-- EU data NEVER flows through US accounts
-- On eu_primary (Azure West Europe):
CREATE FAILOVER GROUP eu_data_fg
  OBJECT_TYPES = DATABASES, USERS, ROLES, INTEGRATIONS
  ALLOWED_DATABASES = eu_customers_db, eu_transactions_db, eu_risk_db
  ALLOWED_ACCOUNTS = global_finance_org.azure_eu_dr
  REPLICATION_SCHEDULE = '5 MINUTE';
```

## Connection URL Strategy

```sql
-- One connection URL per business domain
-- Applications use domain-specific connection URLs

CREATE CONNECTION trading_connection;          -- trading systems
CREATE CONNECTION analytics_connection;        -- BI/analytics tools
CREATE CONNECTION eu_connection;               -- EU applications (on eu_primary)

-- Assign connections to failover groups
ALTER FAILOVER GROUP trading_critical_fg
  ADD CONNECTION trading_connection;
ALTER FAILOVER GROUP analytics_fg
  ADD CONNECTION analytics_connection;
-- eu_connection managed separately on eu_primary account
```

## Failover Decision Matrix

| Scenario | Action | Who Executes | Target |
|----------|--------|--------------|--------|
| AWS us-east-1 AZ outage | Wait — Snowflake auto-recovers | Automated | N/A |
| AWS us-east-1 regional degradation (partial) | Monitor; may failover if SLA breach | On-call SRE | aws_dr_west |
| AWS us-east-1 full regional outage | Failover to aws_dr_west | DR Incident Commander | aws_dr_west |
| AWS complete cloud outage | Failover to azure_dr_east | DR Incident Commander + VP approval | azure_dr_east |
| Data corruption on primary | Do NOT failover; use Time Travel | Data Engineering Lead | N/A |

## Testing Plan (SOC 2 Requirement)

### Monthly (Non-Disruptive)
- Query secondary accounts to validate data freshness
- Verify connection URL DNS resolution
- Review replication lag metrics for the past 30 days
- Document results in DR log

### Quarterly (Disruptive — Maintenance Window)
- Execute full failover to aws_dr_west
- Run application smoke tests against DR endpoint
- Measure actual RTO achieved
- Execute failback to aws_primary
- Document RTO/RPO achieved vs. targets

### Annually (Cross-Cloud)
- Execute failover to azure_dr_east (cross-cloud test)
- Validate all critical trading applications work on Azure endpoint
- Test EU failover separately (azure_eu_dr)
- Full documentation for SOC 2 auditors

```sql
-- DR test report template (run after each test)
INSERT INTO dr_test_log (
  test_date,
  test_type,
  failover_group,
  failover_initiated_time,
  connection_redirected_time,
  smoke_test_passed_time,
  failback_completed_time,
  actual_rto_minutes,
  data_loss_rows,
  issues_encountered
)
VALUES (
  CURRENT_DATE(),
  'QUARTERLY_DISRUPTIVE',
  'TRADING_CRITICAL_FG',
  '2024-10-15 02:00:00'::TIMESTAMP,
  '2024-10-15 02:02:30'::TIMESTAMP,
  '2024-10-15 02:08:00'::TIMESTAMP,
  '2024-10-15 02:45:00'::TIMESTAMP,
  8,           -- actual RTO: 8 minutes (vs. 15 min target) ✅
  0,           -- zero data loss (refresh completed 3 min before failover)
  'Okta SSO required metadata refresh after failover — added to runbook'
);
```

## Cost Justification (20% constraint)

| Component | Monthly Cost Estimate |
|-----------|----------------------|
| Tier 1: Same-cloud DR compute (aws_dr_west) | $3,500 |
| Tier 1: Storage (full copy of trading DBs, ~5TB) | $1,200 |
| Tier 1: Data transfer (same-cloud) | $200 |
| Tier 2: Analytics compute (aws_dr_west, 30-min schedule) | $800 |
| Tier 2: Storage (~20TB analytics) | $2,000 |
| Tier 3: Cross-cloud compute (to Azure, hourly) | $400 |
| Tier 3: Data transfer (cross-cloud, ~500GB/day × 30) | $1,350 |
| EU compliance DR (azure_eu_dr) | $1,200 |
| **Total DR cost** | **~$10,650** |

Assuming primary Snowflake spend is ~$60,000/month:
- DR as % of primary: 10,650 / 60,000 = **17.75%** ✅ (under 20% target)

**Cost control levers if over budget**:
1. Reduce cross-cloud replication frequency (hourly → 4-hourly)
2. Exclude analytics_db from cross-cloud DR (keep only trading data)
3. Move dev/staging accounts out of replication entirely

## Key Design Decisions Explained

**Why same-cloud for Tier 1 (not cross-cloud)?**
Same-cloud (AWS → AWS) replication is ~5–10x cheaper in data transfer costs and achieves lower replication latency. For the 5-minute RPO target, same-cloud is necessary; cross-cloud latency could jeopardize the RPO.

**Why a separate EU primary (not replicate from US)?**
GDPR Article 44–49 restricts transfer of EU personal data to non-EU countries without adequate safeguards. While Standard Contractual Clauses (SCCs) could potentially allow it, the risk is significant. A separate EU primary with EU-only DR accounts is the cleanest GDPR architecture.

**Why cross-cloud at all if it's more expensive?**
The requirement explicitly asks for resilience against an entire AWS outage. This is rare but the regulatory environment for financial services means regulators expect it. The Tier 3 cross-cloud replication covers only the most critical 5TB (trading data) to keep costs manageable.

</details>

</article>
