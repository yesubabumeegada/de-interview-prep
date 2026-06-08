---
title: "Azure SQL & Managed Instance — Scenarios"
topic: azure
subtopic: azure-sql
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, azure-sql, scenarios, interview, migration, performance, design]
---

# Azure SQL & Managed Instance — Interview Scenarios

## Scenario 1: On-Premises SQL Server Migration

**Question:** A company has a 2TB SQL Server 2012 database on-premises with: SQL Agent jobs, cross-database queries (3 databases), linked servers to Oracle, CLR functions, and stored procedures. They want to migrate to Azure with minimum downtime and code changes. What do you recommend?

**Answer:**

```
Assessment:
  Features used: SQL Agent, cross-DB queries, linked server (Oracle), CLR, stored procs
  SQL Database (PaaS): MISSING SQL Agent, cross-DB queries, CLR, linked servers → too many gaps
  SQL Managed Instance: ALL features above are supported → best choice
  
  Run: Azure Database Migration Service (DMS) compatibility assessment
  Expected report: 98% compatible (only SQL 2012 deprecated features to fix)

Migration plan (online migration — minimal downtime):

Phase 1: Infrastructure Setup (1 week)
  Create SQL Managed Instance:
    Tier: Business Critical (for HA + read replica)
    vCores: 16 (match on-prem server sizing)
    Storage: 4TB (2TB data + growth headroom)
    VNet: inject into existing Azure VNet (peered with on-prem via ExpressRoute)
  
  Network:
    ExpressRoute from on-prem data center to Azure (private, 1 Gbps link)
    Private endpoint for SQL MI (not exposed to internet)
    
  Linked server replacement:
    Oracle linked server → create Azure Function as REST bridge
    Or: migrate Oracle data to Azure SQL DB, reconfigure linked server inside Azure

Phase 2: Assessment and Remediation (2 weeks)
  Run SSMA (SQL Server Migration Assistant) on source databases
  Fix deprecated SQL 2012 syntax: SET ROWCOUNT, non-ANSI joins, etc.
  Test all SQL Agent jobs in dev environment on Azure SQL MI
  Validate CLR assemblies (must be safe or EXTERNAL_ACCESS tier on MI)

Phase 3: Online Migration (DMS)
  DMS online mode:
  a) Initial full backup → restore to SQL MI (~2-4 hours for 2TB)
  b) DMS monitors SQL Server transaction log → continuously applies changes to MI
  c) Lag: typically < 1 minute behind source
  d) Runs for days/weeks while you validate

Phase 4: Validation (1-2 weeks)
  Test all stored procedures with production-like data
  Validate SQL Agent job outputs (compare results)
  Test cross-database queries
  Performance test: key queries run within 110% of on-prem time

Phase 5: Cutover (planned weekend)
  a) Set DMS to "ready for cutover" mode
  b) Drain on-prem application connections
  c) Allow DMS final sync (~5 minutes of remaining changes)
  d) Point application connection strings to SQL MI endpoint
  e) Verify: run smoke tests
  f) Total downtime: 5-15 minutes
  
  Rollback plan: keep on-prem database read-write enabled for 1 week (emergency fallback)

Cost estimate:
  SQL MI Business Critical 16 vCores: ~$5,400/month
  Apply Azure Hybrid Benefit (existing SQL Server license): -40% = ~$3,240/month
  ExpressRoute: ~$500-2,000/month (depends on circuit)
  Total: ~$4,000-5,000/month (vs on-prem server depreciation + maintenance)
```

---

## Scenario 2: Database Performance Degradation

**Question:** An Azure SQL DB (General Purpose, 8 vCores) that supports an order management application was running fine for 2 years. This week, response time went from 200ms to 8 seconds. No code changes were deployed. What do you investigate?

**Answer:**

```
Step 1: Check resource metrics (Azure Monitor, last 7 days)
  Portal → SQL Database → Metrics:
    cpu_percent:  was 40% → now 95% (CPU saturation)
    dtu_consumption_percent: confirms resource pressure
    storage_percent: was 60% → now 95% (storage nearly full!)
    
  Discovery: storage is 95% full — this causes massive slowdowns in Azure SQL

Step 2: Why did storage fill up?
  Query: SELECT TOP 10 table_name, row_count, total_space_mb, unused_space_mb
         FROM... sys.dm_db_partition_stats
  Discovery: orders table grew from 50GB to 400GB (2 years × rapid growth, no archival)
  
  Also check: transaction log space
  SELECT log_size_mb = size * 8.0 / 1024,
         log_used_mb = FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024,
         log_used_pct = CAST(100 * FILEPROPERTY(name, 'SpaceUsed') AS FLOAT) / size
  FROM sys.database_files WHERE type_desc = 'LOG';
  Discovery: log is 90% full due to long-running uncommitted transaction

Step 3: Identify the long-running transaction
  SELECT session_id, transaction_id, login_time, last_request_start_time
  FROM sys.dm_exec_sessions WHERE open_transaction_count > 0;
  → Find the blocking transaction, identify the ETL job that left it open

Immediate fixes:
  a) Kill the long-running transaction (KILL session_id) → log frees up
  b) Scale up to 16 vCores AND increase storage to 2TB via portal (no downtime)
  c) Archive old orders: move orders older than 1 year to a separate archive DB
     INSERT INTO archive_db.dbo.orders SELECT * FROM orders WHERE order_date < '2022-01-01';
     DELETE FROM orders WHERE order_date < '2022-01-01';
  d) Add missing index (Query Store shows a full-scan query on orders appeared this week)

Long-term fixes:
  1. Implement table partitioning on order_date → fast partition deletion for archival
  2. Set up Azure Monitor alert: storage > 80% → notify DBA team
  3. Establish data retention policy: 2 years online, archive to cold tier after
  4. Right-size: after archival, evaluate if 8 vCores is still needed
  
Expected outcome:
  After transaction kill + archive: CPU drops to 45%, response time returns to 200ms
  After storage cleanup: storage at 40%
```

---

## Scenario 3: Multi-Tenant SaaS Database Design

**Question:** You're building a SaaS CRM application. You expect 500 small-medium business tenants with variable usage patterns. Each tenant needs data isolation. Design the Azure SQL strategy.

**Answer:**

```
Three design options:

Option A: Single database, row-level security (cheapest, least isolation)
  One database, tenants identified by tenant_id column
  Row-level security: each user only sees their tenant's rows
  
  CREATE SECURITY POLICY TenantFilter
  ADD FILTER PREDICATE dbo.fn_tenant_access_predicate(tenant_id) ON dbo.customers;
  
  Pros: cheapest ($5-50/month total), easy management
  Cons: noisy neighbor (one tenant's heavy query affects others), hard to restore one tenant
  Best for: 500 very small tenants with predictable low usage

Option B: Elastic Pool with one database per tenant (recommended for this case)
  500 databases in 1-2 Elastic Pools
  Each tenant has completely isolated database
  
  Pool sizing:
    500 tenants, avg 10 DTUs each, but peak concurrent = 20 tenants = 200 DTUs
    Pool: 300 DTU Standard pool (headroom) = $149/month
    vs isolated: 500 × 10 DTU = 5,000 DTU = $2,400/month → 16× savings
    
  Implementation:
    Shard map management: shard map tracks tenant_id → database mapping
    Connection routing: application reads shard map → connects to correct DB
    Elastic Query: cross-database analytics via Elastic Query feature
    
  Pros: per-tenant restore, per-tenant scale, data isolation, cost-effective
  Cons: management complexity (500 databases), shard map management

Option C: Dedicated database per tenant (highest isolation, most expensive)
  Each tenant gets their own SQL DB
  No shared resources at all
  
  Pros: complete isolation, per-tenant SLA, custom configurations
  Cons: $5-100+/month per tenant = $2,500-50,000/month for 500 tenants
  Best for: enterprise tenants willing to pay premium

Recommendation: Option B (Elastic Pool) for this case

Implementation details for Option B:
  Pool: Standard 300 DTU (upgrade to 400 DTU during growth)
  Per-database limits: min 0 DTU, max 50 DTU (prevents one tenant hogging pool)
  
  Tenant onboarding automation (Azure DevOps pipeline):
  1. Create new database in pool: az sql db create --elastic-pool tenant-pool --name tenant_{id}
  2. Apply schema: run EF Core migrations or Flyway
  3. Register in shard map: update tenant registry
  4. Seed initial data
  Total time: 2-3 minutes per new tenant
  
  Monitoring:
  Pool utilization dashboard → alert if avg DTU% > 80% for 1 hour → add DTUs
  Per-tenant usage report → identify power users for potential tier upgrade
  
  Data isolation guarantee: each database = completely separate SQL file
  One tenant's data breach doesn't expose other tenants' data
```

---

## Interview Tips

> **Tip 1:** "What's row-level security (RLS) in Azure SQL and how does it work?" — RLS restricts which rows a user can see or modify, enforced at the database engine level. You create a predicate function that takes the filtering column value and returns true/false based on the current user's identity. Apply it as a security policy on the table. Even if a developer writes `SELECT * FROM customers`, the engine transparently adds a WHERE clause using the predicate. Neither the application nor the ORM needs to be aware of the filtering. Key risk: the predicate function must be secure — if it has a bug, all rows may be exposed or hidden.

> **Tip 2:** "How do you handle schema migrations in a production Azure SQL DB with zero downtime?" — Use online schema changes where possible: `ALTER TABLE ... ADD COLUMN` is instantaneous for nullable columns (no rebuild). Changing column type or adding NOT NULL: create a new column, backfill it in batches (small transactions to avoid log bloat), add a check constraint, then drop the old column. For index changes: `CREATE INDEX ... WITH (ONLINE = ON)` in Business Critical/Hyperscale tier allows index creation without locking reads. For table rebuilds: use partition switching — build the new version in a staging table, switch it in as an atomic metadata operation.

> **Tip 3:** "When would you choose Azure SQL Hyperscale over other tiers?" — Use Hyperscale when: (a) database size exceeds 4TB (only option at that scale), (b) you need read scale-out (up to 5 named read replicas served from the same page servers — no data copy lag), (c) fast scale-up is required (minutes not hours), (d) unpredictable size growth (storage auto-expands without downtime), (e) fast point-in-time restore (restores from log service rather than full backup — minutes not hours). Limitations: Hyperscale cannot be downgraded back to General Purpose or Business Critical (one-way migration). This is the most important gotcha — test thoroughly before committing.
