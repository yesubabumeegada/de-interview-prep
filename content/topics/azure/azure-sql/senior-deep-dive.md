---
title: "Azure SQL & Managed Instance — Senior Deep Dive"
topic: azure
subtopic: azure-sql
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, azure-sql, managed-instance, hyperscale, performance, security, bcdr]
---

# Azure SQL & Managed Instance — Senior Deep Dive

## Hyperscale Architecture

```
Hyperscale = cloud-native distributed SQL architecture (Azure SQL Database only)

Problem solved:
  Traditional SQL DB: max 4TB, scale-up only (more vCores = more cost)
  Hyperscale: up to 100TB, scale compute independently of storage

Architecture layers:
  ┌────────────────────────────────────────────┐
  │     Primary Compute Replica                │  ← Reads + Writes (your connection)
  │     (0-80 vCores, Business Critical or GP) │
  └──────────────────┬─────────────────────────┘
                     │ Log stream (WAL)
  ┌──────────────────▼─────────────────────────┐
  │           Log Service                       │  ← Append-only WAL, 3 copies
  └──────────────────┬─────────────────────────┘
                     │
  ┌──────────────────▼─────────────────────────┐
  │         Page Servers (1-16)                 │  ← Shard storage horizontally
  │     Each serves a subset of database pages  │
  └──────────────────┬─────────────────────────┘
                     │
  ┌──────────────────▼─────────────────────────┐
  │          Azure Storage (XStore)             │  ← Durable page storage (Azure blob)
  └────────────────────────────────────────────┘
  
  Named replicas (read replicas):
    Up to 5 additional compute replicas (scale out reads)
    Same Log Service + Page Servers (no data copy per replica)
    Adding a read replica: minutes (not hours) — no data movement

Key properties:
  Scale up compute: <2 minutes (vs 30-60 min traditional SQL DB)
  Add storage: automatic (grows on demand up to 100TB, no downtime)
  Add read replica: ~5 minutes (no data copy — shares page servers)
  Backup: continuous log backup → instant PITR (no full backup window)
  Restore: fast — restores log from Log Service (not a full backup restore)

When to use Hyperscale:
  Database > 4TB (only option above that)
  Need to scale compute independently without storage-compute coupling
  Need 5 read replicas for read-heavy workloads
  Rapid scale-up for peak periods (minutes not hours)
  Fast PITR for operational recovery
```

---

## Managed Instance: Enterprise Migration Features

```sql
-- Managed Instance supports nearly 100% of SQL Server surface area
-- Key features missing from SQL Database but present in MI:

-- 1. SQL Server Agent (scheduled jobs)
USE msdb;
EXEC sp_add_job
    @job_name = N'Daily_Stats_Update',
    @enabled = 1;

EXEC sp_add_jobstep
    @job_name = N'Daily_Stats_Update',
    @step_name = N'Update Statistics',
    @command = N'UPDATE STATISTICS dbo.orders WITH FULLSCAN';

EXEC sp_add_schedule
    @schedule_name = N'Daily_3AM',
    @freq_type = 4,           -- daily
    @freq_interval = 1,
    @active_start_time = 030000;   -- 3:00 AM

EXEC sp_attach_schedule
    @job_name = N'Daily_Stats_Update',
    @schedule_name = N'Daily_3AM';

-- 2. Cross-database queries (impossible in SQL Database)
USE orders_db;
SELECT o.*, c.customer_name
FROM dbo.orders o
JOIN customers_db.dbo.customers c ON o.customer_id = c.customer_id;
-- Works on Managed Instance, fails on SQL Database

-- 3. Linked Servers (connect to on-prem or other SQL Servers)
EXEC sp_addlinkedserver
    @server = N'ONPREM_SQL',
    @srvproduct = N'SQL Server';

SELECT * FROM ONPREM_SQL.legacy_db.dbo.products;

-- 4. CLR integration
-- Import custom .NET assemblies (custom aggregates, functions)
CREATE ASSEMBLY MyCustomAgg FROM 0x4D5A...  -- assembly bytes
CREATE AGGREGATE dbo.MedianAggregate (@value DECIMAL(18,2)) RETURNS DECIMAL(18,2)
EXTERNAL NAME MyCustomAgg.[MyNamespace.MedianAggregate];

-- 5. Service Broker (async message passing between databases)
CREATE MESSAGE TYPE OrderCreated VALIDATION = NONE;
CREATE CONTRACT OrderContracts (OrderCreated SENT BY INITIATOR);
CREATE QUEUE OrderQueue;
CREATE SERVICE OrderService ON QUEUE OrderQueue (OrderContracts);

-- Migration assessment:
-- Use: Azure Database Migration Service (DMS) Assessment tool
-- Output: compatibility report showing which features are used and if MI supports them
-- Typical result: 95%+ of on-prem SQL Server features supported in MI
```

---

## Advanced Security: Always Encrypted and Ledger

```sql
-- Always Encrypted: data encrypted client-side, server never sees plaintext
-- SQL Server stores only ciphertext — even DBAs cannot read the data

-- Setup:
-- 1. Generate Column Master Key (CMK) in Azure Key Vault
-- 2. Generate Column Encryption Key (CEK) encrypted with CMK
-- 3. Create table with encrypted columns

CREATE TABLE dbo.sensitive_customers (
    customer_id         INT PRIMARY KEY,
    name                VARCHAR(100),
    -- Deterministic encryption: same plaintext → same ciphertext (allows equality search)
    ssn                 CHAR(11)     ENCRYPTED WITH (
        COLUMN_ENCRYPTION_KEY = CEK_AzureKeyVault,
        ENCRYPTION_TYPE = DETERMINISTIC,
        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'
    ),
    -- Randomized encryption: same plaintext → different ciphertext (no equality search)
    credit_card         VARCHAR(20)  ENCRYPTED WITH (
        COLUMN_ENCRYPTION_KEY = CEK_AzureKeyVault,
        ENCRYPTION_TYPE = RANDOMIZED,
        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'
    )
);

-- Application must: use Always Encrypted-enabled driver + have access to AKV
-- connection string: Column Encryption Setting=Enabled
-- Driver encrypts before sending to SQL, decrypts after receiving

-- Ledger Tables: tamper-evident append history
-- Every row modification recorded in an append-only ledger table
-- Cryptographic proof: SHA-256 hash chain, stored in Azure Confidential Ledger

-- Updatable ledger table (tracks all changes):
CREATE TABLE dbo.employee_salaries (
    emp_id      INT PRIMARY KEY,
    salary      DECIMAL(10,2),
    modified_by VARCHAR(100)
)
WITH (SYSTEM_VERSIONING = ON, LEDGER = ON);

-- View history:
SELECT * FROM dbo.employee_salaries FOR SYSTEM_TIME ALL;
-- Includes: [ledger_start_transaction_id], [ledger_end_transaction_id], all historical values

-- Verify ledger integrity (detect tampering):
EXEC sp_verify_database_ledger;  -- returns error if any tampering detected
```

---

## Performance Monitoring and Intelligent Insights

```sql
-- Built-in performance diagnostics (Azure SQL DB)

-- 1. sys.dm_exec_query_stats: top resource consumers
SELECT TOP 20
    qs.total_worker_time / qs.execution_count AS avg_cpu_microseconds,
    qs.total_elapsed_time / qs.execution_count AS avg_duration_microseconds,
    qs.execution_count,
    SUBSTRING(qt.text, qs.statement_start_offset/2 + 1,
        (CASE WHEN qs.statement_end_offset = -1
            THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2
            ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY avg_cpu_microseconds DESC;

-- 2. Missing index recommendations (automatic):
SELECT
    CONVERT(DECIMAL(18,2), migs.avg_total_user_cost *
        (migs.avg_user_impact / 100.0) *
        (migs.user_seeks + migs.user_scans)) AS improvement_measure,
    mid.statement AS table_name,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,
    'CREATE INDEX IX_' + REPLACE(mid.statement, '.', '_') +
        ' ON ' + mid.statement +
        '(' + mid.equality_columns + ')' +
        ISNULL(' INCLUDE (' + mid.included_columns + ')', '') AS create_index_statement
FROM sys.dm_db_missing_index_groups mig
JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
ORDER BY improvement_measure DESC;

-- 3. Intelligent Insights (Azure Monitor):
-- Azure SQL DB automatically detects: excessive wait stats, degrading plans, blocking
-- Diagnostics log → Log Analytics:
-- AzureDiagnostics | where Category == "SQLInsights"
-- | where Resource == "ORDERS-DB"
-- | where RootCauseAnalysis contains "PlanRegression"
```

---

## Interview Tips

> **Tip 1:** "Explain the Hyperscale architecture and why it solves the traditional SQL Database scaling problem." — Traditional SQL DB stores data and compute together — to get more storage you scale up compute (and pay for both). Hyperscale separates storage (page servers), logging (log service), and compute (compute replicas). Storage grows automatically (up to 100TB) without any compute changes. Adding a read replica takes 5 minutes because the new compute node connects to the same page servers — no data copy. This is why "scale-out read replicas" are fast and cheap in Hyperscale vs traditional replicas.

> **Tip 2:** "What is Always Encrypted and what SQL features does it prevent?" — Always Encrypted encrypts column data in the client application using keys stored in Azure Key Vault (or local cert store). The SQL Server engine only ever sees ciphertext — not even DBAs can read the values. This prevents: privileged user attacks (DBAs reading sensitive columns), data exposure from SQL injection (returned data is ciphertext), and cloud provider data access (Microsoft cannot read Always Encrypted columns). Limitations: no LIKE/range queries on randomized-encrypted columns, no ORDER BY/GROUP BY, no aggregates on encrypted columns. Only equality search works on deterministic encryption.

> **Tip 3:** "How would you migrate a 5TB on-premises SQL Server database to Azure with minimal downtime?" — Use Azure Database Migration Service (DMS) in online mode: (1) Assessment: run DMS assessment on source to identify compatibility issues (SQL MI is best for minimal changes). (2) Initial load: DMS migrates full backup to MI (hours). (3) Online sync: DMS uses SQL Server log shipping or CDC to continuously sync changes from on-prem to MI (near-zero lag). (4) Cutover: schedule maintenance window (1-5 min), let DMS complete final sync, point applications to MI endpoint, verify. Total downtime: 1-5 minutes (just the DNS/connection string switch). This is Azure's recommended migration approach for large databases.
