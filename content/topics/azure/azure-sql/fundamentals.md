---
title: "Azure SQL & Managed Instance — Fundamentals"
topic: azure
subtopic: azure-sql
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, azure-sql, managed-instance, relational, oltp, sql-server]
---

# Azure SQL & Managed Instance — Fundamentals


## 🎯 Analogy

Think of Azure SQL like SQL Server managed by Microsoft: automatic backups, patching, high availability, and scaling — you just connect with your SQL client and run queries without being a DBA.

---
## Azure SQL Family Overview

```
Azure SQL is a family of managed relational database services built on SQL Server:

1. Azure SQL Database (PaaS — Platform as a Service)
   Hyperscale of a single database
   Always latest SQL Server features
   Best for: new cloud applications, microservices, SaaS
   Deployment: single database or elastic pool

2. Azure SQL Managed Instance (PaaS — near 100% SQL Server compatibility)
   Near-complete SQL Server engine (SQL Agent, cross-database queries, CLR, etc.)
   Drop-in replacement for on-premises SQL Server
   Best for: lift-and-shift migrations with minimal code changes
   VNet-injected (always runs inside your VNet)

3. SQL Server on Azure VM (IaaS — Infrastructure as a Service)
   Full control over SQL Server + OS
   Best for: features not in PaaS (e.g., Windows authentication, FCI, AG)
   You manage: OS patching, backups, HA

Key differences:
                    SQL Database     Managed Instance    SQL on VM
SQL compatibility:  ~95%             ~99%                100%
Managed:           Fully managed    Fully managed       You manage OS
Network:           Public/Private   VNet always         VNet/Public
Cross-DB queries:  No               Yes                 Yes
SQL Agent:         No               Yes                 Yes
Price:             $ (entry $5/mo)  $$ ($0.40+/hr)      $-$$$ (VM cost)
```

---

## Azure SQL Database Purchasing Models

```
DTU Model (legacy, simpler):
  Bundled CPU + memory + I/O as one unit
  Tiers: Basic (5 DTU, $5/mo), Standard (10-3000 DTU), Premium (125-4000 DTU)
  Easy to understand, limited flexibility
  Use for: simple apps, dev/test, predictable workloads

vCore Model (recommended):
  Separate vCPU + memory from storage
  Compute tiers:
    General Purpose:  1-80 vCores, remote SSD, 5.1GB/vCore memory
    Business Critical: 1-80 vCores, local NVMe SSD, faster I/O, built-in read replica
    Hyperscale:       1-80 vCores, cloud-native storage (100TB+, fast scale-out)

  Hardware generations:
    Standard-series (Gen5): most common
    Fsv2 series: compute-optimized (high CPU to memory ratio)
    DC series: confidential computing (encrypted in use)

  Azure Hybrid Benefit (AHB):
    Apply existing SQL Server licenses → 40% cost reduction
    Critical for migrations from on-premises (existing licenses qualify)

Serverless (vCore model — SQL Database only):
  Auto-pause after 1 hour idle → $0 compute cost when paused
  Auto-scale: min 0.5 vCore to max N vCores based on load
  Billed per second of actual compute usage
  Use for: dev/test, sporadic workloads, cost-sensitive non-critical DBs
  Not for: always-on production (cold start ~60 sec after pause)
```

---

## High Availability and Business Continuity

```
Built-in HA (all tiers):
  General Purpose: 3 copies of data in Azure Premium Storage (Log + Data)
    Compute can fail → restart on another node (data in storage, ~30 sec)
  Business Critical: 4-node AlwaysOn Availability Group (3 primary + 1 readable secondary)
    Node failure → automatic failover within the AG (~5-10 sec)
  
Backup:
  Full backup: weekly (auto)
  Differential: every 12-24 hours (auto)
  Log backup: every 5-10 minutes (auto)
  Retention: 1-35 days (configurable) for PITR
  Long-term retention (LTR): archive weekly/monthly backups to Blob for years

Point-in-Time Restore (PITR):
  Restore to any point in last 1-35 days
  Restores to a NEW database (not in-place)
  Portal: SQL Database → Restore → select point in time

Active Geo-Replication (SQL Database):
  Up to 4 readable secondary replicas in different regions
  Async replication (minutes RPO)
  Failover: manual or via Auto-failover group
  Use for: multi-region reads + disaster recovery

Auto-failover groups:
  Logical group of databases with unified endpoint
  Primary endpoint: for read-write (auto-routes to primary region)
  Secondary endpoint: for read-only (always routes to secondary)
  On failover: endpoints auto-redirect (application doesn't change connection string)
```

---

## Common Data Engineering Uses

```
Azure SQL DB as data engineering target/source:

Source patterns:
  ADF Copy Activity: Azure SQL as source for incremental loads
    SELECT * FROM orders WHERE updated_at > @watermark
  Debezium CDC: SQL Server CDC → Event Hubs → Bronze Delta
  Synapse Link: Azure SQL DB → Synapse analytical replica (zero-ETL, sub-minute lag)

Sink patterns:
  Synapse COPY INTO staging → production table
  ADF Copy Activity: write Gold aggregates to SQL for Power BI DirectQuery
  Databricks write: spark.write.format("jdbc").option("url", ...)

Serving layer for BI:
  Gold Delta (ADLS) → ADF → Azure SQL DB → Power BI (Import mode)
  Power BI: use Import mode for best performance (data in Power BI memory)
  Or: Direct Query to SQL DB (slower but always current)

Important SQL DB limits for data engineering:
  Max database size: 4TB (General Purpose), 100TB (Hyperscale)
  Max rows: no hard limit (depends on disk)
  Max columns: 1,024 per table
  Max batch insert: limited by transaction log (batch in chunks of 10K-100K rows)
  Connection pooling: use ADO.NET connection pool, max 30K connections per server
```

---


## ▶️ Try It Yourself

```python
import pyodbc
import os

# Connect to Azure SQL
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']}.database.windows.net;"
    f"DATABASE={os.environ['AZURE_SQL_DB']};"
    f"Authentication=ActiveDirectoryServicePrincipal;"
    f"UID={os.environ['AZURE_CLIENT_ID']};"
    f"PWD={os.environ['AZURE_CLIENT_SECRET']};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

cursor = conn.cursor()
cursor.execute("SELECT TOP 5 order_id, amount FROM dbo.orders ORDER BY order_date DESC")
for row in cursor.fetchall():
    print(row.order_id, row.amount)
conn.close()
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between Azure SQL Database and Azure SQL Managed Instance?" — SQL Database is a fully managed, cloud-native relational database optimized for new applications. It lacks some SQL Server features: no SQL Agent (scheduled jobs), no cross-database queries, limited CLR. SQL Managed Instance has ~99% SQL Server compatibility — runs SQL Agent, cross-database queries, linked servers, CLR, Service Broker, SSAS. Choose SQL Database for new cloud-native apps. Choose Managed Instance for lift-and-shift migrations where your app relies on SQL Server-specific features that SQL Database doesn't support.

> **Tip 2:** "What is DTU vs vCore pricing and which should you use?" — DTU (Database Transaction Unit) bundles CPU, memory, and I/O into one opaque unit — simpler but less flexible. vCore lets you independently choose CPU count and configure memory. vCore is recommended for: production workloads (right-size compute vs storage separately), applying Azure Hybrid Benefit (40% discount using existing SQL Server licenses), and Hyperscale (only available in vCore). DTU is acceptable for: small dev/test databases, simple apps where you just need a "small/medium/large" tier.

> **Tip 3:** "How does Azure SQL Database handle automatic backups differently from SQL Server on-premises?" — On-premises: you schedule and manage backups yourself (SQL Agent jobs, backup to disk/tape). Azure SQL DB: automatic full + differential + log backups managed by Azure. Retention configurable from 1 to 35 days for PITR. For longer retention: configure Long-Term Retention (LTR) to archive backups to Blob Storage (years). No backup management overhead — Azure handles the entire backup lifecycle. The limitation: you can't control backup timing or access the raw .bak files (restore via portal/API only).
