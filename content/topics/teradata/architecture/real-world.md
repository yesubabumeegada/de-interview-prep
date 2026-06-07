---
title: "Teradata - Architecture Real World"
topic: teradata
subtopic: architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, architecture, production, enterprise, banking, retail, telco]
---

# Teradata Architecture — Real World

## Enterprise Deployment Patterns

### Banking: JPMorgan Chase (Multi-System Federation)
Large financial institutions run **multiple Teradata systems** in parallel:
- **Risk system:** dedicated Teradata for real-time risk calculation (tactical workloads, SLA < 2 seconds)
- **Reporting warehouse:** separate Teradata for overnight batch reporting (strategic workloads)
- **QueryGrid** bridges the two — risk reports can JOIN across systems without ETL

**Architecture pattern:**
```
Risk DB (small, fast, NVMe) → QueryGrid → Enterprise DW (large, archival HDDs)
```

This avoids the classic problem where 8-hour batch reports block 1-second risk queries.

---

### Retail: Walmart's Data Warehouse

Walmart ran one of the world's largest Teradata installations (reported at 2.5+ petabytes). Key design decisions:
- **Separate AMP pools** for different departments (merchandising, logistics, finance)
- **TASM workload management** to prioritize POS (point-of-sale) updates over analyst queries during store hours
- **Fallback enabled** on all fact tables; disabled on staging/work tables to save space
- **PPI on sales fact table** partitioned by sale_date to enable partition elimination for date-range queries

**Result:** Buyers could query yesterday's sales across 10,000 stores in seconds.

---

### Telco: AT&T CDR Processing

Telecom companies process billions of **Call Detail Records (CDRs)** daily:
- CDRs land in staging tables via **FastLoad** (empty table, high throughput)
- After load: INSERT/SELECT into the fact table with proper PI (e.g., subscriber_id)
- The fact table PI = subscriber_id (high cardinality, frequently joined with subscriber profile)
- BYNET handles the redistribution during the JOIN — subscriber profile is broadcast (small table)

**Architecture insight:** For a table of 50 billion CDRs, the PI choice determines whether queries are sub-second (AMP-local) or take minutes (full redistribution join).

---

## Migration War Story: Oracle to Teradata

A major insurance company migrated an Oracle-based reporting system to Teradata:

**Challenge:** Oracle queries used `ROWNUM` pagination and Oracle-specific functions.
**Solution process:**
1. Translated all `ROWNUM` → `QUALIFY ROW_NUMBER() OVER (...)`
2. Redesigned table structures: added Primary Indexes matching most common JOIN keys
3. Collected statistics on all dimension tables before going live
4. Ran parallel execution (Oracle + Teradata) for 4 weeks to validate result parity

**Lesson:** The biggest performance win wasn't moving to MPP — it was being *forced* to define Primary Indexes, which made the team think carefully about access patterns for the first time.

---

## Production Operational Patterns

### Node Failure Response Playbook
1. Alert fires: AMP vproc marked offline in DBC.AMPUsage
2. Queries auto-failover to Fallback copies — users see no interruption
3. Hot standby absorbs the failed vproc within 30–60 seconds
4. Operations team investigates root cause (disk failure, memory ECC error, kernel panic)
5. Failed node repaired or replaced
6. AMP vproc migrated back from standby (background process, no downtime)

### Capacity Planning
Production Teradata sizing considers:
- **Raw data size × 2** (fallback) × 1.3 (overhead/indexes) = permanent space needed
- **Spool space** = 2–3× typical query intermediate result size per user × concurrent user count
- **Temp space** = based on Global Temporary Table usage patterns

---

## Common Architecture Anti-Patterns (and Fixes)

### Anti-Pattern 1: Single PI for Everything
**Symptom:** DBA uses `account_id` as PI for all tables because "it's what we join on most."
**Problem:** If `account_id` is low-cardinality (e.g., only 100 distinct values across 200 AMPs), 100 AMPs sit idle.
**Fix:** Composite PI (account_id + transaction_date) for better distribution.

### Anti-Pattern 2: No Fallback on Production Tables
**Symptom:** Team disabled Fallback to save storage on a 10 TB fact table.
**Problem:** Single disk failure causes partial table loss — no recovery without backup restore (hours of downtime).
**Fix:** Always enable Fallback on production tables. Use RAID + Fallback.

### Anti-Pattern 3: All Workloads on One System
**Symptom:** ETL jobs and analyst queries compete for the same AMPs.
**Problem:** A heavy nightly ETL degrades analyst query response from 5 seconds to 5 minutes.
**Fix:** TASM workload separation (tactical vs strategic priority), or separate Teradata systems bridged by QueryGrid.

---

## Monitoring Architecture Health

```sql
-- Check AMP CPU skew (high skew = uneven distribution)
SELECT  
    NodeID,
    AMPNumber,
    CPUSeconds,
    100.0 * (CPUSeconds - AVG(CPUSeconds) OVER ()) / AVG(CPUSeconds) OVER () AS SkewPct
FROM DBC.AMPUsageV
ORDER BY CPUSeconds DESC;

-- Check spool usage by user
SELECT UserName, SUM(CurrentSpool) AS SpoolUsedMB
FROM DBC.SessionInfoV
GROUP BY UserName
ORDER BY SpoolUsedMB DESC;
```

---

## Interview Tips

> **Tip 1:** "How would you architect Teradata for mixed tactical and strategic workloads?" — "Separate the workloads using TASM — assign priority classes so tactical queries (SLA < 2s) preempt strategic (batch) queries. For heavy separation, use dedicated Teradata systems connected by QueryGrid."

> **Tip 2:** "How do you handle a Teradata node failure in production?" — "Fallback serves reads immediately. Hot standby absorbs the failed vproc within minutes. Operations investigates root cause. Users see zero downtime if Fallback is enabled and the standby is pre-configured."

> **Tip 3:** "What's the most important design decision when building a new Teradata table?" — "The Primary Index. It determines data distribution across AMPs, which determines whether queries are AMP-local (fast) or require redistribution (slow). A bad PI causes skew and forces all-AMP operations."

> **Tip 4:** "How do large enterprises use multiple Teradata systems?" — "Separate systems for different workload types (risk, reporting, EDW) connected via QueryGrid for federated queries. This prevents heavy batch workloads from impacting SLA-bound tactical queries."
