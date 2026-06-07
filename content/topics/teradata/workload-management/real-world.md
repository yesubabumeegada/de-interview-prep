---
title: "Teradata - Workload Management Real World"
topic: teradata
subtopic: workload-management
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, workload-management, tasm, production, sla, incident-response]
---

# Workload Management — Real World

## Case Study: SLA Breach Investigation at a Retail Bank

**Company:** Top-5 US retail bank, Teradata DW powering ATM authorization lookups.

**Incident:** ATM authorization response times spiked from 800ms to 8 seconds for 45 minutes on a Tuesday morning. 12,000 ATM transactions timed out. Significant customer impact.

**Root cause investigation:**

```sql
-- DBQL analysis of the incident window (8:15–9:00 AM)
SELECT WorkloadName, AVG(ElapsedTime) AS AvgElapsed,
       COUNT(*) AS QueryCount
FROM DBC.QryLogV
WHERE LogDate = '2024-03-12'
  AND LogTime BETWEEN TIME '08:15:00' AND TIME '09:00:00'
GROUP BY WorkloadName
ORDER BY AvgElapsed DESC;
```

Result:
```
TACTICAL_ATM:    avg elapsed = 8.3 seconds  (SLA: 1 second)
ANALYTICS:       avg elapsed = 45 seconds
BATCH_LOAD:      avg elapsed = 120 seconds
```

```sql
-- What was consuming CPU during the incident?
SELECT UserName, AMPCPUTime, QueryText
FROM DBC.QryLogV
WHERE LogDate = '2024-03-12'
  AND LogTime BETWEEN TIME '08:00:00' AND TIME '08:30:00'
  AND AMPCPUTime > 1000
ORDER BY AMPCPUTime DESC;
```

**Finding:** A data science team had scheduled a new ML feature preparation job — a massive GROUP BY on 10 billion rows, classified incorrectly as ANALYTICS (MEDIUM priority, throttle 30 concurrent). At 8 AM, 30 instances of this job fired simultaneously.

30 × 1000 CPU seconds each = 30,000 CPU seconds consumed → ALL AMPs saturated → tactical ATM queries starved.

**Immediate fix (during incident):**
```sql
-- Cancel the offending jobs via Viewpoint
-- Viewpoint: Sessions → Filter by WorkloadName = ANALYTICS → Sort by CPU → Cancel top 30
```

ATM response times returned to 800ms within 2 minutes of cancellation.

**Root cause:** No IWM rule to reclassify heavy analytics queries. No Active State to throttle ANALYTICS when TACTICAL SLA was under threat.

**Permanent fixes:**
1. IWM rule: Analytics queries consuming > 2000 CPU seconds → demote to LOW priority
2. Active State: "CRITICAL" triggered when TACTICAL avg elapsed > 2 seconds → ANALYTICS throttle reduced to 5 concurrent, BATCH throttle to 2
3. Query band requirement: All new batch/analytics jobs must set `JobName=` query band → enables targeted cancellation
4. ML team: Job rescheduled to 2 AM, classified as BATCH (LOW priority, throttle 5)

---

## Production TASM Configuration (Sanitized)

A large insurance company's TASM setup:

```
Active State: PLANNED (normal operation)
  Workloads:
    TACTICAL_API (SLG):
      Priority: SLG | Throttle: 200 concurrent
      Trigger: Account='WEB_API' OR QueryBand LIKE '%App=CustomerPortal%'
      Response goal: 2 seconds (95th percentile)

    ANALYST_TEAM (MEDIUM):
      Priority: MEDIUM | Throttle: 40 concurrent
      Trigger: User IN (analyst_team members)
      No response goal

    ETL_BATCH (LOW):
      Priority: LOW | Throttle: 8 concurrent
      Trigger: User = ETL_SERVICE_ACCOUNT
      Must complete by 6 AM (enforced via scheduler)

    HEAVY_REPORTS (LOW):
      Priority: LOW | Throttle: 3 concurrent
      Trigger: EstimatedCPU > 3600 OR ElapsedTime > 300 (IWM reclassification)
      Any query exceeding 5 minutes → reclassified here

Active State: BUSY (CPU > 75% for 5 min)
  Override:
    ETL_BATCH: Throttle reduced to 3
    ANALYST_TEAM: Priority reduced to LOW
    TACTICAL_API: Unchanged (protected)

Active State: SURGE (CPU > 90% for 3 min)
  Override:
    ETL_BATCH: Throttle reduced to 1
    ANALYST_TEAM: Throttle reduced to 10
    HEAVY_REPORTS: SUSPENDED
    TACTICAL_API: Unchanged (protected)
```

---

## DBQL-Based Chargeback Reporting

Many enterprises use DBQL for **chargeback** — billing internal departments for their Teradata usage:

```sql
-- Monthly chargeback report by department
SELECT
    UserName,
    COALESCE(u.CostCenter, 'UNKNOWN') AS Department,
    COUNT(*) AS QueryCount,
    SUM(q.AMPCPUTime) AS TotalCPUSec,
    SUM(q.TotalIOCount) AS TotalIO,
    SUM(q.AMPCPUTime) * 0.15 AS EstimatedCost  -- $0.15 per CPU second (example rate)
FROM DBC.QryLogV q
LEFT JOIN corp_users u ON q.UserName = u.TeradataUser
WHERE q.LogDate BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY UserName, Department
ORDER BY TotalCPUSec DESC;
```

This drives accountability — teams that run expensive queries pay more, incentivizing optimization.

---

## Viewpoint: Production Monitoring Dashboards

Standard Teradata Viewpoint dashboards in production:

**Dashboard 1: System Health (always-on, NOC screen)**
- AMP CPU utilization heat map (60-second refresh)
- Active sessions by workload
- Query queue depth by workload
- Alert when tactical SLA compliance < 95%

**Dashboard 2: Daily Review (morning ops review)**
- Top 20 queries by CPU (previous day)
- Workload CPU breakdown by hour
- Queries that were delayed (throttle wait > 30 seconds)
- Users with unusually high resource consumption

**Dashboard 3: Trend Analysis (weekly)**
- Week-over-week CPU growth by workload
- Peak concurrent query trends
- SLA compliance rates over time
- Forecast: at current growth rate, when will we need more AMPs?

---

## Interview Tips

> **Tip 1:** "Describe a workload management incident you've handled or would handle." — "Describe the SLA breach pattern: tactical queries slow, investigate via DBQL, find a batch/analytics job spiking CPU. Immediate response: cancel or throttle the offending workload via Viewpoint. Permanent fix: IWM rules for reclassification, Active States for dynamic response, query band requirements for attribution."

> **Tip 2:** "How do you design TASM rules for a mixed tactical/batch Teradata system?" — "Three-tier workload model: SLG for tactical (protected, no throttle reduction under any state), MEDIUM for analytics (throttle adjustable via Active States), LOW for batch (throttle reduced first when system is busy). Define Active States that kick in automatically when CPU exceeds thresholds, reducing batch and analytics throttles to protect tactical SLA."

> **Tip 3:** "How do you use DBQL for workload chargeback?" — "DBQL captures AMPCPUTime, TotalIOCount, and SpoolUsage per query. Join with user metadata to get department/team. Multiply CPU seconds by a fixed rate to produce an estimated cost. This gives leadership visibility into which teams consume the most Teradata resources and creates optimization incentives."

> **Tip 4:** "How does TASM's Active State mechanism work?" — "Active States are TASM's dynamic adaptation layer. Pre-configured triggers (e.g., AMP CPU > 80% for 10 minutes) automatically transition the system to a different Active State with different workload rules. The system reverts when the trigger condition clears. This gives hands-free response to load spikes — protecting tactical SLAs without human intervention."
