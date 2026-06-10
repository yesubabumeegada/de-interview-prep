---
title: "Teradata - Workload Management Scenarios"
topic: teradata
subtopic: workload-management
content_type: scenario_question
difficulty_level: senior
layer: scenarios
tags: [teradata, workload-management, scenarios, tasm, sla, dbql]
---

# Workload Management — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Explaining Priority Tiers

A junior analyst complains: "I submitted a query 20 minutes ago and it's still running. My colleague submitted a similar query 5 minutes ago and it finished in 30 seconds. Why?"

You look at the system and see the analyst is in the MEDIUM workload (analytics) and the colleague is in the SLG workload (tactical API).

Explain why this happens and whether it's the intended behavior.

<details>
<summary>💡 Hint</summary>

Think about TASM priority tiers and what happens when the system is busy. Is the system at high CPU utilization? What does the throttle queue mean?

</details>

<details>
<summary>✅ Solution</summary>

**What's happening:**

1. **Priority difference:** The colleague's SLG workload has the highest TASM priority. When AMP CPUs are available, SLG queries are served first. The analyst's MEDIUM workload waits for CPU time after all SLG requests are satisfied.

2. **Throttle queueing:** The MEDIUM workload may have hit its concurrent query throttle limit (e.g., 30 concurrent). The analyst's query is waiting in queue for a slot — not running at all yet. Once an ANALYTICS slot opens, it will start.

3. **Query size difference:** Even if priority were equal, a complex analytics query (GROUP BY on billions of rows) takes longer than a tactical single-row lookup. The queries aren't "similar" in terms of work done.

**Is this the intended behavior? Yes:**
- The system is designed to prioritize SLG queries — they have SLA requirements (e.g., ATM authorization must respond in < 1 second)
- An analyst query taking 20 minutes is acceptable; an ATM query taking 20 minutes is a customer impact event

**What the analyst can do:**
- Break large queries into smaller incremental steps
- Run during off-peak hours (early morning or evening) when system load is lower
- Talk to the DBA team if queries routinely take longer than 30 minutes — they may need statistics collection or query tuning
- Don't use MEDIUM workload for clearly heavy work — use BATCH or HEAVY_REPORTS workload instead

**What the DBA can check:**
```sql
-- Is the analyst's query waiting in queue or running slowly?
SELECT SessionNo, State, UserName, WorkloadName, DelayTime, AMPCPUTime
FROM DBC.SessionInfoV
WHERE UserName = 'ANALYST_USER_NAME';
-- DelayTime > 0 means waiting in throttle queue
-- AMPCPUTime growing means actively running but complex
```

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: TASM Configuration for a New System

You are setting up TASM workload management for a new Teradata system that will serve:
- 100 e-commerce API queries per second with 500ms SLA (order lookups, product searches)
- 50 concurrent analyst users running ad-hoc reports (expect 1-30 minute runtimes)
- 5 nightly ETL jobs loading 50 GB each (must complete by 5 AM)
- System has 256 AMPs, typical daytime load 60%, peak 85%

Define the workload groups, classification rules, priority levels, throttle limits, and Active States.

<details>
<summary>✅ Solution</summary>

**Workload Definitions:**

**1. ECOMMERCE_API (SLG)**
```
Classification: Account = 'ECOM_API' OR QueryBand LIKE '%App=ECommerceAPI%'
Priority: SLG
Throttle: 500 concurrent (API queries are fast, high volume)
Response goal: 500ms (95th percentile)
Delay limit: 100ms (if slot wait exceeds this, escalate)
```

**2. ANALYST_STANDARD (MEDIUM)**
```
Classification: User IN (analyst team, data science team)
                AND EstimatedCPU <= 3600
Priority: MEDIUM
Throttle: 40 concurrent (50 users, ~80% active at peak)
No hard response goal (best effort)
```

**3. ANALYST_HEAVY (LOW)**
```
Classification: EstimatedCPU > 3600 (IWM reclassification from MEDIUM)
                OR ElapsedTime > 1800 (30 minutes runtime → demote)
Priority: LOW
Throttle: 5 concurrent
This catches runaway analyst queries before they impact SLA
```

**4. ETL_BATCH (LOW)**
```
Classification: User = ETL_SERVICE_ACCT
Priority: LOW
Throttle: 8 concurrent (5 jobs, some multi-step = 8 slots)
Must-complete time: 5 AM (enforced by scheduler, not TASM)
```

**Active States:**

```
State: PLANNED (CPU < 70%)
  All rules as above — normal operation

State: BUSY (CPU 70-85% for 10 min)
  Trigger: AMP CPU > 70% sustained
  Overrides:
    ETL_BATCH: Throttle 8 → 4
    ANALYST_STANDARD: Priority MEDIUM → no change, Throttle 40 → 30
    ANALYST_HEAVY: Throttle 5 → 2
    ECOMMERCE_API: No change
  Revert: CPU < 60% for 5 min

State: SURGE (CPU > 85% for 5 min)
  Trigger: AMP CPU > 85% sustained
  Overrides:
    ETL_BATCH: Throttle 8 → 2, Priority LOW
    ANALYST_HEAVY: SUSPENDED
    ANALYST_STANDARD: Throttle 40 → 15, Priority LOW
    ECOMMERCE_API: No change (fully protected)
  Revert: CPU < 75% for 10 min

State: CRITICAL (ECOM_API P95 > 500ms)
  Trigger: Tactical SLA breach detected
  Overrides:
    ETL_BATCH: SUSPENDED
    ANALYST_HEAVY: SUSPENDED
    ANALYST_STANDARD: Throttle 40 → 5, Priority VERY_LOW
    ECOMMERCE_API: Priority escalated, no throttle change
  Revert: ECOM_API P95 < 300ms for 5 min
```

**IWM Rules:**

```
Rule 1: MEDIUM workload query running > 30 min → reclassify to ANALYST_HEAVY
Rule 2: MEDIUM workload query consuming > 5000 CPU seconds → reclassify to ANALYST_HEAVY
Rule 3: ETL_BATCH job consuming > 10,000 CPU seconds → alert DBA (possible runaway)
```

**Monitoring:**
```sql
-- Hourly SLA compliance check
SELECT 
    EXTRACT(HOUR FROM LogTime) AS Hour,
    100.0 * COUNT(CASE WHEN ElapsedTime <= 0.5 THEN 1 END) / COUNT(*) AS SLACompliancePct
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE AND WorkloadName = 'ECOMMERCE_API'
GROUP BY Hour
ORDER BY Hour;
-- Alert if SLACompliancePct < 95% for any hour
```

**Justification for key decisions:**
- SLG for e-commerce: 100 QPS at 500ms = tight SLA, needs preemptive CPU access
- Throttle 500 for API: Fast queries, high volume — throttle must be high to avoid queuing
- ANALYST_HEAVY as separate workload: Isolates long-running queries from standard analytics
- Active States with CRITICAL: Automated response to SLA breach, no manual intervention needed

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Multi-System Workload Management Strategy

Your enterprise is migrating from a single large Teradata system to two systems: one dedicated to tactical (operational) queries and one to strategic (analytics). QueryGrid will federate queries between them.

Design the workload management strategy for both systems and the coordination between them. Consider: how do you handle queries that span both systems, how do you manage cross-system resource contention, and what TASM rules are needed on each system.

<details>
<summary>💡 Hint</summary>

Think about the different SLA requirements per system, how QueryGrid queries appear on each system (as a single session), and how workload rules on each system need to align with the overall business priorities.

</details>

<details>
<summary>✅ Solution</summary>

**System Architecture:**

```
System 1: OPERATIONAL (Tactical)
  - Hot data: last 90 days
  - Use cases: API queries, real-time dashboards, tactical lookups
  - SLA: < 1 second for 95th percentile
  - AMP count: 128 (smaller, faster, NVMe)

System 2: ANALYTICAL (Strategic)
  - Historical data: 1-7 years
  - Use cases: Analyst reports, data science, batch reporting
  - SLA: Best effort (1-60 minutes)
  - AMP count: 512 (larger, HDDs for cost efficiency)

QueryGrid: Federation layer for cross-system queries
```

**TASM Configuration: System 1 (OPERATIONAL)**

```
Workloads:
  TACTICAL_LOCAL (SLG):
    Queries that touch only System 1 data
    Priority: SLG | Throttle: 200 | Goal: 1 second

  QUERYGRID_INITIATOR (MEDIUM):
    Queries from System 2 via QueryGrid that initiate on System 1
    Priority: MEDIUM | Throttle: 20
    These are analytics crossing to operational data — 
    should not impact local tactical SLA

  ETL_LOAD (LOW):
    Nightly loads into hot data tables
    Priority: LOW | Throttle: 5

Active States on System 1:
  BUSY (CPU > 70%):
    QUERYGRID_INITIATOR: Throttle 20 → 5 (protect local tactical first)
  
  SURGE (CPU > 90%):
    QUERYGRID_INITIATOR: SUSPENDED (cross-system queries blocked)
    ETL_LOAD: Throttle 5 → 2
```

**TASM Configuration: System 2 (ANALYTICAL)**

```
Workloads:
  ANALYST_STANDARD (MEDIUM):
    Ad-hoc analyst queries
    Throttle: 50 concurrent

  ANALYST_HEAVY (LOW):
    IWM reclassified long/expensive queries
    Throttle: 10

  BATCH_REPORTING (LOW):
    Nightly batch reports, regulatory submissions
    Throttle: 15

  QUERYGRID_CONSUMER (MEDIUM):
    The "other side" of cross-system queries from System 1
    These originate from System 1 but consume System 2 resources
    Priority: MEDIUM | Throttle: 20

Active States on System 2:
  BUSY (CPU > 75%):
    ANALYST_HEAVY: Throttle 10 → 3
    QUERYGRID_CONSUMER: Throttle 20 → 10

  SURGE (CPU > 90%):
    BATCH_REPORTING: Throttle 15 → 5
    ANALYST_HEAVY: SUSPENDED
    QUERYGRID_CONSUMER: Throttle 20 → 5
```

**QueryGrid Cross-System Query Classification:**

QueryGrid queries appear as a session on each system. The challenge: the same query needs appropriate workload on both systems.

**Solution: Query bands propagated via QueryGrid**

```sql
-- Application sets query band on System 1 (initiating system)
SET QUERY_BAND = 'CrossSystemQuery=YES;OriginSystem=OPERATIONAL;Priority=MEDIUM;' FOR SESSION;

-- QueryGrid propagates the query band to System 2 (consuming system)
-- System 2's TASM rules classify based on the propagated query band:
-- If CrossSystemQuery=YES AND Priority=MEDIUM → QUERYGRID_CONSUMER workload
```

**Resource Governance for Cross-System Queries:**

Cross-system queries consume resources on BOTH systems. Governance:

1. **Total resource budget:** A cross-system query's total CPU budget = System 1 budget + System 2 budget. IWM on each system enforces its local portion.

2. **System 1 protection:** When System 1 is in SURGE state, System 2 should not be allowed to pull additional data from System 1. This requires coordination:
   - System 1 SURGE → System 1's QUERYGRID_INITIATOR workload is suspended
   - All new QueryGrid queries from System 2 to System 1 are queued or rejected
   - In-flight cross-system queries continue (mid-query suspension is disruptive)

3. **QueryGrid timeout configuration:** Set reasonable timeouts on cross-system queries to prevent them from holding resources on both systems indefinitely during contention.

**Monitoring Strategy:**

```sql
-- System 1: Monitor impact of QueryGrid queries on tactical SLA
SELECT
    CASE WHEN WorkloadName LIKE '%QUERYGRID%' THEN 'QUERYGRID'
         ELSE 'LOCAL' END AS QueryOrigin,
    AVG(AMPCPUTime) AS AvgCPU,
    COUNT(*) AS QueryCount,
    AVG(CASE WHEN WorkloadName = 'TACTICAL_LOCAL' THEN ElapsedTime END) AS TacticalAvgElapsed
FROM DBC.QryLogV
WHERE LogDate = CURRENT_DATE - 1
GROUP BY QueryOrigin
ORDER BY AvgCPU DESC;
-- High QueryGrid CPU correlated with tactical SLA degradation = tighten QueryGrid throttle
```

```sql
-- System 2: Cross-system query performance
SELECT AVG(ElapsedTime) AS AvgCrossSystemElapsed,
       MAX(ElapsedTime) AS MaxCrossSystemElapsed
FROM DBC.QryLogV
WHERE WorkloadName = 'QUERYGRID_CONSUMER'
  AND LogDate = CURRENT_DATE - 1;
```

**Key design principles to articulate:**

1. **Dedicated systems = cleaner TASM** — each system has a clear primary workload type; cross-system is secondary
2. **QueryGrid is not free** — it consumes resources on both ends; governance must account for both
3. **Query band propagation** enables end-to-end workload classification across systems
4. **Active States must coordinate** — System 1 surge should restrict System 2's ability to initiate cross-system queries
5. **Monitoring is cross-system** — SLA analysis must join data from both systems' DBQL tables (possible via QueryGrid itself)

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Teradata Active System Management (TASM) and what does it control?**
A: TASM is Teradata's workload management framework that governs how queries are classified, prioritized, throttled, and allocated system resources. It enables concurrent workloads (tactical OLTP queries, large batch ETL, ad hoc analytics) to coexist on the same system without any single workload monopolizing resources.

**Q: What is a workload definition in TASM?**
A: A workload definition classifies queries based on attributes (user, account string, application, query complexity, estimated resource usage) and assigns them to a workload group with specific resource guarantees and limits. Workloads can be prioritized, throttled (limited concurrency), or mapped to different resource partitions.

**Q: What is a throttle in Teradata workload management?**
A: A throttle limits the number of queries of a given type that can run concurrently. For example, a throttle of 5 on large batch queries means at most 5 such queries run simultaneously; additional queries queue until a slot is available. Throttles prevent resource exhaustion from runaway queries or unexpected load spikes.

**Q: What is the difference between TASM and Priority Scheduler?**
A: Priority Scheduler is Teradata's foundational resource allocation layer—it assigns CPU and I/O priorities to workloads. TASM (Teradata Active System Management) extends this with dynamic classification, throttling, workload balancing, and system-state-based rule changes (e.g., apply different rules during peak hours). TASM requires the Teradata Workload Management license; Priority Scheduler is available in all editions.

**Q: What is a system state in TASM and how is it used?**
A: A system state represents a named operational condition of the Teradata system (e.g., "peak_hours," "batch_window," "maintenance"). TASM can switch between system states automatically (based on time of day or resource thresholds) or manually, applying different workload rules for each state. This enables workload management rules to adapt to changing operational contexts.

**Q: What is account string manipulation in Teradata and how does it relate to workload classification?**
A: Teradata users can include an account string in their session (e.g., via `.SET ACCOUNT` in BTEQ) that TASM uses to classify the query. Embedding priority codes in account strings (a legacy approach) allows workload classification without TASM. Modern TASM uses richer classification criteria but account strings remain important for compatibility with older pipelines.

**Q: What are the resource partition pools in Teradata workload management?**
A: Teradata resources (CPU, memory, I/O bandwidth) can be divided into partitions allocated to different workload classes. For example, a "tactical" partition gets 20% of CPU for high-priority short queries, while a "batch" partition gets 70% for large ETL jobs, with 10% reserved for administrative work. This guarantees service levels for high-priority workloads even under load.

**Q: How do you diagnose a workload management problem in Teradata?**
A: Use Teradata Viewpoint to examine active session concurrency, queue lengths, and workload throttle utilization. Check DBQLOGTBL (query logging) for average response times by workload class and periods of high queuing. If tactical queries are being delayed, check whether batch throttles are saturated or whether batch jobs are consuming excessive resources.

---

## 💼 Interview Tips

- Frame workload management as a business continuity concern, not just a technical configuration. When batch ETL and user-facing dashboards compete for the same resources, TASM is what prevents one from starving the other. This framing resonates with senior interviewers.
- Know the classification-throttle-prioritization chain: TASM first classifies the query, then applies throttles (concurrency limits), then the Priority Scheduler governs CPU/I/O allocation within running workloads. These are three distinct mechanisms often confused.
- Be ready to discuss a scenario: "How would you configure TASM to ensure tactical queries (< 1 second) always get fast response even during batch loads?" The answer involves dedicated resource partitions, strict throttles on batch, and system states for peak vs. off-peak.
- Mention that incorrect workload classification is a common production problem—a single misconfigured account string can route a heavy batch job into the tactical workload, starving real-time users. Show you'd build monitoring to detect classification anomalies.
- Senior interviewers at large Teradata installations will probe your experience with Viewpoint for workload monitoring. Knowing the specific Viewpoint portlets (Workload Monitor, Productivity) for diagnosing TASM issues shows hands-on operations experience.
