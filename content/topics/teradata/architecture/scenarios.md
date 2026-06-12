---
title: "Teradata - Architecture Scenarios"
topic: teradata
subtopic: architecture
content_type: scenario_question
difficulty_level: senior
tags: [teradata, architecture, scenarios, mpp, amps, bynet, fallback]
---

# Teradata Architecture — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: New Team Member Explanation

You've joined a data engineering team that uses Teradata. A junior analyst asks: *"I ran a COUNT(*) on a 500-million-row table and it came back in 3 seconds. On our old Oracle system the same count took 45 seconds. Why is Teradata so much faster?"*

Explain what happened under the hood in Teradata to produce the 3-second result.

<details>
<summary>💡 Hint</summary>

Think about what each AMP does when it receives the COUNT(*) step. How many AMPs are likely involved? What does the PE do after all AMPs respond?

</details>

<details>
<summary>✅ Solution</summary>

**What happened in Teradata:**

1. **The PE parsed the query** and created an execution plan: "Scan all rows on all AMPs and count."
2. **All AMPs executed in parallel** — if there are 100 AMPs each with ~5 million rows, each AMP counted its 5 million rows simultaneously.
3. **BYNET collected partial counts** — each AMP returned its local count to the PE.
4. **The PE summed the partial counts** (100 numbers) and returned the total.

**The key difference from Oracle:**
- Oracle (SMP) scanned 500 million rows sequentially (or with limited parallelism)
- Teradata (MPP, shared-nothing) scanned all rows simultaneously across all AMPs

**Formula for intuition:**
`Teradata time ≈ Oracle time / number_of_AMPs`

3 seconds ≈ 45 seconds / ~15 AMPs (rough estimate)

**Good follow-up points to mention:**
- This parallelism benefit scales with the number of AMPs
- For a single-row lookup by Primary Index, Teradata routes to exactly 1 AMP — no parallelism needed
- The PE is the aggregation point; BYNET is the highway

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: AMP Failure During Business Hours

You are on-call as a Teradata DBA. At 2 PM on a Tuesday (peak usage time), you receive an alert: **AMP 23 is offline**. Business users are actively querying the system.

Walk through what happens immediately, what you do next, and how you verify the system is healthy.

<details>
<summary>💡 Hint</summary>

Consider: Is data lost? Can users still query? What mechanism provides continuity? What does the hot standby do? What logs do you check?

</details>

<details>
<summary>✅ Solution</summary>

**Immediate automatic actions (no human intervention):**

1. **AMP 23 marked offline** in the hash map (propagated to all PEs in seconds)
2. **Fallback copies** on other AMPs (different clique) immediately serve reads for rows that were primary on AMP 23
3. **Hot standby node** absorbs AMP 23's vproc — takes ownership of the hash buckets
4. **In-flight queries** that were mid-execution: the PE detects the AMP failure, retries the step using fallback data

**From the user's perspective:** Queries may show a brief delay (seconds) while the handoff completes, but no errors (assuming Fallback is enabled and hot standby is configured).

**Your actions as DBA:**

```sql
-- Step 1: Confirm AMP status
SELECT AMPNumber, StatusCode, StatusDesc
FROM DBC.AMPUsageV
WHERE AMPNumber = 23;

-- Step 2: Check if hot standby absorbed the vproc
SELECT * FROM DBC.ResSpmaV WHERE NodeID = <standby_node>;

-- Step 3: Verify no spool errors piling up (queries using fallback are slower)
SELECT UserName, State, NumSteps
FROM DBC.SessionInfoV
WHERE State = 'Active'
ORDER BY NumSteps DESC;
```

**Then:**
- Engage hardware team to diagnose AMP 23's node
- Check system event log for root cause (disk I/O error, memory fault, kernel panic)
- Schedule node replacement during maintenance window
- After replacement: migrate vproc back from standby (background, no query interruption)

**Key assertion:** If Fallback was disabled on any table, those tables now have partial data loss on AMP 23 until the node is recovered. This is why Fallback is non-negotiable on production tables.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Architecture Review — Greenfield System Design

Your company is building a new enterprise data warehouse on Teradata Vantage. The system will handle:
- 50 TB of historical transactional data
- 500 GB/day new data ingestion
- 300 concurrent business analyst users (mix of ad-hoc and scheduled reports)
- 10 tactical API-serving queries per second with 2-second SLA
- Regulatory requirement: 7-year data retention with full audit trail

Design the high-level architecture, justify your decisions, and identify the key risks.

<details>
<summary>💡 Hint</summary>

Think about: workload separation, storage tiers, temporal data handling for audit, AMP sizing, Fallback strategy, and how Vantage's cloud features (object store, QueryGrid) change your design vs traditional on-premise.

</details>

<details>
<summary>✅ Solution</summary>

**Proposed Architecture:**

```
Tier 1: Tactical Query Engine (small, NVMe-backed AMP pool)
  - Hot data: last 90 days
  - TASM: priority class "tactical", max 2s response goal
  - Indexes tuned for API query patterns

Tier 2: Strategic Analytics Engine (larger AMP pool, SSD/HDD mix)
  - 90 days → 3 years of data
  - TASM: priority class "strategic", analyst workloads
  - PPI on date columns for partition elimination

Tier 3: Archive (Vantage object store — S3/Azure Blob)
  - 3–7 year data
  - Accessed via external table definitions
  - Columnar parquet format for cost efficiency
```

**Key Design Decisions:**

1. **Workload separation via TASM:** Tactical queries get their own priority class and AMP CPU time budget — never blocked by analyst batch jobs.

2. **Temporal tables for audit:** Use ANSI temporal (transaction-time tables) for all regulated entities — every change is recorded with system-time, satisfying 7-year audit requirements without custom audit triggers.

3. **Fallback strategy:**
   - Tactical hot data: Fallback ON (revenue-critical, no downtime acceptable)
   - Strategic analytics: Fallback ON for fact tables, OFF for staging/work tables
   - Archive: Object store redundancy (3× replication by cloud provider)

4. **PI design:**
   - Fact tables: PI on the most common JOIN key (e.g., customer_id or transaction_id), PPI on date
   - Dimension tables: UPI on natural key (small tables, correctness matters)
   - Staging tables: NoPI (fastest load, no hashing overhead)

5. **Sizing:**
   - 50 TB data × 2 (Fallback) × 1.3 (overhead) = ~130 TB permanent space needed
   - Spool: 300 users × avg 5 GB spool per query × 50% concurrency = ~750 GB spool capacity
   - Start with ~20 nodes, plan for 25% growth headroom

**Risks:**
- BYNET saturation during heavy analyst workloads — mitigate with statistics collection and schema review
- Archive tier query performance (object store is slower) — pre-warm common archive queries, document SLA expectations
- 7-year retention means accumulating skew as data distribution evolves — schedule annual PI review
- QueryGrid introduces network latency for cross-tier joins — benchmark before committing to cross-tier query patterns

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the core architectural principle of Teradata?**
A: Teradata is a Massively Parallel Processing (MPP) system where data is distributed across many independent nodes (AMPs—Access Module Processors), each with its own CPU, memory, and disk. Queries are broken into parallel tasks executed simultaneously across all AMPs, with results combined by the Parsing Engine.

**Q: What is an AMP in Teradata and what is its role?**
A: An AMP (Access Module Processor) is Teradata's fundamental processing unit—a virtual processor responsible for storing and processing its share of the data. Each AMP manages a subset of the database rows determined by the Primary Index hashing. AMPs work in parallel during query execution.

**Q: What is the Parsing Engine in Teradata?**
A: The Parsing Engine (PE) receives SQL queries from clients, parses and validates them, generates an optimized query execution plan, and distributes query steps to the AMPs. It also collects and assembles results from AMPs before returning them to the client. There are typically multiple PEs for concurrency.

**Q: What is BYNET in Teradata?**
A: BYNET is Teradata's proprietary high-speed interconnect network that allows AMPs and PEs to communicate—sharing data, row hashes, and intermediate results during joins, sorts, and redistributions. Its bandwidth and latency directly affect query performance for data-intensive operations that require inter-AMP communication.

**Q: What is the difference between a Teradata system with shared-nothing vs. shared-disk architecture?**
A: Teradata uses shared-nothing: each AMP has exclusive ownership of its data and disk. No disk is shared between AMPs. This enables full parallelism without I/O contention and is the basis for Teradata's linear scalability. In contrast, shared-disk systems (e.g., Oracle RAC) share storage, introducing coordination overhead.

**Q: What are the Teradata Vantage components and how do they extend the classic architecture?**
A: Teradata Vantage is the modern platform that adds capabilities on top of the classic MPP engine: QueryGrid (federated queries across engines), Vantage Analyst (in-database ML and analytics), object storage integration (for data lake access), and connector support for cloud environments. It positions Teradata as a hybrid on-prem/cloud analytical platform.

**Q: What is a fallback table in Teradata and why is it used?**
A: A Fallback table maintains a second copy of each row on a different AMP. If an AMP fails, the fallback copy on a surviving AMP is used to maintain data availability. Fallback doubles storage cost and adds write overhead but provides automatic AMP-level fault tolerance without requiring external replication.

**Q: What are Teradata's three main table types?**
A: Permanent tables (persist until explicitly dropped), volatile tables (session-scoped temporary tables, no logging, dropped at session end), and global temporary tables (persist definition across sessions but rows are session-scoped). Volatile tables are critical for breaking complex queries into testable steps without disk I/O overhead.

---

## 💼 Interview Tips

- Always anchor Teradata architecture answers on AMPs and parallelism—the entire system's design flows from the assumption that data is evenly distributed across AMPs and queries execute in parallel. This is the lens through which every performance discussion should be viewed.
- Know BYNET's role: inter-AMP communication is expensive, and the best Teradata queries minimize redistribution by aligning Primary Indexes across related tables. Mentioning BYNET cost in join discussions signals architecture-level thinking.
- Distinguish Teradata's shared-nothing model from other MPP systems (Redshift, Snowflake) to show breadth. Each system distributes work differently, and knowing why matters for cross-system design conversations.
- Senior interviewers at Teradata-heavy shops (financial services, telecom) will probe fallback and availability design. Knowing the storage vs. availability trade-off is expected at the senior level.
- Mention Teradata Vantage if the company is modernizing—many large enterprises are moving Teradata workloads to cloud or hybrid environments, and showing awareness of the modern platform signals you're current, not just familiar with legacy Teradata.
