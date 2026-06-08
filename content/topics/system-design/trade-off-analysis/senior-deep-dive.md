---
title: "Trade-off Analysis — Senior Deep Dive"
topic: system-design
subtopic: trade-off-analysis
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, trade-offs, architectural-decisions, adr, build-vs-buy]
---

# Trade-off Analysis — Senior Deep Dive

## Architecture Decision Records (ADR)

```
ADR: a structured document capturing an architectural decision and its context.
Purpose: future team members understand WHY a decision was made, not just WHAT.

ADR template:
  Title: [short description of the decision]
  Date: [when decided]
  Status: [Proposed | Accepted | Deprecated | Superseded]
  Context: [what situation/problem forced this decision?]
  Decision: [what was decided?]
  Consequences: [what are the results — positive and negative?]
  Alternatives considered: [what else was evaluated and why rejected?]

Example ADR:

  Title: Use Delta Lake instead of plain Parquet for all Silver layer tables
  Date: 2024-01-15
  Status: Accepted
  
  Context: Our current pipeline uses plain Parquet files. We've had 3 incidents
  in 6 months where concurrent writes from different jobs produced corrupted tables.
  Additionally, we need to implement CDC (delete propagation from Debezium) and
  currently have no way to process deletes without rewriting entire partitions.
  
  Decision: Migrate all Silver layer tables to Delta Lake format.
  
  Consequences:
    (+) ACID transactions eliminate concurrent write corruption
    (+) MERGE statement enables CDC deletes natively
    (+) Time-travel for debugging (query table state 7 days ago)
    (+) Schema enforcement catches upstream schema drift immediately
    (-) 10-15% write overhead vs plain Parquet
    (-) Delta log introduces metadata files (S3 listing overhead at scale)
    (-) Requires Spark 3.x or Delta RS; not Athena-native (use manifest files)
  
  Alternatives considered:
    Apache Iceberg: similar ACID benefits, better multi-engine support.
      Rejected: team is on Databricks, Delta is more native and Databricks-optimized.
    Hudi: COW/MOR modes, good for incremental. Rejected: more complex than Delta,
      smaller community, harder to troubleshoot.
    Plain Parquet with partition overwrite: current approach. Rejected: no delete
      support, concurrent write risk remains.
```

---

## Build vs Buy Decision Framework

```
Every new capability: build internally, buy a vendor solution, or use open source?

Dimensions to evaluate:
  1. Core competency: is this a competitive differentiator?
     If yes: build (own the IP, iterate freely)
     If no: buy (focus energy on what matters)
  
  2. Total cost of ownership:
     Build: engineering time + maintenance + upgrades + ops + support
     Buy: license/SaaS fee + integration time + vendor risk
     Common mistake: underestimate "build" ongoing cost
  
  3. Availability: does a mature solution exist?
     If yes: buying is usually faster and cheaper
     If no: build (but validate that need is real first)
  
  4. Vendor lock-in risk:
     High-risk: storing data in a proprietary format (vendor-specific DW)
     Mitigate: use open formats (Parquet/Delta) even with proprietary compute
     Low-risk: Airflow (open source, run anywhere)

Build vs Buy examples in DE:

  Data ingestion (Fivetran vs custom):
    Fivetran: $5,000/month, 300+ connectors, maintained by vendor
    Custom: 2 engineers × 3 months = ~$60,000, then ongoing maintenance
    Decision: Buy Fivetran unless you have highly custom sources
    
  Data quality (Great Expectations vs custom assertions):
    GE: free (open source), mature, rich rule library, but complex to configure
    Custom: simple but grows organically into a mess
    Decision: use GE (or Soda Core) for structured approach; don't reinvent
    
  Data catalog (DataHub vs Atlan vs custom):
    Custom: very high effort; metadata management is hard
    DataHub: open source, LinkedIn-proven, requires ops
    Atlan: managed SaaS, $50-100K/year for mid-size team
    Decision: buy for most teams (catalog is infrastructure, not differentiation)
    
  Feature store (Feast vs Tecton vs custom):
    Custom: feasible for simple use cases (Redis + some Python)
    Feast: open source, production-ready, self-hosted
    Tecton: managed, expensive, best for real-time ML at scale
    Decision: custom for <5 models; Feast for 5-20 models; Tecton for 20+ real-time models
```

---

## Consistency Models in Distributed DE Systems

```
Consistency spectrum:

Strong consistency (linearizability):
  Every read sees the most recent write
  Requires: distributed coordination (consensus protocol: Raft, Paxos)
  Cost: high write latency, reduced availability during failures
  Use: financial ledgers, inventory, anything where stale data causes real harm

Sequential consistency:
  All operations appear to execute in some sequential order
  Weaker than linearizable: order not necessarily wall-clock order
  Use: distributed lock services, leader election

Causal consistency:
  Operations causally related appear in correct order
  "If I posted a message and you replied, your reply appears after my post"
  Use: collaborative editing, chat, comment threads

Eventual consistency:
  System converges to consistent state eventually (no time guarantee)
  Reads may return stale data during partition
  Use: user profiles, shopping cart, DNS, most OLAP systems
  
Read-your-writes consistency (session consistency):
  A user always sees their own writes
  Example: after posting a comment, you can immediately see it
  Implementation: route same user's reads to same replica, or use sticky sessions

For data engineering:
  Gold tables (for BI): eventual consistency is fine (dashboard 5min stale = OK)
  Streaming aggregations: causal consistency (events in correct order)
  Financial transactions: strong consistency (no double-counting, no missing)
  Feature store (online): read-your-writes (model sees latest feature after update)
```

---

## Evaluating Technical Debt Trade-offs

```
Not all technical debt is equal. Framework for prioritization:

Quadrant:
  High Impact + Hard to Fix = CRITICAL (fix now; will cause production incidents)
    Example: no idempotency in core pipeline, no monitoring, no DLQ
  
  High Impact + Easy to Fix = QUICK WIN (fix this sprint)
    Example: missing partition filter on expensive query, hardcoded dates
  
  Low Impact + Hard to Fix = BACKLOG (schedule quarterly)
    Example: legacy ETL framework that works but is unmaintainable
  
  Low Impact + Easy to Fix = DO IT NOW (5-minute task, just do it)
    Example: adding a code comment, renaming a confusing variable

Communicating trade-offs when accepting debt:
  "We're choosing to ship faster by skipping [X], which creates [specific risk].
   We'll pay this debt by [date] via [specific fix].
   Until then, the risk is mitigated by [temporary measure]."
  
  Example:
  "We're skipping SCD Type 2 for now and using Type 1 overwrite.
   Risk: historical region analysis will show current region, not historical.
   This is acceptable because: no one has asked for historical region analysis yet.
   We'll add SCD Type 2 when: (1) someone requests it, or (2) we migrate to dbt snapshots.
   Tracked in: JIRA ticket DATA-456."
```

---

## Interview Tips

> **Tip 1:** "How do you decide when to use open source vs managed services?" — Open source is cheaper per unit but has high ops cost. Managed services are expensive per unit but near-zero ops cost. Break-even: if the managed service costs < (2 engineers × 20% of time for ops) → buy. Example: MSK costs $500/month. Self-managed Kafka on EC2 requires 0.2 FTE = $20,000/year. MSK wins. The decision flips at scale: at $50,000/month MSK, self-managed becomes cost-competitive if you have ops expertise.

> **Tip 2:** "How do you evaluate a new data tool before adopting it?" — Proof of concept with realistic data and scale (not a toy dataset). Evaluate: (1) Does it meet functional requirements? (2) Can the team operate it? (3) What's the failure mode and how does it recover? (4) Community: is it maintained, growing, or dying? (5) Vendor/license risk: can we migrate away if needed? (6) Cost at 10× scale. Red flag: vendor claims "no limitations" — push for benchmarks on your specific use case.

> **Tip 3:** "How do you communicate an architecture trade-off to non-technical stakeholders?" — Translate to business language: "Option A (streaming) gives dashboard users data within 2 minutes but costs $30K/year more and requires 3 months to build. Option B (batch) gives data by 7 AM every morning at current cost. 80% of dashboard usage happens after 9 AM — so Option B meets the actual business need. I recommend Option B. We can revisit Option A if sub-hourly freshness becomes a real requirement." Show you understand the business need, not just the technology.
