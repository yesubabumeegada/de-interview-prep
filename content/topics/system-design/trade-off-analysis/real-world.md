---
title: "Trade-off Analysis — Real World"
topic: system-design
subtopic: trade-off-analysis
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, trade-offs, production, decisions, architecture-reviews]
---

# Trade-off Analysis — Real World

## Pattern 1: Weekly Architecture Review

```
How mature DE teams make architectural decisions:

1. Document the problem (not the solution):
   "Our daily dbt run takes 4 hours and is causing SLA misses.
   We need to reduce it to under 1.5 hours."

2. Generate options (at least 3):
   Option A: Optimize existing dbt models (add partition filters, reduce full scans)
   Option B: Split large models into incremental (only process new rows each run)
   Option C: Separate compute (move heavy models to Spark instead of dbt)
   Option D: Add more Snowflake compute (upgrade warehouse size)

3. Evaluate each on key dimensions:
   (Timeline, Cost, Risk, Effectiveness, Maintenance overhead)
   
   Option A: 2 weeks effort, $0 cost, low risk, 50% improvement, low maintenance
   Option B: 4 weeks effort, $0 cost, medium risk, 80% improvement, medium maintenance
   Option C: 8 weeks effort, +$500/month Spark, high risk, 90% improvement, high maintenance
   Option D: 1 day effort, +$2,000/month Snowflake, very low risk, 60% improvement, none
   
4. Recommend with reasoning:
   "Recommend: Option A first (quick wins, low risk), then Option B for full solution.
   Avoid Option D as a first resort — it's a bill increase that masks the problem.
   Option C is over-engineered for this issue."

5. Document the decision (ADR) and track the outcome:
   Measure: did dbt run time actually drop? By how much?
```

---

## Common Trade-off Debates in DE Teams

### Debate 1: Should we use Airflow or dbt as our orchestrator?
```
Context: team runs 90% dbt models + 10% Python scripts for ingestion

dbt as primary orchestrator:
  Pro: natural fit for dbt models, simpler setup, no Airflow server to manage
  Pro: dbt Mesh (cross-project dependencies) handles complex dbt orchestration
  Con: can't orchestrate non-dbt tasks natively (Python, Spark jobs, API calls)
  Con: limited retry/alerting compared to Airflow

Airflow as primary orchestrator:
  Pro: orchestrates anything (dbt via BashOperator or DbtRunOperator, Python, Spark)
  Pro: mature retry logic, SLA miss alerts, complex DAG dependencies
  Con: operational overhead (maintain Airflow server, keep DAG files in sync)
  Con: more boilerplate code vs dbt

Decision: use Airflow if you have significant non-dbt workloads (ingestion, Spark, ML).
Use dbt native scheduling if 95%+ of your pipeline is dbt models. Hybrid: Airflow
orchestrates dbt via the dbt Cloud API or dbt CLI (best of both worlds).
```

### Debate 2: Row-level security in the DW vs application-level filtering
```
Context: multi-tenant SaaS, analysts run ad-hoc SQL in Snowflake

Option A: Row-level security in Snowflake (Row Access Policy)
  Pro: enforced at DB level — impossible to bypass even with direct SQL
  Pro: analysts can't accidentally see wrong tenant data
  Con: adds policy evaluation to every query (small latency overhead)
  Con: complex to debug ("why is this query returning no rows?")

Option B: Application-level filtering (always add WHERE tenant_id = :current_tenant)
  Pro: simpler, no DW policy management
  Con: one mistake in application code → data leak
  Con: direct DB connections (analyst SQL tools) bypass application entirely

Decision: use Row Access Policy for any table with multi-tenant data. Application
filtering is not sufficient when analysts have direct DB access. Defense in depth:
both application AND row access policy. Audit: log all SELECT queries on sensitive tables.
```

---

## Trade-off Decision Matrix Template

```
Use this template in design reviews and interviews:

Decision: [What are we deciding?]

| Option | Latency | Cost | Complexity | Reliability | Build Time | Recommended For |
|--------|---------|------|------------|-------------|------------|-----------------|
| A      |  low    | $$   | simple     | high        | 1 week     | small scale     |
| B      |  medium | $    | medium     | medium      | 3 weeks    | medium scale    |
| C      |  high   | $$$  | complex    | very high   | 8 weeks    | large scale     |

Context factors:
  Current scale: [X events/day]
  Latency requirement: [N seconds]
  Team size: [N engineers]
  Existing stack: [tools you already have]
  Budget constraint: [Y/N, amount]

Recommendation: [Option X] because [1-2 sentences]
Rejected: [Option Y] because [reason], [Option Z] because [reason]
Revisit when: [trigger that would change the recommendation]
```

---

## Interview Tips

> **Tip 1:** "How do you make a tool decision when there's no clear winner?" — Run a time-boxed proof of concept (1-2 weeks). Pick a realistic workload (not a toy example). Measure the things that matter for your use case: query latency at your scale, operational complexity (how long to set up, debug, upgrade), cost estimate, and team ramp-up time. If still tied after POC: go with the one your team already knows (familiarity has compounding value over years).

> **Tip 2:** "How do you avoid analysis paralysis on tool decisions?" — Set a deadline for the decision. Get 80% information, not 100%. Most tool decisions are reversible — you can migrate later. Bias for: what the team already knows, what has the most community support, what you've used before. Avoid: spending 3 months evaluating tools while business waits for data. Ship something, learn, iterate.

> **Tip 3:** "What's an architecture decision you made that turned out to be wrong? What did you learn?" — Strong answer structure: (1) What you decided and why it made sense at the time, (2) What changed (growth, new requirements, team changes), (3) What the wrong decision cost (time, money, incidents), (4) How you fixed it, (5) What you'd do differently. Key: show you can critically evaluate your own decisions and learn from them. Interviewers love this question — it shows maturity and self-awareness.
