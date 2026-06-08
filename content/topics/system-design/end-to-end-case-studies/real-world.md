---
title: "End-to-End Case Studies — Real World"
topic: system-design
subtopic: end-to-end-case-studies
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, case-study, production, architecture, lessons-learned]
---

# End-to-End Case Studies — Real World

## How Netflix-Style Data Platforms Are Built

```
Netflix Data Engineering Architecture (publicly documented):
  Iceberg: table format for the data lake
  Flink: streaming processing
  Spark: batch processing
  Druid: real-time analytics (< 1 second queries on billions of rows)
  Presto/Trino: ad-hoc queries on the data lake
  Metacat: data catalog (schema registry, table metadata)
  Amundsen/DataHub: data discovery

Key lessons from Netflix-scale:
  1. Table formats matter: moved from Hive to Iceberg
     Benefit: schema evolution, time-travel, partition evolution without downtime
     Result: hundreds of TB partition operations in seconds (not hours)
  
  2. Separation of storage and compute
     All data in S3 → compute clusters (Flink, Spark, Druid) read from S3
     Scale compute independently of storage
     Cost: pay only for compute when running (not 24/7 cluster)
  
  3. Metadata is critical
     At scale: 100,000+ tables; without catalog → "which table should I use?"
     DataHub: lineage, ownership, quality, freshness all in one place
     Principle: if a table isn't in the catalog, it doesn't exist officially
  
  4. Tiered storage saves millions
     "Cold" data (>90 days) moved to S3 Glacier automatically
     Retrieval on demand when needed
     Saves: ~70% of S3 storage cost at scale
```

---

## Startup Data Stack Evolution

```
Stage 1 — Pre-product/market fit (0-100K users):
  Data stack: PostgreSQL + Metabase
  ETL: direct SQL queries on production DB
  Cost: near zero
  Lesson: don't build a data platform before you have data worth analyzing

Stage 2 — Growth (100K-1M users):
  Stack: Fivetran → Snowflake → dbt → Looker/Tableau
  Setup time: 2 weeks
  Monthly cost: ~$2,000-5,000
  Benefit: analysts self-serve; no more prod DB queries
  Lesson: this stack handles 90% of analytics needs; don't over-engineer early

Stage 3 — Scale (1M-10M users):
  Add: Kafka for real-time events, Spark for large transformations
  Add: Great Expectations for data quality
  Add: DataHub or Atlan for data catalog
  Monthly cost: ~$15,000-30,000
  Lesson: add complexity only when you hit a specific pain point

Stage 4 — Mature (10M+ users):
  Add: Delta Lake / Iceberg data lake for cost (raw data in S3, not Snowflake)
  Add: ML platform (feature store, MLflow)
  Add: Real-time serving layer (ClickHouse/Druid) for sub-second analytics
  Monthly cost: $50,000-200,000+
  Lesson: DW is still core; data lake is for cost optimization and ML

Anti-patterns at each stage:
  Stage 1: Building Kafka + Spark infrastructure before having users
  Stage 2: Building a data lake before the DW is working
  Stage 3: Over-investing in data catalog before people use the data
  Stage 4: Not investing in data quality (garbage at scale = expensive garbage)
```

---

## Lessons Learned from Real Production Systems

| Lesson | Context | Detail |
|---|---|---|
| Idempotency saves on-call | Every production outage | Every pipeline must be safely re-runnable. Saved dozens of 2am incidents |
| Monitor freshness, not just success | Financial company DW | Pipeline "succeeded" but loaded 0 rows. Nobody noticed for 3 days |
| Cost alerts save careers | AWS bill surprise | A single misconfigured EMR cluster ran for 2 weeks: $45,000 surprise. Set daily cost alerts |
| Schema drift will happen | All long-running systems | Source team renames a column with no notice. Schema registry + drift detection is not optional |
| Data catalog ROI is slow but real | Enterprise analytics | Without catalog: 30% of new hire time finding the right table. With catalog: 1 day |
| Staging table proliferation | Large analytics teams | Without governance: 400+ tables named `final_v2_REAL`. Table naming conventions + ownership fields |
| Partition by query pattern | Performance tuning | Partitioned by `country` but queries filter by `date`. Full scan every run. Repartitioned: 100× speedup |
| Broadcast joins save shuffles | Spark optimization | Joining 10TB fact with 1GB dim: shuffle → OOM. Broadcast join: 10 min → 30 sec |

---

## Interview Tips

> **Tip 1:** "What does a good data engineering on-call look like?" — Runbooks for top 5 failure modes, auto-retry with backoff (no human needed for transient errors), clear escalation path (pipeline failures → data engineering; infrastructure failures → platform team), SLA-driven alerting (page only if SLO at risk, not on every warning), blameless postmortem after every P1/P2. Goal: mean time to detect < 5 minutes, mean time to recover < 30 minutes.

> **Tip 2:** "How do you get buy-in for data platform improvements?" — Frame in business terms: "Our pipeline reliability is 97%. That means 1 out of 33 days dashboards show stale data. Fixing this requires 2 weeks of engineering and will save 4 hours/month of incident response." Show the cost of not fixing vs cost of fixing. For larger investments: pilot on one team/pipeline, show before/after metrics, expand from there. Avoid: "we need to refactor our infrastructure" — this lands badly without business impact framing.

> **Tip 3:** "How do you handle technical debt in a data platform?" — Classify debt: (1) Quality debt (bad/missing tests) — highest priority, causes correctness bugs, (2) Reliability debt (no retries, no monitoring) — causes incidents, (3) Operational debt (manual processes, undocumented pipelines) — causes toil, (4) Architecture debt (wrong tool/pattern for scale). Pay quality and reliability debt first (they hurt users). Schedule architecture debt as a dedicated project with exec sponsorship. Track debt explicitly — if it's not visible on the roadmap, it won't get fixed.
