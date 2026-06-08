---
title: "End-to-End Case Studies — Fundamentals"
topic: system-design
subtopic: end-to-end-case-studies
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, case-study, architecture, interview, design-process]
---

# End-to-End Case Studies — Fundamentals

## How to Approach a System Design Interview

Data engineering system design interviews expect you to design a complete data system from scratch. Use this framework:

```
STEP 1 — Clarify requirements (5 min)
  □ What is the business use case?
  □ What are the data sources? (databases, events, files, APIs)
  □ What is the scale? (rows/day, events/sec, data volume)
  □ What is the latency requirement? (real-time < 1s, near-real-time < 5 min, batch = daily)
  □ What are the consumers? (BI dashboards, ML models, operational APIs)
  □ What is the SLA? (99.9%? 99.99%?)
  □ What is the budget? (startup vs enterprise)

STEP 2 — High-level design (10 min)
  □ Draw the three layers: Ingestion → Processing → Serving
  □ Choose batch vs streaming vs hybrid
  □ Pick the storage architecture (data lake, DW, both)

STEP 3 — Deep dive (15 min)
  □ Detail the most complex component
  □ Address: scalability, fault tolerance, data quality
  □ Schema design (fact/dim tables or event schema)

STEP 4 — Tradeoffs (5 min)
  □ What would you do differently with 10× more scale?
  □ What are the known limitations of your design?
  □ What would you add with more time?
```

---

## The Three-Tier Data Architecture

Every DE system design answer uses this skeleton:

```
┌──────────────────────────────────────────────────────────┐
│                    DATA SOURCES                          │
│  Databases  │  Event Streams  │  Files  │  APIs         │
└─────────────────────────┬────────────────────────────────┘
                          │ Ingestion
┌─────────────────────────▼────────────────────────────────┐
│                  INGESTION LAYER                         │
│  Kafka / Kinesis (streaming)                             │
│  Fivetran / Airbyte / Debezium (CDC/batch)               │
│  S3 / GCS (file landing zone)                            │
└─────────────────────────┬────────────────────────────────┘
                          │ Process
┌─────────────────────────▼────────────────────────────────┐
│                PROCESSING LAYER                          │
│  Spark (batch / micro-batch)                             │
│  Flink (streaming)                                       │
│  dbt (SQL transformations inside DW)                     │
│  Storage: Delta Lake / Iceberg / Snowflake               │
└─────────────────────────┬────────────────────────────────┘
                          │ Serve
┌─────────────────────────▼────────────────────────────────┐
│                  SERVING LAYER                           │
│  BI Tools: Tableau, Power BI, Looker                     │
│  APIs: GraphQL, REST (from DW or operational DB)         │
│  ML Platform: feature store, training data               │
└──────────────────────────────────────────────────────────┘
```

---

## Sizing Quick Reference

Use these numbers in interviews to show credibility:

| Metric | Typical Range | Notes |
|---|---|---|
| Kafka throughput per broker | 100-300 MB/s writes | Standard hardware |
| Kafka partition throughput | ~10 MB/s | Max recommended |
| Spark executor memory | 4-16 GB | Depends on data size |
| Parquet compression ratio | 5-10× vs CSV | With snappy/zstd |
| Delta OPTIMIZE target file size | 1 GB | Per partition |
| Snowflake micro-partition size | 50-500 MB compressed | Automatic |
| S3 Standard cost | $0.023/GB/month | US East |
| Snowflake compute (S warehouse) | 1 credit/hour | ~$2-4/credit |
| EMR m5.xlarge on-demand | $0.192/hour | 4 vCPU, 16 GB RAM |
| EMR m5.xlarge spot | ~$0.05/hour | 60-70% savings |

---

## Common Design Mistakes in Interviews

| Mistake | What to Do Instead |
|---|---|
| Jump to solution without clarifying requirements | Always clarify: scale, latency, consumers first |
| Choose one tool (just Kafka, just Spark) | Draw the full end-to-end architecture |
| Ignore fault tolerance | Always mention: retries, checkpointing, DLQ |
| Ignore data quality | Mention quality checks between each layer |
| Ignore cost | Mention: spot instances, auto-suspend, partitioning for query cost |
| Over-engineer for a startup | Match complexity to stated scale |
| Under-design for a large-scale system | If >1B events/day: streaming required |

---

## Interview Tips

> **Tip 1:** "How do you start a system design interview?" — Start with questions, not solutions. "Before I dive in — can I clarify a few things? What's the approximate data volume? What latency do the consumers need? Are there any existing systems I'm integrating with?" This shows maturity. Interviewers want to see you understand that the best design depends on requirements. Then: draw the three tiers (ingestion → processing → serving) at a high level before diving deep.

> **Tip 2:** "What are the most important things to cover in a DE system design?" — (1) End-to-end data flow (sources → storage → consumers), (2) Storage layer design (which tables, how partitioned, what format), (3) How you handle failures (retries, checkpoints, DLQ), (4) How you ensure data quality (assertions between layers), (5) How it scales (horizontal scaling, partitioning). Optional but impressive: cost estimates, monitoring strategy, schema evolution handling.

> **Tip 3:** "How do you handle 'I don't know the exact numbers'?" — Make reasonable estimates and show your reasoning: "A typical Kafka broker handles 100-300 MB/s. If we have 50,000 events/second at 1KB each, that's 50MB/s — easily handled by one broker, so we'd use 3 for fault tolerance." Interviewers evaluate thinking process and order-of-magnitude correctness, not exact numbers. Confidently estimate and explain assumptions.
