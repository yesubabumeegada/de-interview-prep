---
title: "ETL Interview Questions: Core Concepts and Fundamentals"
description: "Classic ETL interview questions covering ETL vs ELT, when to use each, idempotency basics, and pipeline design foundations."
content_type: study_material
topic: etl-concepts
subtopic: interview-scenarios
layer: fundamentals
difficulty_level: junior
tags: [etl, elt, idempotency, pipeline-design, interview-prep, fundamentals]
---

# ETL Interview Questions: Fundamentals

## The Most Common ETL Interview Questions

This guide covers the classic ETL questions you will encounter in data engineering interviews at every level. For junior/entry-level roles, interviewers are testing whether you understand the core concepts and trade-offs clearly enough to explain them to a non-technical stakeholder.

---

## Question 1: What Is ETL and How Does It Work?

**How to answer:**

ETL stands for **Extract, Transform, Load** — a three-stage process for moving data from source systems into a target data store.

- **Extract:** Read data from source systems (databases, APIs, files, event streams). This may involve full extracts (all records) or incremental extracts (only new/changed records since the last run).

- **Transform:** Apply business logic, clean the data, join datasets, aggregate, convert formats, enforce data types, and derive new fields. Transformation happens before loading.

- **Load:** Write the transformed data into the target system (data warehouse, data lake, database).

**Diagram:**

```
Source Systems          ETL Processing Layer         Target
──────────────          ───────────────────         ──────
CRM Database    ──────► Extract → Transform ──────► Data Warehouse
Payments API    ──────►         ↕           ──────► Analytics DB
Log Files       ──────► (intermediate)      ──────► Data Lake
```

**Key point to mention:** In traditional ETL, transformation happens in a dedicated processing layer (often a separate server or compute cluster) before the data reaches the warehouse. This means the warehouse only receives clean, ready-to-query data.

---

## Question 2: What Is ELT and How Is It Different from ETL?

**How to answer:**

ELT stands for **Extract, Load, Transform** — the order of the last two steps is reversed.

- **Extract:** Same as ETL — read from sources.
- **Load:** Write raw, untransformed data directly into the target system first.
- **Transform:** Apply transformations inside the target system using its own compute power.

```
Source Systems     Target System         Transformation
──────────────     ─────────────         ──────────────
CRM Database ──►  Raw Layer   ──────►   dbt models run
Payments API ──►  (warehouse) ──────►   inside the warehouse
Log Files    ──►             ──────►   Snowflake / BigQuery SQL
```

**Why ELT became dominant:**

Modern cloud data warehouses (Snowflake, BigQuery, Redshift, Databricks) are massively scalable and can run SQL transformations at petabyte scale cheaply. It's now faster and cheaper to load raw data first and transform it inside the warehouse than to maintain a separate transformation infrastructure.

**ETL vs. ELT Comparison:**

| Dimension | ETL | ELT |
|-----------|-----|-----|
| Transform location | External processing layer | Inside target warehouse |
| When to transform | Before loading | After loading |
| Typical tools | Informatica, Talend, SSIS | dbt, Dataform, SQL |
| Data in warehouse | Only clean data | Raw + clean data |
| Re-transformation | Must re-run entire pipeline | Re-run SQL models only |
| Debugging | Harder (black box transformations) | Easier (query intermediate tables) |
| Best for | Legacy systems, compliance, PII masking | Modern cloud warehouses |

---

## Question 3: When Would You Use ETL vs. ELT?

**Use ETL when:**
1. **PII/sensitive data must be masked before entering the warehouse.** You cannot load SSNs or credit card numbers into a warehouse and then mask them — the raw data should never land there.
2. **The target system has limited compute.** An on-premise Oracle warehouse may not handle complex transformations efficiently.
3. **Strict compliance requirements** dictate what data can be stored in what system.
4. **Legacy systems** where the ETL infrastructure is already built and working.

**Use ELT when:**
1. **You use a modern cloud warehouse** (Snowflake, BigQuery, Redshift) that can handle SQL transformations at scale.
2. **You want to preserve raw data** for reprocessing when business logic changes.
3. **Transformation logic changes frequently** — with ELT you just update the dbt model and re-run, without rebuilding a pipeline.
4. **Your team knows SQL** — ELT transformations are SQL-first and accessible to analysts.
5. **You need rapid iteration** — loading raw first is faster, and transformations can evolve independently.

**Sample answer structure:**
> "I'd use ELT for most modern greenfield projects on cloud warehouses because it's simpler, cheaper, and more iterative. The main exceptions are when raw data contains sensitive information that shouldn't land in the warehouse, or when we're integrating with legacy systems where ETL infrastructure is already established."

---

## Question 4: What Is Idempotency and Why Does It Matter in ETL?

**How to answer:**

An **idempotent** ETL pipeline produces the same result whether it runs once or ten times with the same input data. Running it multiple times does not create duplicates or corrupt data.

**Why it matters:**
- ETL pipelines fail. Network timeouts, resource limits, schema changes, and source unavailability all cause failures.
- After a failure, you need to re-run the pipeline to get fresh data.
- Without idempotency, re-running creates duplicate records or corrupted aggregates.

**Non-idempotent example (bad):**
```sql
-- This creates duplicates on re-run
INSERT INTO orders_fact
SELECT * FROM orders_staging
WHERE event_date = '2024-01-15';
-- Run twice → 2x the orders for that date
```

**Idempotent example (good):**
```sql
-- OVERWRITE replaces the partition — safe to re-run
INSERT OVERWRITE INTO orders_fact
PARTITION (event_date = '2024-01-15')
SELECT * FROM orders_staging
WHERE event_date = '2024-01-15';
-- Run twice → same result as running once
```

**Three common patterns to make ETL idempotent:**

1. **Delete + Insert:** Delete target records for the batch window, then insert fresh data.
2. **MERGE / Upsert:** Use a primary key to update existing records and insert new ones.
3. **Partition Overwrite:** Replace the entire partition with the new data.

---

## Question 5: What Are the Main Challenges in ETL Pipelines?

Interviewers ask this to see if you have practical awareness of what can go wrong.

**Key challenges to mention:**

**1. Schema Changes**
Sources change their schemas without warning — a column is renamed, a data type changes from INT to VARCHAR, or a new required field is added. This breaks downstream pipelines.

*Mitigation: Schema evolution handling, contract testing, alerting on schema changes.*

**2. Data Volume Growth**
A pipeline that runs in 2 hours today may run in 8 hours a year from now as data grows. Batch windows become too tight.

*Mitigation: Incremental loading, partitioning, compute scaling.*

**3. Late-Arriving Data**
Events from mobile apps or external systems often arrive hours or days after the event occurred. A daily batch job may process data before all events have arrived.

*Mitigation: Watermarks, restatement jobs, or accepting partial completeness for batch.*

**4. Duplicate Data**
Sources sometimes publish the same event multiple times. Without deduplication, aggregates are inflated.

*Mitigation: Deduplication keys, MERGE patterns, exactly-once delivery semantics.*

**5. Dependency Management**
Pipeline B cannot run until Pipeline A finishes. If A fails, B is blocked. If there are 50 interdependent pipelines, managing dependencies becomes complex.

*Mitigation: Orchestration tools (Airflow, Prefect, Dagster), explicit dependency declarations.*

---

## Question 6: What Is Incremental Loading and How Does It Compare to Full Loads?

**Full Load:**
Every pipeline run extracts all records from the source and replaces the entire target table.

```
Full Load:
Run 1: Load 1M records → target has 1M records
Run 2: Load 1.1M records → target has 1.1M records (all replaced)
```

- Simple to implement and understand
- Always produces a correct result (idempotent by nature)
- Expensive for large tables — re-reads all source data every time

**Incremental Load:**
Each run only extracts records that have changed since the last run (identified by a timestamp or change flag).

```
Incremental Load:
Run 1: Load 1M records (initial full load)
Run 2: Load 50K new/changed records → merged into target
```

- Much faster and cheaper for large tables
- Requires a reliable "changed since" mechanism (updated_at timestamp, CDC)
- Risk of missing records if the changed-since mechanism has gaps

**When to use each:**

| Scenario | Use |
|----------|-----|
| Small table (< 1M rows) | Full load (simpler) |
| Source has reliable updated_at | Incremental |
| Source does hard deletes | Full load (incremental misses deletes) |
| Large table (> 10M rows) | Incremental (full load too slow/expensive) |
| Daily pipeline where all data changes | Full load |

---

## Quick Reference: Core ETL Terms

| Term | Definition |
|------|-----------|
| ETL | Extract, Transform, Load |
| ELT | Extract, Load, Transform |
| Idempotency | Re-running produces the same result |
| Full Load | Load all records every run |
| Incremental Load | Load only new/changed records |
| Schema Evolution | Handling source schema changes without breaking pipelines |
| Deduplication | Removing duplicate records |
| Watermark | Threshold for handling late-arriving data |
| DAG | Directed Acyclic Graph — dependency structure of a pipeline |

---

## How to Structure Your Answers in ETL Interviews

Use the **DEFINE → COMPARE → WHEN TO USE** framework:

1. **Define** the concept clearly in 1-2 sentences
2. **Compare** it to the alternative or related concept
3. **State when to use it** with concrete examples

This structure shows depth of understanding, not just memorization of definitions.
