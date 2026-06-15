---
title: "Interview Practice Problems — Fundamentals"
topic: system-design
subtopic: interview-practice-problems
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, interview, practice, data-engineering]
---

# Interview Practice Problems — Fundamentals

## How to Approach a System Design Interview

System design interviews test your ability to translate vague requirements into a concrete architecture. Data engineering system design questions focus on pipelines, storage, reliability, and scale — not web serving.

### The Framework (Use Every Time)

```
1. Requirements Clarification   (5 min)
2. Capacity Estimation          (5 min)
3. High-Level Design            (10 min)
4. Detailed Design              (15 min)
5. Trade-offs & Bottlenecks     (5 min)
```

**Never skip Step 1.** Interviewers give intentionally vague prompts. Clarifying questions show maturity.

---

## Requirements Clarification Checklist

For every problem, ask:

| Category | Questions to Ask |
|---|---|
| **Scale** | How many events/sec? How many users? How much data/day? |
| **Latency** | Is this real-time, near-real-time, or batch acceptable? |
| **Durability** | What is the data loss tolerance (RPO)? |
| **Consistency** | Strong consistency or eventual? |
| **SLA** | What uptime is required? |
| **Compliance** | PII? GDPR? SOC2? |
| **Budget** | Any cost constraints? |

---

## Capacity Estimation Basics

Practice these mental math shortcuts:

| Metric | Rule of Thumb |
|---|---|
| 1 billion requests/day | ~12,000 req/sec |
| 1 KB per event × 1M events/day | ~1 GB/day raw |
| Snappy compression | ~3–5× reduction |
| 3× replication (Kafka) | 3× raw storage |
| 1 TB/day uncompressed → Parquet | ~150–200 GB/day |
| Parquet query speedup vs CSV | 10–100× |

---

## Problem 1 (Junior): Design a Simple Log Ingestion Pipeline

**Prompt:** Your company's web servers generate 10,000 log lines/minute. Design a system to store and query them.

### Step 1: Requirements Clarification
- Latency: query logs within 5 minutes of generation
- Retention: 90 days
- Query pattern: search by timestamp + service name + log level
- Scale: 10K lines/min = ~167 lines/sec

### Step 2: Capacity Estimation
- Average log line: 500 bytes
- 167 lines/sec × 500 bytes = 83.5 KB/sec = ~7 GB/day raw
- With 5× compression: ~1.4 GB/day stored
- 90-day retention: ~126 GB total

### Step 3: High-Level Design

```
Web Servers → Filebeat/Fluentd → Kafka → Spark Streaming → Parquet (S3/GCS)
                                                          ↓
                                               Athena / BigQuery (query)
```

### Step 4: Detailed Design

**Ingestion:** Filebeat agents on each server ship logs to Kafka.
- Topic: `app-logs` with 6 partitions (parallelism matches downstream consumers)
- Retention: 24 hours on Kafka (buffer for consumer lag)

**Processing:** Spark Structured Streaming reads Kafka, parses JSON, writes Parquet.
- Partition by: `date / service / log_level`
- Micro-batch interval: 1 minute

**Storage:** S3 with partitioned Parquet files.
- Path: `s3://logs/dt=2024-01-15/service=api/level=ERROR/`

**Query:** AWS Athena on top of S3 Parquet. Point Athena at the S3 prefix.

### Step 5: Trade-offs

| Choice | Alternative | Why This |
|---|---|---|
| Kafka buffer | Direct to S3 | Handles spikes, decouples ingestion from processing |
| Parquet on S3 | Elasticsearch | Cheaper at scale; Elasticsearch costs more for cold search |
| Athena | Redshift | Serverless, no cluster to manage for infrequent queries |

---

## Problem 2 (Junior): Design a Daily Sales Report Pipeline

**Prompt:** A retail company wants a daily report of total sales by region, available by 6 AM each morning.

### High-Level Design

```
PostgreSQL (OLTP) → nightly extract → S3 → dbt transform → Redshift → BI Tool
```

### Key Design Decisions

**Extract (11 PM):**
```sql
-- Incremental extract: only yesterday's sales
SELECT * FROM orders
WHERE created_at >= CURRENT_DATE - 1
  AND created_at < CURRENT_DATE;
```

**Transform (dbt):**
```sql
-- models/sales_by_region.sql
SELECT
  region,
  DATE(created_at) AS sale_date,
  SUM(amount) AS total_revenue,
  COUNT(*) AS order_count
FROM {{ ref('stg_orders') }}
GROUP BY 1, 2
```

**Schedule:** Airflow DAG with dependency chain:
```
extract_task >> upload_s3_task >> dbt_run_task >> send_email_task
```

**SLA Guard:** Set `sla=timedelta(hours=7)` on the final task so Airflow alerts if the pipeline hasn't finished by 6 AM + 1 hour.

---

## Common Junior Mistakes to Avoid

1. **Skipping requirements** — Always clarify scale and latency first
2. **Over-engineering** — A daily batch job does not need Kafka
3. **Ignoring failures** — What happens if the extract fails? Add retries + alerts
4. **No monitoring** — Every pipeline needs row count checks and duration tracking
5. **Single point of failure** — What if the scheduler goes down? Use managed Airflow (MWAA, Composer)

---

## Key Terms to Know

| Term | One-Line Definition |
|---|---|
| **RPO** | Recovery Point Objective — max acceptable data loss window |
| **RTO** | Recovery Time Objective — max acceptable downtime |
| **Idempotency** | Running the same job twice produces the same result |
| **Backpressure** | Slowing producers when consumers can't keep up |
| **Partitioning** | Splitting data by a key (date, region) for parallel processing and pruning |
| **Clustering** | Physically sorting data within partitions by a column |
