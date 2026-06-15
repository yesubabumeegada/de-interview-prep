---
title: "Interview Practice Problems — Real World"
topic: system-design
subtopic: interview-practice-problems
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [system-design, interview, practice, data-engineering, real-world]
---

# Interview Practice Problems — Real World

## How Real Companies Solve These Problems

Understanding how production systems are actually built closes the gap between textbook designs and what interviewers at top companies expect.

---

## Problem: Design a Cost Attribution System for a Multi-Cloud DE Platform

**Context:** Your company spends $8M/year on data infrastructure across AWS, GCP, and Azure. Engineering leaders want to know: which team, product, and pipeline is responsible for which costs?

### Real-World Complexity

1. **Shared resources:** A Spark cluster serves 5 teams. How do you split the cost?
2. **Cross-cloud:** Some pipelines read from S3 (AWS) and write to BigQuery (GCP) — egress costs are on AWS, compute on GCP.
3. **Chargeback vs showback:** Finance wants team-level bills; engineering wants to know where to optimize.
4. **Granularity mismatch:** AWS bills by hour; a single Spark job runs for 7 minutes.

### Architecture

```
[Cloud Cost APIs]
   AWS Cost Explorer API
   GCP Billing Export → BigQuery
   Azure Cost Management API
         │
         ▼
[Cost Ingestion Pipeline] ← runs nightly
(Lambda/Cloud Function → S3 → Spark transform)
         │
         ▼
[Cost Attribution Model]
  - Tag-based allocation (team, project, env tags on resources)
  - Shared resource allocation (proportional to CPU hours used)
  - Pipeline-level attribution (Airflow run_id → cluster → cost)
         │
         ▼
[Cost Database (Snowflake / BigQuery)]
  Tables: cloud_costs_raw, allocated_costs, team_budgets
         │
         ▼
[FinOps Dashboard (Grafana / Looker)]
  - Team monthly spend vs budget
  - Top 10 most expensive pipelines
  - Cost per GB processed
  - Anomaly alerts (>20% week-over-week increase)
```

### Tag Enforcement Pattern

```python
# Terraform: enforce tagging policy before resource creation
resource "aws_emr_cluster" "spark" {
  name = "analytics-cluster"
  tags = {
    Team       = var.team_name      # required
    Project    = var.project_name   # required
    CostCenter = var.cost_center    # required
    Env        = var.environment    # required
    Pipeline   = var.pipeline_id    # required for attribution
  }
}

# CI/CD gate: fail if required tags missing
def validate_tags(resource: dict) -> bool:
    required = {"Team", "Project", "CostCenter", "Env"}
    return required.issubset(resource.get("tags", {}).keys())
```

### Shared Cluster Attribution

```python
# Attribute shared Spark cluster cost proportionally
# based on executor-hours consumed per Airflow job

def attribute_shared_cluster_cost(
    cluster_cost: float,
    job_metrics: list[dict]  # [{job_id, team, executor_hours}]
) -> dict:
    total_executor_hours = sum(j["executor_hours"] for j in job_metrics)
    attribution = {}
    for job in job_metrics:
        share = job["executor_hours"] / total_executor_hours
        attribution[job["team"]] = attribution.get(job["team"], 0) + (cluster_cost * share)
    return attribution
```

### Real Example: Airbnb's Cost Attribution

Airbnb built "Minerva" cost attribution that:
- Tracks Spark job metadata (executor-hours, shuffle bytes) via SparkListener
- Joins with Airflow task metadata (team, pipeline, DAG)
- Attributes EMR cluster costs at job granularity
- Publishes weekly cost reports to team Slack channels automatically

**Lesson:** Job metadata (not just AWS tags) is essential for sub-cluster attribution.

---

## Problem: Design a High-Scale Event Tracking System — How Segment Does It

**Segment's actual architecture (public case study):**

```
Client SDK → HTTP collect endpoint → Kafka → Centrifuge (internal router)
                                                    │
              ┌─────────────────────────────────────┤
              ▼           ▼           ▼             ▼
           Redshift   S3 Archive  Webhook     Destination APIs
                                  Delivery    (Amplitude, Mixpanel...)
```

**Key insight from Segment:** They struggled with "destination fan-out" — each event must be delivered to potentially 50+ downstream destinations. Their solution:
- Single canonical Kafka topic per workspace
- **Centrifuge** (internal Go service): reads from Kafka, fans out to per-destination queues
- Each destination has its own consumer with independent retry/DLQ
- This decouples destination failures from ingestion reliability

**Interview takeaway:** Fan-out architectures need per-destination isolation. One slow destination should not block others.

---

## Problem: CDC Pipeline at Scale — How LinkedIn Uses Kafka + Brooklin

LinkedIn's Databus/Brooklin system:
- **Brooklin**: open-source CDC framework that reads MySQL binlog and replicates to Kafka
- They replicate **thousands of MySQL tables** across 500+ Kafka topics
- Key innovation: **Mirror Maker 2** for cross-datacenter Kafka replication
- Latency: < 30 seconds from MySQL commit to consumer application

**Real lesson from LinkedIn's experience:**
1. Binlog replication can fall behind during MySQL DDL operations (ALTER TABLE blocks binlog for the duration). Solution: use `gh-ost` or `pt-online-schema-change` for schema migrations.
2. At scale, schema registry becomes critical — without it, consumer breaks on every schema change.

---

## Problem: Uber's Real-Time Data Platform (Pinot + Kafka)

**The problem:** 100M+ trip events/day, analysts need dashboards that refresh in < 5 seconds.

**Why not BigQuery/Redshift?** At Uber's scale, traditional DWs couldn't meet the < 5-second query latency for real-time aggregations.

**Solution: Apache Pinot**
```
Kafka (trip events) → Pinot Realtime Table (CONSUMING segment)
                    → Pinot Offline Table (completed segments from S3)
                    
Query: Pinot Broker merges results from REALTIME + OFFLINE segments
```

**Pinot's key innovation:** Separate real-time and historical segments, merged transparently at query time.

```sql
-- Query runs in < 2 seconds across billions of rows
SELECT city, COUNT(*), AVG(fare_amount)
FROM trips_hybrid  -- Pinot hybrid table
WHERE trip_start_ts > NOW() - 3600  -- last hour
GROUP BY city
ORDER BY COUNT(*) DESC
LIMIT 10
```

**Interview lesson:** When analysts need sub-5-second latency on streaming aggregations, OLAP-on-Kafka (Pinot, Druid, ClickHouse) outperforms cloud DWs.

---

## Common Failure Modes in Real Systems

### 1. The Thundering Herd (Airflow Example)

**Problem:** 200 DAGs all scheduled at midnight. Airflow scheduler gets overloaded, tasks queue up, SLAs missed.

**Real solution:**
```python
# Spread DAG schedules to avoid thundering herd
# Instead of all at "0 0 * * *", spread across the hour

PIPELINE_SCHEDULE_MAP = {
    "orders_daily": "0 0 * * *",        # midnight exactly
    "payments_daily": "5 0 * * *",      # 12:05 AM
    "inventory_daily": "10 0 * * *",    # 12:10 AM
    "marketing_daily": "15 0 * * *",    # 12:15 AM
}
# Plus: use Airflow 2.x Scheduler with high availability (2+ scheduler pods)
```

### 2. Small File Problem (Spark + S3)

**Problem:** Spark Streaming writes one file per micro-batch per partition → millions of 1 MB files → S3 LIST operations become the bottleneck for readers.

**Solution:**
```python
# Compact small files hourly with Delta Lake OPTIMIZE
spark.sql("OPTIMIZE delta.`s3://lake/events/` ZORDER BY (user_id, event_ts)")

# Or: Spark COALESCE before write
df.coalesce(200)  # target 200 files × ~128 MB each = ~25 GB output
  .write.mode("append")
  .parquet("s3://lake/events/")
```

### 3. Schema Drift Breaking Pipelines

**Problem:** Source team adds a required column to a JSON event. Spark job fails with `StructType mismatch`.

**Solution:**
```python
# Defensive Spark schema reading:
df = spark.read \
    .option("mode", "PERMISSIVE") \          # Don't fail on bad rows
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .schema(expected_schema) \               # Enforce expected schema
    .json("s3://raw/events/")

# Alert on corrupt rows
corrupt_count = df.filter(df["_corrupt"].isNotNull()).count()
if corrupt_count > 0:
    alert(f"Schema drift detected: {corrupt_count} corrupt rows")
```

---

## What Senior Interviewers Actually Want to Hear

1. **Failure mode reasoning** — "What breaks first under load?" not just "here's the happy path"
2. **Cost awareness** — "This would cost approximately $X/month at scale"
3. **Operational experience** — Reference real patterns (Debezium, Delta MERGE, WLM queues)
4. **Trade-off ownership** — "I'd choose X over Y because in this context, Z matters more"
5. **Incremental delivery** — "Phase 1: get data flowing. Phase 2: add quality checks. Phase 3: optimize cost"
