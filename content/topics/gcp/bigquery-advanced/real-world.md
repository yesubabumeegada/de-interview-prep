---
title: "BigQuery Advanced — Real-World Patterns"
topic: gcp
subtopic: bigquery-advanced
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [gcp, bigquery, production, patterns, interview]
---

# BigQuery Advanced — Real-World Patterns

These are the patterns, failure modes, and production decisions that distinguish engineers who have actually run BigQuery at scale from those who've only read the documentation. Each section reflects common interview scenarios drawn from real production environments.

---

## Pattern 1: The Slot Starvation Incident

**Scenario**: Monday morning, 50 analysts kick off dashboard refreshes simultaneously. The on-demand pricing tier handles it fine (each query gets up to 2,000 slots), but after switching to a 500-slot reservation to save costs, the Monday morning rush takes 10x longer.

**Root cause**: 500 slots divided among 50 concurrent queries = 10 slots per query average. Queries that ran in 30 seconds on on-demand now take 5 minutes.

**Solutions applied in production**:

1. **Tier workloads**: separate reservations for interactive queries (analysts) vs. batch ETL, with different slot allocations.
2. **BI Engine for dashboards**: common dashboard queries served from in-memory cache, eliminating slot consumption entirely.
3. **Query scheduling**: shift heavy ETL to off-peak hours (2-6am), freeing peak slots for analysts.
4. **Autoscaling**: Enterprise+ tier autoscaler configured with `max_slots = 2000` to burst during morning rush and scale down by midday.

```bash
# Set up autoscaling on a reservation
gcloud alpha bigquery reservations update prod-reservation \
  --location=US \
  --autoscale-max-slots=2000
```

---

## Pattern 2: Partition Design for a 10-Year Event Log

**Scenario**: You're designing storage for a user event log expected to hold 10 years of data (~5TB/month, ~600TB total). The primary access patterns are:
- Daily batch: process yesterday's events (last 1 day)
- Ad-hoc analytics: date range queries (last 30/90 days)
- Compliance: full history queries (rare, acceptable to be slow)

**Design decisions**:

```sql
-- Final table design
CREATE TABLE `project.events_warehouse.user_events`
(
  event_id STRING NOT NULL,
  user_id INT64,
  event_type STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,   -- materialized date for partition key
  properties JSON
)
PARTITION BY event_date
CLUSTER BY user_id, event_type
OPTIONS (
  partition_expiration_days = 3650,  -- 10-year retention
  require_partition_filter = TRUE    -- prevent full-table scans
);
```

**Why `event_date` as DATE column instead of ingestion-time**: ingestion-time partitions by load time, not event time. Late-arriving events from mobile clients (events sent 2 hours after occurrence) go into the wrong partition. A materialized DATE column ensures events partition by when they happened, not when they arrived.

**Partition filter enforcement**: `require_partition_filter = TRUE` means Looker dashboards and ad-hoc queries MUST include a date filter. Developers learn quickly — and it prevents accidental 600TB scans.

---

## Pattern 3: Multi-Region Compliance Architecture

**Scenario**: GDPR requires EU customer data to stay in EU; CCPA requires US customer data to stay in US. But analysts need to query both.

**Architecture**:

```
EU dataset: project.events_eu (location=europe-west4)
US dataset: project.events_us (location=us-central1)

BigQuery Omni option: not applicable (same cloud)
Cross-region joins: NOT possible in BigQuery (different locations)

Solution: Regional summary tables replicated to a single location
```

```sql
-- Daily job: aggregate EU data to a summary (no PII) → replicate to US
-- Run in EU region:
CREATE OR REPLACE TABLE `project.summaries_eu.daily_revenue` AS
SELECT
  event_date,
  product_category,
  COUNT(*) AS event_count,
  SUM(revenue) AS total_revenue
  -- NO user_id, email, or other PII
FROM `project.events_eu.transactions`
WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1, 2;

-- Transfer job: copy summary (no PII) from EU to a multi-region US dataset
# bq cp --location=US project:summaries_eu.daily_revenue project:global_summaries.daily_revenue_eu
```

The key insight interviewers probe: **cross-region joins are impossible in BigQuery**. You must either co-locate data (moving it, which has compliance implications) or use pre-aggregated/anonymized summaries that can safely move regions.

---

## Pattern 4: Row-Level Security in Practice

**Scenario**: A sales analytics table must be visible to all sales reps, but each rep should only see their own territory's rows.

```sql
-- Create policy for each territory
CREATE OR REPLACE ROW ACCESS POLICY emea_policy
ON `project.sales.pipeline`
GRANT TO ("group:emea-sales@company.com")
FILTER USING (territory = 'EMEA');

CREATE OR REPLACE ROW ACCESS POLICY apac_policy
ON `project.sales.pipeline`
GRANT TO ("group:apac-sales@company.com")
FILTER USING (territory = 'APAC');

-- Revenue ops team sees everything
CREATE OR REPLACE ROW ACCESS POLICY revenue_ops_policy
ON `project.sales.pipeline`
GRANT TO ("group:revenue-ops@company.com")
FILTER USING (TRUE);
```

**Gotcha**: if a sales rep is in BOTH `emea-sales` and `apac-sales` groups, they see BOTH EMEA and APAC rows. Row access policies are unioned for users matching multiple policies — there's no intersection/deny. This means role management must be strict: no one should be in multiple territory groups unless they legitimately need cross-territory visibility.

**Another gotcha**: COUNT(*) in a report will return different numbers for different users because RLS filters before aggregation. Document this explicitly to avoid support tickets about "wrong numbers."

---

## Pattern 5: BigQuery Storage Read API in Spark Pipelines

**Scenario**: A Spark pipeline on Dataproc needs to join a 10TB BigQuery table with 100GB of enrichment data from another BQ table, then write results back to BigQuery.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("large-bq-join") \
    .config("viewsEnabled", "true") \
    .config("materializationDataset", "project.temp_views") \
    .getOrCreate()

# Large fact table: Storage Read API with predicate pushdown
events = spark.read.format("bigquery") \
    .option("table", "project.dataset.events") \
    .option("filter", "event_date = '2024-01-01'") \   # pushed to BQ
    .option("selectedFields", "user_id,event_type,revenue") \  # column pruning
    .option("readDataFormat", "ARROW") \
    .load()

# Small dimension table: broadcast join candidate
users = spark.read.format("bigquery") \
    .option("table", "project.dataset.users") \
    .load()

from pyspark.sql.functions import broadcast
result = events.join(broadcast(users), "user_id")

# Write back to BigQuery
result.write.format("bigquery") \
    .option("table", "project.dataset.enriched_events") \
    .option("temporaryGcsBucket", "my-temp-bucket") \
    .option("partitionField", "event_date") \
    .mode("append") \
    .save()
```

**Production lesson**: always set `selectedFields` and `filter` options — the connector will push these down to BigQuery Storage API, dramatically reducing data transferred to Spark executors. A 10TB table filtered to 1 day and 3 columns might only transfer 50GB to Spark.

---

## Pattern 6: INFORMATION_SCHEMA for Proactive Cost Governance

**Production monitoring query** — run daily via Cloud Scheduler → BigQuery → alert if thresholds exceeded:

```sql
-- Daily cost report with anomaly detection
WITH daily_costs AS (
  SELECT
    DATE(creation_time) AS query_date,
    user_email,
    SUM(total_bytes_processed) / POW(1024, 4) AS tb_processed,
    COUNT(*) AS query_count,
    AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS avg_duration_sec
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND job_type = 'QUERY'
    AND state = 'DONE'
  GROUP BY 1, 2
),
anomalies AS (
  SELECT
    query_date,
    user_email,
    tb_processed,
    -- Flag if today's usage is >3x the 30-day average for this user
    AVG(tb_processed) OVER (PARTITION BY user_email ORDER BY query_date
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING) AS avg_30d,
    tb_processed / NULLIF(
      AVG(tb_processed) OVER (PARTITION BY user_email ORDER BY query_date
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING), 0) AS spike_ratio
  FROM daily_costs
)
SELECT * FROM anomalies
WHERE spike_ratio > 3 AND query_date = CURRENT_DATE() - 1
ORDER BY tb_processed DESC;
```

This query powers automated Slack alerts for unusual spend patterns — a common production data platform feature that interviewers recognize as a sign of operational maturity.

---

## Common Production Pitfalls Summary

| Pitfall | Impact | Fix |
|---|---|---|
| No `require_partition_filter` | Full table scans from BI tools | Enable it; create filtered views for BI tools |
| Materialized views with window functions | Silent fallback to full refresh | Use `allow_non_incremental_definition=FALSE` |
| Row access policies without documentation | Teams confused by different row counts | Document RLS; create a data catalog entry |
| Storage Read API without `selectedFields` | Full column scan sent to Spark | Always specify column projection |
| Slot reservation undersizing | Monday morning slowdowns | Size for peak + configure autoscaling |
| BigQuery Omni without data residency check | Compliance risk | Verify Omni processes data in origin cloud |
