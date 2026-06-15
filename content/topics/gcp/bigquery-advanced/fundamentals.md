---
title: "BigQuery Advanced — Fundamentals"
topic: gcp
subtopic: bigquery-advanced
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [gcp, bigquery, partitioning, clustering, interview]
---

# BigQuery Advanced — Fundamentals

BigQuery is Google's fully managed, serverless data warehouse that scales to petabytes. This guide goes beyond the basics into the advanced features that distinguish a junior analyst from a production-ready data engineer: smart table organization, pricing models, security controls, and query optimization techniques that actually move the needle in real jobs.

---

## Partitioning: Dividing Tables for Performance and Cost

Partitioning splits a large table into segments (partitions) based on a column or ingestion time. BigQuery physically stores each partition separately, so queries that filter on the partition column only scan the relevant partitions — reducing both cost and latency.

### Three Partitioning Strategies

**1. Ingestion-time partitioning** — BigQuery automatically assigns rows to a daily (or hourly) partition based on when the row is loaded. You reference partitions using pseudo-columns `_PARTITIONTIME` (TIMESTAMP) and `_PARTITIONDATE` (DATE).

```sql
-- Create an ingestion-time partitioned table
CREATE TABLE `project.dataset.events`
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (
  partition_expiration_days = 90
) AS
SELECT * FROM `project.dataset.raw_events`;

-- Query: only scans the Nov 1 partition
SELECT user_id, event_type
FROM `project.dataset.events`
WHERE _PARTITIONDATE = '2024-11-01';
```

**2. Column partitioning** — partition by a DATE, DATETIME, TIMESTAMP, or INTEGER column that already exists in your data. This is preferred when you have a meaningful event timestamp in your data.

```sql
CREATE TABLE `project.dataset.orders`
PARTITION BY order_date  -- DATE column
OPTIONS (require_partition_filter = TRUE)
AS SELECT * FROM source;
```

Setting `require_partition_filter = TRUE` forces all queries to include a partition filter — a useful cost guardrail in multi-team environments.

**3. Range partitioning** — partition by an INTEGER column with explicit RANGE buckets. Used for things like user ID ranges or numeric event codes.

```sql
CREATE TABLE `project.dataset.user_events`
PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 10000000, 1000000))
AS SELECT * FROM source;
```

### Partition Limits

- Max 4,000 partitions per table (ingestion-time and column partitioned).
- Range partitioned tables: up to 10,000 partitions.
- Exceeding these limits requires redesigning your partitioning strategy (e.g., coarser granularity or table sharding).

---

## Clustering: Sorting Within Partitions

Clustering sorts the data within each partition (or across an unpartitioned table) by up to four columns. BigQuery uses block-level metadata to skip blocks that don't match filter predicates — no index to maintain, automatic re-clustering happens in the background.

```sql
CREATE TABLE `project.dataset.events`
PARTITION BY event_date
CLUSTER BY customer_id, event_type
AS SELECT * FROM source;
```

**When to cluster:** On columns you frequently filter or GROUP BY that have high cardinality but not so high that every value is unique (e.g., `customer_id`, `country`, `product_category`).

**Key interview point:** Clustering benefits compound with partitioning. A query filtering on `event_date` (partition) AND `customer_id` (cluster column) gets double pruning — partition eliminates entire date segments, clustering eliminates blocks within those segments.

---

## Pricing Models: On-Demand vs. Slot Reservations

Understanding BigQuery's two billing models is critical for cost architecture decisions.

### On-Demand Pricing

Pay per TB of data scanned. As of 2024, ~$5/TB in most regions. Advantages: zero upfront commitment, perfect for sporadic or unpredictable workloads. Disadvantage: a single rogue query scanning a 100TB table costs $500.

### Slot Reservations (Capacity Pricing)

You purchase slots (units of compute — 1 slot ≈ 1 vCPU of BigQuery processing capacity). Purchased slots are reserved exclusively for your projects/folders.

- **Standard editions**: slots come in Standard, Enterprise, and Enterprise Plus tiers with different commitments (flex, 1-year, 3-year).
- **Assignments**: you assign reserved slots to projects or folders via reservations and assignments in the BigQuery Admin panel.

```
# Conceptual reservation hierarchy:
RESERVATION: "prod-reservation" (500 slots, Enterprise)
  └─ ASSIGNMENT: project "data-warehouse-prod" uses this reservation
  └─ ASSIGNMENT: folder "analytics-team" uses this reservation
```

**When to switch:** If your monthly on-demand bill exceeds the cost of committed slots, reservations save money. Rule of thumb: ~100+ TB/month is a reasonable threshold to evaluate reservations.

---

## BI Engine: In-Memory Acceleration

BigQuery BI Engine is an in-memory analysis service that caches frequently accessed data to serve sub-second queries for dashboards (Looker Studio, Connected Sheets, Looker). You reserve memory capacity (1–250 GB per project) and BI Engine automatically decides what to cache.

```sql
-- BI Engine works transparently — no SQL changes needed.
-- Enable via Console or gcloud:
-- gcloud alpha bq bi-engine reserve --location=US --capacity=10
```

BI Engine is not a replacement for good table design — it works best on aggregated or filtered subsets, not full-table scans.

---

## Materialized Views

Materialized views pre-compute and store query results, refreshing automatically (incremental refresh) as base tables change. Unlike regular views, materialized views consume storage but dramatically accelerate repeated aggregations.

```sql
CREATE MATERIALIZED VIEW `project.dataset.daily_revenue_mv`
PARTITION BY order_date
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS
SELECT
  order_date,
  product_id,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `project.dataset.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
GROUP BY 1, 2;
```

**Incremental refresh** means BigQuery only processes new/changed data in the base table since the last refresh — very efficient for append-only tables. Materialized views also participate in **smart tuning**: even if a query doesn't reference the MV directly, BigQuery's query optimizer may rewrite it to use the MV automatically.

---

## Key Interview Concepts to Know

| Concept | One-Line Summary |
|---|---|
| Partition pruning | Filter on partition column → BigQuery skips non-matching partitions entirely |
| Clustering pruning | Filter on cluster columns → BigQuery skips non-matching blocks within partitions |
| `require_partition_filter` | Forces queries to include a partition filter — cost governance |
| BI Engine | In-memory cache for dashboard acceleration |
| Materialized views | Pre-computed aggregations with automatic incremental refresh |
| Slot reservations | Fixed compute capacity purchased upfront; predictable cost |
| On-demand | Pay per TB scanned; unpredictable but zero commitment |

---

## Common Junior Mistakes

1. **Not setting `require_partition_filter`** on large tables — one unfiltered query from a BI tool can scan the entire table.
2. **Clustering on low-cardinality columns** (like boolean flags) — clustering has minimal benefit when there are only 2 distinct values.
3. **Over-partitioning** — choosing hourly partitions on a table that's only queried daily leads to fragmentation and slower metadata operations.
4. **Confusing partitioning with sharding** — table sharding (separate tables per date like `events_20241101`) is legacy; prefer partitioned tables for modern BQ workloads.
