---
title: "Cost Optimization — Senior Deep Dive"
topic: system-design
subtopic: cost-optimization
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, cost-optimization, unit-economics, cost-anomaly, architecture-review]
---

# Cost Optimization — Senior Deep Dive

## Unit Economics for Data Platforms

```
Unit economics: cost per meaningful business unit
  Instead of: "we spend $50,000/month on data infrastructure"
  Think:       "we spend $0.12 per 1,000 processed events"
               "we spend $2.50 per active customer per month"
               "we spend $0.003 per BI query answered"

Why unit economics matter:
  - Validate: is infrastructure cost growing faster than business growth?
  - Communicate: business stakeholders understand $/customer, not $/GB
  - Detect: sudden spike in $/event signals an inefficiency, not volume growth

Calculating unit economics:
  cost_per_event = monthly_compute_cost / monthly_event_count
  cost_per_active_user = monthly_platform_cost / monthly_active_users
  cost_per_query = monthly_dw_cost / monthly_query_count

Target: unit cost should stay flat or decrease as volume grows
  (economies of scale: 10× volume should NOT mean 10× cost)
  If unit cost grows: architecture doesn't scale efficiently
```

---

## Cost Anomaly Detection

```python
# Detect unexpected cost spikes before monthly bill arrives

import boto3
from datetime import datetime, timedelta

def check_cost_anomalies():
    client = boto3.client('ce', region_name='us-east-1')
    
    # Get daily costs for last 30 days
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'End': datetime.now().strftime('%Y-%m-%d')
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )
    
    # Calculate 7-day moving average for each service
    costs_by_day = parse_cost_response(response)
    for service, daily_costs in costs_by_day.items():
        avg_7d = sum(daily_costs[-8:-1]) / 7  # last 7 days avg
        yesterday = daily_costs[-1]
        
        if yesterday > avg_7d * 2.0:   # >100% spike
            alert(f"COST SPIKE: {service} spent ${yesterday:.0f} yesterday vs ${avg_7d:.0f} avg")
        if yesterday > 1000:           # absolute threshold: >$1000/day for any service
            alert(f"COST HIGH: {service} spent ${yesterday:.0f} yesterday")

# AWS Cost Anomaly Detection (managed service):
# Automatically detects unusual spend patterns
# Set up via: AWS Console → Cost Management → Cost Anomaly Detection
# Configure: min threshold ($100), alert via SNS/email
```

---

## Architecture-Level Cost Decisions

```
Trade-off analysis: performance vs cost

1. Compute-optimized vs storage-optimized instances:
   Compute-optimized (c5, m5): high CPU/RAM ratio, expensive
   Storage-optimized (d3, i3): high disk, cheaper per GB
   DE rule: use compute for Spark processing; storage for Kafka brokers

2. Cloud DW vs self-managed:
   Snowflake: $2-4/credit — expensive per query, no ops overhead
   Self-managed ClickHouse on EC2: $0.20/hour — cheap, requires ops
   Decision: Snowflake for orgs with <5 DEs (ops cost > compute cost)
             ClickHouse for high-volume analytics (>10B rows/day)

3. Managed streaming vs self-managed Kafka:
   Confluent Cloud: $0.11/GB + $2.50/hour CKU
   MSK (AWS Managed Kafka): ~$0.21/hour per broker
   Self-managed on EC2: $0.10/hour per broker + ops time
   Break-even: self-managed wins above ~20 GB/hour sustained throughput

4. Batch frequency vs cost:
   Hourly batch: 24× more compute than daily batch
   Question: does business need hourly freshness? Or daily is fine?
   Common finding: 80% of BI dashboards only need daily refresh
   Action: move non-critical pipelines from hourly → daily → 6× compute reduction

5. Data lake vs DW for all analytics:
   Store raw data in S3 ($0.023/GB) not Snowflake ($0.046/GB for storage)
   Query Snowflake only for curated tables (gold layer)
   External tables: Snowflake can query S3 directly when needed (rare, for ad-hoc)
```

---

## Cost Optimization Framework: Where to Start

```
Prioritized cost optimization checklist (biggest impact first):

1. Idle resources (quick win, low risk):
   □ Auto-suspend Snowflake warehouses (60 seconds)
   □ Terminate unused dev/test clusters
   □ Delete S3 files with no recent access (S3 Storage Lens to find)
   Typical savings: 20-30% of total cost

2. Storage optimization (medium effort):
   □ Enable compression (zstd/Parquet) on new pipelines
   □ S3 lifecycle policies (IA after 30d, Glacier after 90d)
   □ Reduce Snowflake Time Travel on staging tables (0 days)
   □ Run Delta VACUUM to clean up old versions
   Typical savings: 15-25% of storage cost

3. Query optimization (high impact, requires profiling):
   □ Add partition filters to top 20 most expensive queries
   □ Replace SELECT * with explicit column lists
   □ Add Snowflake cluster keys / BigQuery partition+cluster
   Typical savings: 30-50% of compute cost

4. Architecture changes (high impact, high effort):
   □ Right-size all clusters based on actual utilization
   □ Move batch workers to spot instances
   □ Shift from hourly to daily batch where freshness SLO allows
   □ Move rarely-queried historical data from DW to data lake
   Typical savings: 40-60% of compute cost

Monthly cost review process:
  1. Pull cost report by team, service, and resource
  2. Identify top 10 most expensive resources
  3. For each: is the spend justified? Is utilization >50%?
  4. Assign action items with owners and deadlines
  5. Track unit economics (cost per event/user) trend over time
```

---

## Interview Tips

> **Tip 1:** "How would you reduce the data platform cost by 40% without impacting SLAs?" — Start with the checklist: (1) Enable auto-suspend on all warehouses — saves 20-30% immediately with zero impact. (2) Audit idle resources (dev clusters, forgotten pipelines). (3) Move batch workers to spot — 60% compute savings. (4) Review pipeline schedules — downgrade non-critical hourly → daily. (5) Run top expensive query optimization. In total: these five actions typically achieve 40%+ savings within 30 days without changing SLAs.

> **Tip 2:** "How do you communicate infrastructure costs to non-technical stakeholders?" — Translate to business units they care about: "We process X million orders/month at $Y per 1,000 orders. As order volume grows 30% next quarter, cost will grow ~10% (not 30%) because we've optimized the platform." Connect cost to business outcomes. Show unit cost trends (not absolute). If cost is growing faster than business: show specific inefficiencies. If cost is flat while business grows: show the leverage.

> **Tip 3:** "A team's Snowflake bill doubled this month. How do you investigate?" — Query `snowflake.account_usage.query_history` grouped by user, warehouse, and day. Compare this month vs last month: which user/warehouse/query drove the increase? Common findings: (1) a new ad-hoc analyst running `SELECT *` on large tables, (2) a new pipeline running more frequently, (3) a clustering key became stale and queries started scanning more micro-partitions, (4) warehouse was accidentally upgraded from M to XL. Fix: add query budget per user, require partition filters, tune warehouse sizes.

## ⚡ Cheat Sheet

**System design framework (DE interviews)**
```
1. Clarify requirements: batch or streaming? latency SLA? scale (rows/day)?
2. Define data flow: source → ingest → transform → serve → consume
3. Choose storage: DW (structured), Data Lake (raw), Lakehouse (both)
4. Choose compute: Spark/Flink for scale; dbt for SQL transforms; Airflow for orchestration
5. Define SLAs: freshness (15 min? 1 hr?), uptime (99.9%?), cost budget
6. Address failure modes: what breaks? how do you detect and recover?
```

**Lambda vs Kappa architecture**
```
Lambda:
  Batch layer:  reprocesses all historical data on a schedule (accurate)
  Speed layer:  processes recent data in real-time (approximate)
  Serving:      merges batch + speed views for queries
  Problem:      two codebases for same logic; complex to maintain

Kappa:
  Streaming only:  one pipeline handles both real-time and reprocessing
  Reprocessing:    replay Kafka from beginning with new consumer group
  Advantage:       single codebase; simpler ops
  Requirement:     Kafka retention must cover reprocessing window
```

**Scalability patterns**
```
Horizontal partitioning:  Kafka partitions, HDFS blocks, table partitions
Data skipping:            Z-ordering, bloom filters, min/max statistics
Push down:                predicates + projections to storage layer
Caching:                  result cache (Snowflake, Databricks SQL), Redis for lookups
Async processing:         decouple ingestion from transformation via message queue
```

**Fault tolerance patterns**
```
Idempotency:     safe to re-run; same output for same input
Checkpointing:   Flink/Spark saves progress; restart from last checkpoint
Dead letter:     failed records go to DLQ for inspection and replay
Circuit breaker: stop pipeline on repeated failures; alert before resuming
Retry with backoff: exponential backoff + jitter for transient failures
Exactly-once:    Kafka + Flink + Delta = end-to-end exactly-once
```

**Cost optimization levers**
```
Compute:
  - Spot/preemptible instances (60-80% cheaper; need checkpointing)
  - Auto-suspend warehouses (pay only when active)
  - Right-size: XL warehouse for batch; S for ad hoc
Storage:
  - Partition + vacuum old snapshots
  - Lifecycle policies: S3 IA after 30 days, Glacier after 1 year
  - Compression: ZSTD > Snappy (better ratio, acceptable CPU cost)
Query:
  - Columnar reads (never SELECT *)
  - Materialized views for expensive repeated aggregations
  - Result cache (Snowflake caches identical queries for 24h)
```

**Data warehouse design checklist**
```
□ Star schema with conformed dimensions
□ Surrogate keys on all dimensions
□ Fact table: numeric measures + FK references only
□ SCD2 on slowly changing dimensions
□ Partition on query predicate (date, region)
□ Cluster/Z-order on high-cardinality filter columns
□ Row counts + DQ checks at each medallion layer boundary
□ Freshness SLA defined and monitored for each gold table
□ Data lineage captured (dbt docs, OpenLineage)
□ Access control: role-based + column masking for PII
```

**Trade-off framework**
```
Latency vs throughput:    streaming (low latency, lower throughput) vs batch (high throughput, higher latency)
Consistency vs availability: strong consistency (slower, single writer) vs eventual (faster, multi-write)
Cost vs freshness:        real-time = expensive compute; hourly batch = cheap; choose based on business SLA
Simplicity vs flexibility: managed service (easy ops) vs self-managed (full control, higher ops burden)
Storage vs compute:       pre-aggregate (storage cost, fast queries) vs compute on demand (fresh data, slower)
```
