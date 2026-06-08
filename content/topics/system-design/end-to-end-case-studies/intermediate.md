---
title: "End-to-End Case Studies — Intermediate"
topic: system-design
subtopic: end-to-end-case-studies
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, case-study, e-commerce, ride-sharing, architecture]
---

# End-to-End Case Studies — Intermediate

## Case Study 1: E-Commerce Analytics Platform

**Requirements:**
- 10M orders/day, 500M page views/day
- BI dashboards updated by 7 AM daily
- Real-time product recommendation API (< 100ms)
- 3 years of history for trend analysis

**Architecture:**

```
Sources:
  PostgreSQL (orders, customers, products) → Fivetran CDC → Kafka
  Web analytics (page views, clicks) → Kinesis (10K events/sec peak)
  Inventory system → S3 file drops (hourly)

Ingestion:
  Kafka: orders.events, customer.events (RF=3, 24-hr retention)
  Kinesis: 30 shards × 1MB/s = 30MB/s capacity for web events
  S3: raw file landing → trigger Lambda → copy to raw zone

Bronze Layer (S3 + Delta Lake):
  s3://data-lake/bronze/orders/      (partitioned by ingestion_date)
  s3://data-lake/bronze/pageviews/   (partitioned by event_date)
  Format: Parquet + snappy, 1GB target file size

Silver Layer (Delta Lake):
  Spark Structured Streaming job (10-min micro-batches for orders)
  Batch Spark job at 1 AM for daily aggregations
  Tables: orders_clean, customers_clean, product_views_clean
  - Schema enforced, nulls handled, deduped
  Partitioned by: (event_date, region)

Gold Layer (Snowflake):
  dbt daily run at 2 AM:
    fct_orders, dim_customer, dim_product, dim_date
    rpt_daily_revenue, rpt_product_performance, rpt_customer_clv
  Snowflake cluster key: (order_date, region) for BI queries

Serving:
  BI: Tableau reads Snowflake gold tables (dashboards ready by 7 AM ✓)
  Recommendations API: 
    Spark batch feature computation → Redis (product embeddings)
    API server: Redis lookup for real-time recommendations (< 5ms)
    Redis: 16GB in-memory cache for top 1M products

Data Quality:
  dbt tests: not_null, unique, accepted_values on all dim tables
  Great Expectations checkpoint at Silver layer entry (assert row count > 0)
  Freshness check: alert if Snowflake not updated by 6:30 AM
```

---

## Case Study 2: Real-Time Ride-Sharing Analytics

**Requirements:**
- 500K ride events/minute (request, start, end, payment)
- Driver earnings dashboard (< 1 minute refresh)
- Surge pricing input: rides per zone per 5 minutes
- Daily/monthly driver payout reports (exact, auditable)

**Architecture:**

```
Sources:
  Mobile apps (driver + rider) → Kafka (ride.events, 100 partitions)
  Payment service → Kafka (payment.events)
  GPS service → Kafka (location.updates, 200 partitions)
  
Scale: 500K events/min = 8,333/sec; at 500 bytes = 4.2 MB/sec
Kafka: 5 brokers, 100 partitions → 100 consumer parallelism

Streaming Pipeline (Flink, 30-second trigger):
  ride.events → per-driver state (earnings, active rides)
  → Redis HASH: driver:12345 → {earnings_today: 142.50, rides_today: 18}
  → Driver earnings dashboard reads Redis (sub-second refresh ✓)

Surge Pricing Pipeline (Flink, 5-minute tumbling window):
  ride.events → count requests per zone per 5-min window
  → surge_zones table (ClickHouse, key-value update)
  → Surge pricing service reads ClickHouse (< 10ms lookup)
  
Historical Pipeline (Spark Structured Streaming → Delta):
  All events → Delta Lake (bronze: partitioned by event_date)
  dbt daily: Silver tables, Gold payout summaries
  Snowflake: monthly payout reports (auditable, exact ✓)
  
Reliability:
  Kafka: RF=3, acks=all, min.insync.replicas=2 → zero data loss
  Flink: checkpoints every 30 sec to S3 → recovery in < 2 min
  Redis: Redis Sentinel (HA, failover in < 30 sec)
  DLQ: failed events → Kafka topic rides.dlq (alert if DLQ > 0)
  
Cost:
  Kafka: self-managed on EC2 (5 × r5.2xlarge: ~$1,200/month)
  Flink: 10 TaskManager pods on Kubernetes (auto-scale)
  Delta: S3 Standard, lifecycle to IA after 90 days
  Snowflake: 2 warehouses (ETL: M, BI: L, auto-suspend 60s)
```

---

## Case Study 3: SaaS Product Analytics Pipeline

**Requirements:**
- 50 SaaS customers, each with 100K-10M users
- Usage tracking: feature clicks, API calls, logins
- Per-customer dashboards (customer can see their own data only)
- ML: churn prediction model (retrained weekly)

**Architecture:**

```
Sources:
  Application events → Kafka (events.usage, partitioned by tenant_id)
  PostgreSQL (subscriptions, accounts) → Debezium CDC → Kafka
  
Multi-tenancy:
  Kafka: partition by tenant_id → same consumer group; tenant isolation in code
  Snowflake: Row Access Policy by tenant_id (data cannot cross tenant boundary)
  S3: prefix structure = /tenant_id=XXX/event_date=YYYY-MM-DD/
  
Bronze (S3, per-tenant prefix):
  Firehose → s3://events-lake/bronze/tenant_id={}/event_date={}/*.parquet
  Retention: 2 years (contractual requirement)
  
Silver (Delta Lake):
  Spark Streaming: 5-min micro-batch per tenant
  Dedup, type-cast, null handling
  Per-tenant statistics: events/day, active users
  
Gold (Snowflake):
  dbt daily: usage_daily_by_tenant, feature_adoption, user_activity_360
  Row Access Policy: tenant_admin role can only see own tenant_id rows
  
Customer-facing dashboard:
  Tableau Embedded → passes tenant_id filter → Snowflake RLS enforces isolation
  
ML Churn Prediction:
  Spark batch: feature extraction from Silver (last 90 days per user)
  Feature store: Feast (user_id → {days_since_login, feature_usage_rate, ...})
  Databricks MLflow: weekly retraining → model registry → inference job
  Churn scores → Snowflake gold table → customer success team dashboard
  
Data Quality:
  per-tenant row count alerts (if tenant data drops >50% from prior day)
  dbt: foreign key tests between usage and subscriptions
  Freshness SLO per tenant: 24-hour freshness guaranteed
```

---

## Interview Tips

> **Tip 1:** "How do you handle multi-tenancy in a shared data platform?" — At each layer: (1) Storage: partition by tenant_id in S3 (physical separation). (2) Processing: include tenant_id in all operations; filter by tenant_id in incremental jobs. (3) Serving: row-level security in Snowflake/BigQuery (row access policy by tenant_id). (4) Access: service accounts per tenant for external connections. Design principle: tenant data should never be visible to another tenant at any layer.

> **Tip 2:** "How do you design for both real-time and batch consumers from the same data?" — Write to Delta Lake / Iceberg: streaming writes from Spark Structured Streaming, batch reads from Spark/SQL/dbt. Delta's ACID transactions make both work correctly from the same table. Real-time consumers can use streaming reads (readStream) or a cache layer (Redis) populated by the streaming job. Batch consumers run SQL against the same Delta table. No separate batch and speed layers needed.

> **Tip 3:** "How would you describe the trade-off between Kafka and Kinesis?" — Kafka: open source, highly tunable, self-managed (or Confluent/MSK managed), unlimited retention, more ecosystem integrations. Kinesis: fully managed AWS service, integrates natively with AWS (Lambda, Firehose, Analytics), limited to 7-day retention, 1MB/sec per shard. Choose Kinesis for: pure AWS shops wanting zero ops, simple pipelines. Choose Kafka for: cross-cloud portability, complex stream processing (Flink/ksqlDB), long retention requirements, or when Kafka skills already exist on the team.
