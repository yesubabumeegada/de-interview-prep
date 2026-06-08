---
title: "End-to-End Case Studies — Scenarios"
topic: system-design
subtopic: end-to-end-case-studies
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, case-study, interview, scenarios, design]
---

# End-to-End Case Studies — Interview Scenarios

## Scenario 1: Design a Data Platform for a Food Delivery App

**Question:** Design the complete data engineering infrastructure for a food delivery app. 5M orders/day, 50K restaurants, 10M users. Need: real-time driver location tracking, daily finance reports, ML model for delivery time estimation.

**Answer:**

```
Clarifying questions answered:
  Scale: 5M orders/day = 58 orders/sec avg, 500/sec peak
  Latency: driver location < 5 sec, finance reports by 6 AM, ETA predictions < 200ms
  Consumers: BI (finance, ops), ML (ETA model), customer-facing (tracking app)
  Compliance: PCI DSS for payment data, GDPR for EU users

Architecture:

1. Real-Time Driver Tracking (< 5 sec):
   Mobile app → WebSocket → Location Service → Kafka (location.updates, 200 partitions)
   Flink (5-second window) → materialize per-driver state
   → Redis (driver:12345 → {lat, lng, status}) → Tracking API → Customer app
   Scale: 100K active drivers × 1 update/5sec = 20K events/sec (fine on 200 partitions)

2. Order Event Pipeline:
   Order service → Kafka (orders.events, 50 partitions)
   Events: order_placed, restaurant_accepted, driver_assigned, picked_up, delivered, cancelled
   
   Real-time: Flink 1-min micro-batch → Redis (per-restaurant active order count for ops dashboard)
   
   Historical: Spark Structured Streaming → Delta Lake (Bronze → Silver)
   Silver: orders_clean, events_clean (deduped, typed)
   Partitioned by (order_date, city)

3. Daily Finance Reports (by 6 AM):
   dbt daily run at 2 AM:
     fct_orders (grain: one order line item, includes restaurant_fee, delivery_fee, tip)
     dim_restaurant (SCD Type 2 — commission rate changes)
     dim_driver (SCD Type 2 — tier changes)
     rpt_restaurant_payouts, rpt_driver_earnings, rpt_platform_revenue
   
   Snowflake + Tableau for finance team
   SLA monitoring: alert at 5:30 AM if any report not ready

4. ML: Delivery Time Estimation:
   Feature engineering (Spark daily):
     restaurant_features: avg prep time by hour, restaurant_id
     zone_features: avg delivery time by zone, time_of_day
     weather_features: precipitation, traffic from external API
   
   Feature Store: Feast (Redis for online, S3 for offline)
   Training: weekly on last 90 days of completed deliveries (Databricks)
   Serving: MLflow Model Registry → FastAPI model server (<200ms response ✓)
   Input: order details + current feature store values → predicted ETA

5. Data Quality:
   dbt tests: not_null on order_id, unique constraint, referential integrity
   Volume check: if daily orders < 80% of 7-day average → alert
   Freshness: Silver tables must refresh within 30 min of order event
```

---

## Scenario 2: Migrate from On-Premises DW to Cloud

**Question:** A company has a 10-year-old on-premises Teradata DW. 50TB of data. 200 business users. 500 ETL jobs. Migration timeline: 12 months. Design the migration strategy.

**Answer:**

```
Migration Strategy: Lift, Shift, and Optimize

Phase 1: Foundation (Month 1-2)
  Set up cloud infrastructure: Snowflake + S3 + Airflow on MWAA
  Configure SSO, networking (PrivateLink), security groups
  Set up monitoring (Datadog), cost alerting
  Establish naming conventions and tagging standards

Phase 2: Extract and Replicate Data (Month 2-4)
  Export Teradata tables to S3 (via Teradata QueryGrid → S3 or BTEQ export)
  Prioritize: top 20 most-used tables (identified via query logs)
  Load to Snowflake staging: CREATE TABLE ... AS SELECT (CTAS)
  Validate: row counts, sum of key metrics match Teradata source
  Keep Teradata as system of record during migration

Phase 3: Migrate ETL Jobs (Month 4-9)
  Categorize 500 jobs:
    Simple SQL transformations (60%): migrate to dbt models (auto-convert most ANSI SQL)
    Complex stored procedures (25%): rewrite in Python/Spark
    External integrations (15%): reconnect to new endpoints
  
  Migration approach per job:
    1. Run on Teradata (production)
    2. Run on Snowflake (shadow/validation)
    3. Compare outputs (row counts, totals, spot checks)
    4. Fix discrepancies
    5. Switch production to Snowflake version
    6. Decommission Teradata version after 2-week stability period

Phase 4: Migrate Users (Month 9-11)
  BI tool migration: Tableau → same Tableau, reconnect to Snowflake (update connection string)
  User training: Snowflake SQL differences (TOP vs LIMIT, DATE functions)
  Performance tuning: add cluster keys for slow queries identified in first weeks
  
  Cutover plan:
    Week 1: 10 power users on Snowflake (catch issues early)
    Week 2-3: 50% of users migrated
    Week 4: all 200 users on Snowflake
    Week 6: Teradata read-only (fallback)
    Week 8: Teradata decommissioned

Phase 5: Optimize (Month 11-12)
  Right-size Snowflake warehouses based on 2 months of actual usage
  Enable auto-clustering on largest tables
  Set up dbt documentation and tests (missing from old Teradata ETL)
  Cost validation: target <60% of old Teradata licensing + hardware cost

Risk Mitigation:
  Never big-bang: always run old and new in parallel
  Keep Teradata until Snowflake proven stable for 4 weeks
  Rollback plan: each job maintains Teradata version until decommission
  Executive sponsor: migrations fail without business buy-in and timeline ownership
```

---

## Scenario 3: Design a Feature Store for an ML Platform

**Question:** Your ML team has 20 data scientists. Each builds their own feature pipelines, causing duplication and inconsistency. Design a centralized feature store that serves both training and real-time inference.

**Answer:**

```
Requirements:
  Training: point-in-time correct feature vectors (no future leakage)
  Inference: sub-10ms feature retrieval for real-time models
  200+ features, 50M users, 10M products
  Reuse: data scientists can share and discover features

Architecture (Feast framework):

Feature Definition (shared registry):
  user_features.py:
    user_profile = Feature(name="days_since_last_purchase", dtype=Int32)
    user_activity = Feature(name="purchase_count_30d", dtype=Int32)
    
  Entity: user_id (primary key for all user features)

Offline Store (for training, point-in-time):
  Source: Delta Lake (user events, purchases, sessions)
  Feast offline: reads feature values AS OF a specific timestamp
    training_df = fs.get_historical_features(
        entity_df=labels_df,  # contains user_id + event_timestamp
        features=["user_features:days_since_last_purchase",
                  "user_features:purchase_count_30d"]
    ).to_df()
  Point-in-time correct: only returns features available BEFORE event_timestamp
  No future data leakage → valid training data

Online Store (for inference, Redis):
  Feature materialization job (Spark, runs every 1 hour):
    Compute feature values for all 50M users
    Write to Redis (user:{user_id} → {feature_json})
  Feast online:
    feature_vector = fs.get_online_features(
        features=["user_features:days_since_last_purchase"],
        entity_rows=[{"user_id": "12345"}]
    )
    Latency: < 5ms (Redis in-memory lookup) ✓

Feature Catalog:
  Feast UI: browse all registered features, see descriptions, owners, freshness
  Data scientists: search "purchase" → find existing purchase_count_30d feature
  Prevents: 5 different teams computing the same feature independently

Monitoring:
  Feature drift: compare feature distribution in training vs inference
    Alert: if mean of purchase_count_30d shifts >20% → model may degrade
  Freshness: materialization job must complete within 2 hours
    Alert if Redis features are > 2 hours stale
  Coverage: % of inference requests where feature is non-null
    Alert if null rate > 5% for critical features
```
