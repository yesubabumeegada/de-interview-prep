---
title: "Data Warehouse Design — Senior Deep Dive"
topic: system-design
subtopic: data-warehouse-design
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, data-warehouse, one-big-table, activity-schema, wide-table, semantic-layer]
---

# Data Warehouse Design — Senior Deep Dive

## One Big Table (OBT) vs Star Schema

### When Each Makes Sense

```sql
-- Star Schema (traditional Kimball):
-- Best when: many analysts, complex ad-hoc queries, BI tools with auto-join
-- Cons: requires knowledge of schema, multiple joins for every query

-- One Big Table (OBT) / Wide Table:
-- Pre-join all dimensions into the fact table at load time
-- Best when: self-service analytics, simple queries, Spark/ClickHouse columnar scan

-- OBT example: daily_sales wide table (pre-joined)
CREATE TABLE daily_sales_obt AS
SELECT
  f.sale_id, f.sale_date, f.total_amount, f.quantity,
  -- Date dimension (inline)
  d.year, d.quarter, d.month_name, d.week_number, d.is_holiday,
  -- Product dimension (inline)
  p.product_name, p.category, p.subcategory, p.brand,
  -- Customer dimension (inline)
  c.customer_name, c.region, c.country, c.segment,
  -- Store dimension (inline)
  s.store_name, s.city, s.state
FROM fact_sales f
JOIN dim_date d     ON f.date_key     = d.date_key
JOIN dim_product p  ON f.product_key  = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_store s    ON f.store_key    = s.store_key;

-- OBT tradeoffs:
-- Pro: any analyst can query without knowing the schema; fast columnar scan
-- Con: data duplication (product_name stored N times per product)
--      SCD type 2 harder to implement (need to denormalize historical versions)
--      Wide tables have high column count → wider schema drift risk
```

---

## Activity Schema (Modern Alternative to Star Schema)

```sql
-- Activity schema: all events in one table, typed by activity_type
-- Built for user-centric analysis (customer journey, funnel analysis)

CREATE TABLE user_activity (
  activity_id    BIGINT,
  user_id        VARCHAR(50),
  activity_type  VARCHAR(50),   -- page_view, purchase, support_ticket, login
  ts             TIMESTAMP,
  feature_1      VARCHAR(500),  -- activity-specific payload (JSON or typed columns)
  feature_2      VARCHAR(500),
  feature_3      DECIMAL(18,2),
  revenue_impact DECIMAL(10,2)
);

-- Funnel query: how many users who viewed product also purchased?
WITH viewers AS (
  SELECT DISTINCT user_id FROM user_activity
  WHERE activity_type = 'product_view' AND DATE(ts) = '2024-01-15'
),
purchasers AS (
  SELECT DISTINCT user_id FROM user_activity
  WHERE activity_type = 'purchase'
    AND DATE(ts) BETWEEN '2024-01-15' AND '2024-01-22'
)
SELECT
  COUNT(DISTINCT v.user_id) AS viewers,
  COUNT(DISTINCT p.user_id) AS purchasers,
  ROUND(100.0 * COUNT(DISTINCT p.user_id) / COUNT(DISTINCT v.user_id), 2) AS conversion_rate
FROM viewers v LEFT JOIN purchasers p ON v.user_id = p.user_id;

-- Activity schema tools: Narrator, Metrics Layer on top of activity schema
```

---

## Semantic Layer Design

```
Problem: 50 analysts define "revenue" differently
  Team A: revenue = SUM(gross_amount)
  Team B: revenue = SUM(gross_amount) - SUM(returns)
  Team C: revenue = SUM(gross_amount) WHERE status != 'cancelled'
  Result: three different numbers in three dashboards → trust collapse

Semantic layer: single place where metrics are defined once
  Tools: dbt Metrics, LookML (Looker), Cube.js, AtScale, Metriql

dbt Metrics example:
  # models/metrics/revenue.yml
  metrics:
    - name: gross_revenue
      label: Gross Revenue
      model: ref('fact_sales')
      description: "Total revenue before returns and cancellations"
      type: sum
      sql: total_amount
      timestamp: sale_date
      time_grains: [day, week, month, quarter, year]
      dimensions: [region, category, brand, segment]
      filters:
        - field: status
          operator: "!="
          value: "'void'"
    
    - name: net_revenue
      label: Net Revenue
      model: ref('fact_sales')
      type: expression
      sql: "{{metric('gross_revenue')}} - {{metric('returns_amount')}}"

-- Every BI tool and analyst uses the same metric definition
-- Change once → propagates everywhere
```

---

## Multi-Hop Architecture (Medallion at Scale)

```
Refined medallion for enterprise scale:

Raw Zone (Bronze):
  - Append-only, exact copy of source
  - No transforms, no joins
  - Retention: 2 years (recovery + compliance)
  - Format: compressed JSON or Parquet

Cleansed Zone (Silver):
  - Schema-enforced, type-cast, null-handled
  - Deduplication via streaming MERGE
  - PII masked (tokenized/hashed)
  - Retention: 1 year

Integrated Zone (Silver+):
  - Cross-source joins (orders + customers + products joined)
  - Conformed dimensions applied
  - SCD Type 2 applied
  - De-normalized for subject area use

Presentation Zone (Gold):
  - Business-specific aggregates
  - One model per BI use case
  - Optimized for specific query patterns

Feature Store (ML Gold):
  - Labeled training data
  - Feature vectors by entity (user, product)
  - Point-in-time correct (no future leakage)
  - Shared between training and inference

Governance:
  - All zones tagged with sensitivity (PII, financial, public)
  - Column-level access control on Silver+ zones
  - Data lineage tracked from raw → presentation
```

---

## Interview Tips

> **Tip 1:** "When would you recommend a wide/OBT table instead of a star schema?" — For self-service analytics platforms where analysts don't know SQL joins well, OBTs reduce the barrier to insight. ClickHouse and BigQuery perform well on wide tables due to columnar storage (unused columns are not read). Also useful in ML feature engineering where wide tables make feature selection easy. Downside: storage duplication and SCD complexity. For enterprise DWs with many analysts and complex hierarchies, star schema remains better.

> **Tip 2:** "What is a semantic layer and why does it matter?" — A semantic layer centralizes metric definitions (revenue, MAU, conversion rate) so they're computed consistently across all BI tools and teams. Without it: every dashboard defines metrics differently → trust collapse ("your revenue number doesn't match mine"). With it: one definition → consistent numbers everywhere. Modern tools: dbt Metrics, LookML, Cube.js. Senior DEs own the semantic layer as part of the data contract with business.

> **Tip 3:** "How would you design a DW for a company that acquired 3 other companies, each with different customer ID systems?" — This is an entity resolution / golden record problem. Build a customer identity graph: map all source customer IDs to a single enterprise_customer_id using deterministic (email, phone match) and probabilistic (name + zip match) rules. Store the mapping in an entity_resolution table. Use the enterprise_customer_id as the DW surrogate key. All 3 source customer IDs remain as attributes in dim_customer for reference. This enables cross-acquisition analysis ("revenue from acquired company X's customers post-acquisition").
