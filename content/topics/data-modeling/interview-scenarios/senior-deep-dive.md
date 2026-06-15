---
title: "Data Modeling Interview — Senior Deep Dive"
description: "Senior-level data modeling interview topics: social media analytics schema, activity schema, OBT, Data Vault, temporal modeling, and schema evolution"
content_type: study_material
topic: data-modeling
subtopic: interview-scenarios
layer: senior-deep-dive
difficulty_level: senior
tags: [data-modeling, activity-schema, one-big-table, data-vault, kimball, inmon, temporal-modeling, schema-evolution, partitioning, clustering, interview]
---

# Data Modeling Interview — Senior Deep Dive

At the senior level, interviewers expect architectural judgment, not just pattern knowledge. You should be able to compare paradigms (Kimball vs Inmon vs Data Vault vs activity schema), design for scale and performance, handle temporal complexity, and explain how you manage schema change without breaking downstream consumers.

---

## 1. Full Schema Design Walkthrough: Social Media Analytics Platform

**Interview prompt:** "Design a data model for a social media analytics platform. Users create posts, interact with each other's content via likes and comments, and follow other users. Advertisers run campaigns targeting users."

### Clarify Requirements

- "What are the key analytical questions? Engagement metrics? Ad performance? User growth?"
- "What is the scale — millions of users or billions of events per day?"
- "Is this a read-heavy OLAP system or does it need to support ML feature computation as well?"
- "Do advertisers need to see reach and frequency, or just clicks and spend?"

### Business Processes Identified

1. **Content engagement** — likes, comments, shares, views per post
2. **Social graph activity** — follow/unfollow events
3. **Ad impressions and clicks** — campaign performance
4. **User growth** — account creation, activation, churn

### Core Schema

**Grain of fact_engagement:** One row per user-content interaction event.

```sql
CREATE TABLE fact_engagement (
    engagement_key      BIGINT,
    event_date_key      INT REFERENCES dim_date(date_key),
    user_key            INT REFERENCES dim_user(user_key),
    content_key         INT REFERENCES dim_content(content_key),
    author_key          INT REFERENCES dim_user(user_key),         -- role-playing
    engagement_type_key INT REFERENCES dim_engagement_type(engagement_type_key),
    platform_key        INT REFERENCES dim_platform(platform_key),
    content_age_hours   INT,
    session_duration_sec INT,
    is_organic          BOOLEAN
) PARTITION BY RANGE (event_date_key);  -- partition by date for query efficiency
```

```sql
CREATE TABLE dim_content (
    content_key         INT PRIMARY KEY,
    post_id             VARCHAR(50),
    content_type        VARCHAR(50),   -- 'photo', 'video', 'reel', 'story'
    topic_category      VARCHAR(100),
    character_count     INT,
    media_count         INT,
    publish_date_key    INT,
    is_sponsored        BOOLEAN,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
);

CREATE TABLE dim_engagement_type (
    engagement_type_key INT PRIMARY KEY,
    engagement_type     VARCHAR(50),   -- 'like', 'comment', 'share', 'view', 'save'
    engagement_weight   DECIMAL(4,2)   -- weighted score for engagement rate calculation
);
```

**Grain of fact_ad_performance:** One row per ad impression or click event.

```sql
CREATE TABLE fact_ad_impression (
    impression_key      BIGINT,
    event_date_key      INT REFERENCES dim_date(date_key),
    user_key            INT REFERENCES dim_user(user_key),
    ad_key              INT REFERENCES dim_ad(ad_key),
    campaign_key        INT REFERENCES dim_campaign(campaign_key),
    placement_key       INT REFERENCES dim_placement(placement_key),
    impression_cost_usd DECIMAL(10,6),
    is_click            BOOLEAN,
    is_conversion       BOOLEAN,
    conversion_value_usd DECIMAL(10,2)
);
```

---

## 2. Activity Schema / Wide Event Tables vs Traditional Dimensional Modeling

### What Is the Activity Schema?

The activity schema (popularized by tools like Narrator.ai) models all user behavior as a single wide table of activities:

```sql
CREATE TABLE activity_stream (
    activity_id         BIGINT,
    ts                  TIMESTAMP,
    activity_type       VARCHAR(100),   -- 'page_view', 'add_to_cart', 'purchase', 'login'
    anonymous_id        VARCHAR(100),
    customer_id         VARCHAR(100),
    feature_json        JSONB,           -- all event-specific attributes in JSON
    -- plus some pre-materialized columns for common fields
    revenue_usd         DECIMAL(10,2),
    product_id          VARCHAR(50),
    page_url            VARCHAR(500)
);
```

### Trade-offs vs Dimensional Modeling

| Aspect | Activity Schema / Wide Event Table | Star Schema (Kimball) |
|---|---|---|
| Schema changes | Easy — add JSON keys, no migrations | Harder — ALTER TABLE or new dimension required |
| Query complexity | Higher — lots of JSON parsing, self-joins for funnels | Lower — analysts can join dim tables naturally |
| Performance | Can be slow for aggregates without pre-aggregation | Excellent with columnar storage and proper indexes |
| Flexibility | Maximum — any new event type immediately available | Limited by fact table grain and schema |
| ML feature serving | Good — event history per user is one table | Harder — must join many fact tables |
| Learning curve | Low for developers, high for SQL analysts | Low for SQL analysts with Kimball background |

### One Big Table (OBT)

The extreme version: one massive denormalized table with all dimension attributes pre-joined into the fact table.

```sql
-- OBT example for a retail scenario
CREATE TABLE obt_sales AS
SELECT
    f.*,
    d.day_of_week, d.month_name, d.fiscal_quarter,
    c.customer_name, c.customer_segment, c.city, c.country,
    p.product_name, p.category, p.brand,
    s.store_name, s.region
FROM fact_sales f
JOIN dim_date     d ON f.date_key     = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product  p ON f.product_key  = p.product_key
JOIN dim_store    s ON f.store_key    = s.store_key;
```

**When OBT is valid:**
- Single analytical domain with no cross-domain joins needed
- BI tool does not support SQL joins well (e.g., embedded analytics with limited SQL)
- Very simple query patterns that don't change
- Data volume is manageable (< a few hundred GB materialized)

**Trade-offs vs normalized:**
- Query performance: excellent (no joins at query time)
- Storage: large (full attribute duplication for every row)
- Maintenance: high cost — updating a dimension means regenerating the OBT
- Flexibility: low — adding a new dimension attribute requires rebuilding

**Interview position:**
> "I treat OBT as a materialization layer on top of a properly modeled warehouse, not a replacement for it. The normalized star schema is the source of truth; OBTs are derived for specific high-traffic query patterns where join cost is prohibitive."

---

## 3. Data Vault vs Kimball vs Inmon — When Each Applies

### Inmon (Corporate Information Factory)

Bill Inmon's approach: build a highly normalized enterprise data warehouse (3NF) at the center, then build data marts on top for specific departments.

```
Source Systems → Enterprise DW (3NF) → Departmental Data Marts (star schemas)
```

**Strengths:**
- Single source of truth — very clean, no redundancy
- Easy to add new subject areas without restructuring

**Weaknesses:**
- Very complex queries on the normalized EDW
- Slower to build and query than dimensional models
- Requires significant upfront modeling effort

**Use when:** Large enterprise with many source systems and a need for an authoritative integration layer. Less common in modern cloud data stacks.

### Kimball (Dimensional Modeling)

Ralph Kimball's approach: build dimension and fact tables directly in the data warehouse, organized around business processes.

**Strengths:**
- Highly optimized for analytical queries
- Intuitive for business users and BI tools
- Conformed dimensions allow cross-process analysis

**Weaknesses:**
- Schema changes (new dimensions, changed grain) are costly
- Hard to handle highly variable event schemas
- Less suitable for raw data vault / operational reporting

**Use when:** Analytics-first organizations, BI-heavy environments, relatively stable business processes. The default choice for most modern data warehouses.

### Data Vault 2.0

Dan Linstedt's approach: model everything as Hubs (business keys), Links (relationships), and Satellites (attributes and context).

```sql
-- Hub: just the business key and metadata
CREATE TABLE hub_customer (
    customer_hash_key   CHAR(32) PRIMARY KEY,   -- MD5/SHA1 of business key
    customer_id         VARCHAR(50),
    load_date           TIMESTAMP,
    record_source       VARCHAR(100)
);

-- Satellite: attributes with full history
CREATE TABLE sat_customer_details (
    customer_hash_key   CHAR(32),
    load_date           TIMESTAMP,
    customer_name       VARCHAR(100),
    email               VARCHAR(255),
    city                VARCHAR(100),
    hash_diff           CHAR(32),
    record_source       VARCHAR(100),
    PRIMARY KEY (customer_hash_key, load_date)
);

-- Link: relationship between two or more hubs
CREATE TABLE link_order_customer (
    order_customer_hash_key CHAR(32) PRIMARY KEY,
    order_hash_key          CHAR(32),
    customer_hash_key       CHAR(32),
    load_date               TIMESTAMP,
    record_source           VARCHAR(100)
);
```

**Strengths:**
- Extremely auditable — full lineage from source to hub/satellite
- Handles schema changes and new sources elegantly — just add satellites
- Parallel load-friendly — hubs, links, and satellites load independently
- Excellent for regulatory environments (GDPR, HIPAA) requiring full audit trail

**Weaknesses:**
- Requires transformation to a presentation layer (star schema) for analytics — query directly is complex
- Higher storage due to metadata columns on every table
- Learning curve is steep

**Use when:** Heavily regulated industries (finance, healthcare), enterprise with many changing source systems, environments where auditability is critical.

### Summary Decision Matrix

| Scenario | Recommended Approach |
|---|---|
| Fast BI analytics on stable processes | Kimball star schema |
| Enterprise with many source systems, complex integration | Inmon or Data Vault |
| Regulatory audit requirements | Data Vault 2.0 |
| Rapidly changing event schemas (SaaS product analytics) | Activity schema or ELT with JSON |
| All of the above in one org | Data Vault raw layer + Kimball presentation layer |

---

## 4. Designing for Query Performance: Partitioning and Clustering

Modern cloud warehouses (BigQuery, Snowflake, Redshift) offer partitioning and clustering. Understanding how to design fact tables around these is a senior-level expectation.

### Partitioning

Partitioning physically divides a table into segments based on a column value. Only the relevant partitions are scanned for a query.

**BigQuery example:**
```sql
CREATE TABLE fact_events
PARTITION BY DATE(event_timestamp)
AS SELECT ...;
```

**Rule of thumb:** Partition by the column most frequently used in `WHERE` filters. For time-series event data, this is almost always the event date.

**Partition pruning:** A query with `WHERE event_date = '2024-01-15'` scans only one day's partition instead of the full table. On a petabyte-scale table, this is the difference between a $50 query and a $0.05 query.

### Clustering

Clustering sorts data within each partition by the specified columns. Queries that filter on clustering columns skip irrelevant micro-partitions.

**BigQuery example:**
```sql
CREATE TABLE fact_engagement
PARTITION BY event_date
CLUSTER BY user_id, content_type;
```

**Snowflake equivalent:**
```sql
CREATE TABLE fact_engagement
CLUSTER BY (event_date, user_id);
```

**When clustering helps:** When queries frequently filter by columns with high cardinality (user_id, product_id, campaign_id). Not helpful for low-cardinality columns (boolean flags) that are better handled by junk dimensions or row-level filtering.

### Design Principle

> Partition by time (almost always). Cluster by the most selective filter column used in queries — often the entity being analyzed (user, product, campaign).

---

## 5. Temporal Modeling — Bi-Temporal Tables

Standard SCD2 tracks **valid time** — when an attribute was true in the real world. But what about when the data was loaded into the warehouse? These are two different axes.

### Uni-temporal (SCD2)

Tracks: "when was this fact true in the real world?"

```sql
| customer_key | city         | valid_from  | valid_to    |
| 1001         | New York     | 2020-01-01  | 2024-06-14  |
| 1002         | Los Angeles  | 2024-06-15  | 9999-12-31  |
```

### Bi-temporal Table

Tracks two time axes independently:

1. **Valid time (VT):** When was this true in the real world?
2. **Transaction time (TT):** When did we know this in our system?

```sql
CREATE TABLE dim_customer_bitemporal (
    customer_key        INT,
    customer_id         VARCHAR(50),
    city                VARCHAR(100),
    valid_from          DATE,          -- real-world effective date
    valid_to            DATE,          -- real-world expiry date
    system_from         TIMESTAMP,     -- when loaded into DW
    system_to           TIMESTAMP,     -- when superseded in DW
    is_current_record   BOOLEAN        -- current in both dimensions
);
```

**Use case:** A regulatory audit requires you to prove what data you had in your system on a specific date, even if that data was later corrected. With bi-temporal modeling, you can query "as of 2024-03-01, what did we believe was true about this customer?"

```sql
-- "What was our view of customer 12345 as of March 1, 2024?"
SELECT *
FROM dim_customer_bitemporal
WHERE customer_id = '12345'
  AND valid_from  <= '2024-03-01'
  AND valid_to    >  '2024-03-01'
  AND system_from <= '2024-03-01'
  AND system_to   >  '2024-03-01';
```

**When to use:** Financial services (must reproduce historical calculations), healthcare (audit requirements), any domain with retroactive corrections to historical data.

---

## 6. Handling Schema Evolution Without Breaking Downstream

One of the most practical senior-level questions: "How do you change your data model without breaking pipelines and dashboards?"

### Strategy 1: Additive-Only Changes (Safest)

Add columns, never remove or rename. New columns have defaults or NULLs for historical data.

```sql
ALTER TABLE dim_product ADD COLUMN sustainability_score INT DEFAULT NULL;
```

**Policy:** Agree with your team that columns are never deleted or renamed, only deprecated (documented and eventually stopped being populated).

### Strategy 2: Views as Abstraction Layer

Never expose raw tables to downstream consumers. Always serve data through views. When the underlying table changes, update the view to maintain the old schema.

```sql
-- Underlying table changes, but view maintains backward compatibility
CREATE OR REPLACE VIEW v_dim_customer AS
SELECT
    customer_key,
    customer_id,
    first_name,
    last_name,
    COALESCE(email, legacy_email) AS email,  -- accommodate renamed column
    city,
    state
FROM dim_customer_v2;
```

### Strategy 3: Schema Versioning

Maintain multiple versions of a schema during transition periods:
- `dim_product_v1` — old schema, deprecated
- `dim_product_v2` — new schema, production

Give consumers a migration window (e.g., 90 days) to update before deprecating v1.

### Strategy 4: Contract Testing

Use tools like dbt tests or Great Expectations to define and enforce schema contracts. Any breaking change fails the CI pipeline before reaching production.

```yaml
# dbt schema.yml
models:
  - name: dim_customer
    columns:
      - name: customer_key
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
```

### Strategy 5: Event Schema Registry (for streaming)

For Kafka-based pipelines, use a schema registry (Confluent Schema Registry, AWS Glue Schema Registry) that enforces compatibility rules:
- **Backward compatible:** New schema can read old data
- **Forward compatible:** Old schema can read new data
- **Full compatible:** Both directions

**Interview answer:**
> "I prevent breaking changes by publishing data through versioned views and enforcing additive-only migrations. For major redesigns, I run both old and new schemas in parallel with a migration window, and I use contract testing in dbt to catch schema drift before it reaches production."

---

## 7. Practice Questions

1. A stakeholder asks: "Should we use a Data Vault or Kimball approach for our new data platform?" What questions do you ask before answering?
2. You have a fact table with 5 trillion rows. Queries are slow even with columnar storage. What physical design changes do you consider first?
3. A business rule changes retroactively — the commission calculation for the prior fiscal year was wrong and needs to be corrected. How does bi-temporal modeling help?
4. How would you model a social graph (follower/following relationships) for analytics at scale?
5. A product manager asks why you don't just "put everything in one big JSON column." How do you respond?
6. Your `dim_customer` SCD2 table is growing by 50 million rows per day due to frequent attribute changes. What are your options?
7. Explain the difference between partitioning and clustering. When would you use both on the same table?
8. A downstream BI dashboard breaks every time the data engineering team changes the schema. What architectural change would you propose?
