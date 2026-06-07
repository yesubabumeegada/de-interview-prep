---
title: "Schema Design Patterns - Fundamentals"
topic: data-modeling
subtopic: schema-design-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [schema-design, medallion, data-lake, data-warehouse, patterns, architecture]
---

# Schema Design Patterns — Fundamentals

## What Are Schema Design Patterns?

Schema design patterns are **proven architectural approaches** for organizing data in warehouses and lakes. They define how tables relate to each other, how data flows between layers, and how to balance performance with flexibility.

```mermaid
graph TD
    subgraph "Common Patterns"
        P1[Star Schema<br>Fact + flat dimensions]
        P2[Snowflake Schema<br>Normalized dimensions]
        P3[Medallion Architecture<br>Bronze → Silver → Gold]
        P4[Data Vault<br>Hubs + Links + Satellites]
        P5[One Big Table<br>Wide denormalized table]
        P6[Activity Schema<br>Events + entities]
    end
    
    style P1 fill:#bbdefb
    style P2 fill:#c8e6c9
    style P3 fill:#fff9c4
    style P4 fill:#e1bee7
    style P5 fill:#ffcdd2
    style P6 fill:#e1f5fe
```

## The Medallion Architecture (Most Popular Today)

```mermaid
graph LR
    SRC[Source Systems] --> BRZ[Bronze<br>Raw, as-is<br>Append-only]
    BRZ --> SLV[Silver<br>Cleaned, typed<br>Deduplicated]
    SLV --> GLD[Gold<br>Business-ready<br>Aggregated/modeled]
    GLD --> CON[Consumers<br>Dashboards, ML, APIs]
    
    style BRZ fill:#efebe9
    style SLV fill:#e0e0e0
    style GLD fill:#fff9c4
```

| Layer | Purpose | Schema Style | Example |
|-------|---------|-------------|---------|
| **Bronze** | Raw ingestion | Schema-on-read, flat | raw_orders (JSON/Parquet as-is) |
| **Silver** | Cleaned & conformed | 3NF or semi-normalized | silver.orders, silver.customers |
| **Gold** | Business-ready | Star schema / wide tables | gold.fact_sales, gold.dim_customer |

```sql
-- Bronze: Raw data, no transformation
CREATE TABLE bronze.raw_orders (
    _raw_data       VARIANT,         -- Full JSON payload
    _source_file    VARCHAR(500),    -- Where it came from
    _ingested_at    TIMESTAMP        -- When we received it
);

-- Silver: Cleaned, typed, deduplicated  
CREATE TABLE silver.orders (
    order_id        VARCHAR(20) PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    order_date      TIMESTAMP NOT NULL,
    total_amount    DECIMAL(12,2),
    status          VARCHAR(20),
    _loaded_at      TIMESTAMP
);

-- Gold: Business-ready star schema
CREATE TABLE gold.fact_sales (
    sale_key        BIGINT PRIMARY KEY,
    date_key        INT,
    customer_key    INT,
    product_key     INT,
    revenue         DECIMAL(12,2),
    quantity        INT
);
```

## Star Schema Pattern

The most common pattern for analytical data warehouses.

```mermaid
graph TD
    F[FACT TABLE<br>Measurements<br>Foreign Keys]
    D1[Dimension 1<br>Who?]
    D2[Dimension 2<br>What?]
    D3[Dimension 3<br>When?]
    D4[Dimension 4<br>Where?]
    
    D1 --> F
    D2 --> F
    D3 --> F
    D4 --> F
    
    style F fill:#ffcdd2
    style D1 fill:#bbdefb
    style D2 fill:#bbdefb
    style D3 fill:#bbdefb
    style D4 fill:#bbdefb
```

**When to use:** Reporting, BI dashboards, ad-hoc analytics.

## One Big Table (OBT) Pattern

A single wide denormalized table containing everything.

```sql
-- One Big Table: all facts and dimension attributes pre-joined
CREATE TABLE analytics.orders_obt (
    -- Order facts:
    order_id, order_date, quantity, revenue, discount,
    -- Customer attributes (denormalized):
    customer_name, customer_email, customer_city, customer_segment,
    -- Product attributes (denormalized):
    product_name, product_category, product_brand,
    -- Store attributes (denormalized):
    store_name, store_region
);
-- ONE table, ZERO joins needed for queries!
```

| Pros | Cons |
|------|------|
| Simplest queries (no JOINs) | Massive redundancy |
| Fast for specific use cases | Hard to maintain (updates affect many rows) |
| Good for ML feature stores | Schema changes are painful |
| Works for small/medium data | Doesn't scale well for many dimensions |

**When to use:** ML feature engineering, specific pre-computed reporting, small datasets.

## Activity Schema Pattern

Modern event-driven approach for product analytics.

```sql
-- Entity table (slowly changing):
CREATE TABLE entities.customers (
    customer_id     VARCHAR(20) PRIMARY KEY,
    attributes      VARIANT,          -- JSON: {name, email, plan, ...}
    updated_at      TIMESTAMP
);

-- Activity stream (append-only events):
CREATE TABLE activities.customer_events (
    event_id        VARCHAR(50) PRIMARY KEY,
    customer_id     VARCHAR(20),
    activity_type   VARCHAR(50),      -- 'purchase', 'login', 'page_view'
    event_timestamp TIMESTAMP,
    properties      VARIANT           -- JSON: event-specific data
);
-- Simple: just entities + their activities over time
```

**When to use:** Product analytics, customer 360, event-driven architectures.

## Choosing the Right Pattern

```mermaid
graph TD
    Q1{What's the primary use case?}
    Q1 -->|"BI & Reporting"| STAR[Star Schema<br>in Gold layer]
    Q1 -->|"ML & Data Science"| OBT[One Big Table<br>Feature store]
    Q1 -->|"Multiple sources<br>+ audit trail"| DV[Data Vault<br>in Silver layer]
    Q1 -->|"Product analytics<br>+ events"| ACT[Activity Schema]
    Q1 -->|"Everything"| MED[Medallion<br>Bronze→Silver→Gold<br>with Star in Gold]
    
    style STAR fill:#bbdefb
    style OBT fill:#ffcdd2
    style DV fill:#e1bee7
    style ACT fill:#e1f5fe
    style MED fill:#fff9c4
```

| Pattern | Best For | Complexity | Query Speed |
|---------|----------|-----------|-------------|
| Star Schema | BI / dashboards | Medium | Fast |
| Snowflake Schema | Deep hierarchies | High | Medium |
| Medallion | General-purpose data platform | Medium | Varies by layer |
| Data Vault | Enterprise DWH with audit needs | High | Slow (needs marts) |
| One Big Table | ML features, simple analytics | Low | Fastest (no joins) |
| Activity Schema | Product/event analytics | Low | Fast for event queries |

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Better Approach |
|-------------|---------|----------------|
| Everything in one schema | No separation of concerns | Medallion (bronze/silver/gold) |
| Circular dependencies | Tables reference each other | Unidirectional data flow |
| Mixing grain in one fact | Can't aggregate correctly | Separate facts per grain |
| Over-normalization in gold | Too many joins for analysts | Denormalize for consumption |
| No surrogate keys | Can't handle SCD, poor joins | Always add surrogate keys |

## Interview Tips

> **Tip 1:** "What schema design pattern would you use?" — Start with medallion architecture (bronze/silver/gold) as the overall framework. Within the gold layer, use star schema for BI/reporting. This is the most common production pattern today. Mention that Data Vault can replace silver for enterprise environments needing full audit trails.

> **Tip 2:** "Star schema vs. One Big Table?" — Star schema: better for general analytics (flexible, any dimension combination). OBT: better for specific use cases (ML feature store, one dashboard). In practice, star schema in gold layer + OBT views for specific consumers. OBT is derived FROM star, not a replacement.

> **Tip 3:** "What is the medallion architecture?" — Three-layer data organization: Bronze (raw, as-is ingestion), Silver (cleaned, typed, deduplicated), Gold (business-ready, modeled). Data flows one direction: source → bronze → silver → gold. Each layer adds value. Silver is source of truth; gold is optimized for consumption.
