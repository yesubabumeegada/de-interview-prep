---
title: "Star Schema - Senior Deep Dive"
topic: data-modeling
subtopic: star-schema
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [data-modeling, star-schema, performance, partitioning, materialized-views, scd, optimization]
---

# Star Schema — Senior-Level Deep Dive

## Physical Design Decisions

Logical star schema must be mapped to physical tables with performance optimizations for the target platform.

---

### Partitioning the Fact Table

Fact tables grow indefinitely. Partitioning is essential for query performance.

**Common partition strategies:**

| Strategy | Partition Key | When | Example |
|----------|-------------|------|---------|
| Date-based (most common) | `date_key` or `event_date` | Time-series data | Daily/monthly partitions |
| Hash-based | Business key | Uniform distribution needed | Hash on `customer_key` |
| List-based | Category | Known fixed values | Partition per region |

```sql
-- Snowflake: Clustering (Snowflake's equivalent of partitioning)
ALTER TABLE fact_sales CLUSTER BY (date_key, store_key);

-- Redshift: Sort key + Distribution key
CREATE TABLE fact_sales (
    sale_key BIGINT,
    date_key INT,
    product_key INT,
    store_key INT,
    amount DECIMAL(10,2)
)
DISTSTYLE KEY DISTKEY(store_key)    -- Collocate with dim_store for fast joins
SORTKEY(date_key);                  -- Range-filter on date first

-- BigQuery: Partition + Cluster
CREATE TABLE fact_sales
PARTITION BY DATE(sale_date)
CLUSTER BY store_key, product_key
AS SELECT ...;

-- Spark/Delta Lake: Partition on write
df.write.partitionBy("year", "month").format("delta").save(path)
```

> **Key principle:** Partition on the column most frequently filtered in WHERE clauses (almost always date). Cluster/sort on the columns most frequently used in JOINs or additional filters.

---

### Aggregate Tables (Pre-Computed Summaries)

For dashboards that hit the same aggregation patterns repeatedly, pre-compute and store summaries:

```sql
-- Daily aggregate table (much smaller than transaction-grain fact)
CREATE TABLE agg_daily_sales AS
SELECT 
    date_key,
    product_key,
    store_key,
    SUM(quantity) AS total_quantity,
    SUM(net_amount) AS total_revenue,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT customer_key) AS unique_customers
FROM fact_sales
GROUP BY date_key, product_key, store_key;

-- Monthly rollup (even smaller)
CREATE TABLE agg_monthly_sales AS
SELECT 
    d.year, d.month_name, 
    p.category,
    s.region,
    SUM(f.net_amount) AS revenue,
    SUM(f.quantity) AS units
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY d.year, d.month_name, p.category, s.region;
```

**Aggregate navigation (BI tool responsibility):**
- Query asks for "monthly revenue by category" → route to `agg_monthly_sales`
- Query asks for "individual transaction for customer X" → route to `fact_sales`
- The BI tool (or a query router) picks the smallest aggregate that satisfies the query

---

### Materialized Views vs Aggregate Tables

| Approach | Auto-Refresh | Flexibility | Platform |
|----------|-------------|-------------|----------|
| Aggregate table + ETL | Manual (schedule) | Full control | Any |
| Materialized view | Automatic (platform-managed) | Limited to SQL | Snowflake, Redshift, BigQuery |
| Delta Lake: liquid clustering | Automatic | Data layout only | Databricks |

```sql
-- Snowflake materialized view (auto-maintained)
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT date_key, store_key, SUM(net_amount) AS revenue
FROM fact_sales
GROUP BY date_key, store_key;

-- BigQuery materialized view
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT DATE(sale_date) AS sale_day, store_key, SUM(amount) AS revenue
FROM fact_sales
GROUP BY sale_day, store_key;
```

---

## Slowly Changing Dimensions in Star Schema

### SCD Type 1 — Overwrite (No History)

```sql
-- Customer changes address: overwrite the existing row
UPDATE dim_customer 
SET city = 'San Francisco', state = 'CA', updated_at = CURRENT_TIMESTAMP
WHERE customer_key = 2001;
```

**Effect:** All historical facts now show the new address. You lose history.

### SCD Type 2 — Add New Row (Full History)

```sql
-- Before change: close the current record
UPDATE dim_customer 
SET effective_to = CURRENT_DATE - 1, is_current = FALSE
WHERE customer_key = 2001 AND is_current = TRUE;

-- After change: insert new version
INSERT INTO dim_customer (customer_key, customer_id, city, state, effective_from, effective_to, is_current)
VALUES (2001_v2, 'CUST-2001', 'San Francisco', 'CA', CURRENT_DATE, '9999-12-31', TRUE);
```

**Effect:** Historical facts still point to the old dimension row. New facts point to the new row. Full audit trail preserved.

**The SCD Type 2 dimension structure:**

| customer_key | customer_id | city | effective_from | effective_to | is_current |
|-------------|------------|------|---------------|-------------|-----------|
| 2001 | CUST-2001 | New York | 2020-01-01 | 2024-01-14 | FALSE |
| 2002 | CUST-2001 | San Francisco | 2024-01-15 | 9999-12-31 | TRUE |

> **Note:** `customer_key` is the surrogate key (unique per version). `customer_id` is the natural key (same across versions). New surrogate key generated for each version.

### SCD Type 3 — Add Column (Limited History)

```sql
-- Store previous value in a separate column
ALTER TABLE dim_customer ADD COLUMN previous_city VARCHAR(50);

UPDATE dim_customer 
SET previous_city = city, city = 'San Francisco'
WHERE customer_key = 2001;
```

**Effect:** Keeps one level of history. Simple but limited — only tracks the most recent change.

---

## Handling Late-Arriving Dimensions

**Problem:** A fact arrives before its dimension record exists (e.g., order placed by a new customer not yet in dim_customer).

**Solution: Inferred member technique**

```sql
-- Step 1: When fact arrives with unknown customer, insert a placeholder
INSERT INTO dim_customer (customer_key, customer_id, first_name, is_inferred)
VALUES (next_key(), 'CUST-NEW-123', 'Unknown', TRUE);

-- Step 2: Fact table uses the placeholder key
INSERT INTO fact_sales (customer_key, ...) VALUES (placeholder_key, ...);

-- Step 3: When real customer data arrives, update the inferred record
UPDATE dim_customer 
SET first_name = 'John', last_name = 'Smith', segment = 'Gold', is_inferred = FALSE
WHERE customer_id = 'CUST-NEW-123' AND is_inferred = TRUE;
```

> **No fact table update needed** — the fact already points to the correct surrogate key. Only the dimension gets enriched.

---

## Query Performance Patterns

### Bitmap Indexes on Dimensions (Oracle, PostgreSQL)

Low-cardinality columns in dimensions benefit from bitmap indexes:

```sql
-- Dimension columns with few distinct values
CREATE BITMAP INDEX idx_product_category ON dim_product(category);
CREATE BITMAP INDEX idx_customer_segment ON dim_customer(segment);

-- Bitmap indexes excel at: WHERE category = 'Electronics' AND segment = 'Gold'
-- The database performs bitmap AND operations (very fast for multidimensional filtering)
```

### Columnar Storage Optimization

In columnar engines (Snowflake, Redshift, Parquet), star schema queries are already optimized because:
- Only the columns referenced in the query are read (column pruning)
- Compression works better on homogeneous data (same column = same type)
- Predicate pushdown filters at the storage layer before data reaches compute

```sql
-- This query only reads 3 columns from a 20-column fact table
SELECT date_key, store_key, SUM(amount)
FROM fact_sales
WHERE date_key BETWEEN 20240101 AND 20240131
GROUP BY date_key, store_key;
-- Columnar storage: reads ~15% of the data volume vs row-store reading 100%
```

---

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Fix |
|-------------|---------|-----|
| Fact table with NULLable FKs | Orphaned rows, broken joins | Use "Unknown" dimension row (key = -1) |
| Mixing grains in one fact | Ambiguous aggregations | Separate fact tables per grain |
| Putting descriptive text in fact | Wide fact table, slow scans | Move to dimension |
| No date dimension (just date column) | Repetitive date logic in every query | Create dim_date with pre-computed attributes |
| Natural keys as fact FKs | Breaks on source key changes | Use integer surrogate keys |
| Over-normalizing dimensions | Too many joins, slow queries | Denormalize into flat dimensions |

---

## Interview Tips

> **Tip 1:** "How do you handle a dimension that changes?" — Describe SCD types: "Type 1 overwrites (simple, loses history). Type 2 adds a new row with effective dates (preserves history, more complex ETL). Type 3 adds a column for the previous value (limited history). I choose based on whether the business needs historical reporting accuracy."

> **Tip 2:** "How do you optimize a slow star schema query?" — "First, check if the fact table is partitioned on the filtered date column. Second, check if small dimensions should be broadcast in distributed SQL. Third, consider pre-computed aggregates for repetitive dashboard queries. Fourth, verify column pruning is working (only needed columns being read)."

> **Tip 3:** "When would you NOT use a star schema?" — "When the use case is ad-hoc exploration with undefined query patterns (Data Vault is better), or when you need real-time updates to dimension attributes (operational/OLTP system is more appropriate). Star schema excels at known, repeated analytical queries."

## ⚡ Cheat Sheet

**Dimensional modeling building blocks**
```
Fact table:       measures/metrics (order_amount, quantity, duration)
Dimension table:  descriptive attributes (customer, product, date, geography)
Grain:            one row = one business event at lowest detail level
Surrogate key:    system-generated integer PK (never use natural keys in dim)
Natural key:      source system business key (stored alongside surrogate key)
```

**Star schema vs Snowflake schema**
```
Star:       fact → dimension (denormalized, faster queries, more storage)
Snowflake:  fact → dimension → sub-dimension (normalized, saves storage, more joins)
Rule:       prefer star for BI; snowflake only when storage cost is critical
```

**SCD (Slowly Changing Dimensions)**
| Type | Strategy | When |
|---|---|---|
| SCD1 | Overwrite old value | History irrelevant |
| SCD2 | New row (add effective_from, effective_to, is_current) | Need full history |
| SCD3 | Add prev_value column | Only need one prior value |
| SCD4 | Separate history table | Large dimension, rare changes |
| SCD6 | SCD1 + SCD2 + SCD3 hybrid | Best of all worlds |

**SCD2 implementation**
```sql
-- Insert new version, expire old
UPDATE dim_customer SET effective_to = CURRENT_DATE - 1, is_current = FALSE
WHERE customer_id = 123 AND is_current = TRUE;

INSERT INTO dim_customer (customer_id, name, city, effective_from, effective_to, is_current)
VALUES (123, 'Jane Doe', 'Chicago', CURRENT_DATE, '9999-12-31', TRUE);
```

**Data Vault pattern**
```
Hub:   business keys (stable identifiers — customer_id, order_id)
Link:  relationships between hubs (many-to-many)
Sat:   descriptive attributes + context (with load timestamp — full history)
```

**Fact table types**
```
Transaction:    one row per event (orders, clicks, payments)
Snapshot:       one row per period per entity (daily account balance)
Accumulating:   one row per lifecycle, updated as process stages complete
```

**Key interview points**
- Grain: define before designing any fact table — drives every design decision
- Degenerate dimensions: order number on fact table with no corresponding dimension
- Factless facts: events with no measures (student enrolled in course — just the relationship)
- Role-playing dimensions: same dimension used multiple times (order_date, ship_date, return_date)
- Conformed dimensions: shared across multiple fact tables (same dim_date in sales and returns facts)
