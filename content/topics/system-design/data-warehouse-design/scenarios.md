---
title: "Data Warehouse Design — Scenarios"
topic: system-design
subtopic: data-warehouse-design
content_type: scenario_question
tags: [data-warehouse, design, snowflake, redshift, scenarios]
---

# Data Warehouse Design — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Designing a Star Schema for E-Commerce

**Scenario:** An e-commerce company wants to analyze sales performance. Their transactional database has: orders, order_items, customers, products, and stores tables. Design a star schema for a sales analytics data warehouse.

<details>
<summary>💡 Hint</summary>

Identify the business process (sales) → the grain (one row per order item) → dimensions (who, what, where, when) → facts (measures: revenue, quantity). The grain determines what each row in the fact table represents.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Define the Grain**

Grain: one row per order line item (most granular = most flexible for aggregation)

**Star Schema Design:**

```sql
-- Fact Table (measures)
CREATE TABLE fact_sales (
    sale_key        BIGINT IDENTITY PRIMARY KEY,
    order_date_key  INT NOT NULL REFERENCES dim_date(date_key),
    customer_key    INT NOT NULL REFERENCES dim_customer(customer_key),
    product_key     INT NOT NULL REFERENCES dim_product(product_key),
    store_key       INT NOT NULL REFERENCES dim_store(store_key),
    -- Degenerate dimensions (no separate table needed)
    order_id        VARCHAR(50) NOT NULL,
    order_item_id   VARCHAR(50) NOT NULL,
    -- Measures
    quantity        INT NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount      DECIMAL(10,2) DEFAULT 0,
    gross_revenue   DECIMAL(10,2) NOT NULL,  -- quantity × unit_price
    net_revenue     DECIMAL(10,2) NOT NULL   -- gross - discount
);

-- Dimension: Date (pre-populated for date arithmetic)
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,          -- 20240115 format
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(10),
    day_name        VARCHAR(10),
    month_num       INT,
    month_name      VARCHAR(10),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
);

-- Dimension: Customer (SCD Type 2 for history)
CREATE TABLE dim_customer (
    customer_key    INT IDENTITY PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL,     -- business key
    name            VARCHAR(200),
    email           VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50),
    country         VARCHAR(50),
    segment         VARCHAR(50),
    -- SCD Type 2 columns
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    is_current      BOOLEAN DEFAULT TRUE
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key     INT IDENTITY PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    cost_price      DECIMAL(10,2)
);

-- Dimension: Store
CREATE TABLE dim_store (
    store_key       INT IDENTITY PRIMARY KEY,
    store_id        VARCHAR(50) NOT NULL,
    store_name      VARCHAR(200),
    city            VARCHAR(100),
    region          VARCHAR(50),
    country         VARCHAR(50),
    store_type      VARCHAR(50)  -- online, retail, wholesale
);
```

**Sample Query — Sales by Category and Month:**

```sql
SELECT
    d.year,
    d.month_name,
    p.category,
    SUM(f.net_revenue) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS order_count
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE d.year = 2024 AND c.is_current = TRUE
GROUP BY 1, 2, 3
ORDER BY 1, d.month_num, 4 DESC;
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Slowly Changing Dimensions — Implementing SCD Type 2

**Scenario:** Your customer dimension stores customer segment (Bronze/Silver/Gold). Customers change segments when their lifetime value crosses thresholds. Your marketing team needs to analyze revenue by the customer's segment AT THE TIME OF PURCHASE, not their current segment. Implement SCD Type 2.

<details>
<summary>💡 Hint</summary>

SCD Type 2 adds a new row for each change, with effective/expiry dates and an is_current flag. The fact table joins to the dimension record that was active at the time of the transaction. The challenge is loading new changes incrementally.

</details>

<details>
<summary>✅ Solution</summary>

**SCD Type 2 Table Structure:**

```sql
CREATE TABLE dim_customer (
    customer_key    INT IDENTITY PRIMARY KEY,  -- surrogate key
    customer_id     VARCHAR(50) NOT NULL,       -- business key (durable)
    name            VARCHAR(200),
    email           VARCHAR(200),
    segment         VARCHAR(50),               -- changes over time
    lifetime_value  DECIMAL(18,2),
    effective_date  DATE NOT NULL,
    expiry_date     DATE,                      -- NULL = current record
    is_current      BOOLEAN DEFAULT TRUE,
    -- Audit
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**SCD Type 2 Load Logic (dbt):**

```sql
-- dbt: models/silver/dim_customer.sql
-- Using dbt's built-in snapshot feature

{% snapshot dim_customer_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['segment', 'lifetime_value', 'name', 'email']
    )
  }}

  SELECT
    customer_id,
    name,
    email,
    CASE
      WHEN lifetime_value >= 10000 THEN 'Gold'
      WHEN lifetime_value >= 1000 THEN 'Silver'
      ELSE 'Bronze'
    END AS segment,
    lifetime_value
  FROM {{ source('silver', 'customers') }}

{% endsnapshot %}
```

dbt automatically adds `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`.

**Custom SCD Type 2 MERGE (Snowflake):**

```sql
-- Step 1: Identify changed records
WITH source AS (
    SELECT
        customer_id,
        name,
        email,
        segment,
        lifetime_value,
        CURRENT_DATE AS effective_date
    FROM silver.customers
),
changes AS (
    SELECT s.*
    FROM source s
    LEFT JOIN dim_customer d
        ON s.customer_id = d.customer_id
        AND d.is_current = TRUE
    WHERE d.customer_key IS NULL  -- new customer
       OR s.segment != d.segment  -- segment changed
       OR s.name != d.name
)

-- Step 2: Expire old records
UPDATE dim_customer d
SET is_current = FALSE,
    expiry_date = CURRENT_DATE - 1
FROM changes c
WHERE d.customer_id = c.customer_id
  AND d.is_current = TRUE;

-- Step 3: Insert new records
INSERT INTO dim_customer (customer_id, name, email, segment, lifetime_value, effective_date, is_current)
SELECT customer_id, name, email, segment, lifetime_value, effective_date, TRUE
FROM changes;
```

**Querying Historical Segment at Transaction Time:**

```sql
-- Revenue by segment AT TIME OF PURCHASE (not current segment)
SELECT
    c.segment,
    SUM(f.net_revenue) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
-- fact_sales.customer_key always points to the SCD2 record
-- that was current at time of sale (set during ETL)
GROUP BY 1;
```

**Loading Facts with Correct SCD2 Key:**

```sql
-- During fact load: look up the customer record active at order time
INSERT INTO fact_sales (order_date_key, customer_key, ...)
SELECT
    d.date_key,
    c.customer_key,  -- SCD2 surrogate key (historical)
    ...
FROM source_orders o
JOIN dim_date d ON d.full_date = o.order_date::DATE
JOIN dim_customer c
    ON c.customer_id = o.customer_id
    AND o.order_date BETWEEN c.effective_date
                         AND COALESCE(c.expiry_date, '9999-12-31');
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Snowflake vs Redshift — Architecture Decision for a Financial Services Firm

**Scenario:** A financial services firm processes $100B in transactions annually. They need a cloud data warehouse for: regulatory reporting (SEC, FINRA), quant analyst ad-hoc queries (complex window functions on 10 years of tick data), and executive dashboards (sub-second response). Evaluate Snowflake vs Redshift and make a recommendation.

<details>
<summary>💡 Hint</summary>

Key differentiators: Snowflake's virtual warehouse model (instant scale, per-second billing) vs Redshift's cluster model (resizing takes minutes/hours). Consider: concurrency requirements (analysts + dashboards + regulatory jobs simultaneously), compliance (both are SOC2/HIPAA), and total cost at scale.

</details>

<details>
<summary>✅ Solution</summary>

**Requirements Matrix:**

| Requirement | Complexity |
|-------------|-----------|
| Regulatory reporting (FINRA) | Scheduled, predictable, long-running |
| Quant ad-hoc queries | Unpredictable, complex, concurrent |
| Executive dashboards | Sub-second, highly concurrent |
| 10 years of tick data | ~50TB historical, range scans |
| GDPR/SEC data retention | 7-year immutable history |

**Snowflake Architecture:**

```sql
-- Multiple virtual warehouses for workload isolation
-- Each can scale independently, suspend when idle

-- Regulatory reporting: large, runs nightly
CREATE WAREHOUSE regulatory_wh
    WAREHOUSE_SIZE = 'X-LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Quant analysts: burst capacity for complex queries
CREATE WAREHOUSE quant_analytics_wh
    WAREHOUSE_SIZE = 'LARGE'
    MAX_CLUSTER_COUNT = 5  -- multi-cluster for concurrency
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY'
    AUTO_SUSPEND = 120;

-- Executive dashboards: always-on small warehouse
CREATE WAREHOUSE dashboard_wh
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300;
```

```sql
-- Snowflake: 10-year tick data with clustering
CREATE TABLE finance.tick_data (
    symbol VARCHAR(20),
    tick_timestamp TIMESTAMP_NTZ,
    bid DECIMAL(18,8),
    ask DECIMAL(18,8),
    bid_size INT,
    ask_size INT
)
CLUSTER BY (symbol, DATE(tick_timestamp));
-- Automatic micro-partitioning + clustering → efficient range scans

-- Query: 10-year history for one symbol
SELECT * FROM finance.tick_data
WHERE symbol = 'AAPL'
  AND tick_timestamp BETWEEN '2014-01-01' AND '2024-01-01';
-- Scans only clustered micro-partitions for AAPL
```

**Redshift Architecture:**

```sql
-- Redshift: manual distribution key for joins
CREATE TABLE tick_data (
    symbol VARCHAR(20),
    tick_timestamp TIMESTAMP,
    bid DECIMAL(18,8),
    ask DECIMAL(18,8)
)
DISTKEY(symbol)
SORTKEY(symbol, tick_timestamp);
-- Good for single-symbol queries, but cluster resize is disruptive

-- RA3 nodes: separate storage (S3) from compute
-- Allows resize without data movement, but still takes 30+ minutes
```

**Detailed Comparison:**

| Factor | Snowflake | Redshift |
|--------|-----------|----------|
| Workload isolation | Virtual warehouses (instant) | WLM queues (shared compute) |
| Concurrency | Auto-scale, unlimited | Fixed concurrency scaling |
| Complex queries | Excellent | Excellent |
| Sub-second dashboards | With result cache | With materialized views |
| Resize time | Instant (new VW) | 30 min - hours (RA3) |
| Data sharing | Native (zero-copy) | Redshift Data Sharing (newer) |
| Cost model | Per-second compute | Per-node-hour |
| AWS integration | Good | Excellent (native) |
| Compliance | SOC2, HIPAA, PCI | SOC2, HIPAA, PCI |

**Cost Modeling at Scale:**

```python
# Snowflake cost estimate
snowflake_monthly = {
    'regulatory_wh': 8 * 16 * 30 * 3.0,   # XL = 16 credits/hr × 8hr/day × $3/credit
    'quant_wh': 8 * 8 * 30 * 3.0 * 2,     # L = 8 credits/hr, avg 2 clusters
    'dashboard_wh': 24 * 2 * 30 * 3.0,    # S = 2 credits/hr, always on
    'storage': 50000 * 0.023,              # 50TB at $23/TB
}
total_snowflake = sum(snowflake_monthly.values())  # ~$60K/month

# Redshift RA3 cost estimate
redshift_monthly = {
    'ra3_16xlarge': 8 * 6.976 * 24 * 30,  # 8 nodes × $6.976/hr × always-on
    'storage': 50000 * 0.024,              # S3 Managed Storage
}
total_redshift = sum(redshift_monthly.values())  # ~$45K/month
```

**Recommendation: Snowflake**

**Rationale:**
1. **Workload isolation is critical:** Quant ad-hoc queries must not block regulatory reporting. Snowflake's separate virtual warehouses provide hard isolation without queue management.
2. **Concurrency for dashboards:** Multi-cluster auto-scaling handles 50 concurrent analysts without degradation. Redshift WLM requires manual tuning.
3. **10-year tick data:** Snowflake's automatic clustering + micro-partitions handle range scans efficiently without manual SORT KEY maintenance.
4. **Compliance:** Both meet requirements; Snowflake's immutable Time Travel (up to 90 days) + Fail-safe (7 days) satisfies most regulatory needs. Longer retention via `CREATE TABLE ... CLONE` or external backup.

**Long-term Compliance Storage:**
```sql
-- Snapshot regulatory data to S3 for 7-year retention
-- (beyond Snowflake's 90-day time travel)
COPY INTO @regulatory_archive_stage/2024/01/15/
FROM (SELECT * FROM finance.regulatory_reports WHERE report_date = '2024-01-15')
FILE_FORMAT = (TYPE = 'PARQUET');
-- S3 with Object Lock COMPLIANCE for WORM storage
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a fact table and a dimension table?" — Fact tables store measurements/events (revenue, clicks, page views) with foreign keys to dimensions. They are wide and tall (many rows). Dimension tables store context (who, what, where, when) — they are wide but shorter. Fact tables are queried with aggregations; dimensions are queried for filtering and grouping.
> **Tip 2:** "When would you use a snowflake schema instead of a star schema?" — A snowflake schema normalizes dimensions (e.g., product → category → department as separate tables). It saves storage and avoids update anomalies but requires more joins. Use star schema for BI tools that benefit from simplicity; snowflake schema when storage is a concern or dimensions are very large.
> **Tip 3:** "How does Snowflake's virtual warehouse model differ from traditional data warehouses?" — Traditional warehouses (Redshift clusters, on-prem) couple compute and storage. Snowflake separates them: storage is always on S3, compute (virtual warehouses) spins up in seconds and suspends when idle. You pay only for compute time used, and multiple warehouses can query the same data concurrently without contention.
