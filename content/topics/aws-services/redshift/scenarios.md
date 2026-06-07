---
title: "AWS Redshift - Scenario Questions"
topic: aws-services
subtopic: redshift
content_type: scenario_question
tags: [aws, redshift, interview, scenarios, data-warehouse]
---

# Scenario Questions — AWS Redshift

<article data-difficulty="junior">

## 🟢 Junior: Choose Distribution and Sort Keys

**Scenario:** You're creating a `fact_sales` table (500M rows) that will be frequently joined with `dim_customer` (5M rows) on `customer_id`, and most queries filter by `sale_date`. Choose the DISTKEY, SORTKEY, and distribution style for both tables.

<details>
<summary>✅ Solution</summary>

```sql
-- fact_sales: DISTKEY on join column, SORTKEY on filter column
CREATE TABLE fact_sales (
    sale_id BIGINT ENCODE az64,
    customer_id INT ENCODE az64,
    product_id INT ENCODE az64,
    sale_date DATE ENCODE az64,
    amount DECIMAL(10,2) ENCODE az64,
    quantity INT ENCODE az64
)
DISTSTYLE KEY
DISTKEY(customer_id)        -- Collocate with dim_customer for fast joins
SORTKEY(sale_date);         -- Queries filter by date → zone map pruning

-- dim_customer: Same DISTKEY so join is collocated
CREATE TABLE dim_customer (
    customer_id INT ENCODE az64,
    name VARCHAR(100) ENCODE lzo,
    segment VARCHAR(20) ENCODE bytedict,
    city VARCHAR(50) ENCODE lzo,
    signup_date DATE ENCODE az64
)
DISTSTYLE KEY
DISTKEY(customer_id);       -- Same as fact → join requires ZERO data movement

-- dim_date: DISTSTYLE ALL (tiny table, copied to every node)
CREATE TABLE dim_date (
    date_key DATE,
    month_name VARCHAR(10),
    quarter VARCHAR(2),
    year INT
)
DISTSTYLE ALL;              -- 365 rows copied everywhere → join with no movement
```

**Why these choices:**
- `fact_sales` DISTKEY on `customer_id`: most frequent join column
- `dim_customer` DISTKEY on `customer_id`: matching key → collocated join (no redistribution)
- `fact_sales` SORTKEY on `sale_date`: most queries filter by date, zone maps enable block skipping
- `dim_date` DISTSTYLE ALL: tiny table, every node has a local copy

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnose a Slow Redshift Query

**Scenario:** This query takes 45 minutes on a 3-node dc2.large cluster with 200M rows:

```sql
SELECT c.segment, SUM(s.amount)
FROM fact_sales s
JOIN dim_customer c ON s.customer_id = c.customer_id
WHERE s.sale_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY c.segment;
```

The execution plan shows: `DS_DIST_BOTH` on the join step. What's wrong and how do you fix it?

<details>
<summary>✅ Solution</summary>

**Problem:** `DS_DIST_BOTH` means BOTH tables are being redistributed (shuffled) across nodes for the join. This means the DISTKEY doesn't match the join column on at least one table.

**Diagnosis check:**

```sql
-- Check distribution styles
SELECT tablename, diststyle
FROM pg_table_def 
WHERE schemaname = 'public' AND tablename IN ('fact_sales', 'dim_customer');
-- If either shows 'EVEN' or a different DISTKEY → mismatch!
```

**Most likely cause:** Tables were created with DISTSTYLE EVEN (default) or DISTKEY on a different column.

**Fix:**

```sql
-- Option 1: Recreate with correct DISTKEY (requires data reload)
CREATE TABLE fact_sales_new (LIKE fact_sales)
DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(sale_date);

INSERT INTO fact_sales_new SELECT * FROM fact_sales;
DROP TABLE fact_sales;
ALTER TABLE fact_sales_new RENAME TO fact_sales;

-- Option 2: If dim_customer is small enough, use ALL distribution
ALTER TABLE dim_customer ALTER DISTSTYLE ALL;
-- Now every node has a local copy → no redistribution needed
```

**After fix:** The join plan should show `DS_DIST_NONE` (no redistribution needed) — massive speedup.

**Expected improvement:** 45 minutes → 2-3 minutes (eliminating the cross-node shuffle of 200M rows).

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Hot/Cold Architecture with Spectrum

**Scenario:** Your Redshift cluster stores 5 years of sales data (10 TB). 95% of queries only access the last 6 months. The cluster costs $50K/month. Leadership wants to reduce cost by 60% without impacting performance for recent data queries. Design the solution.

<details>
<summary>✅ Solution</summary>

**Architecture: Hot data in Redshift, cold data in S3 via Spectrum**

```sql
-- Step 1: Keep last 6 months in Redshift (hot — fast queries)
-- fact_sales: ~1 TB (recent data)
CREATE TABLE fact_sales (
    sale_id BIGINT, customer_id INT, amount DECIMAL, sale_date DATE, ...
)
DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(sale_date);
-- Only holds data WHERE sale_date >= CURRENT_DATE - 180

-- Step 2: Move historical data to S3 (cold — cheap storage)
-- UNLOAD old partitions to S3 as Parquet
UNLOAD ('SELECT * FROM fact_sales WHERE sale_date < CURRENT_DATE - 180')
TO 's3://data-lake/archive/fact_sales/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
PARQUET
PARTITION BY (year, month)
ALLOWOVERWRITE;

-- Delete old data from Redshift to free space
DELETE FROM fact_sales WHERE sale_date < CURRENT_DATE - 180;
VACUUM fact_sales;

-- Step 3: Create external table pointing to S3 archive
CREATE EXTERNAL TABLE spectrum.fact_sales_archive (
    sale_id BIGINT, customer_id INT, amount DECIMAL(10,2), ...
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://data-lake/archive/fact_sales/';

-- Step 4: Create a unified view (transparent to users)
CREATE VIEW unified_sales AS
SELECT * FROM fact_sales              -- Hot: last 6 months (fast, in Redshift)
UNION ALL
SELECT * FROM spectrum.fact_sales_archive  -- Cold: older data (S3, via Spectrum)
;

-- Users query the view — don't need to know about the split
SELECT segment, SUM(amount)
FROM unified_sales
WHERE sale_date >= '2024-01-01'     -- Optimizer routes to Redshift (hot)
GROUP BY segment;
```

**Cost calculation:**

| Component | Before | After | Savings |
|-----------|--------|-------|---------|
| Redshift cluster (10 TB) | $50,000/month | — | — |
| Redshift cluster (1 TB, downsize) | — | $15,000/month | $35,000 |
| S3 storage (9 TB archive) | — | $210/month | — |
| Spectrum queries (occasional) | — | $500/month | — |
| **Total** | **$50,000** | **$15,710** | **68% savings** |

**Automation (monthly maintenance):**

```sql
-- Scheduled procedure: move data older than 6 months to S3
CREATE PROCEDURE archive_old_data()
AS $$
BEGIN
    -- Unload old data
    EXECUTE 'UNLOAD (''SELECT * FROM fact_sales WHERE sale_date < CURRENT_DATE - 180'')
    TO ''s3://data-lake/archive/fact_sales/'' PARQUET PARTITION BY (year, month)';
    
    -- Delete from Redshift
    DELETE FROM fact_sales WHERE sale_date < CURRENT_DATE - 180;
    
    -- Add new Spectrum partitions
    -- (Use Glue Crawler or ALTER TABLE ADD PARTITION)
    
    VACUUM fact_sales;
    ANALYZE fact_sales;
END;
$$ LANGUAGE plpgsql;
```

**Performance impact:**
- Queries on last 6 months: NO change (still hit fast Redshift storage)
- Queries spanning years: slightly slower (Spectrum reads from S3)
- Acceptable trade-off: 95% of queries hit only hot data

</details>

</article>
