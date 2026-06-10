---
title: "Slowly Changing Dimensions - Senior Deep Dive"
topic: data-modeling
subtopic: slowly-changing-dimensions
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [scd, performance, merge-patterns, late-arriving, large-scale, advanced]
---

# Slowly Changing Dimensions — Senior Deep Dive

## SCD Type 2 with MERGE (Modern Pattern)

The MERGE statement handles the complete SCD Type 2 process in a single atomic operation:

```sql
-- Snowflake/Databricks MERGE for SCD Type 2:
MERGE INTO dim_customer target
USING (
    SELECT 
        customer_id,
        customer_name,
        email,
        city,
        state,
        segment,
        MD5(CONCAT_WS('||', customer_name, email, city, state, segment)) AS hash_diff
    FROM staging_customers
) source
ON target.customer_id = source.customer_id AND target.is_current = TRUE

-- Case 1: Existing customer with changed attributes → expire + insert new
WHEN MATCHED AND target.hash_diff != source.hash_diff THEN
    UPDATE SET 
        effective_end = CURRENT_DATE - 1,
        is_current = FALSE

-- Case 2: Brand new customer → insert first version
WHEN NOT MATCHED THEN
    INSERT (customer_key, customer_id, customer_name, email, city, state, segment,
            hash_diff, effective_start, effective_end, is_current)
    VALUES (NEXT_KEY(), source.customer_id, source.customer_name, source.email,
            source.city, source.state, source.segment, source.hash_diff,
            CURRENT_DATE, '9999-12-31', TRUE);

-- Step 2: Insert new versions for expired records (separate INSERT)
INSERT INTO dim_customer
SELECT 
    NEXT_KEY(),
    s.customer_id, s.customer_name, s.email, s.city, s.state, s.segment,
    MD5(CONCAT_WS('||', s.customer_name, s.email, s.city, s.state, s.segment)),
    CURRENT_DATE, '9999-12-31', TRUE
FROM staging_customers s
JOIN dim_customer d ON s.customer_id = d.customer_id
WHERE d.is_current = FALSE 
  AND d.effective_end = CURRENT_DATE - 1  -- Just expired in the MERGE above
  AND NOT EXISTS (SELECT 1 FROM dim_customer d2 
                  WHERE d2.customer_id = s.customer_id AND d2.is_current = TRUE);
```

## Delta Lake SCD Type 2 (Optimized)

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Load current dimension and staging
dim = DeltaTable.forPath(spark, "/delta/dim_customer")
staging = spark.table("staging_customers")

# Add hash for change detection
staging_with_hash = staging.withColumn(
    "hash_diff",
    F.md5(F.concat_ws("||", 
        F.coalesce(F.col("name"), F.lit("")),
        F.coalesce(F.col("email"), F.lit("")),
        F.coalesce(F.col("city"), F.lit(""))))
)

# SCD Type 2 MERGE
dim.alias("target").merge(
    staging_with_hash.alias("source"),
    "target.customer_id = source.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.hash_diff != source.hash_diff",
    set={
        "effective_end": F.current_date() - 1,
        "is_current": F.lit(False)
    }
).whenNotMatchedInsert(
    values={
        "customer_key": F.monotonically_increasing_id(),
        "customer_id": "source.customer_id",
        "customer_name": "source.name",
        "email": "source.email",
        "city": "source.city",
        "hash_diff": "source.hash_diff",
        "effective_start": F.current_date(),
        "effective_end": F.lit("9999-12-31").cast("date"),
        "is_current": F.lit(True)
    }
).execute()

# Insert new versions for changed records
changed = spark.sql("""
    SELECT s.*, current_date() as effective_start
    FROM staging_customers s
    JOIN dim_customer d ON s.customer_id = d.customer_id
    WHERE d.is_current = false AND d.effective_end = current_date() - 1
""")
changed.write.mode("append").format("delta").save("/delta/dim_customer")
```

## Late-Arriving Dimensions in SCD Type 2

The most complex SCD scenario: facts arrive referencing a dimension version that doesn't exist yet, or dimension changes arrive out of order.

```sql
-- Scenario: Order from March 10 arrives on March 20.
-- Customer moved from NY to Chicago on March 15.
-- Which customer_key should the March 10 order use?

-- ANSWER: The version active on March 10 (the NY version!)
-- Point-in-time lookup:
SELECT customer_key
FROM dim_customer
WHERE customer_id = 'C001'
  AND effective_start <= '2024-03-10'
  AND effective_end >= '2024-03-10';
-- Returns the NY version (key=100), NOT the Chicago version (key=501)

-- Late-arriving dimension (rare but important):
-- March 20: We learn the customer was actually in "Boston" from Jan 1 to March 14
-- (we previously thought they were in NY the whole time)
-- Must SPLIT the existing NY record!

-- Before fix:
-- key=100 | NY | 2020-01-01 to 2024-03-14 | FALSE
-- key=501 | Chicago | 2024-03-15 to 9999-12-31 | TRUE

-- After fix (retroactive correction):
-- key=100 | NY | 2020-01-01 to 2023-12-31 | FALSE (shortened!)
-- key=700 | Boston | 2024-01-01 to 2024-03-14 | FALSE (new inserted version!)
-- key=501 | Chicago | 2024-03-15 to 9999-12-31 | TRUE (unchanged)

-- Facts from Jan 1 - Mar 14 need customer_key=700 (Boston version)
-- Must UPDATE fact_sales where date in that range + customer_id = C001!
UPDATE fact_sales
SET customer_key = 700  -- Boston version
WHERE customer_key = 100  -- Was NY version
  AND date_key BETWEEN 20240101 AND 20240314;
```

## SCD at Scale (Billion-Row Dimensions)

### Partitioning Strategy

```sql
-- For very large SCD Type 2 dimensions (100M+ rows):
-- Partition by is_current (99% of queries need only current!)

CREATE TABLE dim_customer (
    customer_key    BIGINT,
    customer_id     VARCHAR(20),
    ...
    is_current      BOOLEAN,
    effective_start DATE,
    effective_end   DATE
) 
PARTITION BY (is_current)
CLUSTER BY (customer_id);

-- Query "current state": only scans is_current=TRUE partition (small!)
-- Query "historical": only scans is_current=FALSE partition (large, but rare)
```

### Incremental SCD Processing

```sql
-- For daily loads: only process records that MIGHT have changed
-- Don't compare the entire staging table to the entire dimension!

-- Use a change data capture (CDC) stream:
CREATE STREAM stg_customers_stream ON TABLE staging_customers;

-- Only process records that changed since last run:
MERGE INTO dim_customer target
USING stg_customers_stream source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND source.metadata$action = 'INSERT'  -- CDC insert = update in source
    AND target.hash_diff != MD5(CONCAT_WS('||', source.name, source.email, source.city))
THEN UPDATE SET effective_end = CURRENT_DATE - 1, is_current = FALSE
WHEN NOT MATCHED AND source.metadata$action = 'INSERT'
THEN INSERT (...) VALUES (...);
-- Only processes CHANGED records, not full table scan!
```

## Multi-Source SCD

When the same dimension is fed from multiple sources with different update schedules:

```sql
-- Customer data from CRM (daily) + Billing (hourly) + Support (real-time)
-- Each source may update different attributes

-- Approach: Separate satellite per source (Data Vault style) + merged view

CREATE TABLE sat_customer_crm (
    customer_id VARCHAR(20), load_date TIMESTAMP,
    name VARCHAR, email VARCHAR, segment VARCHAR,
    hash_diff BINARY(16), PRIMARY KEY (customer_id, load_date)
);

CREATE TABLE sat_customer_billing (
    customer_id VARCHAR(20), load_date TIMESTAMP,
    billing_address VARCHAR, payment_method VARCHAR,
    hash_diff BINARY(16), PRIMARY KEY (customer_id, load_date)
);

-- Unified SCD Type 2 dimension (merged, with priority rules):
CREATE VIEW dim_customer_current AS
SELECT
    customer_id,
    -- CRM wins for name/email:
    crm.name AS customer_name,
    crm.email,
    crm.segment,
    -- Billing wins for address:
    bill.billing_address,
    bill.payment_method,
    GREATEST(crm.load_date, bill.load_date) AS last_updated
FROM sat_customer_crm crm
JOIN sat_customer_billing bill USING (customer_id)
WHERE crm.load_date = (SELECT MAX(load_date) FROM sat_customer_crm WHERE customer_id = crm.customer_id)
  AND bill.load_date = (SELECT MAX(load_date) FROM sat_customer_billing WHERE customer_id = bill.customer_id);
```

## SCD Type 2 Testing Strategy

```sql
-- Test 1: Only one current row per natural key
SELECT customer_id, COUNT(*)
FROM dim_customer
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- Must return 0 rows!

-- Test 2: No gaps in effective date ranges
SELECT d1.customer_id, d1.effective_end, d2.effective_start
FROM dim_customer d1
JOIN dim_customer d2 ON d1.customer_id = d2.customer_id
    AND d1.effective_end + 1 = d2.effective_start  -- Should be consecutive
WHERE d1.effective_end != '9999-12-31'
  AND NOT EXISTS (
    SELECT 1 FROM dim_customer d3
    WHERE d3.customer_id = d1.customer_id
      AND d3.effective_start = d1.effective_end + 1
  );
-- Must return 0 rows! (no gaps)

-- Test 3: No overlapping date ranges
SELECT d1.customer_id, d1.effective_start, d1.effective_end,
       d2.effective_start, d2.effective_end
FROM dim_customer d1
JOIN dim_customer d2 ON d1.customer_id = d2.customer_id
    AND d1.customer_key != d2.customer_key
    AND d1.effective_start <= d2.effective_end
    AND d1.effective_end >= d2.effective_start;
-- Must return 0 rows! (no overlaps)
```

## Interview Tips

> **Tip 1:** "How do you implement SCD Type 2 at scale?" — (1) Use MERGE for atomic expiration + insertion. (2) Hash-based change detection (only process changed rows, not full table comparison). (3) Partition by is_current (most queries only need current). (4) Use CDC streams for incremental processing (don't rescan full staging). (5) Z-order/cluster by customer_id for efficient lookups.

> **Tip 2:** "How do you handle late-arriving dimensions?" — Most complex case. If a historical correction arrives: you may need to SPLIT an existing version (shorten its effective_end, insert corrected version in the gap). Then UPDATE any fact rows that were loaded pointing to the wrong version. This is expensive — minimize by loading dimensions before facts and having strict data quality at source.

> **Tip 3:** "SCD Type 2 testing?" — Three critical tests: (1) Uniqueness: only ONE current row per natural key. (2) No gaps: consecutive effective_start/end dates for same entity (no missing periods). (3) No overlaps: date ranges never overlap for same entity. Run these after every load. Also test: every fact FK exists in dimension (referential integrity).

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
