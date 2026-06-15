---
title: "NULL Handling - Scenario Questions"
topic: sql
subtopic: null-handling
content_type: scenario_question
tags: [sql, null, three-valued-logic, avg, coalesce, nullif, data-quality]
---

# NULL Handling — Scenario Questions

<article data-difficulty="junior">

## Scenario 1 (Junior): Why Does NOT IN Return No Rows?

Given this schema:

```sql
CREATE TABLE products (product_id INT, category VARCHAR);
INSERT INTO products VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing'), (4, NULL);

CREATE TABLE excluded_categories (category VARCHAR);
INSERT INTO excluded_categories VALUES ('Electronics'), ('Books'), (NULL);
```

A junior analyst writes:

```sql
SELECT * FROM products
WHERE category NOT IN (SELECT category FROM excluded_categories);
```

They expect to see product 3 (Clothing) and possibly product 4 (NULL). Instead, the query returns 0 rows.

**Questions**:
1. Explain why 0 rows are returned.
2. Fix the query so that product 3 (Clothing) is returned.
3. What should happen to product 4 (NULL) — should it be included?

<details>
<summary>✅ Solution</summary>

**1. Why 0 rows:**

`NOT IN (SELECT category FROM excluded_categories)` expands to:
```
category != 'Electronics' AND category != 'Books' AND category != NULL
```

The last condition `category != NULL` evaluates to UNKNOWN for every row (including rows where category = 'Clothing'). A `TRUE AND TRUE AND UNKNOWN` chain produces UNKNOWN, which fails the WHERE filter. No rows pass.

**2. Fix: filter NULLs from the subquery**

```sql
-- Option A: filter NULLs out of the subquery
SELECT * FROM products
WHERE category NOT IN (
    SELECT category FROM excluded_categories WHERE category IS NOT NULL
);
-- Returns: (3, 'Clothing')

-- Option B: use NOT EXISTS (NULL-safe by design)
SELECT p.*
FROM products p
WHERE NOT EXISTS (
    SELECT 1 FROM excluded_categories e WHERE e.category = p.category
);
-- Returns: (3, 'Clothing') and (4, NULL) because NULL = NULL is UNKNOWN → NOT EXISTS is TRUE for product 4
```

**3. What to do with product 4 (NULL category):**

It depends on business logic:
- **Exclude NULL-category products**: add `AND p.category IS NOT NULL` to the outer query
- **Include NULL-category products**: `NOT EXISTS` returns them automatically (NULL never matches anything)
- **Treat NULL as a valid unknown**: document the decision and be explicit in the query

The `NOT EXISTS` solution is generally preferred because it handles NULLs predictably and performs well on large datasets with proper indexing.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2 (Mid): Fix a Broken AVG Using COALESCE vs NULLIF

You have an `orders` table:

```sql
orders(order_id INT, customer_id INT, revenue DECIMAL, discount DECIMAL, status VARCHAR)
```

Business rule:
- `revenue = NULL` means the order's revenue wasn't captured (data quality issue)
- `discount = NULL` means no discount was applied (not a data quality issue — it's a valid zero)
- `discount = 0` and `discount = NULL` should both mean "no discount" for AOV calculations

The following query is producing wrong results. Identify all the bugs and fix them:

```sql
-- Buggy query
SELECT
    customer_id,
    AVG(revenue)                           AS avg_order_value,
    AVG(revenue - discount)                AS avg_net_revenue,
    COUNT(DISTINCT order_id) FILTER (WHERE discount > 0) AS discounted_orders
FROM orders
WHERE status != 'cancelled'
GROUP BY customer_id;
```

<details>
<summary>✅ Solution</summary>

**Bug 1: `status != 'cancelled'` excludes NULL status rows**

Three-valued logic: `status != 'cancelled'` is UNKNOWN when status is NULL. Fix:
```sql
WHERE (status != 'cancelled' OR status IS NULL)
-- or equivalently:
WHERE status IS DISTINCT FROM 'cancelled'
```

**Bug 2: `AVG(revenue)` excludes NULL-revenue orders from the denominator**

If you want "average revenue per order including ones with no revenue captured":
```sql
AVG(COALESCE(revenue, 0)) AS avg_order_value   -- treat missing as $0
-- vs
AVG(revenue) AS avg_order_value                 -- only orders with known revenue
```

Document which interpretation is correct for your business.

**Bug 3: `AVG(revenue - discount)` propagates NULL when discount is NULL**

Since NULL discount means $0 discount, fix with COALESCE:
```sql
AVG(revenue - COALESCE(discount, 0)) AS avg_net_revenue
```

**Full fixed query:**

```sql
SELECT
    customer_id,
    -- Decide: include NULL-revenue orders as $0, or only average known revenue
    AVG(COALESCE(revenue, 0))                              AS avg_order_value_incl_unknown,
    AVG(revenue)                                           AS avg_order_value_known_only,
    -- NULL discount = $0 discount
    AVG(COALESCE(revenue, 0) - COALESCE(discount, 0))     AS avg_net_revenue,
    -- Count discounted orders: discount > 0 (NULL is not > 0, so this is fine)
    COUNT(DISTINCT order_id) FILTER (WHERE discount > 0)   AS discounted_orders,
    -- Bonus: NULL rate as a data quality signal
    ROUND(100.0 * COUNT(*) FILTER (WHERE revenue IS NULL) / COUNT(*), 2) AS null_revenue_pct
FROM orders
WHERE status IS DISTINCT FROM 'cancelled'
GROUP BY customer_id;
```

**NULLIF bonus usage:**

If `discount = 0` and `discount = NULL` are truly interchangeable, you can normalize them:
```sql
NULLIF(discount, 0)  -- converts 0 → NULL (treat as "no discount")
-- Then COALESCE(NULLIF(discount, 0), 0) is a no-op, but useful if downstream code checks IS NULL
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3 (Senior): Design a Data Quality System for Intentional vs Accidental NULLs

You are designing a data quality framework for a 500M-row `orders` table in Snowflake. The table has these nullable columns:

```sql
orders(
    order_id        BIGINT NOT NULL,
    customer_id     INT,     -- should never be NULL (bug if NULL)
    revenue         DECIMAL, -- NULL = revenue not yet finalized (valid)
    discount        DECIMAL, -- NULL = no discount applied (valid)
    ship_date       DATE,    -- NULL = not yet shipped (valid)
    cancel_date     DATE,    -- NULL = not cancelled (valid)
    carrier_id      INT,     -- NULL = carrier not assigned or data missing (ambiguous!)
    notes           VARCHAR  -- NULL = no notes (valid)
)
```

The problem: `carrier_id = NULL` can mean two things:
1. "Shipment is pending, carrier not yet assigned" (intentional)
2. "Carrier data was lost during an ETL bug" (accidental)

Design a SQL-based data quality system that:
1. Distinguishes intentional NULLs from accidental NULLs for `carrier_id`
2. Alerts when `customer_id` has any NULLs (always a bug)
3. Tracks NULL rates over time so you can detect sudden increases (pipeline bugs)
4. Runs efficiently at 500M rows without a full table scan every run

<details>
<summary>✅ Solution</summary>

**Architecture: incremental daily DQ checks on new/updated partitions**

```sql
-- Step 1: Tag NULL carrier_id by root cause using correlated columns
-- A pending order: ship_date IS NULL AND cancel_date IS NULL
-- A cancelled order: cancel_date IS NOT NULL (carrier legitimately NULL)
-- A shipped order with NULL carrier: ship_date IS NOT NULL AND carrier_id IS NULL → DATA BUG
CREATE OR REPLACE VIEW v_carrier_null_audit AS
SELECT
    order_id,
    ship_date,
    cancel_date,
    carrier_id,
    CASE
        WHEN carrier_id IS NOT NULL              THEN 'ok'
        WHEN ship_date IS NULL AND cancel_date IS NULL THEN 'pending_not_yet_assigned'  -- intentional
        WHEN cancel_date IS NOT NULL             THEN 'cancelled_no_carrier'            -- intentional
        WHEN ship_date IS NOT NULL AND carrier_id IS NULL THEN 'shipped_missing_carrier' -- accidental BUG
        ELSE 'unknown'
    END AS carrier_null_type
FROM orders
WHERE order_date >= CURRENT_DATE - 7;  -- last 7 days only for efficiency

-- Step 2: customer_id NULL check (any NULL = immediate alert)
CREATE OR REPLACE VIEW v_customer_id_nulls AS
SELECT COUNT(*) AS null_customer_id_count
FROM orders
WHERE order_date = CURRENT_DATE - 1   -- yesterday's partition
  AND customer_id IS NULL;
-- Alert if null_customer_id_count > 0

-- Step 3: NULL rate tracking table
CREATE TABLE IF NOT EXISTS dq_null_rates (
    check_date      DATE,
    table_name      VARCHAR,
    column_name     VARCHAR,
    total_rows      BIGINT,
    null_count      BIGINT,
    null_pct        DECIMAL(5,2),
    inserted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 4: Incremental daily DQ job (runs on yesterday's partition only)
INSERT INTO dq_null_rates (check_date, table_name, column_name, total_rows, null_count, null_pct)
SELECT
    CURRENT_DATE - 1                                              AS check_date,
    'orders'                                                      AS table_name,
    col_name,
    total_rows,
    null_count,
    ROUND(100.0 * null_count / NULLIF(total_rows, 0), 2)         AS null_pct
FROM (
    SELECT
        COUNT(*)                                                   AS total_rows,
        'customer_id'                                              AS col_name,
        COUNT(*) FILTER (WHERE customer_id IS NULL)               AS null_count
    FROM orders WHERE order_date = CURRENT_DATE - 1
    UNION ALL
    SELECT COUNT(*), 'carrier_id', COUNT(*) FILTER (WHERE carrier_id IS NULL)
    FROM orders WHERE order_date = CURRENT_DATE - 1
    UNION ALL
    SELECT COUNT(*), 'revenue', COUNT(*) FILTER (WHERE revenue IS NULL)
    FROM orders WHERE order_date = CURRENT_DATE - 1
) stats;

-- Step 5: Alert query — detect sudden increase (>2x yesterday's NULL rate vs 7-day avg)
WITH weekly_avg AS (
    SELECT column_name,
           AVG(null_pct) AS avg_null_pct_7d
    FROM dq_null_rates
    WHERE table_name = 'orders'
      AND check_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 2
    GROUP BY column_name
),
yesterday AS (
    SELECT column_name, null_pct, null_count
    FROM dq_null_rates
    WHERE table_name = 'orders' AND check_date = CURRENT_DATE - 1
)
SELECT
    y.column_name,
    y.null_pct                AS yesterday_null_pct,
    w.avg_null_pct_7d         AS baseline_null_pct,
    y.null_count              AS null_rows_yesterday,
    CASE
        WHEN y.null_pct > w.avg_null_pct_7d * 2  THEN 'ALERT: NULL rate doubled'
        WHEN y.column_name = 'customer_id' AND y.null_count > 0 THEN 'ALERT: customer_id has NULLs'
        ELSE 'ok'
    END AS alert_status
FROM yesterday y
JOIN weekly_avg w ON y.column_name = w.column_name
WHERE y.null_pct > w.avg_null_pct_7d * 2
   OR (y.column_name = 'customer_id' AND y.null_count > 0);
```

**Design decisions to articulate in the interview:**

1. **Partition-scoped checks**: run DQ on `order_date = CURRENT_DATE - 1` to avoid scanning 500M rows daily. Cost: O(1 day) not O(all time)
2. **Context-aware NULL classification**: use correlated columns (ship_date, cancel_date) to distinguish intentional from accidental NULLs — pure NULL rate checks miss this
3. **Historical tracking in `dq_null_rates`**: enables trend detection (sudden increase = pipeline bug) vs baseline drift
4. **Alert thresholds**: `customer_id` has a zero-tolerance threshold; `carrier_id` has a relative threshold (2x baseline)
5. **Scalability**: the UNION ALL structure lets you add new columns to monitor without changing the alert logic

</details>
</article>
