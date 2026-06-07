---
title: "SQL CTEs - Real-World Production Examples"
topic: sql
subtopic: ctes
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, ctes, production, etl, data-pipeline, dbt, analytics]
---

# SQL CTEs — Real-World Production Examples

## Pattern 1: Month-over-Month Comparison Report

```sql
WITH 
current_month AS (
    SELECT 
        product_category,
        SUM(revenue) AS revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS order_count
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2024 AND d.month_num = 3  -- March 2024
    GROUP BY product_category
),
prior_month AS (
    SELECT 
        product_category,
        SUM(revenue) AS revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS order_count
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2024 AND d.month_num = 2  -- February 2024
    GROUP BY product_category
)
SELECT 
    COALESCE(c.product_category, p.product_category) AS category,
    c.revenue AS mar_revenue,
    p.revenue AS feb_revenue,
    c.revenue - p.revenue AS revenue_change,
    ROUND((c.revenue - p.revenue) / NULLIF(p.revenue, 0) * 100, 1) AS revenue_pct_change,
    c.unique_customers AS mar_customers,
    p.unique_customers AS feb_customers,
    c.unique_customers - p.unique_customers AS customer_change
FROM current_month c
FULL OUTER JOIN prior_month p ON c.product_category = p.product_category
ORDER BY revenue_change DESC;
```

> **Why CTE here:** The same aggregation pattern is used twice (current and prior month). Without CTEs, you'd have a deeply nested correlated subquery.

---

## Pattern 2: Cohort Retention Analysis

```sql
WITH 
-- Step 1: Determine each user's cohort (signup month)
user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', signup_date) AS cohort_month
    FROM users
),

-- Step 2: Get each user's active months
user_activity AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('month', event_date) AS active_month
    FROM events
    WHERE event_type = 'login'
),

-- Step 3: Join cohort with activity to calculate months since signup
cohort_activity AS (
    SELECT 
        uc.cohort_month,
        ua.active_month,
        EXTRACT(MONTH FROM AGE(ua.active_month, uc.cohort_month)) AS months_since_signup,
        COUNT(DISTINCT uc.user_id) AS active_users
    FROM user_cohorts uc
    JOIN user_activity ua ON uc.user_id = ua.user_id
    GROUP BY uc.cohort_month, ua.active_month
),

-- Step 4: Get cohort sizes
cohort_sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_month
)

-- Final: retention percentages
SELECT 
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_signup,
    ca.active_users,
    ROUND(ca.active_users * 100.0 / cs.cohort_size, 1) AS retention_pct
FROM cohort_activity ca
JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month
WHERE ca.months_since_signup <= 12
ORDER BY ca.cohort_month, ca.months_since_signup;
```

**Result (sample):**

| cohort_month | cohort_size | months_since_signup | active_users | retention_pct |
|-------------|------------|-------------------|-------------|--------------|
| 2024-01 | 5000 | 0 | 5000 | 100.0% |
| 2024-01 | 5000 | 1 | 3200 | 64.0% |
| 2024-01 | 5000 | 2 | 2100 | 42.0% |
| 2024-01 | 5000 | 3 | 1800 | 36.0% |

---

## Pattern 3: Sessionization from Raw Events

```sql
WITH 
-- Step 1: Order events and detect gaps > 30 min
event_gaps AS (
    SELECT 
        user_id,
        event_timestamp,
        page_url,
        LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_event,
        EXTRACT(EPOCH FROM (
            event_timestamp - LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp)
        )) / 60.0 AS gap_minutes
    FROM raw_clickstream
    WHERE event_date = '2024-01-15'
),

-- Step 2: Flag session boundaries (gap > 30 min = new session)
session_flags AS (
    SELECT *,
        CASE WHEN gap_minutes > 30 OR gap_minutes IS NULL THEN 1 ELSE 0 END AS is_new_session
    FROM event_gaps
),

-- Step 3: Assign session IDs using cumulative sum of flags
sessionized AS (
    SELECT *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id ORDER BY event_timestamp
        ) AS session_num
    FROM session_flags
),

-- Step 4: Session-level aggregates
session_stats AS (
    SELECT 
        user_id,
        session_num,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        COUNT(*) AS page_views,
        COUNT(DISTINCT page_url) AS unique_pages,
        EXTRACT(EPOCH FROM (MAX(event_timestamp) - MIN(event_timestamp))) / 60.0 AS duration_min
    FROM sessionized
    GROUP BY user_id, session_num
)

SELECT 
    user_id,
    session_num,
    session_start,
    duration_min,
    page_views,
    unique_pages
FROM session_stats
WHERE duration_min > 0
ORDER BY user_id, session_start;
```

> **This is a classic DE interview question.** The CTE structure makes the four-step logic readable: detect gaps → flag boundaries → number sessions → aggregate.

---

## Pattern 4: Incremental CDC Processing

```sql
-- Process only new/changed records since last load
WITH 
-- Get high-water mark (last processed timestamp)
watermark AS (
    SELECT MAX(last_loaded_at) AS hwm
    FROM etl_control
    WHERE table_name = 'orders'
),

-- Extract new records since watermark
new_records AS (
    SELECT o.*
    FROM source_db.orders o
    CROSS JOIN watermark w
    WHERE o.updated_at > w.hwm
),

-- Deduplicate (keep latest version per order)
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM new_records
),

-- Final clean records
final_records AS (
    SELECT 
        order_id, customer_id, amount, status, 
        updated_at, CURRENT_TIMESTAMP AS loaded_at
    FROM deduped
    WHERE rn = 1
)

-- MERGE into target (idempotent upsert)
MERGE INTO warehouse.orders t
USING final_records s ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET 
        customer_id = s.customer_id,
        amount = s.amount,
        status = s.status,
        updated_at = s.updated_at,
        loaded_at = s.loaded_at
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, status, updated_at, loaded_at)
    VALUES (s.order_id, s.customer_id, s.amount, s.status, s.updated_at, s.loaded_at);
```

---

## Pattern 5: dbt Model with CTEs (Industry Standard)

This is how dbt (data build tool) structures transformation models:

```sql
-- models/marts/fct_orders.sql
-- dbt model: transforms staging data into a fact table

WITH 

-- Source references (dbt handles schema resolution)
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Business logic
order_enriched AS (
    SELECT 
        o.order_id,
        o.order_date,
        o.customer_id,
        o.amount,
        o.discount,
        o.amount - o.discount AS net_amount,
        c.customer_segment,
        c.first_order_date,
        CASE 
            WHEN o.order_date = c.first_order_date THEN 'New'
            ELSE 'Returning'
        END AS customer_type
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
),

-- Final grain validation
final AS (
    SELECT 
        order_id,
        order_date,
        customer_id,
        net_amount,
        customer_segment,
        customer_type
    FROM order_enriched
    WHERE net_amount > 0  -- Exclude fully-discounted orders
)

SELECT * FROM final
```

> **dbt convention:** Every model is structured as: source CTEs → transformation CTEs → final CTE. The last line is always `SELECT * FROM final`. This is the industry standard for analytics engineering.

---

## Pattern 6: Recursive Hierarchy Flattening for BI

```sql
-- Flatten an N-level category hierarchy into a fixed-width table for BI tools
WITH RECURSIVE category_tree AS (
    -- Level 1 (root categories)
    SELECT 
        category_id,
        category_name AS level_1,
        CAST(NULL AS VARCHAR) AS level_2,
        CAST(NULL AS VARCHAR) AS level_3,
        1 AS depth
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Deeper levels
    SELECT 
        c.category_id,
        ct.level_1,
        CASE WHEN ct.depth = 1 THEN c.category_name ELSE ct.level_2 END,
        CASE WHEN ct.depth = 2 THEN c.category_name ELSE ct.level_3 END,
        ct.depth + 1
    FROM categories c
    JOIN category_tree ct ON c.parent_id = ct.category_id
    WHERE ct.depth < 4
)
SELECT category_id, level_1, level_2, level_3
FROM category_tree
WHERE depth = (SELECT MAX(depth) FROM category_tree t2 WHERE t2.category_id = category_tree.category_id);
```

**Result:**

| category_id | level_1 | level_2 | level_3 |
|------------|---------|---------|---------|
| 101 | Electronics | Phones | Smartphones |
| 102 | Electronics | Phones | Feature Phones |
| 103 | Electronics | Computers | Laptops |
| 201 | Clothing | Men | Shirts |

> **Why flatten?** BI tools (Tableau, Power BI) work better with fixed columns than variable-depth hierarchies. This CTE produces a BI-friendly flat table.

---

## Interview Tips

> **Tip 1:** "How do you structure a complex analytical query?" — "I use the dbt CTE pattern: source CTEs that reference raw tables, transformation CTEs that apply business logic step-by-step, and a final CTE that defines the output schema. Each CTE has a descriptive name documenting what it does."

> **Tip 2:** "Write a sessionization query" — This is one of the top-5 DE interview questions. Use the four-CTE pattern: calculate gaps (LAG) → flag boundaries (CASE WHEN gap > threshold) → assign session IDs (cumulative SUM) → aggregate per session.

> **Tip 3:** "How do you process hierarchies in SQL?" — "Recursive CTE with anchor (root nodes) and recursive step (join children). I always add cycle detection and depth limits. For very deep hierarchies (1000+ levels), I use an iterative temp table approach instead."
