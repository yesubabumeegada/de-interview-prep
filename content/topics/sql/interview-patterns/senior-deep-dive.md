---
title: "SQL Interview Patterns - Senior Deep Dive"
topic: sql
subtopic: interview-patterns
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, interview-patterns, recursive-cte, market-basket, time-series-interpolation, deduplication, attribution, pivot-dynamic]
---

# SQL Interview Patterns — Senior Deep Dive

Senior SQL interviews go beyond templates: graph traversal in SQL, market basket analysis, time-series gap filling, complex deduplication with ties, dynamic pivots, and multi-touch attribution modeling.

---

## Graph Traversal — Recursive CTE for Org Hierarchy

**Problem**: "Find all employees who report (directly or indirectly) to a given manager."

```sql
-- employees(employee_id, name, manager_id)
WITH RECURSIVE org_tree AS (
    -- Anchor: start with the root manager
    SELECT employee_id, name, manager_id, 0 AS depth
    FROM employees
    WHERE employee_id = 42  -- the manager we're exploring from

    UNION ALL

    -- Recursive: find everyone who reports to someone already in the tree
    SELECT e.employee_id, e.name, e.manager_id, ot.depth + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT employee_id, name, depth
FROM org_tree
ORDER BY depth, name;

-- BigQuery: same syntax (supports recursive CTEs from 2022)
-- Snowflake: same syntax
-- Spark SQL: does NOT support recursive CTEs — use iterative approach or GraphX
```

### Friend-of-Friend (Social Graph)

```sql
-- friends(user_a INT, user_b INT) — undirected edges
WITH RECURSIVE reachable AS (
    -- Direct friends of user 1
    SELECT user_b AS friend_id, 1 AS degree
    FROM friends WHERE user_a = 1
    UNION
    SELECT user_a AS friend_id, 1 AS degree
    FROM friends WHERE user_b = 1

    UNION ALL

    -- 2nd degree: friends of friends (stop at depth 2)
    SELECT
        CASE WHEN f.user_a = r.friend_id THEN f.user_b ELSE f.user_a END,
        r.degree + 1
    FROM friends f
    JOIN reachable r ON (f.user_a = r.friend_id OR f.user_b = r.friend_id)
    WHERE r.degree < 2
)
SELECT DISTINCT friend_id, MIN(degree) AS degree
FROM reachable
WHERE friend_id != 1
GROUP BY friend_id;
```

---

## Market Basket Analysis — Item Co-Occurrence

**Problem**: "Find pairs of products frequently bought together."

```sql
-- order_items(order_id, product_id)
WITH item_pairs AS (
    -- Self-join to create all pairs within the same order
    SELECT
        a.product_id AS product_a,
        b.product_id AS product_b,
        a.order_id
    FROM order_items a
    JOIN order_items b
      ON a.order_id = b.order_id
      AND a.product_id < b.product_id   -- avoid (A,B) and (B,A) duplicates
)
SELECT
    product_a,
    product_b,
    COUNT(DISTINCT order_id)            AS co_occurrence_count,
    -- Support: fraction of orders containing both items
    COUNT(DISTINCT order_id)::float / (SELECT COUNT(DISTINCT order_id) FROM order_items)
                                        AS support,
    -- Lift: how much more likely to be bought together vs independently?
    COUNT(DISTINCT order_id)::float
        / (
            (SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_id = item_pairs.product_a)::float
            * (SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_id = item_pairs.product_b)::float
            / (SELECT COUNT(DISTINCT order_id) FROM order_items)
          ) AS lift
FROM item_pairs
GROUP BY product_a, product_b
HAVING COUNT(DISTINCT order_id) >= 10  -- minimum support threshold
ORDER BY co_occurrence_count DESC
LIMIT 20;
```

---

## Time-Series Interpolation — Filling Gaps

**Problem**: "For a daily metrics table with missing days, produce a row for every day, filling gaps with the last known value."

```sql
-- daily_metrics(metric_date DATE, value DECIMAL) -- has gaps
-- Goal: one row per day, NULL-filled or forward-filled

-- Step 1: generate a complete date spine
WITH date_spine AS (
    SELECT generate_series(         -- Postgres
        '2024-01-01'::date,
        '2024-03-31'::date,
        INTERVAL '1 day'
    )::date AS day
    -- BigQuery: UNNEST(GENERATE_DATE_ARRAY('2024-01-01','2024-03-31',INTERVAL 1 DAY))
    -- Spark: explode(sequence(DATE'2024-01-01', DATE'2024-03-31', INTERVAL 1 DAY))
),
-- Step 2: left join metrics onto spine
joined AS (
    SELECT
        ds.day,
        m.value
    FROM date_spine ds
    LEFT JOIN daily_metrics m ON ds.day = m.metric_date
),
-- Step 3: forward-fill using LAST_VALUE IGNORE NULLS
forward_filled AS (
    SELECT
        day,
        value,
        LAST_VALUE(value IGNORE NULLS) OVER (
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_value
    FROM joined
)
SELECT day, COALESCE(value, filled_value) AS value
FROM forward_filled
ORDER BY day;
```

---

## Complex Deduplication with Tie-Breaking

**Problem**: "Keep the latest version of each customer record. If two records share the same `updated_at`, keep the one with the highest `record_id`. If that's also tied, keep any one deterministically."

```sql
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                updated_at DESC,        -- primary: most recent
                record_id DESC,         -- secondary: highest ID breaks updated_at ties
                MD5(CAST(record_id AS VARCHAR))  -- tertiary: pseudo-random but deterministic
        ) AS rn
    FROM customers_raw
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1;
```

For large datasets, avoid sorting by MD5 in the window function — instead hash-partition the data and use a deterministic filter:

```sql
-- More efficient: filter in two passes
WITH deduped_ts AS (
    SELECT *,
        MAX(updated_at) OVER (PARTITION BY customer_id) AS max_ts
    FROM customers_raw
),
deduped_id AS (
    SELECT *,
        MAX(record_id) OVER (PARTITION BY customer_id) AS max_id
    FROM deduped_ts
    WHERE updated_at = max_ts
)
SELECT * EXCEPT (max_ts, max_id)
FROM deduped_id
WHERE record_id = max_id;
```

---

## Dynamic Pivot with STRING_AGG Approach

When pivot columns are unknown at query-write time, use `STRING_AGG` to build column headers, then generate the query dynamically.

```sql
-- Find all distinct months to pivot on
SELECT STRING_AGG(
    DISTINCT 'SUM(CASE WHEN month = ''' || month_key || ''' THEN revenue END) AS m_' || month_key,
    ', '
    ORDER BY month_key
) AS pivot_columns
FROM (
    SELECT TO_CHAR(order_date, 'YYYY_MM') AS month_key
    FROM orders
    WHERE order_date >= '2024-01-01'
) t;
-- Copy-paste output into a dynamic SQL EXECUTE statement in Postgres
-- Or use Snowflake's dynamic SQL / BigQuery's scripting

-- Snowflake: EXECUTE IMMEDIATE with PIVOT
EXECUTE IMMEDIATE
'SELECT * FROM orders
 PIVOT (SUM(revenue) FOR month IN (' || month_list || '))';
```

---

## Attribution Modeling in SQL

**Problem**: "Assign credit for a conversion to the marketing touchpoints that preceded it."

```sql
-- touchpoints(user_id, channel, touched_at)
-- conversions(user_id, converted_at, revenue)
WITH user_touches AS (
    SELECT
        t.user_id,
        t.channel,
        t.touched_at,
        c.converted_at,
        c.revenue,
        ROW_NUMBER() OVER (PARTITION BY t.user_id ORDER BY t.touched_at)     AS touch_num,
        COUNT(*) OVER (PARTITION BY t.user_id)                               AS total_touches
    FROM touchpoints t
    JOIN conversions c
      ON t.user_id = c.user_id
      AND t.touched_at <= c.converted_at
)
SELECT
    channel,
    -- First-touch: 100% credit to the first touchpoint
    SUM(CASE WHEN touch_num = 1 THEN revenue ELSE 0 END) AS first_touch_revenue,
    -- Last-touch: 100% credit to the last touchpoint
    SUM(CASE WHEN touch_num = total_touches THEN revenue ELSE 0 END) AS last_touch_revenue,
    -- Linear: equal credit split across all touchpoints
    SUM(revenue / total_touches) AS linear_revenue
FROM user_touches
GROUP BY channel
ORDER BY linear_revenue DESC;
```

---

## Writing Readable SQL for Interviews

Interviewers evaluate not just correctness but clarity. CTEs over subqueries every time.

```sql
-- Hard to read: nested subqueries
SELECT user_id, total
FROM (SELECT user_id, SUM(r) AS total FROM (SELECT user_id, revenue AS r FROM orders WHERE status='paid') t GROUP BY 1) u
WHERE total > 1000;

-- Easy to read: named CTEs with intent
-- Step 1: paid orders
WITH paid_orders AS (
    SELECT user_id, revenue
    FROM orders
    WHERE status = 'paid'
),
-- Step 2: total revenue per user
user_totals AS (
    SELECT user_id, SUM(revenue) AS total_revenue
    FROM paid_orders
    GROUP BY user_id
)
-- Step 3: filter high-value users
SELECT user_id, total_revenue
FROM user_totals
WHERE total_revenue > 1000
ORDER BY total_revenue DESC;
```

**In-interview formatting rules:**
1. One CTE per logical step, named after what it contains
2. All keywords uppercase, table/column names lowercase
3. Align `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY` on the left margin
4. Always include a comment above each CTE explaining its purpose
5. Test your logic with 2-3 mental rows before finalizing

---

## Key Takeaways

- Recursive CTEs handle org hierarchies and graph traversal; Spark SQL does NOT support them — mention this in interviews
- Market basket analysis uses a self-join with `a.id < b.id` to avoid duplicate pairs, plus HAVING for minimum support
- Time-series gap filling requires a date spine + LEFT JOIN + `LAST_VALUE IGNORE NULLS` for forward-fill
- Complex deduplication: use multi-column ORDER BY in `ROW_NUMBER()` to break ties deterministically
- Attribution modeling: `touch_num = 1` for first-touch, `touch_num = total_touches` for last-touch, `revenue / total_touches` for linear
- Always use CTEs in interviews — they demonstrate clarity of thought, not just SQL knowledge
