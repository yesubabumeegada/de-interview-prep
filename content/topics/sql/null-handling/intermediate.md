---
title: "NULL Handling - Intermediate"
topic: sql
subtopic: null-handling
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, null, aggregation, window-functions, group-by, anti-join, null-safe-equality]
---

# NULL Handling — Intermediate

Intermediate NULL bugs are subtler: they hide in aggregations, window functions, anti-joins, and GROUP BY results. These are the bugs that appear in production dashboards but are hard to reproduce in unit tests.

---

## NULLs in Aggregations

This is where the most impactful production bugs live. Aggregation functions silently ignore NULLs — which is sometimes right and sometimes catastrophically wrong.

### COUNT(*) vs COUNT(col)

```sql
-- Setup: 5 rows, 2 have NULL revenue
-- | order_id | revenue |
-- | 1        | 100     |
-- | 2        | 200     |
-- | 3        | NULL    |
-- | 4        | 150     |
-- | 5        | NULL    |

SELECT COUNT(*)        AS total_rows,     -- 5 (counts every row)
       COUNT(revenue)  AS non_null_rows,  -- 3 (counts only non-NULL revenue)
       SUM(revenue)    AS total,          -- 450 (ignores NULLs)
       AVG(revenue)    AS average         -- 150.0 = 450/3, NOT 450/5 = 90!
FROM orders;
```

The AVG is 150, not 90. If you expected "average revenue per order including the ones with no revenue recorded" the answer is wrong by 40%.

```sql
-- Correct: include NULLs in the denominator
SELECT SUM(revenue) / COUNT(*) AS avg_including_nulls   -- 90.0
FROM orders;

-- Or explicitly:
SELECT AVG(COALESCE(revenue, 0)) AS avg_including_nulls  -- 90.0
FROM orders;
```

Always decide: should NULLs be excluded (true unknowns) or treated as zeros (missing data that should default to 0)?

---

## NULLs in Window Functions — IGNORE NULLS

`LAG`, `LEAD`, `FIRST_VALUE`, and `LAST_VALUE` have an optional `IGNORE NULLS` clause that skips NULL rows when looking backward/forward.

```sql
-- Without IGNORE NULLS: LAG returns NULL if the previous row was NULL
SELECT user_id,
       event_date,
       score,
       LAG(score) OVER (PARTITION BY user_id ORDER BY event_date) AS prev_score
FROM user_scores;
-- If score on row 3 is NULL, prev_score on row 4 = NULL

-- With IGNORE NULLS: skip over NULL rows, find the last non-NULL value
SELECT user_id,
       event_date,
       score,
       LAG(score IGNORE NULLS) OVER (PARTITION BY user_id ORDER BY event_date) AS last_known_score
FROM user_scores;
-- If score on row 3 is NULL, prev_score on row 4 = score from row 2
```

Support varies:
- Snowflake: `LAG(col IGNORE NULLS)` — supported natively
- BigQuery: `LAG(col IGNORE NULLS)` — supported
- Postgres: NOT supported — requires a workaround with `LAST_VALUE ... FILTER` or a subquery
- Spark SQL 3.2+: `LAG(col, 1, NULL) IGNORE NULLS` — supported

Postgres workaround:
```sql
-- Postgres: last non-null score per user
SELECT user_id, event_date, score,
    LAST_VALUE(score) OVER (
        PARTITION BY user_id
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_non_null_score
FROM (
    SELECT user_id, event_date, score
    FROM user_scores
    WHERE score IS NOT NULL  -- pre-filter NULLs; loses NULL rows entirely
) t;
```

---

## NULLs in GROUP BY

NULLs in GROUP BY columns are treated as a single group — all NULL values are bucketed together. This behavior is standard but can mislead if you don't expect it.

```sql
SELECT region, COUNT(*) AS orders
FROM orders
GROUP BY region;
-- | region | orders |
-- | East   | 150    |
-- | West   | 200    |
-- | NULL   | 45     | ← all NULL-region orders aggregated here
```

This is sometimes useful (you can see "how many orders have no region assigned"), but can also mask data quality issues. Always check if a NULL group is expected.

```sql
-- Flag it explicitly
SELECT COALESCE(region, 'UNKNOWN') AS region,
       COUNT(*) AS orders
FROM orders
GROUP BY region;  -- GROUP BY the original column, COALESCE only in SELECT
```

---

## NULLs in CASE WHEN

A `CASE WHEN` with no matching condition and no `ELSE` clause implicitly returns NULL. This is a common source of silent NULLs.

```sql
-- Implicit NULL: no ELSE
SELECT order_id,
       CASE
           WHEN status = 'shipped'   THEN 'in-transit'
           WHEN status = 'delivered' THEN 'complete'
           -- status = 'cancelled' → falls through → NULL
       END AS status_label
FROM orders;

-- Better: explicit ELSE
SELECT order_id,
       CASE
           WHEN status = 'shipped'   THEN 'in-transit'
           WHEN status = 'delivered' THEN 'complete'
           ELSE status   -- pass through unexpected values as-is
       END AS status_label
FROM orders;
```

---

## Anti-Join NULL Gotcha — NOT IN with NULLs Returns No Rows

This is one of the most dangerous NULL behaviors in SQL. `NOT IN` with a subquery that returns even one NULL row returns **zero rows**.

```sql
-- Suppose blacklist_users has one NULL user_id
SELECT user_id FROM blacklist_users;
-- | user_id |
-- | 5       |
-- | 12      |
-- | NULL    |

-- This returns 0 rows — even for user_id = 1 which is clearly not blacklisted!
SELECT * FROM orders
WHERE user_id NOT IN (SELECT user_id FROM blacklist_users);

-- Why: NOT IN is equivalent to:
-- user_id != 5 AND user_id != 12 AND user_id != NULL
-- user_id != NULL → UNKNOWN → entire AND expression = UNKNOWN → row excluded

-- Fix option 1: filter NULLs in subquery
SELECT * FROM orders
WHERE user_id NOT IN (SELECT user_id FROM blacklist_users WHERE user_id IS NOT NULL);

-- Fix option 2: use NOT EXISTS (NULL-safe)
SELECT * FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM blacklist_users b WHERE b.user_id = o.user_id
);
```

**Always prefer `NOT EXISTS` over `NOT IN` when the subquery might contain NULLs.**

---

## NULL-Safe Equality Operators

Standard `=` returns UNKNOWN when either side is NULL. Some dialects provide NULL-safe equality (returns TRUE when both sides are NULL).

```sql
-- Postgres: IS NOT DISTINCT FROM (NULL-safe =)
SELECT 1 IS NOT DISTINCT FROM 1;    -- TRUE
SELECT 1 IS NOT DISTINCT FROM 2;    -- FALSE
SELECT NULL IS NOT DISTINCT FROM NULL;  -- TRUE  ← unlike = which gives UNKNOWN
SELECT 1 IS NOT DISTINCT FROM NULL; -- FALSE

-- IS DISTINCT FROM (NULL-safe !=)
SELECT NULL IS DISTINCT FROM NULL;  -- FALSE

-- Snowflake: same as Postgres
SELECT NULL IS NOT DISTINCT FROM NULL;  -- TRUE

-- MySQL: <=> (NULL-safe equality operator)
SELECT NULL <=> NULL;  -- 1 (TRUE)
SELECT 1 <=> NULL;     -- 0 (FALSE)

-- BigQuery: no shorthand — use IS NOT DISTINCT FROM or explicit NULL check
-- (col1 = col2 OR (col1 IS NULL AND col2 IS NULL))
```

NULL-safe equality is essential for:
- Deduplication: treating `(NULL, 'NY')` and `(NULL, 'NY')` as the same record
- Change detection in SCD2: don't trigger an update when NULL stays NULL
- Self-joins: matching rows where keys might be NULL

---

## Key Takeaways

- `COUNT(col)` counts non-NULLs; `COUNT(*)` counts all rows — `AVG(col)` divides by non-NULL count, not total rows
- `LAG/LEAD IGNORE NULLS` skips over NULLs when looking for previous/next value — check dialect support
- NULLs in GROUP BY form their own group — decide whether this is intentional or a data quality issue
- `CASE WHEN` without `ELSE` returns NULL for unmatched rows — always add `ELSE`
- `NOT IN (subquery)` returns 0 rows if the subquery contains any NULLs — use `NOT EXISTS` instead
- Use `IS NOT DISTINCT FROM` (Postgres/Snowflake) or `<=>` (MySQL) for NULL-safe equality comparisons
