---
title: "SQL Interview Coding Problems — Senior Deep Dive"
topic: sql
subtopic: interview-coding-problems
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [sql, interview, scd2, funnel, attribution, time-series, streak, senior]
---

# SQL Interview Coding Problems — Senior Deep Dive

These problems are asked in senior DE / staff engineer interviews. They require understanding of dimensional modeling, complex window function logic, and production SQL patterns. Expect to be asked to optimize and explain trade-offs, not just produce working code.

---

## Problem 1: SCD Type 2 Point-in-Time Join

**Interview prompt:** "Your orders fact table has an order_date column. Your customer dimension table is SCD Type 2 — each row has effective_from and effective_to dates. Write a query to report revenue by the customer tier they had AT THE TIME of purchase, not their current tier."

This is the canonical senior SQL interview problem for any company with a data warehouse.

### Setup

```sql
CREATE TABLE dim_customer_scd2 (
    customer_key  INT,     -- surrogate key
    customer_id   INT,     -- business key
    name          VARCHAR(100),
    tier          VARCHAR(20),  -- 'bronze', 'silver', 'gold'
    effective_from DATE,
    effective_to   DATE    -- NULL means current record
);

INSERT INTO dim_customer_scd2 VALUES
(1, 101, 'Alice', 'bronze', '2023-01-01', '2023-06-30'),
(2, 101, 'Alice', 'silver', '2023-07-01', '2023-12-31'),
(3, 101, 'Alice', 'gold',   '2024-01-01', NULL),         -- current
(4, 102, 'Bob',   'silver', '2023-01-01', '2023-09-30'),
(5, 102, 'Bob',   'gold',   '2023-10-01', NULL);          -- current

CREATE TABLE fact_orders (
    order_id    INT,
    customer_id INT,
    order_date  DATE,
    amount      DECIMAL(10,2)
);

INSERT INTO fact_orders VALUES
(1001, 101, '2023-03-15', 200.00),  -- Alice was bronze
(1002, 101, '2023-08-20', 350.00),  -- Alice was silver
(1003, 101, '2024-02-10', 500.00),  -- Alice is gold
(1004, 102, '2023-05-01', 180.00),  -- Bob was silver
(1005, 102, '2024-01-15', 420.00);  -- Bob is gold
```

### The wrong way (current tier join)

```sql
-- WRONG: joins to current record only
SELECT
    f.order_id,
    f.customer_id,
    f.amount,
    d.tier  -- This is Alice's CURRENT tier (gold), not her tier at order time
FROM fact_orders f
JOIN dim_customer_scd2 d
    ON f.customer_id = d.customer_id
WHERE d.effective_to IS NULL;  -- Only gets current records
```

This query incorrectly attributes all of Alice's historical orders to her current gold tier.

### The correct way (range join)

```sql
SELECT
    f.order_id,
    f.customer_id,
    f.order_date,
    f.amount,
    d.tier AS tier_at_time_of_order
FROM fact_orders f
JOIN dim_customer_scd2 d
    ON  f.customer_id = d.customer_id
    AND f.order_date BETWEEN d.effective_from
                         AND COALESCE(d.effective_to, '9999-12-31')
ORDER BY f.order_id;
```

**Expected output:**

| order_id | customer_id | order_date | amount | tier_at_time_of_order |
|---|---|---|---|---|
| 1001 | 101 | 2023-03-15 | 200.00 | bronze |
| 1002 | 101 | 2023-08-20 | 350.00 | silver |
| 1003 | 101 | 2024-02-10 | 500.00 | gold |
| 1004 | 102 | 2023-05-01 | 180.00 | silver |
| 1005 | 102 | 2024-01-15 | 420.00 | gold |

### Why COALESCE(effective_to, '9999-12-31')

Current records have `effective_to = NULL`. NULL BETWEEN any range evaluates to NULL (falsy). Replace NULL with a far-future date so current records match any order_date up to that sentinel.

### Performance note

Range joins (`BETWEEN`) cannot use standard hash or merge join strategies — the engine falls back to nested loop or a range scan. At large scale:
- Partition both tables by customer_id
- Add an index on (customer_id, effective_from, effective_to)
- In columnar stores (Redshift, BigQuery), ensure the range columns are sort/cluster keys

---

## Problem 2: Funnel Analysis

**Interview prompt:** "Calculate the conversion rate at each step of the purchase funnel: view → add_to_cart → purchase. Show the count and drop-off at each step."

### Setup

```sql
CREATE TABLE funnel_events (
    event_id   INT,
    user_id    INT,
    event_type VARCHAR(30),  -- 'view', 'add_to_cart', 'purchase'
    event_time TIMESTAMP
);

INSERT INTO funnel_events VALUES
(1,  1, 'view',        '2024-01-01 10:00'),
(2,  1, 'add_to_cart', '2024-01-01 10:05'),
(3,  1, 'purchase',    '2024-01-01 10:12'),
(4,  2, 'view',        '2024-01-01 11:00'),
(5,  2, 'add_to_cart', '2024-01-01 11:30'),
(6,  3, 'view',        '2024-01-01 12:00'),
(7,  4, 'view',        '2024-01-01 13:00'),
(8,  4, 'purchase',    '2024-01-01 13:45'),  -- skipped add_to_cart
(9,  5, 'view',        '2024-01-01 14:00'),
(10, 5, 'add_to_cart', '2024-01-01 14:10');
```

### Solution 1: Conditional COUNT DISTINCT (simplest)

```sql
WITH funnel AS (
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'view'        THEN user_id END) AS viewers,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS adders,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'    THEN user_id END) AS purchasers
    FROM funnel_events
)
SELECT
    viewers,
    adders,
    purchasers,
    ROUND(100.0 * adders     / NULLIF(viewers,  0), 1) AS view_to_cart_pct,
    ROUND(100.0 * purchasers / NULLIF(adders,   0), 1) AS cart_to_purchase_pct,
    ROUND(100.0 * purchasers / NULLIF(viewers,  0), 1) AS overall_conversion_pct
FROM funnel;
```

### Solution 2: Step ordering verification (strict funnel)

In a strict funnel, step N must happen BEFORE step N+1. User 4 above jumped straight to purchase — decide whether to count them.

```sql
WITH user_steps AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view'        THEN event_time END) AS viewed_at,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN event_time END) AS carted_at,
        MAX(CASE WHEN event_type = 'purchase'    THEN event_time END) AS purchased_at
    FROM funnel_events
    GROUP BY user_id
)
SELECT
    COUNT(*)                                                    AS total_users,
    COUNT(viewed_at)                                           AS step1_view,
    COUNT(CASE WHEN carted_at > viewed_at THEN 1 END)         AS step2_cart_after_view,
    COUNT(CASE WHEN purchased_at > carted_at THEN 1 END)      AS step3_purchase_after_cart
FROM user_steps;
```

### Funnel design trade-off

| Approach | Counts user 4 (view→purchase, skip cart) | Use when |
|---|---|---|
| COUNT DISTINCT per step | Yes — as both viewer and purchaser | Marketing attribution, measuring reach |
| Ordered step verification | No — doesn't meet strict sequence | Product funnel, checkout flow analysis |

---

## Problem 3: Last-Touch Attribution

**Interview prompt:** "Give 100% of purchase credit to the last marketing touchpoint each user had before buying. Show revenue per channel."

### Setup

```sql
CREATE TABLE marketing_touches (
    touch_id   INT,
    user_id    INT,
    channel    VARCHAR(50),
    touch_time TIMESTAMP
);

CREATE TABLE purchases (
    purchase_id INT,
    user_id     INT,
    amount      DECIMAL(10,2),
    purchase_time TIMESTAMP
);

INSERT INTO marketing_touches VALUES
(1, 1, 'email',    '2024-01-10 09:00'),
(2, 1, 'social',   '2024-01-12 14:00'),
(3, 1, 'email',    '2024-01-14 11:00'),  -- last touch before purchase
(4, 2, 'paid_search', '2024-01-08 10:00'),
(5, 2, 'email',    '2024-01-11 16:00'),  -- last touch before purchase
(6, 3, 'organic',  '2024-01-15 08:00');  -- last touch, but no purchase

INSERT INTO purchases VALUES
(101, 1, 299.00, '2024-01-15 10:00'),
(102, 2, 149.00, '2024-01-13 11:00');
```

### Solution

```sql
WITH ranked_touches AS (
    SELECT
        t.touch_id,
        t.user_id,
        t.channel,
        t.touch_time,
        p.purchase_id,
        p.amount,
        p.purchase_time,
        ROW_NUMBER() OVER (
            PARTITION BY t.user_id, p.purchase_id
            ORDER BY t.touch_time DESC
        ) AS rn
    FROM marketing_touches t
    INNER JOIN purchases p
        ON  t.user_id    = p.user_id
        AND t.touch_time < p.purchase_time  -- touch must be BEFORE purchase
),
last_touch AS (
    SELECT user_id, channel, purchase_id, amount
    FROM ranked_touches
    WHERE rn = 1
)
SELECT
    channel,
    COUNT(*)       AS attributed_purchases,
    SUM(amount)    AS attributed_revenue
FROM last_touch
GROUP BY channel
ORDER BY attributed_revenue DESC;
```

### Attribution model comparison

| Model | Description | SQL complexity |
|---|---|---|
| Last touch | 100% credit to last touchpoint | ROW_NUMBER + rn=1 |
| First touch | 100% credit to first touchpoint | ROW_NUMBER ASC + rn=1 |
| Linear | Equal credit split across all touches | 1/COUNT(*) per touch |
| Time decay | More credit to more recent touches | Exponential weight formula |
| Data-driven | ML model assigns weights | Cannot do in SQL alone |

---

## Problem 4: Time-Series Gap Filling

**Interview prompt:** "Show total revenue for every day in January 2024, including days with zero sales."

### The problem

```sql
-- This only returns days with actual sales — missing days are absent
SELECT sale_date, SUM(revenue) FROM sales GROUP BY sale_date;
```

### Solution: Date spine + LEFT JOIN

```sql
-- Step 1: Generate a date spine (every day in range)
WITH RECURSIVE date_spine AS (
    SELECT DATE '2024-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL '1 day'
    FROM date_spine
    WHERE d < DATE '2024-01-31'
),
-- Step 2: LEFT JOIN fact data to spine
daily_revenue AS (
    SELECT
        ds.d                        AS sale_date,
        COALESCE(SUM(s.revenue), 0) AS daily_revenue
    FROM date_spine ds
    LEFT JOIN sales s
        ON ds.d = s.sale_date
    GROUP BY ds.d
)
SELECT
    sale_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY sale_date) AS cumulative_revenue
FROM daily_revenue
ORDER BY sale_date;
```

### Alternative: calendar table (better for production)

```sql
-- A persistent calendar table beats recursive CTE for performance at scale
CREATE TABLE dim_date (
    d          DATE PRIMARY KEY,
    year_num   INT,
    month_num  INT,
    day_of_week VARCHAR(10),
    is_holiday BOOLEAN
    -- ... additional columns
);

-- Then the gap-fill query is simply:
SELECT
    c.d,
    COALESCE(SUM(s.revenue), 0) AS daily_revenue
FROM dim_date c
LEFT JOIN sales s ON c.d = s.sale_date
WHERE c.d BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY c.d
ORDER BY c.d;
```

### Database-native spine generation

| Database | Generate date series |
|---|---|
| PostgreSQL | `GENERATE_SERIES('2024-01-01'::date, '2024-01-31'::date, '1 day')` |
| BigQuery | `UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-01-31', INTERVAL 1 DAY))` |
| Snowflake | `GENERATOR(ROWCOUNT => 31)` + `DATEADD(day, SEQ4(), start_date)` |
| SQL Server | Recursive CTE or system table trick |

---

## Problem 5: Longest Consecutive Streak

**Interview prompt:** "Find each user's longest streak of consecutive login days."

### Solution

```sql
WITH user_activity AS (
    SELECT user_id, activity_date FROM user_activity  -- reuse from intermediate
),
numbered AS (
    SELECT
        user_id,
        activity_date,
        activity_date - INTERVAL '1 day' * ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY activity_date
        ) AS island_group
    FROM user_activity
),
streak_lengths AS (
    SELECT
        user_id,
        island_group,
        COUNT(*) AS streak_days
    FROM numbered
    GROUP BY user_id, island_group
),
max_streaks AS (
    SELECT
        user_id,
        MAX(streak_days) AS longest_streak
    FROM streak_lengths
    GROUP BY user_id
)
SELECT * FROM max_streaks ORDER BY longest_streak DESC;
```

This chains the Gaps & Islands technique from intermediate level with an additional MAX aggregation.

---

## ⚡ Quick Reference: Problem → Pattern Mapping

| Problem Type | SQL Pattern | Key Functions |
|---|---|---|
| Top-N per group | ROW_NUMBER() CTE + WHERE rn <= N | ROW_NUMBER, PARTITION BY |
| Running total | SUM OVER (ORDER BY ... ROWS ...) | SUM OVER, frame clause |
| YoY comparison | LAG(col, 12) or self-join on year-1 | LAG, NULLIF |
| Deduplication | ROW_NUMBER() CTE + DELETE/CTAS | ROW_NUMBER |
| Conditional agg | SUM/COUNT(CASE WHEN) | CASE WHEN, NULLIF |
| Gaps and Islands | date - ROW_NUMBER = constant | ROW_NUMBER, GROUP BY |
| Sessionization | Running SUM of session-start flags | LAG, SUM OVER |
| Median | PERCENTILE_CONT or middle-row AVG | PERCENTILE_CONT |
| Pivot | CASE WHEN per column | SUM(CASE WHEN) |
| Hierarchy / tree | WITH RECURSIVE + UNION ALL | RECURSIVE CTE |
| SCD2 point-in-time | Range join BETWEEN eff_from AND COALESCE(eff_to, '9999') | BETWEEN, COALESCE |
| Funnel | COUNT DISTINCT per step | COUNT DISTINCT, CASE WHEN |
| Attribution | ROW_NUMBER DESC + rn=1 per user/purchase | ROW_NUMBER |
| Date spine gap fill | Recursive CTE or GENERATE_SERIES + LEFT JOIN | LEFT JOIN, COALESCE |
| Longest streak | Gaps & Islands + MAX | ROW_NUMBER, MAX |

---

## What Interviewers Are Testing at Senior Level

> **Senior:** Do you understand dimensional modeling well enough to write a correct SCD2 join without prompting? Can you design a query for a complex analytical problem from first principles, not just recall a pattern?

Senior interviewers want to see:
1. SCD2 awareness — you identify the COALESCE(effective_to, sentinel) pattern without being told
2. Trade-off discussion — can you explain when a strict funnel vs. loose funnel is appropriate?
3. Scale awareness — you mention partition pruning / join strategy implications without being asked
4. Production instincts — you notice the division by zero risk and handle it proactively
5. Ability to extend: "How would you add multi-touch attribution?" demonstrates design thinking

## ⚡ Cheat Sheet

**SCD2 Point-in-Time Join (canonical senior problem)**
```sql
JOIN dim_customer_scd2 d
  ON f.customer_id = d.customer_id
 AND f.order_date BETWEEN d.effective_from
                      AND COALESCE(d.effective_to, '9999-12-31')
```
- `COALESCE(effective_to, '9999-12-31')`: NULL means current record; BETWEEN NULL = FALSE
- Wrong pattern: `WHERE effective_to IS NULL` gives only current tier, not historical

**Funnel Analysis Decision Rule**
- Loose funnel (reach): `COUNT(DISTINCT CASE WHEN event_type='step' THEN user_id END)`
- Strict funnel (ordered): compare timestamps `carted_at > viewed_at AND purchased_at > carted_at`
- Use strict when analyzing checkout flows; use loose for marketing reach measurement

**Attribution Models → SQL Pattern**
| Model | SQL |
|---|---|
| Last touch | `ROW_NUMBER() OVER (PARTITION BY user_id, purchase_id ORDER BY touch_time DESC)` = 1 |
| First touch | same but `ASC` |
| Linear | `1.0 / COUNT(*) OVER (PARTITION BY user_id, purchase_id)` as weight |

**Date Spine Gap Fill**
- PostgreSQL: `GENERATE_SERIES('2024-01-01'::date, '2024-01-31'::date, '1 day')`
- BigQuery: `UNNEST(GENERATE_DATE_ARRAY(...))`
- Snowflake: `GENERATOR(ROWCOUNT=>31)` + `DATEADD(day, SEQ4(), start_date)`
- Always `LEFT JOIN` date spine to fact; `COALESCE(SUM(rev), 0)`

**Gaps and Islands — Consecutive Streak**
```sql
activity_date - INTERVAL '1 day' * ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date)
  AS island_group
-- constant island_group = consecutive dates belong to same streak
```

**Senior-Level Instincts to Demonstrate**
- Spot `/ 0` risk → always wrap with `NULLIF(denominator, 0)`
- Range joins → mention index on `(customer_id, effective_from, effective_to)` and partition co-location
- Strict vs loose funnel → clarify with interviewer before coding
- After writing query → mention scale: "for 1B rows I'd ensure partition pruning on order_date"
