---
title: "SQL Subqueries - Real-World Production Examples"
topic: sql
subtopic: subqueries
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, subqueries, production, analytics, etl, anti-join, lateral]
---

# SQL Subqueries — Real-World Production Examples

## Scenario 1: Customer Segmentation with Anti-Joins

**Business context:** The growth team wants to run a re-engagement email campaign targeting users who signed up in 2023 but haven't made a purchase in the last 90 days AND have never been sent a promotional email before. This requires three separate anti-join conditions — a classic use case for `NOT EXISTS`.

```sql
-- Three anti-join conditions using NOT EXISTS:
-- (1) Never purchased in the last 90 days
-- (2) Never received a promotional email
-- (3) Not already in the "do not contact" suppression list

SELECT 
    u.user_id,
    u.email,
    u.name,
    u.signup_date,
    COALESCE(last_purchase.last_purchase_date, 'Never') AS last_purchase_date,
    u.acquisition_channel
FROM users u
-- Condition 1: Signed up in 2023
WHERE u.signup_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND u.is_email_verified = TRUE

-- Condition 2: No purchase in last 90 days (anti-join)
  AND NOT EXISTS (
      SELECT 1 FROM orders o 
      WHERE o.user_id = u.user_id 
        AND o.order_date >= CURRENT_DATE - INTERVAL '90 days'
        AND o.status NOT IN ('cancelled', 'refunded')
  )

-- Condition 3: Never received a promo email (anti-join)
  AND NOT EXISTS (
      SELECT 1 FROM email_sends es
      WHERE es.user_id = u.user_id
        AND es.email_type = 'promotional'
  )

-- Condition 4: Not on suppression list (anti-join)
  AND NOT EXISTS (
      SELECT 1 FROM email_suppression sup
      WHERE sup.email = u.email
  )

-- Enrich with last purchase info (scalar correlated subquery — acceptable here, runs once per qualifying row)
LEFT JOIN LATERAL (
    SELECT MAX(order_date) AS last_purchase_date
    FROM orders WHERE user_id = u.user_id
) last_purchase ON TRUE

ORDER BY u.signup_date ASC;
```

**Why NOT EXISTS over NOT IN:**
- `email_suppression.email` might contain NULLs (incomplete data) — `NOT IN` would return zero rows
- `NOT EXISTS` stops at the first match per user — faster for large suppression lists
- Each condition is independently readable and debuggable

**Performance note:** With proper indexes on `orders(user_id, order_date)`, `email_sends(user_id, email_type)`, and `email_suppression(email)`, this query runs in under 5 seconds on 10 million users.

---

## Scenario 2: Product Catalog Price Anomaly Detection

**Business context:** The merchandising team reports that some products have prices that are statistical outliers — either suspiciously low (possible data entry error) or suspiciously high (possible pricing error). You need to find all products where the price deviates more than 2 standard deviations from their subcategory average.

```sql
-- Schema:
-- products(product_id, name, subcategory_id, price, is_active)
-- subcategories(subcategory_id, subcategory_name, category_id)

WITH subcategory_stats AS (
    -- Compute mean and stddev per subcategory (minimum 5 products for statistical relevance)
    SELECT 
        p.subcategory_id,
        sc.subcategory_name,
        COUNT(*)                AS product_count,
        AVG(p.price)            AS avg_price,
        STDDEV_POP(p.price)     AS stddev_price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY p.price) AS q1_price,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY p.price) AS q3_price
    FROM products p
    JOIN subcategories sc ON p.subcategory_id = sc.subcategory_id
    WHERE p.is_active = TRUE
    GROUP BY p.subcategory_id, sc.subcategory_name
    HAVING COUNT(*) >= 5  -- Need at least 5 products for meaningful stats
)
SELECT 
    p.product_id,
    p.name                                          AS product_name,
    p.price                                         AS current_price,
    ss.avg_price                                    AS subcategory_avg,
    ss.stddev_price                                 AS subcategory_stddev,
    ROUND((p.price - ss.avg_price) / ss.stddev_price, 2) AS z_score,
    -- IQR-based flag (more robust to extreme outliers)
    CASE 
        WHEN p.price < ss.q1_price - 1.5 * (ss.q3_price - ss.q1_price) THEN 'POSSIBLE_UNDERPRICED'
        WHEN p.price > ss.q3_price + 1.5 * (ss.q3_price - ss.q1_price) THEN 'POSSIBLE_OVERPRICED'
        ELSE 'NORMAL'
    END                                             AS iqr_flag,
    ss.subcategory_name,
    ss.product_count                                AS subcategory_product_count
FROM products p
JOIN subcategory_stats ss ON p.subcategory_id = ss.subcategory_id
WHERE 
    -- 2-sigma outlier condition (using subquery for the threshold):
    ABS(p.price - ss.avg_price) > 2 * ss.stddev_price
    AND p.is_active = TRUE
    -- Exclude subcategories where stddev is 0 (all same price):
    AND ss.stddev_price > 0
ORDER BY ABS(p.price - ss.avg_price) / ss.stddev_price DESC;
```

**Sample result:**

| product_id | product_name | current_price | subcategory_avg | z_score | iqr_flag | subcategory_name |
|------------|------------|--------------|----------------|---------|---------|-----------------|
| 4421 | USB Cable 3m | 0.01 | 12.50 | -4.1 | POSSIBLE_UNDERPRICED | Cables |
| 8832 | Yoga Mat Pro | 899.99 | 45.00 | 2.3 | POSSIBLE_OVERPRICED | Fitness Accessories |

**What makes this production-quality:**
- The CTE pre-computes stats once, then the subquery filter (`WHERE ... > 2 * stddev`) uses pre-computed values rather than re-scanning the table
- `HAVING COUNT(*) >= 5` prevents division by near-zero stddev for subcategories with too few products
- Dual detection method (z-score + IQR) catches different types of outliers
- The merchandising team receives this as a daily Slack report, automatically generated by an Airflow DAG

---

## Scenario 3: LATERAL Join for Per-User Event Sequence Analysis

**Business context:** The product team wants to understand user activation patterns. For each user who signed up in the last month, find their first 5 events in order, and check whether they completed the "activation sequence" (account_created → email_verified → first_purchase → first_review). Users who miss any step should be flagged for a targeted nudge campaign.

```sql
-- Schema:
-- users(user_id, signup_date, email)
-- events(event_id, user_id, event_type, event_timestamp)
-- Activation sequence: account_created → email_verified → first_purchase → first_review

WITH recent_users AS (
    SELECT user_id, email, signup_date
    FROM users
    WHERE signup_date >= CURRENT_DATE - INTERVAL '30 days'
),

-- LATERAL: get first 5 events per user in order
user_first_events AS (
    SELECT 
        ru.user_id,
        ru.email,
        ru.signup_date,
        fe.event_type,
        fe.event_timestamp,
        fe.event_rank
    FROM recent_users ru
    LEFT JOIN LATERAL (
        SELECT 
            event_type,
            event_timestamp,
            ROW_NUMBER() OVER (ORDER BY event_timestamp) AS event_rank
        FROM events
        WHERE user_id = ru.user_id
        ORDER BY event_timestamp
        LIMIT 5
    ) fe ON TRUE
),

-- Pivot: check which activation steps each user has completed
activation_status AS (
    SELECT 
        user_id,
        email,
        signup_date,
        -- Check each required step
        BOOL_OR(event_type = 'account_created')  AS did_create_account,
        BOOL_OR(event_type = 'email_verified')   AS did_verify_email,
        BOOL_OR(event_type = 'first_purchase')   AS did_purchase,
        BOOL_OR(event_type = 'first_review')     AS did_review,
        MIN(CASE WHEN event_type = 'email_verified' THEN event_timestamp END) AS email_verified_at,
        MIN(CASE WHEN event_type = 'first_purchase' THEN event_timestamp END) AS first_purchase_at,
        -- Time from signup to first purchase (activation speed)
        MIN(CASE WHEN event_type = 'first_purchase' THEN 
            EXTRACT(EPOCH FROM (event_timestamp - signup_date)) / 3600.0 END) AS hours_to_first_purchase
    FROM user_first_events
    GROUP BY user_id, email, signup_date
),

-- Classify users by activation stage
classified AS (
    SELECT 
        *,
        CASE 
            WHEN did_review    THEN 'FULLY_ACTIVATED'
            WHEN did_purchase  THEN 'NEEDS_REVIEW_NUDGE'
            WHEN did_verify_email THEN 'NEEDS_PURCHASE_NUDGE'
            WHEN did_create_account THEN 'NEEDS_VERIFICATION_NUDGE'
            ELSE 'INACTIVE'
        END AS activation_stage
    FROM activation_status
)

SELECT 
    activation_stage,
    COUNT(*)                                                AS user_count,
    ROUND(AVG(hours_to_first_purchase), 1)                 AS avg_hours_to_purchase,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)    AS pct_of_cohort
FROM classified
GROUP BY activation_stage
ORDER BY 
    CASE activation_stage
        WHEN 'FULLY_ACTIVATED' THEN 1
        WHEN 'NEEDS_REVIEW_NUDGE' THEN 2
        WHEN 'NEEDS_PURCHASE_NUDGE' THEN 3
        WHEN 'NEEDS_VERIFICATION_NUDGE' THEN 4
        WHEN 'INACTIVE' THEN 5
    END;
```

**Sample output:**

| activation_stage | user_count | avg_hours_to_purchase | pct_of_cohort |
|-----------------|-----------|----------------------|--------------|
| FULLY_ACTIVATED | 1842 | 6.3 | 28.1% |
| NEEDS_REVIEW_NUDGE | 2103 | 18.7 | 32.1% |
| NEEDS_PURCHASE_NUDGE | 1456 | NULL | 22.2% |
| NEEDS_VERIFICATION_NUDGE | 812 | NULL | 12.4% |
| INACTIVE | 340 | NULL | 5.2% |

**Production integration:**
- `user_id` list from each `activation_stage` feeds directly into the marketing automation platform (Braze, Customer.io) via a nightly export
- The `LATERAL` join makes this query elegant — without it, you'd need 5 separate correlated subqueries in SELECT (one per event type check) or a complex PIVOT
- `BOOL_OR` aggregation elegantly handles the "has user ever done this" check without DISTINCT COUNT

---

## Interview Tips

> **Tip 1:** "How do you structure complex eligibility queries with many conditions?" — "I use NOT EXISTS for each exclusion condition — they're composable, NULL-safe, and clearly communicate the intent (exclude users who match this pattern). Each NOT EXISTS clause is independently testable by running the inner query to see how many rows it returns. I avoid NOT IN because of NULL semantics and because large subquery result sets cause memory pressure."

> **Tip 2:** "When would you use LATERAL over a window function?" — "LATERAL is preferable when you need to apply a LIMIT per group (get top N per group), when the subquery needs to reference the outer row in a complex way (e.g., calling a function that returns a table), or when you want to reuse a derived value (like a computed date) across multiple columns. Window functions are simpler for ranking and aggregation without LIMIT. The practical rule: if the subquery needs LIMIT, use LATERAL; if it needs aggregation or ranking, use a window function."

> **Tip 3:** "How do you efficiently run aggregation subqueries at scale?" — "Pre-aggregate in a CTE and join — never put aggregation subqueries in the SELECT list or WHERE clause as correlated subqueries. For extremely large tables (10B+ rows), I push aggregations into a pre-computed summary table refreshed on a schedule, and query that instead. The goal is to ensure each base table is scanned at most once, regardless of how many aggregate values the query needs."
