---
title: "SQL Interview Patterns - Real-World"
topic: sql
subtopic: interview-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, interview-patterns, meta, airbnb, uber, amazon, stratascatch, interview-strategy]
---

# SQL Interview Patterns — Real-World

How do top companies actually test SQL? What question types come up most often? And how should you structure your answer when the clock is running?

---

## How Top Companies Test SQL

### Meta (Facebook) — Product Metrics and Funnels

Meta interviews almost always involve a product metric — DAU, WAU, retention, feature adoption — tied to a specific product like Feed, Messenger, or Instagram.

**Common Meta patterns:**
- "Calculate 7-day active users for the past 90 days" (rolling window + deduplication)
- "What % of users who liked a post also shared it within 24 hours?" (funnel with time constraint)
- "Show week-over-week change in stories posted per user" (LAG + cohort)
- "Find users who were active every day for 7 consecutive days" (gaps and islands)

**Meta SQL style**: They expect you to handle edge cases (new users have no prior week, NULL activity), comment each CTE step, and state your assumptions out loud before writing code.

```sql
-- Meta-style: 7-day rolling DAU with explicit boundary handling
SELECT
    activity_date,
    COUNT(DISTINCT user_id) AS dau,
    COUNT(DISTINCT user_id) OVER (
        ORDER BY activity_date
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
    ) AS rolling_7d_dau   -- RANGE-based window for exact 7-day calendar window
FROM daily_user_activity
ORDER BY activity_date;
```

---

### Airbnb — Cohort Analysis and Pricing

Airbnb interviews center on marketplace dynamics: host and guest behavior, pricing, booking funnel.

**Common Airbnb patterns:**
- "Calculate 30-day retention for new hosts who listed their first property" (cohort retention)
- "Find listings that have been booked fewer times than the median for their city and price range" (window function + percentile)
- "For each booking, find the previous booking by the same guest and calculate the gap in days" (LAG on timestamps)
- "Which city had the highest MoM growth in bookings last quarter?" (month-over-month with ranking)

**Airbnb SQL style**: They care heavily about data completeness — always LEFT JOIN to include hosts or guests with zero bookings, use `COALESCE` for zero-activity cases.

```sql
-- Airbnb-style: host retention 30 days after first listing
WITH first_listing AS (
    SELECT host_id, MIN(listed_date) AS first_listed
    FROM listings GROUP BY host_id
),
retained AS (
    SELECT f.host_id,
           MAX(CASE WHEN b.booking_date BETWEEN f.first_listed + 1
                                             AND f.first_listed + 30
                    THEN 1 ELSE 0 END) AS retained_30d
    FROM first_listing f
    LEFT JOIN bookings b ON f.host_id = b.host_id
    GROUP BY f.host_id
)
SELECT
    ROUND(100.0 * SUM(retained_30d) / COUNT(*), 1) AS day30_host_retention
FROM retained;
```

---

### Uber — Geospatial and Time-Based Analysis

Uber questions combine GPS coordinates, time windows, and supply/demand matching.

**Common Uber patterns:**
- "Calculate surge factor: ratio of ride requests to available drivers by 15-min bucket" (time bucketing + ratio)
- "Find drivers who completed > 5 trips but had at least 1 cancellation on the same day" (conditional count)
- "Identify the most common pickup-dropoff city pair by day of week" (multi-column grouping + ranking)

```sql
-- Uber-style: requests vs drivers ratio by 15-minute bucket
SELECT
    DATE_TRUNC('hour', event_ts) +
        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM event_ts) / 15) AS bucket,
    SUM(CASE WHEN event_type = 'request' THEN 1 ELSE 0 END)  AS requests,
    SUM(CASE WHEN event_type = 'available' THEN 1 ELSE 0 END) AS available_drivers,
    ROUND(
        SUM(CASE WHEN event_type = 'request' THEN 1.0 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN event_type = 'available' THEN 1 ELSE 0 END), 0),
        2
    ) AS demand_supply_ratio
FROM ride_events
GROUP BY 1
ORDER BY 1;
```

---

### Amazon — Ranking and Aggregation

Amazon (especially for DE roles) tests large-scale aggregation, ranking, and data modeling.

**Common Amazon patterns:**
- "Find the top 3 selling products by revenue for each category in each quarter" (top-N per group)
- "Calculate the average time between a user's first and second purchase" (LAG on ordered events per user)
- "Identify orders that were returned and the customer's total return rate" (anti-join + aggregation)

---

## StrataScratch Top 20 Patterns (Condensed)

These are the patterns that appear most on StrataScratch, LeetCode SQL, and real interviews:

| # | Pattern | Key Technique |
|---|---|---|
| 1 | Second highest salary per department | `DENSE_RANK() = 2` |
| 2 | Users active N consecutive days | Gaps & islands |
| 3 | Customers who never ordered | `NOT EXISTS` or `LEFT JOIN ... IS NULL` |
| 4 | Rolling 7-day average | `AVG() OVER (ROWS 6 PRECEDING)` |
| 5 | Month-over-month revenue change | `LAG()` on monthly rollup |
| 6 | Duplicate transactions within 10 minutes | Self-join on user_id + time window |
| 7 | Funnel step conversion rates | `MAX(step)` per user + ratio |
| 8 | Cohort week-1 retention | Cohort join on signup_week + 1 |
| 9 | Running total with reset by group | `SUM() OVER (PARTITION BY group)` |
| 10 | First and last event per session | `MIN/MAX` partitioned by session |
| 11 | Median without built-in function | `ROW_NUMBER() + AVG` trick |
| 12 | Products bought by ALL customers | `COUNT(DISTINCT customer) = total_customers` |
| 13 | Tree parent-child depth | Recursive CTE |
| 14 | Pivot months to columns | `SUM(CASE WHEN month = N)` |
| 15 | Attribution: first vs last touch | Touch number comparison |
| 16 | Sessionization | `LAG + cumulative SUM` |
| 17 | YoY comparison same period | Date offset + `LAG` or self-join |
| 18 | Market basket pairs | Self-join with `a.id < b.id` |
| 19 | Dedup: keep latest per key | `ROW_NUMBER() = 1` + `ORDER BY ts DESC` |
| 20 | Time-series gap fill | Date spine + `LAST_VALUE IGNORE NULLS` |

---

## How to Structure Your Answer in an Interview

### The 5-Step Framework

**Step 1 — Restate and clarify** (30 seconds)
"Let me make sure I understand: we want users who completed all three steps in order, within 7 days of signup, and you want the conversion rate as a percentage. Do we include users who haven't had a chance to complete step 3 yet?"

**Step 2 — Write sample data** (1 minute)
Sketch 4-5 rows that cover the interesting cases: normal user, user who skipped step 2, user who did steps out of order, user with NULL values.

**Step 3 — Write the query bottom-up, CTE by CTE** (3-5 minutes)
Name each CTE before you write the SQL: "First I'll get one row per user per step, then I'll compute the max step reached, then I'll join to get time-to-convert."

**Step 4 — Verify with your sample data** (1 minute)
Walk through your query mentally with your sample rows. Say the output out loud. "User 1 had signup at T+0, step 2 at T+2, step 3 at T+5 — that's within 7 days, they should be in the converted bucket."

**Step 5 — Optimize** (1 minute — only if asked)
"For a 500M row table, I'd add a partition filter on event_date before the CTE. I'd also consider a covering index on (user_id, event_type, event_ts) to avoid a full scan."

### Common Mistakes That Kill Interview Performance

- Writing all the SQL at once without explaining the approach first
- Forgetting `NULLIF` in denominators for percentages
- Using `NOT IN` without checking if the subquery can have NULLs
- Applying functions to partition columns in WHERE clauses
- Not handling the "user has no activity" edge case with LEFT JOIN

---

## Key Takeaways

- Meta tests rolling windows + funnels, Airbnb tests cohort analysis, Uber tests time bucketing, Amazon tests ranking + aggregation
- Know the top 20 StrataScratch patterns — most interview questions are direct variations
- Always use the 5-step framework: clarify → sample data → build CTEs → verify → optimize
- Thinking out loud is as important as getting the answer right — interviewers want to see your reasoning
- A correct slow query beats an incorrect fast one; get correctness first, then mention optimizations
