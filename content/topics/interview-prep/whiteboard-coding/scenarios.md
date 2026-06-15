---
title: "Whiteboard Coding - Scenario Questions"
topic: interview-prep
subtopic: whiteboard-coding
content_type: scenario_question
tags: [interview-prep, whiteboard-coding, sql, python, scenarios]
---

# Whiteboard Coding — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Top-N Per Group SQL

**Scenario:** You are given an `orders` table with columns: `order_id`, `customer_id`, `region`, `order_date`, `amount`. Write a SQL query to find the top 3 customers by total spend in each region. Include all customers tied for 3rd place. Show `region`, `customer_id`, `total_spend`, and their rank within the region.

**Table:**
```
orders(order_id, customer_id, region, order_date, amount)
```

<details>
<summary>✅ Solution</summary>

```sql
WITH customer_spend AS (
    -- Step 1: Aggregate total spend per customer per region
    SELECT
        region,
        customer_id,
        SUM(amount) AS total_spend
    FROM orders
    GROUP BY region, customer_id
),
ranked AS (
    -- Step 2: Rank customers within each region by total spend
    SELECT
        region,
        customer_id,
        total_spend,
        DENSE_RANK() OVER (
            PARTITION BY region
            ORDER BY total_spend DESC
        ) AS spend_rank
    FROM customer_spend
)
-- Step 3: Filter to top 3 ranks (DENSE_RANK handles ties)
SELECT
    region,
    customer_id,
    total_spend,
    spend_rank
FROM ranked
WHERE spend_rank <= 3
ORDER BY region, spend_rank, customer_id;
```

**Why `DENSE_RANK` and not `ROW_NUMBER` or `RANK`?**

- `ROW_NUMBER`: Assigns unique numbers even for ties. If two customers have the same spend, one arbitrarily gets rank 2 and the other rank 3. The problem says "include all tied for 3rd" — this would fail.
- `RANK`: Skips numbers after ties. If two customers tie for 2nd, the next rank is 4. A customer who should be 3rd gets rank 4 and would be excluded by `WHERE spend_rank <= 3`.
- `DENSE_RANK`: No gaps. Two customers tied for 2nd both get rank 2, and the next customer gets rank 3. This correctly includes all tied customers.

**Edge cases to mention:**
- NULLs in `amount`: `SUM` ignores NULLs by default. If you want NULLs treated as 0, use `SUM(COALESCE(amount, 0))`.
- Customers with zero purchases: They won't appear because they're not in `orders`. If you need to include them with 0 spend, LEFT JOIN from a `customers` table.
- Region with fewer than 3 customers: `WHERE spend_rank <= 3` will simply return however many exist — no special handling needed.

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Implement a Python ETL with Deduplication and Aggregation

**Scenario:** You receive a list of raw event dictionaries from an API. Events can arrive duplicated (same `event_id` appearing multiple times). Each event has: `event_id`, `user_id`, `event_type`, `amount` (optional, only for purchase events), `timestamp`. Write a Python function that:
1. Deduplicates events by `event_id` (keep the one with the latest timestamp)
2. For each user, compute: total purchase amount, count of purchase events, count of non-purchase events
3. Return a list of user summary dicts sorted by total purchase amount descending

```python
# Input example
events = [
    {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-01T10:00:00"},
    {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-01T10:00:01"},  # duplicate
    {"event_id": "e2", "user_id": "u1", "event_type": "view", "amount": None, "timestamp": "2024-01-01T09:00:00"},
    {"event_id": "e3", "user_id": "u2", "event_type": "purchase", "amount": 250.0, "timestamp": "2024-01-01T11:00:00"},
]
```

<details>
<summary>✅ Solution</summary>

```python
from collections import defaultdict

def process_events(events: list[dict]) -> list[dict]:
    """
    Deduplicate events by event_id (keep latest timestamp),
    then aggregate per-user stats.
    """

    # Step 1: Deduplicate — keep the event with the latest timestamp per event_id
    deduped = {}
    for event in events:
        eid = event["event_id"]
        if eid not in deduped or event["timestamp"] > deduped[eid]["timestamp"]:
            deduped[eid] = event

    # Step 2: Aggregate per user
    user_stats = defaultdict(lambda: {
        "total_purchase_amount": 0.0,
        "purchase_count": 0,
        "non_purchase_count": 0,
    })

    for event in deduped.values():
        uid = event["user_id"]
        if event["event_type"] == "purchase":
            # Use 0 if amount is None or missing — data quality issue
            amount = event.get("amount") or 0.0
            user_stats[uid]["total_purchase_amount"] += amount
            user_stats[uid]["purchase_count"] += 1
        else:
            user_stats[uid]["non_purchase_count"] += 1

    # Step 3: Build output list with user_id included, sort by total_purchase_amount desc
    result = []
    for user_id, stats in user_stats.items():
        result.append({
            "user_id": user_id,
            "total_purchase_amount": round(stats["total_purchase_amount"], 2),
            "purchase_count": stats["purchase_count"],
            "non_purchase_count": stats["non_purchase_count"],
        })

    return sorted(result, key=lambda x: x["total_purchase_amount"], reverse=True)


# Test
events = [
    {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-01T10:00:00"},
    {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-01T10:00:01"},
    {"event_id": "e2", "user_id": "u1", "event_type": "view", "amount": None, "timestamp": "2024-01-01T09:00:00"},
    {"event_id": "e3", "user_id": "u2", "event_type": "purchase", "amount": 250.0, "timestamp": "2024-01-01T11:00:00"},
]

output = process_events(events)
print(output)
# [{'user_id': 'u2', 'total_purchase_amount': 250.0, 'purchase_count': 1, 'non_purchase_count': 0},
#  {'user_id': 'u1', 'total_purchase_amount': 100.0, 'purchase_count': 1, 'non_purchase_count': 1}]
```

**Complexity analysis:**
- Deduplication pass: O(n) — single pass through events with dict lookup
- Aggregation pass: O(n) — single pass through deduped events
- Sort: O(u log u) where u = unique users
- Total: O(n + u log u) — linear in events, n log n in worst case

**Edge cases mentioned:**
- Duplicate `event_id` with different timestamps: keep latest (by string comparison — ISO8601 strings sort correctly lexicographically)
- `amount = None` for purchase events: treated as 0.0 with `or 0.0`; worth flagging to the interviewer as a data quality issue
- User with only non-purchase events: appears in results with `total_purchase_amount = 0.0`

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Gaps and Islands — Find Login Streaks with Business Logic

**Scenario:** The `user_logins` table has `user_id` and `login_date` (one row per user per day they logged in, no duplicates). Write a SQL query to find, for each user, their longest consecutive login streak in 2024, and classify them as:
- `CHAMPION`: streak ≥ 30 days
- `ENGAGED`: streak 7–29 days
- `CASUAL`: streak < 7 days

Break ties by taking the most recent streak if a user has multiple streaks of equal maximum length. Show `user_id`, `longest_streak`, `streak_start`, `streak_end`, and `engagement_tier`.

```
user_logins(user_id VARCHAR, login_date DATE)
```

<details>
<summary>✅ Solution</summary>

```sql
WITH
-- Step 1: Filter to 2024 only
logins_2024 AS (
    SELECT user_id, login_date
    FROM user_logins
    WHERE login_date BETWEEN '2024-01-01' AND '2024-12-31'
),

-- Step 2: Assign row number per user (ordered by date)
-- Subtract row_number from date to get an "island anchor"
-- Consecutive dates produce the same anchor value
numbered AS (
    SELECT
        user_id,
        login_date,
        login_date - (ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY login_date
        ) * INTERVAL '1 day') AS island_anchor
    FROM logins_2024
),

-- Step 3: Collapse each island into its start date, end date, and length
streaks AS (
    SELECT
        user_id,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end,
        COUNT(*) AS streak_length
    FROM numbered
    GROUP BY user_id, island_anchor
),

-- Step 4: For each user, find their maximum streak length
user_max AS (
    SELECT
        user_id,
        MAX(streak_length) AS longest_streak
    FROM streaks
    GROUP BY user_id
),

-- Step 5: Join back to get the actual streak record(s) matching the max
-- Use ROW_NUMBER to break ties: pick most recent (largest streak_start)
ranked_streaks AS (
    SELECT
        s.user_id,
        s.streak_start,
        s.streak_end,
        s.streak_length AS longest_streak,
        ROW_NUMBER() OVER (
            PARTITION BY s.user_id
            ORDER BY s.streak_start DESC  -- Most recent streak wins ties
        ) AS tie_breaker
    FROM streaks s
    JOIN user_max um
        ON s.user_id = um.user_id
        AND s.streak_length = um.longest_streak
)

-- Step 6: Final output with engagement classification
SELECT
    user_id,
    longest_streak,
    streak_start,
    streak_end,
    CASE
        WHEN longest_streak >= 30 THEN 'CHAMPION'
        WHEN longest_streak >= 7  THEN 'ENGAGED'
        ELSE 'CASUAL'
    END AS engagement_tier
FROM ranked_streaks
WHERE tie_breaker = 1
ORDER BY longest_streak DESC, user_id;
```

**Step-by-step reasoning to narrate:**

1. "The gaps-and-islands trick: subtracting a row number (converted to an interval) from a date gives the same anchor for consecutive dates. Non-consecutive dates produce different anchors."

2. "After collapsing islands, I have one row per streak per user. Now I need the longest one per user — standard `MAX` in a CTE."

3. "The tie-breaking requirement ('most recent streak') requires joining back to the streaks table and using `ROW_NUMBER` ordered by `streak_start DESC` to pick the latest among equal-length streaks."

4. "The engagement tier classification is a simple CASE statement — I'd consider moving this logic to the application layer or a dbt model in production, so it can be updated without changing the query."

**Performance considerations:**
- `ROW_NUMBER()` and `MIN/MAX ... OVER (PARTITION BY)` both require sorting by user_id. On a large table, ensure `user_id, login_date` is indexed.
- The self-join in step 5 could be replaced by `QUALIFY` (Snowflake/BigQuery) or `FILTER` to avoid materializing intermediate results.
- For truly large datasets (billions of rows), consider pre-partitioning the table by year to avoid scanning all historical data for a 2024 filter.

**Snowflake / BigQuery simplification using QUALIFY:**
```sql
-- Steps 4 and 5 combined using QUALIFY (Snowflake/BigQuery)
SELECT user_id, streak_start, streak_end, streak_length AS longest_streak,
    CASE WHEN streak_length >= 30 THEN 'CHAMPION'
         WHEN streak_length >= 7  THEN 'ENGAGED'
         ELSE 'CASUAL'
    END AS engagement_tier
FROM streaks
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id
    ORDER BY streak_length DESC, streak_start DESC
) = 1
ORDER BY longest_streak DESC;
```

</details>

</article>
