---
title: "Whiteboard Coding - Intermediate"
topic: interview-prep
subtopic: whiteboard-coding
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [interview-prep, whiteboard-coding, sql, python, intermediate]
---

# Whiteboard Coding — Intermediate

## Advanced SQL Patterns for DE Interviews

These are the SQL patterns that appear in mid-level DE interviews and separate strong candidates from average ones. Each pattern requires a specific technique that isn't obvious from basic SQL knowledge.

---

## Pattern 1: Gaps and Islands

The "gaps and islands" problem asks you to identify continuous sequences (islands) and breaks in those sequences (gaps) in time-series or sequential data.

**Classic problem**: Find continuous ranges of consecutive dates where a user was active.

```sql
-- Step 1: Assign a group number to each island
-- Subtract a row_number from the date — dates in the same island
-- will all produce the same "anchor date"
WITH ranked AS (
    SELECT
        user_id,
        active_date,
        active_date - INTERVAL '1 day' * ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY active_date
        ) AS island_anchor
    FROM user_activity
),
-- Step 2: Group by the anchor to find each island's extent
islands AS (
    SELECT
        user_id,
        MIN(active_date) AS island_start,
        MAX(active_date) AS island_end,
        COUNT(*) AS consecutive_days
    FROM ranked
    GROUP BY user_id, island_anchor
)
SELECT *
FROM islands
WHERE consecutive_days >= 7  -- e.g., find streaks of 7+ days
ORDER BY user_id, island_start;
```

**What to explain**: "The key insight is that if you subtract a sequential row number from a date, consecutive dates all produce the same value. So dates that are part of a continuous streak share the same anchor, and you can group by it to find the extent of each streak."

---

## Pattern 2: Session Windowing (Sessionization)

Sessionization groups user events into sessions with a timeout. A new session starts when the gap between consecutive events exceeds a threshold (typically 30 minutes).

```sql
WITH event_gaps AS (
    SELECT
        user_id,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time,
        -- Flag the start of a new session (gap > 30 min, or first event)
        CASE
            WHEN event_time - LAG(event_time) OVER (
                PARTITION BY user_id ORDER BY event_time
            ) > INTERVAL '30 minutes'
            OR LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
            THEN 1 ELSE 0
        END AS is_session_start
    FROM user_events
),
session_ids AS (
    SELECT
        user_id,
        event_time,
        -- Cumulative sum of session starts = session number per user
        SUM(is_session_start) OVER (
            PARTITION BY user_id ORDER BY event_time
        ) AS session_id
    FROM event_gaps
)
SELECT
    user_id,
    session_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end,
    COUNT(*) AS events_in_session,
    EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time)) / 60 AS session_duration_minutes
FROM session_ids
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

**Interview tip**: Walk through a 3-row example manually after writing this. It's a complex query and the interviewer will appreciate seeing you verify it mentally.

---

## Pattern 3: Running Totals and Moving Averages

```sql
-- 7-day moving average revenue
SELECT
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM daily_revenue_summary
ORDER BY order_date;
```

**Key distinction to mention**: `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` is a physical frame (counts 7 rows regardless of date gaps). `RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW` is a logical frame (handles missing dates correctly). For data with possible date gaps, `RANGE` is more correct.

---

## Pattern 4: Anti-Join (Users Who Did Not Do X)

```sql
-- Find users who registered but never made a purchase
-- Method 1: LEFT JOIN + IS NULL (most readable)
SELECT u.user_id, u.registered_at
FROM users u
LEFT JOIN purchases p ON u.user_id = p.user_id
WHERE p.user_id IS NULL;

-- Method 2: NOT EXISTS (often more readable for complex conditions)
SELECT u.user_id, u.registered_at
FROM users u
WHERE NOT EXISTS (
    SELECT 1 FROM purchases p WHERE p.user_id = u.user_id
);

-- Method 3: NOT IN (avoid for large datasets — NULL behavior is surprising)
-- NOT IN returns no rows if the subquery contains any NULL values
-- Avoid this in production
SELECT user_id FROM users
WHERE user_id NOT IN (SELECT user_id FROM purchases);  -- Dangerous if NULLs exist
```

Always mention the NULL gotcha with `NOT IN`. It's a classic interview trap.

---

## Python: Intermediate Data Manipulation Patterns

### Implementing a Simple ETL

```python
import json
from datetime import datetime
from collections import defaultdict

def run_etl(raw_events: list[dict]) -> list[dict]:
    """
    Simple ETL: read events, clean, aggregate by user and day,
    write to a list of summary records.
    """
    # Transform: clean and enrich
    cleaned = []
    for event in raw_events:
        # Skip invalid events
        if not event.get("user_id") or not event.get("amount"):
            continue
        if event["amount"] < 0:
            continue  # Reject negative amounts

        cleaned.append({
            "user_id": event["user_id"],
            "amount": float(event["amount"]),
            "event_date": event["timestamp"][:10],  # Extract YYYY-MM-DD
            "event_type": event.get("type", "unknown"),
        })

    # Aggregate: daily spend per user
    daily_totals = defaultdict(float)
    daily_counts = defaultdict(int)

    for event in cleaned:
        key = (event["user_id"], event["event_date"])
        daily_totals[key] += event["amount"]
        daily_counts[key] += 1

    # Load: produce output records
    output = []
    for (user_id, date), total in daily_totals.items():
        output.append({
            "user_id": user_id,
            "date": date,
            "total_spend": round(total, 2),
            "transaction_count": daily_counts[(user_id, date)],
            "processed_at": datetime.utcnow().isoformat(),
        })

    return sorted(output, key=lambda x: (x["user_id"], x["date"]))
```

**What to narrate**: "I'm handling NULLs and negatives in the transform step. In a real pipeline, I'd also log or count rejections for data quality monitoring. The aggregation uses a `defaultdict` to avoid explicit key initialization."

### Group-By Aggregation Without pandas

```python
from collections import defaultdict

def top_n_per_group(records, group_key, value_key, n=3):
    """Return top N records per group, ranked by value descending."""
    groups = defaultdict(list)
    for record in records:
        groups[record[group_key]].append(record)

    result = []
    for group_val, group_records in groups.items():
        sorted_records = sorted(group_records, key=lambda r: r[value_key], reverse=True)
        result.extend(sorted_records[:n])

    return result

# Example
orders = [
    {"region": "East", "customer": "A", "spend": 1000},
    {"region": "East", "customer": "B", "spend": 800},
    {"region": "East", "customer": "C", "spend": 900},
    {"region": "West", "customer": "D", "spend": 1200},
    {"region": "West", "customer": "E", "spend": 500},
]
print(top_n_per_group(orders, "region", "spend", n=2))
```

---

## Handling Being Stuck

Getting stuck in a live coding interview is normal and expected. What matters is how you respond.

### Step 1: Ask a clarifying question

Often you're stuck because you're trying to solve the wrong version of the problem. Ask: "Can I assume the input is sorted?" or "Is it possible to have the same user appear in multiple regions?"

### Step 2: Start with brute force, explicitly

Say: "I know there's a more efficient way, but let me start with the O(n²) approach first to make sure I understand the problem, and then I'll optimize."

A working brute-force solution is always better than an incomplete "optimal" one. Interviewers respect candidates who solve the problem and then reflect on how to improve it.

### Step 3: Think out loud about the data structure

"I need to look up records by user_id quickly — that suggests a hash map (dict). Let me think about what I'd store as the key and value..."

### Step 4: Ask for a hint gracefully

"I'm getting a bit stuck on the session detection piece. Is there a particular data structure you'd recommend, or should I keep going with my current approach?"

Most interviewers will give you a hint if you ask genuinely. Asking for a hint is not a failure — it's a signal that you're self-aware and collaborative.

---

## Companies That Do Live Coding vs Take-Home

| Format | Common At |
|---|---|
| Live SQL (shared SQL editor) | Meta, LinkedIn, Airbnb, Lyft |
| Live Python (CoderPad) | Stripe, Databricks, Snowflake |
| Whiteboard (in-person) | Older enterprise companies, consulting firms |
| Take-home SQL/Python project | Many startups, mid-size tech |
| HackerRank pre-screen | Amazon, large tech for initial screening |

Research the company's interview format before your interview. LinkedIn Glassdoor reviews and the Blind app often have specifics. For take-homes, you have more time, so expect higher polish — handle edge cases, add comments, and sometimes a brief write-up explaining your approach.
