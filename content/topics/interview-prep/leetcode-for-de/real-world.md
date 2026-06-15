---
title: "LeetCode for Data Engineers - Real-World Patterns"
topic: interview-prep
subtopic: leetcode-for-de
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [interview-prep, leetcode-for-de, leetcode, sql, python, real-world, production]
---

# LeetCode for Data Engineers — Real-World Patterns

## How LeetCode Patterns Map to Production DE Work

One of the most useful reframes for LeetCode practice is connecting each algorithmic pattern to something you'll actually build or encounter as a data engineer. When you understand why a pattern matters in production, the practice sessions have more context — and the solutions stick better.

---

## Hash Maps in Production: More Than Deduplication

The hash map pattern from LeetCode #1 (Two Sum) and #49 (Group Anagrams) is the algorithmic foundation of several critical DE operations.

### Broadcast Joins in Distributed Systems

When Spark executes a JOIN between a large table (fact) and a small table (dimension), it can use a **broadcast hash join**: it sends (broadcasts) the small table to every executor as a hash map, then each executor performs O(1) lookups for each row of the large table.

```python
# What Spark's broadcast join does internally (conceptually)
# The small "dim_products" table is hashed into memory on each executor
dim_map = {row["product_id"]: row for row in dim_products}  # Hash map

# Then each row of facts is enriched with an O(1) lookup
enriched = [
    {**fact_row, **dim_map.get(fact_row["product_id"], {})}
    for fact_row in large_fact_table
]
```

Understanding this is why interviewers ask hash map problems. It's not about the algorithm per se — it's about whether you understand how joins work internally and when they're efficient.

### Dimension Table Caching in Streaming

In a Flink or Kafka Streams job enriching events with dimension data, you can't query the database on every event (too slow). Instead, you maintain an in-memory hash map cache of the dimension table, refreshed periodically.

```python
import time
from functools import lru_cache

class DimensionCache:
    def __init__(self, db_fetch_fn, ttl_seconds=300):
        self._cache = {}
        self._db_fetch = db_fetch_fn
        self._ttl = ttl_seconds
        self._last_refresh = 0

    def get(self, key):
        now = time.time()
        if now - self._last_refresh > self._ttl:
            self._cache = self._db_fetch()  # Full refresh every 5 min
            self._last_refresh = now
        return self._cache.get(key)
```

This is exactly the LRU cache pattern from LeetCode #146, applied to a real streaming pipeline.

---

## Sorting Patterns in Production

### Sort-Merge Join

The most common join strategy for large-to-large table joins in distributed systems is sort-merge join:
1. Sort both tables on the join key
2. Merge the two sorted streams in one linear pass

```python
def sort_merge_join(left, right, key):
    """
    Merge two sorted lists on a key. O(n + m) after sorting.
    This is how Spark's SortMergeJoin works.
    """
    left_sorted = sorted(left, key=lambda x: x[key])
    right_sorted = sorted(right, key=lambda x: x[key])

    result = []
    i, j = 0, 0

    while i < len(left_sorted) and j < len(right_sorted):
        lk = left_sorted[i][key]
        rk = right_sorted[j][key]

        if lk == rk:
            # Match: collect all matching rows from right
            rj = j
            while rj < len(right_sorted) and right_sorted[rj][key] == lk:
                result.append({**left_sorted[i], **right_sorted[rj]})
                rj += 1
            i += 1
        elif lk < rk:
            i += 1  # Left has no match
        else:
            j += 1  # Right has no match

    return result
```

This is LeetCode #21 (Merge Two Sorted Lists) generalized. When you understand this, you understand why Spark adds a sort step before large joins and why the `spark.sql.join.preferSortMergeJoin` configuration exists.

---

## Interval Merging in Production

LeetCode #56 (Merge Intervals) maps directly to several real DE problems:

### Compacting CDC Events

In a CDC pipeline, you might receive multiple UPDATE events for the same row in a time window. Before writing to the warehouse, you can compact them by merging overlapping time windows of activity.

```python
def compact_row_events(events_by_key):
    """
    For each primary key, merge overlapping event windows
    and keep only the latest state. Reduces warehouse write volume.
    """
    result = {}
    for key, events in events_by_key.items():
        # Sort by start time
        sorted_events = sorted(events, key=lambda e: e["start_ts"])
        merged = [sorted_events[0].copy()]

        for event in sorted_events[1:]:
            last = merged[-1]
            if event["start_ts"] <= last["end_ts"]:
                # Overlapping — extend window and take latest state
                last["end_ts"] = max(last["end_ts"], event["end_ts"])
                last["state"] = event["state"]  # Latest state wins
            else:
                merged.append(event.copy())

        result[key] = merged
    return result
```

### Finding Data Coverage Gaps

Given a list of successfully loaded time windows, find the gaps — periods where data is missing.

```python
def find_data_gaps(loaded_windows, start, end):
    """
    Find time periods NOT covered by loaded_windows.
    Useful for identifying missing data in a backfill scenario.
    """
    sorted_windows = sorted(loaded_windows, key=lambda w: w[0])
    gaps = []
    current = start

    for window_start, window_end in sorted_windows:
        if current < window_start:
            gaps.append((current, window_start))
        current = max(current, window_end)

    if current < end:
        gaps.append((current, end))

    return gaps

# Example
loaded = [("2024-01-01", "2024-01-05"), ("2024-01-07", "2024-01-10")]
gaps = find_data_gaps(loaded, "2024-01-01", "2024-01-15")
# [("2024-01-05", "2024-01-07"), ("2024-01-10", "2024-01-15")]
```

---

## Top-K Patterns in Production

### Approximate Top-K in Streaming

In a real-time streaming pipeline, computing exact top-K requires tracking all items and their counts — expensive at scale. Production systems often use approximate data structures like **Count-Min Sketch** or **Space-Saving algorithm** to estimate top-K with bounded memory.

```python
from collections import Counter

class StreamingTopK:
    """
    Space-efficient approximate top-K tracker.
    In production, replace Counter with Count-Min Sketch for true streaming.
    This is the in-memory variant suitable for micro-batch windows.
    """
    def __init__(self, k):
        self.k = k
        self.counts = Counter()

    def observe(self, item):
        self.counts[item] += 1

    def top_k(self):
        return self.counts.most_common(self.k)

# Example: track top-K product views in a 1-minute tumbling window
tracker = StreamingTopK(k=10)
for event in window_events:
    if event["type"] == "view":
        tracker.observe(event["product_id"])

print("Top 10 products this minute:", tracker.top_k())
```

This maps to LeetCode #347 (Top K Frequent Elements), but contextualized for streaming.

---

## SQL LeetCode Patterns in Production

### The Cohort Retention Pattern (Derived from LeetCode #570/#571)

The consecutive activity / game play series on LeetCode reflects real product analytics queries:

```sql
-- D1 retention: users who return the day after their first login
-- This is a production analytics query at every consumer app company
WITH first_logins AS (
    SELECT user_id, MIN(login_date) AS first_login_date
    FROM user_logins
    GROUP BY user_id
)
SELECT
    f.first_login_date AS cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_size,
    COUNT(DISTINCT CASE
        WHEN l.login_date = f.first_login_date + INTERVAL '1 day'
        THEN l.user_id
    END) AS retained_d1,
    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN l.login_date = f.first_login_date + INTERVAL '1 day'
            THEN l.user_id
        END) / COUNT(DISTINCT f.user_id),
        1
    ) AS d1_retention_pct
FROM first_logins f
LEFT JOIN user_logins l ON f.user_id = l.user_id
GROUP BY f.first_login_date
ORDER BY cohort_date;
```

This is nearly identical to LeetCode #511–514 (Game Play Analysis series), just slightly more realistic.

---

## Recognizing Patterns in New Problems

The ability to pattern-match is what makes LeetCode practice useful for interviews. When you see a new problem, practice identifying which pattern it belongs to before writing any code:

| If the problem involves... | Pattern to use |
|---|---|
| Counting, grouping, looking up by key | Hash map / Counter |
| Finding top N items | Heap (heapq.nlargest) |
| Consecutive dates/IDs | Gaps and Islands (SQL) or sorting + comparison (Python) |
| Merging ranges, overlapping intervals | Sort + merge intervals |
| Session detection | Sliding window or gaps-and-islands |
| Deduplication with latest-wins logic | Hash map with timestamp comparison |
| Running total, period-over-period | Window function (SQL) or running state (Python) |
| Streaming aggregation over time windows | Sliding window + deque |

Practicing this recognition step — pattern identification before coding — is more valuable than memorizing solutions. Once you can label the pattern, the code follows from the template.
