---
title: "LeetCode for Data Engineers - Senior Deep Dive"
topic: interview-prep
subtopic: leetcode-for-de
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [interview-prep, leetcode-for-de, leetcode, sql, python, senior, advanced]
---

# LeetCode for Data Engineers — Senior Deep Dive

## Advanced SQL Patterns: The Ones That Actually Separate Candidates

At the senior level, SQL problems go beyond ranking and aggregation. The questions that separate senior candidates involve recursive queries, complex multi-step logic, and the ability to reason about query execution plans.

---

## Pattern: Median and Percentile Calculation

Medians are notoriously tricky in SQL because there's no `MEDIAN()` function in standard SQL.

**LeetCode #569: Median Employee Salary**

```sql
-- Find the median salary for each company
-- Key insight: for a set of n values, the median position(s) are:
-- n is odd: position (n+1)/2
-- n is even: positions n/2 and n/2+1
WITH employee_ranked AS (
    SELECT
        company,
        salary,
        ROW_NUMBER() OVER (PARTITION BY company ORDER BY salary) AS rn,
        COUNT(*) OVER (PARTITION BY company) AS total
    FROM Employee
)
SELECT company, salary AS median
FROM employee_ranked
WHERE rn IN (FLOOR((total + 1) / 2.0), CEIL((total + 1) / 2.0));
```

**Why this is hard**: `FLOOR` and `CEIL` of the median position handle both odd and even counts. For odd counts, they produce the same value (middle element). For even counts, they produce two adjacent positions, and both rows are returned.

---

## Pattern: Trips and Users (LeetCode #262) — Cancellation Rate

This Hard problem requires multiple joins, date filtering, and conditional aggregation working together.

```sql
SELECT
    t.request_at AS Day,
    ROUND(
        SUM(CASE WHEN t.status != 'completed' THEN 1 ELSE 0 END) * 1.0
        / COUNT(*),
        2
    ) AS "Cancellation Rate"
FROM Trips t
JOIN Users u_client ON t.client_id = u_client.users_id AND u_client.banned = 'No'
JOIN Users u_driver ON t.driver_id = u_driver.users_id AND u_driver.banned = 'No'
WHERE t.request_at BETWEEN '2013-10-01' AND '2013-10-03'
GROUP BY t.request_at
ORDER BY t.request_at;
```

**What to explain**: "I'm joining Users twice — once for the client and once for the driver — and filtering for unbanned users in both joins. Putting the `banned = 'No'` filter in the JOIN condition (not WHERE) is equivalent here for INNER JOINs, but I'd use WHERE for clarity in production. The CASE-based conditional aggregation counts non-completed trips for the numerator."

---

## Pattern: Friend Requests Acceptance Rate (LeetCode #574)

```sql
-- Overall acceptance rate for friend requests (handle division by zero)
SELECT ROUND(
    IFNULL(
        (SELECT COUNT(DISTINCT requester_id, accepter_id) FROM RequestAccepted)
        / (SELECT COUNT(DISTINCT sender_id, send_to_id) FROM FriendRequest) * 1.0,
        0
    ),
    2
) AS accept_rate;
```

**Key technique**: `IFNULL(expression, 0)` handles the case where no requests were ever sent. The `DISTINCT` on both subqueries ensures duplicate requests and acceptances don't inflate the rate.

---

## Pattern: Running Total with Reset

A more complex running total that resets when a condition is met. This pattern appears in streak calculations and session-based aggregations.

```sql
-- Running sum that resets after each purchase event
-- (e.g., track cumulative views between purchases)
WITH events_ordered AS (
    SELECT
        user_id,
        event_type,
        event_date,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) OVER (
            PARTITION BY user_id
            ORDER BY event_date
        ) AS purchase_group  -- Increments at each purchase, stays same between
    FROM user_events
)
SELECT
    user_id,
    purchase_group,
    COUNT(*) FILTER (WHERE event_type = 'view') AS views_before_purchase
FROM events_ordered
GROUP BY user_id, purchase_group;
```

---

## Deep Python Patterns: When They Actually Matter for DE

### Heap-Based K-Way Merge

The K-way merge is the algorithm behind Spark's sort-merge join and Hadoop's map-reduce shuffle merge. It merges K sorted lists in O(n log K) time.

```python
import heapq

def k_way_merge(sorted_lists):
    """
    Merge K sorted lists into one sorted list.
    Time: O(n log K), Space: O(K).
    Directly analogous to how Spark's sort-merge join works.
    """
    result = []
    # Heap contains (value, list_index, element_index)
    heap = []

    # Initialize with the first element from each list
    for i, lst in enumerate(sorted_lists):
        if lst:
            heapq.heappush(heap, (lst[0], i, 0))

    while heap:
        val, list_idx, elem_idx = heapq.heappop(heap)
        result.append(val)

        # Push the next element from the same list, if available
        next_idx = elem_idx + 1
        if next_idx < len(sorted_lists[list_idx]):
            heapq.heappush(heap, (sorted_lists[list_idx][next_idx], list_idx, next_idx))

    return result

# Example: merging 3 sorted partitions (like Spark shuffle output)
partitions = [[1, 4, 7], [2, 5, 8], [3, 6, 9]]
print(k_way_merge(partitions))
# [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

---

### LRU Cache (LeetCode #146)

The LRU (Least Recently Used) cache appears in DE contexts for caching dimension table lookups in streaming pipelines.

```python
from collections import OrderedDict

class LRUCache:
    """
    O(1) get and put operations.
    OrderedDict maintains insertion order; move_to_end for access tracking.
    Relevant to: caching dimension lookups in Flink/Kafka Streams jobs.
    """
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: int) -> int:
        if key not in self.cache:
            return -1
        self.cache.move_to_end(key)  # Mark as recently used
        return self.cache[key]

    def put(self, key: int, value: int) -> None:
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)  # Evict least recently used

# DE use case: cache product dimension records during stream enrichment
# cache = LRUCache(10000)  # Cache up to 10K product records
# product = cache.get(product_id) or fetch_from_db(product_id)
```

---

## What to Skip and Exactly Why

Being precise about what to skip helps you spend prep time efficiently.

### Skip: Tree Traversal (LC #94, #102, #104, etc.)

Binary tree problems almost never appear in DE interviews. The reasoning: DEs work with tabular data, not pointer-based hierarchical data. The one exception is if you're interviewing for a role building query engines or execution planners — in those cases, tree traversal appears.

### Skip: Graph Algorithms (LC #200, #207, #743, etc.)

Island counting (BFS), topological sort (course schedule), and shortest path (Dijkstra) are SE interview staples. DE interviews rarely touch these. Exception: if you're interviewing for a data lineage or dependency graph role, you might need topological sort.

### Skip: Complex Dynamic Programming

Coin change variants, knapsack, longest palindromic subsequence — these are almost exclusively SWE interview territory. The only DP-adjacent problems worth knowing as a DE are:
- Maximum subarray (Kadane's algorithm) — occasionally appears as a "find the best time window" variant
- Longest increasing subsequence — rarely, in the context of CDC ordering problems

### Skip: Bit Manipulation

Bit operations have essentially no relevance to data engineering work. Skip entirely unless the job description specifically mentions systems-level work.

---

## The 30-Day Study Plan Before an Interview

This schedule assumes 1–2 hours per day and assumes you have a job interview in 30 days.

| Week | Focus | Specific Practice |
|---|---|---|
| Week 1 (Days 1–7) | SQL Easy complete | All 30 LeetCode SQL Easy problems |
| Week 2 (Days 8–14) | SQL Medium + StrataScratch | LeetCode SQL Medium (window functions, self-joins, recursive CTEs). 10 StrataScratch problems. |
| Week 3 (Days 15–21) | Python patterns | Hash map (LC #1, #49, #217, #242). Sorting (LC #56, #252). Two-pointer (LC #88, #21). Heap (LC #215, #347). Counter (LC #692, #451). |
| Week 4 (Days 22–30) | Review + mock interviews | Redo every problem you got wrong. Do 3 timed mock sessions (45 min each: 2 SQL + 1 Python). Practice explaining solutions aloud. |

**Daily practice routine:**
- Day's problems: 30–45 min of new problems
- Review: 15 min reviewing yesterday's problems cold (without hints)
- Narration practice: once per week, solve a problem while narrating your thinking aloud as if in an interview

---

## Resources Beyond LeetCode

| Resource | What It's Good For |
|---|---|
| StrataScratch | Real company SQL problems with business context |
| HackerRank SQL track | Structured SQL progression, good for building speed |
| DataLemur | SQL focused, interview-oriented with good explanations |
| Mode Analytics SQL Tutorial | Window functions tutorial, free and thorough |
| pgexercises.com | PostgreSQL-specific exercises, great for complex SQL |
| interviewing.io | Anonymous mock interviews with real engineers |
| Pramp | Free peer-to-peer mock interviews |

**Recommended sequence**: LeetCode SQL → DataLemur (for interview context) → StrataScratch (for realistic data). For Python, LeetCode Easy/Medium is sufficient with targeted pattern practice.
