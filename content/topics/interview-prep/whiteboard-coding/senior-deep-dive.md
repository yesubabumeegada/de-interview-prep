---
title: "Whiteboard Coding - Senior Deep Dive"
topic: interview-prep
subtopic: whiteboard-coding
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [interview-prep, whiteboard-coding, sql, python, senior, advanced]
---

# Whiteboard Coding — Senior Deep Dive

## What Senior DE Live Coding Interviews Expect

At the senior level, live coding interviews expand beyond "write a query that works" to "write a query that's correct, efficient, handles edge cases, and is readable for a team." The interviewer may also ask you to explain query execution, estimate complexity, or refactor an intentionally bad solution.

Senior candidates should be able to:
- Write advanced SQL without hesitation or syntax lookup
- Reason about query performance (index usage, sort operations, partition pruning)
- Implement clean Python solutions with proper error handling
- Refactor code on demand and explain trade-offs between approaches
- Spot subtle bugs in given code (NULL handling, off-by-one, type mismatch)

---

## Advanced SQL: Query Optimization Reasoning

When asked to optimize a slow query, follow a consistent framework:

1. **Identify what the query is doing**: scan, filter, join, sort, aggregate
2. **Estimate the intermediate result sizes**: how many rows does each step produce?
3. **Find the expensive operations**: large table scans, sorts on large datasets, non-selective joins
4. **Propose targeted optimizations**: predicate pushdown, index hints, query restructure

### Example: Optimizing a Slow Aggregation

**Given (slow):**
```sql
SELECT
    u.region,
    COUNT(DISTINCT o.user_id) AS active_users,
    SUM(o.amount) AS total_revenue
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.order_date >= '2024-01-01'
GROUP BY u.region;
```

**Problems to identify:**
- `COUNT(DISTINCT)` is expensive — requires a sort or hash aggregate
- The JOIN happens before filtering if the planner doesn't push predicates
- If `users` is large and unindexed on `user_id`, the join is a full scan

**Optimized:**
```sql
-- Pre-filter and aggregate orders before joining to reduce join cardinality
WITH recent_orders AS (
    SELECT
        user_id,
        SUM(amount) AS user_revenue
    FROM orders
    WHERE order_date >= '2024-01-01'  -- predicate pushed to the CTE
    GROUP BY user_id
)
SELECT
    u.region,
    COUNT(*) AS active_users,        -- COUNT(*) on pre-grouped data, no DISTINCT needed
    SUM(ro.user_revenue) AS total_revenue
FROM recent_orders ro
JOIN users u ON ro.user_id = u.user_id
GROUP BY u.region;
```

**Explain**: "By pre-aggregating orders to one row per user before the join, the join touches far fewer rows. The `COUNT(DISTINCT user_id)` becomes `COUNT(*)` on the already-deduplicated CTE, which is much cheaper. The WHERE clause filters happen before the join."

---

## Advanced SQL: The Self-Join Pattern

Self-joins solve problems that involve comparing rows within the same table.

**Problem**: Find pairs of products that were purchased together by the same customer in the same order.

```sql
-- Find product pairs purchased together
SELECT
    oi1.product_id AS product_a,
    oi2.product_id AS product_b,
    COUNT(DISTINCT oi1.order_id) AS co_occurrence_count
FROM order_items oi1
JOIN order_items oi2
    ON oi1.order_id = oi2.order_id
    AND oi1.product_id < oi2.product_id  -- Avoid (A,B) and (B,A) duplicates
GROUP BY oi1.product_id, oi2.product_id
HAVING COUNT(DISTINCT oi1.order_id) >= 10  -- Filter for meaningful pairs
ORDER BY co_occurrence_count DESC;
```

**What to call out**: "The `oi1.product_id < oi2.product_id` condition is crucial — without it, we'd get each pair twice (A,B and B,A) plus each product paired with itself. This is a pattern you see in affinity analysis and recommendation systems."

---

## Advanced SQL: Recursive CTEs

Recursive CTEs handle hierarchical data. They appear in interviews for roles involving organizational hierarchies, bill-of-materials, or graph-like data.

**Problem**: Given an employee table with `employee_id` and `manager_id`, find all employees under a given manager (at any depth).

```sql
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: the root manager
    SELECT employee_id, name, manager_id, 1 AS level
    FROM employees
    WHERE employee_id = 101  -- Start from this manager

    UNION ALL

    -- Recursive case: employees who report to someone already in the hierarchy
    SELECT e.employee_id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT *
FROM employee_hierarchy
ORDER BY level, name;
```

**Pitfall to mention**: "Recursive CTEs can infinite-loop if the data has cycles (employee A reports to B who reports to A). In production, I'd add a depth limit or cycle detection: `WHERE eh.level < 10` as a safety guard."

---

## Python: Time Complexity for Data Operations

Senior DE candidates should be able to reason about time complexity for common data operations. You won't be asked to prove Big-O formally, but you should be able to say "this is O(n log n) because it sorts" without hesitation.

| Operation | Time Complexity | Why |
|---|---|---|
| Dict lookup | O(1) average | Hash table |
| List append | O(1) amortized | Dynamic array |
| List sort | O(n log n) | Timsort |
| `in` on a list | O(n) | Linear scan |
| `in` on a set | O(1) average | Hash table |
| Group-by with defaultdict | O(n) | Single pass |
| Top-K with heapq | O(n log K) | Min-heap of size K |

**Interview application**: When you write `if element in large_list`, say "this is O(n) — if performance matters, I'd convert `large_list` to a set first for O(1) lookups, at the cost of O(n) extra memory."

### Efficient Top-K Using a Heap

```python
import heapq

def top_k_products_by_revenue(sales_records, k):
    """
    Find top-K products by total revenue.
    Time: O(n log k) — better than O(n log n) sort when k << n.
    """
    # First pass: aggregate revenue per product
    revenue = {}
    for record in sales_records:
        product = record["product_id"]
        revenue[product] = revenue.get(product, 0) + record["amount"]

    # Second pass: use a min-heap of size k to track top-k
    # heapq maintains smallest at top; negate revenue to simulate max-heap
    return heapq.nlargest(k, revenue.items(), key=lambda x: x[1])

# Example
records = [
    {"product_id": "A", "amount": 500},
    {"product_id": "B", "amount": 1200},
    {"product_id": "A", "amount": 300},
    {"product_id": "C", "amount": 900},
]
print(top_k_products_by_revenue(records, 2))
# [('B', 1200), ('C', 900)]
```

---

## Python: Implementing a Sliding Window

Sliding windows appear in data engineering for rate limiting, rolling aggregations, and stream processing logic.

```python
from collections import deque

def max_in_sliding_window(values, window_size):
    """
    Return the maximum value in each sliding window of size k.
    Uses a deque (monotonic queue) for O(n) time instead of O(n*k).
    """
    result = []
    dq = deque()  # Stores indices; front is always the max

    for i, val in enumerate(values):
        # Remove indices outside the current window
        while dq and dq[0] < i - window_size + 1:
            dq.popleft()

        # Maintain monotonically decreasing deque
        # Remove indices whose values are smaller than current
        while dq and values[dq[-1]] < val:
            dq.pop()

        dq.append(i)

        # Window is full once we've seen k elements
        if i >= window_size - 1:
            result.append(values[dq[0]])

    return result

# Example
print(max_in_sliding_window([2, 1, 5, 3, 6, 4, 8, 7], window_size=3))
# [5, 5, 6, 6, 8, 8]
```

**Narrate**: "The naive approach is O(n × k) — scan each window. Using a monotonic deque reduces it to O(n) because each element enters and exits the deque at most once. This pattern is relevant to stream processing — computing rolling max or min over a time window."

---

## Refactoring a Given Buggy Solution

Senior interviews sometimes give you intentionally broken code to fix. Common bug types:

**NULL handling bug:**
```python
# Buggy: crashes if 'amount' key is missing
total = sum(r['amount'] for r in records)

# Fixed: handle missing or None values
total = sum(r.get('amount', 0) or 0 for r in records)
```

**Off-by-one in date ranges:**
```sql
-- Buggy: excludes the end date
WHERE order_date >= '2024-01-01' AND order_date < '2024-12-31'
-- This misses Dec 31 if order_date is a DATE type

-- Fixed:
WHERE order_date >= '2024-01-01' AND order_date <= '2024-12-31'
-- Or more explicitly for TIMESTAMP columns:
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```

**Aggregate before join bug:**
```sql
-- Buggy: inflates counts due to join duplication
SELECT u.user_id, COUNT(o.order_id) AS order_count
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY u.user_id;
-- COUNT is inflated by number of line items per order

-- Fixed: count distinct or aggregate before joining
SELECT u.user_id, COUNT(DISTINCT o.order_id) AS order_count
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY u.user_id;
```

Spotting these bugs quickly and explaining them clearly is a strong senior signal.
