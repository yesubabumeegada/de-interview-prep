---
title: "LeetCode for Data Engineers - Intermediate"
topic: interview-prep
subtopic: leetcode-for-de
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [interview-prep, leetcode-for-de, leetcode, sql, python, intermediate]
---

# LeetCode for Data Engineers — Intermediate

## The 20 Most-Tested SQL Patterns on LeetCode

These patterns appear repeatedly in both LeetCode SQL problems and actual DE interviews. Mastering them means you can solve new problems quickly by pattern-matching rather than reasoning from scratch every time.

---

### 1. Consecutive Numbers (LeetCode #180)

Find numbers that appear at least N times consecutively in a table.

```sql
-- Consecutive numbers appearing 3+ times (classic solution)
SELECT DISTINCT l1.num AS ConsecutiveNums
FROM Logs l1
JOIN Logs l2 ON l2.id = l1.id + 1 AND l2.num = l1.num
JOIN Logs l3 ON l3.id = l1.id + 2 AND l3.num = l1.num;

-- Modern approach using LAG/LEAD (cleaner, scales better)
SELECT DISTINCT num AS ConsecutiveNums
FROM (
    SELECT
        num,
        LAG(num, 1) OVER (ORDER BY id) AS prev1,
        LAG(num, 2) OVER (ORDER BY id) AS prev2
    FROM Logs
) windowed
WHERE num = prev1 AND num = prev2;
```

**Pattern**: Consecutive detection. Variations include finding consecutive dates, consecutive IDs, and streaks.

---

### 2. Nth Highest Salary (LeetCode #177)

```sql
-- Return the Nth highest salary (NULL if N doesn't exist)
CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
BEGIN
    RETURN (
        SELECT DISTINCT salary
        FROM (
            SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
            FROM Employee
        ) ranked
        WHERE rnk = N
    );
END;

-- Without stored function (works in any SQL dialect)
SELECT DISTINCT salary AS NthHighestSalary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM Employee
) r
WHERE rnk = 5;  -- Replace 5 with N
```

---

### 3. Department Top 3 Salaries (LeetCode #185)

Find employees in the top 3 unique salaries in each department (including ties).

```sql
SELECT Department, Employee, Salary
FROM (
    SELECT
        d.name AS Department,
        e.name AS Employee,
        e.salary AS Salary,
        DENSE_RANK() OVER (
            PARTITION BY e.departmentId
            ORDER BY e.salary DESC
        ) AS salary_rank
    FROM Employee e
    JOIN Department d ON e.departmentId = d.id
) ranked
WHERE salary_rank <= 3;
```

**Why this is important**: This is the most tested DE SQL pattern at FAANG companies. The combination of JOIN + DENSE_RANK + PARTITION BY appears in many variations.

---

### 4. Rank Scores (LeetCode #178)

Assign ranks without gaps (DENSE_RANK equivalent).

```sql
SELECT
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) AS rank
FROM Scores
ORDER BY score DESC;
```

---

### 5. Delete Duplicate Emails (LeetCode #196)

Keep only one row per email (the one with the smallest ID). This tests understanding of delete with subquery.

```sql
-- Standard approach
DELETE FROM Person
WHERE id NOT IN (
    SELECT MIN(id)
    FROM Person
    GROUP BY email
);

-- CTE approach (cleaner)
WITH duplicates AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) AS rn
    FROM Person
)
DELETE FROM Person WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);
```

---

### 6. Rising Temperature (LeetCode #197)

Self-join on dates to compare consecutive rows.

```sql
SELECT w1.id
FROM Weather w1
JOIN Weather w2
    ON DATEDIFF(w1.recordDate, w2.recordDate) = 1
WHERE w1.temperature > w2.temperature;
```

**Alternative using LAG:**
```sql
SELECT id FROM (
    SELECT id,
        temperature,
        LAG(temperature) OVER (ORDER BY recordDate) AS prev_temp,
        LAG(recordDate) OVER (ORDER BY recordDate) AS prev_date,
        recordDate
    FROM Weather
) t
WHERE temperature > prev_temp
  AND DATEDIFF(recordDate, prev_date) = 1;
```

---

### 7. Customers Who Never Order (LeetCode #183)

Classic anti-join pattern.

```sql
SELECT name AS Customers
FROM Customers
WHERE id NOT IN (SELECT customerId FROM Orders);

-- Safer equivalent (handles NULLs):
SELECT c.name AS Customers
FROM Customers c
LEFT JOIN Orders o ON c.id = o.customerId
WHERE o.customerId IS NULL;
```

---

### 8. Human Traffic of Stadium (LeetCode #601)

Find consecutive rows (by ID) where all three have people ≥ 100. This is a gaps-and-islands variant.

```sql
SELECT DISTINCT s1.*
FROM Stadium s1, Stadium s2, Stadium s3
WHERE s1.people >= 100 AND s2.people >= 100 AND s3.people >= 100
  AND (
    (s1.id + 1 = s2.id AND s2.id + 1 = s3.id)  -- s1, s2, s3 consecutive
    OR (s2.id + 1 = s1.id AND s1.id + 1 = s3.id)  -- s2, s1, s3
    OR (s2.id + 1 = s3.id AND s3.id + 1 = s1.id)  -- s2, s3, s1
  )
ORDER BY s1.id;
```

---

### 9. Game Play Analysis (LeetCode #511-514 series)

Multi-part series on player activity analysis. Pattern: first login date, consecutive days active, retention rate.

```sql
-- #511: First login date per player
SELECT player_id, MIN(event_date) AS first_login
FROM Activity
GROUP BY player_id;

-- #512: Device used on first login
SELECT player_id, device_id
FROM (
    SELECT player_id, device_id,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY event_date) AS rn
    FROM Activity
) t
WHERE rn = 1;

-- #514: Fraction retained (logged in the day after first login)
SELECT ROUND(
    SUM(CASE WHEN DATEDIFF(a.event_date, fa.first_login) = 1 THEN 1 ELSE 0 END)
    / COUNT(DISTINCT a.player_id),
    2
) AS fraction
FROM Activity a
JOIN (
    SELECT player_id, MIN(event_date) AS first_login
    FROM Activity
    GROUP BY player_id
) fa ON a.player_id = fa.player_id;
```

---

### 10. Immediate Food Delivery (LeetCode #1174)

Compute percentages with conditional aggregation.

```sql
-- % of orders where first order was an immediate delivery
SELECT ROUND(
    100.0 * SUM(CASE WHEN d.order_date = d.customer_pref_delivery_date THEN 1 ELSE 0 END)
    / COUNT(*),
    2
) AS immediate_percentage
FROM Delivery d
WHERE (d.customer_id, d.order_date) IN (
    SELECT customer_id, MIN(order_date)
    FROM Delivery
    GROUP BY customer_id
);
```

---

## Python Intermediate Patterns: The Big 3

### 1. Interval Merging (Meeting Rooms pattern)

```python
def merge_intervals(intervals):
    """
    Merge overlapping intervals. Relevant to: session deduplication,
    time window merging, CDC event compaction.
    Time: O(n log n) for sort, O(n) for merge pass.
    """
    if not intervals:
        return []

    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]

    for start, end in intervals[1:]:
        if start <= merged[-1][1]:  # Overlaps with last merged interval
            merged[-1][1] = max(merged[-1][1], end)  # Extend
        else:
            merged.append([start, end])

    return merged

# Example
print(merge_intervals([[1,3],[2,6],[8,10],[15,18]]))
# [[1, 6], [8, 10], [15, 18]]
```

**DE application**: Merging time windows, compacting CDC event ranges, finding gaps in data coverage.

---

### 2. Anagram Grouping (Hash of Sorted Characters)

```python
from collections import defaultdict

def group_anagrams(strs):
    """
    Group strings that are anagrams of each other.
    Key insight: anagrams have the same sorted characters.
    Time: O(n * k log k) where k = max string length.
    """
    groups = defaultdict(list)
    for s in strs:
        key = tuple(sorted(s))  # Canonical form — same for all anagrams
        groups[key].append(s)
    return list(groups.values())

# DE application: group equivalent records by a canonical key
# E.g., "NYC" / "New York City" / "new york" → canonical form after normalization
```

---

### 3. Most Frequent Element with Bucket Sort (LeetCode #347)

```python
from collections import Counter

def top_k_frequent_bucket(nums, k):
    """
    Top K frequent elements using bucket sort — O(n) vs O(n log n) for sort.
    Key insight: frequency is bounded by n, so use frequency as array index.
    """
    count = Counter(nums)
    # Bucket i contains all elements with frequency i
    buckets = [[] for _ in range(len(nums) + 1)]

    for num, freq in count.items():
        buckets[freq].append(num)

    result = []
    for freq in range(len(buckets) - 1, 0, -1):  # High frequency first
        result.extend(buckets[freq])
        if len(result) >= k:
            return result[:k]

    return result

# Example
print(top_k_frequent_bucket([1,1,1,2,2,3], k=2))
# [1, 2]
```

---

## StrataScratch for SQL Practice

StrataScratch (stratascratch.com) is the best supplement to LeetCode for DE SQL prep. Unlike LeetCode's simplified schemas, StrataScratch uses schemas from real companies (Airbnb, Facebook, Google, Amazon, Uber) and asks business-context questions.

**Top StrataScratch problems to practice:**
- Airbnb: Find the host popularity percentile, calculate host response rates
- Facebook: Calculate 30-day rolling average of daily active users
- Google: Find the top-performing ad per campaign with ties
- Uber: Calculate surge pricing impact on average fare
- Amazon: Find products with consecutive months of increasing sales

These problems feel more realistic than LeetCode SQL because they involve messy data, real business logic, and require you to think about NULLs and edge cases.

**HackerRank SQL track**: The HackerRank SQL track covers basic to advanced SQL and is useful for building speed. The "Advanced" section includes complex joins, aggregations, and subqueries comparable to LeetCode Medium.
