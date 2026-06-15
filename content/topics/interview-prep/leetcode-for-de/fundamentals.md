---
title: "LeetCode for Data Engineers - Fundamentals"
topic: interview-prep
subtopic: leetcode-for-de
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [interview-prep, leetcode-for-de, leetcode, sql, python, fundamentals]
---

# LeetCode for Data Engineers — Fundamentals

## Why LeetCode Matters (and Doesn't) for DE Roles

LeetCode is a polarizing topic in data engineering interview prep. The truth is nuanced: most DE interviews do NOT test hard algorithmic LeetCode problems. But specific patterns — particularly SQL problems and a handful of Python patterns — appear frequently. The key is knowing what to study and what to skip.

**What LeetCode is useful for DE prep:**
- SQL problem set: directly relevant, frequently tested
- Easy and Medium Python problems that involve hash maps, counters, and sorting
- Building speed and fluency so you're not nervous about simple problems

**What LeetCode is largely a waste of time for DE prep:**
- Hard graph problems (Dijkstra, topological sort, BFS/DFS on complex graphs)
- Tree traversals and binary tree manipulation
- Dynamic programming on strings (edit distance, knapsack variants)
- Bit manipulation problems

Unless you're interviewing for a company that runs the same SE interview loop for DEs (some big tech companies do), focus your limited prep time on SQL and Python data manipulation.

---

## The Most Important Algorithmic Patterns for DEs

### Pattern 1: Hash Maps for Deduplication and Grouping

The hash map (Python `dict`) is the most useful data structure for data engineering problems. It enables O(1) lookups for deduplication, grouping, and counting.

**LeetCode problems that use this pattern:**
- #1 Two Sum (Easy)
- #217 Contains Duplicate (Easy)
- #242 Valid Anagram (Easy)
- #49 Group Anagrams (Medium)

```python
# Pattern: Group items by a key — like a GROUP BY in SQL
from collections import defaultdict

def group_by_first_letter(words):
    groups = defaultdict(list)
    for word in words:
        groups[word[0]].append(word)
    return dict(groups)

# Pattern: Count occurrences — like COUNT(*) GROUP BY
from collections import Counter

def most_common_elements(nums, k):
    return Counter(nums).most_common(k)
```

**DE application**: This maps directly to deduplication in ETL pipelines, building lookup dictionaries for dimension tables, and computing frequency distributions.

### Pattern 2: Sorting for Grouping and Ranking

Many problems that look complex become simple once the data is sorted. Sorting is foundational in data engineering — Spark uses sort-merge join, Parquet files are sorted by row group min/max for predicate pushdown.

```python
# Sort by multiple keys (like ORDER BY col1, col2 in SQL)
records = [
    {"region": "East", "amount": 100},
    {"region": "West", "amount": 200},
    {"region": "East", "amount": 150},
]
sorted_records = sorted(records, key=lambda r: (r["region"], -r["amount"]))
# Groups East records together, highest amount first within each group
```

**LeetCode problems:**
- #56 Merge Intervals (Medium) — sort by start, then merge
- #252 Meeting Rooms (Easy) — sort by start time
- #179 Largest Number (Medium) — custom sort key

### Pattern 3: Two Pointers for Merge Operations

The two-pointer pattern is used in merge-sort, merging sorted datasets, and finding pairs. This is directly relevant to how sort-merge joins work internally.

```python
def merge_sorted_arrays(a, b):
    """Merge two sorted arrays in O(n+m) — how sort-merge join works."""
    result = []
    i, j = 0, 0
    while i < len(a) and j < len(b):
        if a[i] <= b[j]:
            result.append(a[i])
            i += 1
        else:
            result.append(b[j])
            j += 1
    result.extend(a[i:])
    result.extend(b[j:])
    return result
```

**LeetCode problems:**
- #21 Merge Two Sorted Lists (Easy)
- #88 Merge Sorted Array (Easy)

### Pattern 4: Sliding Window for Stream Processing

Sliding windows compute aggregations over a moving time or count window. This is exactly what `ROWS BETWEEN N PRECEDING AND CURRENT ROW` does in SQL, and what Flink/Kafka Streams tumbling/sliding windows do.

```python
def sliding_window_max_sum(nums, k):
    """Maximum sum of any k consecutive elements — O(n)."""
    window_sum = sum(nums[:k])
    max_sum = window_sum

    for i in range(k, len(nums)):
        window_sum += nums[i] - nums[i - k]  # Add new, remove old
        max_sum = max(max_sum, window_sum)

    return max_sum
```

**LeetCode problems:**
- #643 Maximum Average Subarray I (Easy)
- #209 Minimum Size Subarray Sum (Medium)

### Pattern 5: Heap for Top-K

The heap (Python `heapq`) efficiently maintains the K largest or smallest elements. This maps directly to `SELECT ... ORDER BY col DESC LIMIT K` queries and is how Spark implements `LIMIT` at scale.

```python
import heapq

def top_k_frequent(nums, k):
    """Find k most frequent elements — O(n log k)."""
    counts = Counter(nums)
    # heapq.nlargest is O(n log k) — better than sorting O(n log n) when k << n
    return [item for item, _ in heapq.nlargest(k, counts.items(), key=lambda x: x[1])]
```

**LeetCode problems:**
- #347 Top K Frequent Elements (Medium)
- #215 Kth Largest Element in an Array (Medium)
- #373 Find K Pairs with Smallest Sums (Medium)

---

## SQL LeetCode: The Essential Problems

The SQL section of LeetCode is the most directly relevant practice for DE interviews. Unlike the algorithm problems, almost every SQL LeetCode problem tests a pattern you'll actually use.

### The 5 Must-Know SQL Patterns (Fundamentals)

**1. Basic Aggregation with HAVING**

LeetCode #182: Duplicate Emails — Find emails that appear more than once.

```sql
SELECT email
FROM Person
GROUP BY email
HAVING COUNT(*) > 1;
```

**2. Simple JOIN**

LeetCode #175: Combine Two Tables — Return all persons with their city/state, even if no address exists.

```sql
SELECT p.firstName, p.lastName, a.city, a.state
FROM Person p
LEFT JOIN Address a ON p.personId = a.personId;
```

**3. Subquery for Comparison**

LeetCode #181: Employees Earning More Than Their Managers

```sql
SELECT e.name AS Employee
FROM Employee e
JOIN Employee m ON e.managerId = m.id
WHERE e.salary > m.salary;
```

**4. LIMIT / Ranking for Nth Value**

LeetCode #176: Second Highest Salary

```sql
-- Using subquery
SELECT MAX(salary) AS SecondHighestSalary
FROM Employee
WHERE salary < (SELECT MAX(salary) FROM Employee);

-- Using window function (cleaner, handles no-2nd-salary case)
SELECT DISTINCT salary AS SecondHighestSalary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM Employee
) ranked
WHERE rnk = 2;
-- Returns NULL automatically if no second-highest exists
```

**5. Date Functions**

LeetCode #197: Rising Temperature — Find days where temperature was higher than the previous day.

```sql
SELECT w1.id
FROM Weather w1
JOIN Weather w2 ON w1.recordDate = w2.recordDate + INTERVAL '1 day'
WHERE w1.temperature > w2.temperature;
```

---

## What to Skip and Why

**Graph algorithms**: DE interviews almost never test Dijkstra, BFS tree traversal, or flood fill. These appear in SWE interviews. Skip unless you're interviewing at a company known for SWE-style DE interviews.

**String manipulation algorithms**: KMP pattern matching, palindrome partitioning, and similar problems are not DE-relevant. Basic string parsing (split, strip, regex) is enough.

**Dynamic programming**: Coin change, longest increasing subsequence, and similar DP problems rarely appear in DE interviews. The only DP-adjacent thing that occasionally shows up is "find the minimum cost to process these steps" type questions at data platform companies.

**Hard difficulty problems**: For DE roles at most companies, Medium is the ceiling. Hard LeetCode problems appear mainly at FAANG for SWE roles and occasionally for DE roles at companies that use a unified interview loop.

---

## Building a Study Habit

The most efficient LeetCode practice for DE prep follows this cadence:

- **Week 1**: All SQL Easy problems on LeetCode (there are ~30). Do every single one. This builds speed and exposes all the basic patterns.
- **Week 2**: SQL Medium problems — focus on window functions, self-joins, and ranking problems.
- **Week 3**: Python Easy + Medium — hash map, sorting, two-pointer patterns only.
- **Week 4**: Review. Redo any problem you got wrong. Practice under time pressure (30 min per Medium).

Total: ~80–100 problems across 4 weeks is sufficient for most DE interviews. More than 200 problems without targeted practice is diminishing returns.
