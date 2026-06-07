---
title: "Recursive Queries - Scenario Questions"
topic: sql
subtopic: recursive-queries
content_type: scenario_question
tags: [sql, recursive-cte, hierarchies, interview, scenarios]
---

# Scenario Questions — Recursive Queries

<article data-difficulty="junior">

## 🟢 Junior: Find All Subordinates of a Manager

**Scenario:** You have an `employees` table with `employee_id`, `name`, and `manager_id`. Given a specific manager (Bob, id=2), write a query that returns all direct and indirect reports under him, along with their depth level.

```sql
-- employees table
-- | employee_id | name    | manager_id |
-- |-------------|---------|------------|
-- | 1           | Alice   | NULL       |
-- | 2           | Bob     | 1          |
-- | 3           | Carol   | 1          |
-- | 4           | David   | 2          |
-- | 5           | Eve     | 2          |
-- | 6           | Frank   | 4          |
-- | 7           | Grace   | 5          |
```

<details>
<summary>💡 Hint</summary>

Start the base case with Bob (employee_id = 2), then recursively find anyone whose `manager_id` matches a previously found `employee_id`. Track the level by incrementing a counter at each step.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE subordinates AS (
    -- Base case: Start with Bob
    SELECT employee_id, name, manager_id, 0 AS level
    FROM employees
    WHERE employee_id = 2

    UNION ALL

    -- Recursive step: Find direct reports of current level
    SELECT e.employee_id, e.name, e.manager_id, s.level + 1
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.employee_id
)
SELECT employee_id, name, level
FROM subordinates
WHERE level > 0  -- Exclude Bob himself
ORDER BY level, name;
```

**Result:**

| employee_id | name  | level |
|-------------|-------|-------|
| 4           | David | 1     |
| 5           | Eve   | 1     |
| 6           | Frank | 2     |
| 7           | Grace | 2     |

**Why this works:**
- Iteration 1: Finds Bob (level 0)
- Iteration 2: Finds David, Eve (manager_id = 2, level 1)
- Iteration 3: Finds Frank (manager_id = 4), Grace (manager_id = 5) — level 2
- Iteration 4: No one reports to Frank or Grace → recursion stops

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Find the Shortest Path Between Two Nodes

**Scenario:** You have a `connections` table representing a social network. Each row is a bidirectional friendship. Write a query to find the shortest path (fewest hops) between user 1 (Alice) and user 7 (Grace), and return the path as a string.

```sql
-- connections table
-- | user_a | user_b |
-- |--------|--------|
-- | 1      | 2      |
-- | 1      | 3      |
-- | 2      | 4      |
-- | 3      | 5      |
-- | 4      | 6      |
-- | 5      | 6      |
-- | 6      | 7      |
```

<details>
<summary>💡 Hint</summary>

Since connections are bidirectional, you need to consider both directions (user_a→user_b and user_b→user_a). Use a path array to track visited nodes and prevent cycles. Stop when you reach the target node. Use array length to find the shortest path.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE paths AS (
    -- Base case: Start from user 1
    SELECT 
        1 AS current_node,
        ARRAY[1] AS path,
        1 AS path_length
    
    UNION ALL
    
    -- Recursive step: Explore neighbors in both directions
    SELECT 
        CASE 
            WHEN c.user_a = p.current_node THEN c.user_b
            ELSE c.user_a
        END AS current_node,
        p.path || CASE 
            WHEN c.user_a = p.current_node THEN c.user_b
            ELSE c.user_a
        END,
        p.path_length + 1
    FROM connections c
    JOIN paths p ON (c.user_a = p.current_node OR c.user_b = p.current_node)
    WHERE NOT (CASE 
            WHEN c.user_a = p.current_node THEN c.user_b
            ELSE c.user_a
        END) = ANY(p.path)  -- Prevent cycles
    AND p.path_length < 10  -- Safety limit
)
SELECT path, path_length
FROM paths
WHERE current_node = 7
ORDER BY path_length
LIMIT 1;
```

**Result:**

| path            | path_length |
|-----------------|-------------|
| {1,3,5,6,7}    | 5           |

**Key design decisions:**
- Bidirectional edges are handled by checking both `user_a` and `user_b`
- `NOT ... = ANY(p.path)` prevents revisiting nodes (cycle detection)
- `LIMIT 1` with `ORDER BY path_length` gives the shortest path
- Safety limit prevents infinite exploration in larger graphs

**Alternative paths exist:** 1→2→4→6→7 (also length 5). BFS-style exploration finds them all.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Generate a Running Total with Gap-Filled Dates

**Scenario:** You have a `transactions` table with sporadic entries (not every day has a transaction). Write a single query using recursive CTEs to: (1) generate all dates in the range, (2) left join transactions to fill gaps with zero, and (3) compute a running cumulative balance.

```sql
-- transactions table
-- | txn_date   | amount  |
-- |------------|---------|
-- | 2024-01-01 | 1000.00 |
-- | 2024-01-03 | -200.00 |
-- | 2024-01-03 | 500.00  |
-- | 2024-01-06 | -150.00 |
-- | 2024-01-08 | 300.00  |
```

Expected output should show ALL dates from Jan 1-8 with daily totals and running balance.

<details>
<summary>💡 Hint</summary>

Use one recursive CTE to generate the date series from min to max date. Then aggregate transactions by date, LEFT JOIN the date series to the aggregated amounts, COALESCE missing days to zero, and apply a window function for the running sum.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE date_series AS (
    -- Generate all dates from min to max
    SELECT MIN(txn_date) AS dt FROM transactions
    UNION ALL
    SELECT dt + INTERVAL '1 day'
    FROM date_series
    WHERE dt < (SELECT MAX(txn_date) FROM transactions)
),
daily_totals AS (
    -- Aggregate multiple transactions per day
    SELECT txn_date, SUM(amount) AS daily_amount
    FROM transactions
    GROUP BY txn_date
),
filled AS (
    -- Join dates with totals, fill gaps with zero
    SELECT 
        ds.dt AS txn_date,
        COALESCE(dt_totals.daily_amount, 0) AS daily_amount
    FROM date_series ds
    LEFT JOIN daily_totals dt_totals ON ds.dt = dt_totals.txn_date
)
SELECT 
    txn_date,
    daily_amount,
    SUM(daily_amount) OVER (ORDER BY txn_date) AS running_balance
FROM filled
ORDER BY txn_date;
```

**Result:**

| txn_date   | daily_amount | running_balance |
|------------|-------------|-----------------|
| 2024-01-01 | 1000.00     | 1000.00         |
| 2024-01-02 | 0.00        | 1000.00         |
| 2024-01-03 | 300.00      | 1300.00         |
| 2024-01-04 | 0.00        | 1300.00         |
| 2024-01-05 | 0.00        | 1300.00         |
| 2024-01-06 | -150.00     | 1150.00         |
| 2024-01-07 | 0.00        | 1150.00         |
| 2024-01-08 | 300.00      | 1450.00         |

**Why this approach:**
- Recursive CTE generates the continuous date spine (no gaps)
- Separate CTE aggregates multi-transaction days (Jan 3 had two txns)
- LEFT JOIN ensures every date appears even with no transactions
- Window function `SUM() OVER (ORDER BY)` computes the running total efficiently

**Production note:** For large date ranges, `generate_series()` (PostgreSQL) is more efficient than a recursive CTE. The recursive approach shown here is portable across databases.

</details>

</article>
