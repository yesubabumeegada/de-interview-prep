---
title: "SQL Interview Coding Problems — Intermediate"
topic: sql
subtopic: interview-coding-problems
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, interview, gaps-and-islands, sessionization, pivot, recursive-cte, median]
---

# SQL Interview Coding Problems — Intermediate

These patterns separate mid-level candidates from junior ones. Each requires multi-step reasoning and a non-obvious trick. Master these and you will handle the majority of real DE interview coding rounds.

---

## Problem 1: Gaps and Islands

**Interview prompt:** "You have a table of user activity dates. Find consecutive date ranges (islands) where each user was active — the start and end date of each continuous streak."

This is the most famous intermediate SQL interview problem. It appears at virtually every company that does analytical SQL interviews.

### Setup

```sql
CREATE TABLE user_activity (
    user_id       INT,
    activity_date DATE
);

INSERT INTO user_activity VALUES
(1, '2024-01-01'), (1, '2024-01-02'), (1, '2024-01-03'),
(1, '2024-01-07'), (1, '2024-01-08'),
(1, '2024-01-15'),
(2, '2024-01-05'), (2, '2024-01-06'), (2, '2024-01-07'),
(2, '2024-01-10'), (2, '2024-01-11'), (2, '2024-01-12'), (2, '2024-01-13');
```

### The trick: date minus row number = constant within an island

```sql
WITH numbered AS (
    SELECT
        user_id,
        activity_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) AS rn
    FROM user_activity
),
islands AS (
    SELECT
        user_id,
        activity_date,
        -- This difference is CONSTANT for consecutive dates within the same streak
        activity_date - INTERVAL '1 day' * rn AS island_group
    FROM numbered
)
SELECT
    user_id,
    MIN(activity_date) AS streak_start,
    MAX(activity_date) AS streak_end,
    COUNT(*)           AS streak_length_days
FROM islands
GROUP BY user_id, island_group
ORDER BY user_id, streak_start;
```

**Expected output:**

| user_id | streak_start | streak_end | streak_length_days |
|---|---|---|---|
| 1 | 2024-01-01 | 2024-01-03 | 3 |
| 1 | 2024-01-07 | 2024-01-08 | 2 |
| 1 | 2024-01-15 | 2024-01-15 | 1 |
| 2 | 2024-01-05 | 2024-01-07 | 3 |
| 2 | 2024-01-10 | 2024-01-13 | 4 |

### Why the trick works — explained visually

```
user_id=1:
Date        | ROW_NUMBER | date - rn (as date offset)
2024-01-01  | 1          | 2023-12-31  ← same group
2024-01-02  | 2          | 2023-12-31  ← same group
2024-01-03  | 3          | 2023-12-31  ← same group
2024-01-07  | 4          | 2024-01-03  ← new group (gap of 3 days)
2024-01-08  | 5          | 2024-01-03  ← same group as Jan 7
2024-01-15  | 6          | 2024-01-09  ← new group (gap)
```

When dates are consecutive, incrementing the date by 1 and incrementing ROW_NUMBER by 1 cancel each other out — the difference stays constant. Any gap in the date sequence breaks the constancy, creating a new group value.

### PostgreSQL vs other databases

```sql
-- PostgreSQL: date - integer works directly
activity_date - rn AS island_group

-- MySQL: use DATE_SUB
DATE_SUB(activity_date, INTERVAL rn DAY) AS island_group

-- SQL Server: use DATEADD
DATEADD(day, -rn, activity_date) AS island_group

-- BigQuery / Snowflake
DATE_SUB(activity_date, INTERVAL rn DAY) AS island_group
```

---

## Problem 2: Sessionization

**Interview prompt:** "You have a table of page view events. A new session starts when there's a gap of more than 30 minutes since the user's last event. Assign a session ID to each event."

### Setup

```sql
CREATE TABLE page_views (
    event_id   INT,
    user_id    INT,
    page       VARCHAR(100),
    event_time TIMESTAMP
);

INSERT INTO page_views VALUES
(1,  1, '/home',    '2024-01-01 10:00:00'),
(2,  1, '/product', '2024-01-01 10:05:00'),
(3,  1, '/cart',    '2024-01-01 10:22:00'),
(4,  1, '/home',    '2024-01-01 11:10:00'),  -- >30 min gap: new session
(5,  1, '/product', '2024-01-01 11:15:00'),
(6,  2, '/home',    '2024-01-01 09:00:00'),
(7,  2, '/about',   '2024-01-01 09:45:00'),  -- >30 min gap: new session
(8,  2, '/contact', '2024-01-01 09:50:00');
```

### Solution

```sql
WITH time_gaps AS (
    SELECT
        event_id,
        user_id,
        page,
        event_time,
        -- Time since previous event for the same user (NULL for first event)
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time,
        -- Is this event more than 30 minutes after the previous? (1 = new session)
        CASE
            WHEN event_time - LAG(event_time) OVER (
                    PARTITION BY user_id ORDER BY event_time
                 ) > INTERVAL '30 minutes'
              OR LAG(event_time) OVER (
                    PARTITION BY user_id ORDER BY event_time
                 ) IS NULL
            THEN 1
            ELSE 0
        END AS is_session_start
    FROM page_views
),
session_numbers AS (
    SELECT
        event_id,
        user_id,
        page,
        event_time,
        is_session_start,
        -- Running sum of session starts = session number per user
        SUM(is_session_start) OVER (
            PARTITION BY user_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_num
    FROM time_gaps
)
SELECT
    event_id,
    user_id,
    page,
    event_time,
    -- Combine user_id and session_num for a globally unique session ID
    user_id || '-' || session_num AS session_id
FROM session_numbers
ORDER BY user_id, event_time;
```

**Expected output:**

| event_id | user_id | page | event_time | session_id |
|---|---|---|---|---|
| 1 | 1 | /home | 10:00 | 1-1 |
| 2 | 1 | /product | 10:05 | 1-1 |
| 3 | 1 | /cart | 10:22 | 1-1 |
| 4 | 1 | /home | 11:10 | 1-2 |
| 5 | 1 | /product | 11:15 | 1-2 |

### Key insight

The `SUM(is_session_start) OVER (...)` pattern is extremely versatile. Any time you need to group consecutive rows based on a condition, you can:
1. Flag the condition (0/1)
2. Take a running sum of the flags
3. The running sum becomes a group ID

---

## Problem 3: Median Calculation

**Interview prompt:** "Find the median salary for each department. Do not use a MEDIAN() function."

Most production databases either lack MEDIAN() or behave differently from each other. You need to know the portable approach.

### Setup

```sql
CREATE TABLE employees (
    emp_id     INT,
    dept       VARCHAR(50),
    salary     DECIMAL(10,2)
);

INSERT INTO employees VALUES
(1, 'Engineering', 95000), (2, 'Engineering', 110000),
(3, 'Engineering', 87000), (4, 'Engineering', 102000),
(5, 'Engineering', 115000),
(6, 'Sales', 65000), (7, 'Sales', 72000),
(8, 'Sales', 68000), (9, 'Sales', 80000);
```

### Solution 1: PERCENTILE_CONT (PostgreSQL, SQL Server, BigQuery, Snowflake)

```sql
SELECT
    dept,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) AS p25_salary,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) AS p75_salary
FROM employees
GROUP BY dept;
```

`PERCENTILE_CONT` interpolates between values if the median falls between two rows (continuous). `PERCENTILE_DISC` returns the actual nearest value (discrete).

### Solution 2: Manual ROW_NUMBER approach (universal)

```sql
WITH ranked AS (
    SELECT
        dept,
        salary,
        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary)       AS rn_asc,
        COUNT(*) OVER (PARTITION BY dept)                           AS total_count
    FROM employees
)
SELECT
    dept,
    AVG(salary) AS median_salary  -- AVG handles both odd and even counts
FROM ranked
WHERE
    -- For odd count: middle row. For even count: two middle rows.
    rn_asc IN (
        (total_count + 1) / 2,
        (total_count + 2) / 2
    )
GROUP BY dept;
```

**Why `AVG` works here:** For an odd count (5 rows), both expressions `(5+1)/2=3` and `(5+2)/2=3` point to the same row — AVG of one value = that value. For an even count (4 rows), they point to rows 2 and 3 — AVG gives the interpolated median.

---

## Problem 4: Pivot / Crosstab

**Interview prompt:** "Show monthly revenue as a wide table with months as columns: Jan | Feb | Mar | ..."

### Setup

```sql
CREATE TABLE sales (
    sale_date DATE,
    revenue   DECIMAL(12,2)
);

INSERT INTO sales VALUES
('2024-01-05', 1200), ('2024-01-12', 800), ('2024-01-20', 950),
('2024-02-03', 1500), ('2024-02-14', 600), ('2024-02-22', 1100),
('2024-03-01', 2000), ('2024-03-10', 750), ('2024-03-25', 1300);
```

### Solution 1: CASE WHEN (works in every database)

```sql
SELECT
    EXTRACT(YEAR FROM sale_date)                                    AS year_num,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 1  THEN revenue ELSE 0 END) AS jan,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 2  THEN revenue ELSE 0 END) AS feb,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 3  THEN revenue ELSE 0 END) AS mar,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 4  THEN revenue ELSE 0 END) AS apr,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 5  THEN revenue ELSE 0 END) AS may,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 6  THEN revenue ELSE 0 END) AS jun,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 7  THEN revenue ELSE 0 END) AS jul,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 8  THEN revenue ELSE 0 END) AS aug,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 9  THEN revenue ELSE 0 END) AS sep,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 10 THEN revenue ELSE 0 END) AS oct,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 11 THEN revenue ELSE 0 END) AS nov,
    SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 12 THEN revenue ELSE 0 END) AS dec
FROM sales
GROUP BY EXTRACT(YEAR FROM sale_date);
```

### Solution 2: PIVOT syntax (SQL Server / Snowflake)

```sql
-- SQL Server
SELECT *
FROM (
    SELECT
        YEAR(sale_date) AS year_num,
        DATENAME(MONTH, sale_date) AS month_name,
        revenue
    FROM sales
) AS src
PIVOT (
    SUM(revenue)
    FOR month_name IN ([January], [February], [March], [April],
                       [May], [June], [July], [August],
                       [September], [October], [November], [December])
) AS pvt;
```

### When to pivot in SQL vs application layer

| Approach | Use when |
|---|---|
| SQL PIVOT / CASE WHEN | Fixed, known set of columns; report goes directly to a BI tool |
| Application layer pivot | Dynamic column list not known at query time; generating reports via Python/pandas |

> **Interview tip:** Interviewers often ask "how would you do this if the months were dynamic?" Answer: you can't do it in pure SQL without dynamic SQL. In practice, you'd either use a reporting layer (pandas pivot_table, Tableau, etc.) or build a stored procedure with dynamic SQL.

---

## Problem 5: Recursive CTE

**Interview prompt:** "You have an employees table with a manager_id column. Show the full org chart hierarchy with each employee's depth from the CEO."

### Setup

```sql
CREATE TABLE org (
    emp_id     INT,
    name       VARCHAR(100),
    manager_id INT  -- NULL for CEO
);

INSERT INTO org VALUES
(1,  'Alice (CEO)',    NULL),
(2,  'Bob (VP Eng)',   1),
(3,  'Carol (VP Mkt)', 1),
(4,  'Dave (Sr Eng)',  2),
(5,  'Eve (Eng)',      2),
(6,  'Frank (Mkt Mgr)',3),
(7,  'Grace (Mkt)',    6),
(8,  'Hank (Eng)',     4);
```

### Solution

```sql
WITH RECURSIVE org_chart AS (
    -- Anchor member: start with the CEO
    SELECT
        emp_id,
        name,
        manager_id,
        0                          AS depth,
        CAST(name AS VARCHAR(500)) AS path
    FROM org
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: join children to their parent
    SELECT
        o.emp_id,
        o.name,
        o.manager_id,
        oc.depth + 1,
        oc.path || ' > ' || o.name
    FROM org o
    INNER JOIN org_chart oc
        ON o.manager_id = oc.emp_id
)
SELECT
    emp_id,
    REPEAT('  ', depth) || name  AS indented_name,  -- visual indentation
    depth,
    path
FROM org_chart
ORDER BY path;
```

**Expected output:**

```
Alice (CEO)                       depth=0
  Bob (VP Eng)                    depth=1
    Dave (Sr Eng)                 depth=2
      Hank (Eng)                  depth=3
    Eve (Eng)                     depth=2
  Carol (VP Mkt)                  depth=1
    Frank (Mkt Mgr)               depth=2
      Grace (Mkt)                 depth=3
```

### How recursive CTEs work

```
1. ANCHOR runs once → produces the CEO row
2. RECURSIVE part joins org_chart (current results) to org (the full table)
   → finds direct reports of current results
3. Repeat step 2 until no new rows are found
4. Final result = UNION ALL of all iterations
```

### Safety: MAX recursion depth

Most databases cap recursion at 100 (SQL Server) or 1000 (PostgreSQL). For very deep hierarchies:

```sql
-- PostgreSQL: add a depth limit
WHERE oc.depth < 50  -- prevent infinite loops from circular refs
```

### Recursive CTE use cases in DE

| Use case | Description |
|---|---|
| Org hierarchy | Manager → employee chains |
| Product BOM | Bill of materials (parent product → components) |
| Date spine generation | Generate a series of dates (see senior-deep-dive) |
| Graph traversal | Find connected nodes (within limits) |

---

## Intermediate Pattern Quick Reference

| Problem | Trick | Key functions |
|---|---|---|
| Gaps and Islands | date - row_number = constant in island | ROW_NUMBER, GROUP BY island_group |
| Sessionization | Running sum of session-start flags | LAG, SUM OVER |
| Median | PERCENTILE_CONT or middle-row ROW_NUMBER | PERCENTILE_CONT, ROW_NUMBER, AVG |
| Pivot | CASE WHEN per column or PIVOT syntax | SUM(CASE WHEN), PIVOT |
| Hierarchy | WITH RECURSIVE + UNION ALL + anchor | RECURSIVE CTE |

---

## What Interviewers Are Testing at Mid-Level

> **Mid-level:** Can you construct a multi-step solution? Do you understand why the gaps-and-islands trick works, not just copy it? Can you pivot without a built-in function?

Mid-level interviewers want to see:
1. Comfort with CTEs chaining multiple steps
2. Understanding of window function frames and ordering
3. Ability to explain non-obvious tricks (like the date - rn pattern)
4. Awareness of database differences (PERCENTILE_CONT availability, PIVOT syntax)
5. Knowing when SQL is the wrong tool (dynamic pivots → application layer)
