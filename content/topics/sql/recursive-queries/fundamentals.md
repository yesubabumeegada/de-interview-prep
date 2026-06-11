---
title: "Recursive Queries - Fundamentals"
topic: sql
subtopic: recursive-queries
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [sql, recursive-cte, hierarchies, tree-traversal, org-chart, bill-of-materials]
---

# Recursive Queries — Fundamentals


## 🎯 Analogy

Think of recursive CTEs like a mirror facing a mirror: the query refers to its own output, letting you traverse hierarchies (org charts, bill of materials, folder trees) without knowing the depth in advance.

---
## What Are Recursive CTEs?

A **recursive CTE** (Common Table Expression) is a query that references itself to traverse hierarchical or graph-like data. Think of it as giving SQL a "loop" — it repeatedly executes until it runs out of rows to process.

> **Analogy:** Imagine you're exploring a family tree. You start with one person (the base case), then find their children, then their children's children, and so on. A recursive CTE does exactly this — it starts with a known set of rows and keeps expanding until it reaches the leaves.

---

## Syntax Structure

Every recursive CTE has exactly two parts joined by `UNION ALL`:

```sql
WITH RECURSIVE cte_name AS (
    -- 1. BASE CASE: The starting point (anchor member)
    SELECT columns
    FROM table
    WHERE starting_condition

    UNION ALL

    -- 2. RECURSIVE STEP: References itself to get the next level
    SELECT columns
    FROM table
    JOIN cte_name ON parent-child relationship
)
SELECT * FROM cte_name;
```

**How execution works:**

| Iteration | What Happens |
|-----------|-------------|
| 1 | Base case runs → produces initial rows |
| 2 | Recursive step runs using iteration 1 results |
| 3 | Recursive step runs using iteration 2 results |
| ... | Continues until recursive step returns 0 rows |
| Final | All iterations' results are combined |

---

## Example 1: Organizational Chart

Given an `employees` table:

```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    manager_id INT REFERENCES employees(employee_id)
);

INSERT INTO employees VALUES
(1, 'Alice',  NULL),  -- CEO (no manager)
(2, 'Bob',    1),     -- Reports to Alice
(3, 'Carol',  1),     -- Reports to Alice
(4, 'David',  2),     -- Reports to Bob
(5, 'Eve',    2),     -- Reports to Bob
(6, 'Frank',  3);     -- Reports to Carol
```

**Query: Find all employees under Alice with their level in the hierarchy:**

```sql
WITH RECURSIVE org_tree AS (
    -- Base case: start with Alice (CEO)
    SELECT employee_id, name, manager_id, 0 AS level, 
           CAST(name AS VARCHAR(500)) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive step: find direct reports of current level
    SELECT e.employee_id, e.name, e.manager_id, ot.level + 1,
           CAST(ot.path || ' → ' || e.name AS VARCHAR(500))
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT employee_id, name, level, path
FROM org_tree
ORDER BY level, name;
```

**Result:**

| employee_id | name  | level | path |
|-------------|-------|-------|------|
| 1 | Alice | 0 | Alice |
| 2 | Bob   | 1 | Alice → Bob |
| 3 | Carol | 1 | Alice → Carol |
| 4 | David | 2 | Alice → Bob → David |
| 5 | Eve   | 2 | Alice → Bob → Eve |
| 6 | Frank | 2 | Alice → Carol → Frank |

---

## Example 2: Bill of Materials (BOM)

A classic manufacturing scenario — parts contain sub-parts which contain more sub-parts:

```sql
CREATE TABLE parts (
    part_id INT PRIMARY KEY,
    part_name VARCHAR(100),
    parent_part_id INT,
    quantity INT
);

INSERT INTO parts VALUES
(1, 'Bicycle',     NULL, 1),
(2, 'Frame',       1,    1),
(3, 'Wheel',       1,    2),
(4, 'Spoke',       3,    36),
(5, 'Rim',         3,    1),
(6, 'Tube',        2,    3),
(7, 'Hub',         3,    1);
```

**Query: Explode the full BOM with total quantities:**

```sql
WITH RECURSIVE bom AS (
    -- Base case: top-level product
    SELECT part_id, part_name, parent_part_id, quantity,
           quantity AS total_qty, 0 AS depth
    FROM parts
    WHERE parent_part_id IS NULL

    UNION ALL

    -- Recursive step: sub-parts, multiplying quantities
    SELECT p.part_id, p.part_name, p.parent_part_id, p.quantity,
           p.quantity * b.total_qty AS total_qty, b.depth + 1
    FROM parts p
    JOIN bom b ON p.parent_part_id = b.part_id
)
SELECT part_name, depth, quantity AS unit_qty, total_qty
FROM bom
ORDER BY depth, part_name;
```

**Result:**

| part_name | depth | unit_qty | total_qty |
|-----------|-------|----------|-----------|
| Bicycle   | 0     | 1        | 1         |
| Frame     | 1     | 1        | 1         |
| Wheel     | 1     | 2        | 2         |
| Hub       | 2     | 1        | 2         |
| Rim       | 2     | 1        | 2         |
| Spoke     | 2     | 36       | 72        |
| Tube      | 2     | 3        | 3         |

> **Key Insight:** The `total_qty` multiplies at each level. A Bicycle has 2 Wheels, each Wheel has 36 Spokes, so total Spokes = 72.

---

## Cycle Detection

If your data has accidental circular references (A → B → C → A), the recursive CTE will loop forever. Databases handle this differently:

**PostgreSQL — using a path array:**

```sql
WITH RECURSIVE hierarchy AS (
    SELECT id, parent_id, name,
           ARRAY[id] AS visited,        -- Track visited nodes
           false AS has_cycle
    FROM nodes
    WHERE parent_id IS NULL

    UNION ALL

    SELECT n.id, n.parent_id, n.name,
           h.visited || n.id,
           n.id = ANY(h.visited)        -- Detect if we've been here
    FROM nodes n
    JOIN hierarchy h ON n.parent_id = h.id
    WHERE NOT n.id = ANY(h.visited)     -- Stop if cycle detected
)
SELECT * FROM hierarchy WHERE has_cycle;
```

**SQL Server — using MAXRECURSION:**

```sql
WITH org AS (
    SELECT employee_id, manager_id, 0 AS level
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, o.level + 1
    FROM employees e JOIN org o ON e.manager_id = o.employee_id
    WHERE o.level < 100  -- Safety limit
)
SELECT * FROM org
OPTION (MAXRECURSION 100);  -- Hard stop at 100 iterations
```

---

## Practical Use Cases in Data Engineering

| Use Case | Base Case | Recursive Step |
|----------|-----------|----------------|
| Org chart traversal | CEO (no manager) | Find direct reports |
| Bill of Materials | Top-level product | Find sub-components |
| File system paths | Root directories | Find child directories |
| Category trees | Root categories | Find subcategories |
| Graph path finding | Start node | Explore adjacent nodes |
| Date/number series | Start value | Increment by 1 |

**Generating a date series (useful for filling gaps):**

```sql
WITH RECURSIVE dates AS (
    SELECT DATE '2024-01-01' AS dt
    UNION ALL
    SELECT dt + INTERVAL '1 day'
    FROM dates
    WHERE dt < DATE '2024-01-31'
)
SELECT dt FROM dates;
```

---

## Performance Considerations

1. **Always include a termination condition** — either a depth limit (`WHERE level < N`) or cycle detection
2. **Index the join columns** — the parent/child relationship column is hit on every iteration
3. **Avoid wide paths** — concatenating strings at each level gets expensive for deep trees
4. **Consider materialized path** — for read-heavy hierarchies, store the path (e.g., `/1/3/6/`) as a column for faster queries without recursion

---


## ▶️ Try It Yourself

```sql
-- Recursive CTE: traverse an employee hierarchy
WITH RECURSIVE org_tree AS (
    -- Base case: start with top-level managers (no parent)
    SELECT id, name, manager_id, 1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case: find direct reports of the previous level
    SELECT e.id, e.name, e.manager_id, ot.level + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.id
)
SELECT level, name FROM org_tree ORDER BY level, name;
-- Works for any depth: CEO → VP → Manager → IC
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When would you use a recursive CTE?" — "Anytime I need to traverse hierarchical data — org charts, category trees, bill of materials, or graph paths. The key structure is: base case selects the root nodes, recursive step joins back to find children, and UNION ALL combines all levels. I always add a depth limit as a safety net against circular references."

> **Tip 2:** "How does a recursive CTE execute internally?" — "It's an iterative process, not true recursion. The database executes the base case first, stores results in a working table, then repeatedly executes the recursive step using only the previous iteration's output. It stops when the recursive step returns zero rows. Each iteration's results are appended to the final output."

> **Tip 3:** "What are the performance pitfalls?" — "Three main issues: (1) missing termination conditions can cause infinite loops on cyclic data, (2) unindexed join columns cause full table scans per iteration, and (3) string concatenation for paths is O(n²) over depth. For production hierarchies with frequent reads, I'd consider a materialized path or nested set model instead."
