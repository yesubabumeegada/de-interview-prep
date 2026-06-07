---
title: "SQL CTEs - Intermediate"
topic: sql
subtopic: ctes
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [sql, ctes, recursive-cte, hierarchies, graph-traversal, materialization]
---

# SQL CTEs — Intermediate Concepts

## Recursive CTEs — Traversing Hierarchies

A recursive CTE references itself, enabling iteration over hierarchical or graph-structured data without knowing the depth in advance.

### Syntax Structure

```sql
WITH RECURSIVE cte_name AS (
    -- ANCHOR: Starting rows (base case)
    SELECT ... FROM ... WHERE ...

    UNION ALL

    -- RECURSIVE: Join back to CTE (iterative step)
    SELECT ... FROM ... JOIN cte_name ON ...
    WHERE termination_condition
)
SELECT * FROM cte_name;
```

**How it executes:**
1. Run the anchor query → produces the initial row set
2. Run the recursive query using the previous iteration's results
3. Repeat step 2 until it produces zero new rows (or hits the termination condition)
4. UNION ALL of all iterations = final result

---

### Example 1: Org Hierarchy (Employee → Manager Chain)

```sql
WITH RECURSIVE org_tree AS (
    -- Anchor: start from the CEO (no manager)
    SELECT 
        emp_id, name, manager_id, 
        1 AS level,
        name AS chain
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive: find direct reports of the previous level
    SELECT 
        e.emp_id, e.name, e.manager_id,
        ot.level + 1,
        ot.chain || ' > ' || e.name
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.emp_id
    WHERE ot.level < 10  -- Safety: prevent infinite loops
)
SELECT level, name, chain
FROM org_tree
ORDER BY level, name;
```

**Result:**

| level | name | chain |
|-------|------|-------|
| 1 | Alice | Alice |
| 1 | Eve | Eve |
| 2 | Bob | Alice > Bob |
| 2 | Charlie | Alice > Charlie |
| 2 | Diana | Eve > Diana |
| 2 | Frank | Eve > Frank |
| 2 | Grace | Eve > Grace |

> **Safety:** Always include a `WHERE level < N` or row count check in the recursive part. Without it, circular references (data bugs) cause infinite loops.

---

### Example 2: Date Series Generation

```sql
-- Generate all dates in a range (useful for filling gaps)
WITH RECURSIVE date_series AS (
    -- Anchor: start date
    SELECT DATE '2024-01-01' AS dt
    
    UNION ALL
    
    -- Recursive: add one day
    SELECT dt + INTERVAL '1 day'
    FROM date_series
    WHERE dt < DATE '2024-01-31'
)
SELECT dt FROM date_series;
```

**Result:** 31 rows, one per date in January 2024.

**Use case:** Join this with sales data to fill in zero-sales days in a report (instead of having gaps).

```sql
-- Fill missing dates with zero
WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' AS dt
    UNION ALL
    SELECT dt + INTERVAL '1 day' FROM date_series WHERE dt < '2024-01-31'
),
daily_sales AS (
    SELECT sale_date, SUM(amount) AS revenue
    FROM orders
    WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
    GROUP BY sale_date
)
SELECT 
    ds.dt AS date,
    COALESCE(s.revenue, 0) AS revenue
FROM date_series ds
LEFT JOIN daily_sales s ON ds.dt = s.sale_date
ORDER BY ds.dt;
```

---

### Example 3: Bill of Materials (BOM) Explosion

```sql
-- Find all components needed to build a product (nested assemblies)
WITH RECURSIVE bom AS (
    -- Anchor: top-level product
    SELECT 
        component_id, component_name, quantity, 
        1 AS level,
        ARRAY[parent_id] AS path
    FROM bill_of_materials
    WHERE parent_id = 'PRODUCT-X'
    
    UNION ALL
    
    -- Recursive: sub-components of each component
    SELECT 
        b.component_id, b.component_name, 
        b.quantity * bom.quantity AS total_quantity,  -- Multiply quantities down
        bom.level + 1,
        bom.path || b.parent_id
    FROM bill_of_materials b
    JOIN bom ON b.parent_id = bom.component_id
    WHERE NOT b.component_id = ANY(bom.path)  -- Prevent cycles
)
SELECT level, component_name, total_quantity
FROM bom
ORDER BY level, component_name;
```

---

## CTE Materialization

Some databases let you control whether a CTE is computed once (materialized) or inlined into the main query:

### PostgreSQL

```sql
-- MATERIALIZED: compute once, store in temp
WITH cte AS MATERIALIZED (
    SELECT ... -- expensive computation
)
SELECT * FROM cte WHERE ...;

-- NOT MATERIALIZED: optimizer may inline it
WITH cte AS NOT MATERIALIZED (
    SELECT ...
)
SELECT * FROM cte WHERE ...;
```

### When Materialization Helps

| Scenario | Materialize? | Why |
|----------|-------------|-----|
| CTE referenced multiple times | YES | Avoids recomputing |
| CTE with expensive aggregation | YES | Compute once |
| CTE referenced once with selective WHERE | NO | Let optimizer push filters in |
| CTE in a loop/recursive context | Auto | Engine decides |

> **Note:** In MySQL, CTEs are always materialized. In SQL Server and Snowflake, the optimizer decides. In PostgreSQL 12+, you have explicit control.

---

## CTEs for Data Pipeline Steps

A common production pattern — build a multi-step transformation as named CTEs:

```sql
WITH 
-- Step 1: Raw data with basic cleaning
cleaned AS (
    SELECT 
        order_id,
        customer_id,
        TRIM(LOWER(product_name)) AS product_name,
        amount,
        order_date,
        COALESCE(region, 'Unknown') AS region
    FROM raw_orders
    WHERE order_date = CURRENT_DATE - 1
      AND amount > 0
      AND customer_id IS NOT NULL
),

-- Step 2: Deduplicate (keep latest per order)
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
    FROM cleaned
),

-- Step 3: Enrich with customer segment
enriched AS (
    SELECT 
        d.order_id,
        d.customer_id,
        d.product_name,
        d.amount,
        d.order_date,
        d.region,
        c.segment AS customer_segment,
        c.lifetime_value
    FROM deduped d
    JOIN dim_customer c ON d.customer_id = c.customer_id AND c.is_current = TRUE
    WHERE d.rn = 1
),

-- Step 4: Aggregate by segment
segment_summary AS (
    SELECT 
        customer_segment,
        region,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_order_value
    FROM enriched
    GROUP BY customer_segment, region
)

-- Final output
SELECT * FROM segment_summary
ORDER BY total_revenue DESC;
```

> **This pattern works like a mini-ETL within a single query:** clean → dedupe → enrich → aggregate. Each step is testable independently by changing the final SELECT.

---

## CTEs vs Temp Tables vs Views

| Feature | CTE | Temp Table | View |
|---------|-----|-----------|------|
| Scope | Single query | Session | Permanent |
| Storage | None (usually inlined) | Physical table in tempdb | None (just stored SQL) |
| Indexable | No | Yes | No (but underlying tables are) |
| Reusable across queries | No | Yes (within session) | Yes |
| Good for | Multi-step single queries | Multi-query workflows | Shared logic |
| Performance | Optimizer decides | Guaranteed materialization | Optimizer decides |

**Decision guide:**
- Need result in ONE query → CTE
- Need result across MULTIPLE queries in same script → Temp table
- Need result available to ALL users permanently → View

---

## Interview Tips

> **Tip 1:** "Write a recursive CTE" is a common senior-level question. Practice the org-hierarchy pattern until it's automatic: anchor (root nodes with no parent) → recursive (join current level's IDs to the parent column) → termination (level < N).

> **Tip 2:** When asked about CTE vs temp table: "I use CTEs for single-query readability and recursive traversal. I use temp tables when I need to reuse results across multiple statements in a script, or when I need indexes on intermediate results for performance."

> **Tip 3:** Name CTEs like you name functions — descriptively. `daily_revenue`, `ranked_products`, `active_customers` are good. `cte1`, `temp`, `sub` are bad. The names ARE the documentation.
