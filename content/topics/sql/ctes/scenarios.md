---
title: "SQL CTEs - Scenario Questions"
topic: sql
subtopic: ctes
content_type: scenario_question
tags: [sql, ctes, interview, scenarios, recursive]
---

# Scenario Questions — SQL CTEs

<article data-difficulty="junior">

## 🟢 Junior: Find Employees Above Department Average

**Scenario:** You have an `employees` table with `employee_id`, `name`, `department`, and `salary`. Write a query using a CTE to find all employees whose salary exceeds their department's average. Show the employee name, department, salary, and how much above the average they are.

<details>
<summary>💡 Hint</summary>

Create a CTE that computes the average salary per department using `GROUP BY`. Then join the original `employees` table back to this CTE on department, filtering where salary exceeds the average.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH dept_avg AS (
    SELECT 
        department,
        AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
)
SELECT 
    e.name,
    e.department,
    e.salary,
    ROUND(e.salary - da.avg_salary, 2) AS above_avg_by
FROM employees e
JOIN dept_avg da ON e.department = da.department
WHERE e.salary > da.avg_salary
ORDER BY above_avg_by DESC;
```

**Explanation:**
- The CTE computes department averages once, making the main query clean and readable
- Without a CTE, you'd need a correlated subquery (recalculating the average for every row) or a window function
- The `JOIN` + `WHERE` pattern is more readable than nesting subqueries, especially as logic grows

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Year-over-Year Revenue Comparison

**Scenario:** Given an `orders` table with `order_date` and `amount`, write a query that shows each month's revenue alongside the same month from the previous year, plus the year-over-year growth percentage. Use the same CTE referenced twice.

<details>
<summary>💡 Hint</summary>

Create one CTE that aggregates monthly revenue. Then join it to itself — once for the current year and once for the prior year — matching on the month column with a year offset of 1.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH monthly_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS yr,
        EXTRACT(MONTH FROM order_date) AS mo,
        SUM(amount) AS revenue
    FROM orders
    GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
)
SELECT 
    curr.yr AS year,
    curr.mo AS month,
    curr.revenue AS current_revenue,
    prev.revenue AS prior_year_revenue,
    ROUND(
        (curr.revenue - prev.revenue) * 100.0 / NULLIF(prev.revenue, 0), 1
    ) AS yoy_growth_pct
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.mo = prev.mo 
    AND curr.yr = prev.yr + 1
ORDER BY curr.yr, curr.mo;
```

**Explanation:**
- The CTE is computed once and referenced twice (as `curr` and `prev`) — the optimizer typically materializes it once
- `LEFT JOIN` ensures months without prior-year data still appear (with NULL for comparison columns)
- `NULLIF(prev.revenue, 0)` prevents division-by-zero errors when prior year revenue is zero

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Deduplicate Records Keeping Latest Version

**Scenario:** Your `customer_updates` table receives multiple versions of the same customer record (`customer_id`, `name`, `email`, `updated_at`). Write a query using a CTE to keep only the most recent version of each customer.

<details>
<summary>💡 Hint</summary>

Use `ROW_NUMBER()` inside a CTE, partitioned by `customer_id` and ordered by `updated_at DESC`. The row numbered 1 is the most recent version.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH ranked AS (
    SELECT 
        customer_id,
        name,
        email,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY updated_at DESC
        ) AS rn
    FROM customer_updates
)
SELECT customer_id, name, email, updated_at
FROM ranked
WHERE rn = 1
ORDER BY customer_id;
```

**Explanation:**
- `ROW_NUMBER()` with `PARTITION BY customer_id ORDER BY updated_at DESC` assigns 1 to the most recent record for each customer
- Filtering `WHERE rn = 1` keeps only the latest version — this is the standard deduplication pattern in SQL
- This approach is preferred over `GROUP BY` with `MAX(updated_at)` because it lets you easily select all columns without additional joins

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Multi-Step Transformation — Clean, Filter, Aggregate

**Scenario:** You have a `raw_events` table with `event_id`, `user_id`, `event_type`, `event_value` (sometimes negative due to data errors), and `event_date`. Write a pipeline using chained CTEs to: (1) remove invalid rows where `event_value < 0`, (2) keep only events from the last 30 days, (3) compute total value per user per event type.

<details>
<summary>💡 Hint</summary>

Chain three CTEs: the first cleans the data (removes negatives), the second filters by date range, and the third aggregates. Each step references the previous CTE, creating a clear data pipeline.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH cleaned AS (
    -- Step 1: Remove invalid records
    SELECT *
    FROM raw_events
    WHERE event_value >= 0
),
recent AS (
    -- Step 2: Keep only last 30 days
    SELECT *
    FROM cleaned
    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
),
aggregated AS (
    -- Step 3: Aggregate per user per event type
    SELECT 
        user_id,
        event_type,
        COUNT(*) AS event_count,
        SUM(event_value) AS total_value,
        AVG(event_value) AS avg_value
    FROM recent
    GROUP BY user_id, event_type
)
SELECT *
FROM aggregated
ORDER BY total_value DESC;
```

**Explanation:**
- Chained CTEs create a readable, step-by-step transformation pipeline — each step has a single responsibility
- This is conceptually similar to DataFrame transformations in Spark/Pandas but expressed in SQL
- The optimizer typically collapses these CTEs into a single scan with combined predicates, so there's no performance penalty for readability

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Find the Second Highest Salary per Department

**Scenario:** Given an `employees` table with `employee_id`, `name`, `department`, and `salary`, use a CTE with a ranking function to find the employee with the second-highest salary in each department. Handle ties appropriately.

<details>
<summary>💡 Hint</summary>

Use `DENSE_RANK()` inside a CTE to rank salaries per department. `DENSE_RANK` handles ties by assigning the same rank to equal values, so "second highest" means the second distinct salary value.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH ranked AS (
    SELECT 
        employee_id,
        name,
        department,
        salary,
        DENSE_RANK() OVER (
            PARTITION BY department 
            ORDER BY salary DESC
        ) AS salary_rank
    FROM employees
)
SELECT employee_id, name, department, salary
FROM ranked
WHERE salary_rank = 2
ORDER BY department;
```

**Explanation:**
- `DENSE_RANK()` assigns the same rank to tied salaries (e.g., two people earning $100K both get rank 1, and the next salary gets rank 2)
- Using `RANK()` instead would skip rank 2 if there's a tie at rank 1, potentially returning no results
- Using `ROW_NUMBER()` would arbitrarily pick one person from a tie, which may not be desired
- The CTE makes it easy to test the ranking logic independently before filtering

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Consecutive Days of User Activity

**Scenario:** Given a `user_logins` table (`user_id`, `login_date` — one row per user per active day), use CTEs to find each user's longest consecutive activity streak. Show clear, step-by-step logic with multiple CTEs.

<details>
<summary>💡 Hint</summary>

The "date minus row_number" trick groups consecutive dates. For dates Jan 1, 2, 3 with row numbers 1, 2, 3, subtracting gives the same result (Dec 31) — creating a group key. Gaps break the pattern and produce a different group value.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH numbered AS (
    -- Step 1: Assign sequential numbers per user
    SELECT 
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM user_logins
),
grouped AS (
    -- Step 2: Consecutive dates minus sequential row_number = same group
    SELECT 
        user_id,
        login_date,
        login_date - (rn * INTERVAL '1 day') AS streak_group
    FROM numbered
),
streak_lengths AS (
    -- Step 3: Measure each streak
    SELECT 
        user_id,
        streak_group,
        COUNT(*) AS streak_length,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end
    FROM grouped
    GROUP BY user_id, streak_group
)
-- Step 4: Get longest streak per user
SELECT 
    user_id,
    MAX(streak_length) AS longest_streak
FROM streak_lengths
GROUP BY user_id
ORDER BY longest_streak DESC;
```

**Explanation:**
- Each CTE represents one logical step, making the date-minus-rownumber trick easy to understand and debug
- The grouping works because consecutive dates (1, 2, 3) minus sequential numbers (1, 2, 3) yield the same constant — any gap resets the constant
- CTEs here improve readability over deeply nested subqueries; the optimizer handles them equivalently

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Recursive CTE — Generate Category Breadcrumb Paths

**Scenario:** You have a `categories` table with `id`, `name`, and `parent_id` (self-referencing hierarchy where root categories have `parent_id IS NULL`). Write a recursive CTE that builds the full breadcrumb path for every category (e.g., "Electronics > Phones > Smartphones").

<details>
<summary>💡 Hint</summary>

Start the recursion with root categories (anchor: `WHERE parent_id IS NULL`). In the recursive part, join children to already-processed parents and concatenate names with a separator. Add a depth limit to prevent infinite loops from bad data.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE category_path AS (
    -- Anchor: root categories (no parent)
    SELECT 
        id,
        name,
        parent_id,
        name::TEXT AS breadcrumb,
        1 AS depth
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: append child to parent's breadcrumb
    SELECT 
        c.id,
        c.name,
        c.parent_id,
        cp.breadcrumb || ' > ' || c.name,
        cp.depth + 1
    FROM categories c
    JOIN category_path cp ON c.parent_id = cp.id
    WHERE cp.depth < 10  -- Safety limit prevents infinite recursion
)
SELECT id, name, breadcrumb, depth
FROM category_path
ORDER BY breadcrumb;
```

**Explanation:**
- The anchor member seeds the recursion with top-level categories (no parent)
- Each recursive iteration joins unprocessed children to their already-resolved parents, building the path string
- `WHERE cp.depth < 10` is a safety valve — without it, circular references in data would cause infinite recursion
- This pattern is used for navigation menus, file system paths, org charts, and any tree-structured data

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Funnel Analysis with Drop-Off Rates

**Scenario:** Your e-commerce funnel has steps: `page_view` → `add_to_cart` → `begin_checkout` → `payment` → `purchase`. Given an `events` table (`user_id`, `event_type`, `event_timestamp`), use multiple CTEs to calculate the conversion rate and drop-off count at each step.

<details>
<summary>💡 Hint</summary>

Define funnel steps in one CTE, count distinct users per step in another, then use `LAG()` to compare each step to the previous one. Build it step-by-step with chained CTEs for clarity.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH funnel_steps AS (
    SELECT unnest AS step_name, ordinality AS step_order
    FROM unnest(ARRAY['page_view','add_to_cart','begin_checkout','payment','purchase']) 
    WITH ORDINALITY
),
step_counts AS (
    SELECT 
        fs.step_order,
        fs.step_name,
        COUNT(DISTINCT e.user_id) AS users_at_step
    FROM funnel_steps fs
    LEFT JOIN events e ON e.event_type = fs.step_name
    GROUP BY fs.step_order, fs.step_name
),
with_metrics AS (
    SELECT 
        step_order,
        step_name,
        users_at_step,
        LAG(users_at_step) OVER (ORDER BY step_order) AS prev_step_users,
        FIRST_VALUE(users_at_step) OVER (ORDER BY step_order) AS top_of_funnel
    FROM step_counts
)
SELECT 
    step_order,
    step_name,
    users_at_step,
    prev_step_users - users_at_step AS users_dropped,
    ROUND(users_at_step * 100.0 / NULLIF(prev_step_users, 0), 1) AS step_conversion_pct,
    ROUND(users_at_step * 100.0 / NULLIF(top_of_funnel, 0), 1) AS overall_conversion_pct
FROM with_metrics
ORDER BY step_order;
```

**Explanation:**
- Each CTE handles one concern: define steps → count users → compute metrics — making the logic auditable
- `LAG()` pulls the previous step's count without self-joining
- `FIRST_VALUE()` grabs the top-of-funnel count for overall conversion calculation
- This pattern is reusable: swap the step array for any funnel (onboarding, feature adoption, etc.)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Find Circular References in a Self-Referencing Table

**Scenario:** You have a `nodes` table with `id` and `parent_id` that should form a tree. However, bad data may have introduced circular references (A→B→C→A). Write a recursive CTE that detects all nodes involved in cycles.

<details>
<summary>💡 Hint</summary>

Traverse the hierarchy recursively while tracking the path as an array. A cycle is detected when you encounter a node that already exists in the current path. Use `ANY()` or array containment to check for revisits.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE traversal AS (
    -- Start from every node
    SELECT 
        id,
        parent_id,
        ARRAY[id] AS path,
        FALSE AS is_cycle
    FROM nodes
    
    UNION ALL
    
    -- Follow parent_id links
    SELECT 
        t.id,
        n.parent_id,
        t.path || n.id,
        n.id = ANY(t.path) AS is_cycle
    FROM traversal t
    JOIN nodes n ON t.parent_id = n.id
    WHERE NOT n.id = ANY(t.path)  -- Stop if cycle detected
      AND array_length(t.path, 1) < 100  -- Safety limit
)
SELECT DISTINCT unnest(path) AS node_in_cycle
FROM traversal
WHERE is_cycle = TRUE
ORDER BY node_in_cycle;
```

**Explanation:**
- The recursive CTE starts from each node and follows `parent_id` links upward, accumulating the path in an array
- If a node already appears in the current path (`= ANY(path)`), we've found a cycle
- `WHERE NOT n.id = ANY(t.path)` terminates recursion at the cycle point to prevent infinite loops
- The final `unnest(path)` with `DISTINCT` extracts all individual nodes that participate in any cycle

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Pivot Monthly Data into Columns Using CTE

**Scenario:** Given a `monthly_sales` table with `product_id`, `sale_month` (1-12), and `revenue`, write a query using CTEs to pivot the data so each product has one row with 12 monthly columns (jan_revenue, feb_revenue, ... dec_revenue).

<details>
<summary>💡 Hint</summary>

Use conditional aggregation (`SUM(CASE WHEN month = N THEN revenue END)`) inside a CTE or directly. The CTE can first filter/clean data, then the main query performs the pivot with CASE expressions for each month.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH monthly_data AS (
    SELECT 
        product_id,
        sale_month,
        SUM(revenue) AS total_revenue
    FROM monthly_sales
    GROUP BY product_id, sale_month
)
SELECT 
    product_id,
    SUM(CASE WHEN sale_month = 1 THEN total_revenue END) AS jan_revenue,
    SUM(CASE WHEN sale_month = 2 THEN total_revenue END) AS feb_revenue,
    SUM(CASE WHEN sale_month = 3 THEN total_revenue END) AS mar_revenue,
    SUM(CASE WHEN sale_month = 4 THEN total_revenue END) AS apr_revenue,
    SUM(CASE WHEN sale_month = 5 THEN total_revenue END) AS may_revenue,
    SUM(CASE WHEN sale_month = 6 THEN total_revenue END) AS jun_revenue,
    SUM(CASE WHEN sale_month = 7 THEN total_revenue END) AS jul_revenue,
    SUM(CASE WHEN sale_month = 8 THEN total_revenue END) AS aug_revenue,
    SUM(CASE WHEN sale_month = 9 THEN total_revenue END) AS sep_revenue,
    SUM(CASE WHEN sale_month = 10 THEN total_revenue END) AS oct_revenue,
    SUM(CASE WHEN sale_month = 11 THEN total_revenue END) AS nov_revenue,
    SUM(CASE WHEN sale_month = 12 THEN total_revenue END) AS dec_revenue
FROM monthly_data
GROUP BY product_id
ORDER BY product_id;
```

**Explanation:**
- The CTE pre-aggregates in case there are multiple rows per product per month, keeping the pivot logic clean
- `CASE WHEN ... END` inside `SUM()` is the standard cross-tab/pivot technique in ANSI SQL
- Months with no data return NULL (not 0) — wrap in `COALESCE(..., 0)` if zeros are preferred
- For dynamic pivoting (unknown number of columns), you'd need dynamic SQL or a tool like `crosstab()` in PostgreSQL

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Recursive CTE — Find All Downstream Table Dependencies

**Scenario:** You have a `table_dependencies` table (`table_name`, `depends_on`) representing your data warehouse lineage. Given a corrupted source table, find ALL downstream tables affected transitively (tables depending on tables that depend on the bad source). Include dependency depth and the full dependency path.

<details>
<summary>💡 Hint</summary>

Use a recursive CTE anchored on direct dependents of the corrupted table. Recursively join to find tables depending on already-identified affected tables. Track the path as an array to detect and prevent cycles.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE affected AS (
    -- Anchor: direct dependents of corrupted table
    SELECT 
        table_name,
        depends_on,
        1 AS depth,
        ARRAY['raw_orders', table_name] AS dep_path
    FROM table_dependencies
    WHERE depends_on = 'raw_orders'
    
    UNION ALL
    
    -- Recursive: find tables depending on already-affected tables
    SELECT 
        td.table_name,
        td.depends_on,
        a.depth + 1,
        a.dep_path || td.table_name
    FROM table_dependencies td
    JOIN affected a ON td.depends_on = a.table_name
    WHERE NOT td.table_name = ANY(a.dep_path)  -- Cycle prevention
      AND a.depth < 20  -- Safety limit
)
SELECT DISTINCT ON (table_name)
    table_name AS affected_table,
    depth AS distance_from_source,
    array_to_string(dep_path, ' → ') AS dependency_path
FROM affected
ORDER BY table_name, depth;
```

**Explanation:**
- The anchor finds all tables that directly depend on the corrupted source
- Each recursive step discovers the next layer of dependencies (tables depending on already-identified tables)
- `NOT table_name = ANY(dep_path)` prevents infinite loops from circular dependencies in the graph
- `DISTINCT ON (table_name) ... ORDER BY depth` keeps only the shortest path to each affected table
- This is the standard pattern for graph traversal in SQL — used for data lineage, impact analysis, and dependency resolution

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Bill of Materials Explosion with Quantity Multiplication

**Scenario:** You have a `bom` (bill of materials) table with `parent_part`, `child_part`, and `quantity` (how many child parts are needed per parent). Given a finished product, recursively explode the full parts list with cumulative quantities. For example, if Product A needs 2× Assembly B, and Assembly B needs 3× Part C, then Product A needs 6× Part C total.

<details>
<summary>💡 Hint</summary>

In the recursive step, multiply the current quantity by the parent's cumulative quantity. The anchor starts with direct children of the target product (quantity as-is), and each level multiplies further.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE exploded_bom AS (
    -- Anchor: direct components of the finished product
    SELECT 
        parent_part,
        child_part,
        quantity AS unit_qty,
        quantity AS cumulative_qty,
        1 AS bom_level,
        ARRAY[parent_part, child_part] AS assembly_path
    FROM bom
    WHERE parent_part = 'FINISHED_PRODUCT_A'
    
    UNION ALL
    
    -- Recursive: sub-components with multiplied quantities
    SELECT 
        b.parent_part,
        b.child_part,
        b.quantity,
        eb.cumulative_qty * b.quantity,
        eb.bom_level + 1,
        eb.assembly_path || b.child_part
    FROM bom b
    JOIN exploded_bom eb ON b.parent_part = eb.child_part
    WHERE NOT b.child_part = ANY(eb.assembly_path)  -- Cycle guard
      AND eb.bom_level < 15
)
SELECT 
    child_part AS part,
    SUM(cumulative_qty) AS total_qty_needed,
    MIN(bom_level) AS shallowest_level,
    MAX(bom_level) AS deepest_level,
    COUNT(*) AS appears_in_n_assemblies
FROM exploded_bom
GROUP BY child_part
ORDER BY total_qty_needed DESC;
```

**Explanation:**
- `cumulative_qty * b.quantity` propagates the multiplication through each level — this is the core BOM explosion logic
- A part appearing in multiple sub-assemblies will have multiple rows; `SUM(cumulative_qty)` in the final aggregation gives the total needed
- The cycle guard prevents infinite recursion if a part incorrectly references itself as a component
- This is a classic manufacturing/ERP query pattern used in inventory planning and cost roll-up calculations

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Incremental CDC Processing Using CTE + MERGE

**Scenario:** You receive change data capture (CDC) events in a `cdc_events` table (`record_id`, `operation` [INSERT/UPDATE/DELETE], `payload` JSONB, `event_timestamp`). Write a query using CTEs and MERGE to apply these changes to a `target_customers` table, processing only events since the last watermark. Handle out-of-order events by keeping only the latest per record.

<details>
<summary>💡 Hint</summary>

Use a CTE to deduplicate CDC events per `record_id` (keeping the latest by timestamp). Then use `MERGE` (or `INSERT ... ON CONFLICT` in PostgreSQL) to apply inserts, updates, and deletes in a single statement based on the operation type.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH latest_changes AS (
    -- Deduplicate: keep only the most recent event per record
    SELECT DISTINCT ON (record_id)
        record_id,
        operation,
        payload,
        event_timestamp
    FROM cdc_events
    WHERE event_timestamp > (SELECT watermark FROM processing_state WHERE job_name = 'customer_sync')
    ORDER BY record_id, event_timestamp DESC
)
MERGE INTO target_customers t
USING latest_changes s ON t.customer_id = s.record_id
WHEN MATCHED AND s.operation = 'DELETE' THEN
    DELETE
WHEN MATCHED AND s.operation = 'UPDATE' THEN
    UPDATE SET 
        name = s.payload->>'name',
        email = s.payload->>'email',
        updated_at = s.event_timestamp
WHEN NOT MATCHED AND s.operation IN ('INSERT', 'UPDATE') THEN
    INSERT (customer_id, name, email, created_at, updated_at)
    VALUES (
        s.record_id,
        s.payload->>'name',
        s.payload->>'email',
        s.event_timestamp,
        s.event_timestamp
    );

-- After successful MERGE, advance the watermark
UPDATE processing_state 
SET watermark = (SELECT MAX(event_timestamp) FROM latest_changes)
WHERE job_name = 'customer_sync';
```

**Explanation:**
- `DISTINCT ON (record_id) ORDER BY event_timestamp DESC` deduplicates out-of-order events, keeping only the final state per record
- The watermark pattern (`WHERE event_timestamp > watermark`) ensures only new events are processed each run — this is idempotent
- `MERGE` handles all three operations (INSERT/UPDATE/DELETE) in a single pass, which is more efficient than separate statements
- This pattern is the foundation of incremental ETL pipelines — it processes only deltas rather than full reloads

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build a Cohort Retention Matrix Using CTEs

**Scenario:** Given a `user_events` table (`user_id`, `event_date`), build a cohort retention matrix: group users by their first-activity month (acquisition cohort), then for each subsequent month, calculate what percentage of the cohort was still active. The output should be a matrix with cohort months as rows and retention periods (Month 0, Month 1, ... Month 6) as columns.

<details>
<summary>💡 Hint</summary>

First CTE: determine each user's cohort (month of first activity). Second CTE: join all activity back to the cohort to compute month offsets. Third CTE: count distinct active users per cohort per offset. Finally, pivot into a matrix using conditional aggregation.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH user_cohorts AS (
    -- Determine each user's acquisition cohort
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(event_date)) AS cohort_month
    FROM user_events
    GROUP BY user_id
),
activity_with_cohort AS (
    -- Join all activity with cohort info, compute month offset
    SELECT 
        uc.cohort_month,
        uc.user_id,
        DATE_TRUNC('month', ue.event_date) AS activity_month,
        EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', ue.event_date), uc.cohort_month)) * 12
        + EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', ue.event_date), uc.cohort_month)) AS month_offset
    FROM user_events ue
    JOIN user_cohorts uc ON ue.user_id = uc.user_id
),
cohort_counts AS (
    -- Count distinct users per cohort per period
    SELECT 
        cohort_month,
        month_offset,
        COUNT(DISTINCT user_id) AS active_users
    FROM activity_with_cohort
    WHERE month_offset BETWEEN 0 AND 6
    GROUP BY cohort_month, month_offset
),
cohort_sizes AS (
    SELECT cohort_month, active_users AS cohort_size
    FROM cohort_counts
    WHERE month_offset = 0
)
SELECT 
    cc.cohort_month,
    cs.cohort_size,
    ROUND(MAX(CASE WHEN month_offset = 0 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_0_pct,
    ROUND(MAX(CASE WHEN month_offset = 1 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_1_pct,
    ROUND(MAX(CASE WHEN month_offset = 2 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_2_pct,
    ROUND(MAX(CASE WHEN month_offset = 3 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_3_pct,
    ROUND(MAX(CASE WHEN month_offset = 4 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_4_pct,
    ROUND(MAX(CASE WHEN month_offset = 5 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_5_pct,
    ROUND(MAX(CASE WHEN month_offset = 6 THEN active_users END) * 100.0 / cs.cohort_size, 1) AS month_6_pct
FROM cohort_counts cc
JOIN cohort_sizes cs ON cc.cohort_month = cs.cohort_month
GROUP BY cc.cohort_month, cs.cohort_size
ORDER BY cc.cohort_month;
```

**Explanation:**
- The CTE pipeline follows a logical progression: identify cohorts → compute offsets → count users → pivot into matrix
- Month 0 is always 100% (all users are active in their acquisition month by definition)
- The retention curve (how fast percentages drop) reveals product health — healthy products have flatter curves
- This is a fundamental product analytics query that powers retention dashboards at virtually every SaaS company

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Recursive Date Spine and Fill Gaps in Time Series

**Scenario:** Your `daily_metrics` table has gaps (missing dates). Write a query that: (1) generates a complete date spine using a recursive CTE covering the full range of your data, (2) left-joins actual data onto the spine, and (3) fills gaps using the last known value (forward-fill / LOCF — Last Observation Carried Forward).

<details>
<summary>💡 Hint</summary>

Use a recursive CTE to generate consecutive dates from MIN(date) to MAX(date). Left-join the actual data onto this spine. For forward-fill, use `LAST_VALUE(value IGNORE NULLS)` or a conditional window function to carry the most recent non-null value forward.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH RECURSIVE date_range AS (
    SELECT MIN(metric_date) AS dt FROM daily_metrics
    UNION ALL
    SELECT dt + INTERVAL '1 day'
    FROM date_range
    WHERE dt < (SELECT MAX(metric_date) FROM daily_metrics)
),
spine_with_data AS (
    SELECT 
        dr.dt AS metric_date,
        dm.value AS raw_value,
        dm.value IS NOT NULL AS has_data
    FROM date_range dr
    LEFT JOIN daily_metrics dm ON dr.dt = dm.metric_date
),
with_groups AS (
    -- Create groups for forward-fill: each non-null value starts a new group
    SELECT 
        metric_date,
        raw_value,
        has_data,
        COUNT(raw_value) OVER (
            ORDER BY metric_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS fill_group
    FROM spine_with_data
)
SELECT 
    metric_date,
    raw_value,
    FIRST_VALUE(raw_value) OVER (
        PARTITION BY fill_group 
        ORDER BY metric_date
    ) AS filled_value,
    NOT has_data AS was_filled
FROM with_groups
ORDER BY metric_date;
```

**Explanation:**
- The recursive CTE generates every date between the min and max of the dataset — this is the "date spine"
- `LEFT JOIN` preserves all spine dates, with NULL values for missing days
- The forward-fill uses the `COUNT(raw_value)` trick: `COUNT` ignores NULLs, so it only increments at non-null values, creating group IDs for fill segments
- `FIRST_VALUE` within each group pulls the non-null value at the group's start, effectively carrying it forward
- In engines supporting `IGNORE NULLS` (Snowflake, BigQuery), you can simplify with `LAST_VALUE(value IGNORE NULLS) OVER (ORDER BY date)`

</details>

</article>
