---
title: "SQL Joins - Scenario Questions"
topic: sql
subtopic: joins
content_type: scenario_question
tags: [sql, joins, interview, scenarios]
---

# Scenario Questions — SQL Joins

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Find Customers With No Orders

**Scenario:** Given `customers(customer_id, name, signup_date)` and `orders(order_id, customer_id, amount, order_date)`, find all customers who have never placed an order.

<details>
<summary>💡 Hint</summary>
You need rows from customers that have NO match in orders. Think LEFT JOIN + IS NULL or NOT EXISTS.
</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT c.customer_id, c.name, c.signup_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL
ORDER BY c.signup_date;
```

**Explanation:**
- LEFT JOIN keeps ALL customers, fills NULL for those without orders
- `WHERE o.customer_id IS NULL` filters to only the non-matching ones
- This is the "anti-join" pattern — one of the most common interview questions

**Follow-up:** "Which is faster: LEFT JOIN + IS NULL or NOT EXISTS?" → NOT EXISTS can short-circuit (stops at first match), often faster on indexed tables.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Total Revenue Per Category (Including Empty Categories)

**Scenario:** Tables: `products(product_id, name, category_id)`, `categories(category_id, category_name)`, `order_items(order_id, product_id, quantity, unit_price)`. Show each category's total revenue. Categories with no sales should show 0.

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    c.category_name,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS order_count
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY c.category_name
ORDER BY total_revenue DESC;
```

**Explanation:**
- Double LEFT JOIN ensures categories with no products AND products with no orders still appear
- `COALESCE(..., 0)` converts NULL aggregates to 0 for empty categories
- Without LEFT JOIN, empty categories would disappear from results

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Identify the Join Type

**Scenario:** For each business question below, which join type would you use and why?
1. "Show all employees with their department names"
2. "Find departments with no employees"
3. "List customers alongside their orders (only those who have orders)"
4. "Compare records between two systems to find mismatches"
5. "Generate all possible product-store combinations"

<details>
<summary>✅ Solution</summary>

| Question | Join Type | Reasoning |
|----------|-----------|-----------|
| 1. All employees + dept names | LEFT JOIN (employees LEFT JOIN departments) | Want ALL employees even if dept is NULL |
| 2. Departments with no employees | LEFT JOIN departments → employees WHERE emp IS NULL | Anti-join: all depts, filter to unmatched |
| 3. Customers with orders only | INNER JOIN | Only want rows that exist in BOTH tables |
| 4. Compare two systems | FULL OUTER JOIN | Need to find records missing in EITHER direction |
| 5. All product-store combos | CROSS JOIN | Cartesian product of all combinations |

**Key insight:** The join type is determined by what happens to NON-MATCHING rows.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Fix the Duplicate Rows

**Scenario:** This query returns 15 rows instead of the expected 3 (one per customer). Why, and how do you fix it?

```sql
SELECT c.name, o.order_id, o.amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;
-- Expected: 3 rows (one per customer)
-- Actual: 15 rows!
```

<details>
<summary>✅ Solution</summary>

**Why:** The join is 1:many. Each customer has ~5 orders, so 3 customers × 5 orders = 15 rows. This is correct join behavior — not a bug.

**Fix depends on what you actually want:**

```sql
-- Option 1: One row per customer with aggregated order data
SELECT c.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.name;

-- Option 2: Only the most recent order per customer
SELECT c.name, o.order_id, o.amount
FROM customers c
JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) o ON c.customer_id = o.customer_id AND o.rn = 1;
```

**Key lesson:** Always consider cardinality (1:1, 1:many, many:many) before joining.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Self Join — Employees and Managers

**Scenario:** Given `employees(emp_id, name, salary, manager_id)` where `manager_id` references `emp_id`, write a query showing each employee's name alongside their manager's name. Include employees who have no manager (CEO).

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    e.name AS employee_name,
    e.salary,
    COALESCE(m.name, 'No Manager (CEO)') AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id
ORDER BY e.name;
```

**Explanation:**
- This is a self-join: same table used twice with different aliases
- LEFT JOIN ensures the CEO (manager_id IS NULL) still appears
- COALESCE handles the NULL manager case with a friendly label

</details>

</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Detect Data Gaps Between Systems

**Scenario:** You migrated from `legacy_orders` to `new_orders`. Both have `order_id` and `amount`. Produce a reconciliation report showing: orders only in legacy, orders only in new system, and orders in both but with different amounts.

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    COALESCE(l.order_id, n.order_id) AS order_id,
    CASE
        WHEN n.order_id IS NULL THEN 'LEGACY_ONLY'
        WHEN l.order_id IS NULL THEN 'NEW_ONLY'
        WHEN l.amount != n.amount THEN 'AMOUNT_MISMATCH'
        ELSE 'MATCHED'
    END AS status,
    l.amount AS legacy_amount,
    n.amount AS new_amount,
    ABS(COALESCE(n.amount, 0) - COALESCE(l.amount, 0)) AS discrepancy
FROM legacy_orders l
FULL OUTER JOIN new_orders n ON l.order_id = n.order_id
WHERE l.order_id IS NULL 
   OR n.order_id IS NULL 
   OR l.amount != n.amount
ORDER BY discrepancy DESC;
```

**Explanation:**
- FULL OUTER JOIN captures mismatches in BOTH directions
- CASE classifies each type of discrepancy
- WHERE filters out the "MATCHED" rows (only show problems)

**Follow-up:** "How would you automate this as a daily data quality check?" → Schedule as an Airflow task, alert if count > threshold.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Employees Earning More Than Their Manager

**Scenario:** Using `employees(id, name, salary, manager_id)`, find all employees who earn more than their direct manager. Show the salary gap.

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    e.name AS employee_name,
    e.salary AS employee_salary,
    m.name AS manager_name,
    m.salary AS manager_salary,
    e.salary - m.salary AS overpay_gap
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary
ORDER BY overpay_gap DESC;
```

**Explanation:**
- Self-join: employee table as both "employee" (e) and "manager" (m)
- INNER JOIN: only employees who HAVE a manager (excludes CEO)
- WHERE filters to the overpaying cases

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Fan-Out Prevention

**Scenario:** A customer has 10 orders and 5 returns. Joining both to customers produces 50 rows per customer (10 × 5). Write a query showing: customer name, total orders, total amount, total returns, total refunded — with correct numbers.

<details>
<summary>💡 Hint</summary>
Pre-aggregate each relationship independently BEFORE joining to avoid the Cartesian explosion.
</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH order_summary AS (
    SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total_spent
    FROM orders GROUP BY customer_id
),
return_summary AS (
    SELECT customer_id, COUNT(*) AS return_count, SUM(refund_amount) AS total_refunded
    FROM returns GROUP BY customer_id
)
SELECT 
    c.name,
    COALESCE(os.order_count, 0) AS orders,
    COALESCE(os.total_spent, 0) AS revenue,
    COALESCE(rs.return_count, 0) AS returns,
    COALESCE(rs.total_refunded, 0) AS refunds,
    COALESCE(os.total_spent, 0) - COALESCE(rs.total_refunded, 0) AS net_revenue
FROM customers c
LEFT JOIN order_summary os ON c.customer_id = os.customer_id
LEFT JOIN return_summary rs ON c.customer_id = rs.customer_id
ORDER BY net_revenue DESC;
```

**Explanation:**
- Aggregating BEFORE joining ensures 1 row per customer per CTE
- Left joins become 1:1 (no fan-out)
- Each customer gets exactly 1 output row with correct totals

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Latest Order Per Customer

**Scenario:** Show each customer's name and their most recent order (date, amount, product). Only include customers who have placed at least one order. If a customer has multiple orders on the same date, show the one with the highest amount.

<details>
<summary>✅ Solution</summary>

```sql
WITH ranked_orders AS (
    SELECT 
        customer_id, order_date, amount, product_name,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date DESC, amount DESC
        ) AS rn
    FROM orders
)
SELECT c.name, ro.order_date, ro.amount, ro.product_name
FROM customers c
INNER JOIN ranked_orders ro ON c.customer_id = ro.customer_id AND ro.rn = 1
ORDER BY ro.order_date DESC;
```

**Explanation:**
- ROW_NUMBER with PARTITION BY customer_id picks the "best" row per customer
- ORDER BY date DESC, amount DESC handles tiebreaking
- INNER JOIN ensures only customers with orders appear
- Filtering rn = 1 in the join condition is cleaner than a WHERE clause

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: SCD Type 2 Point-in-Time Join

**Scenario:** `fact_orders` has `order_date`. `dim_customer` is SCD Type 2 with `effective_from`, `effective_to`, `is_current`. Join each order to the customer version that was active at the time of the order.

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    f.order_id,
    f.order_date,
    f.amount,
    d.customer_name,
    d.segment  -- The segment AT THE TIME of the order
FROM fact_orders f
JOIN dim_customer d 
    ON f.customer_id = d.customer_id
    AND f.order_date >= d.effective_from
    AND f.order_date < COALESCE(d.effective_to, '9999-12-31'::DATE);
```

**Explanation:**
- Range join: order_date must fall within the dimension's effective period
- COALESCE handles current records where effective_to is NULL
- This ensures historical accuracy (order shows customer's segment at purchase time, not current segment)

**Follow-up:** "This is a range join — what physical join algorithm does the optimizer use?" → Nested loop (can't hash a range condition). Performance tip: filter fact to recent dates first.

</details>

</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Multi-Source Attribution

**Scenario:** `ad_campaigns(campaign_id, channel, start_date, end_date)` and `conversions(conversion_id, user_id, conversion_date, revenue)`. A conversion is attributed to a campaign if it occurs during the campaign's date range. One conversion may match multiple campaigns. Calculate:
1. Last-touch attribution (100% credit to most recent campaign)
2. Linear attribution (split equally among overlapping campaigns)

<details>
<summary>✅ Solution</summary>

```sql
WITH campaign_matches AS (
    SELECT 
        c.conversion_id, c.revenue,
        a.campaign_id, a.channel, a.end_date,
        COUNT(*) OVER (PARTITION BY c.conversion_id) AS overlapping_count,
        ROW_NUMBER() OVER (PARTITION BY c.conversion_id ORDER BY a.end_date DESC) AS recency_rank
    FROM conversions c
    JOIN ad_campaigns a ON c.conversion_date BETWEEN a.start_date AND a.end_date
)
-- Last-touch: 100% to most recent campaign
SELECT campaign_id, channel,
    SUM(CASE WHEN recency_rank = 1 THEN revenue ELSE 0 END) AS last_touch_revenue,
    SUM(revenue / overlapping_count) AS linear_revenue
FROM campaign_matches
GROUP BY campaign_id, channel
ORDER BY last_touch_revenue DESC;
```

**Explanation:**
- Range join matches conversions to all overlapping campaigns
- Window functions compute both attribution models in one pass
- ROW_NUMBER picks the most recent campaign (last-touch)
- 1/overlapping_count splits revenue equally (linear)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Optimize a 5-Table Join (45 min → 2 min)

**Scenario:** This query runs 45 minutes. `fact_events` (2B rows), `dim_user` (50M), `dim_product` (1M), `dim_geo` (10K), `dim_time` (365). Diagnose and fix:

```sql
SELECT dt.month_name, dg.country, dp.category,
    COUNT(DISTINCT du.user_id) AS unique_users, SUM(f.revenue) AS total
FROM fact_events f
JOIN dim_time dt ON f.event_date = dt.date_key
JOIN dim_geo dg ON f.geo_id = dg.geo_id
JOIN dim_product dp ON f.product_id = dp.product_id
JOIN dim_user du ON f.user_id = du.user_id
WHERE dt.year = 2024 AND dg.country IN ('US', 'UK', 'DE')
GROUP BY dt.month_name, dg.country, dp.category;
```

<details>
<summary>✅ Solution</summary>

**Diagnosis:**
1. Full scan of 2B rows (filter should push down to fact via date range)
2. JOIN to dim_user is unnecessary (user_id already in fact table!)
3. All dimensions are small enough to broadcast

**Fix:**

```sql
SELECT /*+ BROADCAST(dt), BROADCAST(dg), BROADCAST(dp) */
    dt.month_name, dg.country, dp.category,
    COUNT(DISTINCT f.user_id) AS unique_users,  -- user_id from FACT (no join to dim_user!)
    SUM(f.revenue) AS total
FROM fact_events f
JOIN dim_time dt ON f.event_date = dt.date_key
JOIN (SELECT geo_id, country FROM dim_geo WHERE country IN ('US','UK','DE')) dg ON f.geo_id = dg.geo_id
JOIN dim_product dp ON f.product_id = dp.product_id
WHERE f.event_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY dt.month_name, dg.country, dp.category;
```

**Changes:**
- Removed dim_user join entirely (user_id exists in fact table — no need to join 50M rows)
- Pushed date filter directly onto fact (partition pruning: 2B → ~200M rows)
- Pre-filtered dim_geo (10K → 3 rows before join)
- Broadcast all small dimensions (eliminates shuffle of fact table)
- Result: 45 min → ~2 min

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Sessionization with Joins

**Scenario:** Given `clickstream(user_id, page_url, event_timestamp)`, define sessions with 30-minute timeout. Calculate per-session: page count, duration, entry page. Then join with `users(user_id, segment)` to get session metrics by user segment.

<details>
<summary>✅ Solution</summary>

```sql
WITH event_gaps AS (
    SELECT user_id, page_url, event_timestamp,
        event_timestamp - LAG(event_timestamp) OVER (
            PARTITION BY user_id ORDER BY event_timestamp
        ) AS gap
    FROM clickstream
),
sessionized AS (
    SELECT *,
        SUM(CASE WHEN gap > INTERVAL '30 minutes' OR gap IS NULL THEN 1 ELSE 0 END) 
            OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_id
    FROM event_gaps
),
session_stats AS (
    SELECT 
        user_id, session_id,
        COUNT(*) AS page_views,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) - MIN(event_timestamp) AS duration,
        FIRST_VALUE(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) AS entry_page
    FROM sessionized
    GROUP BY user_id, session_id, page_url, event_timestamp
)
SELECT 
    u.segment,
    COUNT(DISTINCT ss.session_id) AS total_sessions,
    AVG(ss.page_views) AS avg_pages_per_session,
    AVG(EXTRACT(EPOCH FROM ss.duration)/60) AS avg_duration_min
FROM session_stats ss
JOIN users u ON ss.user_id = u.user_id
GROUP BY u.segment
ORDER BY total_sessions DESC;
```

**Explanation:**
- LAG detects gaps > 30 min
- Cumulative SUM of flags assigns session IDs
- Session-level aggregation before joining to users (prevents fan-out)
- Final join is 1:1 (session → user) — efficient

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Handle Data Skew in a Join

**Scenario:** Joining `orders` (2B rows) with `customers` (50M rows) on `customer_id`. One customer_id is NULL for 300M rows (30% of orders). The join stage takes 4 hours because one executor processes all 300M NULL rows. Design a fix.

<details>
<summary>✅ Solution</summary>

```sql
-- Split into hot path (NULLs) and cold path (non-NULLs)

-- Cold path: normal join, balanced (no skew)
SELECT o.*, c.name, c.segment
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL

UNION ALL

-- Hot path: NULL customer_id → assign "Unknown" customer (broadcast tiny result)
SELECT o.*, 'Unknown' AS name, 'Unknown' AS segment
FROM orders o
WHERE o.customer_id IS NULL;
```

**Alternative (salting for non-NULL hot keys):**
```sql
-- If a specific customer_id has 100M rows (not NULL):
-- Add random salt to spread across partitions
SELECT o.*, c.name
FROM (SELECT *, customer_id || '_' || (RANDOM() % 10) AS salted_key FROM orders WHERE customer_id = 'HOT_KEY') o
JOIN (SELECT *, customer_id || '_' || s AS salted_key FROM customers CROSS JOIN generate_series(0,9) s WHERE customer_id = 'HOT_KEY') c
ON o.salted_key = c.salted_key;
```

**Explanation:**
- NULL values never match in joins (NULL = NULL is UNKNOWN, not TRUE)
- Separating NULLs removes the skew entirely
- For non-NULL hot keys: salting splits the hot partition across N sub-partitions
- In Spark 3.0+: AQE handles this automatically with `spark.sql.adaptive.skewJoin.enabled`

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Real-Time Inventory System with Joins

**Scenario:** Design SQL for a real-time inventory dashboard that shows:
- Current stock level per product per warehouse (running sum of movements)
- Days until stockout (based on 30-day average consumption)
- Products that stockouted in the last 90 days

Table: `inventory_movements(product_id, warehouse_id, movement_date, quantity_change)` — positive for inbound, negative for outbound.

<details>
<summary>✅ Solution</summary>

```sql
WITH stock_levels AS (
    SELECT product_id, warehouse_id, movement_date, quantity_change,
        SUM(quantity_change) OVER (
            PARTITION BY product_id, warehouse_id 
            ORDER BY movement_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_stock
    FROM inventory_movements
),
current_stock AS (
    SELECT DISTINCT product_id, warehouse_id,
        LAST_VALUE(running_stock) OVER (
            PARTITION BY product_id, warehouse_id 
            ORDER BY movement_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS current_level
    FROM stock_levels
),
consumption AS (
    SELECT product_id, warehouse_id,
        ABS(SUM(CASE WHEN quantity_change < 0 THEN quantity_change END)) 
            / NULLIF(COUNT(DISTINCT movement_date), 0) AS daily_consumption
    FROM inventory_movements
    WHERE movement_date >= CURRENT_DATE - 30 AND quantity_change < 0
    GROUP BY product_id, warehouse_id
),
stockouts AS (
    SELECT product_id, warehouse_id, movement_date AS stockout_date
    FROM stock_levels
    WHERE running_stock <= 0 AND movement_date >= CURRENT_DATE - 90
)
SELECT 
    cs.product_id, cs.warehouse_id, cs.current_level,
    c.daily_consumption,
    CASE WHEN c.daily_consumption > 0 
         THEN ROUND(cs.current_level / c.daily_consumption, 1)
    END AS days_until_stockout,
    COUNT(so.stockout_date) AS stockouts_90d
FROM current_stock cs
LEFT JOIN consumption c ON cs.product_id = c.product_id AND cs.warehouse_id = c.warehouse_id
LEFT JOIN stockouts so ON cs.product_id = so.product_id AND cs.warehouse_id = so.warehouse_id
GROUP BY cs.product_id, cs.warehouse_id, cs.current_level, c.daily_consumption
ORDER BY days_until_stockout ASC NULLS LAST;
```

**Explanation:**
- Running SUM for real-time stock levels (window function)
- 30-day consumption rate for prediction
- LEFT JOINs ensure all products appear (even those with no consumption or no stockouts)
- This demonstrates: window functions + CTEs + multiple joins + business logic

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN?**
A: INNER JOIN returns only rows with matching keys in both tables. LEFT JOIN returns all rows from the left table plus matched rows from the right (NULL for unmatched right columns). RIGHT JOIN is the mirror image. FULL OUTER JOIN returns all rows from both tables, with NULLs where no match exists.

**Q: What is a CROSS JOIN and when would you use it?**
A: A CROSS JOIN returns the Cartesian product of two tables—every row from the left paired with every row from the right (n × m rows). It's used intentionally for generating combinations (e.g., pairing every product with every region), and unintentionally when a JOIN condition is accidentally omitted.

**Q: What is a self join and what is it used for?**
A: A self join joins a table to itself, typically to compare rows within the same table. Common use cases include finding employee-manager relationships (joining the employees table to itself on manager_id = employee_id) or finding pairs of rows meeting some condition (e.g., flights with the same origin and different destinations).

**Q: What is the difference between a hash join and a sort-merge join?**
A: A hash join builds an in-memory hash table from the smaller relation and probes it with each row from the larger—O(n+m) but requires memory. A sort-merge join sorts both inputs on the join key and scans them in parallel—O(n log n + m log m) but avoids random memory access. The optimizer chooses based on table sizes, available memory, and whether data is pre-sorted.

**Q: What causes a NULL in JOIN results and how do you filter for unmatched rows?**
A: NULLs appear in OUTER JOIN results for the non-driving table's columns when no matching row exists. To find unmatched rows (the "anti-join" pattern), use `WHERE right_table.key IS NULL` after a LEFT JOIN, or use `NOT EXISTS` / `NOT IN` subqueries. The IS NULL approach is often the most readable.

**Q: What is a non-equi join and when is it useful?**
A: A non-equi join uses inequality operators (>, <, BETWEEN, !=) in the join condition instead of equality. Common uses: date range joins (joining events to applicable promotions by date range), bucketing (joining a value to a range table), and comparing rows within the same table with a condition beyond equality.

**Q: What is join elimination and when does the optimizer apply it?**
A: Join elimination is an optimizer transformation that removes a join from the query plan when the optimizer can prove it doesn't affect the result. For example, if you JOIN on a UNIQUE NOT NULL column and never SELECT any column from that table, the join can be eliminated. This is common in views and BI tool-generated queries.

**Q: What is the impact of data skew on distributed join performance?**
A: In distributed query engines (Spark, Redshift, BigQuery), joins require co-locating matching keys on the same node (shuffle). If one join key value appears in millions of rows (e.g., a NULL or a single popular category), one partition becomes a hotspot, causing a skewed shuffle that bottlenecks the entire query. Mitigations include salting the key, broadcasting the smaller table, or pre-filtering skewed values.

---

## 💼 Interview Tips

- Be ready to write any join type on a whiteboard without hesitation—joins are tested in every DE interview and mechanical fluency is baseline.
- The anti-join pattern (LEFT JOIN + WHERE IS NULL vs. NOT EXISTS vs. NOT IN) is a classic senior question. Know all three forms and their behavioral differences with NULLs (NOT IN behaves unexpectedly when the subquery returns a NULL).
- When discussing join performance, always bring up the distribution of data—data skew is the most common production join performance problem in analytical systems like Spark and Redshift.
- Senior interviewers at analytics companies often ask about non-equi joins (date range joins for slowly changing dimensions)—know how to write them and understand their performance implications (they can't use hash joins efficiently).
- Mention join order as a optimization lever: putting the most selective filter on the driving table reduces the number of rows the hash/merge join must process. Some databases (PostgreSQL) determine this automatically; others (older Teradata) relied on query order.
