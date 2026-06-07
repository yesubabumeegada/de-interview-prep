---
title: "SQL Window Functions - Scenario Questions"
topic: sql
subtopic: window-functions
content_type: scenario_question
tags: [sql, window-functions, interview, scenarios]
---

# Scenario Questions — SQL Window Functions

<article data-difficulty="junior">

## 🟢 Junior: Top 3 Products by Revenue per Category

**Scenario:** You have an `orders` table with columns `product_id`, `category`, `order_date`, and `revenue`. Write a query to find the top 3 products by total revenue in each category. Return the product, category, total revenue, and rank.

<details>
<summary>💡 Hint</summary>

Use `ROW_NUMBER()` with `PARTITION BY category` to assign a sequential rank within each category. Aggregate revenue first using a CTE or subquery, then apply the window function on the aggregated result.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH product_revenue AS (
    SELECT 
        product_id,
        category,
        SUM(revenue) AS total_revenue
    FROM orders
    GROUP BY product_id, category
),
ranked AS (
    SELECT 
        product_id,
        category,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY category 
            ORDER BY total_revenue DESC
        ) AS rn
    FROM product_revenue
)
SELECT product_id, category, total_revenue, rn AS rank
FROM ranked
WHERE rn <= 3
ORDER BY category, rn;
```

**Explanation:**
- `ROW_NUMBER()` guarantees exactly 3 rows per category even if there are ties
- Aggregation with `GROUP BY` must happen before ranking — window functions operate on the result set
- Use `DENSE_RANK()` instead if you want all tied products to share the same rank and potentially return more than 3 rows

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Running Total of Daily Sales Resetting Monthly

**Scenario:** Given a `daily_sales` table with `sale_date` and `amount`, write a query that shows each day's sales amount alongside a running total that resets at the beginning of each month.

<details>
<summary>💡 Hint</summary>

Use `SUM() OVER()` with `PARTITION BY` on the month portion of the date so the accumulation resets each month. Specify an explicit frame clause (`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) to avoid unexpected behavior with the default `RANGE` frame.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY DATE_TRUNC('month', sale_date)
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS mtd_running_total
FROM daily_sales
ORDER BY sale_date;
```

**Explanation:**
- `PARTITION BY DATE_TRUNC('month', sale_date)` resets the running total at each new month boundary
- `ORDER BY sale_date` ensures accumulation happens in chronological order
- The explicit `ROWS` frame avoids the default `RANGE` behavior which groups rows with the same `sale_date` value together, potentially giving incorrect totals on days with multiple entries

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Compare Each Employee's Salary to Department Average

**Scenario:** You have an `employees` table with `employee_id`, `name`, `department`, and `salary`. Write a query that shows each employee alongside their department's average salary and how much their salary differs from that average.

<details>
<summary>💡 Hint</summary>

Use `AVG(salary) OVER (PARTITION BY department)` to compute the department average without collapsing rows. This lets you keep individual employee rows while adding the aggregate as a new column.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    employee_id,
    name,
    department,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY department), 2) AS dept_avg_salary,
    ROUND(salary - AVG(salary) OVER (PARTITION BY department), 2) AS diff_from_avg
FROM employees
ORDER BY department, diff_from_avg DESC;
```

**Explanation:**
- `AVG() OVER (PARTITION BY department)` calculates the average per department without needing a `GROUP BY` or self-join
- Each row retains individual employee detail while gaining access to the aggregate
- The difference column immediately shows who is above or below the department average (positive = above, negative = below)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Assign Quartiles to Customers by Total Spend

**Scenario:** Given an `orders` table with `customer_id` and `amount`, categorize customers into four equal-sized groups (quartiles) based on their total lifetime spend. Return the customer, total spend, and which quartile they fall into (1 = lowest spenders, 4 = highest).

<details>
<summary>💡 Hint</summary>

First aggregate total spend per customer, then use `NTILE(4)` ordered by total spend to divide customers into four roughly equal buckets.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH customer_spend AS (
    SELECT 
        customer_id,
        SUM(amount) AS total_spend
    FROM orders
    GROUP BY customer_id
)
SELECT 
    customer_id,
    total_spend,
    NTILE(4) OVER (ORDER BY total_spend) AS spend_quartile
FROM customer_spend
ORDER BY spend_quartile DESC, total_spend DESC;
```

**Explanation:**
- `NTILE(4)` divides the result set into 4 approximately equal groups based on the ordering
- If the total number of customers isn't perfectly divisible by 4, the earlier groups will have one extra row
- This is commonly used for customer segmentation, grading, or percentile bucketing in analytics

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Show Previous Month's Revenue Alongside Current

**Scenario:** Given a `monthly_revenue` table with `revenue_month` (date) and `revenue`, write a query that shows each month's revenue alongside the previous month's revenue and the absolute change between them.

<details>
<summary>💡 Hint</summary>

Use `LAG(revenue, 1)` ordered by `revenue_month` to access the previous row's revenue. `LAG` returns NULL for the first row where there is no previous value.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    revenue_month,
    revenue AS current_revenue,
    LAG(revenue, 1) OVER (ORDER BY revenue_month) AS prev_month_revenue,
    revenue - LAG(revenue, 1) OVER (ORDER BY revenue_month) AS month_over_month_change
FROM monthly_revenue
ORDER BY revenue_month;
```

**Explanation:**
- `LAG(revenue, 1)` looks back exactly one row in the ordered window to retrieve the previous month's value
- The first row will show NULL for `prev_month_revenue` since there's no preceding row
- This pattern is foundational for any time-series comparison (day-over-day, week-over-week, etc.)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Find Consecutive Login Streaks per User

**Scenario:** Given a `user_logins` table with `user_id` and `login_date` (one row per user per day they logged in), find each user's longest consecutive login streak. Return the user, streak length, start date, and end date.

<details>
<summary>💡 Hint</summary>

Use the "date minus row_number" trick: for consecutive dates, subtracting a sequential row number from the date produces the same value, creating a natural group key for each streak.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH numbered AS (
    SELECT 
        user_id,
        login_date,
        login_date - CAST(ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY login_date
        ) AS INT) AS streak_group
    FROM user_logins
),
streaks AS (
    SELECT 
        user_id,
        streak_group,
        COUNT(*) AS streak_length,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end
    FROM numbered
    GROUP BY user_id, streak_group
),
ranked_streaks AS (
    SELECT 
        user_id,
        streak_length,
        streak_start,
        streak_end,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY streak_length DESC) AS rn
    FROM streaks
)
SELECT user_id, streak_length, streak_start, streak_end
FROM ranked_streaks
WHERE rn = 1
ORDER BY streak_length DESC;
```

**Explanation:**
- For consecutive dates (Jan 1, Jan 2, Jan 3) with row numbers (1, 2, 3), subtracting gives the same result (Dec 31), forming a natural grouping key
- A gap in dates produces a different group value, breaking the streak automatically
- The final ranking step picks each user's longest streak; ties go to the most recent one due to `ROW_NUMBER`

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Calculate Month-over-Month Growth Percentage

**Scenario:** Given an `orders` table with `order_date` and `amount`, calculate the month-over-month revenue growth percentage. Flag months where growth dropped below -10% as "declining."

<details>
<summary>💡 Hint</summary>

Aggregate revenue by month first, then use `LAG()` to access the prior month's revenue. Compute growth as `(current - previous) / previous * 100`. Watch out for division by zero if any month has zero revenue.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH monthly AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS revenue_month,
        SUM(amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT 
    revenue_month,
    revenue,
    LAG(revenue) OVER (ORDER BY revenue_month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY revenue_month)) * 100.0 
        / NULLIF(LAG(revenue) OVER (ORDER BY revenue_month), 0),
        1
    ) AS mom_growth_pct,
    CASE 
        WHEN (revenue - LAG(revenue) OVER (ORDER BY revenue_month)) * 100.0 
             / NULLIF(LAG(revenue) OVER (ORDER BY revenue_month), 0) < -10
        THEN 'DECLINING'
        ELSE 'OK'
    END AS status
FROM monthly
ORDER BY revenue_month;
```

**Explanation:**
- `LAG(revenue)` without an explicit offset defaults to 1 row back — the previous month
- `NULLIF(..., 0)` prevents division by zero by converting 0 to NULL, making the result NULL instead of erroring
- The `CASE` expression categorizes each month's health, which can feed into alerting dashboards or SLA reports

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Detect Gaps in Sequential Data

**Scenario:** You have a `daily_metrics` table with `metric_date` and `value` that should have one row per day. Some days are missing. Write a query to detect all missing days (gaps) in the data.

<details>
<summary>💡 Hint</summary>

Use `LEAD(metric_date)` to look at the next row's date. If the difference between the current date and the next date is greater than 1 day, there's a gap. Report the start and end of each gap.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH with_next AS (
    SELECT 
        metric_date,
        LEAD(metric_date) OVER (ORDER BY metric_date) AS next_date
    FROM daily_metrics
)
SELECT 
    metric_date AS last_present_date,
    next_date AS next_present_date,
    metric_date + INTERVAL '1 day' AS gap_start,
    next_date - INTERVAL '1 day' AS gap_end,
    (next_date - metric_date - 1) AS missing_days
FROM with_next
WHERE next_date - metric_date > 1
ORDER BY metric_date;
```

**Explanation:**
- `LEAD(metric_date)` peeks at the next row's date in chronological order
- If `next_date - metric_date > 1`, there are missing days between them
- This pattern is critical for data quality monitoring — detecting gaps in time-series ingestion pipelines

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Moving 7-Day Average of Daily Metrics

**Scenario:** Given a `daily_metrics` table with `metric_date` and `value`, calculate the 7-day moving average for each day. The window should include the current day and the 6 preceding days. Only compute the average when at least 4 days of data are available in the window.

<details>
<summary>💡 Hint</summary>

Use `AVG() OVER()` with `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` for the window frame. Use `COUNT()` with the same frame to check if at least 4 rows exist before reporting the average.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    metric_date,
    value,
    CASE 
        WHEN COUNT(*) OVER (
            ORDER BY metric_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) >= 4
        THEN ROUND(AVG(value) OVER (
            ORDER BY metric_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2)
        ELSE NULL
    END AS moving_7d_avg,
    COUNT(*) OVER (
        ORDER BY metric_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS days_in_window
FROM daily_metrics
ORDER BY metric_date;
```

**Explanation:**
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` creates a sliding window of at most 7 rows (current + 6 prior)
- Using `ROWS` instead of `RANGE` ensures exactly 6 preceding rows are considered, not all rows with dates within 6 days
- The `COUNT` check prevents misleading averages when insufficient data exists (e.g., the first 3 days of the series)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Rank Products Within Category and Show Overall Rank Too

**Scenario:** Given a `products` table with `product_id`, `category`, and `total_sales`, write a single query that shows each product's rank within its category AND its overall rank across all categories simultaneously.

<details>
<summary>💡 Hint</summary>

You can use multiple window functions in the same SELECT, each with a different `OVER()` clause. One partitions by category, the other has no partition (global window).

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    product_id,
    category,
    total_sales,
    RANK() OVER (
        PARTITION BY category 
        ORDER BY total_sales DESC
    ) AS category_rank,
    RANK() OVER (
        ORDER BY total_sales DESC
    ) AS overall_rank
FROM products
ORDER BY category, category_rank;
```

**Explanation:**
- The first `RANK()` partitions by category so ranking resets within each group
- The second `RANK()` uses no `PARTITION BY`, treating the entire result set as one window for global ranking
- Multiple window functions with different `OVER()` clauses in the same query is perfectly valid and avoids self-joins or subqueries

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Sessionize Clickstream Data with 30-Minute Timeout

**Scenario:** You have a `clickstream` table with `user_id`, `page_url`, and `event_timestamp`. Define a session as a series of events where no gap exceeds 30 minutes. Assign a session ID to each event and compute session-level metrics: duration, page count, and entry page.

<details>
<summary>💡 Hint</summary>

Use `LAG()` to calculate the time gap between consecutive events per user. Flag gaps >30 minutes as new session starts (1/0). Then use a cumulative `SUM()` of those flags to assign incrementing session IDs.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH gap_flagged AS (
    SELECT 
        user_id,
        page_url,
        event_timestamp,
        CASE 
            WHEN event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY user_id ORDER BY event_timestamp
            ) > INTERVAL '30 minutes'
            OR LAG(event_timestamp) OVER (
                PARTITION BY user_id ORDER BY event_timestamp
            ) IS NULL
            THEN 1
            ELSE 0
        END AS new_session_flag
    FROM clickstream
),
sessionized AS (
    SELECT 
        user_id,
        page_url,
        event_timestamp,
        SUM(new_session_flag) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM gap_flagged
)
SELECT 
    user_id,
    session_id,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    MAX(event_timestamp) - MIN(event_timestamp) AS session_duration,
    COUNT(*) AS page_count,
    MIN(page_url) FILTER (WHERE event_timestamp = MIN(event_timestamp)) AS entry_page
FROM sessionized
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

**Explanation:**
- `LAG()` computes time since the user's previous event; NULL (first event) or >30 min triggers a new session flag
- Cumulative `SUM()` of the 0/1 flags creates an auto-incrementing session ID per user
- This is the standard sessionization technique used in web analytics (Google Analytics uses a similar approach with configurable timeout)
- The pattern scales well on distributed engines (Spark, BigQuery) since `PARTITION BY user_id` isolates each user's computation

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Calculate Median Salary per Department

**Scenario:** Write a query to compute the median salary for each department using `PERCENTILE_CONT`. Also show the mean, and flag departments where the median and mean differ by more than 20% (indicating skewed salary distributions).

<details>
<summary>💡 Hint</summary>

`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)` computes the interpolated median. This is an ordered-set aggregate that can be used with `GROUP BY`. Compare it to `AVG()` to detect skew.

</details>

<details>
<summary>✅ Solution</summary>

```sql
SELECT 
    department,
    COUNT(*) AS emp_count,
    ROUND(AVG(salary), 2) AS mean_salary,
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)::NUMERIC, 2
    ) AS median_salary,
    ROUND(
        ABS(AVG(salary) - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)) 
        * 100.0 / NULLIF(AVG(salary), 0), 1
    ) AS mean_median_diff_pct,
    CASE 
        WHEN ABS(AVG(salary) - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)) 
             * 100.0 / NULLIF(AVG(salary), 0) > 20
        THEN 'SKEWED'
        ELSE 'NORMAL'
    END AS distribution_flag
FROM employees
GROUP BY department
ORDER BY mean_median_diff_pct DESC;
```

**Explanation:**
- `PERCENTILE_CONT(0.5)` returns the interpolated 50th percentile (true median); `PERCENTILE_DISC` would return an actual row value
- A large gap between mean and median indicates outliers pulling the mean (e.g., one executive salary in a small team)
- This is an ordered-set aggregate function, not a window function — it requires `WITHIN GROUP (ORDER BY ...)` syntax and works with `GROUP BY`
- In engines without `PERCENTILE_CONT` (e.g., MySQL), approximate the median using `ROW_NUMBER` and selecting the middle row(s)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: First and Last Purchase per Customer in a Single Query

**Scenario:** Given an `orders` table with `customer_id`, `order_date`, `product_name`, and `amount`, write a single query that shows each customer's first purchase details (date, product, amount) and last purchase details side by side, without self-joins.

<details>
<summary>💡 Hint</summary>

Use `FIRST_VALUE()` and `LAST_VALUE()` with `PARTITION BY customer_id ORDER BY order_date`. For `LAST_VALUE` to work correctly, you must override the default frame with `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH annotated AS (
    SELECT 
        customer_id,
        order_date,
        product_name,
        amount,
        FIRST_VALUE(order_date) OVER w AS first_order_date,
        FIRST_VALUE(product_name) OVER w AS first_product,
        FIRST_VALUE(amount) OVER w AS first_amount,
        LAST_VALUE(order_date) OVER w AS last_order_date,
        LAST_VALUE(product_name) OVER w AS last_product,
        LAST_VALUE(amount) OVER w AS last_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
    FROM orders
    WINDOW w AS (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT 
    customer_id,
    first_order_date,
    first_product,
    first_amount,
    last_order_date,
    last_product,
    last_amount,
    last_order_date - first_order_date AS customer_lifespan
FROM annotated
WHERE rn = 1
ORDER BY customer_id;
```

**Explanation:**
- `FIRST_VALUE` works with the default frame, but `LAST_VALUE` requires `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` — the default frame ends at `CURRENT ROW`, so `LAST_VALUE` would just return the current row's value
- The `WINDOW` clause (named window) avoids repeating the frame definition for every function
- `ROW_NUMBER()` with `WHERE rn = 1` deduplicates to one row per customer
- This avoids expensive self-joins or correlated subqueries that would scan the table multiple times

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Detect Salary Inversions in Org Hierarchy

**Scenario:** In your `employees` table (`employee_id`, `name`, `manager_id`, `salary`, `level`), a salary inversion occurs when a manager earns less than one of their direct reports, OR when someone at a higher org level earns less than someone at a lower level in the same department chain. Find all inversions with supporting context.

<details>
<summary>💡 Hint</summary>

Join employees to their managers via `manager_id`. Use `MAX(salary) OVER (PARTITION BY manager_id)` to find the highest-paid report for each manager. Compare manager salary to this maximum to detect inversions.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH report_stats AS (
    SELECT 
        e.employee_id,
        e.name AS employee_name,
        e.salary AS employee_salary,
        e.manager_id,
        m.name AS manager_name,
        m.salary AS manager_salary,
        m.level AS manager_level,
        e.level AS employee_level,
        MAX(e.salary) OVER (PARTITION BY e.manager_id) AS max_report_salary,
        RANK() OVER (PARTITION BY e.manager_id ORDER BY e.salary DESC) AS salary_rank_in_team
    FROM employees e
    JOIN employees m ON e.manager_id = m.employee_id
)
SELECT 
    manager_name,
    manager_salary,
    employee_name AS highest_paid_report,
    employee_salary AS report_salary,
    employee_salary - manager_salary AS inversion_amount,
    ROUND((employee_salary - manager_salary) * 100.0 / manager_salary, 1) AS inversion_pct
FROM report_stats
WHERE employee_salary > manager_salary
  AND salary_rank_in_team = 1
ORDER BY inversion_amount DESC;
```

**Explanation:**
- The self-join connects each employee to their manager, bringing both salaries into the same row
- `MAX(salary) OVER (PARTITION BY manager_id)` computes the highest report salary without collapsing rows
- `salary_rank_in_team = 1` filters to only show the worst inversion per manager (the highest-paid report)
- This type of query is used in HR analytics and compensation reviews to flag pay equity issues before they cause attrition

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Running Distinct Count Approximation Using Window Functions

**Scenario:** Given a `page_views` table with `user_id`, `view_date`, and `page_id`, compute the cumulative number of distinct pages each user has viewed up to each date. Standard `COUNT(DISTINCT)` doesn't work as a window function — find a workaround.

<details>
<summary>💡 Hint</summary>

Assign a flag to each row indicating whether this is the first time a user viewed that page (using `ROW_NUMBER` partitioned by user and page). Then compute a running `SUM` of those first-occurrence flags to get a running distinct count.

</details>

<details>
<summary>✅ Solution</summary>

```sql
WITH first_views AS (
    SELECT 
        user_id,
        view_date,
        page_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, page_id 
            ORDER BY view_date
        ) AS page_occurrence
    FROM page_views
),
flagged AS (
    SELECT 
        user_id,
        view_date,
        page_id,
        CASE WHEN page_occurrence = 1 THEN 1 ELSE 0 END AS is_new_page
    FROM first_views
)
SELECT 
    user_id,
    view_date,
    page_id,
    SUM(is_new_page) OVER (
        PARTITION BY user_id 
        ORDER BY view_date, page_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_distinct_pages
FROM flagged
ORDER BY user_id, view_date;
```

**Explanation:**
- `COUNT(DISTINCT x) OVER(...)` is not supported in most SQL engines — this is the standard workaround
- `ROW_NUMBER() PARTITION BY user_id, page_id` gives `1` for the first occurrence of each page per user
- Summing those first-occurrence flags cumulatively provides an exact running distinct count
- For approximate distinct counts at scale (billions of rows), consider HyperLogLog functions available in BigQuery (`APPROX_COUNT_DISTINCT`) or Snowflake

</details>

</article>
