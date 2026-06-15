---
title: "LeetCode for Data Engineers - Scenario Questions"
topic: interview-prep
subtopic: leetcode-for-de
content_type: scenario_question
tags: [interview-prep, leetcode-for-de, leetcode, sql, python, scenarios]
---

# LeetCode for Data Engineers — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: SQL — Department Top 3 Earners

**Scenario:** You're given two tables: `Employee (id, name, salary, departmentId)` and `Department (id, name)`. Write a SQL query to find employees who are in the top 3 salary earners within their department. Include all employees tied for the 3rd position. Return `Department`, `Employee`, and `Salary`, ordered by department name and salary descending.

This is a slightly simplified version of LeetCode #185 (Department Top Three Salaries), one of the most commonly asked SQL problems in DE interviews.

<details>
<summary>✅ Solution</summary>

```sql
SELECT
    d.name AS Department,
    e.name AS Employee,
    e.salary AS Salary
FROM (
    SELECT
        id,
        name,
        salary,
        departmentId,
        DENSE_RANK() OVER (
            PARTITION BY departmentId
            ORDER BY salary DESC
        ) AS salary_rank
    FROM Employee
) e
JOIN Department d ON e.departmentId = d.id
WHERE e.salary_rank <= 3
ORDER BY Department, Salary DESC;
```

**Step-by-step breakdown:**

1. **Inner subquery**: Compute `DENSE_RANK()` for each employee within their department, ordered by salary descending. `DENSE_RANK` handles ties by assigning the same rank to employees with equal salary and not skipping ranks.

2. **JOIN**: Join with the Department table to get the department name.

3. **Filter**: `WHERE salary_rank <= 3` keeps only top-3 ranks. Because of DENSE_RANK, if two employees tie for rank 2, they both have rank 2 and the next employee has rank 3 — all are included.

**Why this is commonly asked**: It tests the combination of window functions, partition logic, and join in one problem. The key insight (DENSE_RANK vs RANK vs ROW_NUMBER) is what distinguishes candidates who understand window functions from those who've only memorized syntax.

**Edge cases:**
- Department with fewer than 3 employees: All are returned (ranks 1, 2, etc. — never exceeds n employees)
- Multiple employees with identical salaries in the same department: All tied employees at a given rank are included
- Department with only 1 employee: That one employee gets rank 1 and is returned

**Variant to practice**: Modify to return the top-1 employee per department (most common variant of this problem).
```sql
SELECT d.name AS Department, e.name AS Employee, e.salary AS Salary
FROM (
    SELECT id, name, salary, departmentId,
        DENSE_RANK() OVER (PARTITION BY departmentId ORDER BY salary DESC) AS rnk
    FROM Employee
) e
JOIN Department d ON e.departmentId = d.id
WHERE e.rnk = 1;
```

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Python — Group Anagrams (with DE Context)

**Scenario:** You work at a company where product names are entered inconsistently across source systems. Two product names are considered equivalent if they are anagrams of each other after lowercasing (contain the same characters in any order). Given a list of product name strings, group them so that equivalent product names are together. Return a list of groups. Order within groups and order of groups does not matter.

Then, as a follow-up: in production, the input could be 10 million product names. What would you do differently?

This is based on LeetCode #49 (Group Anagrams), but contextualized for a real DE problem.

<details>
<summary>✅ Solution</summary>

```python
from collections import defaultdict

def group_equivalent_products(product_names: list[str]) -> list[list[str]]:
    """
    Group product names that are anagrams of each other.
    Time: O(n * k log k) where n = number of products, k = max name length.
    Space: O(n * k) for the hash map.
    """
    groups = defaultdict(list)

    for name in product_names:
        # Canonical form: lowercase, sorted characters
        # Two anagrams will always produce the same canonical key
        canonical = tuple(sorted(name.lower()))
        groups[canonical].append(name)

    return list(groups.values())


# Example
products = ["listen", "enlist", "silent", "google", "googel", "Widget", "getiwid"]
result = group_equivalent_products(products)
print(result)
# [['listen', 'enlist', 'silent'], ['google', 'googel'], ['Widget', 'getiwid']]
```

**Trace through:**
- "listen" → sorted lowercase → ('e','i','l','n','s','t') — group key A
- "enlist" → ('e','i','l','n','s','t') — same key A
- "google" → ('e','g','g','l','o','o') — group key B
- "Widget" → ('d','e','g','i','t','w') — group key C
- "getiwid" → ('d','e','g','i','t','w') — same key C

**Follow-up: at 10 million product names**

For 10M items, the pure Python implementation hits memory limits and becomes slow. Production approaches:

1. **Spark-based**: Use a Spark job. The canonical key computation is a map operation (embarrassingly parallel). The grouping is a `groupBy` on the canonical key.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, collect_list, sort_array
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

@udf(StringType())
def canonical_key(name):
    return "".join(sorted(name.lower()))

df = spark.createDataFrame([(name,) for name in product_names], ["name"])
result = (
    df
    .withColumn("canonical", canonical_key("name"))
    .groupBy("canonical")
    .agg(collect_list("name").alias("group"))
    .drop("canonical")
)
result.show()
```

2. **Streaming / batch with Kafka**: If new product names arrive continuously, emit (canonical_key, original_name) pairs to Kafka, then use Flink or a stateful Kafka Streams application to maintain running groups.

3. **Database approach**: If the data lives in a warehouse (BigQuery/Snowflake), compute the canonical key as a SQL expression and GROUP BY it.

```sql
SELECT
    ARRAY_AGG(product_name) AS equivalent_names,
    COUNT(*) AS group_size
FROM (
    SELECT
        product_name,
        ARRAY_TO_STRING(ARRAY(SELECT ch FROM UNNEST(SPLIT(LOWER(product_name), '')) AS ch ORDER BY ch), '') AS canonical_key
    FROM products
) t
GROUP BY canonical_key
HAVING COUNT(*) > 1  -- Only return groups with multiple equivalents
ORDER BY group_size DESC;
```

**Complexity analysis:**
- Python solution: O(n × k log k) time, O(n × k) space
- Spark solution: O(n × k log k / parallelism) time — linear speedup with more executors
- The sort step (k log k per name) is the bottleneck; with very long product names (k >> 1), consider a hash of the character frequency instead of sorted tuple

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: SQL — Running Metrics with Conditional Reset and Rate Calculation

**Scenario:** You're analyzing a `pipeline_runs` table with columns: `run_id`, `pipeline_name`, `run_date`, `status` (`success` or `failure`), `rows_processed`, `duration_seconds`. Write a SQL query that computes for each pipeline, for each run:

1. The cumulative success rate up to and including that run (successes / total runs so far)
2. The current consecutive failure streak (number of consecutive failures ending at this run; reset to 0 on success)
3. The 7-run rolling average of `rows_processed`

Return all columns plus these three computed columns. This tests multiple window functions, conditional running aggregation, and a rolling average simultaneously.

<details>
<summary>✅ Solution</summary>

```sql
WITH run_numbered AS (
    -- Assign row number per pipeline in chronological order
    SELECT
        run_id,
        pipeline_name,
        run_date,
        status,
        rows_processed,
        duration_seconds,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY run_date, run_id
        ) AS run_seq
    FROM pipeline_runs
),
success_flags AS (
    SELECT
        *,
        CASE WHEN status = 'success' THEN 1 ELSE 0 END AS is_success,
        CASE WHEN status = 'failure' THEN 1 ELSE 0 END AS is_failure
    FROM run_numbered
),
with_streaks AS (
    SELECT
        *,
        -- Cumulative success rate: successes so far / runs so far
        ROUND(
            SUM(is_success) OVER (
                PARTITION BY pipeline_name
                ORDER BY run_date, run_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) * 1.0
            / ROW_NUMBER() OVER (
                PARTITION BY pipeline_name
                ORDER BY run_date, run_id
            ),
            4
        ) AS cumulative_success_rate,

        -- 7-run rolling average of rows processed
        ROUND(
            AVG(rows_processed) OVER (
                PARTITION BY pipeline_name
                ORDER BY run_date, run_id
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        ) AS rolling_7_avg_rows,

        -- For consecutive failure streak: find the last success run_seq
        -- Consecutive failures = current run_seq minus last success run_seq
        -- (or current run_seq if never succeeded)
        MAX(CASE WHEN status = 'success' THEN run_seq ELSE NULL END) OVER (
            PARTITION BY pipeline_name
            ORDER BY run_date, run_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_success_seq
    FROM success_flags
)
SELECT
    run_id,
    pipeline_name,
    run_date,
    status,
    rows_processed,
    duration_seconds,
    cumulative_success_rate,
    rolling_7_avg_rows,
    -- Consecutive failure streak:
    -- If current run is a success, streak is 0
    -- Otherwise: runs since last success (or all runs if never succeeded)
    CASE
        WHEN status = 'success' THEN 0
        WHEN last_success_seq IS NULL THEN run_seq  -- Never succeeded
        ELSE run_seq - last_success_seq              -- Runs since last success
    END AS consecutive_failure_streak
FROM with_streaks
ORDER BY pipeline_name, run_date, run_id;
```

**Annotated breakdown:**

**Cumulative success rate:**
```sql
SUM(is_success) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
/ ROW_NUMBER() OVER (...)
```
Running sum of successes divided by total runs processed so far. `ROW_NUMBER()` gives us the denominator (total runs) without needing a separate COUNT.

**Consecutive failure streak:**
The trick is using `MAX(CASE WHEN status = 'success' THEN run_seq ELSE NULL END) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` to find the sequence number of the most recent success up to and including the current row. The streak is then `current_run_seq - last_success_run_seq`. If there's never been a success, `last_success_seq` is NULL (MAX of all NULLs), so we use `run_seq` directly.

**7-run rolling average:**
```sql
AVG(rows_processed) OVER (
    PARTITION BY pipeline_name
    ORDER BY run_date, run_id
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```
Physical frame of 7 rows. Uses `ROWS` not `RANGE` — `RANGE` would use value-based bounds, not row-based, which would behave unexpectedly here since runs may happen on the same date.

**What to mention in an interview:**
- The `run_id` in the ORDER BY tiebreaker ensures deterministic ordering when multiple runs happen on the same date
- `ROUND(..., 4)` on the success rate prevents floating-point precision display issues
- The CASE logic for the streak handles all three states: current success (0), first run of the pipeline (no prior success), and normal failure streak
- In a real implementation, I'd consider materializing this CTE to avoid recomputing window functions multiple times — query planners sometimes re-execute subqueries

**Performance note**: Three window functions partitioned by `pipeline_name` and ordered the same way should result in a single sort pass in most query engines (Snowflake, BigQuery, Postgres all optimize this). Check the execution plan to confirm.

</details>

</article>
