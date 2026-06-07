---
title: "PySpark Spark SQL - Intermediate"
topic: pyspark
subtopic: spark-sql
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, spark-sql, hive, udfs, window-functions, cte, explain-plan]
---

# PySpark Spark SQL — Intermediate

## Hive Integration

Spark SQL can read/write Hive tables directly when configured with Hive metastore access:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("HiveIntegration")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://metastore-host:9083")
    .enableHiveSupport()
    .getOrCreate())

# Query Hive tables directly
result = spark.sql("""
    SELECT * FROM production_db.user_events
    WHERE event_date = '2024-01-15'
    AND event_type = 'purchase'
""")

# Create Hive-managed table
spark.sql("""
    CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
        date DATE,
        region STRING,
        revenue DECIMAL(12,2),
        order_count BIGINT
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
""")

# Insert into partitioned table
spark.sql("""
    INSERT OVERWRITE TABLE analytics.daily_revenue
    PARTITION (year=2024, month=1)
    SELECT 
        event_date AS date,
        region,
        SUM(amount) AS revenue,
        COUNT(*) AS order_count
    FROM production_db.user_events
    WHERE year = 2024 AND month = 1
    GROUP BY event_date, region
""")
```

### External Tables

```python
# External table — Spark doesn't manage the data lifecycle
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
        timestamp STRING,
        level STRING,
        message STRING,
        service STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION 'hdfs:///data/raw/logs/'
""")

# Data persists even if table is dropped
spark.sql("DROP TABLE raw_logs")  # Only removes metadata
```

---

## UDFs in Spark SQL

Register Python functions for use in SQL queries:

```python
from pyspark.sql.types import StringType, IntegerType

# Define and register a UDF
def categorize_amount(amount):
    if amount > 1000:
        return "premium"
    elif amount > 100:
        return "standard"
    return "basic"

spark.udf.register("categorize_amount", categorize_amount, StringType())

# Use in SQL
result = spark.sql("""
    SELECT 
        order_id,
        amount,
        categorize_amount(amount) AS tier
    FROM orders
""")

# Register with decorator (for DataFrame API use)
from pyspark.sql.functions import udf

@udf(returnType=IntegerType())
def extract_year(date_str):
    return int(date_str[:4]) if date_str else None
```

> **Performance Warning:** SQL UDFs serialize data between JVM and Python for each row. Prefer built-in functions when possible.

---

## Window Functions in SQL

Window functions are essential for analytics — they compute values across rows related to the current row:

```python
# Ranking within groups
spark.sql("""
    SELECT
        employee_id,
        department,
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_with_ties,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank
    FROM employees
""")

# Running totals and moving averages
spark.sql("""
    SELECT
        order_date,
        daily_revenue,
        SUM(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_revenue,
        AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7day_avg
    FROM daily_sales
""")

# Lead/Lag for time-series comparison
spark.sql("""
    SELECT
        month,
        revenue,
        LAG(revenue, 1) OVER (ORDER BY month) AS prev_month,
        revenue - LAG(revenue, 1) OVER (ORDER BY month) AS mom_change,
        LEAD(revenue, 1) OVER (ORDER BY month) AS next_month
    FROM monthly_revenue
""")

# NTILE for percentile bucketing
spark.sql("""
    SELECT
        customer_id,
        total_spend,
        NTILE(4) OVER (ORDER BY total_spend DESC) AS spend_quartile
    FROM customer_summary
""")
```

---

## CTEs (Common Table Expressions) in Spark SQL

CTEs improve readability and allow query decomposition:

```python
# Multi-step analytics with CTEs
result = spark.sql("""
    WITH 
    -- Step 1: Calculate per-customer metrics
    customer_metrics AS (
        SELECT
            customer_id,
            COUNT(*) AS order_count,
            SUM(amount) AS total_spend,
            MIN(order_date) AS first_order,
            MAX(order_date) AS last_order
        FROM orders
        WHERE order_date >= '2023-01-01'
        GROUP BY customer_id
    ),
    -- Step 2: Segment customers
    customer_segments AS (
        SELECT
            *,
            CASE
                WHEN total_spend > 10000 THEN 'whale'
                WHEN total_spend > 1000 THEN 'regular'
                ELSE 'occasional'
            END AS segment,
            DATEDIFF(CURRENT_DATE(), last_order) AS days_since_last_order
        FROM customer_metrics
    ),
    -- Step 3: Identify at-risk customers
    at_risk AS (
        SELECT *
        FROM customer_segments
        WHERE segment IN ('whale', 'regular')
        AND days_since_last_order > 90
    )
    -- Final: Join with customer details
    SELECT
        c.customer_name,
        c.email,
        a.segment,
        a.total_spend,
        a.days_since_last_order
    FROM at_risk a
    JOIN customers c ON a.customer_id = c.customer_id
    ORDER BY a.total_spend DESC
""")
```

---

## EXPLAIN — Understanding Query Plans

```python
# Basic explain
spark.sql("EXPLAIN SELECT * FROM orders WHERE amount > 100").show(truncate=False)

# Extended explain (logical + physical plan)
spark.sql("EXPLAIN EXTENDED SELECT * FROM orders JOIN customers USING (customer_id)").show(truncate=False)

# Formatted explain (Spark 3.0+)
spark.sql("""
    EXPLAIN FORMATTED
    SELECT department, AVG(salary)
    FROM employees
    WHERE hire_date > '2020-01-01'
    GROUP BY department
""").show(truncate=False)

# Cost-based explain
spark.sql("""
    EXPLAIN COST
    SELECT o.*, c.customer_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
""").show(truncate=False)
```

### Reading the Output

```
== Physical Plan ==
*(2) HashAggregate(keys=[department], functions=[avg(salary)])
+- Exchange hashpartitioning(department, 200)    ← SHUFFLE
   +- *(1) HashAggregate(keys=[department], functions=[partial_avg(salary)])
      +- *(1) Filter (hire_date > 2020-01-01)    ← FILTER PUSHED
         +- *(1) FileScan parquet [department,salary,hire_date]
              Pushed Filters: [GreaterThan(hire_date,2020-01-01)]
              ReadSchema: struct<department:string,salary:double,hire_date:date>
```

Key indicators:
- `*(n)` = Whole-stage code generation (good)
- `Exchange` = Shuffle operation (expensive)
- `Pushed Filters` = Predicate pushdown to storage (good)
- `ReadSchema` = Column pruning (only reads needed columns)

---

## Combining SQL and DataFrame API

```python
# SQL for complex analytics, DataFrame for programmatic operations
cohort_sql = spark.sql("""
    WITH first_purchase AS (
        SELECT customer_id, MIN(order_date) AS cohort_date
        FROM orders
        GROUP BY customer_id
    )
    SELECT
        DATE_TRUNC('month', fp.cohort_date) AS cohort_month,
        DATEDIFF(o.order_date, fp.cohort_date) / 30 AS months_since,
        COUNT(DISTINCT o.customer_id) AS active_customers
    FROM orders o
    JOIN first_purchase fp ON o.customer_id = fp.customer_id
    GROUP BY 1, 2
""")

# Continue with DataFrame API for pivoting and formatting
from pyspark.sql import functions as F

pivot_result = (cohort_sql
    .groupBy("cohort_month")
    .pivot("months_since", list(range(12)))
    .agg(F.first("active_customers"))
    .orderBy("cohort_month")
)

pivot_result.write.parquet("hdfs:///analytics/cohort_analysis/")
```

---

## Interview Tips

> **Tip 1:** "Explain window functions in Spark SQL." — "Window functions compute a value for each row based on a window of related rows, without collapsing the result. PARTITION BY defines the groups, ORDER BY defines the sequence, and the frame clause (ROWS BETWEEN) defines which rows participate. Common uses: ranking with ROW_NUMBER/RANK, running totals with SUM OVER, time comparisons with LAG/LEAD, and percentile bucketing with NTILE."

> **Tip 2:** "How do you read an EXPLAIN plan?" — "Read bottom-up. The leaf nodes are scans — check for Pushed Filters (predicate pushdown) and ReadSchema (column pruning). Exchange nodes indicate shuffles — these are expensive. The asterisk notation like *(1) means whole-stage code generation is active. Look for BroadcastHashJoin for small tables or SortMergeJoin for large ones. If you see BroadcastNestedLoopJoin, your query likely has a cartesian product."

> **Tip 3:** "When would you use CTEs in Spark SQL?" — "CTEs improve readability by breaking complex queries into named steps. They're essential for multi-step transformations like customer segmentation pipelines or cohort analysis. In Spark, each CTE materializes as a subquery in the plan — it doesn't cache the intermediate result. For reused intermediate results, materialize explicitly with createOrReplaceTempView and cache."
