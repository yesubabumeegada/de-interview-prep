---
title: "PySpark Spark SQL - Fundamentals"
topic: pyspark
subtopic: spark-sql
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, spark-sql, sparksession, sql, dataframe-api, catalog, temp-view]
---

# PySpark Spark SQL — Fundamentals


## 🎯 Analogy

Think of Spark SQL like a translator: you write familiar SQL, and Spark converts it into an optimized distributed execution plan — same Catalyst optimizer, same performance, just SQL syntax.

---
## What Is Spark SQL?

Spark SQL is the module for structured data processing. It provides a SQL interface on top of Spark DataFrames, allowing you to query data using familiar SQL syntax while benefiting from Spark's distributed execution engine.

> **Key Insight:** Spark SQL and the DataFrame API produce identical execution plans. The optimizer doesn't care whether you write SQL or Python — it optimizes both the same way.

---

## SparkSession — The Entry Point

```python
from pyspark.sql import SparkSession

# Create SparkSession (replaces SQLContext and HiveContext from Spark 1.x)
spark = (SparkSession.builder
    .appName("SparkSQL_Fundamentals")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .enableHiveSupport()  # Optional: enables Hive metastore access
    .getOrCreate())

# SparkSession provides:
# - spark.sql()       → Run SQL queries
# - spark.read        → Read structured data
# - spark.catalog     → Access metadata
# - spark.conf        → Configuration management
```

---

## Creating Temp Views for SQL Access

DataFrames must be registered as views before SQL can reference them:

```python
# Read data into a DataFrame
orders_df = spark.read.parquet("hdfs:///data/orders/")
customers_df = spark.read.json("hdfs:///data/customers/")

# Register as temp views (session-scoped)
orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")

# Now query with SQL
result = spark.sql("""
    SELECT 
        c.customer_name,
        COUNT(o.order_id) AS order_count,
        SUM(o.amount) AS total_spent
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01'
    GROUP BY c.customer_name
    HAVING total_spent > 1000
    ORDER BY total_spent DESC
""")

result.show()
```

### View Types

```python
# Temp View — visible only in current SparkSession
df.createOrReplaceTempView("my_view")

# Global Temp View — visible across all sessions in the application
df.createOrReplaceGlobalTempView("global_view")
# Access with: spark.sql("SELECT * FROM global_temp.global_view")

# Drop a view
spark.catalog.dropTempView("my_view")
spark.catalog.dropGlobalTempView("global_view")
```

---

## spark.sql() — Running SQL Queries

```python
# Basic query — returns a DataFrame
active_users = spark.sql("SELECT * FROM users WHERE status = 'active'")

# Parameterized queries (Spark 3.4+)
result = spark.sql(
    "SELECT * FROM orders WHERE amount > :threshold AND region = :region",
    args={"threshold": 500, "region": "US"}
)

# Multi-line SQL with CTEs
monthly_summary = spark.sql("""
    WITH monthly_orders AS (
        SELECT
            customer_id,
            DATE_TRUNC('month', order_date) AS month,
            SUM(amount) AS monthly_total
        FROM orders
        GROUP BY customer_id, DATE_TRUNC('month', order_date)
    )
    SELECT
        month,
        COUNT(DISTINCT customer_id) AS active_customers,
        AVG(monthly_total) AS avg_spend
    FROM monthly_orders
    GROUP BY month
    ORDER BY month
""")
```

---

## SQL vs DataFrame API — Side-by-Side

Both produce the same execution plan:

```python
from pyspark.sql import functions as F

# SQL approach
sql_result = spark.sql("""
    SELECT 
        department,
        AVG(salary) AS avg_salary,
        MAX(salary) AS max_salary
    FROM employees
    WHERE hire_date >= '2020-01-01'
    GROUP BY department
    HAVING AVG(salary) > 75000
    ORDER BY avg_salary DESC
""")

# DataFrame API equivalent
df_result = (employees_df
    .filter(F.col("hire_date") >= "2020-01-01")
    .groupBy("department")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("avg_salary") > 75000)
    .orderBy(F.desc("avg_salary"))
)

# Verify: same plan
sql_result.explain()
df_result.explain()
# Both produce identical physical plans!
```

### When to Choose SQL vs DataFrame API

| Choose SQL When... | Choose DataFrame API When... |
|-------------------|------------------------------|
| Team knows SQL well | Complex transformations with Python logic |
| Query is analytics/reporting style | Chaining many operations programmatically |
| Migrating from Hive/Presto | Building reusable functions |
| Ad-hoc data exploration | Dynamic column selection |
| Query reads naturally as SQL | IDE autocomplete and type checking |

---

## Catalog Operations

The catalog provides metadata about tables, views, databases, and functions:

```python
# List databases
spark.catalog.listDatabases()

# List tables in current database
spark.catalog.listTables()

# List tables in specific database
spark.catalog.listTables("production_db")

# Check if table exists
spark.catalog.tableExists("orders")

# List columns of a table
spark.catalog.listColumns("orders")

# Get current database
spark.catalog.currentDatabase()

# Switch database
spark.sql("USE production_db")

# List functions
spark.catalog.listFunctions()

# Cache a table (materializes for repeated access)
spark.catalog.cacheTable("orders")
spark.catalog.uncacheTable("orders")
spark.catalog.isCached("orders")

# Refresh metadata after external changes
spark.catalog.refreshTable("orders")
```

---

## Data Types in Spark SQL

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, ArrayType, MapType, BooleanType
)

# Define schema explicitly
order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("items", ArrayType(StringType()), nullable=True),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
])

# Use schema when reading
df = spark.read.schema(order_schema).json("hdfs:///data/orders/")

# Cast types in SQL
spark.sql("""
    SELECT 
        CAST(amount AS DECIMAL(10,2)) AS amount,
        CAST(created_at AS DATE) AS order_date,
        SIZE(items) AS item_count
    FROM orders
""")
```

---

## Common SQL Functions

```python
# String functions
spark.sql("""
    SELECT
        UPPER(name) AS upper_name,
        TRIM(email) AS clean_email,
        CONCAT(first_name, ' ', last_name) AS full_name,
        SUBSTRING(phone, 1, 3) AS area_code,
        REGEXP_REPLACE(address, '\\s+', ' ') AS clean_address
    FROM customers
""")

# Date functions
spark.sql("""
    SELECT
        CURRENT_DATE() AS today,
        DATE_ADD(order_date, 30) AS due_date,
        DATEDIFF(ship_date, order_date) AS days_to_ship,
        DATE_FORMAT(created_at, 'yyyy-MM') AS year_month,
        YEAR(order_date) AS order_year
    FROM orders
""")

# Conditional logic
spark.sql("""
    SELECT
        order_id,
        CASE
            WHEN amount > 1000 THEN 'high'
            WHEN amount > 100 THEN 'medium'
            ELSE 'low'
        END AS order_tier,
        COALESCE(discount, 0) AS discount,
        IF(is_prime, amount * 0.9, amount) AS final_amount
    FROM orders
""")
```

---


## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("sparksql").getOrCreate()
data = [("Alice","Sales",300),("Bob","Eng",400),("Carol","Sales",250)]
df = spark.createDataFrame(data, ["name","dept","salary"])
df.createOrReplaceTempView("employees")
result = spark.sql('SELECT dept, AVG(salary) as avg_salary, COUNT(*) as headcount FROM employees GROUP BY dept ORDER BY avg_salary DESC')
result.show()
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between SQL and DataFrame API in Spark?" — "They're functionally equivalent. Both go through the same Catalyst optimizer and produce identical physical plans. SQL is better for analytics-style queries and when your team knows SQL. DataFrame API is better for programmatic transformations and when you need IDE support. I choose based on readability for the specific operation."

> **Tip 2:** "Explain createOrReplaceTempView." — "It registers a DataFrame as a named SQL view within the current SparkSession. The 'replace' part means it overwrites if the view already exists. The view is session-scoped — other sessions can't see it. For cross-session access, use createOrReplaceGlobalTempView, which requires the 'global_temp' database prefix to query."

> **Tip 3:** "How would you explore an unfamiliar dataset with Spark SQL?" — "Start with spark.catalog.listTables() to see what's available. Use DESCRIBE TABLE for schema info, then SELECT * LIMIT 10 for a preview. Check data quality with COUNT, COUNT(DISTINCT), and null counts. Use EXPLAIN to understand how queries execute. For partitioned tables, SHOW PARTITIONS reveals the partition structure."
