---
title: "Snowflake Snowpark & UDFs - Fundamentals"
topic: snowflake
subtopic: snowpark-udfs
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, snowpark, udf, python, dataframe, stored-procedures]
---

# Snowflake Snowpark & UDFs — Fundamentals

## 🎯 Analogy

Traditional Snowflake is like working at a restaurant where you can only order from the menu (SQL). **Snowpark** gives you access to the kitchen — you write Python, Java, or Scala code that runs *inside* Snowflake's compute. **UDFs** (User-Defined Functions) are like creating custom menu items that other people can order using SQL.

---

## What Is Snowpark?

Snowpark is Snowflake's framework that lets you write data processing logic in Python, Java, or Scala and run it on Snowflake's compute — without moving data out.

**Key benefit:** Your Python/Scala/Java code runs where the data lives. No moving TBs out to a Spark cluster or Pandas on a laptop.

```
Traditional approach:
  Snowflake → export data → Python on local machine → results

Snowpark approach:
  Snowflake + Python code → Python runs inside Snowflake → results stay in Snowflake
```

---

## Snowpark Python: DataFrames

The primary Snowpark API uses a **lazy DataFrame** abstraction — similar to Spark or Pandas, but executes as SQL on Snowflake:

```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, avg

# Connect
session = Session.builder.configs({
    "account":   "myaccount",
    "user":      "myuser",
    "password":  "mypassword",
    "warehouse": "analytics_wh",
    "database":  "analytics",
    "schema":    "curated"
}).create()

# Read a table as a DataFrame
orders_df = session.table("fact_orders")

# Transform (lazy — no query runs yet)
result_df = (
    orders_df
    .filter(col("order_date") >= "2024-01-01")
    .group_by(col("region"))
    .agg(
        sum_("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
    .sort(col("total_revenue").desc())
)

# Execute (SQL runs on Snowflake)
result_df.show()        # display top 10 rows
result_df.collect()     # return all rows as list of Row objects
result_df.write.save_as_table("revenue_by_region")  # write back
```

---

## User-Defined Functions (UDFs)

UDFs let you extend SQL with custom Python (or Java/JavaScript) logic:

### JavaScript UDFs (simplest, no library imports)

```sql
-- Simple JavaScript UDF
CREATE OR REPLACE FUNCTION mask_email(email VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    if (!EMAIL) return null;
    var parts = EMAIL.split('@');
    return '****@' + parts[1];
$$;

-- Use it in SQL like any built-in function
SELECT customer_id, mask_email(email) AS masked_email FROM customers;
```

### Python UDFs

```sql
-- Python UDF with a third-party package
CREATE OR REPLACE FUNCTION parse_url_domain(url VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('tldextract')
HANDLER = 'extract_domain'
AS $$
import tldextract

def extract_domain(url):
    if not url:
        return None
    extracted = tldextract.extract(url)
    return f"{extracted.domain}.{extracted.suffix}"
$$;

-- Use in SQL
SELECT page_url, parse_url_domain(page_url) AS domain FROM web_events;
```

---

## UDF Types

| Type | Input/Output | Best For |
|------|-------------|---------|
| Scalar UDF | 1 row in → 1 value out | Per-row transformation |
| Table Function (UDTF) | 1 row in → N rows out | Exploding arrays, parsing |
| Aggregate UDF | N rows in → 1 value out | Custom aggregations |
| Vectorized Python UDF | Pandas batch in → batch out | High-performance Python with numpy/pandas |

---

## Python Vectorized UDFs (Performance)

Standard Python UDFs call the function row-by-row (slow). Vectorized UDFs receive a Pandas Series:

```sql
CREATE OR REPLACE FUNCTION score_lead(revenue FLOAT, days_since_visit FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas')
HANDLER = 'score'
AS $$
import pandas as pd

def score(revenue: pd.Series, days_since_visit: pd.Series) -> pd.Series:
    # Vectorized numpy operations — much faster than row-by-row
    score = (revenue / 1000) * (1 / (days_since_visit + 1))
    return score.clip(0, 100)
$$;
```

---

## Stored Procedures with Snowpark

Unlike UDFs (used in SELECT), stored procedures execute imperative logic:

```sql
CREATE OR REPLACE PROCEDURE refresh_revenue_summary()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
from snowflake.snowpark import Session

def main(session: Session) -> str:
    # Truncate and reload the summary
    session.sql("TRUNCATE TABLE analytics.curated.revenue_summary").collect()

    (session.table("fact_orders")
        .filter("order_date >= '2024-01-01'")
        .group_by("region")
        .agg({"amount": "sum"})
        .write.save_as_table("analytics.curated.revenue_summary", mode="overwrite")
    )
    return "Done"
$$;

-- Call it
CALL refresh_revenue_summary();
```

---

## Try It Yourself

```python
# Install: pip install snowflake-snowpark-python

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

session = Session.builder.configs({
    "account": "your_account",
    "user": "your_user",
    "authenticator": "externalbrowser",  # SSO login
    "database": "analytics",
    "schema": "curated",
    "warehouse": "dev_wh"
}).create()

# Explore available tables
session.sql("SHOW TABLES").show()

# Read and inspect
df = session.table("fact_orders").limit(5)
df.show()
print(df.schema)  # print column types
```

---

## Interview Tips

> **Tip 1:** "What is Snowpark?" — "Snowpark is Snowflake's developer framework that lets you write Python, Java, or Scala code that executes inside Snowflake's compute. You use a DataFrame API similar to Spark, but the computation happens in Snowflake — no data movement. It's used for complex transformations, ML preprocessing, and custom logic that's hard to express in SQL."

> **Tip 2:** "What's the difference between a UDF and a stored procedure in Snowflake?" — "UDFs return a value and are called inside SQL expressions (SELECT, WHERE). Stored procedures run imperative logic (loops, multiple statements, DDL) and are called with CALL. A UDF can't CREATE TABLE; a stored procedure can."

> **Tip 3:** "When would you use a Python UDF over SQL?" — "When the logic requires libraries (regex, ML model inference, URL parsing), complex string manipulation, or business logic that's too verbose in SQL. Keep UDFs lightweight — heavy computation inside a UDF called 100M times is expensive. Use vectorized UDFs for bulk operations."
