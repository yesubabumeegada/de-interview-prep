---
title: "Python Pandas - Fundamentals"
topic: python
subtopic: pandas
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, pandas, dataframe, groupby, merge, filtering, transformations]
---

# Python Pandas — Fundamentals

## What is Pandas and When to Use It

Pandas is Python's data manipulation library — the go-to tool for tabular data processing when datasets fit in memory (typically up to 5-10GB depending on your machine).

**The analogy:** Pandas is like Excel on steroids — all the familiar operations (filter, sort, group, pivot) but scriptable, reproducible, and capable of handling millions of rows.

> **DE context:** Pandas is ideal for prototyping transforms, small-scale ETL, data quality checks, and local development. For datasets exceeding memory, graduate to PySpark or Polars.

---

## DataFrame Basics

```python
import pandas as pd

# Creating DataFrames
df = pd.DataFrame({
    "user_id": ["u1", "u2", "u3", "u1"],
    "event_type": ["login", "purchase", "login", "purchase"],
    "amount": [0, 99.99, 0, 45.50],
    "event_date": ["2024-01-15", "2024-01-15", "2024-01-16", "2024-01-16"],
})

# Reading data (most common in DE)
df = pd.read_csv("data/events.csv")
df = pd.read_parquet("data/events.parquet")
df = pd.read_json("data/events.json", lines=True)  # JSONL format

# Quick inspection
print(df.shape)       # (rows, columns)
print(df.dtypes)      # Column types
print(df.head())      # First 5 rows
print(df.describe())  # Stats for numeric columns
print(df.info())      # Memory usage and null counts
```

---

## Selecting and Filtering

```python
# Select columns
emails = df["email"]                     # Single column (Series)
subset = df[["user_id", "email"]]        # Multiple columns (DataFrame)

# Filter rows
active_users = df[df["status"] == "active"]
high_value = df[df["amount"] > 100]
recent = df[df["event_date"] >= "2024-01-01"]

# Multiple conditions (use & for AND, | for OR, wrap each in parentheses)
target = df[(df["status"] == "active") & (df["amount"] > 100)]
multi = df[(df["event_type"] == "purchase") | (df["event_type"] == "refund")]

# isin for multiple values
purchases_and_refunds = df[df["event_type"].isin(["purchase", "refund"])]

# Filter with .query() — cleaner syntax for complex filters
result = df.query("status == 'active' and amount > 100 and region in ['US', 'EU']")
```

---

## GroupBy — Aggregation

```python
# Basic aggregation
daily_totals = df.groupby("event_date")["amount"].sum()

# Multiple aggregations
summary = df.groupby("user_id").agg(
    total_spent=("amount", "sum"),
    order_count=("order_id", "count"),
    avg_order=("amount", "mean"),
    first_purchase=("event_date", "min"),
    last_purchase=("event_date", "max"),
)

# Group by multiple columns
regional_stats = df.groupby(["region", "event_type"]).agg(
    count=("user_id", "count"),
    total_amount=("amount", "sum"),
).reset_index()

# Named aggregation with custom functions
def percentile_95(x):
    return x.quantile(0.95)

stats = df.groupby("category").agg(
    median_amount=("amount", "median"),
    p95_amount=("amount", percentile_95),
    unique_users=("user_id", "nunique"),
)
```

---

## Merging DataFrames (Joins)

```python
# Inner join — only matching records
merged = pd.merge(orders, users, on="user_id", how="inner")

# Left join — all orders, matching user info
merged = pd.merge(orders, users, on="user_id", how="left")

# Join on different column names
merged = pd.merge(
    orders, products,
    left_on="product_code",
    right_on="code",
    how="left"
)

# Multiple join keys
merged = pd.merge(
    fact_table, dim_table,
    on=["date", "region"],
    how="inner"
)

# Handling duplicate column names
merged = pd.merge(
    orders, returns,
    on="order_id",
    how="left",
    suffixes=("_order", "_return")
)
```

The diagram below summarizes the four merge strategies and which rows each one keeps, from inner (matches only) to outer (every row from both sides).

```mermaid
flowchart LR
    subgraph "Join Types"
        A[Inner: only matches]
        B[Left: all left + matches]
        C[Right: all right + matches]
        D[Outer: everything]
    end
```

---

## Basic Transformations

```python
# New columns
df["amount_cents"] = (df["amount"] * 100).astype(int)
df["is_high_value"] = df["amount"] > 100
df["year_month"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m")

# String operations
df["email_domain"] = df["email"].str.split("@").str[1]
df["name_clean"] = df["name"].str.strip().str.title()

# Replace values
df["status"] = df["status"].replace({"active": 1, "inactive": 0})

# Fill missing values
df["amount"] = df["amount"].fillna(0)
df["category"] = df["category"].fillna("unknown")

# Drop duplicates
df_deduped = df.drop_duplicates(subset=["user_id", "event_date"], keep="last")

# Sort
df_sorted = df.sort_values(["user_id", "event_date"], ascending=[True, False])

# Type conversion
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")  # Invalid → NaN
df["event_date"] = pd.to_datetime(df["event_date"])
```

---

## Writing Data

```python
# CSV
df.to_csv("output/results.csv", index=False)

# Parquet (preferred for DE)
df.to_parquet("output/results.parquet", index=False, compression="snappy")

# Partitioned Parquet
df.to_parquet(
    "output/events/",
    index=False,
    partition_cols=["event_date"],
    compression="snappy"
)
```

---

## Common DE Operations

```python
# Check for nulls
null_report = df.isnull().sum()
null_pct = df.isnull().mean() * 100

# Value counts (frequency distribution)
df["event_type"].value_counts()

# Rename columns
df = df.rename(columns={"old_name": "new_name", "col2": "better_name"})

# Reorder columns
df = df[["user_id", "event_type", "amount", "event_date"]]

# Sample (for testing/exploration)
sample = df.sample(n=1000, random_state=42)
```

---

## Interview Tips

> **Tip 1:** Know when to use Pandas vs PySpark: "I use Pandas for datasets under ~5GB, data exploration, and prototyping. For production pipelines processing 100GB+, I use PySpark. The API is similar enough that Pandas prototypes translate to Spark easily."

> **Tip 2:** When asked about joins, mention: "I always check for key duplicates before joining — a many-to-many join can silently explode row counts. I validate with `df['key'].duplicated().sum()` before merging." This shows production awareness.

> **Tip 3:** For aggregation questions, use named aggregation syntax: `df.groupby("key").agg(metric_name=("column", "function"))`. This produces clean, self-documenting column names in the output — much better than the old multi-level column headers.
