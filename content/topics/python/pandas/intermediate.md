---
title: "Python Pandas - Intermediate"
topic: python
subtopic: pandas
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, pandas, window-functions, pivot, multiindex, apply, method-chaining, memory]
---

# Python Pandas — Intermediate Concepts

## Window Functions

Window functions compute values across related rows without collapsing the DataFrame — essential for running totals, rankings, and moving averages:

```python
import pandas as pd

# Running total per user
df["cumulative_spend"] = df.groupby("user_id")["amount"].cumsum()

# Rank within group
df["purchase_rank"] = df.groupby("user_id")["amount"].rank(ascending=False)

# Lag/Lead (previous/next value)
df["prev_amount"] = df.groupby("user_id")["amount"].shift(1)
df["next_amount"] = df.groupby("user_id")["amount"].shift(-1)

# Difference from previous
df["amount_change"] = df.groupby("user_id")["amount"].diff()

# Rolling window — 7-day moving average
df = df.sort_values("event_date")
df["rolling_7d_avg"] = (
    df.groupby("user_id")["amount"]
    .rolling(window=7, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

# Expanding window — running max
df["running_max"] = df.groupby("user_id")["amount"].expanding().max().reset_index(level=0, drop=True)

# Percent of group total
df["pct_of_total"] = df["amount"] / df.groupby("category")["amount"].transform("sum")
```

---

## Pivot and Melt — Reshaping Data

```python
# Pivot: long → wide (like a pivot table)
pivot_df = df.pivot_table(
    values="amount",
    index="user_id",
    columns="event_type",
    aggfunc="sum",
    fill_value=0
)
# Result: user_id | login | purchase | refund

# Melt: wide → long (unpivot)
wide_df = pd.DataFrame({
    "user_id": ["u1", "u2"],
    "jan_revenue": [100, 200],
    "feb_revenue": [150, 250],
    "mar_revenue": [120, 300],
})

long_df = wide_df.melt(
    id_vars=["user_id"],
    value_vars=["jan_revenue", "feb_revenue", "mar_revenue"],
    var_name="month",
    value_name="revenue"
)
# Result: user_id | month | revenue (one row per user-month)

# Cross-tabulation
crosstab = pd.crosstab(
    df["region"],
    df["event_type"],
    values=df["amount"],
    aggfunc="sum",
    margins=True  # Add totals row/column
)
```

---

## MultiIndex — Hierarchical Indexing

```python
# GroupBy with multiple keys creates MultiIndex
summary = df.groupby(["region", "category"]).agg(
    total=("amount", "sum"),
    count=("user_id", "count"),
)

# Access levels
summary.loc["US"]           # All US categories
summary.loc[("US", "electronics")]  # Specific combo

# Reset to flat DataFrame (usually preferred in DE)
flat = summary.reset_index()

# Stack/Unstack for reshaping
# Unstack: move inner index level to columns
wide = summary.unstack(level="category", fill_value=0)

# Stack: move columns back to index (opposite of unstack)
long = wide.stack()
```

---

## apply() and transform()

```python
# apply — flexible but slow (avoid on large datasets)
def categorize_amount(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"

df["tier"] = df["amount"].apply(categorize_amount)

# transform — returns same shape, useful for group-level values
df["group_mean"] = df.groupby("category")["amount"].transform("mean")
df["deviation"] = df["amount"] - df["group_mean"]

# apply on groups — custom group-level operations
def top_n_per_group(group, n=3):
    return group.nlargest(n, "amount")

top_3_per_category = df.groupby("category", group_keys=False).apply(top_n_per_group, n=3)

# Vectorized alternatives (much faster than apply)
# Instead of: df["tier"] = df["amount"].apply(categorize_amount)
# Use: numpy where/select
import numpy as np
conditions = [df["amount"] > 1000, df["amount"] > 100]
choices = ["high", "medium"]
df["tier"] = np.select(conditions, choices, default="low")
```

---

## Method Chaining — Clean Pipeline Style

```python
# Ugly: intermediate variables everywhere
df2 = df[df["status"] == "active"]
df3 = df2.rename(columns={"ts": "timestamp"})
df4 = df3.assign(amount_usd=df3["amount"] * exchange_rate)
df5 = df4.sort_values("timestamp")

# Clean: method chaining
result = (
    df
    .query("status == 'active'")
    .rename(columns={"ts": "timestamp"})
    .assign(
        amount_usd=lambda x: x["amount"] * exchange_rate,
        event_date=lambda x: pd.to_datetime(x["timestamp"]).dt.date,
    )
    .sort_values("timestamp")
    .drop_duplicates(subset=["user_id", "event_date"], keep="last")
    .reset_index(drop=True)
)

# Using pipe() for custom functions in the chain
def add_quality_flags(df):
    return df.assign(
        has_nulls=df.isnull().any(axis=1),
        is_outlier=df["amount"] > df["amount"].quantile(0.99),
    )

result = (
    df
    .pipe(add_quality_flags)
    .query("not has_nulls and not is_outlier")
)
```

---

## Memory Optimization

```python
# Check memory usage
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

# Downcast numeric types
df["amount"] = pd.to_numeric(df["amount"], downcast="float")
df["count"] = pd.to_numeric(df["count"], downcast="integer")

# Use categories for low-cardinality strings
df["status"] = df["status"].astype("category")      # "active", "inactive" etc.
df["region"] = df["region"].astype("category")       # "US", "EU", "APAC"
df["event_type"] = df["event_type"].astype("category")

# Memory savings example:
# String column with 1M rows, 10 unique values:
#   object dtype: ~60 MB
#   category dtype: ~1 MB (60x reduction!)

# Read with optimized types from the start
df = pd.read_csv(
    "large_file.csv",
    dtype={
        "user_id": "string",  # PyArrow string (more efficient)
        "status": "category",
        "count": "int32",
        "amount": "float32",
    }
)

# Chunked reading for files larger than memory
chunks = pd.read_csv("huge_file.csv", chunksize=100_000)
results = []
for chunk in chunks:
    processed = transform(chunk)
    results.append(processed)
df = pd.concat(results, ignore_index=True)
```

---

## Interview Tips

> **Tip 1:** For window function questions, know the difference between `transform` and `apply`: "transform returns a Series the same size as the group (broadcasts), while apply can return any shape. I use transform for adding group-level columns (like percent of total) and apply for custom group operations (like top-N per group)."

> **Tip 2:** Method chaining shows clean coding style. In interviews, write Pandas code as a chain: `df.query(...).assign(...).groupby(...).agg(...)`. This makes the data transformation pipeline readable as a sequence of steps. Mention that `assign` with `lambda x:` enables referencing columns created earlier in the same chain.

> **Tip 3:** Memory optimization matters for DE. Mention: "I use category dtype for low-cardinality columns — a status column with 5 unique values in 10M rows goes from 80MB to 1MB. For numeric columns, I downcast to float32/int32 when precision allows." This shows you think about production resource constraints.
