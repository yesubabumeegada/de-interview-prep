---
title: "Python Pandas - Scenario Questions"
topic: python
subtopic: pandas
content_type: scenario_question
tags: [python, pandas, interview, scenarios, dataframe]
---

# Scenario Questions — Python Pandas

<article data-difficulty="junior">

## 🟢 Junior: Clean and Aggregate Sales Data

**Scenario:** You have a CSV file with columns: `order_id`, `customer_id`, `amount`, `date`, `region`. Some rows have NULL amounts and duplicate order_ids. Write Pandas code to: remove duplicates, drop null amounts, compute total revenue per region, and export to Parquet.

<details>
<summary>✅ Solution</summary>

```python
import pandas as pd

# Read
df = pd.read_csv('sales.csv', parse_dates=['date'])

# Clean
df = df.drop_duplicates(subset=['order_id'], keep='last')
df = df.dropna(subset=['amount'])
df = df[df['amount'] > 0]

# Aggregate
revenue_by_region = df.groupby('region').agg(
    total_revenue=('amount', 'sum'),
    order_count=('order_id', 'count'),
    avg_order=('amount', 'mean')
).reset_index()

# Export
revenue_by_region.to_parquet('revenue_by_region.parquet', index=False)
print(revenue_by_region)
```

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Merge Two DataFrames

**Scenario:** You have `orders` (order_id, customer_id, amount) and `customers` (customer_id, name, segment). Merge them to get a complete view. Handle: customers with no orders should still appear; orders with unknown customers should show "Unknown".

<details>
<summary>✅ Solution</summary>

```python
# Full outer merge
merged = orders.merge(customers, on='customer_id', how='outer')

# Fill missing customer names for orphan orders
merged['name'] = merged['name'].fillna('Unknown')
merged['segment'] = merged['segment'].fillna('Unknown')

# Fill missing amounts for customers with no orders
merged['amount'] = merged['amount'].fillna(0)

print(merged[['order_id', 'name', 'segment', 'amount']])
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Optimize a Slow Pandas Pipeline

**Scenario:** Processing 10M rows takes 45 minutes with this code. Optimize to under 5 minutes without switching to PySpark:

```python
df = pd.read_csv('events_10M.csv')  # 8 GB file → OOM!
df['date'] = df['timestamp'].apply(lambda x: x[:10])  # Slow apply
df['is_valid'] = df.apply(lambda row: row['amount'] > 0 and row['user_id'] is not None, axis=1)
```

<details>
<summary>✅ Solution</summary>

```python
# Fix 1: Read in chunks (avoid OOM on 8 GB file)
chunks = pd.read_csv('events_10M.csv', chunksize=500_000,
                     dtype={'user_id': 'str', 'amount': 'float32'})

results = []
for chunk in chunks:
    # Fix 2: Vectorized string operation (not apply with lambda)
    chunk['date'] = chunk['timestamp'].str[:10]
    
    # Fix 3: Vectorized boolean (not row-by-row apply)
    chunk['is_valid'] = (chunk['amount'] > 0) & (chunk['user_id'].notna())
    
    # Process only valid rows
    valid = chunk[chunk['is_valid']]
    results.append(valid.groupby('date').agg({'amount': 'sum', 'user_id': 'nunique'}))

# Combine all chunks
final = pd.concat(results).groupby(level=0).sum()
final.to_parquet('daily_summary.parquet')
```

**Key optimizations:**
- Chunked reading: constant memory regardless of file size
- `str[:10]` instead of `apply(lambda)`: 50-100x faster (vectorized C code)
- Boolean vectorized ops instead of `apply(axis=1)`: 100x faster
- Specify dtypes: `float32` halves memory vs `float64`

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Data Validation Framework

**Scenario:** Design a reusable Pandas-based validation framework that checks: not-null columns, value ranges, uniqueness, referential integrity, and row count thresholds. It should produce a report showing pass/fail per check.

<details>
<summary>✅ Solution</summary>

```python
from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    details: str
    failed_rows: int = 0

class DataValidator:
    def __init__(self, df: pd.DataFrame, name: str):
        self.df = df
        self.name = name
        self.results: list[ValidationResult] = []
    
    def not_null(self, columns: list[str]):
        for col in columns:
            nulls = self.df[col].isna().sum()
            self.results.append(ValidationResult(
                f"not_null({col})", nulls == 0,
                f"{nulls} nulls found" if nulls else "OK", nulls
            ))
        return self
    
    def unique(self, columns: list[str]):
        dupes = self.df.duplicated(subset=columns).sum()
        self.results.append(ValidationResult(
            f"unique({columns})", dupes == 0,
            f"{dupes} duplicates" if dupes else "OK", dupes
        ))
        return self
    
    def value_range(self, column: str, min_val=None, max_val=None):
        violations = 0
        if min_val is not None:
            violations += (self.df[column] < min_val).sum()
        if max_val is not None:
            violations += (self.df[column] > max_val).sum()
        self.results.append(ValidationResult(
            f"range({column},[{min_val},{max_val}])", violations == 0,
            f"{violations} violations" if violations else "OK", violations
        ))
        return self
    
    def row_count(self, min_rows: int, max_rows: Optional[int] = None):
        count = len(self.df)
        passed = count >= min_rows and (max_rows is None or count <= max_rows)
        self.results.append(ValidationResult(
            f"row_count(min={min_rows})", passed, f"actual={count}"
        ))
        return self
    
    def report(self) -> pd.DataFrame:
        return pd.DataFrame([vars(r) for r in self.results])
    
    def assert_all_passed(self):
        failed = [r for r in self.results if not r.passed]
        if failed:
            raise ValueError(f"{self.name}: {len(failed)} checks failed:\n" +
                           "\n".join(f"  - {r.check_name}: {r.details}" for r in failed))

# Usage
validator = DataValidator(orders_df, "fact_orders") \
    .not_null(['order_id', 'customer_id', 'amount']) \
    .unique(['order_id']) \
    .value_range('amount', min_val=0, max_val=1_000_000) \
    .row_count(min_rows=1000)

print(validator.report())
validator.assert_all_passed()  # Raises if any check fails
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `.loc` and `.iloc` in Pandas?**
A: `.loc` is label-based indexing—it uses the index label and column name to select data. `.iloc` is integer position-based—it uses 0-based integer positions regardless of index labels. Use `.loc` when you want to select by meaningful labels; use `.iloc` for positional slicing. Mixing them up when the index is non-integer causes subtle bugs.

**Q: How do you efficiently apply a function to a DataFrame column?**
A: In order of preference: (1) Vectorized Pandas/NumPy operations (`df['col'] * 2`), (2) `.str`/`.dt` accessor methods, (3) `.map()` for element-wise transforms using a dict or function, (4) `.apply()` with `axis=0/1`. Avoid `.apply()` in hot paths—it executes a Python loop under the hood and is 10–100x slower than vectorized operations.

**Q: What is the difference between `merge` and `join` in Pandas?**
A: `merge` is the general SQL-style join on arbitrary columns (`pd.merge(left, right, on='key', how='left')`). `join` is a convenience method that joins on the index by default (`df1.join(df2, on='key')`). `merge` is more explicit and flexible; `join` is concise when joining on indices. Both support inner, left, right, and outer joins.

**Q: How do you handle memory efficiently when processing a large CSV with Pandas?**
A: Use `pd.read_csv(path, chunksize=N)` to read in chunks (returns an iterator), specify `dtype` to avoid default object inference (e.g., `{'id': 'int32', 'amount': 'float32'}`), and use `usecols` to read only needed columns. For very large files, prefer Polars or Dask for out-of-core processing.

**Q: What is `groupby` with `transform` vs. `agg` and when do you use each?**
A: `agg` reduces groups to one row per group (produces a smaller DataFrame). `transform` returns a Series with the same index as the original DataFrame, broadcasting the group result back to each row. Use `transform` to add a group-level statistic (e.g., group mean) as a new column alongside the original data—without losing any rows.

**Q: How do you handle missing values in Pandas?**
A: Detect with `df.isna()` / `df.notna()`. Remove with `df.dropna(subset=['col'])`. Fill with `df.fillna(value)`, `df.fillna(method='ffill')` (forward fill), or `df.fillna(method='bfill')`. For imputation in ML pipelines, prefer sklearn's `SimpleImputer` over manual fillna to ensure train/test consistency.

**Q: What is `pd.Categorical` and when does it save memory?**
A: `pd.Categorical` stores a column of repeated string values as integer codes mapped to a category dictionary, rather than storing the full string for every row. For a column with 10 unique values in 10 million rows, Categorical uses ~40 MB instead of ~800 MB. Use for low-cardinality string columns (status, region, category).

**Q: What is the difference between `copy()` and a view in Pandas and why does the `SettingWithCopyWarning` appear?**
A: A slice of a DataFrame may return a view (same underlying data) or a copy (new data)—behavior depends on internal layout. Modifying a view changes the original DataFrame; modifying a copy does not. The `SettingWithCopyWarning` fires when Pandas detects you may be setting values on a copy. Always use `.copy()` explicitly after chained indexing to make intent clear, or use `.loc` to set values directly.

---

## 💼 Interview Tips

- Vectorization is the first Pandas performance principle—always attempt vectorized operations before reaching for `.apply()`. Being able to articulate why `.apply()` is slow (Python interpreter loop, no C-level batch execution) impresses interviewers.
- Know the memory optimization toolkit: `dtype` specification, `Categorical`, `usecols`, and `chunksize`. For a "how would you process a 50 GB CSV?" question, walk through each tool in order.
- The `SettingWithCopyWarning` is a common gotcha in production code. Explain it clearly—it shows you understand Pandas' copy vs. view semantics, which is a genuine source of bugs in pipelines.
- Senior interviewers ask when to move beyond Pandas: single-machine, in-memory datasets fit Pandas; multi-node or out-of-core needs Dask, Polars (single-machine but faster), or PySpark. Show you know the boundary.
- Connect Pandas to DE pipelines: Pandas is the lingua franca for data manipulation in Python, but production DE pipelines should validate data with Pydantic/Great Expectations, log record counts, and handle exceptions per-chunk rather than loading everything then crashing.
