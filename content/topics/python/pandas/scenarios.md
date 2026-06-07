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
