---
title: "Profiling & Performance — Fundamentals"
topic: python
subtopic: profiling-performance
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, profiling, cProfile, timeit, performance, anti-patterns, DE]
---

# Profiling & Performance — Fundamentals

"Make it work, then make it right, then make it fast." — Kent Beck. Most DE performance problems come from not following the middle step: before optimizing, you need to know WHERE the bottleneck actually is. Profiling tells you that.

---

## Why Profile Before Optimizing?

```python
# A common DE mistake: optimizing the wrong thing
import time

def process_data(records: list[dict]) -> list[dict]:
    result = []
    for record in records:
        # Step 1: Parse dates (fast)
        record["date"] = parse_date(record["date_str"])
        # Step 2: Lookup user details (SLOW — DB call per record!)
        record["user"] = db.lookup_user(record["user_id"])
        # Step 3: Format output (fast)
        result.append(format_record(record))
    return result

# Without profiling, you might spend 2 hours optimizing
# the date parsing (fast) instead of fixing the N+1 DB query (slow).
# Profiling shows: 99% of time is in db.lookup_user()
```

**Rule:** never optimize based on intuition. Profile first, then optimize the bottleneck.

---

## cProfile: Finding Function-Level Bottlenecks

`cProfile` is the standard Python profiler. It measures time spent in each function call.

```python
import cProfile
import pstats
import io


def slow_etl(records: list[dict]) -> list[dict]:
    """A realistic ETL with some obvious bottlenecks."""
    import time
    result = []
    for record in records:
        # Simulate slow computation per record
        cleaned = clean_record(record)         # Fast: 0.001s
        validated = validate_record(cleaned)   # Medium: 0.01s
        enriched = enrich_with_api(validated)  # SLOW: 0.1s (external call!)
        result.append(enriched)
    return result


# Method 1: Profile from command line
# python -m cProfile -s cumtime my_etl_script.py

# Method 2: Profile programmatically
def profile_function(func, *args, **kwargs):
    """Profile a function and print stats."""
    profiler = cProfile.Profile()
    profiler.enable()
    result = func(*args, **kwargs)
    profiler.disable()

    # Print stats sorted by cumulative time
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(20)  # Top 20 functions
    print(stream.getvalue())

    return result


# Profile the ETL with 100 records
records = [{"user_id": i, "amount": i * 10, "date_str": "2024-01-15"} for i in range(100)]
result = profile_function(slow_etl, records)
```

### Reading cProfile Output

```
ncalls  tottime  percall  cumtime  percall filename:lineno(function)
   100    0.003    0.0000   10.532    0.105 etl.py:15(slow_etl)
   100   10.012    0.100   10.489    0.105 etl.py:22(enrich_with_api)
   100    0.021    0.0002    0.477    0.005 etl.py:19(validate_record)
   100    0.004    0.00004   0.066    0.0006 etl.py:17(clean_record)

Columns:
- ncalls:   How many times the function was called
- tottime:  Time spent IN this function (excluding sub-calls)
- percall:  tottime / ncalls
- cumtime:  Total time including all sub-calls — the most useful for finding bottlenecks
- percall:  cumtime / ncalls
```

**How to read it:** sort by `cumtime` (largest first). The top entries are your bottlenecks.

In the example: `enrich_with_api` has `cumtime = 10.489s` out of total `10.532s`. That's 99.6% of the runtime → **this is the only thing worth optimizing**.

---

## timeit: Micro-Benchmarking

Use `timeit` when you want to compare two implementations of the same operation.

```python
import timeit


# Compare two ways to filter a list
records = [{"user_id": i, "amount": i * 10, "active": i % 2 == 0} for i in range(10_000)]

# Option A: List comprehension
def filter_with_list_comp(records):
    return [r for r in records if r["active"]]

# Option B: filter() builtin
def filter_with_builtin(records):
    return list(filter(lambda r: r["active"], records))

# Time both (1000 repetitions for stable measurement)
list_comp_time = timeit.timeit(
    lambda: filter_with_list_comp(records),
    number=1000
)
builtin_time = timeit.timeit(
    lambda: filter_with_builtin(records),
    number=1000
)

print(f"List comprehension: {list_comp_time:.3f}s total, {list_comp_time/1000*1000:.3f}ms per call")
print(f"filter() builtin:   {builtin_time:.3f}s total, {builtin_time/1000*1000:.3f}ms per call")
# List comprehension is typically faster and more readable
```

### Comparing Pandas Operations

```python
import pandas as pd
import timeit

df = pd.DataFrame({
    "user_id": range(100_000),
    "amount":  [float(i * 1.5) for i in range(100_000)],
    "status":  ["active" if i % 3 else "inactive" for i in range(100_000)],
})

# Option A: apply() — slow Python loop
def categorize_with_apply(df):
    return df["amount"].apply(lambda x: "high" if x > 50000 else "low")

# Option B: vectorized numpy operation
def categorize_vectorized(df):
    import numpy as np
    return np.where(df["amount"] > 50000, "high", "low")

apply_time = timeit.timeit(lambda: categorize_with_apply(df), number=10)
vectorized_time = timeit.timeit(lambda: categorize_vectorized(df), number=10)

print(f"apply():     {apply_time:.3f}s")
print(f"vectorized:  {vectorized_time:.3f}s")
# apply():     2.3s  (slow — Python-level loop)
# vectorized:  0.04s (fast — C-level NumPy operation)
# Speedup: ~57x
```

---

## Common DE Performance Anti-Patterns

### 1. Row-by-Row Processing (N+1 Problem)

```python
import pandas as pd

df = pd.DataFrame({"value": range(1_000_000)})

# ANTI-PATTERN: iterrows() — Python loop over pandas rows
# Slow: ~5-10 minutes for 1M rows
for idx, row in df.iterrows():
    df.at[idx, "doubled"] = row["value"] * 2

# FIX: Vectorized operation — same result, ~1000x faster
df["doubled"] = df["value"] * 2  # ~0.01s
```

### 2. Reading Entire File into Memory

```python
# ANTI-PATTERN: loading a 50 GB CSV entirely into RAM
import pandas as pd

df = pd.read_csv("huge_file.csv")  # OOM if RAM < file size
filtered = df[df["status"] == "active"]

# FIX: Use chunked reading
chunk_size = 100_000
results = []
for chunk in pd.read_csv("huge_file.csv", chunksize=chunk_size):
    filtered_chunk = chunk[chunk["status"] == "active"]
    results.append(filtered_chunk)

df_filtered = pd.concat(results, ignore_index=True)
```

### 3. String Concatenation in a Loop

```python
# ANTI-PATTERN: building a string in a loop — O(n²) memory
result = ""
for item in items:
    result += str(item) + ","  # Creates a new string object on every iteration

# FIX: join()
result = ",".join(str(item) for item in items)
```

### 4. Using a List Where a Set Is Appropriate

```python
# ANTI-PATTERN: checking membership in a list — O(n) per lookup
active_user_ids = [101, 202, 303, 404, 505]  # list

for record in records:
    if record["user_id"] in active_user_ids:  # O(n) scan per record!
        process(record)

# FIX: use a set — O(1) membership check
active_user_ids_set = set(active_user_ids)

for record in records:
    if record["user_id"] in active_user_ids_set:  # O(1) hash lookup
        process(record)

# For 1M records with 10K IDs: list = ~10B operations, set = ~1M operations
```

### 5. Repeated Heavy Computation in a Loop

```python
# ANTI-PATTERN: calling expensive function inside the loop
import re

pattern_string = r"\d{4}-\d{2}-\d{2}"

for record in records:
    # re.compile() called on every iteration — compiles the regex 1M times!
    match = re.compile(pattern_string).match(record["date"])

# FIX: compile outside the loop
compiled_pattern = re.compile(pattern_string)

for record in records:
    match = compiled_pattern.match(record["date"])  # Just a match, no compile
```

---

## The Profiling Workflow

```
1. Suspect something is slow? → Don't guess, profile it.
2. Run cProfile → identify the function with the highest cumtime.
3. Is it I/O? (network, disk, DB) → Can it be batched, cached, or made async?
4. Is it computation? → Is there a vectorized/built-in alternative?
5. Use timeit to compare the before/after of your fix.
6. Only after profiling confirms improvement → merge the change.

Rule: if you can't measure the improvement, you shouldn't ship the "optimization."
```

---

## Key Takeaways for Junior DEs

1. **Profile before optimizing** — `cProfile` + `sort_stats("cumulative")` shows you where time is actually spent.
2. **`timeit`** is for micro-benchmarks — comparing two implementations of the same operation.
3. **Avoid `iterrows()`** — it's a Python-level loop over a DataFrame; use vectorized operations instead.
4. **Chunked reading** handles files larger than RAM — `pd.read_csv(..., chunksize=N)` processes the file in segments.
5. **Lists for membership testing** on large sets is O(n) — use `set()` for O(1) lookups.
6. **Compile regex outside loops** — `re.compile()` is expensive; call it once, reuse the compiled object.
