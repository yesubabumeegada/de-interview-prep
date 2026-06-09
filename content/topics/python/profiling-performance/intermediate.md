---
title: "Profiling & Performance — Intermediate"
topic: python
subtopic: profiling-performance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, line_profiler, memory_profiler, vectorization, chunked-processing, generators]
---

# Profiling & Performance — Intermediate

Line-level profiling to find bottlenecks within functions, memory profiling to prevent OOM crashes, Pandas vectorization patterns, and generator-based pipelines for memory-efficient processing.

---

## line_profiler: Line-by-Line Timing

`cProfile` tells you which functions are slow. `line_profiler` tells you which *lines* within those functions are slow.

```bash
pip install line_profiler
```

```python
# profile_me.py
from line_profiler import LineProfiler


def process_dataframe(df):
    """A function with multiple steps — where is the time?"""
    import pandas as pd
    from datetime import datetime

    # Step 1: Filter
    filtered = df[df["amount"] > 100]

    # Step 2: Parse dates (potentially slow for large strings)
    filtered = filtered.copy()
    filtered["parsed_date"] = pd.to_datetime(filtered["date_str"])

    # Step 3: Compute metrics
    filtered["tax"] = filtered["amount"] * 0.1
    filtered["total"] = filtered["amount"] + filtered["tax"]

    # Step 4: String operations (often slow with apply)
    filtered["customer_tier"] = filtered["customer_id"].apply(
        lambda x: "vip" if x < 1000 else "standard"
    )

    # Step 5: Groupby aggregation
    result = filtered.groupby("parsed_date").agg(
        total_revenue=("total", "sum"),
        order_count=("total", "count"),
    )
    return result


# Profile it
lp = LineProfiler()
lp.add_function(process_dataframe)

import pandas as pd
import numpy as np

df = pd.DataFrame({
    "amount":      np.random.uniform(10, 1000, 100_000),
    "date_str":    ["2024-01-15"] * 100_000,
    "customer_id": np.random.randint(1, 5000, 100_000),
})

lp.run("process_dataframe(df)")
lp.print_stats()
```

### Reading line_profiler Output

```
Line #   Hits    Time    Per Hit   % Time   Line Contents
==============================================================
    12      1   1203.0   1203.0      0.1   filtered = df[df["amount"] > 100]
    15      1   4521.0   4521.0      0.4   filtered["parsed_date"] = pd.to_datetime(...)
    18      1    892.0    892.0      0.1   filtered["tax"] = filtered["amount"] * 0.1
    19      1    943.0    943.0      0.1   filtered["total"] = filtered["amount"] + filtered["tax"]
    22      1  934821.0  934821.0    98.9  filtered["customer_tier"] = filtered["customer_id"].apply(...)
    26      1   2341.0   2341.0      0.2  result = filtered.groupby(...)

The `apply()` on line 22 consumes 98.9% of runtime!
```

**Action:** replace `apply()` with a vectorized operation:

```python
import numpy as np

# Before: 934ms
filtered["customer_tier"] = filtered["customer_id"].apply(
    lambda x: "vip" if x < 1000 else "standard"
)

# After: ~5ms (vectorized)
filtered["customer_tier"] = np.where(filtered["customer_id"] < 1000, "vip", "standard")
```

---

## memory_profiler: Finding Memory Hogs

```bash
pip install memory_profiler
```

```python
# memory_demo.py
from memory_profiler import profile


@profile
def load_and_process(filepath: str):
    """Profile memory usage line by line."""
    import pandas as pd

    # Line 1: Read the CSV — memory spike here
    df = pd.read_csv(filepath)                        # +500 MB

    # Line 2: Filter — creates a new DataFrame copy
    df_filtered = df[df["status"] == "active"]        # +200 MB (copy!)

    # Line 3: Drop original — frees memory
    del df                                            # -500 MB

    # Line 4: Heavy transformation
    df_filtered["score"] = df_filtered["amount"] * 1.5  # +10 MB

    # Line 5: Convert to records — another copy
    records = df_filtered.to_dict("records")          # +300 MB

    return records

# Run with: python -m memory_profiler memory_demo.py
```

### Output

```
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
     5   52.3 MiB      52.3 MiB         1     def load_and_process(filepath):
     8  552.3 MiB     500.0 MiB         1       df = pd.read_csv(filepath)
    11  752.3 MiB     200.0 MiB         1       df_filtered = df[df["status"] == "active"]
    14  252.3 MiB    -500.0 MiB         1       del df
    17  262.3 MiB      10.0 MiB         1       df_filtered["score"] = ...
    20  562.3 MiB     300.0 MiB         1       records = df_filtered.to_dict("records")
```

Peak memory: 752 MB (before `del df`). If your machine has 8 GB RAM and this is just one step in a larger pipeline, the 752 MB peak may cause issues.

**Fix:** use `inplace` operations, `filter()` before loading, or chunked processing.

---

## Vectorization vs apply(): The Definitive Guide

`apply()` is a Python-level loop that invokes your function on each row/element. Vectorized operations use NumPy or Pandas C-level code that processes entire arrays at once.

```python
import pandas as pd
import numpy as np
import timeit

df = pd.DataFrame({
    "price":    np.random.uniform(1.0, 1000.0, 100_000),
    "quantity": np.random.randint(1, 100, 100_000),
    "discount": np.random.uniform(0.0, 0.5, 100_000),
    "category": np.random.choice(["A", "B", "C", "D"], 100_000),
})


# ── Simple arithmetic: always vectorize ──────────────────────────────────

# Bad: apply()
bad = timeit.timeit(
    lambda: df.apply(lambda r: r["price"] * r["quantity"] * (1 - r["discount"]), axis=1),
    number=5
)  # ~15 seconds for 100K rows

# Good: vectorized
good = timeit.timeit(
    lambda: df["price"] * df["quantity"] * (1 - df["discount"]),
    number=5
)  # ~0.05 seconds

print(f"apply(): {bad:.2f}s | vectorized: {good:.3f}s | speedup: {bad/good:.0f}x")


# ── Conditional assignment ────────────────────────────────────────────────

# Bad: apply()
def categorize_slow(row):
    if row["price"] < 100:
        return "budget"
    elif row["price"] < 500:
        return "mid"
    else:
        return "premium"

df.apply(categorize_slow, axis=1)  # Slow

# Good: np.select() for multiple conditions
conditions = [
    df["price"] < 100,
    df["price"] < 500,
]
choices = ["budget", "mid"]
df["tier"] = np.select(conditions, choices, default="premium")

# Or pd.cut() for range-based binning:
df["tier_cut"] = pd.cut(
    df["price"],
    bins=[0, 100, 500, float("inf")],
    labels=["budget", "mid", "premium"]
)


# ── String operations: use str accessor ──────────────────────────────────

# Bad: apply() on strings
df["clean_category"] = df["category"].apply(lambda x: x.strip().upper())

# Good: str accessor (vectorized string operations)
df["clean_category"] = df["category"].str.strip().str.upper()


# ── When apply() is acceptable ──────────────────────────────────────────

# 1. Complex business logic that can't be vectorized easily
# (but profile it first — often there IS a vectorized equivalent)

# 2. Operations on less-than-a-million rows where speed doesn't matter
# for the pipeline's overall runtime

# 3. Functions involving multiple column lookups and conditional logic
# with no clean vectorized expression — BUT consider building a lookup
# dict and using .map() instead:

tier_map = {"A": "premium", "B": "mid", "C": "budget", "D": "budget"}
df["tier_from_category"] = df["category"].map(tier_map)  # Fast .map() replaces apply()
```

---

## Chunked Processing for Large Files

```python
import pandas as pd
from pathlib import Path
from typing import Generator, Iterator


def read_csv_in_chunks(
    filepath: str,
    chunksize: int = 100_000,
    filters: dict = None,
) -> Iterator[pd.DataFrame]:
    """
    Generator that yields processed chunks.
    Memory usage stays bounded at one chunk at a time.
    """
    for chunk in pd.read_csv(filepath, chunksize=chunksize):
        # Apply filters immediately — reduce memory before processing
        if filters:
            for col, value in filters.items():
                chunk = chunk[chunk[col] == value]

        if not chunk.empty:
            yield chunk


def process_large_csv(filepath: str, output_path: str):
    """
    Process a multi-GB CSV without loading it all into RAM.
    Writes output incrementally.
    """
    output = Path(output_path)
    first_chunk = True

    total_rows = 0
    for chunk in read_csv_in_chunks(filepath, filters={"status": "active"}):
        # Process each chunk
        chunk["revenue"] = chunk["price"] * chunk["quantity"] * (1 - chunk["discount"])
        chunk["tier"] = pd.cut(
            chunk["price"],
            bins=[0, 100, 500, float("inf")],
            labels=["budget", "mid", "premium"]
        )

        # Write chunk — append to output file
        mode = "w" if first_chunk else "a"
        header = first_chunk
        chunk.to_csv(output_path, mode=mode, header=header, index=False)
        first_chunk = False
        total_rows += len(chunk)
        print(f"Processed {total_rows:,} rows...")

    print(f"Total: {total_rows:,} rows written to {output_path}")


# For Parquet output (much better for DE use cases):
def process_large_csv_to_parquet(filepath: str, output_dir: str):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = None
    writer = None

    try:
        for chunk in read_csv_in_chunks(filepath, chunksize=200_000):
            # Transform
            chunk["revenue"] = chunk["price"] * chunk["quantity"]

            # Convert to Arrow table for Parquet writing
            table = pa.Table.from_pandas(chunk)

            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(f"{output_dir}/output.parquet", schema)

            writer.write_table(table)
    finally:
        if writer:
            writer.close()
```

---

## Generator-Based Pipelines

Generators process data one element at a time, holding only the current element in memory instead of the entire dataset.

```python
from typing import Generator, Iterator


def read_json_records(filepath: str) -> Generator[dict, None, None]:
    """Generator: yield one record at a time from a JSON Lines file."""
    import json
    with open(filepath) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def validate_record(record: dict) -> dict | None:
    """Return record if valid, None if invalid."""
    if not record.get("user_id") or record.get("amount", 0) < 0:
        return None
    return record


def enrich_record(record: dict) -> dict:
    """Enrich one record."""
    record["revenue"] = record["amount"] * (1 - record.get("discount", 0))
    return record


def pipeline(filepath: str) -> Generator[dict, None, None]:
    """
    Generator pipeline: read → validate → enrich, one record at a time.
    Memory usage: O(1) — only one record in memory at any time.
    """
    for record in read_json_records(filepath):
        validated = validate_record(record)
        if validated is None:
            continue  # Skip invalid records
        yield enrich_record(validated)


# Process without materializing the entire dataset:
output_records = []
for record in pipeline("large_events.jsonl"):
    output_records.append(record)
    # Could also write directly: write_to_db(record) instead of accumulating

# Even better: batch write for efficiency
def pipeline_to_db(filepath: str, db_writer, batch_size: int = 1000):
    """Process pipeline and write in batches for efficiency."""
    batch = []
    for record in pipeline(filepath):
        batch.append(record)
        if len(batch) >= batch_size:
            db_writer.write_many(batch)
            batch = []
    if batch:  # Write remaining records
        db_writer.write_many(batch)
```

---

## Key Takeaways

1. **`line_profiler`** tells you which specific lines are slow within a function — use it after `cProfile` identifies the slow function.
2. **`memory_profiler`** shows peak memory per line — essential for understanding why a pipeline crashes on large data.
3. **`apply()` is almost always the bottleneck** in Pandas — `np.where()`, `np.select()`, `pd.cut()`, `.map()`, and vectorized column arithmetic are the alternatives.
4. **Chunked processing** bounds memory usage to `O(chunk_size)` instead of `O(file_size)` — the standard fix for "loads entire file into memory."
5. **Generator pipelines** are the Python equivalent of lazy evaluation — process arbitrarily large datasets with constant memory.
