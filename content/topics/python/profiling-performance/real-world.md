---
title: "Profiling & Performance — Real-World Patterns"
topic: python
subtopic: profiling-performance
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [python, performance, pandas, chunked-processing, JSON-streaming, profiling-production]
---

# Profiling & Performance — Real-World Patterns

Production optimization patterns with concrete before/after code.

---

## Pattern 1: Optimizing a Slow Pandas ETL (apply → vectorize)

This is the most common DE performance fix. A Pandas ETL using `apply()` that takes 20 minutes replaced with vectorized operations that take 30 seconds.

```python
import pandas as pd
import numpy as np
import timeit
import cProfile


# ── BEFORE: The slow ETL (using apply everywhere) ─────────────────────────

def slow_etl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Original ETL — uses apply() for everything.
    Runtime on 1M rows: ~22 minutes.
    """

    # Problem 1: apply() for arithmetic
    df["total_revenue"] = df.apply(
        lambda r: r["price"] * r["quantity"] * (1 - r["discount"]),
        axis=1
    )  # ~12 min for 1M rows

    # Problem 2: apply() for conditional
    def categorize_order(row):
        if row["total_revenue"] >= 1000:
            return "large"
        elif row["total_revenue"] >= 100:
            return "medium"
        else:
            return "small"

    df["order_size"] = df.apply(categorize_order, axis=1)  # ~6 min

    # Problem 3: apply() for string operation
    df["normalized_sku"] = df["sku"].apply(lambda x: x.strip().upper())  # ~2 min

    # Problem 4: apply() for date parsing
    df["order_month"] = df["order_date"].apply(lambda x: x[:7])  # ~2 min

    return df


# ── AFTER: Optimized ETL (vectorized operations) ─────────────────────────

def fast_etl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized ETL — uses vectorized operations throughout.
    Runtime on 1M rows: ~3 seconds.
    """

    # Fix 1: Vectorized arithmetic (same formula, C-speed)
    df["total_revenue"] = df["price"] * df["quantity"] * (1 - df["discount"])
    # ~0.1s for 1M rows

    # Fix 2: np.select() for conditional categorization
    conditions = [
        df["total_revenue"] >= 1000,
        df["total_revenue"] >= 100,
    ]
    choices = ["large", "medium"]
    df["order_size"] = np.select(conditions, choices, default="small")
    # ~0.2s for 1M rows

    # Fix 3: Vectorized string operations via str accessor
    df["normalized_sku"] = df["sku"].str.strip().str.upper()
    # ~0.3s for 1M rows

    # Fix 4: Vectorized string slicing via str accessor
    df["order_month"] = df["order_date"].str[:7]
    # Or if it's already a datetime: df["order_date"].dt.to_period("M").astype(str)
    # ~0.05s for 1M rows

    return df


# Benchmark
n = 1_000_000
df = pd.DataFrame({
    "price":      np.random.uniform(1.0, 200.0, n),
    "quantity":   np.random.randint(1, 50, n),
    "discount":   np.random.uniform(0.0, 0.4, n),
    "sku":        [f" SKU-{i:06d} " for i in range(n)],
    "order_date": ["2024-01-15"] * n,
})

slow_time = timeit.timeit(lambda: slow_etl(df.copy()), number=1)
fast_time = timeit.timeit(lambda: fast_etl(df.copy()), number=1)
print(f"Slow (apply):  {slow_time/60:.1f} minutes")
print(f"Fast (vector): {fast_time:.1f} seconds")
print(f"Speedup:       {slow_time/fast_time:.0f}x")
# Slow (apply):  ~22 minutes
# Fast (vector): ~3 seconds
# Speedup:       ~440x
```

---

## Pattern 2: Chunked File Processor for Large CSVs

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging
import time

logger = logging.getLogger(__name__)


class ChunkedCsvProcessor:
    """
    Memory-efficient processor for large CSV files.
    Processes one chunk at a time, writes output as Parquet.
    Memory usage: O(chunk_size) not O(file_size).
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        chunk_size: int = 200_000,
        dtype_spec: dict = None,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.dtype_spec = dtype_spec or {}
        self._stats = {"chunks": 0, "rows_read": 0, "rows_written": 0}

    def transform_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Override this method with your pipeline's transformation logic.
        Default: basic cleaning.
        """
        # Filter invalid rows
        chunk = chunk[chunk["amount"].notna() & (chunk["amount"] > 0)]

        # Vectorized transformations
        chunk = chunk.copy()
        chunk["revenue"] = chunk["price"] * chunk["quantity"] * (1 - chunk.get("discount", 0))
        chunk["customer_tier"] = np.where(chunk["customer_id"] < 10000, "enterprise", "smb")
        chunk["normalized_email"] = chunk["email"].str.strip().str.lower()

        return chunk

    def run(self) -> dict:
        """Process the full CSV file in chunks."""
        start = time.perf_counter()
        writer = None
        schema = None

        try:
            reader = pd.read_csv(
                self.input_path,
                chunksize=self.chunk_size,
                dtype=self.dtype_spec,
                low_memory=False,
            )

            for chunk_df in reader:
                self._stats["chunks"] += 1
                self._stats["rows_read"] += len(chunk_df)

                # Apply transformation
                transformed = self.transform_chunk(chunk_df)

                if transformed.empty:
                    continue

                # Convert to Arrow and write to Parquet
                arrow_table = pa.Table.from_pandas(transformed, preserve_index=False)

                if writer is None:
                    schema = arrow_table.schema
                    writer = pq.ParquetWriter(
                        self.output_path,
                        schema,
                        compression="snappy",
                    )

                writer.write_table(arrow_table)
                self._stats["rows_written"] += len(transformed)

                if self._stats["chunks"] % 10 == 0:
                    logger.info(
                        "Progress: chunk %d, rows read=%d, written=%d",
                        self._stats["chunks"],
                        self._stats["rows_read"],
                        self._stats["rows_written"],
                    )

        finally:
            if writer:
                writer.close()

        elapsed = time.perf_counter() - start
        self._stats["duration_s"] = round(elapsed, 2)
        self._stats["throughput_rows_per_sec"] = int(
            self._stats["rows_read"] / elapsed
        ) if elapsed > 0 else 0

        logger.info("Processing complete: %s", self._stats)
        return self._stats


# Usage:
class OrdersProcessor(ChunkedCsvProcessor):
    def transform_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        chunk = chunk[chunk["status"] == "active"]  # Filter first
        chunk = chunk.copy()
        chunk["revenue"] = chunk["price"] * chunk["qty"]
        chunk["year_month"] = pd.to_datetime(chunk["order_date"]).dt.to_period("M").astype(str)
        return chunk


processor = OrdersProcessor(
    input_path="s3://bucket/raw/orders_50gb.csv",
    output_path="s3://bucket/processed/orders.parquet",
    chunk_size=500_000,
    dtype_spec={"order_id": str, "customer_id": str, "price": float},
)
stats = processor.run()
print(f"Processed {stats['rows_read']:,} rows in {stats['duration_s']:.0f}s "
      f"({stats['throughput_rows_per_sec']:,} rows/sec)")
```

---

## Pattern 3: Memory-Efficient JSON Streaming Parser

```python
import json
import ijson  # Streaming JSON parser
from typing import Generator
import gzip


def stream_json_array(filepath: str, array_key: str = "records") -> Generator[dict, None, None]:
    """
    Stream a large JSON file without loading it into memory.
    Handles both regular and gzipped JSON.

    Example JSON structure:
    {
        "metadata": {"source": "crm", "total": 5000000},
        "records": [
            {"id": 1, "name": "Alice", ...},
            {"id": 2, "name": "Bob", ...},
            ...5M records...
        ]
    }

    Without streaming: pd.read_json("5gb_file.json") → OOM
    With streaming: processes one record at a time → O(1) memory
    """
    open_func = gzip.open if filepath.endswith(".gz") else open

    with open_func(filepath, "rb") as f:
        # ijson parses the JSON lazily — yields one item at a time from the array
        parser = ijson.items(f, f"{array_key}.item")
        for record in parser:
            yield record


def process_large_json_pipeline(input_path: str, output_path: str):
    """
    Process a 5 GB JSON file with millions of records using streaming.
    Peak memory: ~50 MB (one batch at a time).
    Without streaming: would require ~15 GB RAM.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None
    batch = []
    batch_size = 10_000
    total_records = 0
    valid_records = 0

    try:
        for record in stream_json_array(input_path, array_key="customers"):
            total_records += 1

            # Validate and transform each record
            if not record.get("customer_id") or not record.get("email"):
                continue  # Skip invalid records

            cleaned = {
                "customer_id": int(record["customer_id"]),
                "email":       record["email"].strip().lower(),
                "status":      record.get("status", "unknown"),
                "ltv":         float(record.get("lifetime_value", 0.0)),
            }
            batch.append(cleaned)
            valid_records += 1

            # Write in batches
            if len(batch) >= batch_size:
                table = pa.table({
                    "customer_id": pa.array([r["customer_id"] for r in batch], type=pa.int64()),
                    "email":       pa.array([r["email"] for r in batch]),
                    "status":      pa.array([r["status"] for r in batch]),
                    "ltv":         pa.array([r["ltv"] for r in batch], type=pa.float64()),
                })
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
                writer.write_table(table)
                batch = []

        # Write remaining records
        if batch:
            table = pa.table({
                "customer_id": pa.array([r["customer_id"] for r in batch], type=pa.int64()),
                "email":       pa.array([r["email"] for r in batch]),
                "status":      pa.array([r["status"] for r in batch]),
                "ltv":         pa.array([r["ltv"] for r in batch], type=pa.float64()),
            })
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
            writer.write_table(table)

    finally:
        if writer:
            writer.close()

    print(f"Processed {total_records:,} records, {valid_records:,} valid, "
          f"written to {output_path}")
```

---

## Pattern 4: Profiling and Fixing a Slow Python Ingestion Script

```python
"""
Before optimization: a real-world ingestion script that runs for 4 hours.
Profiling reveals the bottleneck. After fix: 12 minutes.
"""

# ── STEP 1: Profile the existing script ──────────────────────────────────
import cProfile
import pstats
import io


def profile_ingestion():
    profiler = cProfile.Profile()
    profiler.enable()

    # Run a representative sample (1000 records, not 10M)
    run_ingestion_pipeline(limit=1000)

    profiler.disable()
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats("cumulative")
    ps.print_stats(15)
    print(s.getvalue())

# cProfile output reveals:
# cumtime   function
# 3521.4s   requests.get  ← 98% of time in HTTP requests (serial API calls)
#   71.2s   pandas.DataFrame.apply  ← 2% in pandas apply
#    0.8s   everything else

# FINDING: 98% of time is serial HTTP requests to the CRM API (I/O-bound)
# The pandas apply is 2% — not worth optimizing first


# ── STEP 2: Fix the bottleneck (async HTTP) ───────────────────────────────
import asyncio
import aiohttp
import time


async def fetch_all_records_async(record_ids: list[int]) -> list[dict]:
    """Replace serial requests.get() with concurrent aiohttp."""
    semaphore = asyncio.Semaphore(20)  # Max 20 concurrent API calls

    async def fetch_one(session, record_id):
        async with semaphore:
            async with session.get(
                f"https://crm.internal/api/records/{record_id}",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                return await resp.json()

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, rid) for rid in record_ids]
        return await asyncio.gather(*tasks, return_exceptions=True)


# ── STEP 3: Fix the secondary bottleneck (pandas apply) ──────────────────
import pandas as pd
import numpy as np


def transform_records(df: pd.DataFrame) -> pd.DataFrame:
    """Replace apply() with vectorized operations."""

    # Before: apply() — 71s for 10M records
    # df["score"] = df.apply(lambda r: r["ltv"] * 0.1 + r["engagement"] * 0.9, axis=1)

    # After: vectorized — 0.2s for 10M records
    df["score"] = df["ltv"] * 0.1 + df["engagement"] * 0.9

    # Before: apply() for categorization — 45s
    # df["tier"] = df["score"].apply(lambda x: "gold" if x > 100 else "silver" if x > 50 else "bronze")

    # After: np.select() — 0.1s
    conditions = [df["score"] > 100, df["score"] > 50]
    df["tier"] = np.select(conditions, ["gold", "silver"], default="bronze")

    return df


# ── STEP 4: Measure the improvement ──────────────────────────────────────
async def run_optimized_pipeline(record_ids: list[int]) -> pd.DataFrame:
    start = time.perf_counter()

    # Fetch all records concurrently
    raw_records = await fetch_all_records_async(record_ids)
    fetch_time = time.perf_counter() - start
    print(f"Fetch: {fetch_time:.1f}s ({len(record_ids)/fetch_time:.0f} records/sec)")

    # Transform
    t = time.perf_counter()
    valid = [r for r in raw_records if isinstance(r, dict)]
    df = pd.DataFrame(valid)
    df = transform_records(df)
    transform_time = time.perf_counter() - t
    print(f"Transform: {transform_time:.1f}s")

    total = time.perf_counter() - start
    print(f"Total: {total/60:.1f} minutes (was 4 hours → {240/(total/60):.0f}x speedup)")
    return df


# asyncio.run(run_optimized_pipeline(record_ids))
# Fetch: 38.2s (262 records/sec with 20 concurrent connections)
# Transform: 0.3s
# Total: 0.7 minutes (was 4 hours → 343x speedup)
```

---

## Key Takeaways

1. **Profile FIRST** — `cProfile` on a sample reveals whether the bottleneck is I/O (async fix), CPU-Python (vectorize fix), or something else.
2. **The `apply()` → vectorize swap** is the most common DE optimization — 100-1000x speedup for the same logic.
3. **Chunked CSV processing** with `pd.read_csv(chunksize=N)` + PyArrow Parquet writer is the production pattern for files larger than available RAM.
4. **`ijson`** for JSON streaming is essential when source APIs send massive JSON arrays — load-all-into-memory causes OOM at scale.
5. **Measure before and after** — if you can't quantify the speedup, you can't justify the change or know if your fix actually helped.
