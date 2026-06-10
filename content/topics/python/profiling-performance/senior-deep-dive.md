---
title: "Profiling & Performance — Senior Deep Dive"
topic: python
subtopic: profiling-performance
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, PySpark-UDF, Arrow, py-spy, memory-leak, CPU-vs-IO, profiling-production]
---

# Profiling & Performance — Senior Deep Dive

At the senior level, performance means understanding Python UDF overhead in PySpark, profiling async code, detecting memory leaks, using `py-spy` for production profiling without code changes, and precisely diagnosing CPU-bound vs I/O-bound bottlenecks.

---

## Profiling PySpark Python UDFs (and Why They're Slow)

Python UDFs in PySpark are infamous for poor performance. Understanding why helps you choose the right fix.

### The Serialization Problem

```
Without UDF (Spark native operations):
Executor JVM → operates on JVM objects directly → very fast

With Python UDF:
Executor JVM → serialize row to Python pickle → cross-process IPC → Python process
→ deserialize row → run Python function → serialize result → cross-process IPC
→ JVM deserializes result

Each UDF call involves 2 serializations + 2 IPC round-trips per row.
For 100M rows: 200M serializations → catastrophic performance.
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

spark = SparkSession.builder.appName("udf-perf").getOrCreate()

df = spark.range(1_000_000).withColumn("amount", (col("id") * 1.5).cast("double"))

# ── Method 1: Native Spark (fastest) ─────────────────────────────────────
# No Python involved at all — pure JVM/C++
result_native = df.withColumn("tax", col("amount") * 0.1)
# ~0.5 seconds for 1M rows ✓


# ── Method 2: Python UDF (slowest) ───────────────────────────────────────
@udf(returnType=DoubleType())
def compute_tax_udf(amount: float) -> float:
    return amount * 0.1

# This triggers Python UDF overhead for EVERY row
result_udf = df.withColumn("tax", compute_tax_udf(col("amount")))
# ~45 seconds for 1M rows ✗ (90x slower than native!)


# ── Method 3: Pandas UDF (good balance) ──────────────────────────────────
# Uses Apache Arrow for batch serialization — much faster than row-by-row
@pandas_udf(DoubleType())
def compute_tax_pandas_udf(amounts: pd.Series) -> pd.Series:
    return amounts * 0.1

result_pandas_udf = df.withColumn("tax", compute_tax_pandas_udf(col("amount")))
# ~1.5 seconds for 1M rows ✓ (only 3x overhead vs native)


# ── Arrow optimization for UDFs ──────────────────────────────────────────
# Enable Arrow-based Pandas UDF serialization (should be on by default in Spark 3+)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")


# ── When you MUST use a Python UDF (complex business logic) ─────────────
# Use Pandas UDF with batch processing

@pandas_udf(DoubleType())
def complex_scoring(amounts: pd.Series, categories: pd.Series) -> pd.Series:
    """Complex business logic that can't be expressed in native Spark SQL."""
    # All pandas operations here — vectorized within the batch
    base_score = amounts * 0.1
    category_multiplier = categories.map({"A": 1.2, "B": 1.0, "C": 0.8}).fillna(1.0)
    return base_score * category_multiplier

# This processes batches of rows as Pandas Series — much faster than row-by-row
```

---

## Profiling Async Code

Profiling async code requires different tools because the event loop's cooperative scheduling means standard synchronous profilers don't capture wait times accurately.

```python
import asyncio
import time
from contextlib import asynccontextmanager


# Method 1: Manual timing with context manager
@asynccontextmanager
async def async_timer(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"[{label}] {elapsed:.3f}s")


async def profiled_pipeline():
    async with async_timer("fetch_all_apis"):
        results = await fetch_from_50_apis()

    async with async_timer("process_results"):
        processed = await process_batch(results)

    async with async_timer("write_to_db"):
        await write_to_postgres(processed)


# Method 2: asyncio debug mode — logs coroutines that take too long
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with debug mode — automatically logs slow callbacks
loop = asyncio.new_event_loop()
loop.set_debug(True)
loop.slow_callback_duration = 0.1  # Warn if a callback takes > 100ms
asyncio.set_event_loop(loop)
# Now any coroutine that takes > 100ms without yielding gets logged


# Method 3: yappi — supports async profiling
# pip install yappi
import yappi

async def profile_async_function():
    yappi.set_clock_type("wall")  # Wall time (includes I/O wait)
    yappi.start()

    await run_full_async_pipeline()

    yappi.stop()

    # Get stats per coroutine
    yappi.get_func_stats().print_all()
    # Shows: total_time (including await), ncall, avg_per_call per coroutine

asyncio.run(profile_async_function())
```

---

## Memory Leak Detection

Python memory leaks are subtle because the garbage collector handles most cleanup. Leaks typically come from global containers that grow unboundedly.

```python
import tracemalloc
import gc


def find_memory_leaks():
    """Use tracemalloc to detect what's growing over time."""
    tracemalloc.start()

    # Take snapshot BEFORE the operation
    snapshot_before = tracemalloc.take_snapshot()

    # Run the potentially leaky code N times
    for _ in range(100):
        run_ingestion_batch()

    gc.collect()  # Force garbage collection before measuring

    # Take snapshot AFTER
    snapshot_after = tracemalloc.take_snapshot()

    # Compare: show what grew
    top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
    print("\nTop 10 memory changes:")
    for stat in top_stats[:10]:
        print(stat)
    # Example output:
    # +1024 B: my_pipeline.py:42: global_cache = {} ← growing dict!

    tracemalloc.stop()


# Common memory leak patterns in DE code:

# LEAK 1: Global cache that grows without bound
global_cache = {}  # Never evicted

def process_record(record):
    key = record["user_id"]
    if key not in global_cache:
        global_cache[key] = fetch_user_details(key)  # Never removed!
    return enrich(record, global_cache[key])

# FIX: Use functools.lru_cache with maxsize, or time-based eviction:
from functools import lru_cache

@lru_cache(maxsize=10_000)  # Evicts LRU entries when full
def fetch_user_details_cached(user_id: int) -> dict:
    return fetch_user_details(user_id)


# LEAK 2: Unclosed file handles
def bad_reader(filepath):
    f = open(filepath)  # File handle never closed if exception occurs
    data = f.read()
    return data  # f.close() never called!

def good_reader(filepath):
    with open(filepath) as f:  # Always closed, even on exception
        return f.read()


# LEAK 3: Growing list in a long-running process
class StreamProcessor:
    def __init__(self):
        self.processed_ids = []  # LEAK: grows forever

    def process(self, record):
        self.processed_ids.append(record["id"])  # Never trimmed
        # After 1M records: this list uses ~80 MB and growing

# FIX: Use a bounded collection
from collections import deque

class FixedStreamProcessor:
    def __init__(self, dedup_window: int = 10_000):
        self.recent_ids = deque(maxlen=dedup_window)  # Bounded!
        self.recent_ids_set = set()

    def process(self, record):
        record_id = record["id"]
        if record_id in self.recent_ids_set:
            return  # Deduplicate within window
        if len(self.recent_ids) == self.recent_ids.maxlen:
            # Remove oldest ID from set when deque evicts it
            oldest = self.recent_ids[0]
            self.recent_ids_set.discard(oldest)
        self.recent_ids.append(record_id)
        self.recent_ids_set.add(record_id)
```

---

## py-spy: Production Profiling Without Code Changes

`py-spy` attaches to a running Python process and samples its call stack without modifying the code or restarting the process.

```bash
# Install
pip install py-spy

# Profile a running process (no code changes required)
py-spy top --pid <PID>
# Shows a top-like view of live function call percentages

# Record a flame graph (best for finding bottlenecks)
py-spy record -o profile.svg --pid <PID> --duration 30
# Opens profile.svg in a browser: wide functions = slow = bottleneck

# Dump current stack traces (useful for diagnosing hangs)
py-spy dump --pid <PID>

# Profile from the start of a new process
py-spy record -o profile.svg -- python my_pipeline.py
```

### Reading a Flame Graph

```
The flame graph shows the call stack:
- Wide bars = function that runs a lot (hot path)
- Narrow bars = fast functions, not the bottleneck
- Look for the WIDEST bar that's a LEAF (no children) → that's your bottleneck

Example:
[========== ingest_all_records ==========]
[=== fetch_records ===] [== transform == ] [== write ==]
[== http_get ==]         [= pandas apply =]
          ↑ leaf, very wide            ↑ another leaf

Both http_get and pandas.apply are bottlenecks:
- http_get: make it async or add caching
- pandas.apply: vectorize it
```

---

## CPU-Bound vs I/O-Bound: Precise Identification

```python
import time
import asyncio
import concurrent.futures
import multiprocessing


def identify_bottleneck_type(func, *args, **kwargs):
    """
    Determine if a function is I/O-bound or CPU-bound.
    CPU-bound: threads don't help (GIL), need processes
    I/O-bound: threads or async help
    """
    # Run with 1 thread (baseline)
    start = time.perf_counter()
    result_single = func(*args, **kwargs)
    single_time = time.perf_counter() - start

    # Run same work with 4 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        start = time.perf_counter()
        futures = [executor.submit(func, *args, **kwargs) for _ in range(4)]
        concurrent.futures.wait(futures)
        thread_time = time.perf_counter() - start

    # Run same work with 4 processes
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        start = time.perf_counter()
        futures = [executor.submit(func, *args, **kwargs) for _ in range(4)]
        concurrent.futures.wait(futures)
        process_time = time.perf_counter() - start

    print(f"Single-threaded:   {single_time:.3f}s (4 jobs sequential)")
    print(f"4 threads:         {thread_time:.3f}s")
    print(f"4 processes:       {process_time:.3f}s")
    print()

    if process_time < single_time * 0.5:
        print("Result: CPU-BOUND — use ProcessPoolExecutor or distribute across Spark workers")
    elif thread_time < single_time * 0.5:
        print("Result: I/O-BOUND — use ThreadPoolExecutor or asyncio")
    else:
        print("Result: Mix or GIL-heavy — profile further with py-spy")


# Example: CPU-bound task (JSON parsing + computation)
def cpu_task():
    import json
    data = json.loads('{"values": ' + '[1.5,' * 10000 + '0]}')
    return sum(x * x for x in data["values"])

# Example: I/O-bound task (HTTP request)
def io_task():
    import urllib.request
    with urllib.request.urlopen("https://httpbin.org/delay/1") as r:
        return r.read()

# identify_bottleneck_type(cpu_task)
# CPU-BOUND: process_time ≈ single_time/4 (true parallelism)
# threads give NO speedup (GIL prevents parallel Python execution)

# identify_bottleneck_type(io_task)
# I/O-BOUND: thread_time ≈ single_time/4 (GIL released during I/O wait)
```

---

## Production Performance Monitoring Pattern

```python
import time
import functools
import logging
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Accumulates performance metrics across pipeline stages."""
    stage_times: dict = field(default_factory=dict)
    stage_counts: dict = field(default_factory=dict)
    peak_memory_mb: float = 0.0

    def record(self, stage: str, duration: float, count: int = 0):
        self.stage_times[stage] = self.stage_times.get(stage, 0) + duration
        self.stage_counts[stage] = self.stage_counts.get(stage, 0) + count

    def report(self) -> dict:
        total = sum(self.stage_times.values())
        return {
            "total_duration_s": round(total, 3),
            "stages": {
                stage: {
                    "duration_s": round(dur, 3),
                    "pct_of_total": round(100 * dur / total, 1) if total else 0,
                    "records": self.stage_counts.get(stage, 0),
                }
                for stage, dur in sorted(self.stage_times.items(), key=lambda x: -x[1])
            }
        }


def timed_stage(metrics: PipelineMetrics, stage_name: str):
    """Context manager for timing a pipeline stage."""
    from contextlib import contextmanager

    @contextmanager
    def _timer():
        start = time.perf_counter()
        yield
        elapsed = time.perf_counter() - start
        metrics.record(stage_name, elapsed)
        logger.info("stage=%s duration=%.3fs", stage_name, elapsed)

    return _timer()


# Usage:
def run_monitored_pipeline(records: list):
    metrics = PipelineMetrics()

    with timed_stage(metrics, "validate"):
        valid_records = [r for r in records if validate(r)]

    with timed_stage(metrics, "transform"):
        transformed = [transform(r) for r in valid_records]

    with timed_stage(metrics, "enrich_api"):
        enriched = enrich_from_api(transformed)

    with timed_stage(metrics, "write_db"):
        write_to_db(enriched)

    report = metrics.report()
    logger.info("Pipeline performance report: %s", report)
    # report["stages"] sorted by duration → shows bottleneck first
    return report
```

---

## Key Takeaways for Senior DEs

1. **Python UDFs in PySpark = row-by-row serialization** — always use `pandas_udf` with Arrow if you must use Python, or replace with native Spark SQL.
2. **Async profiling** requires `yappi` (wall time per coroutine) or `asyncio.set_debug(True)` for identifying slow callbacks.
3. **Memory leaks** in long-running pipelines usually come from unbounded global containers — use `lru_cache(maxsize=N)` and `deque(maxlen=N)`.
4. **`py-spy`** can profile any Python process without code changes — essential for production performance investigation.
5. **CPU vs I/O identification** determines the right solution: I/O-bound → async/threads, CPU-bound → multiprocessing/Spark. Threading Python CPU-bound work doesn't help due to the GIL.

## ⚡ Cheat Sheet

**UDF Performance in PySpark**
| Method | 1M row time | Overhead |
|--------|-------------|----------|
| Native Spark SQL | ~0.5 s | 1× |
| Pandas UDF (Arrow) | ~1.5 s | 3× |
| Python UDF (row-by-row) | ~45 s | 90× |

- Rule: native → pandas_udf → Python UDF (last resort)
- Enable: `spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")`
- `spark.sql.execution.arrow.maxRecordsPerBatch = 10000` — tune batch size

**CPU-Bound vs I/O-Bound Identification**
- Run 4 threads vs single: if `thread_time < single_time/2` → I/O-bound → use threads/async
- Run 4 processes vs single: if `process_time < single_time/2` → CPU-bound → use multiprocessing
- Neither improves: profile with `py-spy` — likely bottleneck inside C extension or wrong diagnosis

**py-spy Commands**
```bash
py-spy top --pid <PID>                          # Live top view
py-spy record -o profile.svg --pid <PID> --duration 30  # Flame graph
py-spy dump --pid <PID>                         # Stack trace dump (diagnosing hangs)
py-spy record -o profile.svg -- python script.py  # Profile from start
```
- Wide flat bars at bottom of flame graph = hot path → optimize those first
- No code changes, no restart — safe for production use

**Memory Leak Patterns**
- Unbounded global dict: `global_cache = {}` never evicted → fix with `@lru_cache(maxsize=N)`
- Growing list in long-running process: use `deque(maxlen=N)` for sliding window
- Unclosed file handles: always use `with open()` — never bare `open()`
- `tracemalloc`: `take_snapshot()` before/after + `.compare_to(before, "lineno")` → shows growth

**Profiling Async Code**
- `asyncio.set_debug(True)` + `loop.slow_callback_duration = 0.1` — logs coroutines blocking > 100 ms
- `yappi.set_clock_type("wall")` — measures wall time including I/O wait per coroutine
- Manual: `@asynccontextmanager async def async_timer(label)` with `time.perf_counter()`

**Production Monitoring Pattern**
- `PipelineMetrics.stage_times` sorted by duration → bottleneck always first in report
- `timed_stage(metrics, "stage_name")` context manager wraps each step
- Log `stage=X duration=Ns` as structured key-value → queryable in Elasticsearch/CloudWatch

**Key Rules**
- Never `time.sleep()` to "wait for results" — measure with `perf_counter()` instead
- `cProfile` misses thread wait time — prefer `py-spy` (wall time) for real bottleneck ID
- Profile before optimizing: assumption-based optimization is usually wrong
