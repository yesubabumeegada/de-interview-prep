---
title: "Profiling & Performance — Scenarios"
topic: python
subtopic: profiling-performance
content_type: scenario_question
tags: [python, profiling, performance, pandas, memory, optimization, interview]
---

# Profiling & Performance — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: A Pandas Script Processes 1M Rows in 10 Minutes — What Do You Check First?

**Scenario:** You've inherited a Python ETL script that reads a CSV file with 1 million rows and takes 10 minutes to run. Your manager says it needs to run in under 1 minute. You haven't seen the code yet. Walk through your diagnostic process — what do you look for first, in what order, and what tools do you use?

<details>
<summary>💡 Hint</summary>

Think about the most common Pandas anti-patterns that cause 10x-100x slowdowns. What's your first step — do you start reading the code line by line, or do you profile it first? What are the top 3 most likely culprits in a Pandas script with 1M rows?

</details>

<details>
<summary>✅ Solution</summary>

```python
# STEP 1: Profile before reading the code
# Run cProfile on a sample to find the actual bottleneck
import cProfile
import pstats
import io


def profile_script():
    """Profile the existing script with a sample."""
    profiler = cProfile.Profile()
    profiler.enable()

    # Run with a smaller sample first (100K rows) to get quick results
    run_etl_on_sample("data_100k_rows.csv")

    profiler.disable()

    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.sort_stats("cumulative")
    stats.print_stats(20)
    print(s.getvalue())

# What to look for in the cProfile output:
# 1. High cumtime in pandas functions → check for apply(), iterrows()
# 2. High cumtime in requests/urllib → serial API calls → make async
# 3. High cumtime in read_csv → file I/O bottleneck
# 4. High cumtime in json.loads → parsing overhead

# ── After profiling, you find it's pandas.apply ──────────────────────────

import pandas as pd
import numpy as np
import timeit

# The script (simplified):
df = pd.read_csv("1m_rows.csv")

# ANTI-PATTERN 1: iterrows() — the worst offender
# for idx, row in df.iterrows():
#     df.at[idx, "total"] = row["price"] * row["qty"]

# ANTI-PATTERN 2: apply() on arithmetic
# df["total"] = df.apply(lambda r: r["price"] * r["qty"] * (1 - r["discount"]), axis=1)
# → ~8 minutes of the 10 minute runtime

# ANTI-PATTERN 3: apply() for categorization
# df["category"] = df["total"].apply(lambda x: "large" if x > 1000 else "small")
# → ~1.5 minutes

# ── FIX: Vectorize everything ─────────────────────────────────────────────

# Fix 1: Vectorized arithmetic (replaces apply() arithmetic)
df["total"] = df["price"] * df["qty"] * (1 - df["discount"])
# Before: ~8 min | After: ~0.1s | Speedup: 4800x

# Fix 2: np.where() for binary condition (replaces apply() categorization)
df["category"] = np.where(df["total"] > 1000, "large", "small")
# Before: ~1.5 min | After: ~0.05s

# Fix 3: np.select() for multiple conditions
conditions = [
    df["total"] >= 10000,
    df["total"] >= 1000,
    df["total"] >= 100,
]
df["tier"] = np.select(conditions, ["enterprise", "large", "medium"], default="small")
# Before: ~2 min (if it was apply()) | After: ~0.1s

# Fix 4: str accessor for string operations
df["clean_sku"] = df["sku"].str.strip().str.upper()
# Before: ~1 min (if it was apply(str.strip)) | After: ~0.2s

# ── Measure total improvement ─────────────────────────────────────────────
# Before: 10 minutes
# After:  ~5-10 seconds
# Meets the < 1 minute target ✓
```

**Diagnostic order:**
1. `cProfile` on a small sample — find the slow function (don't guess)
2. In most Pandas scripts: `apply()` and `iterrows()` are the culprit 90% of the time
3. Replace with: vectorized arithmetic, `np.where()`, `np.select()`, `.str` accessor, `.map()`
4. Re-measure with `timeit` to confirm the fix

**Common gotcha:** sometimes students skip profiling and go straight to "optimizing" something fast (date parsing, file reading) while `apply()` on 1M rows is the 99% bottleneck. Always profile first.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Optimize a Python ETL That Loads a 50 GB CSV into Memory and Crashes

**Scenario:** A Python ETL loads a 50 GB CSV into a Pandas DataFrame, filters it to `status = 'active'`, transforms it, and writes it to Parquet. It runs fine on a laptop with the 5 GB test file but crashes on the production server with an OOM error when processing the 50 GB file. The server has 32 GB RAM. Fix it without increasing server resources.

<details>
<summary>💡 Hint</summary>

The issue is that the entire 50 GB file is loaded into memory at once. Think about: chunked reading (`chunksize`), filtering early (before transformations), writing output incrementally (not accumulating everything in memory), and the right output format. Also consider `dtype` optimization in `read_csv` — int64 vs int32 can halve memory usage.

</details>

<details>
<summary>✅ Solution</summary>

```python
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
import logging

logger = logging.getLogger(__name__)


# ── BEFORE: OOM version ───────────────────────────────────────────────────

def bad_etl(input_path: str, output_path: str):
    """
    This version loads 50 GB into RAM → OOM on 32 GB server.
    """
    df = pd.read_csv(input_path)          # OOM: tries to load 50 GB → crashes
    df_active = df[df["status"] == "active"]
    df_active["revenue"] = df_active["price"] * df_active["quantity"]
    df_active.to_parquet(output_path)


# ── AFTER: Memory-efficient version ──────────────────────────────────────

def good_etl(input_path: str, output_path: str, chunk_size: int = 200_000):
    """
    Chunked ETL — peak memory usage: ~2-3 GB per chunk regardless of file size.
    """
    start = time.perf_counter()
    writer = None
    stats = {"rows_read": 0, "rows_written": 0, "chunks": 0}

    # Optimization 1: Specify dtypes to reduce per-row memory
    # Default: pandas reads integers as int64 (8 bytes). If values fit in int32 (4 bytes), use it.
    dtypes = {
        "order_id":   "int32",     # vs int64: saves 4 bytes/row × 50M rows = 200 MB
        "customer_id": "int32",
        "quantity":   "int16",     # Quantities rarely > 32767
        "price":      "float32",   # vs float64: saves 4 bytes/row
        "status":     "category",  # String categories: ~10x memory reduction
    }

    # Optimization 2: Only read needed columns
    needed_cols = ["order_id", "customer_id", "price", "quantity", "discount", "status"]

    try:
        for chunk in pd.read_csv(
            input_path,
            chunksize=chunk_size,
            dtype=dtypes,
            usecols=needed_cols,   # Don't read unused columns
            low_memory=False,
        ):
            stats["chunks"] += 1
            stats["rows_read"] += len(chunk)

            # Optimization 3: Filter FIRST — reduces data before transformation
            active = chunk[chunk["status"] == "active"]
            if active.empty:
                continue

            # Optimization 4: Vectorized transformation (no apply)
            active = active.copy()
            active["revenue"] = (
                active["price"].astype("float32") *
                active["quantity"].astype("float32") *
                (1 - active["discount"].fillna(0).astype("float32"))
            )
            active["tier"] = np.where(active["revenue"] >= 1000, "large", "small")

            # Drop the status column (not needed in output)
            active = active.drop(columns=["status"])

            # Optimization 5: Write each chunk to Parquet incrementally
            table = pa.Table.from_pandas(active, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression="snappy",
                    use_dictionary=True,  # Efficient for low-cardinality strings
                )

            writer.write_table(table)
            stats["rows_written"] += len(active)

            if stats["chunks"] % 50 == 0:
                elapsed = time.perf_counter() - start
                throughput = stats["rows_read"] / elapsed
                logger.info(
                    "Progress: chunk=%d rows_read=%d rows_written=%d throughput=%.0f rows/s",
                    stats["chunks"], stats["rows_read"], stats["rows_written"], throughput
                )

    finally:
        if writer:
            writer.close()

    elapsed = time.perf_counter() - start
    stats["duration_s"] = round(elapsed, 2)
    stats["throughput_rps"] = int(stats["rows_read"] / elapsed) if elapsed else 0

    logger.info("ETL complete: %s", stats)
    return stats


# ── Memory analysis ───────────────────────────────────────────────────────
# chunk_size = 200,000 rows
# Each row: ~8 columns × ~6 bytes avg (with dtype optimization) ≈ 48 bytes
# Chunk memory: 200,000 × 48 bytes ≈ 9.6 MB per chunk
# Peak memory: ~3-4 chunks in flight simultaneously ≈ 40 MB
# vs before: ~150 GB RAM to load 50 GB CSV (pandas overhead is ~3x file size)

# Result:
# Before: OOM crash on 32 GB server
# After:  ~40 MB peak memory, runs in ~25 minutes (200K rows/chunk × 250 chunks)
# Throughput: ~33K rows/second
```

**Key optimizations applied:**
1. `chunksize=200_000` — process one chunk at a time
2. `usecols=[...]` — don't read columns you don't need (saves I/O + memory)
3. `dtype={...}` — use smaller dtypes (int32 vs int64 = 50% memory reduction for ints)
4. Filter FIRST (`status == 'active'`) before transformation — reduces data volume early
5. Write each chunk to Parquet immediately (PyArrow writer) — never accumulate all output in memory
6. Vectorized `revenue` calculation — no `apply()`

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Profiling Strategy to Find Bottlenecks in a Multi-Step Python Pipeline in Production

**Scenario:** A production Python pipeline with 6 steps runs on schedule every 2 hours. It normally takes 25 minutes. Over the past 2 weeks, it's been getting slower — now it's taking 90 minutes. The pipeline runs on a cloud VM with no development access. You cannot restart it, add new dependencies, or modify environment variables. Design a complete profiling and diagnosis strategy.

<details>
<summary>💡 Hint</summary>

You can't run cProfile or install new tools (no code changes, no new deps). Think: `py-spy` attaches to a live process without changes. How do you get a flame graph? For the progressive slowdown pattern — it's probably a resource exhaustion issue (memory leak, growing cache, disk filling up) rather than a code bug. What production telemetry do you look at first?

</details>

<details>
<summary>✅ Solution</summary>

```python
# ── Phase 1: Gather existing telemetry (no code changes) ─────────────────

"""
Before touching the running process, check existing observability:

1. OS-level metrics (cloud VM):
   - CPU: is it maxed out? (CPU-bound regression) or idle? (I/O-bound or sleeping)
   - Memory: is RAM growing over time? (memory leak)
   - Disk I/O: is temp/scratch disk saturated?
   - Network: is outbound bandwidth saturated? (API rate limiting?)

   Tool: CloudWatch / Datadog / Prometheus — already collecting these
   Look at: the last 2 weeks of memory usage per pipeline run

2. Application logs:
   - Each pipeline step should log timing. Extract step-level durations.
   - Pattern: is one step getting slower, or all steps proportionally slower?
   - 'grep "step=.*duration" pipeline.log | tail -1000 | sort' to find slow steps

3. External service logs:
   - Is the API we call returning 429 (rate limited) more often?
   - Is the database query time increasing? (missing index, table bloat, lock contention)
"""

# ── Phase 2: py-spy — attach to live process without code changes ─────────

"""
py-spy is already installed on the VM (confirm: which py-spy).
If not: pip install py-spy in a virtual env — no code changes required.

# Find the pipeline PID
ps aux | grep pipeline.py → PID 12345

# Option 1: Live top view (see where CPU time is going RIGHT NOW)
py-spy top --pid 12345
# Shows: which functions are currently consuming CPU %
# Look for: unexpected functions in the top list (e.g., gc.collect = memory pressure)

# Option 2: Flame graph (30-second sample → SVG)
py-spy record --pid 12345 --duration 30 --output /tmp/profile.svg
# Fetch the SVG to your machine:
# scp user@vm:/tmp/profile.svg ./profile.svg
# Open in browser: widest leaf = bottleneck

# Option 3: Stack dump for hangs
py-spy dump --pid 12345
# Shows current call stack — if it's sleeping/waiting, shows what it's waiting on
"""

# ── Phase 3: Diagnose the progressive slowdown pattern ─────────────────

"""
A 2-week progressive slowdown = resource exhaustion, not a code bug.
Code bugs cause step-function changes (suddenly slow), not gradual degradation.

Most likely causes, in order of probability:

1. Memory leak → garbage collector thrashing
   Evidence: memory grows run-over-run, never released
   Signal: py-spy top shows high % in gc.collect / gc.collect_generations
   Diagnosis:

   import tracemalloc
   tracemalloc.start()
   # (add this to the pipeline temporarily, next run)

   # Or check from outside: /proc/PID/status shows VmRSS growing

2. Growing in-memory cache without eviction
   Evidence: memory grows, no GC pressure (cache holds references)
   Signal: py-spy top shows cache lookup functions
   Fix: add maxsize to lru_cache, use TTL, or call cache_clear() periodically

3. Temp file / scratch disk filling up
   Evidence: disk usage grows, I/O time increases
   Signal: df -h shows disk filling; py-spy shows high time in file writes
   Fix: clean up temp files, move to object storage

4. External API degradation (rate limiting / slowdown)
   Evidence: network I/O time growing
   Signal: py-spy shows requests.get dominating AND response times growing
   Fix: add caching, reduce call frequency, check API status page

5. Database index bloat / missing index on growing table
   Evidence: DB query time grows with table size
   Signal: py-spy shows psycopg2 / sqlalchemy in hot path
   Fix: ANALYZE TABLE, VACUUM, or add index
"""

# ── Phase 4: Instrument the NEXT run (minimal code change) ───────────────

"""
If you can deploy a one-line change, add structured timing logging:
"""

import time
import logging
import tracemalloc
import gc

logger = logging.getLogger(__name__)


class PipelineStepTimer:
    """
    Minimal instrumentation: add to each pipeline step.
    Deploy in the NEXT scheduled run to capture timing data.
    """

    def __init__(self, step_name: str):
        self.step_name = step_name
        self.start = None

    def __enter__(self):
        gc.collect()  # Force GC before measuring memory
        self.mem_before = self._get_memory_mb()
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        elapsed = time.perf_counter() - self.start
        mem_after = self._get_memory_mb()
        mem_delta = mem_after - self.mem_before

        logger.info(
            "step=%s duration_s=%.2f memory_before_mb=%.0f memory_after_mb=%.0f "
            "memory_delta_mb=%.0f gc_counts=%s",
            self.step_name,
            elapsed,
            self.mem_before,
            mem_after,
            mem_delta,
            gc.get_count(),
        )

        # Alert on excessive memory growth (likely a leak)
        if mem_delta > 500:
            logger.error(
                "MEMORY ALERT: step=%s grew by %.0f MB. "
                "Possible memory leak — inspect this step.",
                self.step_name, mem_delta
            )

    @staticmethod
    def _get_memory_mb() -> float:
        import psutil, os
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024


# Add to each pipeline step:
def run_pipeline():
    with PipelineStepTimer("fetch_crm"):
        records = fetch_from_crm()

    with PipelineStepTimer("validate"):
        valid = validate_records(records)

    with PipelineStepTimer("transform"):
        transformed = transform(valid)

    with PipelineStepTimer("enrich_api"):
        enriched = enrich(transformed)

    with PipelineStepTimer("write_db"):
        write(enriched)

# After ONE run with this instrumentation, you'll know:
# - Which step is taking the most time
# - Which step's memory is growing (leak candidate)
# - Whether GC is being called excessively (memory pressure)


# ── Phase 5: Resolution (based on findings) ──────────────────────────────

"""
Once profiling reveals the bottleneck:

Memory leak → cache without maxsize:
    @lru_cache(maxsize=50_000)  # ← add maxsize
    def lookup_user(user_id: int) -> dict: ...

    Or: call lookup_user.cache_clear() at start of each run

DB query degradation → missing index:
    EXPLAIN ANALYZE SELECT * FROM events WHERE user_id = $1 AND dt = $2;
    → Add: CREATE INDEX CONCURRENTLY idx_events_user_dt ON events(user_id, dt);

API rate limiting → add caching layer:
    from functools import lru_cache
    import time

    @lru_cache(maxsize=100_000)
    def get_product_details(product_id: str) -> dict:
        return api_client.get(f"/products/{product_id}")
    # Products rarely change — cache for the duration of the pipeline run

Temp disk filling → add cleanup:
    import tempfile, shutil, atexit

    # At pipeline start:
    tmpdir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, tmpdir, ignore_errors=True)
"""
```

**Investigation methodology summary:**
1. **Check OS metrics first** (memory trend, disk, network) — 10 minutes, no code changes
2. **Extract timing from existing logs** — if structured logging is in place
3. **`py-spy top`** on the live process — 0 code changes, shows current CPU hot path
4. **`py-spy record`** for a flame graph — 0 code changes, 30-second sample
5. **Instrument the next run** with `PipelineStepTimer` — one small code change, reveals which step + memory delta
6. **Fix the specific root cause** based on evidence, not guessing

The key insight: a **gradual 2-week slowdown** is almost always resource exhaustion (memory leak, cache growth, disk fill, table bloat), not a code logic bug. Profile to confirm, then fix the resource management issue.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What's your first step when a Python script is slow?" — Never "I'd optimize the for loop" or "I'd use a different data structure." The correct answer is: profile it first with `cProfile`, sort by cumulative time, and identify the actual bottleneck. 90% of DE scripts are slow because of `apply()`, N+1 DB queries, or serial API calls — but you can't know which without profiling.

> **Tip 2:** "When would vectorization NOT help?" — When the logic requires state that depends on previous rows (rolling custom logic), when the branching is too complex for `np.select()`, or when you need to call an external service per record. For those cases: batch the external calls (group all IDs and fetch in one request), then do a DataFrame merge to enrich all records at once. "vectorize the merge, not the lookup" is the pattern.

> **Tip 3:** "How do you profile a production Python process without restarting it?" — `py-spy`. It attaches to a running process via the OS's process inspection API, samples the call stack at high frequency, and produces a flame graph — all without code changes or restarts. It's the production profiler of choice precisely because it doesn't require modifying or redeploying the application.
