---
title: "Python Multithreading & Multiprocessing - Fundamentals"
topic: python
subtopic: multithreading-multiprocessing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, multithreading, multiprocessing, concurrency, parallelism, GIL]
---

# Python Multithreading & Multiprocessing — Fundamentals


## 🎯 Analogy

Think of threading vs multiprocessing like restaurant staff: threads are waiters sharing the same kitchen (GIL limits true CPU parallelism — good for I/O), processes are separate kitchens (true CPU parallelism — good for compute).

---
## Why Concurrency Matters in Data Engineering

Data pipelines spend time doing two things:
1. **Waiting** — for API responses, database queries, file downloads (I/O-bound)
2. **Computing** — transforming, parsing, aggregating data (CPU-bound)

Understanding concurrency lets you extract from 20 APIs simultaneously or transform data across all CPU cores instead of processing sequentially.

---

## The GIL — Python's Most Misunderstood Feature

The **Global Interpreter Lock (GIL)** is a mutex that allows only one thread to execute Python bytecode at a time within a single process.

```python
# This does NOT run in parallel — GIL prevents it
import threading

def cpu_work(n):
    """CPU-bound: GIL means threads take turns, not parallel."""
    total = 0
    for i in range(n):
        total += i * i
    return total

# Two threads doing CPU work — no speedup due to GIL
t1 = threading.Thread(target=cpu_work, args=(10_000_000,))
t2 = threading.Thread(target=cpu_work, args=(10_000_000,))
t1.start(); t2.start()
t1.join(); t2.join()
# Takes roughly the SAME time as running sequentially!
```

**Key rule:**
- GIL is released during I/O operations (network, disk, sleep)
- GIL is held during CPU computation

| Task Type | Threads Help? | Processes Help? |
|-----------|:---:|:---:|
| API calls | ✅ Yes | ✅ Yes (overkill) |
| File downloads | ✅ Yes | ✅ Yes (overkill) |
| Database queries | ✅ Yes | ✅ Yes (overkill) |
| Data parsing/transforms | ❌ No (GIL) | ✅ Yes |
| Compression/encryption | ❌ No (GIL) | ✅ Yes |

---

## Threading — For I/O-Bound Work

Threads share memory within a process. The GIL is released during I/O, so multiple threads can wait for network responses simultaneously.

```python
from concurrent.futures import ThreadPoolExecutor
import time

def fetch_data(source_id: int) -> dict:
    """Simulate an API call (I/O-bound)."""
    time.sleep(1)  # Simulates network wait — GIL is released here
    return {"source": source_id, "rows": 1000}

# Sequential: 5 sources × 1 second = 5 seconds
# Parallel with threads: ~1 second total
sources = [1, 2, 3, 4, 5]

with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(fetch_data, sources))

print(f"Fetched {len(results)} sources")
# All 5 API calls happen simultaneously — total time ≈ 1 second
```

---

## Multiprocessing — For CPU-Bound Work

Processes have their own memory space and their own GIL. True parallel execution on multiple CPU cores.

```python
from concurrent.futures import ProcessPoolExecutor
import math

def transform_chunk(records: list[dict]) -> list[dict]:
    """CPU-intensive transformation — benefits from multiple cores."""
    result = []
    for record in records:
        # Expensive computation
        enriched = {
            **record,
            "score": math.sqrt(record["value"] ** 2 + record["weight"] ** 2),
            "category": classify_record(record),
        }
        result.append(enriched)
    return result

def classify_record(record: dict) -> str:
    # Simulated CPU work
    return "A" if record["value"] > 50 else "B"

# Split data into chunks and process in parallel
all_data = [{"value": i, "weight": i * 0.5} for i in range(100_000)]
chunk_size = 25_000
chunks = [all_data[i:i + chunk_size] for i in range(0, len(all_data), chunk_size)]

with ProcessPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(transform_chunk, chunks))

# Flatten results
flat_results = [record for chunk in results for record in chunk]
print(f"Processed {len(flat_results)} records across 4 cores")
```

---

## concurrent.futures — The Unified Interface

`concurrent.futures` provides the same API for both threads and processes. Swap between them with a single line change.

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

def process_file(filepath: str) -> dict:
    """Process a single file — could be I/O or CPU bound."""
    # Read file (I/O)
    with open(filepath) as f:
        data = f.read()
    # Parse and transform (CPU)
    records = parse_data(data)
    return {"file": filepath, "records": len(records)}

files = ["data_001.csv", "data_002.csv", "data_003.csv"]

# For I/O-heavy: ThreadPoolExecutor
# For CPU-heavy: ProcessPoolExecutor
ExecutorClass = ThreadPoolExecutor  # Switch to ProcessPoolExecutor for CPU work

with ExecutorClass(max_workers=4) as executor:
    # Submit all tasks
    futures = {executor.submit(process_file, f): f for f in files}
    
    # Process results as they complete (not in submission order)
    for future in as_completed(futures):
        filepath = futures[future]
        try:
            result = future.result()
            print(f"✓ {filepath}: {result['records']} records")
        except Exception as e:
            print(f"✗ {filepath}: {e}")
```

---

## Quick Decision Guide

```
Is your bottleneck...
│
├── Waiting for I/O? (APIs, databases, network)
│   └── Use ThreadPoolExecutor
│       • Lightweight (shared memory)
│       • Dozens or hundreds of threads OK
│       • GIL released during I/O
│
├── Heavy computation? (parsing, transforms, ML)
│   └── Use ProcessPoolExecutor
│       • True parallelism (separate memory)
│       • Workers ≈ CPU core count
│       • Data must be serialized between processes
│
└── Both?
    └── Threads for I/O extraction → Queue → Processes for transforms
```

---

## Common Pitfalls for Beginners

| Pitfall | Problem | Fix |
|---------|---------|-----|
| Too many workers | System overload, OOM | Workers = min(task_count, cores × 2) for threads |
| Large data in processes | Slow pickling, high memory | Share via files, not function args |
| No error handling | Silent failures | Use `future.result()` with try/except |
| Shared mutable state in threads | Race conditions | Use Queue or Lock |

---


## ▶️ Try It Yourself

```python
import concurrent.futures
import time

def io_task(n: int) -> str:
    time.sleep(0.1)  # Simulate I/O wait
    return f"IO result {n}"

def cpu_task(n: int) -> int:
    return sum(i * i for i in range(n * 100000))  # CPU-bound

# Threading: good for I/O-bound (network, disk, DB calls)
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    start = time.perf_counter()
    results = list(executor.map(io_task, range(10)))
    print(f"Threading (I/O): {time.perf_counter()-start:.2f}s for 10 tasks")

# Multiprocessing: good for CPU-bound (transforms, parsing, compression)
with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
    start = time.perf_counter()
    results = list(executor.map(cpu_task, range(8)))
    print(f"Multiprocessing (CPU): {time.perf_counter()-start:.2f}s for 8 tasks")
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** When asked about the GIL, say: "The GIL prevents parallel CPU execution in threads, but it's released during I/O. For API extraction, threads are fine. For heavy transforms, I use multiprocessing or move to PySpark." This shows nuanced understanding.

> **Tip 2:** Know when concurrency isn't the answer. If you're processing a 1GB file with Pandas, the bottleneck is often Pandas itself (single-threaded C code). The answer might be "use Polars" rather than "add threads."

> **Tip 3:** Always mention `concurrent.futures` as your go-to tool. It's the modern, clean API. If you start talking about `threading.Thread` manually, interviewers might think you're not current on Python best practices.
