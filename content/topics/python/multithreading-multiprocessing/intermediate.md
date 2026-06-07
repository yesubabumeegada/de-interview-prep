---
title: "Python Multithreading & Multiprocessing - Intermediate"
topic: python
subtopic: multithreading-multiprocessing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, multithreading, multiprocessing, asyncio, concurrent-futures]
---

# Python Multithreading & Multiprocessing — Intermediate

## Beyond Basic Parallelism

At the mid-level, you need to understand asyncio for I/O-bound pipelines, thread safety primitives, shared state with multiprocessing, and how to choose the right concurrency model.

---

## asyncio — Non-Blocking I/O for Pipeline Extraction

asyncio uses a single thread with an event loop. While one coroutine waits for I/O, others run. Lower overhead than threads for high-concurrency I/O tasks.

```python
import asyncio
import aiohttp
from datetime import datetime

async def fetch_api(session: aiohttp.ClientSession, url: str) -> dict:
    """Non-blocking API fetch."""
    async with session.get(url) as response:
        data = await response.json()
        return {"url": url, "status": response.status, "records": len(data)}

async def extract_all_sources(urls: list[str], max_concurrent: int = 10) -> list[dict]:
    """Fetch multiple APIs with concurrency limit."""
    semaphore = asyncio.Semaphore(max_concurrent)  # Rate limiting
    
    async def limited_fetch(session, url):
        async with semaphore:
            return await fetch_api(session, url)
    
    async with aiohttp.ClientSession() as session:
        tasks = [limited_fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Separate successes from failures
    successes = [r for r in results if isinstance(r, dict)]
    failures = [r for r in results if isinstance(r, Exception)]
    print(f"Success: {len(successes)}, Failed: {len(failures)}")
    return successes

# Run the async extraction
urls = [f"https://api.example.com/data/{i}" for i in range(50)]
results = asyncio.run(extract_all_sources(urls, max_concurrent=10))
```

**When to use asyncio over threads:**

| Factor | asyncio | Threads |
|--------|---------|---------|
| Concurrency level | Thousands | Hundreds |
| Memory per task | ~KB | ~8MB stack |
| Context switching | Cooperative (fast) | OS preemptive (slower) |
| Learning curve | Higher | Lower |
| Library support | Needs async libraries | Works with any library |

---

## Thread Safety — Locks and Queues

When threads share mutable state, you need synchronization to prevent race conditions.

```python
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

class ThreadSafeCounter:
    """Metrics counter safe for multi-threaded access."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
    
    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:  # Only one thread can modify at a time
            self._counts[key] = self._counts.get(key, 0) + amount
    
    def get(self, key: str) -> int:
        with self._lock:
            return self._counts.get(key, 0)
    
    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

# Thread-safe queue for producer-consumer
class PipelineBuffer:
    """Bounded buffer between extraction and transformation stages."""
    
    def __init__(self, max_size: int = 1000):
        self._queue: Queue = Queue(maxsize=max_size)
        self._metrics = ThreadSafeCounter()
    
    def put(self, record: dict) -> None:
        self._queue.put(record)  # Blocks if full (backpressure)
        self._metrics.increment("enqueued")
    
    def get(self, timeout: float = 5.0) -> dict | None:
        try:
            record = self._queue.get(timeout=timeout)
            self._metrics.increment("dequeued")
            return record
        except Exception:
            return None
    
    @property
    def size(self) -> int:
        return self._queue.qsize()

# Usage with multiple producer threads
buffer = PipelineBuffer(max_size=100)
metrics = ThreadSafeCounter()

def producer(source_id: int):
    for i in range(10):
        buffer.put({"source": source_id, "record": i})
    metrics.increment("sources_completed")

with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(producer, range(5))

print(f"Buffer size: {buffer.size}")  # 50 records
print(f"Sources done: {metrics.get('sources_completed')}")  # 5
```

---

## Multiprocessing with Shared State

Processes have separate memory. Use `multiprocessing.Manager()` for shared state, or better — use queues for communication.

```python
from multiprocessing import Process, Manager, Queue
import multiprocessing as mp

def worker_with_shared_state(worker_id: int, shared_dict: dict, shared_list: list):
    """Worker that writes to shared state via Manager proxy."""
    result = {"worker": worker_id, "processed": worker_id * 100}
    shared_dict[f"worker_{worker_id}"] = result
    shared_list.append(worker_id)

def run_with_manager():
    """Use Manager for shared state between processes."""
    with Manager() as manager:
        shared_dict = manager.dict()
        shared_list = manager.list()
        
        processes = []
        for i in range(4):
            p = Process(target=worker_with_shared_state, args=(i, shared_dict, shared_list))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        print(f"Results: {dict(shared_dict)}")
        print(f"Completed workers: {list(shared_list)}")

run_with_manager()
```

> **Performance note:** Manager proxies are slow (they use IPC). For high-throughput scenarios, pass data via `Queue` or write results to files that the main process reads.

---

## Chunked Parallel Processing Pattern

The standard pattern for processing large datasets with multiprocessing:

```python
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator
import math

def chunk_iterable(data: list, chunk_size: int) -> Iterator[list]:
    """Split data into chunks for parallel processing."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def process_chunk(chunk: list[dict]) -> list[dict]:
    """Transform a chunk of records — runs in a separate process."""
    results = []
    for record in chunk:
        transformed = {
            "id": record["id"],
            "amount_usd": record["amount"] * record.get("exchange_rate", 1.0),
            "category": categorize(record["amount"]),
        }
        results.append(transformed)
    return results

def categorize(amount: float) -> str:
    if amount > 1000: return "high"
    if amount > 100: return "medium"
    return "low"

def parallel_transform(data: list[dict], workers: int = None) -> list[dict]:
    """Process data in parallel chunks."""
    workers = workers or mp.cpu_count()
    chunk_size = max(1, math.ceil(len(data) / workers))
    chunks = list(chunk_iterable(data, chunk_size))
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        result_chunks = list(executor.map(process_chunk, chunks))
    
    # Flatten
    return [record for chunk in result_chunks for record in chunk]

# Usage
data = [{"id": i, "amount": i * 10.5, "exchange_rate": 1.2} for i in range(100_000)]
results = parallel_transform(data, workers=4)
print(f"Processed {len(results)} records")
```

---

## When to Use Which Concurrency Model

```
┌─────────────────────────────────────────────────────┐
│              CONCURRENCY DECISION TREE               │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Is your bottleneck I/O?                           │
│  ├── Yes: How many concurrent tasks?               │
│  │   ├── < 100: ThreadPoolExecutor                 │
│  │   └── > 100: asyncio (lower memory overhead)    │
│  │                                                  │
│  └── No (CPU-bound):                               │
│      ├── Data fits in memory? → ProcessPoolExecutor │
│      ├── Data > RAM? → Dask / PySpark              │
│      └── Simple number crunching? → NumPy/Polars   │
│                                                     │
│  Mixed workload?                                    │
│  └── ThreadPool for I/O → Queue → ProcessPool      │
│      for transforms                                 │
└─────────────────────────────────────────────────────┘
```

---

## Memory Considerations with Multiprocessing

| Issue | Cause | Solution |
|-------|-------|----------|
| Memory explosion | Each process copies data | Use chunked processing, don't pass huge args |
| Pickle errors | Lambdas/closures can't serialize | Use module-level functions |
| Fork bomb | Processes spawning more processes | Set `max_workers` explicitly |
| Zombie processes | Unjoined processes | Use context managers (`with ProcessPoolExecutor`) |

```python
# BAD — copies 1GB to each process
large_data = load_huge_dataset()  # 1 GB
with ProcessPoolExecutor(max_workers=4) as executor:
    executor.map(process, [large_data] * 4)  # 4 GB total!

# GOOD — each process reads its own chunk
def process_file_chunk(filepath: str, start: int, end: int) -> list[dict]:
    """Each process reads only its portion."""
    import pandas as pd
    return pd.read_csv(filepath, skiprows=range(1, start), nrows=end - start).to_dict("records")

# Split by line ranges, not by copying data
chunks = [(filepath, i * 10000, (i + 1) * 10000) for i in range(4)]
with ProcessPoolExecutor(max_workers=4) as executor:
    # Note: ProcessPoolExecutor doesn't have starmap — use submit with unpacking
    futures = [executor.submit(process_file_chunk, *chunk) for chunk in chunks]
    results = [f.result() for f in futures]
```

---

## Interview Tips

> **Tip 1:** For asyncio questions, explain the event loop: "One thread, many coroutines. While coroutine A awaits a network response, the event loop runs coroutine B. No OS thread switching overhead." Then mention the semaphore pattern for rate limiting.

> **Tip 2:** When discussing thread safety, show you understand the hierarchy: "Immutable data needs no locks. For counters, I'd use `threading.Lock`. For producer-consumer, I'd use `queue.Queue` which handles locking internally. For complex shared state across processes, I'd use Redis or a message queue."

> **Tip 3:** The chunking pattern is the #1 interview answer for "how would you parallelize data processing?" Show the full flow: split data → map to workers → collect results → flatten. Mention that chunk size should balance overhead vs parallelism.
