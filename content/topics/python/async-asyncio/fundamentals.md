---
title: "Async / asyncio — Fundamentals"
topic: python
subtopic: async-asyncio
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, asyncio, async-await, event-loop, concurrency, API-ingestion]
---

# Async / asyncio — Fundamentals

Most data engineers learn async programming when they hit a wall: an API ingestion job that calls 200 endpoints sequentially takes 45 minutes, but asynchronous code does it in 90 seconds. Understanding why requires understanding the difference between synchronous and asynchronous execution.

---

## Sync vs Async: What's the Difference?

### Synchronous Execution

```python
import time
import requests

def fetch_user(user_id: int) -> dict:
    """Fetch one user from an API."""
    response = requests.get(f"https://api.example.com/users/{user_id}")
    return response.json()

def fetch_all_users_sync(user_ids: list) -> list:
    """Fetch 100 users one at a time — SLOW."""
    return [fetch_user(uid) for uid in user_ids]

# Timing:
start = time.time()
users = fetch_all_users_sync(range(100))
print(f"Sync: {time.time() - start:.1f}s")
# Sync: 50.3s  (100 requests × ~500ms each = 50 seconds)
```

The problem: `requests.get()` is **blocking**. While waiting for the HTTP response (typically 100-500ms), your Python process does nothing. With 100 API calls, that's 50 seconds of waiting.

### Asynchronous Execution

```python
import asyncio
import aiohttp
import time

async def fetch_user_async(session: aiohttp.ClientSession, user_id: int) -> dict:
    """Fetch one user — non-blocking."""
    async with session.get(f"https://api.example.com/users/{user_id}") as response:
        return await response.json()

async def fetch_all_users_async(user_ids: list) -> list:
    """Fetch 100 users concurrently — FAST."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_user_async(session, uid) for uid in user_ids]
        return await asyncio.gather(*tasks)

# Timing:
start = time.time()
users = asyncio.run(fetch_all_users_async(range(100)))
print(f"Async: {time.time() - start:.1f}s")
# Async: 1.2s  (all 100 requests in flight simultaneously)
```

**The key insight:** async doesn't make any single HTTP request faster. It allows your program to start the next request *while waiting for the previous one to respond*. You're utilizing the waiting time.

---

## The Event Loop

The event loop is the engine that makes async work. It manages a queue of tasks and runs them cooperatively.

```
Event Loop execution model:

Task A: send HTTP request → [WAITING for response] ....... → process response
Task B:                      send HTTP request → [WAITING] ...... → process response
Task C:                                           send HTTP request → [WAITING] ... → process response

Time ─────────────────────────────────────────────────────────────────────────────►
         ↑                  ↑                   ↑
     All 3 start           Tasks yield control  Results arrive
     nearly simultaneously  (await keyword)      as responses come in
```

```python
import asyncio


async def demo_event_loop():
    """Show that tasks run concurrently via the event loop."""
    print("Task A: start")
    await asyncio.sleep(1)  # Yield control to event loop for 1 second
    print("Task A: done")   # Event loop comes back here after 1s


async def main():
    # Run two "tasks" concurrently
    task_a = asyncio.create_task(demo_event_loop())
    task_b = asyncio.create_task(demo_event_loop())

    await task_a
    await task_b
    # Total time: ~1 second (not 2!) — they ran concurrently

asyncio.run(main())
# Task A: start
# Task A: start   ← Both start before either finishes
# Task A: done
# Task A: done
```

---

## async/await Syntax

```python
# async def: declares a coroutine function
# await: suspends the current coroutine, yields to event loop

async def my_coroutine():
    # This is a coroutine function. Calling it returns a coroutine OBJECT,
    # not the result. You must await it.
    result = await some_async_operation()
    return result

# WRONG — this doesn't run the coroutine:
my_coroutine()  # Returns a coroutine object, runs nothing

# CORRECT — await it (from within another async function):
result = await my_coroutine()

# CORRECT — run from synchronous code using asyncio.run():
result = asyncio.run(my_coroutine())
```

### The await Rule

You can only `await` something that is:
1. A coroutine (async function result)
2. An awaitable object (implements `__await__`)
3. A Task or Future

```python
import asyncio

async def fetch_data(source: str) -> str:
    await asyncio.sleep(0.5)  # Simulates I/O wait
    return f"data from {source}"

async def process_pipeline():
    # Sequential awaits — still runs one at a time
    result_a = await fetch_data("source_A")
    result_b = await fetch_data("source_B")
    # Total time: ~1 second

    # Concurrent awaits with gather — runs both at once
    result_a, result_b = await asyncio.gather(
        fetch_data("source_A"),
        fetch_data("source_B"),
    )
    # Total time: ~0.5 seconds

asyncio.run(process_pipeline())
```

---

## asyncio.run() — The Entry Point

```python
import asyncio

async def main():
    print("Pipeline starting")
    await asyncio.sleep(1)
    print("Pipeline done")

# asyncio.run() creates an event loop, runs the coroutine, and closes the loop
asyncio.run(main())

# DO NOT call asyncio.run() from within an existing async context:
# It will raise: RuntimeError: This event loop is already running
# In that case, use: await main()  (from within another async function)
# Or in Jupyter: asyncio.get_event_loop().run_until_complete(main())
```

---

## When Async Helps in Data Engineering

Async is the right tool for **I/O-bound** tasks — operations where your code spends time waiting for external systems:

| Use Case | Benefit |
|---|---|
| Calling 50 REST APIs to gather source data | Run all calls concurrently (50x speedup for I/O-bound work) |
| Writing records to multiple databases simultaneously | Concurrent DB writes |
| Reading from multiple S3 buckets | Concurrent object reads |
| Polling multiple Kafka topics | Non-blocking consumer loops |
| Health-checking 20 external services | Concurrent health checks |

**When async does NOT help:**

```python
import asyncio
import time

# CPU-bound task — async won't help, use multiprocessing instead
async def cpu_heavy_task(n: int) -> int:
    """This runs Python code, not I/O. The event loop cannot multitask here."""
    result = 0
    for i in range(n):
        result += i * i
    return result

# Even with gather, these run one at a time (GIL + no yield points)
async def slow_main():
    results = await asyncio.gather(
        cpu_heavy_task(10_000_000),
        cpu_heavy_task(10_000_000),
    )
    # Still takes ~2x as long as one task — no parallelism for CPU work!
```

**Rule:** async = good for waiting on I/O. For CPU-heavy computation (parsing, transformations, ML inference), use `concurrent.futures.ProcessPoolExecutor` instead.

---

## A Simple DE Example: Async API Ingestion

```python
import asyncio
import aiohttp
import json
from datetime import date


async def fetch_daily_metrics(
    session: aiohttp.ClientSession,
    source: str,
    api_date: str
) -> dict:
    """Fetch one day's metrics from one data source."""
    url = f"https://metrics-api.internal/sources/{source}/daily?date={api_date}"
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Warning: {source} returned {response.status} for {api_date}")
            return {"source": source, "date": api_date, "error": response.status}
        data = await response.json()
        data["source"] = source
        return data


async def ingest_all_sources(sources: list[str], api_date: str) -> list[dict]:
    """Fetch metrics from all sources concurrently."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_daily_metrics(session, source, api_date)
            for source in sources
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions
    return [r for r in results if not isinstance(r, Exception)]


# Run the ingestion
sources = ["salesforce", "stripe", "hubspot", "zendesk", "mixpanel"]
today = date.today().isoformat()

metrics = asyncio.run(ingest_all_sources(sources, today))
print(f"Ingested {len(metrics)} sources in one async batch")

# Write to storage (sync write is fine here — only one write call)
with open(f"metrics_{today}.json", "w") as f:
    json.dump(metrics, f)
```

---

## Key Takeaways for Junior DEs

1. **Async solves I/O wait**, not CPU work. If your bottleneck is waiting for HTTP responses or database acknowledgments, async can multiply throughput.
2. **`await` yields control** to the event loop, allowing other coroutines to run while the current one waits.
3. **`asyncio.gather()`** runs multiple coroutines concurrently — the single most useful async primitive for DE ingestion pipelines.
4. **`asyncio.run(main())`** is the entry point from synchronous code.
5. **CPU-bound work** (pandas transformations, JSON parsing of huge files) won't benefit from asyncio — use `multiprocessing` for that.
6. **aiohttp** is the async HTTP client to use instead of `requests`.
