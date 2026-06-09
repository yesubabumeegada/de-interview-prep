---
title: "Async / asyncio — Intermediate"
topic: python
subtopic: async-asyncio
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, asyncio, gather, semaphore, aiohttp, error-handling, async-context-manager]
---

# Async / asyncio — Intermediate

The real DE challenges with async: handling errors gracefully when 1 of 50 API calls fails, using semaphores to avoid hammering rate-limited APIs, writing async context managers for resource management, and structuring async pipelines cleanly.

---

## asyncio.gather() — Concurrent Execution

`asyncio.gather()` is the workhorse of async DE pipelines. It runs multiple coroutines concurrently and collects all results.

```python
import asyncio
import aiohttp
import time


async def fetch_report(session: aiohttp.ClientSession, report_id: int) -> dict:
    """Fetch a single report from the API."""
    async with session.get(f"https://reports-api.internal/reports/{report_id}") as r:
        r.raise_for_status()
        return await r.json()


async def fetch_all_reports(report_ids: list[int]) -> list[dict]:
    async with aiohttp.ClientSession() as session:
        # All requests fire concurrently
        results = await asyncio.gather(
            *[fetch_report(session, rid) for rid in report_ids],
            return_exceptions=True  # CRITICAL: don't let one failure cancel all
        )
    return results


# With return_exceptions=True:
# - Successful results are dicts
# - Failed requests are Exception objects
# - All results are returned, even if some failed
results = asyncio.run(fetch_all_reports(range(1, 51)))

successes = [r for r in results if not isinstance(r, Exception)]
failures  = [r for r in results if isinstance(r, Exception)]
print(f"Succeeded: {len(successes)}, Failed: {len(failures)}")
```

### gather() vs create_task() vs wait()

```python
import asyncio

# gather(): run all, return all results in order
async def gather_example():
    results = await asyncio.gather(coro1(), coro2(), coro3())
    # results[0] = result of coro1, results[1] = result of coro2, etc.
    # Order matches input order regardless of completion order

# create_task(): fire and forget, check results later
async def task_example():
    task1 = asyncio.create_task(coro1())
    task2 = asyncio.create_task(coro2())
    # Tasks are running NOW, even before you await them
    # ... do other work here ...
    result1 = await task1
    result2 = await task2

# wait(): more control — process as completed
async def wait_example():
    tasks = [asyncio.create_task(fetch_report(session, i)) for i in range(10)]
    done, pending = await asyncio.wait(tasks, timeout=30.0)

    for task in done:
        if task.exception():
            print(f"Task failed: {task.exception()}")
        else:
            process_result(task.result())

    for task in pending:
        task.cancel()  # Cancel tasks that didn't finish in time
```

---

## Semaphores: Controlling Concurrency

Without limits, `asyncio.gather()` with 1000 coroutines will open 1000 simultaneous connections. APIs rate-limit you, databases have connection limits, your OS has file descriptor limits. Use `asyncio.Semaphore` to cap concurrency.

```python
import asyncio
import aiohttp


async def fetch_with_semaphore(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    url: str
) -> dict:
    """Fetch a URL, waiting for a semaphore slot first."""
    async with semaphore:  # Blocks until a slot is available
        async with session.get(url) as response:
            return await response.json()


async def ingest_api_with_rate_limit(
    urls: list[str],
    max_concurrent: int = 10  # Max 10 simultaneous requests
) -> list[dict]:
    """Ingest multiple URLs with controlled concurrency."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_with_semaphore(semaphore, session, url)
            for url in urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    return results


# Usage: 500 URLs, but never more than 10 simultaneous connections
urls = [f"https://api.example.com/data/{i}" for i in range(500)]
results = asyncio.run(ingest_api_with_rate_limit(urls, max_concurrent=10))
```

### Semaphore for Database Connection Pooling

```python
import asyncio
import asyncpg  # async PostgreSQL client


async def write_batch_to_db(
    pool: asyncpg.Pool,
    semaphore: asyncio.Semaphore,
    records: list[dict]
) -> int:
    """Write a batch of records to DB with connection pool management."""
    async with semaphore:  # Max N concurrent DB operations
        async with pool.acquire() as conn:
            # Bulk insert
            await conn.executemany(
                "INSERT INTO events (user_id, event_type, amount) VALUES ($1, $2, $3)",
                [(r["user_id"], r["event_type"], r["amount"]) for r in records]
            )
            return len(records)


async def parallel_db_writer(all_records: list[dict], batch_size: int = 100):
    """Write records to DB in parallel batches, respecting connection pool."""
    pool = await asyncpg.create_pool("postgresql://user:pass@host/db", max_size=20)
    semaphore = asyncio.Semaphore(20)  # Match pool max_size

    # Split into batches
    batches = [all_records[i:i+batch_size] for i in range(0, len(all_records), batch_size)]

    tasks = [write_batch_to_db(pool, semaphore, batch) for batch in batches]
    counts = await asyncio.gather(*tasks, return_exceptions=True)

    await pool.close()
    total = sum(c for c in counts if isinstance(c, int))
    print(f"Wrote {total} records in {len(batches)} parallel batches")
```

---

## Error Handling in Async Code

Async error handling has two modes: let one error propagate and cancel others, or collect all errors and continue.

```python
import asyncio
import aiohttp
from typing import Any


class FetchError(Exception):
    """Wraps an API fetch error with context."""
    def __init__(self, source: str, status: int, message: str):
        self.source = source
        self.status = status
        super().__init__(f"FetchError({source}): HTTP {status} — {message}")


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    source: str,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> dict:
    """Fetch with exponential backoff retry."""
    last_error = None
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 429:  # Rate limited
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"{source}: Rate limited. Waiting {wait_time}s before retry {attempt+1}")
                    await asyncio.sleep(wait_time)
                    continue
                if response.status >= 500:  # Server error — retry
                    wait_time = retry_delay * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                    continue
                if response.status >= 400:  # Client error — don't retry
                    raise FetchError(source, response.status, await response.text())
                return await response.json()

        except aiohttp.ClientConnectionError as e:
            last_error = e
            wait_time = retry_delay * (2 ** attempt)
            print(f"{source}: Connection error on attempt {attempt+1}. Retrying in {wait_time}s")
            await asyncio.sleep(wait_time)

    raise FetchError(source, 0, f"Failed after {max_retries} retries: {last_error}")


async def ingest_with_error_collection(sources: dict[str, str]) -> dict[str, Any]:
    """
    Ingest from multiple sources. Collect errors without failing the whole pipeline.
    Returns: {"results": {...}, "errors": {...}}
    """
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[fetch_with_retry(session, url, source) for source, url in sources.items()],
            return_exceptions=True
        )

    output = {"results": {}, "errors": {}}
    for (source, url), result in zip(sources.items(), results):
        if isinstance(result, Exception):
            output["errors"][source] = str(result)
            print(f"ERROR: {source} failed: {result}")
        else:
            output["results"][source] = result

    success_rate = len(output["results"]) / len(sources) * 100
    print(f"Ingestion complete: {success_rate:.0f}% success rate")

    # Fail the pipeline if success rate is too low
    if success_rate < 80:
        raise RuntimeError(
            f"Ingestion success rate {success_rate:.0f}% below 80% threshold. "
            f"Failed sources: {list(output['errors'].keys())}"
        )

    return output
```

---

## Async Context Managers

Async context managers manage resources that require async setup/teardown.

```python
import asyncio
import aiohttp
from contextlib import asynccontextmanager


# Built-in: aiohttp.ClientSession is an async context manager
async with aiohttp.ClientSession() as session:
    # Session is open
    response = await session.get("https://api.example.com")
# Session is closed (even if an exception occurred)


# Custom async context manager using @asynccontextmanager
@asynccontextmanager
async def managed_db_connection(dsn: str):
    """Async context manager for database connections."""
    import asyncpg
    conn = await asyncpg.connect(dsn)
    print("DB connection opened")
    try:
        yield conn
    except Exception as e:
        print(f"DB operation failed: {e}")
        raise
    finally:
        await conn.close()
        print("DB connection closed")


# Usage:
async def write_events(events: list[dict]):
    async with managed_db_connection("postgresql://user:pass@host/db") as conn:
        await conn.executemany(
            "INSERT INTO events VALUES ($1, $2, $3)",
            [(e["id"], e["type"], e["value"]) for e in events]
        )
    # Connection guaranteed to be closed after this block


# Class-based async context manager
class AsyncRateLimiter:
    """Token bucket rate limiter for async API calls."""

    def __init__(self, calls_per_second: float):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self._last_call_time = 0.0

    async def __aenter__(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_call_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self._last_call_time = asyncio.get_event_loop().time()
        return self

    async def __aexit__(self, *args):
        pass


# Usage:
async def rate_limited_fetch(urls: list[str]) -> list[dict]:
    limiter = AsyncRateLimiter(calls_per_second=5.0)  # Max 5 req/sec
    async with aiohttp.ClientSession() as session:
        results = []
        for url in urls:
            async with limiter:  # Waits if we're going too fast
                async with session.get(url) as r:
                    results.append(await r.json())
    return results
```

---

## Key Takeaways

1. **`asyncio.gather(return_exceptions=True)`** is essential for pipelines that call multiple APIs — one failure shouldn't cancel all others.
2. **Semaphores** are your concurrency governor — without them, `gather(1000 tasks)` will attempt 1000 simultaneous connections and get you rate-limited or cause OOM.
3. **Exponential backoff retry** is mandatory for production API clients — implement it at the individual fetch level, not at the gather level.
4. **Async context managers** (`async with`) guarantee cleanup even on exceptions — always use them for sessions, DB connections, and file handles.
5. **Collect errors, don't propagate** for pipeline ingestion — log failures, track success rate, and fail only when below your SLA threshold.
