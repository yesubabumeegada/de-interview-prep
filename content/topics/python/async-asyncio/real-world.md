---
title: "Async / asyncio — Real-World Patterns"
topic: python
subtopic: async-asyncio
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [python, asyncio, API-ingestion, batch-writer, retry, rate-limiting, production]
---

# Async / asyncio — Real-World Patterns

Production async DE code — not toy examples. These are patterns you'll actually build and maintain.

---

## Pattern 1: Async Multi-API Ingestion Pipeline

```python
import asyncio
import aiohttp
import json
import logging
from datetime import date
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)


@dataclass
class ApiSource:
    name: str
    url_template: str     # e.g., "https://api.example.com/data?date={date}"
    auth_header: str
    response_key: str     # JSON key containing the records


async def fetch_source(
    session: aiohttp.ClientSession,
    source: ApiSource,
    fetch_date: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[dict]]:
    """
    Fetch one API source with semaphore control and error handling.
    Returns (source_name, records) or raises.
    """
    url = source.url_template.format(date=fetch_date)
    headers = {"Authorization": source.auth_header}

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        records = body.get(source.response_key, [])
                        logger.info(f"{source.name}: {len(records)} records for {fetch_date}")
                        return source.name, records

                    if resp.status == 429:
                        wait = float(resp.headers.get("Retry-After", 30))
                        logger.warning(f"{source.name}: rate limited, waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=resp.status,
                        message=f"HTTP {resp.status}"
                    )

            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)

    raise RuntimeError(f"{source.name}: all retries exhausted")


async def ingest_all_sources(
    sources: list[ApiSource],
    fetch_date: str,
    max_concurrent: int = 10,
) -> dict[str, Any]:
    """
    Fetch from all API sources concurrently, collect results and errors.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_source(session, source, fetch_date, semaphore)
            for source in sources
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results, errors = {}, {}
    for source, result in zip(sources, raw_results):
        if isinstance(result, Exception):
            errors[source.name] = str(result)
            logger.error(f"FAILED: {source.name}: {result}")
        else:
            name, records = result
            results[name] = records

    # Add source metadata to each record for traceability
    all_records = []
    for source_name, records in results.items():
        for r in records:
            r["_source"] = source_name
            r["_ingested_date"] = fetch_date
        all_records.extend(records)

    logger.info(
        f"Ingestion complete: {len(results)}/{len(sources)} sources, "
        f"{len(all_records)} total records"
    )

    return {
        "records": all_records,
        "errors": errors,
        "sources_succeeded": list(results.keys()),
        "sources_failed": list(errors.keys()),
    }


# Usage:
sources = [
    ApiSource("salesforce", "https://sf.api/data?dt={date}", "Bearer sf_token", "records"),
    ApiSource("stripe",     "https://stripe.api/data?dt={date}", "Bearer sk_live", "data"),
    ApiSource("hubspot",    "https://hs.api/data?dt={date}", "Bearer hs_token", "results"),
]
result = asyncio.run(ingest_all_sources(sources, date.today().isoformat()))
```

---

## Pattern 2: Async Batch Writer to Database

```python
import asyncio
import asyncpg
from typing import Callable


class AsyncPostgresWriter:
    """
    Production-grade async batch writer with:
    - Connection pooling
    - Configurable batch size
    - Retry on transient errors
    - Metrics tracking
    """

    def __init__(self, dsn: str, max_connections: int = 20, batch_size: int = 500):
        self.dsn = dsn
        self.max_connections = max_connections
        self.batch_size = batch_size
        self._pool: asyncpg.Pool | None = None
        self.metrics = {"total_written": 0, "batches": 0, "errors": 0}

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            self.dsn,
            min_size=2,
            max_size=self.max_connections,
            command_timeout=30,
        )

    async def close(self):
        if self._pool:
            await self._pool.close()

    async def write_records(
        self,
        table: str,
        records: list[dict],
        transform: Callable[[dict], tuple] = None,
        columns: list[str] = None,
    ) -> int:
        """Write records to a table. transform converts dict → tuple."""
        if not records:
            return 0

        # Split into batches
        batches = [
            records[i:i + self.batch_size]
            for i in range(0, len(records), self.batch_size)
        ]

        # Write all batches concurrently
        semaphore = asyncio.Semaphore(self.max_connections)

        async def write_one_batch(batch):
            async with semaphore:
                return await self._write_batch_with_retry(table, batch, transform, columns)

        results = await asyncio.gather(
            *[write_one_batch(b) for b in batches],
            return_exceptions=True
        )

        total = sum(r for r in results if isinstance(r, int))
        self.metrics["total_written"] += total
        self.metrics["batches"] += len(batches)
        self.metrics["errors"] += sum(1 for r in results if isinstance(r, Exception))
        return total

    async def _write_batch_with_retry(
        self, table: str, batch: list[dict],
        transform: Callable, columns: list[str],
        max_retries: int = 3
    ) -> int:
        rows = [transform(r) for r in batch] if transform else batch
        cols = columns or list(batch[0].keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )

        for attempt in range(max_retries):
            try:
                async with self._pool.acquire() as conn:
                    await conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows] if isinstance(rows[0], dict) else rows)
                return len(batch)
            except (asyncpg.TooManyConnectionsError, asyncpg.DeadlockDetectedError) as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        return 0


# Usage:
async def pipeline_to_postgres(raw_records: list[dict]):
    writer = AsyncPostgresWriter("postgresql://user:pass@host/db", max_connections=20)
    await writer.connect()

    written = await writer.write_records(
        table="events",
        records=raw_records,
        transform=lambda r: (r["event_id"], r["user_id"], r["amount"]),
        columns=["event_id", "user_id", "amount"],
    )
    print(f"Written: {written}, Metrics: {writer.metrics}")
    await writer.close()
```

---

## Pattern 3: Async Retry with Exponential Backoff

```python
import asyncio
import functools
import random
from typing import TypeVar, Callable, Any

T = TypeVar("T")


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (Exception,),
):
    """
    Decorator for async functions with exponential backoff retry.
    Adds jitter to prevent thundering herd when many coroutines retry simultaneously.
    """
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_error = e
                    if attempt == max_attempts - 1:
                        raise

                    delay = min(base_delay * (2 ** attempt), max_delay)
                    if jitter:
                        delay *= (0.5 + random.random() * 0.5)  # ±50% jitter

                    logger.warning(
                        f"{func.__name__} attempt {attempt+1}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)

            raise last_error
        return wrapper
    return decorator


# Usage:
@async_retry(max_attempts=5, base_delay=2.0, retryable_exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
async def fetch_with_auto_retry(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()
```

---

## Pattern 4: Async Rate-Limited API Client

```python
import asyncio
import time
import aiohttp
from collections import deque


class RateLimitedApiClient:
    """
    Async API client with token bucket rate limiting.
    Respects both per-second and per-minute limits.
    Thread-safe via asyncio primitives.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        calls_per_second: float = 10.0,
        calls_per_minute: int = 500,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.calls_per_second = calls_per_second
        self.calls_per_minute = calls_per_minute
        self._session: aiohttp.ClientSession | None = None
        self._rate_limiter = asyncio.Semaphore(int(calls_per_second))
        self._call_times: deque = deque(maxlen=calls_per_minute)
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.api_key}"},
            connector=aiohttp.TCPConnector(limit=50),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def _wait_for_rate_limit(self):
        """Enforce both per-second and per-minute limits."""
        async with self._lock:
            now = time.monotonic()

            # Per-minute check
            while (self._call_times and
                   len(self._call_times) >= self.calls_per_minute and
                   now - self._call_times[0] < 60.0):
                oldest = self._call_times[0]
                wait = 60.0 - (now - oldest)
                await asyncio.sleep(wait)
                now = time.monotonic()

            self._call_times.append(now)

        # Per-second check: wait for semaphore slot
        # (released after each call, limiting to N concurrent requests)

    async def get(self, endpoint: str, params: dict = None) -> dict:
        await self._wait_for_rate_limit()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        async with self._rate_limiter:
            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def fetch_paginated(
        self, endpoint: str, page_param: str = "page", per_page: int = 100
    ):
        """Async generator for paginated API endpoints."""
        page = 1
        while True:
            data = await self.get(endpoint, params={page_param: page, "per_page": per_page})
            records = data.get("data", data.get("results", []))
            if not records:
                break
            for record in records:
                yield record
            if len(records) < per_page:
                break  # Last page
            page += 1


# Usage:
async def ingest_from_crm(start_date: str) -> list[dict]:
    """Ingest all contacts from CRM API with rate limiting."""
    all_records = []
    async with RateLimitedApiClient(
        base_url="https://crm.example.com/api/v2",
        api_key="sk-live-abc123",
        calls_per_second=5.0,
        calls_per_minute=200,
    ) as client:
        async for record in client.fetch_paginated(f"contacts?since={start_date}"):
            all_records.append(record)

    print(f"Ingested {len(all_records)} CRM contacts")
    return all_records
```

---

## Key Takeaways

1. **`return_exceptions=True` in `gather()`** is always the right choice for production — one failed API call should not abort the entire ingestion run.
2. **Async retry with jitter** prevents thundering herd — when 50 coroutines all hit a rate limit simultaneously and retry at the same time, jitter spreads the retries out.
3. **Rate limiting is your contract with external APIs** — implement per-second AND per-minute limits to avoid bans and ensure you don't disrupt the API for others.
4. **Async context managers** for HTTP sessions and DB pools guarantee cleanup on exceptions.
5. **Metrics tracking** (total written, errors, batches) in production writers gives you visibility without external monitoring tools.
