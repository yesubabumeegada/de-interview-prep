---
title: "Async / asyncio — Scenarios"
topic: python
subtopic: async-asyncio
content_type: scenario_question
tags: [python, asyncio, API-ingestion, rate-limiting, concurrent, interview]
---

# Async / asyncio — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Explain Async vs Sync in a DE Context

**Scenario:** Your team has a Python script that ingests data from 20 REST API endpoints sequentially. It takes 30 minutes to run. A senior engineer says "we should make this async." You're asked in a code review meeting: "What does async mean here, and will it actually help? What's the risk?"

<details>
<summary>💡 Hint</summary>

Think about what the script is spending most of its time doing — is it computation or waiting? What does "async" mean conceptually (event loop, concurrency vs parallelism)? What risks exist when moving from sequential to concurrent API calls?

</details>

<details>
<summary>✅ Solution</summary>

```python
# Current sequential code (30 minutes):
import requests
import time

def fetch_source(url: str, token: str) -> dict:
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    return resp.json()

def ingest_all_sequential(sources: list[dict]) -> list[dict]:
    return [fetch_source(s["url"], s["token"]) for s in sources]

# Analysis:
# 20 API calls × avg 90 seconds each = 30 minutes
# Each call: ~0.5s of actual computation + ~89.5s waiting for response
# 99.7% of the time is spent WAITING, not computing
# → Perfect candidate for async

# Async version (expected ~90-120 seconds total instead of 30 minutes):
import asyncio
import aiohttp

async def fetch_source_async(
    session: aiohttp.ClientSession,
    source: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Fetch one source — non-blocking."""
    async with semaphore:  # Limit to 5 concurrent calls
        async with session.get(
            source["url"],
            headers={"Authorization": f"Bearer {source['token']}"}
        ) as resp:
            return await resp.json()


async def ingest_all_async(sources: list[dict]) -> list[dict]:
    semaphore = asyncio.Semaphore(5)  # At most 5 simultaneous calls
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_source_async(session, s, semaphore) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Separate successes from errors
    successes = [r for r in results if not isinstance(r, Exception)]
    failures  = [r for r in results if isinstance(r, Exception)]
    if failures:
        print(f"Warning: {len(failures)} sources failed")
    return successes


# Run it:
sources = [{"url": f"https://api{i}.example.com/data", "token": "tok"} for i in range(20)]
results = asyncio.run(ingest_all_async(sources))
# Expected time: ~90-120 seconds (limited by slowest API, not sum of all)
```

**Explanation for the meeting:**

Async = concurrency, not parallelism. The script doesn't need to do 20 things at once — it needs to *wait* for 20 things at once. Async lets Python fire all 20 requests, then process responses as they arrive, instead of waiting for each one to complete before starting the next.

**Expected improvement:** 30 minutes → ~90-120 seconds (the slowest single API call, not the sum of all).

**Risks to flag:**
1. **Rate limiting:** 20 simultaneous calls might trigger API rate limits. Solution: `asyncio.Semaphore(5)` limits to 5 concurrent calls.
2. **Error handling:** if one call fails and exceptions aren't caught, `gather()` can silently drop results. Use `return_exceptions=True`.
3. **Memory:** all 20 responses are held in memory simultaneously. For large responses, add streaming/chunking.
4. **Testing complexity:** async code is harder to test and debug than sequential code.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Rewrite a Slow Sequential API Ingestion Job to Be Async

**Scenario:** You have this production script that ingests metrics from 50 partner APIs and stores them in PostgreSQL. It takes 2 hours to run. Rewrite it to run in under 5 minutes, keeping the same error handling and database write guarantees.

```python
# Current slow code:
import requests, psycopg2, logging

def fetch_partner_metrics(partner_id: str, api_key: str, date: str) -> list[dict]:
    url = f"https://partners.api.com/v1/{partner_id}/metrics?date={date}"
    resp = requests.get(url, headers={"X-API-Key": api_key}, timeout=120)
    resp.raise_for_status()
    return resp.json()["metrics"]

def write_to_db(conn, metrics: list[dict], partner_id: str):
    cur = conn.cursor()
    for m in metrics:
        cur.execute(
            "INSERT INTO partner_metrics (partner_id, date, metric_name, value) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (partner_id, m["date"], m["name"], m["value"])
        )
    conn.commit()

def run(partners: list[dict], date: str, db_conn_str: str):
    conn = psycopg2.connect(db_conn_str)
    for partner in partners:
        try:
            metrics = fetch_partner_metrics(partner["id"], partner["api_key"], date)
            write_to_db(conn, metrics, partner["id"])
        except Exception as e:
            logging.error(f"Partner {partner['id']} failed: {e}")
    conn.close()
```

<details>
<summary>💡 Hint</summary>

The bottleneck is the sequential HTTP calls (50 × ~2.4 min each = 2 hours). Async + `asyncio.gather` with a semaphore can run all 50 calls concurrently. For the DB writes, use asyncpg with a connection pool. Keep the error isolation (one partner failure shouldn't stop others).

</details>

<details>
<summary>✅ Solution</summary>

```python
import asyncio
import aiohttp
import asyncpg
import logging
from datetime import date as date_type

logger = logging.getLogger(__name__)


async def fetch_partner_metrics_async(
    session: aiohttp.ClientSession,
    partner: dict,
    fetch_date: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[dict]]:
    """Fetch metrics for one partner. Returns (partner_id, metrics)."""
    async with semaphore:
        url = f"https://partners.api.com/v1/{partner['id']}/metrics?date={fetch_date}"
        async with session.get(
            url,
            headers={"X-API-Key": partner["api_key"]},
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
            metrics = body["metrics"]
            logger.info(f"Partner {partner['id']}: {len(metrics)} metrics fetched")
            return partner["id"], metrics


async def write_partner_metrics(
    pool: asyncpg.Pool,
    partner_id: str,
    metrics: list[dict],
    fetch_date: str,
) -> int:
    """Write metrics to DB using connection pool."""
    rows = [
        (partner_id, fetch_date, m["name"], m["value"])
        for m in metrics
    ]
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO partner_metrics (partner_id, date, metric_name, value) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            rows,
        )
    return len(rows)


async def run_async(
    partners: list[dict],
    fetch_date: str,
    db_dsn: str,
    max_concurrent_fetches: int = 10,  # Semaphore limit
) -> dict:
    """
    Async version: fetch all partners concurrently, write to DB concurrently.
    Same error isolation as the sync version — one partner failure doesn't stop others.
    """
    results = {"success": [], "errors": {}}

    # Shared resources
    semaphore = asyncio.Semaphore(max_concurrent_fetches)
    pool = await asyncpg.create_pool(db_dsn, min_size=5, max_size=20)

    # Step 1: Fetch all partners concurrently
    async with aiohttp.ClientSession() as session:
        fetch_tasks = [
            fetch_partner_metrics_async(session, partner, fetch_date, semaphore)
            for partner in partners
        ]
        fetch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    # Step 2: Write successful fetches to DB concurrently
    write_tasks = []
    for partner, fetch_result in zip(partners, fetch_results):
        if isinstance(fetch_result, Exception):
            results["errors"][partner["id"]] = str(fetch_result)
            logger.error(f"FETCH FAILED: {partner['id']}: {fetch_result}")
        else:
            partner_id, metrics = fetch_result
            write_tasks.append(write_partner_metrics(pool, partner_id, metrics, fetch_date))

    write_results = await asyncio.gather(*write_tasks, return_exceptions=True)

    for partner, write_result in zip(
        [p for p in partners if p["id"] not in results["errors"]], write_results
    ):
        if isinstance(write_result, Exception):
            results["errors"][partner["id"]] = f"DB write failed: {write_result}"
            logger.error(f"WRITE FAILED: {partner['id']}: {write_result}")
        else:
            results["success"].append(partner["id"])

    await pool.close()

    logger.info(
        f"Completed: {len(results['success'])}/{len(partners)} partners. "
        f"Failures: {len(results['errors'])}"
    )
    return results


# Run it:
# asyncio.run(run_async(partners, "2024-01-20", "postgresql://..."))
```

**Performance improvement:**
- Before: 50 partners × ~2.4 min sequential = **120 minutes**
- After: 50 partners with semaphore=10 (10 concurrent) = slowest 10 batches × ~2.4 min = **~5 minutes**

Key improvements over the sequential version:
1. `asyncio.gather` for all fetches — starts all 50 nearly simultaneously
2. `asyncio.Semaphore(10)` prevents overwhelming the partner API
3. `asyncpg` connection pool for concurrent DB writes
4. Error isolation preserved — one failed partner doesn't stop others
5. Separated fetch and write phases so DB writes don't delay fetches

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design an Async Pipeline for 50 REST APIs with Rate Limiting and Error Handling

**Scenario:** Your company ingests data from 50 external REST APIs. Each API has different rate limits (some 5 req/min, some 1000 req/min), authentication methods, response formats, and reliability (some are flaky). The pipeline must: complete within 10 minutes, handle per-API rate limits correctly, retry transient errors with backoff, report per-source SLA metrics, and be observable in production. Design the architecture.

<details>
<summary>💡 Hint</summary>

You need per-source rate limiting (not a global semaphore), a retry decorator that respects retry-after headers, a results aggregator that tracks SLA metrics per source, and structured logging for observability. Think about how the retry + rate limit + error collection pieces compose.

</details>

<details>
<summary>✅ Solution</summary>

```python
import asyncio
import aiohttp
import time
import logging
from dataclasses import dataclass, field
from typing import Any
import random

logger = logging.getLogger(__name__)


@dataclass
class ApiConfig:
    """Configuration for one API source."""
    name: str
    base_url: str
    auth_token: str
    calls_per_minute: int = 60       # Per-source rate limit
    timeout_seconds: int = 30
    max_retries: int = 3
    response_records_key: str = "data"


@dataclass
class IngestionResult:
    """Tracks results and SLA metrics for one API source."""
    source: str
    records: list[dict] = field(default_factory=list)
    error: str | None = None
    attempts: int = 0
    total_duration_s: float = 0.0
    rate_limited_count: int = 0

    @property
    def success(self) -> bool:
        return self.error is None

    def to_metrics(self) -> dict:
        return {
            "source": self.source,
            "success": self.success,
            "record_count": len(self.records),
            "attempts": self.attempts,
            "duration_s": round(self.total_duration_s, 2),
            "rate_limited_count": self.rate_limited_count,
            "error": self.error,
        }


class PerSourceRateLimiter:
    """Token bucket rate limiter — one instance per API source."""

    def __init__(self, calls_per_minute: int):
        self.interval = 60.0 / calls_per_minute
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self._last_call = time.monotonic()


async def fetch_source_full(
    session: aiohttp.ClientSession,
    config: ApiConfig,
    fetch_date: str,
    rate_limiter: PerSourceRateLimiter,
) -> IngestionResult:
    """
    Full-featured fetch for one API source:
    - Per-source rate limiting
    - Retry with exponential backoff + jitter
    - Retry-After header handling
    - Structured result with SLA metrics
    """
    result = IngestionResult(source=config.name)
    start_time = time.monotonic()

    for attempt in range(config.max_retries):
        result.attempts = attempt + 1
        await rate_limiter.acquire()

        try:
            url = f"{config.base_url}/data?date={fetch_date}"
            async with session.get(
                url,
                headers={"Authorization": f"Bearer {config.auth_token}"},
                timeout=aiohttp.ClientTimeout(total=config.timeout_seconds),
            ) as resp:

                if resp.status == 200:
                    body = await resp.json()
                    result.records = body.get(config.response_records_key, [])
                    result.total_duration_s = time.monotonic() - start_time
                    logger.info(
                        "source=%s status=success records=%d attempts=%d duration=%.1fs",
                        config.name, len(result.records), result.attempts, result.total_duration_s
                    )
                    return result

                if resp.status == 429:
                    result.rate_limited_count += 1
                    retry_after = float(resp.headers.get("Retry-After", 30))
                    logger.warning("source=%s status=rate_limited wait=%.0f", config.name, retry_after)
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status >= 500:
                    delay = min(2 ** attempt + random.uniform(0, 1), 30)
                    logger.warning("source=%s status=%d attempt=%d retry_in=%.1f",
                                   config.name, resp.status, attempt + 1, delay)
                    await asyncio.sleep(delay)
                    continue

                # 4xx client error — don't retry
                result.error = f"HTTP {resp.status}: {await resp.text()}"
                result.total_duration_s = time.monotonic() - start_time
                logger.error("source=%s status=client_error error=%s", config.name, result.error)
                return result

        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            delay = min(2 ** attempt + random.uniform(0, 1), 30)
            logger.warning("source=%s connection_error=%s attempt=%d retry_in=%.1f",
                           config.name, type(e).__name__, attempt + 1, delay)
            await asyncio.sleep(delay)

    result.error = f"All {config.max_retries} attempts failed"
    result.total_duration_s = time.monotonic() - start_time
    logger.error("source=%s status=exhausted_retries", config.name)
    return result


async def run_ingestion_pipeline(
    api_configs: list[ApiConfig],
    fetch_date: str,
) -> dict[str, Any]:
    """
    Run ingestion for all 50 APIs concurrently with per-source rate limiting.
    """
    # Create per-source rate limiters
    rate_limiters = {
        config.name: PerSourceRateLimiter(config.calls_per_minute)
        for config in api_configs
    }

    pipeline_start = time.monotonic()

    # All APIs run concurrently, each managing its own rate limit
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=100)  # Global connection limit
    ) as session:
        tasks = [
            fetch_source_full(
                session, config, fetch_date, rate_limiters[config.name]
            )
            for config in api_configs
        ]
        results: list[IngestionResult] = await asyncio.gather(*tasks)

    pipeline_duration = time.monotonic() - pipeline_start

    # Aggregate metrics
    all_records = []
    metrics_by_source = {}
    for result in results:
        metrics_by_source[result.source] = result.to_metrics()
        all_records.extend(result.records)

    successes = sum(1 for r in results if r.success)
    failures  = sum(1 for r in results if not r.success)
    failed_sources = [r.source for r in results if not r.success]

    summary = {
        "total_duration_s": round(pipeline_duration, 2),
        "sources_total": len(api_configs),
        "sources_succeeded": successes,
        "sources_failed": failures,
        "failed_sources": failed_sources,
        "total_records": len(all_records),
        "sla_metrics": metrics_by_source,
    }

    logger.info(
        "pipeline=complete duration=%.1fs sources=%d/%d records=%d",
        pipeline_duration, successes, len(api_configs), len(all_records)
    )

    # SLA check: fail if success rate < 90%
    success_rate = successes / len(api_configs)
    if success_rate < 0.9:
        raise RuntimeError(
            f"SLA breach: {success_rate:.0%} success rate "
            f"(failed: {failed_sources})"
        )

    return {"records": all_records, "summary": summary}
```

**Architecture highlights:**
- **Per-source `PerSourceRateLimiter`**: each API gets its own token bucket — a fast API isn't throttled by a slow one.
- **Retry-After header**: rate limit responses include a `Retry-After` header; we respect it instead of using arbitrary backoff.
- **Structured logging** (`key=value` format): every event is observable without parsing freeform strings.
- **`IngestionResult` dataclass**: carries both the data and SLA metrics, keeping the function signature clean.
- **SLA check at the end**: pipeline fails loudly if too many sources fail — prevents silently processing incomplete data.
- **Total wall time**: with 50 APIs and per-source rate limiting, all run concurrently, so total time ≈ slowest single API (not sum of all).

</details>

</article>

---

## Interview Tips

> **Tip 1:** "When would you use `asyncio.gather` vs `asyncio.wait`?" — Use `gather` when you want all results in order and can tolerate all tasks running to completion. Use `wait` when you need to react to tasks as they complete (process first-available), or when you need a timeout to cancel remaining tasks. In DE pipelines, `gather(return_exceptions=True)` is most common.

> **Tip 2:** "Does async make Python use multiple CPU cores?" — No. asyncio is single-threaded and single-process. The event loop runs on one CPU core. Async only helps when tasks are I/O-bound — they spend time waiting for external systems, not burning CPU. For multiple cores, use `multiprocessing` or `concurrent.futures.ProcessPoolExecutor`. Many candidates confuse async (concurrency) with parallelism (simultaneous CPU execution).

> **Tip 3:** "How do you test async code?" — Use `pytest-asyncio` which allows `async def test_` functions. For testing functions that call external APIs, use `aiohttp.web.Application` to create a mock HTTP server in the test, or mock `aiohttp.ClientSession` using `unittest.mock.AsyncMock`. Always test error paths: what happens when the API returns 429, 500, or times out.
