---
title: "Python APIs (requests/FastAPI) - Intermediate"
topic: python
subtopic: apis-requests-fastapi
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, apis, fastapi, async, httpx, aiohttp, rate-limiting, sessions, retry]
---

# Python APIs — Intermediate Concepts

## FastAPI Basics — Building Data APIs

FastAPI is the modern Python framework for building APIs. It's async-first, uses type hints for validation, and generates docs automatically:

```python
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

app = FastAPI(title="Data Pipeline API", version="1.0.0")

# Request/Response models with Pydantic
class PipelineRunRequest(BaseModel):
    pipeline_name: str = Field(..., description="Name of pipeline to trigger")
    execution_date: date
    parameters: Optional[dict] = None

class PipelineRunResponse(BaseModel):
    run_id: str
    status: str
    started_at: str

@app.post("/pipelines/trigger", response_model=PipelineRunResponse)
async def trigger_pipeline(request: PipelineRunRequest):
    """Trigger a pipeline execution."""
    run_id = generate_run_id()
    # Queue the pipeline execution
    await queue_pipeline(request.pipeline_name, request.execution_date)
    return PipelineRunResponse(
        run_id=run_id,
        status="queued",
        started_at=datetime.utcnow().isoformat()
    )

@app.get("/data/events")
async def get_events(
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Query events with pagination."""
    events = await fetch_events(start_date, end_date, limit, offset)
    return {"results": events, "count": len(events), "offset": offset}
```

---

## Async HTTP Clients — httpx and aiohttp

### httpx — Modern Async HTTP

```python
import httpx
import asyncio
from typing import List, Dict

async def fetch_multiple_endpoints(urls: List[str]) -> List[Dict]:
    """Fetch from multiple APIs concurrently."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [client.get(url) for url in urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    results = []
    for url, response in zip(urls, responses):
        if isinstance(response, Exception):
            print(f"Failed: {url} — {response}")
        elif response.status_code == 200:
            results.append(response.json())
    
    return results

async def stream_large_response(url: str):
    """Stream a large API response without loading fully into memory."""
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url) as response:
            async for chunk in response.aiter_lines():
                record = json.loads(chunk)
                yield record
```

### aiohttp — High-Performance Async Client

```python
import aiohttp
import asyncio
from typing import AsyncIterator, Dict

async def bulk_api_extraction(
    endpoints: List[str],
    headers: Dict[str, str],
    concurrency: int = 10
) -> List[Dict]:
    """
    Fetch many endpoints with controlled concurrency.
    Analogy: Like a pool of workers — max 10 working at once,
    but total throughput is much higher than sequential.
    """
    semaphore = asyncio.Semaphore(concurrency)
    results = []
    
    async def fetch_one(session, url):
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                return None
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, url) for url in endpoints]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    return [r for r in responses if r and not isinstance(r, Exception)]
```

---

## Rate Limiting — Respecting API Constraints

```python
import time
import asyncio
from typing import Iterator, Dict

class RateLimiter:
    """
    Token bucket rate limiter.
    Allows bursting up to bucket size, then limits to rate.
    """
    
    def __init__(self, requests_per_second: float, burst_size: int = 1):
        self.rate = requests_per_second
        self.burst_size = burst_size
        self._tokens = burst_size
        self._last_refill = time.time()
    
    def wait(self):
        """Block until a request token is available."""
        self._refill()
        while self._tokens < 1:
            sleep_time = (1 - self._tokens) / self.rate
            time.sleep(sleep_time)
            self._refill()
        self._tokens -= 1
    
    async def async_wait(self):
        """Async version — doesn't block the event loop."""
        self._refill()
        while self._tokens < 1:
            sleep_time = (1 - self._tokens) / self.rate
            await asyncio.sleep(sleep_time)
            self._refill()
        self._tokens -= 1
    
    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst_size, self._tokens + elapsed * self.rate)
        self._last_refill = now

# Usage
limiter = RateLimiter(requests_per_second=5, burst_size=10)

def fetch_with_rate_limit(urls: list[str]) -> list[dict]:
    results = []
    for url in urls:
        limiter.wait()  # Blocks if rate exceeded
        response = requests.get(url, timeout=30)
        results.append(response.json())
    return results
```

---

## Session Management — Connection Reuse

```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_resilient_session(
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    base_url: str = None
) -> requests.Session:
    """
    Create a session with connection pooling and automatic retry.
    Sessions reuse TCP connections — much faster for multiple requests.
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20,
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    if base_url:
        session.headers["Authorization"] = f"Bearer {get_token()}"
    
    return session

# Usage — session reuses connections (much faster than individual requests)
session = create_resilient_session(max_retries=3)
session.headers["Authorization"] = "Bearer my-token"

for endpoint in endpoints:
    response = session.get(f"https://api.example.com/{endpoint}", timeout=30)
    process(response.json())
```

---

## Retry Patterns for API Calls

```python
import time
import random
import requests
from functools import wraps
from typing import Callable

def api_retry(
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    retryable_codes: tuple = (429, 500, 502, 503, 504)
):
    """Decorator for API calls with intelligent retry."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    response = func(*args, **kwargs)
                    
                    if response.status_code == 429:
                        # Use Retry-After header if available
                        wait = float(response.headers.get("Retry-After", backoff_base ** attempt))
                        time.sleep(wait)
                        continue
                    
                    if response.status_code in retryable_codes:
                        if attempt < max_attempts:
                            wait = backoff_base ** attempt + random.uniform(0, 1)
                            time.sleep(wait)
                            continue
                    
                    return response
                    
                except (requests.ConnectionError, requests.Timeout) as e:
                    if attempt == max_attempts:
                        raise
                    wait = backoff_base ** attempt
                    time.sleep(wait)
            
            return response
        return wrapper
    return decorator

@api_retry(max_attempts=5)
def fetch_user_data(user_id: str):
    return requests.get(
        f"https://api.example.com/users/{user_id}",
        timeout=10
    )
```

---

## Combining Patterns — Production Data Extractor

```python
class ProductionAPIExtractor:
    """Combines session, rate limiting, retry, and pagination."""
    
    def __init__(self, base_url: str, api_key: str, rate_limit: float = 10.0):
        self.base_url = base_url
        self.session = create_resilient_session(max_retries=3)
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        self.limiter = RateLimiter(rate_limit, burst_size=5)
    
    def extract_all(self, endpoint: str, params: dict = None) -> list[dict]:
        """Extract all pages from an endpoint."""
        all_records = []
        cursor = None
        
        while True:
            self.limiter.wait()
            
            request_params = {**(params or {}), "limit": 200}
            if cursor:
                request_params["cursor"] = cursor
            
            response = self.session.get(
                f"{self.base_url}/{endpoint}",
                params=request_params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            all_records.extend(data.get("results", []))
            cursor = data.get("next_cursor")
            
            if not cursor:
                break
        
        return all_records
```

---

## Interview Tips

> **Tip 1:** When discussing API integration, mention sessions over individual requests: "I use `requests.Session()` with connection pooling for API extraction. It reuses TCP connections, reducing latency by avoiding the TLS handshake on every request. For extracting 10K pages, this cuts total time by 30-50%."

> **Tip 2:** Rate limiting shows you respect external services. Explain the token bucket algorithm: "I implement a token bucket that allows short bursts but maintains long-term average below the limit. This maximizes throughput while staying within API constraints." Interviewers want to know you won't get their API keys revoked.

> **Tip 3:** For FastAPI questions, highlight Pydantic validation: "FastAPI + Pydantic gives me automatic request validation, response serialization, and OpenAPI documentation. If a client sends an invalid date format, they get a clear 422 error without any manual validation code."
