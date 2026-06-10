---
title: "Python APIs (requests/FastAPI) - Scenario Questions"
topic: python
subtopic: apis-requests-fastapi
content_type: scenario_question
tags: [python, apis, interview, scenarios, pagination, rate-limiting, api-design]
---

# Scenario Questions — Python APIs

<article data-difficulty="junior">

## Junior: Fetch Data from a REST API

**Scenario:** Write a function that fetches user data from `https://api.example.com/v1/users`. The API requires an API key in the header, returns JSON, and you need to handle common errors (timeout, 404, server errors).

<details>
<summary>Solution</summary>

```python
import requests
from typing import Optional, Dict

def fetch_users(api_key: str, status: str = "active") -> list[dict]:
    """Fetch users from API with proper error handling."""
    url = "https://api.example.com/v1/users"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"status": status, "limit": 100}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()["users"]
        elif response.status_code == 401:
            raise ValueError("Invalid API key — check credentials")
        elif response.status_code == 404:
            return []  # No users found
        elif response.status_code == 429:
            raise RuntimeError("Rate limited — retry later")
        else:
            response.raise_for_status()
    
    except requests.Timeout:
        raise RuntimeError("API request timed out after 30s")
    except requests.ConnectionError:
        raise RuntimeError("Cannot connect to API — check network")

# Usage
users = fetch_users(api_key="sk_live_abc123", status="active")
```

</details>

</article>

<article data-difficulty="mid-level">

## Mid-Level: Handle Pagination and Rate Limits

**Scenario:** Extract all records from an API that uses cursor pagination and enforces a rate limit of 100 requests/minute. The API returns `{"data": [...], "meta": {"next_cursor": "abc", "total": 5000}}`. Design a generator that handles pagination, rate limits, and retries.

<details>
<summary>Solution</summary>

```python
import time
import requests
from typing import Iterator, Dict
import logging

logger = logging.getLogger(__name__)

def extract_all_records(
    base_url: str,
    api_key: str,
    page_size: int = 100,
    max_requests_per_minute: int = 100,
) -> Iterator[Dict]:
    """
    Generator that handles cursor pagination with rate limiting.
    Yields individual records — caller doesn't need to know about pages.
    """
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {api_key}"
    
    min_interval = 60.0 / max_requests_per_minute  # seconds between requests
    cursor = None
    total_yielded = 0
    request_count = 0
    
    while True:
        # Rate limiting
        time.sleep(min_interval)
        
        # Build request
        params = {"limit": page_size}
        if cursor:
            params["cursor"] = cursor
        
        # Fetch with retry
        response = _fetch_with_retry(session, base_url, params)
        request_count += 1
        
        data = response.json()
        records = data.get("data", [])
        meta = data.get("meta", {})
        
        if not records:
            break
        
        for record in records:
            yield record
            total_yielded += 1
        
        # Progress logging
        total = meta.get("total", "unknown")
        logger.info(f"Fetched page {request_count}: {total_yielded}/{total} records")
        
        # Next page
        cursor = meta.get("next_cursor")
        if not cursor:
            break
    
    logger.info(f"Extraction complete: {total_yielded} records in {request_count} requests")

def _fetch_with_retry(session, url, params, max_retries=3):
    """Fetch with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            return response
        
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** (attempt + 1)
            logger.warning(f"Request failed (attempt {attempt+1}), retrying in {wait}s: {e}")
            time.sleep(wait)

# Usage — simple iteration hides all complexity
for record in extract_all_records(
    base_url="https://api.crm.com/v2/contacts",
    api_key="sk_live_key",
    max_requests_per_minute=80  # Stay under limit
):
    insert_to_staging(record)
```

</details>

</article>

<article data-difficulty="senior">

## Senior: Design a Data Ingestion API

**Scenario:** Design a FastAPI-based data ingestion API that:
1. Accepts batch record uploads (up to 10K records per request)
2. Validates records against a configurable schema
3. Returns detailed validation errors for bad records
4. Writes valid records to a staging table asynchronously
5. Provides a status endpoint to check batch processing state
6. Handles concurrent uploads without data loss

<details>
<summary>Solution</summary>

```python
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum
import uuid
import asyncio

app = FastAPI(title="Data Ingestion API")

class BatchStatus(str, Enum):
    ACCEPTED = "accepted"
    VALIDATING = "validating"
    LOADING = "loading"
    COMPLETED = "completed"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"

class RecordValidationError(BaseModel):
    record_index: int
    field: str
    error: str
    value: Any = None

class BatchSubmission(BaseModel):
    source_system: str
    schema_name: str
    records: List[Dict[str, Any]] = Field(..., max_items=10000)
    idempotency_key: Optional[str] = None

class BatchResponse(BaseModel):
    batch_id: str
    status: BatchStatus
    total_records: int
    accepted_records: int = 0
    rejected_records: int = 0
    validation_errors: List[RecordValidationError] = []
    created_at: datetime
    completed_at: Optional[datetime] = None

# In-memory state (production: use Redis or DB)
batch_registry: Dict[str, BatchResponse] = {}

@app.post("/api/ingest/batch", response_model=BatchResponse)
async def submit_batch(
    submission: BatchSubmission,
    background_tasks: BackgroundTasks
):
    """Submit a batch of records for ingestion."""
    # Idempotency check
    if submission.idempotency_key:
        existing = _find_by_idempotency_key(submission.idempotency_key)
        if existing:
            return existing
    
    # Validate record count
    if len(submission.records) == 0:
        raise HTTPException(400, "Batch must contain at least one record")
    
    batch_id = str(uuid.uuid4())
    batch = BatchResponse(
        batch_id=batch_id,
        status=BatchStatus.ACCEPTED,
        total_records=len(submission.records),
        created_at=datetime.utcnow(),
    )
    batch_registry[batch_id] = batch
    
    # Process asynchronously
    background_tasks.add_task(
        process_batch, batch_id, submission
    )
    
    return batch

@app.get("/api/ingest/batch/{batch_id}", response_model=BatchResponse)
async def get_batch_status(batch_id: str):
    """Check the processing status of a submitted batch."""
    batch = batch_registry.get(batch_id)
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")
    return batch

async def process_batch(batch_id: str, submission: BatchSubmission):
    """Background task: validate and load records."""
    batch = batch_registry[batch_id]
    batch.status = BatchStatus.VALIDATING
    
    # Validate records
    schema = await get_schema(submission.schema_name)
    valid_records = []
    errors = []
    
    for i, record in enumerate(submission.records):
        record_errors = validate_record(record, schema)
        if record_errors:
            errors.extend([
                RecordValidationError(record_index=i, **err)
                for err in record_errors
            ])
        else:
            valid_records.append(record)
    
    batch.validation_errors = errors[:100]  # Limit error response size
    batch.rejected_records = len(submission.records) - len(valid_records)
    batch.accepted_records = len(valid_records)
    
    # Load valid records
    if valid_records:
        batch.status = BatchStatus.LOADING
        try:
            await load_to_staging(valid_records, submission.schema_name)
            batch.status = (
                BatchStatus.COMPLETED if not errors
                else BatchStatus.PARTIAL_FAILURE
            )
        except Exception as e:
            batch.status = BatchStatus.FAILED
            batch.validation_errors.append(
                RecordValidationError(record_index=-1, field="", error=f"Load failed: {e}")
            )
    else:
        batch.status = BatchStatus.FAILED
    
    batch.completed_at = datetime.utcnow()
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `requests` and `httpx` for making HTTP calls in Python?**
A: `requests` is synchronous and battle-tested but blocks the event loop. `httpx` supports both sync and async interfaces with an API nearly identical to `requests`, making it the preferred choice for FastAPI applications or any async codebase where blocking I/O would defeat the purpose of async.

**Q: How do you handle rate limiting and retries with the `requests` library?**
A: Use `urllib3.util.retry.Retry` with a `requests.adapters.HTTPAdapter`: configure `total` retries, `backoff_factor`, and `status_forcelist` (e.g., 429, 503). Mount the adapter on the session. For rate limiting specifically, also inspect the `Retry-After` response header and sleep accordingly.

**Q: What is FastAPI's dependency injection system and why is it useful for data engineering APIs?**
A: FastAPI's `Depends()` mechanism injects shared resources (database connections, configuration, auth tokens) into path functions without manual wiring. For DE APIs this means a single `get_db_connection` dependency can be declared once, managed with a context manager, and reused across all endpoints—clean separation of concerns and easy mocking in tests.

**Q: What is Pydantic's role in FastAPI and how does it help with data validation?**
A: FastAPI uses Pydantic models for automatic request body parsing, type coercion, and validation. If a client sends the wrong type or missing required fields, FastAPI returns a structured 422 response automatically—no manual validation code needed. Pydantic also serializes response models, enforcing output schema contracts.

**Q: How do you add authentication to a FastAPI endpoint?**
A: Use `HTTPBearer` or `OAuth2PasswordBearer` security schemes with a `Depends()` function that validates the token (JWT verification, database lookup, etc.). This keeps auth logic out of the path function and makes it composable—a single auth dependency can be shared across all protected routes.

**Q: What is the difference between a 4xx and 5xx HTTP status code in an API context?**
A: 4xx codes indicate client errors (400 Bad Request, 401 Unauthorized, 404 Not Found, 422 Unprocessable Entity)—the client sent an invalid request. 5xx codes indicate server errors (500 Internal Server Error, 502 Bad Gateway, 503 Service Unavailable)—the server failed to process a valid request. Retry logic should typically only trigger on 429 and 5xx responses.

**Q: How would you paginate a large dataset response in a FastAPI endpoint?**
A: Accept `page` and `page_size` query parameters, apply `OFFSET`/`LIMIT` in the query (or cursor-based pagination for large datasets), and return a response model with `items`, `total`, `page`, and `pages` fields. Cursor-based pagination (using a last-seen ID or timestamp) is preferred for deep pagination to avoid performance degradation.

**Q: What is background task processing in FastAPI and when would you use it?**
A: FastAPI's `BackgroundTasks` allows you to schedule a function to run after the response is returned. Use it for fire-and-forget operations (sending email notifications, writing audit logs, triggering a downstream pipeline) where the client does not need to wait for the result. For heavy work, prefer Celery or an async task queue.

---

## 💼 Interview Tips

- Demonstrate production awareness: always use `requests.Session` (not bare `requests.get`) for connection pooling, header reuse, and retry configuration in long-running DE services.
- FastAPI interviewers often ask about async: know that `async def` endpoints use the event loop and should never call blocking I/O (use `httpx.AsyncClient`, `asyncpg`, not `psycopg2`). Calling a blocking function in an `async def` route stalls the entire event loop.
- Show you understand OpenAPI documentation as a feature: FastAPI auto-generates `/docs` and `/openapi.json` from Pydantic models and route definitions—mention this as a DE API best practice for self-documenting internal tools.
- Senior interviewers test error handling: demonstrate `HTTPException(status_code=404, detail="...")` for expected errors and global exception handlers via `@app.exception_handler` for unexpected ones.
- Connect API design to DE context: data ingestion APIs, metadata APIs for catalog services, and webhook receivers are common FastAPI use cases in DE. Show you know the patterns, not just the syntax.
