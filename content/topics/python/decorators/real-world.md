---
title: "Python Decorators - Real-World Production Examples"
topic: python
subtopic: decorators
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, decorators, production, airflow, fastapi, pipeline, monitoring]
---

# Python Decorators — Real-World Production Examples

## Pattern 1: Airflow TaskFlow API (How @task Works)

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def daily_etl():
    """The @dag decorator turns this function into a DAG definition."""
    
    @task()
    def extract(date: str) -> dict:
        """@task makes this a PythonOperator that auto-serializes return values via XCom."""
        data = api.fetch(date=date)
        return {"records": data, "count": len(data)}
    
    @task()
    def transform(raw: dict) -> dict:
        """Input comes from XCom automatically (no explicit xcom_pull)."""
        cleaned = [clean(r) for r in raw["records"]]
        return {"records": cleaned, "count": len(cleaned)}
    
    @task()
    def load(transformed: dict):
        warehouse.insert("fact_events", transformed["records"])
    
    # Dependencies defined by data flow (not >> operator)
    raw = extract(date="{{ ds }}")
    cleaned = transform(raw)
    load(cleaned)

# Instantiate the DAG
daily_etl_dag = daily_etl()
```

> **Under the hood:** `@task` is a decorator that wraps your function in a PythonOperator, auto-serializes the return value to XCom, and auto-deserializes inputs from upstream task XComs. It's decorator magic that eliminates boilerplate.

---

## Pattern 2: FastAPI Endpoint Decorators (API for Data)

```python
from fastapi import FastAPI, Depends, HTTPException
from functools import wraps

app = FastAPI()

# Custom decorator: require API key authentication
def require_api_key(func):
    @wraps(func)
    async def wrapper(*args, api_key: str = None, **kwargs):
        if not api_key or api_key not in VALID_API_KEYS:
            raise HTTPException(status_code=401, detail="Invalid API key")
        return await func(*args, **kwargs)
    return wrapper

# Custom decorator: rate limit per client
def rate_limit(calls_per_minute=60):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, request=None, **kwargs):
            client_ip = request.client.host if request else "unknown"
            if is_rate_limited(client_ip, calls_per_minute):
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            return await func(*args, **kwargs)
        return wrapper
    return decorator

@app.get("/api/v1/metrics/{table_name}")
@require_api_key
@rate_limit(calls_per_minute=30)
async def get_table_metrics(table_name: str):
    """Expose data quality metrics via API."""
    metrics = await fetch_metrics(table_name)
    return {"table": table_name, "metrics": metrics}
```

---

## Pattern 3: Pipeline Step Decorator with Full Observability

```python
import functools
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class StepResult:
    step_name: str
    status: str  # "success", "failed", "skipped"
    duration_seconds: float
    input_count: int = 0
    output_count: int = 0
    error: str = None
    started_at: datetime = field(default_factory=datetime.now)

class PipelineObserver:
    """Collects metrics from all pipeline steps."""
    
    def __init__(self):
        self.results: list[StepResult] = []
    
    def step(self, name=None, skip_if=None):
        """Decorator that records execution metrics for each pipeline step."""
        def decorator(func):
            step_name = name or func.__name__
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Check skip condition
                if skip_if and skip_if():
                    result = StepResult(step_name, "skipped", 0.0)
                    self.results.append(result)
                    return None
                
                start = time.perf_counter()
                try:
                    output = func(*args, **kwargs)
                    elapsed = time.perf_counter() - start
                    
                    # Try to count output rows
                    output_count = len(output) if hasattr(output, '__len__') else 0
                    
                    result = StepResult(
                        step_name=step_name,
                        status="success",
                        duration_seconds=elapsed,
                        output_count=output_count
                    )
                    self.results.append(result)
                    return output
                    
                except Exception as e:
                    elapsed = time.perf_counter() - start
                    result = StepResult(
                        step_name=step_name,
                        status="failed",
                        duration_seconds=elapsed,
                        error=f"{type(e).__name__}: {str(e)[:200]}"
                    )
                    self.results.append(result)
                    raise
            
            return wrapper
        return decorator
    
    def summary(self) -> dict:
        total_time = sum(r.duration_seconds for r in self.results)
        failed = [r for r in self.results if r.status == "failed"]
        return {
            "total_steps": len(self.results),
            "successful": sum(1 for r in self.results if r.status == "success"),
            "failed": len(failed),
            "skipped": sum(1 for r in self.results if r.status == "skipped"),
            "total_duration_seconds": round(total_time, 2),
            "failed_steps": [f.step_name for f in failed],
        }

# Usage
observer = PipelineObserver()

@observer.step(name="extract_orders")
def extract(date):
    return db.query(f"SELECT * FROM orders WHERE date = '{date}'")

@observer.step(name="validate")
def validate(data):
    if len(data) == 0:
        raise ValueError("No data extracted")
    return data

@observer.step(name="transform")
def transform(data):
    return [clean(row) for row in data]

@observer.step(name="load")
def load(data):
    warehouse.bulk_insert("fact_orders", data)

# Run pipeline
try:
    raw = extract("2024-01-15")
    validated = validate(raw)
    transformed = transform(validated)
    load(transformed)
finally:
    print(observer.summary())
    # {"total_steps": 4, "successful": 4, "failed": 0, "total_duration_seconds": 12.5}
```

---

## Pattern 4: Idempotency Decorator

```python
import functools
import hashlib
import json

def idempotent(key_func=None, state_backend="dynamodb"):
    """
    Ensure a function only executes once for a given set of inputs.
    Uses an external state store to track completed executions.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate idempotency key from function inputs
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key_data = json.dumps({"func": func.__name__, "args": str(args), "kwargs": str(kwargs)})
                key = hashlib.sha256(key_data.encode()).hexdigest()
            
            # Check if already executed
            if state_store.exists(key):
                cached = state_store.get(key)
                print(f"Idempotent skip: {func.__name__} already executed for key={key[:12]}...")
                return cached["result"]
            
            # Execute
            result = func(*args, **kwargs)
            
            # Record execution
            state_store.put(key, {"result": result, "executed_at": datetime.now().isoformat()})
            
            return result
        return wrapper
    return decorator

@idempotent(key_func=lambda date, table: f"{table}_{date}")
def daily_load(date, table):
    """Won't re-execute if already ran successfully for this date+table."""
    data = extract(date)
    warehouse.merge(table, data)
    return {"rows_loaded": len(data)}

# First call: executes normally
daily_load("2024-01-15", "fact_orders")  # Runs ETL

# Second call (retry/rerun): returns cached result without re-executing
daily_load("2024-01-15", "fact_orders")  # "Idempotent skip: already executed"
```

---

## Pattern 5: Feature Flags with Decorators

```python
import functools

class FeatureFlags:
    """Control which code paths are active via configuration."""
    
    def __init__(self, config_source):
        self.config = config_source
    
    def enabled(self, flag_name, fallback=None):
        """Only execute if feature flag is enabled, otherwise run fallback."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if self.config.get(flag_name, False):
                    return func(*args, **kwargs)
                elif fallback:
                    return fallback(*args, **kwargs)
                else:
                    print(f"Feature '{flag_name}' disabled, skipping {func.__name__}")
                    return None
            return wrapper
        return decorator

flags = FeatureFlags(config_source=load_flags_from_db())

def old_transform(data):
    """Legacy transformation logic."""
    return legacy_clean(data)

@flags.enabled("new_transform_engine", fallback=old_transform)
def transform_data(data):
    """New transformation — only runs if feature flag is enabled."""
    return new_clean(data)

# If flag "new_transform_engine" is True: runs new transform
# If flag is False: automatically falls back to old_transform
```

---

## Production Checklist for Decorators

| Check | Why |
|-------|-----|
| Always use `@functools.wraps(func)` | Preserves name, docstring, module |
| Handle `*args, **kwargs` | Works with any function signature |
| Return the function's result | Don't accidentally return None |
| Handle exceptions properly | Re-raise or log, don't swallow silently |
| Consider thread safety | Use locks if decorator maintains mutable state |
| Make testable | Expose `__wrapped__` or skip-in-test option |
| Document decorator behavior | What it adds, what side effects |
| Bound state growth | Caches/logs should have size limits or TTL |

---

## Interview Tips

> **Tip 1:** "Show me a production-ready retry decorator" — Include: max_attempts parameter, exponential backoff (delay * 2^attempt), optional jitter, specific exception types to retry on, logging each retry, re-raise on final failure, and `@functools.wraps`.

> **Tip 2:** "How do you test code that uses decorators?" — "Three approaches: (1) Access `func.__wrapped__` to test the raw function without decorator behavior. (2) Set an environment variable that makes the decorator pass-through in test mode. (3) Test the decorator separately with a mock function that raises/returns controlled values."

> **Tip 3:** "Design a decorator that makes pipeline steps observable" — Describe the Observer pattern: decorator records step name, duration, input/output counts, success/failure. After pipeline runs, generate a summary report. This is how production pipelines get monitoring without polluting business logic.
