---
title: "Python Decorators - Intermediate"
topic: python
subtopic: decorators
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, decorators, class-decorators, context-managers, parametrized, real-world]
---

# Python Decorators — Intermediate Concepts

## Class-Based Decorators

Instead of nested functions, you can use a class with `__call__` as a decorator:

```python
import functools
import time

class Timer:
    """Class-based decorator that tracks cumulative execution time."""
    
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        self.total_time = 0
        self.call_count = 0
    
    def __call__(self, *args, **kwargs):
        start = time.perf_counter()
        result = self.func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        self.total_time += elapsed
        self.call_count += 1
        print(f"{self.func.__name__}: {elapsed:.3f}s (total: {self.total_time:.3f}s, calls: {self.call_count})")
        return result

@Timer
def process_batch(batch):
    time.sleep(0.5)
    return [x * 2 for x in batch]

process_batch([1, 2, 3])  # "process_batch: 0.500s (total: 0.500s, calls: 1)"
process_batch([4, 5, 6])  # "process_batch: 0.500s (total: 1.000s, calls: 2)"

# Access accumulated stats
print(process_batch.total_time)   # 1.0
print(process_batch.call_count)   # 2
```

**When to use class-based decorators:**
- When you need to maintain state across calls (counters, caches, timings)
- When the decorator logic is complex enough to benefit from class structure
- When you want to expose additional methods/attributes on the decorated function

---

## Decorators That Work on Both Functions and Methods

A common gotcha: decorators that work on plain functions may break when applied to class methods (due to `self`):

```python
import functools

def validate_input(func):
    """Works on both functions and methods."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # *args captures both regular args AND self/cls for methods
        # This works because we pass everything through unchanged
        for arg in args:
            if arg is None:
                raise ValueError(f"{func.__name__}: None argument not allowed")
        for key, val in kwargs.items():
            if val is None:
                raise ValueError(f"{func.__name__}: {key}=None not allowed")
        return func(*args, **kwargs)
    return wrapper

# Works on plain function
@validate_input
def process(data):
    return data

# Also works on method (self is just the first arg in *args)
class Pipeline:
    @validate_input
    def transform(self, data):
        return data.upper()
```

---

## Decorator Factories with Optional Arguments

Make a decorator that works both with and without parentheses:

```python
import functools

def retry(_func=None, *, max_attempts=3, delay=1):
    """
    Can be used as:
        @retry            → uses defaults
        @retry()          → uses defaults  
        @retry(max_attempts=5, delay=2)  → custom args
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    time.sleep(delay * attempt)
            return None
        return wrapper
    
    if _func is not None:
        # Called without parentheses: @retry
        return decorator(_func)
    # Called with parentheses: @retry() or @retry(max_attempts=5)
    return decorator

# All three work:
@retry
def func_a(): pass

@retry()
def func_b(): pass

@retry(max_attempts=5, delay=2)
def func_c(): pass
```

> **The trick:** `_func=None` with keyword-only args (`*`). If called without parens, `_func` receives the function directly. If called with parens, `_func` stays None and we return the decorator.

---

## Real-World DE Decorator Patterns

### Pattern 1: Database Connection Management

```python
import functools
from contextlib import contextmanager

def with_db_connection(db_name="warehouse"):
    """Provides a database connection to the decorated function."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            conn = get_connection(db_name)
            try:
                result = func(conn, *args, **kwargs)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        return wrapper
    return decorator

@with_db_connection("analytics_db")
def load_data(conn, table_name, data):
    """conn is automatically provided by the decorator."""
    conn.execute(f"INSERT INTO {table_name} VALUES (...)", data)
```

### Pattern 2: Rate Limiter

```python
import functools
import time
from collections import deque

def rate_limit(calls_per_second=10):
    """Limit function calls to N per second (for API rate limits)."""
    def decorator(func):
        call_times = deque()
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            # Remove calls older than 1 second
            while call_times and call_times[0] < now - 1.0:
                call_times.popleft()
            
            if len(call_times) >= calls_per_second:
                sleep_time = 1.0 - (now - call_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            call_times.append(time.time())
            return func(*args, **kwargs)
        return wrapper
    return decorator

@rate_limit(calls_per_second=5)
def call_external_api(endpoint, params):
    """Won't exceed 5 calls/second regardless of how fast you call it."""
    return requests.get(endpoint, params=params)
```

### Pattern 3: Schema Validator for Pipeline Functions

```python
import functools
from dataclasses import dataclass
from typing import Any

def validate_output(expected_columns: list, min_rows: int = 0):
    """Validate that a pipeline function returns a DataFrame with expected schema."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            # Check columns exist
            missing = set(expected_columns) - set(result.columns)
            if missing:
                raise ValueError(
                    f"{func.__name__}: Output missing columns: {missing}. "
                    f"Got: {list(result.columns)}"
                )
            
            # Check minimum row count
            row_count = len(result) if hasattr(result, '__len__') else result.count()
            if row_count < min_rows:
                raise ValueError(
                    f"{func.__name__}: Output has {row_count} rows, "
                    f"expected at least {min_rows}"
                )
            
            return result
        return wrapper
    return decorator

@validate_output(expected_columns=["user_id", "amount", "date"], min_rows=1)
def extract_orders(date_str):
    """Must return a DataFrame with user_id, amount, date columns."""
    return spark.read.parquet(f"s3://raw/orders/dt={date_str}/")
```

### Pattern 4: Caching with TTL (Time-to-Live)

```python
import functools
import time

def cache_with_ttl(ttl_seconds=300):
    """Cache function results for N seconds."""
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            
            if key in cache:
                result, timestamp = cache[key]
                if now - timestamp < ttl_seconds:
                    return result  # Cache hit
            
            # Cache miss or expired
            result = func(*args, **kwargs)
            cache[key] = (result, now)
            return result
        
        wrapper.clear_cache = lambda: cache.clear()
        return wrapper
    return decorator

@cache_with_ttl(ttl_seconds=60)
def get_config(env):
    """Fetch config from DB — cached for 60 seconds."""
    return db.query(f"SELECT * FROM configs WHERE env = '{env}'")
```

---

## Decorating Async Functions

For `async def` functions, the wrapper must also be async:

```python
import functools
import asyncio
import time

def async_timer(func):
    """Timer decorator for async functions."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"{func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

@async_timer
async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()
```

---

## Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Forgetting `@functools.wraps(func)` | Loses function name/docs | Always include it |
| Forgetting `return result` in wrapper | Function returns None | Always return the inner call result |
| Using mutable default args in factory | Shared state across instances | Use `None` default + create inside |
| Not handling `*args, **kwargs` | Breaks with different arg counts | Always use `*args, **kwargs` |
| Applying sync decorator to async func | Blocks the event loop | Write separate async-aware decorator |

---

## Interview Tips

> **Tip 1:** "Write a decorator from scratch" — Use the template: `def decorator(func): @functools.wraps(func) def wrapper(*args, **kwargs): [before] result = func(*args, **kwargs) [after] return result; return wrapper`. Practice until you can write this from memory in 30 seconds.

> **Tip 2:** "When would you use a class-based decorator?" — "When I need to maintain state across multiple calls — like counting invocations, tracking cumulative time, or implementing a circuit breaker pattern. The class instance persists between calls, unlike closure variables which require `nonlocal`."

> **Tip 3:** "How do decorators relate to DE?" — "They're how Airflow's `@task` decorator works, how retry logic wraps API calls, how we add timing/logging to every pipeline step without polluting business logic, and how frameworks like FastAPI define endpoints. They separate infrastructure concerns from business logic."
