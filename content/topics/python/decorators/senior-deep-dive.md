---
title: "Python Decorators - Senior Deep Dive"
topic: python
subtopic: decorators
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, decorators, advanced, metaclass, descriptor, testing, performance]
---

# Python Decorators — Senior-Level Deep Dive

## How Decorators Work Under the Hood

### The Descriptor Protocol and Method Decorators

When you apply a decorator to a class method, Python's descriptor protocol manages how `self` gets passed:

```python
# Why does this work with methods?
@timer
def method(self, data):
    pass

# When accessed via instance, Python calls:
# type(obj).__dict__['method'].__get__(obj, type(obj))
# This is the descriptor protocol — it binds 'self' to the function
# functools.wraps preserves the descriptor behavior
```

**The implication:** If your decorator returns a plain function (not a descriptor-aware wrapper), it works fine. But if it returns a class instance, you need `__get__` for method binding:

```python
class ClassDecorator:
    def __init__(self, func):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
    
    def __get__(self, obj, objtype=None):
        """Support instance method binding."""
        if obj is None:
            return self
        return functools.partial(self.__call__, obj)
```

---

## Advanced Pattern: Decorator Registry

Create a registry that collects all decorated functions — used in plugin systems, task registration, and test discovery:

```python
class TaskRegistry:
    """Register pipeline tasks with metadata. Similar to Airflow's @task decorator."""
    
    def __init__(self):
        self._tasks = {}
    
    def register(self, name=None, schedule=None, retries=0):
        """Decorator that registers a function as a pipeline task."""
        def decorator(func):
            task_name = name or func.__name__
            self._tasks[task_name] = {
                'func': func,
                'schedule': schedule,
                'retries': retries,
                'module': func.__module__,
            }
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            wrapper._task_name = task_name
            return wrapper
        return decorator
    
    def get_all_tasks(self):
        return dict(self._tasks)
    
    def run_task(self, name, *args, **kwargs):
        task = self._tasks[name]
        return task['func'](*args, **kwargs)

# Global registry
pipeline = TaskRegistry()

@pipeline.register(name="extract_orders", schedule="0 6 * * *", retries=3)
def extract_orders(date):
    return db.query(f"SELECT * FROM orders WHERE date = '{date}'")

@pipeline.register(name="transform_orders", schedule="0 7 * * *")
def transform_orders(raw_data):
    return clean_and_validate(raw_data)

# Discover all registered tasks (like Airflow discovers DAGs)
for name, task in pipeline.get_all_tasks().items():
    print(f"Task: {name}, Schedule: {task['schedule']}, Retries: {task['retries']}")
```

> **This is how many frameworks work internally:** Airflow's `@task`, Flask's `@app.route`, pytest's `@pytest.fixture` — they all use decorator registries to collect functions and their metadata.

---

## Advanced Pattern: Circuit Breaker

Prevent cascading failures by stopping calls to a failing service:

```python
import functools
import time
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"       # Normal: all calls go through
    OPEN = "open"           # Failing: all calls rejected immediately
    HALF_OPEN = "half_open" # Testing: allow one call to test recovery

class CircuitBreaker:
    """
    Circuit breaker decorator for external service calls.
    
    - CLOSED: normal operation, calls pass through
    - OPEN: service is down, fail fast without calling
    - HALF_OPEN: after reset_timeout, allow one test call
    """
    
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker OPEN for {func.__name__}. "
                        f"Resets in {self.reset_timeout - (time.time() - self.last_failure_time):.0f}s"
                    )
            
            try:
                result = func(*args, **kwargs)
                # Success: reset failure count
                self.failure_count = 0
                self.state = CircuitState.CLOSED
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                
                raise
        
        wrapper.circuit_state = lambda: self.state
        wrapper.reset = lambda: setattr(self, 'state', CircuitState.CLOSED)
        return wrapper
    
class CircuitBreakerOpenError(Exception):
    pass

# Usage
@CircuitBreaker(failure_threshold=3, reset_timeout=30)
def call_payment_api(order_id, amount):
    """After 3 consecutive failures, stop calling for 30 seconds."""
    response = requests.post(PAYMENT_URL, json={"order": order_id, "amount": amount})
    response.raise_for_status()
    return response.json()
```

---

## Testing Decorated Functions

### Testing the Function Logic (Bypass Decorator)

```python
# Problem: decorator adds retry/timing — makes unit tests slow and flaky

# Solution 1: Access the unwrapped function via __wrapped__
@retry(max_attempts=3)
def fetch_data(url):
    return requests.get(url).json()

# In tests: call the unwrapped version directly
def test_fetch_data(mock_requests):
    # fetch_data.__wrapped__ is the original function (thanks to functools.wraps)
    result = fetch_data.__wrapped__("http://test.com/api")
    assert result == expected

# Solution 2: Make decorators configurable/disablable in tests
import os

def retry(max_attempts=3, delay=1):
    def decorator(func):
        if os.getenv("TESTING") == "true":
            return func  # No wrapping in test mode
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # ... retry logic
            pass
        return wrapper
    return decorator
```

### Testing the Decorator Itself

```python
def test_retry_decorator_retries_on_failure():
    call_count = 0
    
    @retry(max_attempts=3, delay=0)  # delay=0 for fast tests
    def flaky_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Temporary failure")
        return "success"
    
    result = flaky_function()
    assert result == "success"
    assert call_count == 3  # Called 3 times total

def test_retry_decorator_raises_after_max_attempts():
    @retry(max_attempts=2, delay=0)
    def always_fails():
        raise ValueError("Permanent failure")
    
    with pytest.raises(ValueError, match="Permanent failure"):
        always_fails()
```

---

## Performance Considerations

### Decorator Overhead

Each decorator adds function call overhead (~0.1-0.5 microseconds per call):

```python
import timeit

def bare_function(x):
    return x * 2

@simple_decorator
def decorated_function(x):
    return x * 2

# Benchmark
bare_time = timeit.timeit(lambda: bare_function(42), number=1_000_000)
decorated_time = timeit.timeit(lambda: decorated_function(42), number=1_000_000)
# Typical: bare=0.08s, decorated=0.12s (50% overhead per call)
# BUT: 0.04 microseconds per call — negligible for DE workloads
```

> **Rule of thumb:** Decorator overhead is irrelevant for data engineering (where functions process thousands of records, not millions of individual calls). Only worry about it in tight inner loops processing individual elements.

### Avoiding Memory Leaks in Decorators

```python
# BAD: Unbounded cache grows forever
def cache(func):
    results = {}  # Never cleaned!
    @functools.wraps(func)
    def wrapper(*args):
        if args not in results:
            results[args] = func(*args)
        return results[args]
    return wrapper

# GOOD: Bounded cache with eviction
from functools import lru_cache

@lru_cache(maxsize=1024)  # Bounded: evicts LRU entries after 1024
def lookup(key):
    return db.query(key)

# GOOD: TTL-based expiry (shown in intermediate section)
```

---

## Decorator Composition Patterns

### Creating a Composed Pipeline Decorator

```python
def pipeline_task(name, retries=2, timeout=300, log=True):
    """Composite decorator that applies multiple behaviors in one."""
    def decorator(func):
        # Apply in reverse order (innermost first)
        wrapped = func
        
        if timeout:
            wrapped = with_timeout(timeout)(wrapped)
        if retries:
            wrapped = retry(max_attempts=retries)(wrapped)
        if log:
            wrapped = log_execution(wrapped)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return wrapped(*args, **kwargs)
        
        # Attach metadata
        wrapper._pipeline_task = True
        wrapper._task_name = name
        return wrapper
    return decorator

# Single decorator applies logging + retry + timeout
@pipeline_task(name="extract_users", retries=3, timeout=600)
def extract_users(date):
    return api.get_users(since=date)
```

---

## Interview Tips

> **Tip 1:** "Design a retry decorator with exponential backoff and jitter" — This is a very common senior-level question. Include: configurable max attempts, base delay with exponential increase (delay * 2^attempt), random jitter to avoid thundering herd, re-raise on final failure, and proper `@functools.wraps`.

> **Tip 2:** "How would you implement a rate limiter decorator?" — Use a sliding window approach: track call timestamps in a deque, remove entries older than the window, sleep if at capacity. Show awareness of thread safety (use a lock for multi-threaded code).

> **Tip 3:** "Explain the circuit breaker pattern" — "Three states: Closed (normal), Open (failing fast), Half-Open (testing recovery). After N consecutive failures, the circuit opens — all calls fail immediately without hitting the service. After a timeout, one test call is allowed. If it succeeds, circuit closes. This prevents cascading failures when a dependency is down."

## ⚡ Cheat Sheet

**Decorator Fundamentals**
- Always use `@functools.wraps(func)` — preserves `__name__`, `__doc__`, `__wrapped__`
- `func.__wrapped__` → access original unwrapped function in tests
- Class-based decorator needs `__get__(self, obj, objtype)` to work as instance method
- Decorator overhead: ~0.04 µs per call — irrelevant for DE batch workloads

**Circuit Breaker States**
| State | Behavior | Transition |
|-------|----------|------------|
| CLOSED | All calls pass through | → OPEN after N failures |
| OPEN | Fail immediately (no call) | → HALF_OPEN after timeout |
| HALF_OPEN | Allow one test call | → CLOSED on success, → OPEN on failure |

- `failure_threshold=5, reset_timeout=60` typical defaults
- Expose `.circuit_state()` and `.reset()` on wrapper for monitoring/testing

**Decorator Registry Pattern**
- `_registry = {}` on a class; `__new__` or `__init_subclass__` auto-registers subclasses
- Powers: Airflow `@task`, Flask `@route`, pytest `@fixture`
- `wrapper._task_name = name` — attach metadata to wrapped function

**Retry Decorator Rules**
- Exponential backoff: `delay * (2 ** attempt)` with random jitter `+= random.uniform(0, delay)`
- Jitter prevents thundering herd (all retries hitting at the same second)
- Catch specific exceptions; re-raise on final failure with original traceback
- `max_attempts=3, base_delay=1` → delays: 1 s, 2 s (total wait ~3 s before giving up)

**Memory Leak Prevention**
- Unbounded `results = {}` inside decorator → grows forever; use `@lru_cache(maxsize=N)`
- `lru_cache` evicts LRU entries after `maxsize`; thread-safe; `cache_clear()` for tests
- Never store mutable state in decorator closure if the function is called concurrently

**Testing Decorators**
- Bypass: call `func.__wrapped__(*args)` to test logic without retry/timing overhead
- Test behavior: `delay=0` for fast tests; use `nonlocal call_count` to assert retry count
- Disable in CI: `if os.getenv("TESTING"): return func` inside decorator
