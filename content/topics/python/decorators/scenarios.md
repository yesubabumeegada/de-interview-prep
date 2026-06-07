---
title: "Python Decorators - Scenario Questions"
topic: python
subtopic: decorators
content_type: scenario_question
tags: [python, decorators, interview, scenarios]
---

# Scenario Questions — Python Decorators

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Write a Timer Decorator

**Scenario:** Write a `@timed` decorator that prints how long a function takes to execute. It should work with any function regardless of arguments.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time

def timed(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"{func.__name__} executed in {elapsed:.4f}s")
        return result
    return wrapper

@timed
def process_records(records):
    time.sleep(0.5)
    return [r * 2 for r in records]

process_records([1, 2, 3])  # "process_records executed in 0.5012s"
```

**Explanation:**
- `@functools.wraps(func)` preserves the original function's `__name__` and `__doc__`
- `*args, **kwargs` accepts any argument combination
- `time.perf_counter()` is higher precision than `time.time()`
- Always `return result` — forgetting this makes the decorated function return None

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Trace Stacked Decorator Execution

**Scenario:** Given this code, what prints when `hello()` is called? Explain the execution order.

```python
def bold(func):
    def wrapper():
        return f"<b>{func()}</b>"
    return wrapper

def italic(func):
    def wrapper():
        return f"<i>{func()}</i>"
    return wrapper

@bold
@italic
def hello():
    return "Hello"

print(hello())
```

<details>
<summary>✅ Solution</summary>

**Output:** `<b><i>Hello</i></b>`

**Explanation:**
- Decorators apply bottom-up: `@italic` wraps `hello` first, then `@bold` wraps the result
- Equivalent to: `hello = bold(italic(hello))`
- Execution order (at call time): bold's wrapper runs first (outermost), calls italic's wrapper, which calls original hello
- Think of it as layers: bold is the outer shell, italic is inner, hello is the core

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Log Function Calls

**Scenario:** Write a `@log_calls` decorator that logs function name, arguments, and return value.

<details>
<summary>✅ Solution</summary>

```python
import functools

def log_calls(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        print(f"Calling {func.__name__}({signature})")
        result = func(*args, **kwargs)
        print(f"{func.__name__} returned {result!r}")
        return result
    return wrapper

@log_calls
def add(a, b):
    return a + b

add(3, 5)
# Calling add(3, 5)
# add returned 8
```

**Explanation:**
- `repr()` gives unambiguous string representation of each argument
- `!r` in f-string is shorthand for `repr()`
- This pattern is used in production for audit logging and debugging

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Fix the Lost Function Name

**Scenario:** After decorating, `process.__name__` returns "wrapper" instead of "process". Why, and how do you fix it?

```python
def my_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@my_decorator
def process(data):
    """Process data records."""
    return data

print(process.__name__)  # "wrapper" — WRONG!
print(process.__doc__)   # None — WRONG!
```

<details>
<summary>✅ Solution</summary>

```python
import functools

def my_decorator(func):
    @functools.wraps(func)  # THIS fixes it
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@my_decorator
def process(data):
    """Process data records."""
    return data

print(process.__name__)  # "process" ✓
print(process.__doc__)   # "Process data records." ✓
```

**Explanation:**
- Without `@functools.wraps`, the wrapper replaces the original function's metadata
- `functools.wraps(func)` copies `__name__`, `__doc__`, `__module__`, and `__wrapped__` from func to wrapper
- `__wrapped__` also allows accessing the original undecorated function for testing

</details>
</article>

<article data-difficulty="junior">

## 🟢 Junior: Validate Positive Arguments

**Scenario:** Write a `@validate_positive` decorator that raises ValueError if any numeric argument is <= 0.

<details>
<summary>✅ Solution</summary>

```python
import functools

def validate_positive(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for i, arg in enumerate(args):
            if isinstance(arg, (int, float)) and arg <= 0:
                raise ValueError(f"Argument {i} must be positive, got {arg}")
        for key, val in kwargs.items():
            if isinstance(val, (int, float)) and val <= 0:
                raise ValueError(f"Argument '{key}' must be positive, got {val}")
        return func(*args, **kwargs)
    return wrapper

@validate_positive
def calculate_price(quantity, unit_price, discount=0):
    return quantity * unit_price * (1 - discount)

calculate_price(5, 29.99)           # Works fine
calculate_price(-1, 29.99)          # ValueError: Argument 0 must be positive
calculate_price(5, 29.99, discount=-0.5)  # ValueError: Argument 'discount' must be positive
```

**Explanation:**
- Checks both positional args and keyword args
- Only validates numeric types (skips strings, None, etc.)
- This pattern is useful for input validation in pipeline functions (fail fast on bad data)

</details>
</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Retry with Exponential Backoff

**Scenario:** Write a `@retry` decorator with: configurable max_attempts, exponential backoff delay, specific exception types to catch.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time

def retry(max_attempts=3, base_delay=1, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"{func.__name__} attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=4, base_delay=2, exceptions=(ConnectionError, TimeoutError))
def fetch_api(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
```

**Explanation:**
- Three-level nesting: factory (receives config) → decorator (receives func) → wrapper (executes)
- Exponential backoff: 2s, 4s, 8s (doubles each retry)
- Only retries specified exceptions (won't catch ValueError — that's a bug, not transient)
- Re-raises on final attempt (doesn't swallow the error)

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cache with TTL

**Scenario:** Write a `@cache_with_ttl(seconds)` decorator that caches results for N seconds, then recomputes on the next call.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time

def cache_with_ttl(ttl_seconds=300):
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            if key in cache:
                result, cached_at = cache[key]
                if now - cached_at < ttl_seconds:
                    return result
            result = func(*args, **kwargs)
            cache[key] = (result, now)
            return result
        
        wrapper.cache_clear = lambda: cache.clear()
        return wrapper
    return decorator

@cache_with_ttl(ttl_seconds=60)
def get_exchange_rate(currency):
    return api.fetch_rate(currency)  # Expensive API call
```

**Explanation:**
- Cache key: tuple of args + sorted kwargs (hashable)
- TTL check: if cached value is within ttl_seconds, return it
- Expired entries are overwritten on next call (lazy eviction)
- `cache_clear()` exposed for testing/manual invalidation

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Rate Limiter

**Scenario:** Write `@rate_limit(calls_per_second=10)` that prevents a function from being called more than N times per second.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time
from collections import deque

def rate_limit(calls_per_second=10):
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
def call_api(endpoint):
    return requests.get(endpoint)
```

**Explanation:**
- Sliding window: deque tracks timestamps of recent calls
- If at capacity: sleeps until the oldest call expires (1 second window)
- `deque.popleft()` is O(1) for removing expired entries
- This prevents HTTP 429 errors from rate-limited APIs

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Class-Based Decorator with Stats

**Scenario:** Write a class-based decorator that tracks: call count, total execution time, average execution time. Stats should be accessible via attributes.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time

class TrackStats:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        self.call_count = 0
        self.total_time = 0.0
    
    def __call__(self, *args, **kwargs):
        start = time.perf_counter()
        result = self.func(*args, **kwargs)
        self.total_time += time.perf_counter() - start
        self.call_count += 1
        return result
    
    @property
    def avg_time(self):
        return self.total_time / self.call_count if self.call_count else 0
    
    def reset(self):
        self.call_count = 0
        self.total_time = 0.0

@TrackStats
def process(data):
    time.sleep(0.1)
    return len(data)

process([1, 2, 3])
process([4, 5])
print(f"Calls: {process.call_count}, Avg: {process.avg_time:.3f}s")
```

**Explanation:**
- Class-based: `__call__` makes the instance callable (acts as the wrapper)
- State persists across calls (unlike closure variables which need `nonlocal`)
- `functools.update_wrapper` preserves the original function's metadata
- Stats accessible as attributes on the decorated function object

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Validate DataFrame Output

**Scenario:** Write `@validate_output(columns, min_rows)` that checks the returned DataFrame has expected columns and minimum row count.

<details>
<summary>✅ Solution</summary>

```python
import functools

def validate_output(expected_columns: list[str], min_rows: int = 0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            # Check columns
            missing = set(expected_columns) - set(result.columns)
            if missing:
                raise ValueError(f"{func.__name__}: Missing columns {missing}")
            
            # Check row count
            row_count = len(result)
            if row_count < min_rows:
                raise ValueError(f"{func.__name__}: Got {row_count} rows, need >= {min_rows}")
            
            return result
        return wrapper
    return decorator

@validate_output(columns=["user_id", "amount", "date"], min_rows=1)
def extract_orders(date_str):
    return spark.read.parquet(f"s3://raw/orders/dt={date_str}/")
```

**Explanation:**
- Validates output (not input) — acts as a contract on what the function MUST return
- Fails fast with clear error message naming the function and the violation
- This pattern is used in data pipeline frameworks to catch schema drift early

</details>
</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Pipeline Step Registry Framework

**Scenario:** Design a decorator-based framework where `@pipeline.step(name, depends_on)` registers functions as pipeline steps, automatically resolves execution order via topological sort, and runs them with data passing between steps.

<details>
<summary>✅ Solution</summary>

```python
import functools
from collections import defaultdict, deque

class Pipeline:
    def __init__(self, name):
        self.name = name
        self._steps = {}
    
    def step(self, name=None, depends_on=None):
        depends_on = depends_on or []
        def decorator(func):
            step_name = name or func.__name__
            self._steps[step_name] = {'func': func, 'depends_on': depends_on}
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def run(self):
        order = self._topological_sort()
        outputs = {}
        for step_name in order:
            config = self._steps[step_name]
            inputs = {dep: outputs[dep] for dep in config['depends_on'] if dep in outputs}
            outputs[step_name] = config['func'](**inputs) if inputs else config['func']()
            print(f"  [{step_name}] done")
        return outputs
    
    def _topological_sort(self):
        in_degree = {n: 0 for n in self._steps}
        graph = defaultdict(list)
        for name, cfg in self._steps.items():
            for dep in cfg['depends_on']:
                graph[dep].append(name)
                in_degree[name] += 1
        queue = deque(n for n in in_degree if in_degree[n] == 0)
        order = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for child in graph[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
        if len(order) != len(self._steps):
            raise ValueError("Circular dependency detected")
        return order

etl = Pipeline("daily_orders")

@etl.step(name="extract")
def extract():
    return [{"id": 1, "amount": 100}]

@etl.step(name="transform", depends_on=["extract"])
def transform(extract):
    return [{"id": r["id"], "cents": r["amount"] * 100} for r in extract]

@etl.step(name="load", depends_on=["transform"])
def load(transform):
    print(f"Loading {len(transform)} records")

etl.run()
```

**Explanation:**
- Decorator registers functions + metadata (dependencies) into a dict
- `run()` resolves execution order via Kahn's algorithm (topological sort)
- Each step's output is passed as kwargs to dependent steps
- This is how Airflow's @task decorator, Prefect, and Dagster work internally

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Circuit Breaker Pattern

**Scenario:** Implement `@circuit_breaker(failure_threshold=5, reset_timeout=60)` with three states: CLOSED (normal), OPEN (failing fast), HALF_OPEN (testing recovery).

<details>
<summary>✅ Solution</summary>

```python
import functools
import time
from enum import Enum

class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.threshold = failure_threshold
        self.timeout = reset_timeout
        self.failures = 0
        self.last_failure = None
        self.state = State.CLOSED
    
    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self.state == State.OPEN:
                if time.time() - self.last_failure > self.timeout:
                    self.state = State.HALF_OPEN
                else:
                    raise RuntimeError(f"Circuit OPEN for {func.__name__}")
            try:
                result = func(*args, **kwargs)
                self.failures = 0
                self.state = State.CLOSED
                return result
            except Exception as e:
                self.failures += 1
                self.last_failure = time.time()
                if self.failures >= self.threshold:
                    self.state = State.OPEN
                raise
        wrapper.state = lambda: self.state
        wrapper.reset = lambda: setattr(self, 'state', State.CLOSED)
        return wrapper

@CircuitBreaker(failure_threshold=3, reset_timeout=30)
def call_payment_api(order_id):
    response = requests.post(PAYMENT_URL, json={"order": order_id})
    response.raise_for_status()
    return response.json()
```

**Explanation:**
- CLOSED: normal operation, tracks consecutive failures
- OPEN: after N failures, immediately reject all calls (protect downstream)
- HALF_OPEN: after timeout, allow ONE test call. Success → CLOSED, failure → OPEN again
- Prevents cascading failures when a dependency is down

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Idempotent Decorator with External State

**Scenario:** Write `@idempotent(key_func)` that ensures a function only executes once for a given set of inputs, using DynamoDB as the state store.

<details>
<summary>✅ Solution</summary>

```python
import functools
import hashlib
import json
import boto3
from datetime import datetime

def idempotent(key_func=None, table_name='idempotency_tokens'):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate idempotency key
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                raw = json.dumps({"fn": func.__name__, "args": str(args), "kwargs": str(kwargs)})
                key = hashlib.sha256(raw.encode()).hexdigest()
            
            # Check if already executed
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)
            try:
                table.put_item(
                    Item={'token_id': key, 'executed_at': datetime.now().isoformat(),
                          'ttl': int(datetime.now().timestamp()) + 86400 * 7},
                    ConditionExpression='attribute_not_exists(token_id)'
                )
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"Idempotent skip: {func.__name__} already executed for key={key[:12]}")
                return None  # Already executed
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

@idempotent(key_func=lambda date, table: f"{table}_{date}")
def daily_load(date, table):
    """Won't re-execute for same date+table combination."""
    data = extract(date)
    warehouse.merge(table, data)
```

**Explanation:**
- DynamoDB conditional write: atomic check-and-insert (no race conditions)
- TTL auto-deletes old tokens after 7 days (keeps table bounded)
- If token exists: skip (function already ran successfully for these inputs)
- This is how AWS Lambda PowerTools implements idempotency

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Feature Flag with Fallback

**Scenario:** Write `@feature_flag(flag_name, fallback)` that runs the decorated function only if the flag is enabled; otherwise runs the fallback function.

<details>
<summary>✅ Solution</summary>

```python
import functools

class FeatureFlags:
    _flags = {}
    
    @classmethod
    def set(cls, name, enabled):
        cls._flags[name] = enabled
    
    @classmethod
    def is_enabled(cls, name):
        return cls._flags.get(name, False)

def feature_flag(flag_name, fallback=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if FeatureFlags.is_enabled(flag_name):
                return func(*args, **kwargs)
            elif fallback:
                return fallback(*args, **kwargs)
            else:
                print(f"Feature '{flag_name}' disabled, skipping {func.__name__}")
                return None
        wrapper._flag_name = flag_name
        return wrapper
    return decorator

def old_transform(data):
    return [r * 2 for r in data]

@feature_flag("new_transform_v2", fallback=old_transform)
def transform(data):
    return [r * 3 for r in data]  # New logic

# Control via config/DB:
FeatureFlags.set("new_transform_v2", True)   # New function runs
FeatureFlags.set("new_transform_v2", False)  # Fallback runs instead
```

**Explanation:**
- Enables gradual rollout: deploy new code, enable flag for % of traffic
- Fallback ensures the old behavior continues when flag is off
- Flag state can be read from DB/config file (not just in-memory)
- Used in production for A/B testing data pipeline logic

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Observability Decorator (Metrics + Logging)

**Scenario:** Write `@observe(metric_namespace)` that automatically publishes execution metrics (duration, success/failure count) to CloudWatch and logs structured JSON.

<details>
<summary>✅ Solution</summary>

```python
import functools
import time
import json
import boto3

cloudwatch = boto3.client('cloudwatch')

def observe(namespace='DataPipeline/ETL'):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            metric_name = func.__name__
            
            try:
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                
                # Publish success metrics
                cloudwatch.put_metric_data(Namespace=namespace, MetricData=[
                    {'MetricName': f'{metric_name}.duration', 'Value': elapsed, 'Unit': 'Seconds'},
                    {'MetricName': f'{metric_name}.success', 'Value': 1, 'Unit': 'Count'},
                ])
                
                # Structured log
                print(json.dumps({"event": "step_complete", "step": metric_name,
                                  "duration_s": round(elapsed, 3), "status": "success"}))
                return result
                
            except Exception as e:
                elapsed = time.perf_counter() - start
                cloudwatch.put_metric_data(Namespace=namespace, MetricData=[
                    {'MetricName': f'{metric_name}.duration', 'Value': elapsed, 'Unit': 'Seconds'},
                    {'MetricName': f'{metric_name}.failure', 'Value': 1, 'Unit': 'Count'},
                ])
                print(json.dumps({"event": "step_failed", "step": metric_name,
                                  "duration_s": round(elapsed, 3), "error": str(e)[:200]}))
                raise
        return wrapper
    return decorator

@observe(namespace='DataPipeline/Orders')
def extract_orders(date):
    return db.query(f"SELECT * FROM orders WHERE date = %s", [date])
```

**Explanation:**
- Automatic instrumentation: every decorated function publishes duration + success/fail metrics
- CloudWatch enables dashboards and alarms (alert if failure count > threshold)
- Structured JSON logging enables log-based querying (CloudWatch Insights, Splunk)
- Separates observability from business logic (decorator pattern advantage)

</details>
</article>
