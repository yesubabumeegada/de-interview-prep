---
title: "Python Decorators - Fundamentals"
topic: python
subtopic: decorators
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, decorators, functions, higher-order-functions, wrapping]
---

# Python Decorators — Fundamentals


## 🎯 Analogy

Think of decorators like gift wrapping: the gift (original function) stays the same inside, but the wrapping adds something (logging, timing, retry logic) on the outside without touching the gift itself.

---
## What Is a Decorator?

A decorator is a **function that wraps another function** to add behavior before or after it runs — without modifying the original function's code.

**The analogy:** Think of a decorator like a gift wrapper. The gift (your function) stays the same, but the wrapper adds something extra (logging, timing, validation) around it.

> **Why decorators matter for DE:** Production pipelines use decorators for retry logic, logging, timing, caching, and access control. Frameworks like Airflow (`@task`), Flask (`@app.route`), and pytest (`@pytest.fixture`) are built on decorators.

---

## Functions Are Objects in Python

Before understanding decorators, you need to know that functions in Python are **first-class objects** — they can be passed around like variables:

```python
def greet(name):
    return f"Hello, {name}"

# Assign a function to a variable
say_hello = greet
print(say_hello("Alice"))  # "Hello, Alice"

# Pass a function as an argument
def apply(func, value):
    return func(value)

result = apply(greet, "Bob")  # "Hello, Bob"

# Return a function from another function
def create_greeter(greeting):
    def greeter(name):
        return f"{greeting}, {name}"
    return greeter

hi = create_greeter("Hi")
print(hi("Charlie"))  # "Hi, Charlie"
```

> **Key insight:** If functions can be passed as arguments and returned from other functions, we can create a function that takes a function and returns an enhanced version of it. That's a decorator.

---

## Your First Decorator

```python
import time

def timer(func):
    """Decorator that measures execution time."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)       # Call the original function
        elapsed = time.time() - start
        print(f"{func.__name__} took {elapsed:.2f}s")
        return result
    return wrapper

# Apply the decorator
@timer
def process_data(records):
    """Simulate data processing."""
    time.sleep(1)
    return [r * 2 for r in records]

# When you call process_data, it actually calls wrapper()
output = process_data([1, 2, 3])
# Prints: "process_data took 1.00s"
# output = [2, 4, 6]
```

**What `@timer` does (step by step):**

1. Python sees `@timer` above `process_data`
2. It executes: `process_data = timer(process_data)`
3. Now `process_data` points to the `wrapper` function
4. When you call `process_data([1,2,3])`, it actually calls `wrapper([1,2,3])`
5. `wrapper` records start time, calls the original function, records end time, prints duration

---

## The @syntax Is Just Syntactic Sugar

These two are identical:

```python
# Using @ syntax (standard way)
@timer
def my_function():
    pass

# Without @ (what Python actually does)
def my_function():
    pass
my_function = timer(my_function)
```

---

## Decorator Template (Use This Pattern)

```python
import functools

def my_decorator(func):
    @functools.wraps(func)  # Preserves original function's name and docstring
    def wrapper(*args, **kwargs):
        # Code to run BEFORE the original function
        print(f"Calling {func.__name__}")
        
        result = func(*args, **kwargs)  # Call the original function
        
        # Code to run AFTER the original function
        print(f"Finished {func.__name__}")
        
        return result  # Don't forget to return the result!
    return wrapper
```

> **Always use `@functools.wraps(func)`** — without it, the wrapped function loses its name, docstring, and other metadata. This causes debugging nightmares.

```python
# Without @functools.wraps:
print(process_data.__name__)  # "wrapper" ← Wrong! Should be "process_data"

# With @functools.wraps:
print(process_data.__name__)  # "process_data" ← Correct!
```

---

## Common Decorator Examples for DE

### 1. Retry on Failure

```python
import functools
import time

def retry(max_attempts=3, delay=1):
    """Retry a function on exception."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise  # Re-raise on final attempt
                    print(f"{func.__name__} failed (attempt {attempt}): {e}. Retrying in {delay}s...")
                    time.sleep(delay * attempt)  # Exponential backoff
        return wrapper
    return decorator

@retry(max_attempts=3, delay=2)
def fetch_from_api(url):
    """Fetch data from an unreliable API."""
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
```

### 2. Logging

```python
import functools
import logging

logger = logging.getLogger(__name__)

def log_execution(func):
    """Log function entry, exit, and any exceptions."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"START: {func.__name__}(args={args}, kwargs={kwargs})")
        try:
            result = func(*args, **kwargs)
            logger.info(f"END: {func.__name__} → success")
            return result
        except Exception as e:
            logger.error(f"FAILED: {func.__name__} → {type(e).__name__}: {e}")
            raise
    return wrapper

@log_execution
def load_to_warehouse(table_name, data):
    """Load data to the warehouse."""
    warehouse.insert(table_name, data)
```

### 3. Timing with Metrics

```python
import functools
import time

def timed(metric_name=None):
    """Record execution time as a metric."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = metric_name or func.__name__
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                metrics.record(f"{name}.duration_seconds", elapsed)
                metrics.record(f"{name}.status", "success")
                return result
            except Exception as e:
                elapsed = time.perf_counter() - start
                metrics.record(f"{name}.duration_seconds", elapsed)
                metrics.record(f"{name}.status", "failure")
                raise
        return wrapper
    return decorator

@timed(metric_name="etl.extract_orders")
def extract_orders(date):
    return db.query("SELECT * FROM orders WHERE date = %s", [date])
```

---

## Decorators With Arguments

When a decorator needs parameters (like `@retry(max_attempts=3)`), you need an extra layer of nesting:

```python
# Decorator WITHOUT arguments: two levels (decorator → wrapper)
def simple_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

# Decorator WITH arguments: three levels (factory → decorator → wrapper)
def decorator_with_args(arg1, arg2):      # Factory: receives decorator args
    def decorator(func):                   # Decorator: receives the function
        @functools.wraps(func)
        def wrapper(*args, **kwargs):      # Wrapper: receives function args
            print(f"Args: {arg1}, {arg2}")
            return func(*args, **kwargs)
        return wrapper
    return decorator

@decorator_with_args("hello", 42)
def my_function():
    pass
```

**How to read it:** `@decorator_with_args("hello", 42)` first calls `decorator_with_args("hello", 42)` which returns a decorator, which then wraps `my_function`.

---

## Stacking Multiple Decorators

Decorators apply bottom-up (closest to the function applies first):

```python
@log_execution          # Applied third (outermost wrapper)
@retry(max_attempts=3)  # Applied second (middle wrapper)
@timed()                # Applied first (innermost, closest to function)
def critical_etl_step(data):
    """A function with multiple decorators."""
    return transform(data)

# Execution order (outside-in at call time):
# 1. log_execution starts (logs "START")
# 2. retry wraps the call (catches exceptions)
# 3. timed measures duration
# 4. critical_etl_step runs
# 5. timed records duration
# 6. retry returns result (or retries on failure)
# 7. log_execution logs "END"
```

---

## Built-in Decorators You Should Know

| Decorator | Purpose | Example |
|-----------|---------|---------|
| `@staticmethod` | Method that doesn't use `self` | Utility functions in a class |
| `@classmethod` | Method that receives the class, not instance | Alternative constructors |
| `@property` | Access a method like an attribute | Computed properties |
| `@functools.lru_cache` | Memoize function results | Expensive repeated computations |
| `@functools.wraps` | Preserve wrapped function's metadata | Always use inside decorators |

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def expensive_lookup(customer_id):
    """Cache repeated lookups to avoid hitting the DB."""
    return db.query("SELECT * FROM customers WHERE id = %s", [customer_id])

# First call: hits DB (slow)
# Second call with same customer_id: returns cached result (instant)
```

---


## ▶️ Try It Yourself

```python
import time
import functools

def timer(func):
    @functools.wraps(func)  # Preserve original function name/docstring
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"{func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

def retry(times=3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == times - 1:
                        raise
                    print(f"Retry {attempt+1}/{times} after error: {e}")
        return wrapper
    return decorator

@timer
@retry(times=3)
def fetch_data():
    return [1, 2, 3]

print(fetch_data())
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "Explain decorators" — "A decorator is a function that takes a function and returns an enhanced version of it. The `@decorator` syntax is sugar for `func = decorator(func)`. I use them for cross-cutting concerns like retry logic, logging, and timing — things you want on many functions without duplicating code."

> **Tip 2:** "Write a retry decorator from scratch" — Know the three-level pattern (factory → decorator → wrapper) for decorators with arguments. Include exponential backoff and re-raise on final attempt.

> **Tip 3:** "Why use `@functools.wraps`?" — "Without it, the decorated function loses its `__name__`, `__doc__`, and other attributes — they get replaced by the wrapper's. This breaks debugging, logging, and introspection tools that rely on `func.__name__`."
