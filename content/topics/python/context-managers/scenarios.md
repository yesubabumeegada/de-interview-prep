---
title: "Python Context Managers - Scenario Questions"
topic: python
subtopic: context-managers
content_type: scenario_question
tags: [python, context-managers, interview, scenarios]
---

# Scenario Questions — Python Context Managers

<article data-difficulty="junior">

## 🟢 Junior: Fix the Resource Leak

**Scenario:** This pipeline function leaks database connections when exceptions occur. Fix it using a context manager.

```python
# BUG: connection leaks if transform() or load() throws an exception
def run_pipeline():
    conn = get_db_connection()
    data = conn.execute("SELECT * FROM source")
    transformed = transform(data)   # May throw!
    load(conn, transformed)         # May throw!
    conn.close()                    # Never reached on exception!
```

<details>
<summary>✅ Solution</summary>

```python
from contextlib import contextmanager

@contextmanager
def db_connection():
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()    # Only on success
    except Exception:
        conn.rollback()  # On any failure
        raise
    finally:
        conn.close()     # ALWAYS (success or failure)

def run_pipeline():
    with db_connection() as conn:
        data = conn.execute("SELECT * FROM source")
        transformed = transform(data)   # If this throws: rollback + close
        load(conn, transformed)         # If this throws: rollback + close
    # If all succeed: commit + close
    # Connection is NEVER leaked regardless of what happens
```

**Why the original was dangerous:** In production, leaked connections accumulate. After 100 failed runs, you have 100 open connections exhausting the database connection pool → new jobs can't connect → cascading failure.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Pipeline Step Tracker with Context Manager

**Scenario:** Build a context manager that tracks pipeline step execution: logs start/end time, handles errors gracefully, and reports metrics. It should work like this:

```python
with pipeline_step("Extract Orders") as step:
    data = extract()
    step.record_metric("row_count", len(data))
# Automatically logs: "[Extract Orders] completed in 12.3s, metrics: {row_count: 50000}"
# On failure: "[Extract Orders] FAILED after 5.1s: ValueError: no data"
```

<details>
<summary>✅ Solution</summary>

```python
import time
from contextlib import contextmanager
from dataclasses import dataclass, field

@dataclass
class StepContext:
    name: str
    start_time: float = field(default_factory=time.perf_counter)
    metrics: dict = field(default_factory=dict)
    
    def record_metric(self, key: str, value):
        self.metrics[key] = value
    
    @property
    def elapsed(self) -> float:
        return time.perf_counter() - self.start_time

@contextmanager
def pipeline_step(name: str):
    """Context manager that tracks pipeline step execution."""
    step = StepContext(name=name)
    print(f"[{name}] Starting...")
    
    try:
        yield step
        # Success
        print(f"[{name}] Completed in {step.elapsed:.1f}s | metrics: {step.metrics}")
    except Exception as e:
        # Failure
        print(f"[{name}] FAILED after {step.elapsed:.1f}s: {type(e).__name__}: {e}")
        # Optionally: send alert, record to monitoring
        raise

# Usage
with pipeline_step("Extract Orders") as step:
    data = extract_from_api()
    step.record_metric("row_count", len(data))
    step.record_metric("source", "orders_api")

with pipeline_step("Transform") as step:
    result = transform(data)
    step.record_metric("input_rows", len(data))
    step.record_metric("output_rows", len(result))

with pipeline_step("Load") as step:
    load_to_warehouse(result)
    step.record_metric("table", "fact_orders")
```

**Output on success:**
```
[Extract Orders] Starting...
[Extract Orders] Completed in 12.3s | metrics: {'row_count': 50000, 'source': 'orders_api'}
[Transform] Starting...
[Transform] Completed in 3.2s | metrics: {'input_rows': 50000, 'output_rows': 48500}
[Load] Starting...
[Load] Completed in 5.1s | metrics: {'table': 'fact_orders'}
```

**Output on failure:**
```
[Extract Orders] Starting...
[Extract Orders] FAILED after 5.1s: ConnectionError: API timeout
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a context manager in Python and what problem does it solve?**
A: A context manager wraps setup and teardown logic around a block of code using the `with` statement. It guarantees that cleanup (closing a file, releasing a lock, committing/rolling back a transaction) runs even if an exception is raised inside the block, eliminating resource leak patterns from try/finally boilerplate.

**Q: What are the two methods required by the context manager protocol?**
A: `__enter__` (called when entering the `with` block; its return value is bound to the `as` variable) and `__exit__` (called when leaving the block; receives exception type, value, and traceback as arguments). Returning `True` from `__exit__` suppresses the exception; returning `None` or `False` propagates it.

**Q: How do you create a simple context manager using `contextlib.contextmanager`?**
A: Decorate a generator function with `@contextlib.contextmanager`. Yield once in the try block (the yielded value becomes the `as` variable). Put cleanup in the finally block. The decorator wraps the generator into a proper `__enter__`/`__exit__` object without writing a class.

**Q: When would you write a class-based context manager vs. using `@contextmanager`?**
A: Use `@contextmanager` for simple, linear setup/teardown that reads naturally as a generator. Write a class when you need multiple methods, stateful teardown that depends on results computed during the `with` block, or when the context manager needs to be picklable or inheritable.

**Q: How are context managers used for database transactions in data engineering?**
A: A transaction context manager begins a transaction on `__enter__` and commits on clean exit or rolls back on exception in `__exit__`. This ensures no partial writes are committed and the connection is always returned to a pool, which is critical for correctness in ETL pipelines.

**Q: What is `contextlib.ExitStack` and when is it useful?**
A: `ExitStack` manages a dynamic number of context managers—you push them on to the stack at runtime. Useful when the number of resources to manage is not known at write time (e.g., opening N files from a list, acquiring locks from a variable set of resources). All pushed managers are exited in LIFO order.

**Q: What does it mean to suppress an exception in `__exit__` and how do you do it?**
A: If `__exit__` returns a truthy value, the exception that triggered the exit is swallowed and execution continues after the `with` block. Example: `contextlib.suppress(FileNotFoundError)` creates a context manager that silently ignores the specified exception type—useful for idempotent cleanup operations.

**Q: How can context managers be composed (nested) and what are the risks?**
A: Nest `with` statements or use comma-separated context managers in one `with` statement (`with A() as a, B() as b:`). If `A.__enter__` succeeds and `B.__enter__` fails, `A.__exit__` is still called. Deep nesting can obscure flow—`ExitStack` is cleaner for many resources.

---

## 💼 Interview Tips

- Write a minimal class-based and a `@contextmanager` version of the same concept (e.g., a timer) to demonstrate both approaches. This shows you choose the right tool for the complexity.
- Database transaction context managers are the most common DE use case—be specific about commit on success, rollback on exception, and connection pool release in `__exit__`.
- Senior interviewers test `__exit__` exception handling: what arguments does it receive? When does returning `True` make sense (e.g., suppressing expected transient errors in retry logic)? When is it dangerous (swallowing real bugs)?
- Mention `contextlib.AsyncContextManager` / `@asynccontextmanager` for async code—critical when using async database drivers (asyncpg, aiomysql) in FastAPI or Airflow async operators.
- Connect context managers to the DE concept of resource governance: database connections, file handles, and network sockets are finite—context managers are the idiomatic Python mechanism for bounded resource usage.
