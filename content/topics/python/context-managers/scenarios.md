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
