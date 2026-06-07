---
title: "Python Context Managers - Fundamentals"
topic: python
subtopic: context-managers
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, context-managers, with-statement, resource-management, cleanup]
---

# Python Context Managers — Fundamentals

## What Is a Context Manager?

A context manager is a Python pattern that **automatically handles setup and cleanup** of resources — ensuring cleanup happens even if errors occur. You use them with the `with` statement.

**The analogy:** A context manager is like a hotel room. You `check in` (setup), use the room (do your work), and `check out` (cleanup) — even if you're asked to leave early (an exception occurs), checkout still happens.

> **Why context managers matter for DE:** Data pipelines work with files, database connections, S3 clients, and temporary resources. If cleanup doesn't happen (connection not closed, temp file not deleted), you get resource leaks that crash jobs over time.

---

## The Problem Context Managers Solve

```python
# BAD: If process_data() throws an exception, the file is never closed!
f = open('data.csv', 'r')
data = process_data(f)
f.close()  # Never reached if process_data() crashes

# GOOD: Context manager guarantees close() is called, even on exception
with open('data.csv', 'r') as f:
    data = process_data(f)
# f.close() is called automatically here — ALWAYS
```

---

## Built-In Context Managers You Already Use

```python
# File handling
with open('output.parquet', 'wb') as f:
    f.write(data)
# File closed automatically

# Database connections
with psycopg2.connect(DSN) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM orders")
        rows = cursor.fetchall()
# Connection and cursor closed automatically

# Threading locks
import threading
lock = threading.Lock()
with lock:
    shared_resource.update(value)
# Lock released automatically (even on exception)
```

---

## Creating Your Own Context Manager

### Method 1: Class-Based (Using __enter__ and __exit__)

```python
class DatabaseConnection:
    """Context manager for database connections."""
    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None
    
    def __enter__(self):
        """Called when entering 'with' block. Returns the resource."""
        print(f"Connecting to {self.connection_string}")
        self.conn = psycopg2.connect(self.connection_string)
        return self.conn  # This is what 'as conn' receives
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Called when exiting 'with' block. Always runs (even on exception)."""
        if self.conn:
            if exc_type is None:
                self.conn.commit()   # Commit if no exception
                print("Transaction committed")
            else:
                self.conn.rollback()  # Rollback on exception
                print(f"Transaction rolled back due to: {exc_val}")
            self.conn.close()
            print("Connection closed")
        return False  # Don't suppress the exception (re-raise it)

# Usage
with DatabaseConnection("postgresql://localhost/warehouse") as conn:
    conn.execute("INSERT INTO orders ...")
    conn.execute("UPDATE inventory ...")
# If any INSERT/UPDATE fails: rollback + close
# If all succeed: commit + close
```

**`__exit__` parameters:**
- `exc_type`: Exception class (or None if no exception)
- `exc_val`: Exception instance
- `exc_tb`: Traceback
- Return `True` to suppress the exception, `False` to re-raise it

### Method 2: Function-Based (Using @contextmanager — simpler)

```python
from contextlib import contextmanager

@contextmanager
def database_connection(connection_string):
    """Same behavior, less boilerplate."""
    conn = psycopg2.connect(connection_string)
    try:
        yield conn              # Everything before yield = __enter__
        conn.commit()           # Runs if no exception in 'with' block
    except Exception:
        conn.rollback()         # Runs on exception
        raise                   # Re-raise the exception
    finally:
        conn.close()            # ALWAYS runs (cleanup)

# Usage (identical)
with database_connection("postgresql://localhost/warehouse") as conn:
    conn.execute("INSERT INTO orders ...")
```

> **Rule of thumb:** Use `@contextmanager` for simple cases (most DE use cases). Use class-based when you need to store state across multiple uses or implement complex logic.

---

## DE Use Cases for Context Managers

### 1. Temporary File Handling

```python
import tempfile
import os
from contextlib import contextmanager

@contextmanager
def temp_parquet_file():
    """Create a temp file, yield its path, delete on exit."""
    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    tmp_path = tmp.name
    tmp.close()
    try:
        yield tmp_path
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            print(f"Cleaned up temp file: {tmp_path}")

# Usage: download from S3, process, upload, auto-cleanup
with temp_parquet_file() as tmp_path:
    s3.download_file('bucket', 'data.parquet', tmp_path)
    df = pd.read_parquet(tmp_path)
    process(df)
# Temp file deleted automatically — no leaked files on disk
```

### 2. Timing Pipeline Steps

```python
import time
from contextlib import contextmanager

@contextmanager
def timer(step_name: str):
    """Measure execution time of a pipeline step."""
    start = time.perf_counter()
    print(f"[START] {step_name}")
    yield
    elapsed = time.perf_counter() - start
    print(f"[DONE] {step_name}: {elapsed:.2f}s")

# Usage
with timer("Extract orders"):
    orders = extract_from_source()

with timer("Transform"):
    transformed = transform(orders)

with timer("Load to warehouse"):
    load(transformed)

# Output:
# [START] Extract orders
# [DONE] Extract orders: 12.34s
# [START] Transform
# [DONE] Transform: 5.67s
```

### 3. Spark Session Management

```python
@contextmanager
def spark_session(app_name: str, **configs):
    """Create and properly stop a Spark session."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        print(f"Spark session '{app_name}' stopped")

# Usage
with spark_session("DailyETL", **{"spark.sql.adaptive.enabled": "true"}) as spark:
    df = spark.read.parquet("s3://data/orders/")
    result = df.groupBy("region").count()
    result.write.parquet("s3://output/regional_counts/")
# SparkSession always cleaned up (prevents resource leaks in notebooks/tests)
```

---

## Nesting Context Managers

```python
# Multiple resources: nest or use contextlib.ExitStack
from contextlib import ExitStack

@contextmanager
def pipeline_resources(source_db, target_db, s3_bucket):
    """Manage multiple resources with guaranteed cleanup."""
    with ExitStack() as stack:
        source_conn = stack.enter_context(database_connection(source_db))
        target_conn = stack.enter_context(database_connection(target_db))
        s3_client = stack.enter_context(s3_session(s3_bucket))
        yield source_conn, target_conn, s3_client
    # ALL resources closed in reverse order, even if one fails

# Usage
with pipeline_resources("source://...", "target://...", "my-bucket") as (src, tgt, s3):
    data = src.execute("SELECT * FROM orders")
    tgt.execute("INSERT INTO warehouse.orders ...", data)
    s3.upload(summary, "reports/daily.json")
```

---

## Interview Tips

> **Tip 1:** "What is a context manager?" — "A pattern for resource management using the `with` statement. It guarantees cleanup (closing files, releasing connections, deleting temp resources) even when exceptions occur. Defined by implementing `__enter__` and `__exit__`, or using the `@contextmanager` decorator."

> **Tip 2:** "When do you use a context manager in data engineering?" — "Anytime I work with resources that need cleanup: database connections (commit/rollback + close), temporary files (delete after use), Spark sessions (stop on exit), S3 multipart uploads (abort on failure), and locks for concurrent access. The rule is: if it can leak, wrap it in a context manager."

> **Tip 3:** "Write a context manager from scratch" — Use `@contextmanager`: define the function, `yield` the resource (with setup before and cleanup in `finally` after). Remember: code before yield = setup, code after yield = cleanup, `try/finally` ensures cleanup even on exception.
