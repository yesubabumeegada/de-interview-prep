---
title: "SQL and Databases - Fundamentals"
topic: python
subtopic: sql-and-databases
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, sql, psycopg2, db-api, parameterized-queries, sqlite, pandas, connection-string]
---

# SQL and Databases — Fundamentals

## 🎯 Analogy

Think of Python's database API like a phone call to a restaurant. You dial (connect), place your order using a specific script so there's no misunderstanding (parameterized query), wait for your food (execute/fetch), and then hang up (close). Speaking off-script — inserting user input directly into the SQL string — is like letting a stranger dictate your entire order over the phone, including telling them to cancel everyone else's tables.

---

## Python DB-API 2.0 (PEP 249)

The Python Database API Specification (PEP 249) defines a standard interface that all Python database drivers must implement. This means code written for `psycopg2` (Postgres) looks nearly identical to code for `sqlite3`, `mysql-connector-python`, or `pyodbc`.

**The core objects:**
- **Connection:** represents a connection to the database
- **Cursor:** executes queries and holds results; one connection can have many cursors
- **Transaction:** all operations on a connection are in an implicit transaction; you must call `commit()` or `rollback()`

```python
import psycopg2

# 1. Connect
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="warehouse",
    user="etl_user",
    password="secret",
)

# 2. Create a cursor
cur = conn.cursor()

# 3. Execute a query
cur.execute("SELECT id, name, amount FROM orders WHERE status = %s", ("completed",))

# 4. Fetch results
rows = cur.fetchall()   # Returns list of tuples: [(1, "Alice", 99.99), ...]
# OR
row = cur.fetchone()    # Returns one tuple (or None)
# OR
for row in cur:         # Iterate directly (memory-efficient for large results)
    print(row)

# Get column names from the cursor description
columns = [desc[0] for desc in cur.description]  # ["id", "name", "amount"]
rows_as_dicts = [dict(zip(columns, row)) for row in rows]

# 5. Commit (required for INSERT/UPDATE/DELETE)
conn.commit()

# 6. Always close
cur.close()
conn.close()
```

---

## Parameterized Queries: Why f-strings are Dangerous

**Never** build SQL by concatenating user input with Python strings. Parameterized queries are not optional — they are the only safe way to include variable data in SQL.

```python
# ❌ DANGEROUS — SQL Injection vulnerability
user_id = "1 OR 1=1 --"  # Attacker-supplied input
cur.execute(f"SELECT * FROM users WHERE id = {user_id}")
# This executes: SELECT * FROM users WHERE id = 1 OR 1=1 --
# Returns EVERY row in the users table!

# ✅ SAFE — Parameterized query
cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
# psycopg2 escapes the value: SELECT * FROM users WHERE id = '1 OR 1=1 --'
# No SQL injection — the input is treated as data, not code

# Different drivers use different placeholder styles:
# psycopg2 (Postgres):      %s
# sqlite3:                  ? or :name
# mysql-connector-python:   %s
# pyodbc:                   ?

# Named parameters with psycopg2 (clearer for many params):
cur.execute(
    "INSERT INTO orders (id, amount, status) VALUES (%(id)s, %(amount)s, %(status)s)",
    {"id": 123, "amount": 99.99, "status": "pending"}
)

# sqlite3 named style:
import sqlite3
conn = sqlite3.connect(":memory:")
cur = conn.cursor()
cur.execute(
    "SELECT * FROM users WHERE email = :email AND active = :active",
    {"email": "user@example.com", "active": True}
)
```

---

## Context Managers for Connections

Always use context managers so connections and transactions are handled correctly even if an exception occurs.

```python
import psycopg2
from contextlib import contextmanager


@contextmanager
def get_db_connection(dsn: str):
    """Context manager that closes the connection on exit."""
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()       # Commit if no exception was raised
    except Exception:
        conn.rollback()     # Rollback on any error
        raise
    finally:
        conn.close()        # Always close, success or failure


# Usage
DSN = "postgresql://etl_user:secret@localhost:5432/warehouse"

with get_db_connection(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO logs (message) VALUES (%s)", ("pipeline started",))
# conn.commit() called automatically on exit
# conn.close() called automatically in finally block

# psycopg2 connection as context manager handles transactions automatically:
conn = psycopg2.connect(DSN)
with conn:          # Commits on __exit__, rolls back on exception
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("processed", 42))
# Note: conn itself is NOT closed by `with conn:` — close manually
conn.close()
```

---

## SQLite for Local Testing

SQLite requires no server — it's a file (or in-memory database) perfect for unit tests and local development.

```python
import sqlite3

# In-memory database: fast, disappears when connection closes
conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row  # Makes rows accessible as dicts or by column name

with conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_id TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT DEFAULT 'pending'
        )
    """)
    conn.execute(
        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
        (1, "cust_001", 99.99)
    )

cur = conn.execute("SELECT * FROM orders WHERE status = ?", ("pending",))
for row in cur:
    print(dict(row))  # {"id": 1, "customer_id": "cust_001", "amount": 99.99, ...}

conn.close()
```

---

## Reading SQL Results into Pandas

```python
import pandas as pd
from sqlalchemy import create_engine
import psycopg2


# Method 1: pd.read_sql with SQLAlchemy engine (preferred)
engine = create_engine("postgresql://user:pass@host:5432/db")

df = pd.read_sql(
    "SELECT order_id, amount, status, created_at FROM orders WHERE status = %(status)s",
    engine,
    params={"status": "completed"},
    parse_dates=["created_at"],     # Convert to datetime automatically
    index_col="order_id",           # Use order_id as the DataFrame index
)

# Method 2: pd.read_sql with a raw psycopg2 connection
conn = psycopg2.connect("postgresql://user:pass@host:5432/db")
df = pd.read_sql("SELECT * FROM orders LIMIT 10000", conn)
conn.close()

# Method 3: Chunked reading for large tables (avoids OOM)
chunks = pd.read_sql(
    "SELECT * FROM fact_events",
    engine,
    chunksize=50_000,   # Returns an iterator of DataFrames
)
for chunk in chunks:
    process(chunk)      # Process 50K rows at a time

# Writing DataFrame back to SQL
df_processed.to_sql(
    "processed_orders",
    engine,
    if_exists="append",  # "replace" drops and recreates the table
    index=False,
    method="multi",      # Batch INSERT, faster than row-by-row
    chunksize=10_000,
)
```

---

## Connection String Formats

```python
# PostgreSQL (psycopg2 / SQLAlchemy)
"postgresql://user:password@host:5432/database"
"postgresql+psycopg2://user:password@host:5432/database"

# MySQL
"mysql+mysqlconnector://user:password@host:3306/database"

# Snowflake
"snowflake://user:password@account.region/database/schema?warehouse=COMPUTE_WH&role=ANALYST"

# BigQuery (uses service account JSON, not user/pass)
"bigquery://project-id/dataset"
# Set GOOGLE_APPLICATION_CREDENTIALS env var to service account JSON path

# Redshift (via psycopg2 — Redshift is Postgres-compatible)
"postgresql://user:password@cluster.account.us-east-1.redshift.amazonaws.com:5439/database"

# SQLite
"sqlite:///path/to/file.db"   # File-based
"sqlite://"                   # In-memory
"sqlite:///:memory:"          # In-memory (explicit)

# Example: build connection string from environment variables (secure)
import os
db_url = (
    f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', '5432')}"
    f"/{os.environ['DB_NAME']}"
)
```

---

## Interview Tips

- **PEP 249** is the spec name — knowing it signals you understand why all Python DB drivers look similar.
- **Always** mention parameterized queries in any discussion about SQL in Python. "I never use f-strings or string concatenation to build SQL" is a correct, confident answer.
- Know the difference between `fetchall()` (all rows in memory), `fetchone()` (one row), and iterating the cursor directly (memory-efficient for large result sets).
- `conn.row_factory = sqlite3.Row` in SQLite is the equivalent of getting dicts from psycopg2's `RealDictCursor` — both return column-accessible rows instead of plain tuples.
- `pd.read_sql` is the standard way to load query results into pandas for analysis pipelines — know the `chunksize` parameter for large tables.
