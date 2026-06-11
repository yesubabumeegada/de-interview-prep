---
title: "Python Type Hints - Fundamentals"
topic: python
subtopic: type-hints
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, type-hints, typing, mypy, annotations, static-analysis]
---

# Python Type Hints — Fundamentals


## 🎯 Analogy

Think of type hints like street signs on a highway: they don't physically stop you from going the wrong way, but they tell you (and your IDE/linter) exactly what's expected, catching mistakes before runtime.

---
## What Are Type Hints?

Type hints are **optional annotations** that declare what types a function expects and returns. Python doesn't enforce them at runtime — they're used by tools (mypy, IDEs) for static analysis and documentation.

```python
# Without type hints (unclear what goes in/out)
def process_orders(data, threshold):
    return [o for o in data if o['amount'] > threshold]

# With type hints (self-documenting, IDE-assisted)
def process_orders(data: list[dict], threshold: float) -> list[dict]:
    return [o for o in data if o['amount'] > threshold]
```

> **Why type hints matter for DE:** Production pipelines process millions of records. A type mismatch (passing a string where an int is expected) can silently corrupt data. Type hints catch these bugs before production via static analysis (mypy) and give IDEs the ability to auto-complete and warn.

---

## Basic Type Annotations

```python
# Variable annotations
name: str = "Alice"
age: int = 30
salary: float = 95000.50
is_active: bool = True

# Function parameters and return type
def calculate_tax(income: float, rate: float) -> float:
    return income * rate

# None return (function doesn't return a value)
def log_event(message: str) -> None:
    print(f"[LOG] {message}")
```

---

## Common Types for DE

```python
from typing import Optional, Any

# Optional: value can be None
def get_customer(customer_id: str) -> Optional[dict]:
    """Returns customer dict or None if not found."""
    result = db.query(f"SELECT * FROM customers WHERE id = %s", [customer_id])
    return result[0] if result else None

# Collections (Python 3.9+ — use built-in types directly)
def process_batch(records: list[dict]) -> list[dict]:
    return [transform(r) for r in records]

def get_config() -> dict[str, Any]:
    return {"batch_size": 1000, "source": "s3://data/", "retries": 3}

# Tuple (fixed-length, typed)
def get_stats(data: list[float]) -> tuple[float, float, int]:
    """Returns (mean, std_dev, count)."""
    return (sum(data)/len(data), std(data), len(data))

# Set
def get_unique_ids(records: list[dict]) -> set[str]:
    return {r['id'] for r in records}
```

---

## Union Types (Multiple Possible Types)

```python
# Python 3.10+: use | (pipe) syntax
def parse_value(raw: str | int | float) -> float:
    return float(raw)

# Python 3.9 and below: use Union
from typing import Union
def parse_value(raw: Union[str, int, float]) -> float:
    return float(raw)

# Optional is shorthand for Union[X, None]
# These are identical:
from typing import Optional
name: Optional[str] = None
name: str | None = None  # Python 3.10+
```

---

## Type Hints for Common DE Patterns

### Pipeline Configuration

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class PipelineConfig:
    source_path: str
    target_path: str
    batch_size: int = 10000
    max_retries: int = 3
    filter_column: Optional[str] = None
    
def run_pipeline(config: PipelineConfig) -> dict[str, int]:
    """Returns metrics: {'rows_read': N, 'rows_written': M}."""
    data = read_source(config.source_path)
    if config.filter_column:
        data = filter_data(data, config.filter_column)
    write_target(data, config.target_path)
    return {'rows_read': len(data), 'rows_written': len(data)}
```

### Callable Types (Functions as Arguments)

```python
from typing import Callable

def apply_transform(
    records: list[dict], 
    transform_fn: Callable[[dict], dict]  # Takes a dict, returns a dict
) -> list[dict]:
    return [transform_fn(r) for r in records]

# Usage
def uppercase_name(record: dict) -> dict:
    record['name'] = record['name'].upper()
    return record

result = apply_transform(records, uppercase_name)
```

### Generator Types

```python
from typing import Generator, Iterator

def read_batches(path: str, batch_size: int) -> Generator[list[dict], None, None]:
    """Yields batches of records."""
    # Generator[YieldType, SendType, ReturnType]
    batch = []
    for record in read_file(path):
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# Simpler: just use Iterator for read-only generators
def read_lines(path: str) -> Iterator[str]:
    with open(path) as f:
        for line in f:
            yield line.strip()
```

---

## Running mypy (Static Type Checker)

```bash
# Install
pip install mypy

# Check a single file
mypy pipeline.py

# Check entire project
mypy src/

# Common mypy output:
# pipeline.py:15: error: Argument 1 to "process" has incompatible type "str"; expected "int"
# pipeline.py:22: error: Returning "None" from function with return type "dict[str, int]"
```

**mypy configuration (pyproject.toml):**
```toml
[tool.mypy]
python_version = "3.11"
strict = false                    # Start lenient, tighten over time
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true      # All functions must have type hints
ignore_missing_imports = true     # Don't fail on untyped third-party libraries

[[tool.mypy.overrides]]
module = "tests.*"
disallow_untyped_defs = false     # Tests don't need strict typing
```

---

## Benefits of Type Hints

| Benefit | How It Helps |
|---------|-------------|
| **Catches bugs early** | mypy finds type mismatches before runtime |
| **Self-documenting** | Function signature tells you what goes in/out |
| **IDE autocomplete** | VS Code/PyCharm know the types → better suggestions |
| **Refactoring safety** | Change a type → mypy shows everywhere that breaks |
| **Team communication** | New team members understand interfaces immediately |

---

## When to Add Type Hints

| Scenario | Priority |
|----------|----------|
| Public function/class interfaces | Always |
| Pipeline entry points (extract, transform, load) | Always |
| Shared libraries used by multiple teams | Always |
| Internal helper functions | Good practice |
| One-off scripts | Nice to have |
| Tests | Optional (test frameworks handle it) |

---


## ▶️ Try It Yourself

```python
from typing import Optional, Union
from dataclasses import dataclass

def process_orders(
    orders: list[dict],
    min_amount: float = 0.0,
    region: Optional[str] = None,
) -> list[dict]:
    result = [o for o in orders if o["amount"] >= min_amount]
    if region:
        result = [o for o in result if o.get("region") == region]
    return result

@dataclass
class Order:
    id: int
    amount: float
    region: str
    status: str = "pending"

orders = [Order(1, 300.0, "US"), Order(2, 50.0, "EU")]
us_orders = [o for o in orders if o.region == "US"]
print(us_orders)  # IDE knows o.amount is float, not Any
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "Why use type hints in Python?" — "Three reasons: (1) Catch bugs before production — mypy finds type mismatches statically. (2) Self-documenting code — function signatures clearly show expected inputs/outputs. (3) IDE support — autocomplete, jump-to-definition, and inline warnings. They're especially valuable in data pipelines where a wrong type can silently corrupt millions of records."

> **Tip 2:** "Does Python enforce type hints at runtime?" — "No. Type hints are purely informational at runtime (Python ignores them). They're enforced by external tools: mypy for static analysis, IDEs for inline warnings. You CAN enforce them at runtime using libraries like Pydantic or beartype, but that's optional."

> **Tip 3:** "How do you add type hints to an existing codebase?" — "Incrementally. Start with public interfaces (function signatures) in the most critical modules. Run mypy in non-strict mode first. Fix the easy errors. Gradually tighten settings. Don't try to type everything at once — focus on code where type bugs have actually caused production issues."
