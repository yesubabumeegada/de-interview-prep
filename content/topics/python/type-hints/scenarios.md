---
title: "Python Type Hints - Scenario Questions"
topic: python
subtopic: type-hints
content_type: scenario_question
tags: [python, type-hints, interview, scenarios, mypy]
---

# Scenario Questions — Python Type Hints

<article data-difficulty="junior">

## 🟢 Junior: Add Type Hints to a Pipeline Function

**Scenario:** Add complete type hints to this untyped function:

```python
def extract_and_filter(source_path, min_amount, columns):
    data = read_parquet(source_path)
    filtered = [row for row in data if row.get('amount', 0) > min_amount]
    result = [{col: row[col] for col in columns if col in row} for row in filtered]
    return result
```

<details>
<summary>✅ Solution</summary>

```python
from typing import Optional

def extract_and_filter(
    source_path: str,
    min_amount: float,
    columns: list[str]
) -> list[dict[str, object]]:
    """Extract data from parquet, filter by minimum amount, select columns."""
    data: list[dict[str, object]] = read_parquet(source_path)
    filtered: list[dict[str, object]] = [
        row for row in data if float(row.get('amount', 0)) > min_amount
    ]
    result: list[dict[str, object]] = [
        {col: row[col] for col in columns if col in row} 
        for row in filtered
    ]
    return result
```

**Key decisions:**
- `source_path: str` — clearly a file path
- `min_amount: float` — threshold for filtering (could be int too, float is more general)
- `columns: list[str]` — list of column names to select
- Return `list[dict[str, object]]` — list of records, values can be any type
- Using `object` instead of `Any` is more restrictive (can't call methods without checks)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Typed Plugin Interface

**Scenario:** Design a type-safe plugin system for data sources. Create a Protocol (structural typing) that any data source must implement. Then write a function that accepts any conforming source.

<details>
<summary>✅ Solution</summary>

```python
from typing import Protocol, Iterator, runtime_checkable

@runtime_checkable
class DataSource(Protocol):
    """Any class with these methods is a valid DataSource (duck typing + type safety)."""
    
    def connect(self) -> None: ...
    def read(self, query: str) -> Iterator[dict[str, object]]: ...
    def close(self) -> None: ...
    
    @property
    def is_connected(self) -> bool: ...

# Implementations don't need to inherit — just implement the methods
class PostgresSource:
    def __init__(self, connection_string: str):
        self._conn_str = connection_string
        self._connected = False
    
    def connect(self) -> None:
        self._connected = True
    
    def read(self, query: str) -> Iterator[dict[str, object]]:
        # Actual DB query here
        yield {"id": 1, "name": "test"}
    
    def close(self) -> None:
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected

# This function accepts ANY class that matches the Protocol
def extract_data(source: DataSource, query: str) -> list[dict[str, object]]:
    """Works with any DataSource implementation — type-checked by mypy."""
    source.connect()
    try:
        return list(source.read(query))
    finally:
        source.close()

# PostgresSource matches the Protocol (no inheritance needed!)
pg = PostgresSource("postgresql://localhost/db")
data = extract_data(pg, "SELECT * FROM orders")  # mypy: OK ✓

# Runtime check also works:
assert isinstance(pg, DataSource)  # True (because @runtime_checkable)
```

**Why Protocol over ABC:**
- No inheritance required (structural/duck typing)
- Third-party classes can conform without modification
- mypy validates at analysis time (not runtime)
- More Pythonic (matches how Python actually works)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Type a Complex Pipeline Framework

**Scenario:** Design a fully-typed, generic pipeline framework where:
- Steps are typed with their input/output types
- The framework validates that step outputs match next step's inputs
- Supports both sync and async steps

<details>
<summary>✅ Solution</summary>

```python
from typing import TypeVar, Generic, Callable, Awaitable
from dataclasses import dataclass

# Generic type variables for input and output
T_In = TypeVar('T_In')
T_Out = TypeVar('T_Out')
T_Mid = TypeVar('T_Mid')

@dataclass
class StepResult(Generic[T_Out]):
    """Typed result from a pipeline step."""
    data: T_Out
    metadata: dict[str, object]

class PipelineStep(Generic[T_In, T_Out]):
    """A typed pipeline step: takes T_In, produces T_Out."""
    
    def __init__(self, name: str, fn: Callable[[T_In], T_Out]):
        self.name = name
        self._fn = fn
    
    def execute(self, input_data: T_In) -> StepResult[T_Out]:
        result = self._fn(input_data)
        return StepResult(data=result, metadata={'step': self.name})

class Pipeline(Generic[T_In, T_Out]):
    """A typed pipeline: T_In → ... → T_Out."""
    
    def __init__(self, steps: list[PipelineStep]):
        self._steps = steps
    
    def run(self, input_data: T_In) -> StepResult[T_Out]:
        current = input_data
        for step in self._steps:
            result = step.execute(current)
            current = result.data
        return StepResult(data=current, metadata={'steps': len(self._steps)})
    
    def then(self, step: PipelineStep[T_Out, T_Mid]) -> 'Pipeline[T_In, T_Mid]':
        """Chain a new step — mypy validates types match!"""
        return Pipeline(self._steps + [step])

# Usage with type safety:
# Step 1: str → list[dict]
extract_step: PipelineStep[str, list[dict]] = PipelineStep(
    "extract", lambda path: [{"id": 1, "amount": 100.0}]
)

# Step 2: list[dict] → list[dict] (filter)
filter_step: PipelineStep[list[dict], list[dict]] = PipelineStep(
    "filter", lambda records: [r for r in records if r['amount'] > 50]
)

# Step 3: list[dict] → int (count)
count_step: PipelineStep[list[dict], int] = PipelineStep(
    "count", lambda records: len(records)
)

# Type-safe pipeline composition:
pipeline: Pipeline[str, int] = Pipeline([extract_step, filter_step, count_step])
result: StepResult[int] = pipeline.run("s3://data/orders/")
print(result.data)  # int — mypy knows this!

# Type ERROR caught by mypy:
# bad_pipeline = Pipeline([extract_step, count_step, filter_step])
# mypy error: count_step outputs int, but filter_step expects list[dict]!
```

**This demonstrates:**
- `Generic[T_In, T_Out]` for type-parameterized classes
- `TypeVar` for reusable type variables
- Type-safe method chaining (`.then()` validates types align)
- mypy catches mismatched step connections at analysis time

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are type hints in Python and do they affect runtime behavior?**
A: Type hints are annotations (e.g., `def func(x: int) -> str:`) that document the expected types of variables, parameters, and return values. They are completely ignored at runtime by the Python interpreter—they exist solely for static analysis tools (mypy, pyright) and IDE tooling. Adding incorrect type hints does not raise a runtime error.

**Q: What is the difference between `Optional[X]` and `X | None` (Python 3.10+)?**
A: Both express that a value can be `X` or `None`. `Optional[X]` is the pre-3.10 spelling from the `typing` module. `X | None` is the modern union syntax introduced in Python 3.10 (PEP 604). They are semantically identical; prefer `X | None` in new code targeting Python 3.10+.

**Q: What is `Union` and when should you use `TypeVar` instead?**
A: `Union[A, B]` means the value is either type A or type B—the function may behave differently for each. `TypeVar` defines a generic type variable: `T = TypeVar('T')` means the function is generic (works for any type, and if the input is A the output is also A). Use `TypeVar` when the relationship between input and output types must be preserved.

**Q: What are `TypedDict` and `dataclass` and how do they differ for typed records?**
A: Both describe structured data with named, typed fields. `TypedDict` is a dict subtype—the runtime object is still a plain dict, so it is fully serializable but has no methods or validation. `@dataclass` creates a real class with `__init__` and methods, supports default values, and can be frozen (immutable). Use `TypedDict` for JSON-like interchange; `dataclass` for domain objects.

**Q: What is `Protocol` in Python typing and why is it useful for duck typing?**
A: `Protocol` (PEP 544) defines a structural interface—any class that implements the required methods is considered compatible, without explicit inheritance. This formalizes Python's duck typing: `class Readable(Protocol): def read(self) -> str: ...` matches any class with a `read` method, enabling static type checking without forcing inheritance.

**Q: What is `mypy` and how do you integrate it into a DE project?**
A: `mypy` is the standard static type checker for Python. Run it with `mypy src/mypackage` to check all annotated code. Integrate into CI by adding `mypy` to the test step. Configure with `mypy.ini` or `[tool.mypy]` in `pyproject.toml`—set `strict=true` for new projects, use per-module overrides for legacy code with `ignore_missing_imports=true`.

**Q: What is `Annotated` and how does it extend type hints?**
A: `Annotated[T, metadata]` attaches arbitrary metadata to a type without affecting the type itself. Pydantic uses it for field validation: `Annotated[int, Field(gt=0)]` means a positive integer. FastAPI uses it for dependency injection: `Annotated[Session, Depends(get_db)]`. It enables framework-specific semantics without inventing new type constructs.

**Q: What is `Final` and `ClassVar` in Python typing?**
A: `Final[T]` marks a variable as a constant—mypy raises an error if it is reassigned after initialization. `ClassVar[T]` marks a class attribute that should not appear on instances—mypy warns if you set it via `self`. Both are documentation and enforcement tools for design intent.

---

## 💼 Interview Tips

- Lead with the runtime no-op point: type hints are not enforced at runtime. Show you know this to clarify that type safety comes from static analysis in CI (mypy/pyright), not from the Python interpreter.
- Demonstrate `Protocol` for DE interfaces: a `DataReader` protocol with `read() -> pd.DataFrame` matches S3Reader, DBReader, and LocalReader without inheritance—this is the modern Pythonic way to type duck-typed architectures.
- Senior interviewers appreciate `TypeVar` usage for generic utility functions (e.g., a generic `retry(func: Callable[..., T]) -> T` decorator that preserves the wrapped function's return type)—it shows advanced type system fluency.
- Mention `py.typed` marker file (PEP 561)—a shared DE package that ships type hints needs this file in the package root so mypy can use the annotations. Without it, mypy ignores the package's type information.
- Connect type hints to DE quality gates: mypy in CI catches type-related bugs before code reaches production, acting as a lightweight form of contract testing for function signatures across pipeline components.
