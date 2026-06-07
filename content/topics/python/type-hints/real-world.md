---
title: "Python Type Hints - Real-World Production Examples"
topic: python
subtopic: type-hints
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, type-hints, production, pydantic, pipeline]
---

# Python Type Hints — Real-World Production Examples

## Pattern 1: Typed Pipeline Config with Pydantic

Production pipelines need validated configuration. Pydantic catches bad config at startup rather than mid-pipeline.

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal
from datetime import datetime

class SourceConfig(BaseModel):
    type: Literal["s3", "jdbc", "kafka"]
    path: str
    format: Literal["parquet", "csv", "json", "avro"] = "parquet"
    partition_columns: list[str] = []

class TransformConfig(BaseModel):
    deduplicate_keys: list[str] = []
    filter_expression: str | None = None
    rename_columns: dict[str, str] = {}

class SinkConfig(BaseModel):
    type: Literal["s3", "redshift", "snowflake"]
    path: str
    write_mode: Literal["append", "overwrite", "merge"] = "append"
    partition_by: list[str] = []

class PipelineConfig(BaseModel):
    """Fully typed and validated pipeline configuration."""
    pipeline_name: str = Field(min_length=3, max_length=64)
    owner: str
    schedule: str  # cron expression
    source: SourceConfig
    transforms: list[TransformConfig] = []
    sink: SinkConfig
    timeout_minutes: int = Field(default=60, ge=5, le=480)
    alert_emails: list[str] = []
    tags: dict[str, str] = {}

    @field_validator('pipeline_name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.replace('-', '').replace('_', '').isalnum():
            raise ValueError("pipeline_name must be alphanumeric with - or _")
        return v

    @model_validator(mode='after')
    def validate_merge_requires_keys(self) -> 'PipelineConfig':
        if self.sink.write_mode == "merge" and not self.transforms:
            raise ValueError("merge write_mode requires at least one transform with deduplicate_keys")
        return self

# Load from YAML/JSON with full validation
import yaml

def load_pipeline_config(path: str) -> PipelineConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return PipelineConfig(**raw)  # Raises ValidationError with details

# Usage in main entry point
config = load_pipeline_config("pipelines/daily_users.yaml")
print(f"Running: {config.pipeline_name}, source: {config.source.type}")
```

---

## Pattern 2: Typed ETL Function Interfaces

Define clear contracts between ETL stages using Protocol and TypedDict.

```python
from typing import Protocol, TypedDict, Iterator
from datetime import datetime

class ExtractionResult(TypedDict):
    records: list[dict]
    metadata: dict[str, str | int]
    extracted_at: str

class LoadResult(TypedDict):
    rows_loaded: int
    target: str
    load_duration_seconds: float

class Extractor(Protocol):
    """Contract: any class with this method qualifies as an Extractor."""
    def extract(self, since: datetime | None = None) -> Iterator[ExtractionResult]: ...

class Transformer(Protocol):
    def transform(self, batch: list[dict]) -> list[dict]: ...

class Loader(Protocol):
    def load(self, records: list[dict]) -> LoadResult: ...

# Concrete implementations
class S3ParquetExtractor:
    def __init__(self, bucket: str, prefix: str) -> None:
        self.bucket = bucket
        self.prefix = prefix

    def extract(self, since: datetime | None = None) -> Iterator[ExtractionResult]:
        for partition in self._list_partitions(since):
            records = self._read_parquet(partition)
            yield ExtractionResult(
                records=records,
                metadata={"partition": partition, "count": len(records)},
                extracted_at=datetime.utcnow().isoformat()
            )

    def _list_partitions(self, since: datetime | None) -> list[str]:
        ...
    def _read_parquet(self, path: str) -> list[dict]:
        ...

class RedshiftLoader:
    def __init__(self, connection_string: str, table: str) -> None:
        self.conn_str = connection_string
        self.table = table

    def load(self, records: list[dict]) -> LoadResult:
        import time
        start = time.time()
        # COPY from staged S3 file
        rows = self._copy_to_redshift(records)
        return LoadResult(
            rows_loaded=rows,
            target=self.table,
            load_duration_seconds=time.time() - start
        )

    def _copy_to_redshift(self, records: list[dict]) -> int:
        ...

# Pipeline orchestrator uses only the protocols
def run_pipeline(
    extractor: Extractor,
    transformer: Transformer,
    loader: Loader
) -> dict[str, int]:
    total_extracted = 0
    total_loaded = 0

    for batch_result in extractor.extract():
        transformed = transformer.transform(batch_result["records"])
        load_result = loader.load(transformed)
        total_extracted += len(batch_result["records"])
        total_loaded += load_result["rows_loaded"]

    return {"extracted": total_extracted, "loaded": total_loaded}
```

---

## Pattern 3: Mypy in CI Rejecting PRs

A real GitHub Actions workflow that gates PRs on type safety.

```yaml
# .github/workflows/quality-gate.yml
name: Quality Gate
on:
  pull_request:
    paths: ['src/**/*.py', 'tests/**/*.py']

jobs:
  type-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'
      - run: pip install -e ".[dev]"
      - name: Run mypy
        run: |
          mypy src/ \
            --config-file pyproject.toml \
            --no-error-summary \
            --show-error-codes \
            --show-column-numbers
      - name: Check for type: ignore growth
        run: |
          COUNT=$(grep -r "type: ignore" src/ | wc -l)
          BASELINE=42  # tracked in repo
          if [ "$COUNT" -gt "$BASELINE" ]; then
            echo "ERROR: type: ignore count grew from $BASELINE to $COUNT"
            echo "Fix the new type errors instead of suppressing them"
            exit 1
          fi
```

```toml
# pyproject.toml — tiered strictness
[tool.mypy]
python_version = "3.11"
strict = true
plugins = ["pydantic.mypy"]

[[tool.mypy.overrides]]
module = "legacy.*"
strict = false
disallow_untyped_defs = false

[[tool.mypy.overrides]]
module = "generated.*"
ignore_errors = true
```

**Key practices:**
- Track `type: ignore` count — prevent suppression growth
- Use `--show-error-codes` for targeted ignores: `# type: ignore[arg-type]`
- Run on changed paths only (not full repo) for speed
- Cache pip dependencies for faster CI runs

---

## Pattern 4: TypedDict for API Responses

Type external API responses to catch schema drift early.

```python
from typing import TypedDict, NotRequired
from datetime import datetime

class PaginationInfo(TypedDict):
    page: int
    per_page: int
    total: int
    total_pages: int

class UserRecord(TypedDict):
    id: int
    email: str
    first_name: str
    last_name: str
    created_at: str
    is_active: bool
    metadata: NotRequired[dict[str, str]]

class UsersAPIResponse(TypedDict):
    data: list[UserRecord]
    pagination: PaginationInfo
    request_id: str

def fetch_all_users(base_url: str) -> list[UserRecord]:
    """Paginate through API with typed responses."""
    all_users: list[UserRecord] = []
    page = 1

    while True:
        response: UsersAPIResponse = _call_api(f"{base_url}/users?page={page}")
        all_users.extend(response["data"])

        if page >= response["pagination"]["total_pages"]:
            break
        page += 1

    return all_users

def _call_api(url: str) -> UsersAPIResponse:
    import requests
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()  # Runtime: trust but verify with Pydantic if critical
```

---

## Gradual Typing Migration Guide

### Step 1: Assess the Codebase

```bash
# Count untyped functions
grep -r "def " src/ | grep -v "->\ " | wc -l

# Find most-imported modules (type these first — biggest impact)
grep -rh "^from " src/ | sort | uniq -c | sort -rn | head -20
```

### Step 2: Prioritize by Impact

| Priority | What to Type | Why |
|----------|-------------|-----|
| 1 | Public APIs / shared libraries | Most consumers benefit |
| 2 | Pipeline entry points (extract, transform, load) | Catch data type bugs |
| 3 | Configuration classes | Prevent bad config deploys |
| 4 | Utility functions | High reuse = high impact |
| 5 | Internal helpers | Nice to have |

### Step 3: Tooling Setup

```toml
# Start lenient, tighten over sprints
[tool.mypy]
strict = false
disallow_untyped_defs = false
warn_return_any = true
warn_unused_configs = true

# Require types on new modules only
[[tool.mypy.overrides]]
module = "src.new_pipeline.*"
disallow_untyped_defs = true
```

### Step 4: Automate with Pre-commit

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [pydantic, types-requests]
        args: [--config-file=pyproject.toml]
```

### Step 5: Track Progress

```python
# scripts/typing_coverage.py
import subprocess
import json

def get_typing_stats() -> dict[str, float]:
    """Run mypy and calculate coverage metrics."""
    result = subprocess.run(
        ["mypy", "src/", "--txt-report", "/tmp/mypy_report"],
        capture_output=True, text=True
    )
    # Parse report for typed vs untyped function count
    with open("/tmp/mypy_report/linecount.txt") as f:
        lines = f.readlines()
    # Return coverage percentage per module
    ...
```

---

## Interview Tips

> **Tip 1:** "How do you enforce typing in a team?" — "Three layers: (1) CI gate with mypy — PRs can't merge with type errors. (2) Pre-commit hooks for instant local feedback. (3) Track and prevent `type: ignore` count growth. The goal is making typed code the path of least resistance, not a burden."

> **Tip 2:** "What's the value of Protocol over ABC in data pipelines?" — "Protocol enables plugin architectures without coupling. Teams can write an S3Extractor or KafkaExtractor without importing a base class — they just implement the right methods. This is critical for data platforms where different teams own different connectors. It also makes testing trivial — any object with the right methods works as a mock."

> **Tip 3:** "How would you type a dynamic API response?" — "Two options: (1) TypedDict for static analysis — gives IDE support and mypy checking but no runtime enforcement. (2) Pydantic for runtime validation — catches schema drift when the API changes. In production, I use Pydantic at the boundary (where external data enters) and TypedDict internally (cheaper, static-only)."
