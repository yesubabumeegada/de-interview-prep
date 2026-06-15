---
title: "Data Pipeline Patterns - Fundamentals"
topic: python
subtopic: data-pipeline-patterns
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, etl, pipeline, retry, logging, configuration, idempotency]
---

# Data Pipeline Patterns — Fundamentals

## 🎯 Analogy

A data pipeline is like an assembly line in a factory. Raw materials (data) come in at one end, get shaped and inspected at several stations (transform), and finished products go out the other end (load). Each station has one job, and if one station breaks you can restart from there — you don't rebuild the entire car.

---

## ETL Class Design: Separating Extract, Transform, Load

The single most important pattern in pipeline code is **separation of concerns**. Each phase of ETL should be independently testable and replaceable.

```python
from abc import ABC, abstractmethod
from typing import Iterator, Any
import logging

logger = logging.getLogger(__name__)


class Extractor(ABC):
    """Abstract base: every extractor must implement extract()."""
    @abstractmethod
    def extract(self) -> Iterator[dict]:
        """Yield records one at a time from the source."""
        ...


class Transformer(ABC):
    """Abstract base: every transformer must implement transform()."""
    @abstractmethod
    def transform(self, record: dict) -> dict | None:
        """Return transformed record, or None to drop the record."""
        ...


class Loader(ABC):
    """Abstract base: every loader must implement load()."""
    @abstractmethod
    def load(self, records: list[dict]) -> int:
        """Load a batch of records. Return count of rows loaded."""
        ...


class ETLPipeline:
    """Orchestrates one Extractor → one Transformer → one Loader."""

    def __init__(self, extractor: Extractor, transformer: Transformer,
                 loader: Loader, batch_size: int = 1000):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.batch_size = batch_size

    def run(self) -> dict:
        batch, total_loaded, total_dropped = [], 0, 0

        for raw in self.extractor.extract():
            transformed = self.transformer.transform(raw)
            if transformed is None:
                total_dropped += 1
                continue
            batch.append(transformed)
            if len(batch) >= self.batch_size:
                total_loaded += self.loader.load(batch)
                batch = []

        if batch:
            total_loaded += self.loader.load(batch)

        return {"loaded": total_loaded, "dropped": total_dropped}
```

**Why this structure?** You can swap `PostgresLoader` for `BigQueryLoader` without touching extraction logic. You can unit-test the transformer with mock data. Each class has one reason to change.

---

## Pipeline State Machine

Production pipelines need explicit state tracking so you can monitor, alert on, and resume runs.

```python
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime


class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    status: PipelineStatus = PipelineStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    rows_processed: int = 0
    error: str | None = None

    def start(self):
        self.status = PipelineStatus.RUNNING
        self.started_at = datetime.utcnow()

    def succeed(self, rows: int):
        self.status = PipelineStatus.SUCCESS
        self.rows_processed = rows
        self.finished_at = datetime.utcnow()

    def fail(self, error: str):
        self.status = PipelineStatus.FAILED
        self.error = error
        self.finished_at = datetime.utcnow()

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None
```

---

## Idempotency: Safe Re-runs with run_id

**Idempotent** means running a pipeline twice produces the same result as running it once. This is critical — pipelines fail and get re-run constantly.

```python
import hashlib
import json
from pathlib import Path


def make_run_id(pipeline_name: str, logical_date: str) -> str:
    """Deterministic run_id from pipeline + date (same inputs → same ID)."""
    key = f"{pipeline_name}:{logical_date}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


class CheckpointStore:
    """File-based checkpoint: tracks which run_ids have completed."""

    def __init__(self, checkpoint_dir: str = "/tmp/pipeline_checkpoints"):
        self.path = Path(checkpoint_dir)
        self.path.mkdir(parents=True, exist_ok=True)

    def is_complete(self, run_id: str) -> bool:
        return (self.path / f"{run_id}.done").exists()

    def mark_complete(self, run_id: str, metadata: dict):
        checkpoint_file = self.path / f"{run_id}.done"
        checkpoint_file.write_text(json.dumps(metadata))

    def get_metadata(self, run_id: str) -> dict:
        checkpoint_file = self.path / f"{run_id}.done"
        if checkpoint_file.exists():
            return json.loads(checkpoint_file.read_text())
        return {}


# Usage in a pipeline
store = CheckpointStore()
run_id = make_run_id("orders_pipeline", "2024-01-15")

if store.is_complete(run_id):
    print(f"Run {run_id} already complete — skipping")
else:
    result = pipeline.run()
    store.mark_complete(run_id, result)
    print(f"Run {run_id} complete: {result}")
```

---

## Retry Logic with Exponential Backoff

External APIs and databases fail transiently. Retry with **exponential backoff** to avoid hammering a recovering service.

```python
import time
import random
from functools import wraps


def retry_with_backoff(max_attempts: int = 3, base_delay: float = 1.0,
                       max_delay: float = 60.0, exceptions: tuple = (Exception,)):
    """Decorator: retry a function with exponential backoff + jitter."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    # Exponential backoff with jitter to avoid retry storms
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    jitter = random.uniform(0, delay * 0.1)
                    wait = delay + jitter
                    logger.warning(f"{func.__name__} attempt {attempt} failed: {e}. "
                                   f"Retrying in {wait:.1f}s")
                    time.sleep(wait)
        return wrapper
    return decorator


# tenacity library (pip install tenacity) — more feature-rich
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def fetch_api_data(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()
```

---

## Structured Logging for Pipelines

Log in JSON format so logs are queryable in Datadog, Splunk, or CloudWatch.

```python
import logging
import json
import sys
from datetime import datetime


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline": getattr(record, "pipeline", None),
            "run_id": getattr(record, "run_id", None),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def get_pipeline_logger(name: str, run_id: str) -> logging.LoggerAdapter:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    # LoggerAdapter injects run_id into every log record automatically
    return logging.LoggerAdapter(logger, {"pipeline": name, "run_id": run_id})


log = get_pipeline_logger("orders_pipeline", run_id="abc123")
log.info("Pipeline started", extra={"rows_expected": 50000})
# Outputs: {"timestamp": "2024-01-15T10:00:00", "level": "INFO",
#           "message": "Pipeline started", "pipeline": "orders_pipeline",
#           "run_id": "abc123", "rows_expected": 50000}
```

---

## Configuration Management

```python
import os
from pydantic_settings import BaseSettings
from pydantic import Field


class PipelineConfig(BaseSettings):
    """Config loaded from environment variables or .env file."""

    # Database
    db_host: str = Field(..., env="DB_HOST")
    db_port: int = Field(5432, env="DB_PORT")
    db_name: str = Field(..., env="DB_NAME")
    db_password: str = Field(..., env="DB_PASSWORD")

    # Pipeline behavior
    batch_size: int = Field(1000, env="BATCH_SIZE")
    max_retries: int = Field(3, env="MAX_RETRIES")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        env_file = ".env"          # Reads from .env in dev
        env_file_encoding = "utf-8"

    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_host}:{self.db_port}/{self.db_name}"


# Usage — raises ValidationError immediately if required env vars are missing
config = PipelineConfig()
print(config.db_url)   # postgresql://prod-db:5432/warehouse
```

**Key principle:** Fail fast at startup with a clear error about missing config rather than failing mid-pipeline after processing 500K records.

---

## Interview Tips

- **Always mention idempotency** when asked about pipeline design. The follow-up is almost always "what if it runs twice?" — have an answer.
- Distinguish `retry` (same record, transient error) from `dead letter queue` (record consistently fails, skip and investigate later).
- Structured JSON logging is a signal of production experience — mention it when discussing observability.
- `tenacity` is the standard Python retry library — know its `stop_after_attempt`, `wait_exponential`, and `retry_if_exception_type` parameters.
