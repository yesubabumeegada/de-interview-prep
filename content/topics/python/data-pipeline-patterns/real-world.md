---
title: "Data Pipeline Patterns - Real World"
topic: python
subtopic: data-pipeline-patterns
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, pipeline, production, incident, retry-storm, oom, checkpoint, config-drift]
---

# Data Pipeline Patterns — Real World

## Production Incident: Retry Storm After Outage

**Situation:** A database outage lasted 15 minutes. When it recovered, 200 pipeline jobs simultaneously retried — each retry hit the database within the same 2-second window, causing a thundering herd that immediately took the database down again.

**Root cause:** All jobs used `time.sleep(5)` between retries with no jitter. After the outage, every job's 5-second timer expired at approximately the same time.

**The fix — exponential backoff with jitter:**

```python
import time
import random
import logging

logger = logging.getLogger(__name__)


def retry_with_jitter(func, max_attempts=5, base_delay=2.0, max_delay=120.0):
    """
    Exponential backoff with FULL JITTER to spread retries across time.
    Full jitter: sleep random(0, cap) — not the deterministic exp value.
    This is AWS's recommended pattern for avoiding thundering herd.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts:
                raise

            # Exponential cap (doubles each attempt)
            cap = min(base_delay * (2 ** (attempt - 1)), max_delay)
            # Full jitter: sleep ANYWHERE between 0 and the cap
            sleep_time = random.uniform(0, cap)

            logger.warning(
                f"Attempt {attempt}/{max_attempts} failed: {e}. "
                f"Retrying in {sleep_time:.1f}s (cap={cap:.1f}s)"
            )
            time.sleep(sleep_time)


# With tenacity (preferred in production):
from tenacity import retry, stop_after_attempt, wait_random_exponential

@retry(
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=60),  # Full jitter built-in
    reraise=True,
)
def load_batch_to_db(records: list[dict]) -> int:
    return db.bulk_insert("fact_orders", records)
```

**Result:** After adding full jitter, the retry window spread over 60 seconds instead of 2 seconds. Database load during recovery dropped from 200 concurrent connections to ~3 per second, and the database recovered cleanly.

**Key lesson:** Jitter is not optional — it is what makes distributed retries safe. `wait_exponential` without jitter is nearly as bad as no backoff.

---

## Production Incident: Memory OOM from Loading 10GB CSV into Pandas

**Situation:** A pipeline used `pd.read_csv("orders_2024.csv")` to load a monthly orders file. In January the file grew past 10 GB. The pipeline's container (16 GB RAM) ran OOM and was killed by the kernel mid-run, leaving partial data in the destination table.

**Diagnosis:**

```python
# The original code — loads EVERYTHING into memory at once
import pandas as pd

df = pd.read_csv("orders_2024.csv")         # 10 GB CSV → ~25 GB in-memory DataFrame
df_filtered = df[df["status"] == "completed"]
df_filtered.to_sql("fact_orders", engine, if_exists="append")
# Process killed: OOM after ~15 minutes
```

**The fix — chunked processing:**

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(os.environ["DB_URL"])
CHUNK_SIZE = 100_000  # ~200 MB per chunk for a wide CSV


def process_orders_chunked(filepath: str, destination_table: str) -> int:
    """
    Process a large CSV in chunks.
    Memory usage: O(chunk_size) not O(file_size).
    """
    total_loaded = 0
    dtype_map = {
        "order_id": str,
        "amount": float,
        "status": str,
        "customer_id": str,
    }

    # pd.read_csv with chunksize returns a TextFileReader (iterator), not a DataFrame
    reader = pd.read_csv(
        filepath,
        chunksize=CHUNK_SIZE,
        dtype=dtype_map,        # Specify dtypes: avoids pandas guessing wrong type
        parse_dates=["created_at"],
        na_values=["", "NULL", "N/A"],
    )

    for i, chunk in enumerate(reader):
        # Filter and transform within the chunk
        filtered = chunk[chunk["status"] == "completed"].copy()
        filtered["loaded_at"] = pd.Timestamp.utcnow()

        # Load chunk to DB
        filtered.to_sql(
            destination_table,
            engine,
            if_exists="append",
            index=False,
            method="multi",         # Batch INSERT (faster than row-by-row)
            chunksize=5_000,        # Inner batching for to_sql
        )
        total_loaded += len(filtered)

        if (i + 1) % 10 == 0:
            logger.info(f"Processed {(i+1) * CHUNK_SIZE:,} rows, loaded {total_loaded:,}")

    return total_loaded


# Alternative: if pandas is not needed, use CSV + psycopg2 COPY for maximum throughput
def load_csv_with_copy(filepath: str, table: str, conn):
    """Postgres COPY is 10-100x faster than INSERT for bulk loads."""
    with open(filepath) as f:
        next(f)  # Skip header
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER false)",
                f
            )
    conn.commit()
```

**Result:** Memory usage dropped from 25 GB (OOM) to under 500 MB. Processing time actually improved because smaller working sets fit in CPU cache.

---

## Production Pattern: Pipeline Checkpoint Recovery After Mid-Run Failure

When a pipeline fails after processing 3M of 10M records, you want to resume from record 3M, not restart from scratch.

```python
import json
from pathlib import Path
from typing import Iterator


class ResumablePipeline:
    """
    Tracks the last successfully processed batch offset.
    On re-run, skips to the checkpoint and continues.
    """

    def __init__(self, pipeline_name: str, run_id: str, checkpoint_dir: str = "/tmp/checkpoints"):
        self.name = pipeline_name
        self.run_id = run_id
        self.checkpoint_path = Path(checkpoint_dir) / f"{pipeline_name}_{run_id}.json"
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load_checkpoint()

    def _load_checkpoint(self) -> dict:
        if self.checkpoint_path.exists():
            state = json.loads(self.checkpoint_path.read_text())
            logger.info(f"Resuming from checkpoint: offset={state['last_offset']}, "
                        f"rows_loaded={state['rows_loaded']}")
            return state
        return {"last_offset": 0, "rows_loaded": 0}

    def _save_checkpoint(self, offset: int, rows_loaded: int):
        self._state = {"last_offset": offset, "rows_loaded": rows_loaded}
        self.checkpoint_path.write_text(json.dumps(self._state))

    def run(self, source: Iterator[dict], batch_size: int = 10_000):
        start_offset = self._state["last_offset"]
        total_loaded = self._state["rows_loaded"]
        batch = []
        current_offset = 0

        for record in source:
            current_offset += 1

            # Skip already-processed records
            if current_offset <= start_offset:
                continue

            batch.append(record)

            if len(batch) >= batch_size:
                load_to_db(batch)
                total_loaded += len(batch)
                self._save_checkpoint(current_offset, total_loaded)
                logger.info(f"Checkpoint saved at offset {current_offset:,}")
                batch = []

        if batch:
            load_to_db(batch)
            total_loaded += len(batch)
            self._save_checkpoint(current_offset, total_loaded)

        # Clean up checkpoint on success
        self.checkpoint_path.unlink(missing_ok=True)
        return total_loaded
```

---

## Production Pattern: Config Drift Between Environments

**Situation:** A pipeline worked in staging but failed in production with `KeyError: 'WAREHOUSE_HOST'`. Investigation showed staging used a `.env` file (committed to the repo with defaults), while production used environment variables — but an ops team member had renamed `WAREHOUSE_HOST` to `DW_HOST` in production 3 weeks earlier without updating the pipeline code.

**The fix — Pydantic validation at startup:**

```python
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class PipelineSettings(BaseSettings):
    """
    All configuration declared here.
    Pydantic raises a clear ValidationError at import time if any required
    variable is missing or has the wrong type — not 3 hours into a production run.
    """

    # Required: will raise ValidationError if absent
    warehouse_host: str = Field(..., env="DW_HOST")          # Matches production name
    warehouse_db: str = Field(..., env="DW_DATABASE")
    warehouse_user: str = Field(..., env="DW_USER")
    warehouse_password: str = Field(..., env="DW_PASSWORD")
    warehouse_schema: str = Field(..., env="DW_SCHEMA")

    # Optional with defaults
    batch_size: int = Field(10_000, env="BATCH_SIZE")
    max_workers: int = Field(4, env="MAX_WORKERS")
    enable_dry_run: bool = Field(False, env="DRY_RUN")

    @validator("max_workers")
    def workers_must_be_positive(cls, v):
        if v < 1:
            raise ValueError("max_workers must be >= 1")
        return v

    @validator("warehouse_host")
    def host_must_not_be_localhost_in_prod(cls, v):
        import os
        if os.getenv("ENV") == "production" and v in ("localhost", "127.0.0.1"):
            raise ValueError("Cannot use localhost as warehouse host in production")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False   # DW_HOST and dw_host both work


# This line runs at module import — fails fast before any data is processed
settings = PipelineSettings()
```

**Best practices from this incident:**

1. **Never use `.env` files in production.** They hide config in the filesystem instead of making it explicit in the deployment system (Kubernetes secrets, AWS Parameter Store, etc.).
2. **Use one config class per pipeline** — not scattered `os.getenv()` calls. A single class is the single source of truth and is testable.
3. **Validate config at startup, not when the value is first used** — fail in 1 second, not after 3 hours of processing.
4. **Document every environment variable** in the class with a `Field` comment — the class doubles as your runbook.

---

## Best Practices Checklist for Production Pipelines

| Concern | Pattern |
|---|---|
| Transient failures | Retry with exponential backoff + full jitter |
| Large files | Chunked reading (CSV chunks, DB server-side cursors) |
| Mid-run failures | Checkpoint recovery with offset tracking |
| Config errors | Pydantic Settings validation at startup |
| Re-runs | Idempotent upserts with dedup keys |
| External service failure | Circuit breaker |
| Memory pressure | Bounded queues, generator pipelines |
| Observability | Structured JSON logging with run_id, stage, row counts |
| Failed records | Dead letter queue for later replay |

---

## Interview Tips

- Frame production incidents as **problem → root cause → fix → result** — this is the STAR format and keeps the answer focused.
- The retry storm story is a crowd favorite — interviewers immediately understand the problem and the fix demonstrates deep understanding of distributed systems.
- The OOM story has a clear numeric impact (25 GB → 500 MB memory, chunked processing) — quantify it.
- Config drift is extremely common in real companies — mentioning Pydantic Settings validation at startup signals that you've been burned by missing env vars in production.
