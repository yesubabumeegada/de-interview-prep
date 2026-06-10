---
title: "Dataclasses & Pydantic — Senior Deep Dive"
topic: python
subtopic: dataclasses-pydantic
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, pydantic, BaseSettings, discriminated-unions, schema-evolution, FastAPI, data-quality]
---

# Dataclasses & Pydantic — Senior Deep Dive

Senior-level Pydantic usage: BaseSettings for environment-based configuration, discriminated unions for schema evolution, Pydantic v2 performance improvements, custom types, FastAPI integration for DE APIs, and schema-based data quality validation.

---

## Pydantic BaseSettings: Environment-Driven Pipeline Config

`BaseSettings` reads configuration from environment variables and `.env` files, with type validation. This is the production pattern for configuring pipelines without hardcoded values.

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator, SecretStr
from typing import Optional
from functools import lru_cache


class DatabaseSettings(BaseSettings):
    host: str
    port: int = 5432
    database: str
    username: str
    password: SecretStr  # Pydantic SecretStr prevents accidental logging of passwords
    pool_min_size: int = Field(default=2, ge=1)
    pool_max_size: int = Field(default=20, ge=1)

    model_config = SettingsConfigDict(env_prefix="DB_")
    # Reads: DB_HOST, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD, etc.

    @property
    def dsn(self) -> str:
        """Construct the connection string — password shown plaintext for driver use."""
        return (
            f"postgresql://{self.username}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class PipelineSettings(BaseSettings):
    """
    Full pipeline configuration from environment variables.
    Can also read from a .env file for local development.
    """
    model_config = SettingsConfigDict(
        env_file=".env",           # Load from .env in local dev
        env_file_encoding="utf-8",
        env_prefix="PIPELINE_",
        case_sensitive=False,      # PIPELINE_NAME == pipeline_name
    )

    name: str
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    # Nested settings — reads DB_HOST, DB_PASSWORD etc.
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)

    # S3 config
    s3_bucket: str
    s3_prefix: str = "data/"
    aws_region: str = "us-east-1"

    # Pipeline behavior
    batch_size: int = Field(default=1000, ge=1, le=100_000)
    enable_retries: bool = True
    max_retries: int = Field(default=3, ge=0, le=10)
    slack_webhook_url: Optional[SecretStr] = None

    @field_validator("environment")
    @classmethod
    def validate_env(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v.lower()


@lru_cache(maxsize=1)
def get_settings() -> PipelineSettings:
    """
    Load settings once and cache.
    Pattern: call get_settings() throughout the codebase instead of
    reading env vars directly — makes mocking in tests trivial.
    """
    return PipelineSettings()


# Usage:
settings = get_settings()
print(settings.db.host)
print(settings.db.dsn)        # Full connection string with password
print(repr(settings.db.password))  # SecretStr('**********')  — safe to log

# In tests: override with env vars or monkeypatch
# import os; os.environ["PIPELINE_BATCH_SIZE"] = "500"
# get_settings.cache_clear(); settings = get_settings()
```

---

## Discriminated Unions for Schema Evolution

When an event stream carries multiple event types with different schemas, discriminated unions provide type-safe dispatch.

```python
from pydantic import BaseModel, Field
from typing import Literal, Union, Annotated
from datetime import datetime


# Base event
class BaseEvent(BaseModel):
    event_id: str
    user_id: int
    timestamp: datetime
    event_type: str  # Discriminator field


# Specific event types
class PageViewEvent(BaseEvent):
    event_type: Literal["page_view"]
    page_url: str
    referrer: Optional[str] = None
    session_id: str


class PurchaseEvent(BaseEvent):
    event_type: Literal["purchase"]
    order_id: str
    amount: float
    currency: str = "USD"
    items: list[dict]


class RefundEvent(BaseEvent):
    event_type: Literal["refund"]
    original_order_id: str
    refund_amount: float
    reason: Optional[str] = None


# Discriminated union — Pydantic uses event_type to choose the right model
Event = Annotated[
    Union[PageViewEvent, PurchaseEvent, RefundEvent],
    Field(discriminator="event_type")
]


class EventBatch(BaseModel):
    events: list[Event]
    batch_id: str
    source: str


# Parse a mixed batch of events:
raw_batch = {
    "batch_id": "batch_001",
    "source": "kafka",
    "events": [
        {
            "event_id": "e1", "user_id": 42,
            "timestamp": "2024-01-15T10:00:00",
            "event_type": "page_view",
            "page_url": "/products/widget",
            "session_id": "sess_abc"
        },
        {
            "event_id": "e2", "user_id": 42,
            "timestamp": "2024-01-15T10:05:00",
            "event_type": "purchase",
            "order_id": "ord_123",
            "amount": 99.99,
            "items": [{"sku": "W001", "qty": 1}]
        },
        {
            "event_id": "e3", "user_id": 99,
            "timestamp": "2024-01-15T10:10:00",
            "event_type": "refund",
            "original_order_id": "ord_100",
            "refund_amount": 49.99,
        }
    ]
}

batch = EventBatch.model_validate(raw_batch)

# Type-safe dispatch:
for event in batch.events:
    if isinstance(event, PurchaseEvent):
        print(f"Purchase: {event.order_id} — ${event.amount}")
    elif isinstance(event, PageViewEvent):
        print(f"Page view: {event.page_url}")
    elif isinstance(event, RefundEvent):
        print(f"Refund: {event.original_order_id} — ${event.refund_amount}")
```

---

## Custom Types and Type Annotations

```python
from pydantic import BaseModel
from pydantic.functional_validators import AfterValidator
from typing import Annotated
import re


# Custom type: S3 URI
def validate_s3_uri(v: str) -> str:
    if not v.startswith("s3://"):
        raise ValueError(f"Must be an S3 URI starting with s3://, got: {v}")
    parts = v[5:].split("/", 1)
    if not parts[0]:
        raise ValueError("S3 URI must include a bucket name")
    return v


S3Uri = Annotated[str, AfterValidator(validate_s3_uri)]


# Custom type: non-empty string (common validation)
def validate_non_empty(v: str) -> str:
    if not v.strip():
        raise ValueError("String must not be empty or whitespace-only")
    return v.strip()


NonEmptyStr = Annotated[str, AfterValidator(validate_non_empty)]


# Custom type: partition date string (YYYY-MM-DD)
def validate_partition_date(v: str) -> str:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        raise ValueError(f"partition_date must be YYYY-MM-DD, got: {v}")
    return v


PartitionDate = Annotated[str, AfterValidator(validate_partition_date)]


# Use custom types in models:
class ExportJob(BaseModel):
    name: NonEmptyStr
    source_path: S3Uri
    target_path: S3Uri
    partition_date: PartitionDate


job = ExportJob(
    name="  daily export  ",  # Will be stripped
    source_path="s3://raw-bucket/events/",
    target_path="s3://clean-bucket/events/",
    partition_date="2024-01-15",
)
print(job.name)  # "daily export" (stripped)
```

---

## Schema-Based Data Quality Validation

Use Pydantic not just for config, but as a data quality gate in pipelines.

```python
from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class OrderRecord(BaseModel):
    """Schema contract for orders table — used as a data quality gate."""
    order_id: str
    customer_id: int = Field(gt=0)
    product_id: str
    quantity: int = Field(gt=0, le=10_000)
    unit_price: float = Field(gt=0.0, le=100_000.0)
    discount: float = Field(ge=0.0, le=1.0)
    order_date: date
    status: str

    # Computed validation
    @model_validator(mode="after")
    def validate_business_rules(self) -> "OrderRecord":
        # Discount can't make revenue negative
        revenue = self.unit_price * self.quantity * (1 - self.discount)
        if revenue < 0:
            raise ValueError(f"Computed revenue is negative: {revenue}")

        # Status must be a known value
        valid_statuses = {"pending", "processing", "shipped", "delivered", "cancelled", "refunded"}
        if self.status not in valid_statuses:
            raise ValueError(f"Unknown status '{self.status}', expected one of {valid_statuses}")

        return self

    @field_validator("order_date")
    @classmethod
    def order_date_not_future(cls, v: date) -> date:
        if v > date.today():
            raise ValueError(f"order_date {v} is in the future")
        return v


class DataQualityValidator:
    """
    Validate a batch of records against a Pydantic model.
    Returns valid records and a quality report.
    """

    def __init__(self, model_class, source: str):
        self.model_class = model_class
        self.source = source
        self.metrics = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "error_categories": {},
        }

    def validate_batch(self, records: list[dict]) -> tuple[list, list[dict]]:
        """
        Returns (valid_models, error_report).
        error_report: list of dicts with {record, errors}
        """
        valid, errors = [], []
        self.metrics["total"] = len(records)

        for record in records:
            try:
                validated = self.model_class.model_validate(record)
                valid.append(validated)
            except Exception as e:
                error_dict = {
                    "raw_record": record,
                    "validation_errors": str(e),
                }
                errors.append(error_dict)

                # Categorize error types for reporting
                for err in e.errors() if hasattr(e, "errors") else [{"type": "unknown"}]:
                    category = err.get("type", "unknown")
                    self.metrics["error_categories"][category] = \
                        self.metrics["error_categories"].get(category, 0) + 1

        self.metrics["valid"] = len(valid)
        self.metrics["invalid"] = len(errors)
        self.metrics["quality_score"] = (
            len(valid) / len(records) if records else 1.0
        )

        return valid, errors

    def report(self) -> dict:
        return self.metrics


# Usage in a pipeline:
def process_orders_with_dq_gate(raw_records: list[dict]) -> list:
    validator = DataQualityValidator(OrderRecord, source="orders-api")
    valid, errors = validator.validate_batch(raw_records)

    report = validator.report()
    logger.info("DQ Report: %s", report)

    # Write error records to a DLQ (dead letter queue / bad records table)
    if errors:
        write_to_dlq(errors, source="orders-api")

    # Fail pipeline if quality is below threshold
    if report["quality_score"] < 0.95:
        raise ValueError(
            f"Data quality gate failed: {report['quality_score']:.1%} valid "
            f"(threshold: 95%). See DLQ for {len(errors)} invalid records."
        )

    return [r.model_dump() for r in valid]
```

---

## FastAPI Integration for DE APIs

Pydantic is FastAPI's native validation layer. DE teams often build lightweight APIs for serving data, pipeline status, or configuration.

```python
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date
import uvicorn

app = FastAPI(title="Pipeline Management API")


class PipelineRunRequest(BaseModel):
    pipeline_name: str
    partition_date: date
    force_full_refresh: bool = False
    notify_email: Optional[str] = None


class PipelineRunResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: str
    message: str
    estimated_duration_minutes: Optional[int] = None


class PipelineStatusResponse(BaseModel):
    pipeline_name: str
    last_successful_run: Optional[date] = None
    last_run_status: str
    records_processed_today: int = 0


@app.post("/pipelines/run", response_model=PipelineRunResponse)
async def trigger_pipeline_run(request: PipelineRunRequest) -> PipelineRunResponse:
    """
    Trigger a pipeline run.
    Pydantic validates the request body automatically.
    FastAPI returns a 422 Unprocessable Entity if validation fails.
    """
    # Validate the pipeline exists
    known_pipelines = ["daily_orders", "hourly_events", "weekly_reports"]
    if request.pipeline_name not in known_pipelines:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown pipeline: {request.pipeline_name}"
        )

    # Trigger the actual pipeline (Airflow, Prefect, etc.)
    run_id = submit_pipeline_job(
        pipeline=request.pipeline_name,
        date=request.partition_date,
        full_refresh=request.force_full_refresh,
    )

    return PipelineRunResponse(
        run_id=run_id,
        pipeline_name=request.pipeline_name,
        status="submitted",
        message=f"Pipeline {request.pipeline_name} submitted for {request.partition_date}",
        estimated_duration_minutes=30,
    )


@app.get("/pipelines/{pipeline_name}/status", response_model=PipelineStatusResponse)
async def get_pipeline_status(
    pipeline_name: str,
    date: Optional[date] = Query(default=None),
) -> PipelineStatusResponse:
    """Get pipeline status — date parameter is automatically validated as a date."""
    status = get_pipeline_status_from_db(pipeline_name, date)
    return PipelineStatusResponse(**status)
```

---

## Key Takeaways for Senior DEs

1. **`BaseSettings` + `lru_cache`** is the production pattern for pipeline config — reads from env vars, validates types, caches on first access.
2. **`SecretStr`** prevents passwords from appearing in logs, tracebacks, and `repr()` — always use it for credentials.
3. **Discriminated unions** enable type-safe handling of polymorphic event schemas — the `Literal` discriminator tells Pydantic which model to instantiate.
4. **Custom annotated types** (`S3Uri`, `PartitionDate`) are reusable validators that self-document your domain conventions.
5. **Schema-based data quality validation** turns Pydantic from a convenience tool into a data reliability mechanism — a Pydantic DQ gate that rejects > 5% of records and writes to a DLQ is a lightweight alternative to Great Expectations for many use cases.

## ⚡ Cheat Sheet

**BaseSettings Production Pattern**
- `env_prefix="DB_"` → reads `DB_HOST`, `DB_PORT`, etc. automatically
- `SettingsConfigDict(env_file=".env")` → local dev convenience
- `@lru_cache(maxsize=1)` on `get_settings()` → load once, cache forever; `cache_clear()` in tests
- `SecretStr` → `repr()` shows `**********`; use `.get_secret_value()` for actual string
- Nested settings: `db: DatabaseSettings = Field(default_factory=DatabaseSettings)`

**Discriminated Unions**
- `event_type: Literal["purchase"]` on each subclass enables fast dispatch
- `Field(discriminator="event_type")` on the `Union[...]` type → Pydantic picks correct model
- O(1) dispatch vs isinstance chain — scales with number of event types
- Add new event type: create new class with new `Literal`, add to `Union` — zero other changes

**Custom Annotated Types**
- `Annotated[str, AfterValidator(fn)]` → reusable domain types (`S3Uri`, `PartitionDate`)
- Compose: `Annotated[str, AfterValidator(fn1), AfterValidator(fn2)]`
- Self-documents intent: `S3Uri` in a model is clearer than `str` with a comment

**Data Quality Gate**
- `model_validator(mode="after")` for cross-field business rules (revenue > 0, status in allowed set)
- `field_validator` runs before `model_validator` — validate individual fields first
- DQ gate pattern: validate batch → write errors to DLQ → fail pipeline if quality < threshold
- `e.errors()` on `ValidationError` → structured list with field, type, message per error

**FastAPI Integration**
- Pydantic models auto-generate OpenAPI docs — no separate schema file needed
- Invalid request body → FastAPI returns `422 Unprocessable Entity` with error detail
- `Depends()` with Pydantic model for complex query params (vs individual `Query()` args)
- `response_model=` strips extra fields and validates output — catches bugs in return values

**Key Numbers / Rules**
- Pydantic v2 (Rust core) is 5–50× faster than v1 for validation
- `model_dump()` replaces `.dict()` (v2); `model_validate()` replaces `.parse_obj()` (v2)
- Use `model_config = SettingsConfigDict(case_sensitive=False)` for env var matching
