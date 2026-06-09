---
title: "Dataclasses & Pydantic — Intermediate"
topic: python
subtopic: dataclasses-pydantic
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, pydantic, validators, nested-models, model-serialization, API-validation]
---

# Dataclasses & Pydantic — Intermediate

Custom validators, model-level validation, nested model composition, serialization options, and handling real-world messiness from external APIs.

---

## Field-Level Validators (@field_validator)

`@field_validator` runs after Pydantic's built-in type coercion. Use it for business logic validation that goes beyond simple type checking.

```python
from pydantic import BaseModel, Field, field_validator
from typing import Optional
import re
from datetime import date


class IngestionConfig(BaseModel):
    source_table: str
    partition_date: str
    parallelism: int = Field(default=4, ge=1, le=128)
    output_format: str = "parquet"
    notify_email: Optional[str] = None

    @field_validator("partition_date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Ensure partition_date is YYYY-MM-DD format."""
        try:
            parsed = date.fromisoformat(v)
            if parsed > date.today():
                raise ValueError(f"partition_date {v} is in the future")
            return v
        except ValueError as e:
            raise ValueError(f"partition_date must be YYYY-MM-DD, got '{v}': {e}")

    @field_validator("source_table")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        """Enforce catalog.schema.table format."""
        parts = v.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"source_table must be in catalog.schema.table format, got '{v}'"
            )
        if not all(re.match(r"^[a-z][a-z0-9_]*$", p) for p in parts):
            raise ValueError(
                f"Table name parts must be lowercase alphanumeric/underscore: '{v}'"
            )
        return v.lower()

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        allowed = {"parquet", "delta", "json", "csv"}
        if v.lower() not in allowed:
            raise ValueError(f"output_format must be one of {allowed}, got '{v}'")
        return v.lower()

    @field_validator("notify_email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError(f"Invalid email address: '{v}'")
        return v.lower()


# Test the validators
try:
    config = IngestionConfig(
        source_table="prod.analytics.orders",
        partition_date="2099-01-01",  # Future date → ValidationError
    )
except Exception as e:
    print(e)
    # partition_date: Value error, partition_date 2099-01-01 is in the future

config = IngestionConfig(
    source_table="prod.analytics.orders",
    partition_date="2024-01-15",
    output_format="DELTA",   # Will be lowercased to "delta"
    notify_email="TEAM@COMPANY.COM",  # Will be lowercased
)
print(config.output_format)   # "delta"
print(config.notify_email)    # "team@company.com"
```

---

## Model-Level Validators (@model_validator)

`@model_validator` runs after all fields are set. Use it for cross-field validation.

```python
from pydantic import BaseModel, model_validator, Field
from typing import Optional
from datetime import date


class DateRangeConfig(BaseModel):
    start_date: date
    end_date: date
    incremental_from: Optional[date] = None
    full_refresh: bool = False

    @model_validator(mode="after")
    def validate_date_range(self) -> "DateRangeConfig":
        """Cross-field validation: end_date must be after start_date."""
        if self.end_date < self.start_date:
            raise ValueError(
                f"end_date ({self.end_date}) must be >= start_date ({self.start_date})"
            )

        # Can't have both incremental_from and full_refresh
        if self.incremental_from and self.full_refresh:
            raise ValueError(
                "Cannot set both incremental_from and full_refresh=True. "
                "Use one or the other."
            )

        # If incremental, incremental_from must be within the date range
        if self.incremental_from:
            if not (self.start_date <= self.incremental_from <= self.end_date):
                raise ValueError(
                    f"incremental_from ({self.incremental_from}) must be between "
                    f"start_date ({self.start_date}) and end_date ({self.end_date})"
                )

        return self


# Valid:
config = DateRangeConfig(
    start_date="2024-01-01",
    end_date="2024-01-31",
    incremental_from="2024-01-15",
)

# Invalid — cross-field violation:
try:
    bad = DateRangeConfig(
        start_date="2024-01-31",
        end_date="2024-01-01",  # Before start_date
    )
except Exception as e:
    print(e)  # ValueError: end_date (2024-01-01) must be >= start_date (2024-01-31)
```

---

## Nested Models and Composition

```python
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum


class DatabaseType(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"


class DatabaseConnection(BaseModel):
    host: str
    port: int = Field(default=5432, ge=1, le=65535)
    database: str
    username: str
    password: str
    db_type: DatabaseType = DatabaseType.POSTGRES
    ssl: bool = True
    pool_size: int = Field(default=10, ge=1, le=100)


class S3Location(BaseModel):
    bucket: str
    prefix: str
    region: str = "us-east-1"

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix.lstrip('/')}"


class PipelineStep(BaseModel):
    name: str
    enabled: bool = True
    timeout_minutes: int = Field(default=60, ge=1, le=1440)
    retry_on_failure: bool = True


class FullPipelineConfig(BaseModel):
    pipeline_name: str
    source_db: DatabaseConnection
    target_location: S3Location
    steps: List[PipelineStep]
    schedule_cron: Optional[str] = None
    owner_email: str
    tags: dict[str, str] = {}

    @property
    def enabled_steps(self) -> List[PipelineStep]:
        return [s for s in self.steps if s.enabled]


# Load from a YAML or JSON config file:
import json

config_json = """
{
    "pipeline_name": "orders_daily_export",
    "source_db": {
        "host": "prod-db.internal",
        "database": "orders_db",
        "username": "pipeline_user",
        "password": "secret123",
        "db_type": "postgres"
    },
    "target_location": {
        "bucket": "data-lake-prod",
        "prefix": "exports/orders/",
        "region": "us-west-2"
    },
    "steps": [
        {"name": "extract",    "timeout_minutes": 30},
        {"name": "transform",  "timeout_minutes": 15},
        {"name": "load",       "timeout_minutes": 20, "enabled": true},
        {"name": "validate",   "timeout_minutes": 5}
    ],
    "owner_email": "de-team@company.com",
    "tags": {"env": "production", "team": "data-engineering"}
}
"""

config = FullPipelineConfig.model_validate(json.loads(config_json))
print(config.target_location.uri)  # "s3://data-lake-prod/exports/orders/"
print(len(config.enabled_steps))   # 4 (all enabled by default)
print(config.source_db.db_type)    # DatabaseType.POSTGRES
```

---

## Optional Fields and Handling Messy External APIs

Real APIs return optional fields, wrong types, and extra keys that you don't care about.

```python
from pydantic import BaseModel, Field, model_validator
from typing import Optional, Any


class FlexibleApiResponse(BaseModel):
    """
    Model for a messy external API that:
    - Uses 'customerId' (camelCase) instead of 'customer_id'
    - Sometimes returns amount as string
    - Has extra fields we don't care about
    - Returns None for missing optional fields
    """
    model_config = {"populate_by_name": True, "extra": "ignore"}
    # extra="ignore": Pydantic ignores any fields not in the model
    # populate_by_name: allows both alias and field name

    customer_id: int = Field(alias="customerId")
    order_id: str = Field(alias="orderId")
    amount: float  # Will coerce string "99.99" to float 99.99
    currency: str = "USD"
    status: Optional[str] = None
    items: Optional[list] = None
    internal_notes: Optional[str] = Field(default=None, exclude=True)
    # exclude=True: this field won't appear in model_dump() output


# Simulate a messy API response:
raw_response = {
    "customerId": "42",        # String! Pydantic coerces to int
    "orderId": "ord_12345",
    "amount": "199.99",        # String! Coerced to float
    "currency": "GBP",
    "status": None,
    "extra_field_1": "ignored",  # Ignored due to extra="ignore"
    "extra_field_2": [1, 2, 3],  # Ignored
}

order = FlexibleApiResponse.model_validate(raw_response)
print(order.customer_id)  # 42 (int, not "42")
print(order.amount)       # 199.99 (float, not "199.99")
print(order.status)       # None
print(order.model_dump())
# {'customer_id': 42, 'order_id': 'ord_12345', 'amount': 199.99, 'currency': 'GBP', 'status': None, 'items': None}
# Note: internal_notes excluded, extra fields ignored
```

### Handling Missing Required Fields

```python
from pydantic import BaseModel, ValidationError
import logging

logger = logging.getLogger(__name__)


def safe_parse_api_record(raw: dict, model_class, source: str = "unknown") -> Optional[BaseModel]:
    """
    Parse an API record, logging validation errors without crashing the pipeline.
    Returns None for invalid records.
    """
    try:
        return model_class.model_validate(raw)
    except ValidationError as e:
        logger.warning(
            "Validation failed for record from %s: %s. Raw: %s",
            source, e.error_count(), str(raw)[:200]
        )
        return None


# In a pipeline: validate each record, skip invalid ones
raw_records = fetch_api_response()
valid_records = [
    parsed for raw in raw_records
    if (parsed := safe_parse_api_record(raw, FlexibleApiResponse, "orders-api"))
]
invalid_count = len(raw_records) - len(valid_records)
print(f"Valid: {len(valid_records)}, Invalid (skipped): {invalid_count}")

# Alert if invalid rate is too high
if invalid_count / len(raw_records) > 0.05:  # More than 5% invalid
    raise ValueError(
        f"Data quality alert: {invalid_count/len(raw_records):.1%} of records failed validation"
    )
```

---

## Model Serialization Options

```python
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class PipelineRun(BaseModel):
    run_id: str
    pipeline_name: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    records_processed: int = 0
    status: str = "running"
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


run = PipelineRun(
    run_id="run_abc123",
    pipeline_name="daily_orders",
    started_at=datetime(2024, 1, 15, 8, 0, 0),
    completed_at=datetime(2024, 1, 15, 8, 45, 30),
    records_processed=1_500_000,
    status="success",
)

# Default: all fields, types as Python objects
print(run.model_dump())
# {'run_id': 'run_abc123', 'pipeline_name': 'daily_orders',
#  'started_at': datetime(2024, 1, 15, 8, 0, 0), ...}

# JSON-serializable format (datetimes become strings)
print(run.model_dump(mode="json"))
# {'run_id': 'run_abc123', 'started_at': '2024-01-15T08:00:00', ...}

# Exclude None fields (cleaner output)
print(run.model_dump(exclude_none=True))

# Exclude specific fields
print(run.model_dump(exclude={"error_message"}))

# JSON string directly
print(run.model_dump_json(indent=2, exclude_none=True))

# Rename fields in output (via alias)
class PipelineRunOutput(PipelineRun):
    model_config = {"populate_by_name": True}
    records_processed: int = Field(alias="recordsProcessed")

run_out = PipelineRunOutput.model_validate(run.model_dump())
print(run_out.model_dump(by_alias=True))
# {'recordsProcessed': 1500000, ...}  ← camelCase output for API response
```

---

## Key Takeaways

1. **`@field_validator`** is for single-field business rules: format checks, range validation, normalization (lowercasing, stripping whitespace).
2. **`@model_validator(mode="after")`** is for cross-field rules: date range consistency, mutually exclusive options, conditional requirements.
3. **`extra="ignore"`** is your friend for real-world APIs that return undocumented fields — don't crash, just ignore.
4. **`Field(alias="camelCase")`** handles the camelCase vs snake_case mismatch between APIs and Python conventions.
5. **`model_dump(mode="json")`** gives you JSON-safe output (datetimes as strings) — use this when serializing to Kafka, S3, or REST responses.
