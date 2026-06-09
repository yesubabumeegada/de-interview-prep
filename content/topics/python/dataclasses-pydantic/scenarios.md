---
title: "Dataclasses & Pydantic — Scenarios"
topic: python
subtopic: dataclasses-pydantic
content_type: scenario_question
tags: [python, pydantic, dataclasses, validation, schema-contract, BaseSettings, interview]
---

# Dataclasses & Pydantic — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Write a Pydantic Model for a Pipeline Config

**Scenario:** Your team's pipeline reads its config from a YAML file that looks like this. Write a Pydantic `BaseModel` that validates this config with appropriate constraints.

```yaml
pipeline_name: orders_daily_load
source_bucket: my-data-lake
source_prefix: raw/orders/
target_table: warehouse.silver.orders
batch_size: 5000
write_mode: append
enable_retries: true
max_retries: 3
notify_on_failure: de-team@company.com
```

<details>
<summary>💡 Hint</summary>

Think about: what constraints make sense for each field? `batch_size` should be positive, `max_retries` should be non-negative, `write_mode` should be one of a fixed set, `notify_on_failure` should be a valid email if provided. Use `Field()` for numeric constraints and a validator for email.

</details>

<details>
<summary>✅ Solution</summary>

```python
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Literal
import yaml


class PipelineConfig(BaseModel):
    """
    Pipeline configuration with validation.
    Loaded from YAML/JSON config files.
    """
    pipeline_name: str
    source_bucket: str
    source_prefix: str
    target_table: str
    batch_size: int = Field(default=1000, gt=0, le=100_000,
                            description="Records per batch. Must be 1-100,000.")
    write_mode: Literal["append", "overwrite", "merge"] = "append"
    enable_retries: bool = True
    max_retries: int = Field(default=3, ge=0, le=10,
                             description="Max retry attempts. 0 = no retries.")
    notify_on_failure: Optional[str] = None

    @field_validator("pipeline_name")
    @classmethod
    def pipeline_name_must_be_slug(cls, v: str) -> str:
        """Pipeline names should be lowercase with underscores only."""
        import re
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                f"pipeline_name must be lowercase alphanumeric with underscores, got: '{v}'"
            )
        return v

    @field_validator("target_table")
    @classmethod
    def target_table_format(cls, v: str) -> str:
        """Enforce catalog.schema.table three-part format."""
        parts = v.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"target_table must be in catalog.schema.table format, got: '{v}'"
            )
        return v

    @field_validator("notify_on_failure")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if "@" not in v:
            raise ValueError(f"notify_on_failure must be a valid email address, got: '{v}'")
        return v.lower()

    @field_validator("source_bucket")
    @classmethod
    def bucket_without_s3_prefix(cls, v: str) -> str:
        if v.startswith("s3://"):
            raise ValueError("source_bucket should not include 's3://' prefix")
        return v


# Load from YAML file
def load_pipeline_config(config_path: str) -> PipelineConfig:
    """Load and validate pipeline config from a YAML file."""
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    try:
        config = PipelineConfig.model_validate(raw)
        print(f"Config loaded successfully: {config.pipeline_name}")
        return config
    except Exception as e:
        raise ValueError(f"Invalid pipeline config in {config_path}:\n{e}") from e


# Test the model:
config = PipelineConfig.model_validate({
    "pipeline_name": "orders_daily_load",
    "source_bucket": "my-data-lake",
    "source_prefix": "raw/orders/",
    "target_table": "warehouse.silver.orders",
    "batch_size": 5000,
    "write_mode": "append",
    "enable_retries": True,
    "max_retries": 3,
    "notify_on_failure": "de-team@company.com",
})

print(config.batch_size)        # 5000
print(config.write_mode)        # "append"
print(config.notify_on_failure) # "de-team@company.com"

# Validation errors:
try:
    bad_config = PipelineConfig.model_validate({
        "pipeline_name": "My Pipeline!",  # Invalid chars
        "source_bucket": "my-bucket",
        "source_prefix": "raw/",
        "target_table": "just_a_table",   # Missing catalog.schema prefix
        "batch_size": -100,               # Negative
    })
except Exception as e:
    print(e)
    # 3 validation errors:
    # pipeline_name: pipeline_name must be lowercase alphanumeric with underscores
    # target_table: target_table must be in catalog.schema.table format
    # batch_size: Input should be greater than 0
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Add Validation to Catch Bad API Responses Before Corrupting a Pipeline

**Scenario:** Your pipeline ingests customer data from a CRM API. Recently, a bug in the CRM caused it to return `customer_id` as a string instead of an integer, and `lifetime_value` as `null` for all records. This corrupted your Silver table. How would you add Pydantic validation to catch these issues before they reach the warehouse? Show the validator code and how you'd integrate it into the pipeline.

<details>
<summary>💡 Hint</summary>

You need: a Pydantic model for the CRM API response, field-level validators for business rules (e.g., lifetime_value can't be null for active customers), a batch validation function that logs errors and writes them to a DLQ, and a quality gate that fails the pipeline if too many records are invalid. Show how the existing pipeline code changes minimally.

</details>

<details>
<summary>✅ Solution</summary>

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CustomerRecord(BaseModel):
    """
    Schema contract for CRM API customer records.
    Catches the exact bugs that corrupted Silver:
    - customer_id returned as string
    - lifetime_value returned as null
    """
    model_config = {"extra": "ignore"}  # Ignore unexpected CRM fields

    customer_id: int = Field(gt=0)  # MUST be positive integer, coerces string "42" → 42
    name: str
    email: str
    status: str  # "active", "inactive", "churned"
    lifetime_value: float = Field(ge=0.0)  # Cannot be null or negative
    signup_date: datetime
    last_order_date: Optional[datetime] = None

    @field_validator("email")
    @classmethod
    def email_must_be_valid(cls, v: str) -> str:
        if "@" not in v or len(v) < 5:
            raise ValueError(f"Invalid email: '{v}'")
        return v.lower()

    @field_validator("status")
    @classmethod
    def status_must_be_known(cls, v: str) -> str:
        known_statuses = {"active", "inactive", "churned"}
        if v.lower() not in known_statuses:
            raise ValueError(
                f"Unknown customer status '{v}'. Expected one of {known_statuses}. "
                f"This may indicate a CRM schema change — investigate before ingesting."
            )
        return v.lower()

    @model_validator(mode="after")
    def active_customers_must_have_ltv(self) -> "CustomerRecord":
        """Business rule: active customers must have a positive lifetime_value."""
        if self.status == "active" and self.lifetime_value == 0.0:
            raise ValueError(
                f"Active customer {self.customer_id} has lifetime_value=0.0. "
                f"This likely indicates a data error (CRM bug?). "
                f"Active customers must have LTV > 0."
            )
        return self


class CrmApiResponse(BaseModel):
    """Wrapper for CRM paginated response."""
    model_config = {"extra": "ignore"}
    customers: List[dict]  # We validate each record individually for better error isolation
    total_count: int
    page: int
    has_more: bool


def validate_crm_records(raw_records: List[dict]) -> tuple[List[CustomerRecord], List[dict]]:
    """
    Validate CRM records against the schema contract.
    Returns (valid_records, error_report).
    Writes errors to DLQ for investigation.
    """
    valid, errors = [], []

    for i, raw in enumerate(raw_records):
        try:
            validated = CustomerRecord.model_validate(raw)
            valid.append(validated)
        except Exception as e:
            error_entry = {
                "raw_record": raw,
                "validation_error": str(e),
                "record_index": i,
                "ingested_at": datetime.utcnow().isoformat(),
            }
            errors.append(error_entry)
            logger.warning(
                "CRM record validation failed for customer_id=%s: %s",
                raw.get("customer_id", "UNKNOWN"),
                str(e)[:200]
            )

    # Log summary
    total = len(raw_records)
    logger.info(
        "CRM validation: %d/%d valid (%.1f%%), %d invalid",
        len(valid), total, 100 * len(valid) / total if total else 0, len(errors)
    )

    return valid, errors


# ── Integrate into the existing pipeline ──────────────────────────────────

def run_crm_ingestion_pipeline(api_client, target_table: str, page_date: str):
    """
    Updated pipeline with Pydantic validation gate.
    Original pipeline had zero validation — this adds it minimally.
    """
    # 1. Fetch from CRM API (unchanged)
    raw_records = api_client.fetch_customers(modified_since=page_date)
    logger.info("Fetched %d raw records from CRM", len(raw_records))

    # 2. NEW: Validate all records before any transformation
    valid_records, error_records = validate_crm_records(raw_records)

    # 3. NEW: Write invalid records to DLQ for investigation
    if error_records:
        dlq_path = f"s3://data-lake/dlq/crm/dt={page_date}/"
        write_to_dlq(error_records, dlq_path)
        logger.warning(
            "Wrote %d invalid records to DLQ: %s", len(error_records), dlq_path
        )

    # 4. NEW: Quality gate — fail if too many records are invalid
    validity_rate = len(valid_records) / len(raw_records) if raw_records else 1.0
    if validity_rate < 0.95:
        raise ValueError(
            f"Data quality gate FAILED: only {validity_rate:.1%} of CRM records are valid "
            f"(minimum required: 95%). "
            f"This may indicate a CRM schema change or data feed issue. "
            f"Check DLQ at {dlq_path} for details. "
            f"Pipeline halted to prevent corrupting Silver table."
        )

    # 5. Transform and write to Silver (mostly unchanged)
    rows = [
        {
            "customer_id":     r.customer_id,     # Now guaranteed to be int
            "name":            r.name,
            "email":           r.email,            # Now guaranteed lowercase with @
            "status":          r.status,           # Now guaranteed to be known value
            "lifetime_value":  r.lifetime_value,   # Now guaranteed to be >= 0
            "signup_date":     r.signup_date.isoformat(),
            "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
            "validated_at":    datetime.utcnow().isoformat(),
        }
        for r in valid_records
    ]

    write_to_delta(rows, target_table)
    logger.info("Pipeline complete: wrote %d valid records to %s", len(rows), target_table)
```

**What this catches:**
- `customer_id` as string `"42"` → Pydantic coerces to `int` `42`. If it's non-numeric (e.g., `"abc"`), it raises `ValidationError` immediately.
- `lifetime_value = null` → Pydantic requires `float`, so `None` fails validation.
- Active customers with `lifetime_value = 0.0` → Model validator catches this business rule violation.
- Unknown status values → Field validator catches schema changes in the CRM.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Schema Contract System Using Pydantic for a Multi-Team Data Platform

**Scenario:** Your data platform has 15 producing pipelines and 30+ consuming pipelines. Schema changes break consumers regularly because there's no formal contract system. Design a Pydantic-based schema contract system that: defines contracts in code, tests contracts in CI for both producers and consumers, supports backward-compatible evolution, and alerts consumers when a contract changes.

<details>
<summary>💡 Hint</summary>

Think about: where the contracts live (shared library), how versioning works (SemVer in the model), how consumers detect breaking changes (CI tests against the shared model), how you handle the migration window (support both old and new schema simultaneously), and what tooling makes adoption low-friction.

</details>

<details>
<summary>✅ Solution</summary>

```python
# ── shared_contracts/orders.py (installed as a package in all pipelines) ──

from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Union, Annotated
from datetime import date, datetime
from enum import Enum
from functools import lru_cache


# Version control for the contract
SCHEMA_VERSION = "3.0.0"
SUPPORTED_SCHEMA_VERSIONS = {"3.0.0", "2.1.0"}  # Producer still writes both during migration


class OrderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class OrderLineItem(BaseModel):
    product_id: str
    sku: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0.0)
    discount_pct: float = Field(ge=0.0, le=1.0, default=0.0)

    @property
    def line_total(self) -> float:
        return self.unit_price * self.quantity * (1 - self.discount_pct)


class OrderContractV3(BaseModel):
    """
    Published schema contract for silver.orders — Version 3.0.0
    BREAKING CHANGES from 2.x:
    - customer_id renamed to customer_uuid (breaking)
    - items is now required (was optional)
    - Added: fulfillment_region (new, required)

    Migration window: Producer writes BOTH v2 and v3 schemas for 30 days.
    Consumers must upgrade to v3 within 30 days.
    """
    schema_version: Literal["3.0.0"] = "3.0.0"
    order_id: str
    customer_uuid: str        # Renamed from customer_id in v2
    order_date: date
    status: OrderStatus
    items: List[OrderLineItem]  # Now required
    subtotal: float = Field(ge=0.0)
    tax: float = Field(ge=0.0)
    total: float = Field(ge=0.0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    fulfillment_region: str   # New required field
    created_at: datetime
    updated_at: datetime

    class Config:
        # Allow extra fields for future forward compatibility
        extra = "ignore"


class OrderContractV2(BaseModel):
    """
    Deprecated — Version 2.1.0.
    Consumers MUST migrate to v3 by 2024-02-15.
    """
    schema_version: Literal["2.1.0"] = "2.1.0"
    order_id: str
    customer_id: int          # Was integer, now UUID string in v3
    order_date: date
    status: OrderStatus
    items: Optional[List[OrderLineItem]] = None  # Was optional in v2
    subtotal: float = Field(ge=0.0)
    tax: float = Field(ge=0.0)
    total: float = Field(ge=0.0)
    currency: str = Field(default="USD")
    created_at: datetime
    updated_at: datetime

    class Config:
        extra = "ignore"


# Discriminated union for multi-version consumers
AnyOrderVersion = Annotated[
    Union[OrderContractV3, OrderContractV2],
    Field(discriminator="schema_version")
]


# ── Consumer utilities ─────────────────────────────────────────────────────

def parse_order(raw: dict) -> Union[OrderContractV3, OrderContractV2]:
    """Parse an order record from any supported schema version."""
    version = raw.get("schema_version", "2.1.0")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(
            f"Unsupported schema version '{version}'. "
            f"Supported: {SUPPORTED_SCHEMA_VERSIONS}"
        )
    if version == "3.0.0":
        return OrderContractV3.model_validate(raw)
    elif version == "2.1.0":
        return OrderContractV2.model_validate(raw)


def get_customer_identifier(order: Union[OrderContractV3, OrderContractV2]) -> str:
    """Version-aware customer ID accessor."""
    if isinstance(order, OrderContractV3):
        return order.customer_uuid
    else:
        return str(order.customer_id)  # Normalize v2 int to string


# ── Contract tests (run in BOTH producer and consumer CI) ─────────────────

import pytest


class TestOrderContractV3:
    """
    These tests are SHARED between producer and consumer pipelines.
    Both import and run these tests in their CI.
    A schema change that breaks these tests must be a coordinated migration.
    """

    CANONICAL_V3_RECORD = {
        "schema_version": "3.0.0",
        "order_id": "ord_001",
        "customer_uuid": "cust-uuid-abc-123",
        "order_date": "2024-01-15",
        "status": "delivered",
        "items": [
            {"product_id": "p001", "sku": "SKU-001", "quantity": 2, "unit_price": 49.99}
        ],
        "subtotal": 99.98,
        "tax": 8.50,
        "total": 108.48,
        "currency": "USD",
        "fulfillment_region": "us-west-2",
        "created_at": "2024-01-15T10:00:00",
        "updated_at": "2024-01-15T14:00:00",
    }

    def test_canonical_record_parses(self):
        """The canonical record must parse without errors."""
        order = OrderContractV3.model_validate(self.CANONICAL_V3_RECORD)
        assert order.order_id == "ord_001"
        assert order.status == OrderStatus.DELIVERED

    def test_required_fields_present(self):
        """All required fields must be present."""
        required = ["order_id", "customer_uuid", "order_date", "status",
                    "items", "subtotal", "tax", "total", "fulfillment_region"]
        for field_name in required:
            reduced = {k: v for k, v in self.CANONICAL_V3_RECORD.items()
                       if k != field_name}
            with pytest.raises(Exception):
                OrderContractV3.model_validate(reduced)

    def test_field_types_enforced(self):
        """Type violations must raise validation errors."""
        # subtotal must be numeric
        bad = {**self.CANONICAL_V3_RECORD, "subtotal": "not-a-number"}
        with pytest.raises(Exception):
            OrderContractV3.model_validate(bad)

    def test_backward_compatible_with_extra_fields(self):
        """Future fields in records should not break current consumers."""
        future_record = {**self.CANONICAL_V3_RECORD, "new_future_field": "value"}
        # extra="ignore" means this should NOT raise
        order = OrderContractV3.model_validate(future_record)
        assert not hasattr(order, "new_future_field")

    def test_v2_and_v3_parseable_by_multi_version_consumer(self):
        """Multi-version consumer must handle both schemas."""
        v3 = parse_order(self.CANONICAL_V3_RECORD)
        assert isinstance(v3, OrderContractV3)

        v2_record = {
            "schema_version": "2.1.0",
            "order_id": "ord_002",
            "customer_id": 42,
            "order_date": "2024-01-15",
            "status": "shipped",
            "subtotal": 50.0,
            "tax": 5.0,
            "total": 55.0,
            "created_at": "2024-01-15T10:00:00",
            "updated_at": "2024-01-15T10:00:00",
        }
        v2 = parse_order(v2_record)
        assert isinstance(v2, OrderContractV2)

        # Version-aware accessor works for both
        assert get_customer_identifier(v3) == "cust-uuid-abc-123"
        assert get_customer_identifier(v2) == "42"
```

**System design summary:**

The contract system has three components:
1. **Shared library** (`shared_contracts/`): Pydantic models versioned with SemVer, installed as a package in all pipelines via PyPI or a private registry.
2. **CI tests** (`TestOrderContractV3`): Run in both producer and consumer CI. A producer schema change that breaks the canonical test blocks the producer's PR. A consumer that ignores the migration deadline will fail its CI when it tries to parse v3 records.
3. **Migration protocol**: Producer writes both old and new schema versions during the transition window (30 days). Consumers use discriminated unions to handle both. After the window, the producer drops the old version and consumers must have upgraded.

The `extra="ignore"` + forward-compatibility test ensures that *additive* changes (new optional fields) don't break consumers.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "When would you use a Python dataclass vs a Pydantic BaseModel?" — Use `@dataclass` for internal data containers you fully control: metrics, intermediate results, pipeline state objects. Use `BaseModel` when data crosses a boundary you don't control: API responses, config files, user input, Kafka messages. The key difference: Pydantic validates types at runtime; dataclasses only provide IDE hints and no runtime enforcement.

> **Tip 2:** "What's the performance cost of Pydantic?" — Pydantic v2 (using Rust-based core) is roughly 5-50x faster than v1. For most DE workloads, the validation cost is negligible because it's a small percentage of I/O time. Where it matters: validating millions of records in a tight loop. In that case, pre-validate a sample, then process the rest with assumptions, or use `model_construct()` to skip validation for trusted data.

> **Tip 3:** "How do you handle breaking schema changes across teams?" — The honest answer involves process AND technology: (1) version your models with SemVer, (2) run schema contract tests in consumer CI, (3) define a migration window where the producer publishes both old and new schemas, (4) consumers use discriminated unions to handle both during migration. Without this coordination, any schema change is a production incident.
