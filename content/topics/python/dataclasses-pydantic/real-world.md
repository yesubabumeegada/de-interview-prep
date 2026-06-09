---
title: "Dataclasses & Pydantic — Real-World Patterns"
topic: python
subtopic: dataclasses-pydantic
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [python, pydantic, BaseSettings, schema-contract, API-validation, delta-schema]
---

# Dataclasses & Pydantic — Real-World Patterns

Production patterns: environment-driven pipeline config, validating API responses before ingestion, schema contract testing between producer and consumer, and modeling Delta table schemas with Pydantic.

---

## Pattern 1: Pipeline Config with Pydantic BaseSettings

```python
# pipeline_config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, field_validator
from typing import Optional, Literal
from functools import lru_cache
import os


class RedshiftSettings(BaseSettings):
    host: str
    port: int = 5439
    database: str
    user: str
    password: SecretStr
    schema_name: str = "public"

    model_config = SettingsConfigDict(env_prefix="REDSHIFT_")

    @property
    def connection_string(self) -> str:
        return (
            f"redshift+psycopg2://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class S3Settings(BaseSettings):
    bucket: str
    region: str = "us-east-1"
    prefix: str = ""
    endpoint_url: Optional[str] = None  # For MinIO / local dev

    model_config = SettingsConfigDict(env_prefix="S3_")


class PipelineSettings(BaseSettings):
    """
    Reads from environment variables or .env file.
    Typical environment setup:
        PIPELINE_ENV=production
        PIPELINE_NAME=orders_etl
        PIPELINE_BATCH_SIZE=5000
        REDSHIFT_HOST=my-cluster.redshift.amazonaws.com
        REDSHIFT_PASSWORD=...
        S3_BUCKET=my-data-lake
    """
    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    name: str
    env: Literal["development", "staging", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    batch_size: int = Field(default=1000, ge=1, le=50_000)
    enable_retries: bool = True
    max_retries: int = Field(default=3, ge=0, le=10)
    dq_failure_threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    slack_webhook: Optional[SecretStr] = None

    # Sub-settings (constructed from their own env prefixes)
    redshift: RedshiftSettings = Field(default_factory=RedshiftSettings)
    s3: S3Settings = Field(default_factory=S3Settings)

    @field_validator("env")
    @classmethod
    def normalize_env(cls, v: str) -> str:
        return v.lower()


@lru_cache(maxsize=1)
def get_pipeline_settings() -> PipelineSettings:
    """Singleton settings — call throughout the codebase."""
    settings = PipelineSettings()
    # Log config on startup (sensitive fields are masked by SecretStr)
    import logging
    logger = logging.getLogger(__name__)
    logger.info(
        "Pipeline settings loaded: name=%s env=%s batch_size=%d",
        settings.name, settings.env, settings.batch_size
    )
    return settings


# In any module:
from pipeline_config import get_pipeline_settings

def run_etl():
    cfg = get_pipeline_settings()
    conn = create_engine(cfg.redshift.connection_string)
    # ...

# In tests: override specific settings
def test_with_custom_settings(monkeypatch):
    monkeypatch.setenv("PIPELINE_BATCH_SIZE", "100")
    monkeypatch.setenv("PIPELINE_ENV", "development")
    get_pipeline_settings.cache_clear()
    settings = get_pipeline_settings()
    assert settings.batch_size == 100
```

---

## Pattern 2: Validating API Responses Before Ingestion

```python
# api_validator.py
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
from datetime import datetime
import logging
import requests

logger = logging.getLogger(__name__)


class SalesforceOpportunity(BaseModel):
    """Schema for Salesforce Opportunity records."""
    model_config = {"extra": "ignore", "populate_by_name": True}

    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    account_id: Optional[str] = Field(default=None, alias="AccountId")
    amount: Optional[float] = Field(default=None, alias="Amount")
    close_date: Optional[str] = Field(default=None, alias="CloseDate")
    stage: str = Field(alias="StageName")
    probability: float = Field(alias="Probability", ge=0.0, le=100.0)
    created_date: datetime = Field(alias="CreatedDate")
    last_modified_date: datetime = Field(alias="LastModifiedDate")
    owner_id: str = Field(alias="OwnerId")

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, v: str) -> str:
        known_stages = {
            "Prospecting", "Qualification", "Needs Analysis",
            "Value Proposition", "Id. Decision Makers",
            "Perception Analysis", "Proposal/Price Quote",
            "Negotiation/Review", "Closed Won", "Closed Lost"
        }
        if v not in known_stages:
            logger.warning("Unknown opportunity stage: '%s'", v)
            # Don't reject — unknown stages may be custom, log and continue
        return v


class SalesforceQueryResponse(BaseModel):
    """Wrapper for Salesforce SOQL query response."""
    total_size: int = Field(alias="totalSize")
    done: bool = Field(alias="done")
    records: List[SalesforceOpportunity] = Field(alias="records")
    next_records_url: Optional[str] = Field(default=None, alias="nextRecordsUrl")


def fetch_opportunities(sf_client, modified_since: str) -> List[SalesforceOpportunity]:
    """
    Fetch opportunities from Salesforce, validating each page of results.
    Gracefully handles individual record validation failures.
    """
    all_records = []
    page = 0
    validation_errors = 0

    soql = f"""
        SELECT Id, Name, AccountId, Amount, CloseDate, StageName,
               Probability, CreatedDate, LastModifiedDate, OwnerId
        FROM Opportunity
        WHERE LastModifiedDate >= {modified_since}
        ORDER BY LastModifiedDate ASC
    """

    response_data = sf_client.query(soql)

    while True:
        page += 1
        try:
            # Validate the page-level response structure
            page_response = SalesforceQueryResponse.model_validate(response_data)
        except Exception as e:
            raise ValueError(
                f"Salesforce API response structure unexpected on page {page}: {e}"
            ) from e

        all_records.extend(page_response.records)

        if page_response.done or not page_response.next_records_url:
            break

        response_data = sf_client.query_more(page_response.next_records_url, identifier_is_url=True)

    logger.info(
        "Fetched %d opportunities across %d pages, %d validation errors skipped",
        len(all_records), page, validation_errors
    )
    return all_records


# Validate before writing to database/warehouse
def ingest_opportunities_to_warehouse(opportunities: List[SalesforceOpportunity], db_conn):
    """Convert validated Pydantic models to dicts for DB insertion."""
    rows = []
    for opp in opportunities:
        row = opp.model_dump(by_alias=False, mode="json", exclude_none=False)
        # All type coercions already handled by Pydantic — safe to insert
        rows.append(row)

    # Bulk insert (psycopg2 example)
    db_conn.executemany(
        """
        INSERT INTO salesforce_opportunities
            (id, name, account_id, amount, close_date, stage, probability,
             created_date, last_modified_date, owner_id)
        VALUES
            (%(id)s, %(name)s, %(account_id)s, %(amount)s, %(close_date)s,
             %(stage)s, %(probability)s, %(created_date)s, %(last_modified_date)s, %(owner_id)s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            amount = EXCLUDED.amount,
            stage = EXCLUDED.stage,
            last_modified_date = EXCLUDED.last_modified_date
        """,
        rows
    )
    logger.info("Inserted/updated %d opportunity records", len(rows))
```

---

## Pattern 3: Schema Contract Testing Between Producer and Consumer

```python
# schema_contracts.py
"""
Define schema contracts as Pydantic models.
Both the producing pipeline and consuming pipeline import from here.
This ensures that a schema change in the producer is immediately
caught as a test failure in the consumer's test suite.
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from enum import Enum


class OrderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class OrderLineItem(BaseModel):
    product_id: str
    product_name: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0.0)
    discount: float = Field(ge=0.0, le=1.0, default=0.0)

    @property
    def line_total(self) -> float:
        return self.unit_price * self.quantity * (1 - self.discount)


# Contract version: bump this when schema changes break backward compatibility
ORDERS_SILVER_SCHEMA_VERSION = "2.1.0"


class OrderSilverRecord(BaseModel):
    """
    Published schema contract for silver.orders table.
    Version: 2.1.0

    Consumers: reporting_pipeline, ml_feature_pipeline, finance_export_pipeline
    Producer:  orders_etl_pipeline

    Change log:
    - 2.1.0: Added `fulfillment_center` (nullable, backward compatible)
    - 2.0.0: Renamed `customer_key` to `customer_id` (breaking change)
    - 1.0.0: Initial schema
    """
    order_id: str
    customer_id: int = Field(gt=0)
    order_date: date
    status: OrderStatus
    items: List[OrderLineItem]
    subtotal: float = Field(ge=0.0)
    tax: float = Field(ge=0.0)
    total: float = Field(ge=0.0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    created_at: datetime
    updated_at: datetime
    schema_version: str = ORDERS_SILVER_SCHEMA_VERSION

    # New in 2.1.0 — nullable for backward compatibility
    fulfillment_center: Optional[str] = None


# Consumer: validate records read from silver.orders against the contract
def validate_orders_from_silver(records: List[dict]) -> List[OrderSilverRecord]:
    """
    Validate records against the published schema contract.
    Run in the consuming pipeline before processing.
    """
    valid, invalid = [], []
    for record in records:
        try:
            validated = OrderSilverRecord.model_validate(record)
            # Version compatibility check
            if validated.schema_version != ORDERS_SILVER_SCHEMA_VERSION:
                logger.warning(
                    "Schema version mismatch: expected %s, got %s for order %s",
                    ORDERS_SILVER_SCHEMA_VERSION, validated.schema_version, validated.order_id
                )
            valid.append(validated)
        except Exception as e:
            invalid.append({"record": record, "error": str(e)})

    if invalid:
        logger.error(
            "Schema contract violations: %d/%d records failed validation",
            len(invalid), len(records)
        )
        # Write to error queue for investigation
        write_contract_violations(invalid)

    return valid


# Test: runs in both producer and consumer CI pipelines
def test_orders_silver_schema_contract():
    """
    This test is run by BOTH the producer and consumer pipelines.
    If the producer changes the schema, this test fails in the consumer's CI
    before the change reaches production.
    """
    # A sample valid record that the producer is expected to produce
    sample_valid = {
        "order_id": "ord_001",
        "customer_id": 42,
        "order_date": "2024-01-15",
        "status": "delivered",
        "items": [
            {"product_id": "sku_001", "product_name": "Widget", "quantity": 2, "unit_price": 49.99}
        ],
        "subtotal": 99.98,
        "tax": 8.50,
        "total": 108.48,
        "currency": "USD",
        "created_at": "2024-01-15T10:00:00",
        "updated_at": "2024-01-15T14:30:00",
        "schema_version": "2.1.0",
    }

    # Must parse without error
    record = OrderSilverRecord.model_validate(sample_valid)
    assert record.order_id == "ord_001"
    assert record.status == OrderStatus.DELIVERED
    assert abs(record.items[0].line_total - 99.98) < 0.001

    # Backward compatibility: old records without fulfillment_center still valid
    old_record = {**sample_valid, "schema_version": "2.0.0"}
    old_parsed = OrderSilverRecord.model_validate(old_record)
    assert old_parsed.fulfillment_center is None
```

---

## Pattern 4: Pydantic Model for Delta Table Schema

```python
# delta_schema_models.py
from pydantic import BaseModel, Field, model_validator
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    TimestampType, DateType, BooleanType, ArrayType, MapType, IntegerType
)


PYDANTIC_TO_SPARK_TYPE = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
}


class DeltaColumnDef(BaseModel):
    """Definition for a single Delta table column."""
    name: str
    python_type: type
    nullable: bool = True
    comment: Optional[str] = None


class DeltaTableDefinition(BaseModel):
    """
    Define a Delta table schema using Pydantic.
    Generates both the PySpark StructType and DDL SQL.
    """
    table_name: str  # catalog.schema.table
    columns: List[DeltaColumnDef]
    partition_by: List[str] = []
    z_order_by: List[str] = []
    enable_cdf: bool = True
    comment: Optional[str] = None

    @model_validator(mode="after")
    def validate_partition_columns_exist(self) -> "DeltaTableDefinition":
        col_names = {c.name for c in self.columns}
        for pc in self.partition_by:
            if pc not in col_names:
                raise ValueError(f"Partition column '{pc}' not in column list")
        return self

    def to_spark_schema(self) -> StructType:
        """Convert to PySpark StructType."""
        fields = []
        for col in self.columns:
            spark_type = PYDANTIC_TO_SPARK_TYPE.get(col.python_type, StringType())
            fields.append(StructField(col.name, spark_type, col.nullable))
        return StructType(fields)

    def to_create_ddl(self) -> str:
        """Generate CREATE TABLE DDL."""
        col_defs = []
        for col in self.columns:
            type_map = {str: "STRING", int: "BIGINT", float: "DOUBLE", bool: "BOOLEAN"}
            sql_type = type_map.get(col.python_type, "STRING")
            comment = f" COMMENT '{col.comment}'" if col.comment else ""
            col_defs.append(f"    {col.name} {sql_type}{comment}")

        partition_clause = ""
        if self.partition_by:
            partition_clause = f"\nPARTITIONED BY ({', '.join(self.partition_by)})"

        cdf_prop = "'delta.enableChangeDataFeed' = 'true'" if self.enable_cdf else ""
        tblprops = f"\nTBLPROPERTIES ({cdf_prop})" if cdf_prop else ""

        return f"""
CREATE TABLE IF NOT EXISTS {self.table_name} (
{chr(10).join(col_defs)}
)
USING DELTA{partition_clause}{tblprops}
""".strip()


# Define the schema once — use everywhere:
EVENTS_TABLE_DEF = DeltaTableDefinition(
    table_name="warehouse.silver_events",
    columns=[
        DeltaColumnDef(name="event_id",    python_type=str,   nullable=False, comment="Unique event identifier"),
        DeltaColumnDef(name="user_id",     python_type=int,   nullable=True,  comment="User ID, null for anonymous"),
        DeltaColumnDef(name="event_type",  python_type=str,   nullable=False),
        DeltaColumnDef(name="amount",      python_type=float, nullable=True),
        DeltaColumnDef(name="event_date",  python_type=str,   nullable=False, comment="Partition column: YYYY-MM-DD"),
        DeltaColumnDef(name="created_at",  python_type=str,   nullable=False),
    ],
    partition_by=["event_date"],
    z_order_by=["user_id", "event_type"],
    enable_cdf=True,
)

# Create the table:
spark.sql(EVENTS_TABLE_DEF.to_create_ddl())

# Use the schema for reading (ensures consistent schema across pipeline):
events = spark.read.schema(EVENTS_TABLE_DEF.to_spark_schema()) \
    .format("delta") \
    .load("s3://lakehouse/silver/events/")
```

---

## Key Takeaways

1. **`BaseSettings` + `@lru_cache`** is the production config pattern — single source of truth, reads from environment, testable via monkeypatching.
2. **Validate API responses before any transformation** — once bad data enters your pipeline state, it's expensive to remove.
3. **Schema contracts as shared Pydantic models** create a compile-time contract between teams — a breaking change fails in the consumer's CI before reaching production.
4. **`model_dump(mode="json")`** gives you database-safe output — datetimes as ISO strings, no Python-specific objects.
5. **Pydantic models for Delta schemas** give you a single source of truth for table structure, PySpark `StructType`, and DDL — no schema drift between the definition and the actual table.
