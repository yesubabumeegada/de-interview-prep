---
title: "Schema Management - Scenario Questions"
topic: pyspark
subtopic: schema-management
content_type: scenario_question
tags: [pyspark, schema, structtype, schema-drift, schema-evolution, delta-lake]
---

# Schema Management — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Define an Explicit Schema for Nested JSON

You receive a JSON file where each record looks like this:

```json
{
  "order_id": 10045,
  "customer_id": 7891,
  "placed_at": "2024-03-15 10:23:00",
  "address": {
    "street": "123 Main St",
    "city": "Austin",
    "zip_code": "78701"
  },
  "items": [
    {"product_id": 501, "quantity": 2, "unit_price": 29.99},
    {"product_id": 502, "quantity": 1, "unit_price": 14.99}
  ]
}
```

Define an explicit `StructType` schema for this JSON and read the file. Extract the city from the nested address and explode the items array so each item is its own row.

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType, DoubleType,
    TimestampType, ArrayType
)
from pyspark.sql.functions import col, explode

# Define nested schemas bottom-up
item_schema = StructType([
    StructField("product_id", LongType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
])

address_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("zip_code", StringType(), nullable=True),
])

order_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("placed_at", TimestampType(), nullable=True),
    StructField("address", address_schema, nullable=True),
    StructField("items", ArrayType(item_schema), nullable=True),
])

# Read with explicit schema — no inferSchema scan
df = (
    spark.read
    .schema(order_schema)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .json("/data/orders.json")
)

df.printSchema()
# root
#  |-- order_id: long (nullable = false)
#  |-- customer_id: long (nullable = false)
#  |-- placed_at: timestamp (nullable = true)
#  |-- address: struct (nullable = true)
#  |    |-- street: string (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- zip_code: string (nullable = true)
#  |-- items: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- product_id: long (nullable = false)
#  |    |    |-- quantity: integer (nullable = false)
#  |    |    |-- unit_price: double (nullable = false)

# Extract city from nested address struct
df_with_city = df.withColumn("city", col("address.city"))

# Explode items array so each item becomes its own row
df_exploded = (
    df_with_city
    .withColumn("item", explode(col("items")))
    .select(
        "order_id",
        "customer_id",
        "placed_at",
        "city",
        col("item.product_id"),
        col("item.quantity"),
        col("item.unit_price"),
    )
)

df_exploded.show(5, truncate=False)
```

**Why this approach:**
- Schemas defined bottom-up (leaf types first, then composites) are easier to read and maintain.
- `timestampFormat` tells Spark how to parse the string timestamp — without it, the column reads as null.
- `explode()` on `ArrayType` creates one row per array element, which is the standard way to normalize nested arrays for aggregation.
- `col("address.city")` uses dot notation to traverse the nested struct — no `getField()` needed.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Detect Schema Drift and Raise Descriptive Errors

Your pipeline reads from an upstream S3 path daily. The expected schema is defined in a contract. Write a function that compares the incoming DataFrame's schema against the expected schema and raises a descriptive `ValueError` for:
1. Missing columns that are required (non-nullable in the contract)
2. Type mismatches on any column
3. Unexpected nullable status for non-nullable columns

Do NOT raise errors for extra columns in the incoming data (non-breaking additions are allowed).

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType
from pyspark.sql import DataFrame

# Define the contract
EXPECTED_SCHEMA = StructType([
    StructField("user_id", LongType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("revenue_ltv", DecimalType(18, 2), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("country_code", StringType(), nullable=True),
])


def detect_schema_drift(df: DataFrame, contract: StructType, pipeline_name: str) -> None:
    """
    Validates df schema against contract.
    Raises ValueError with all violations listed together (fail-all, not fail-fast).
    Allows extra columns in df (additive drift is non-breaking).
    """
    actual_fields = {f.name: f for f in df.schema.fields}
    contract_fields = {f.name: f for f in contract.fields}
    
    errors = []
    warnings = []
    
    for col_name, contract_field in contract_fields.items():
        if col_name not in actual_fields:
            if not contract_field.nullable:
                # Required column is missing — hard error
                errors.append(
                    f"  [MISSING] Column '{col_name}' is required (non-nullable in contract) "
                    f"but not present in incoming data"
                )
            else:
                # Optional column is missing — warning, will be null
                warnings.append(
                    f"  [WARN] Optional column '{col_name}' missing; will default to null"
                )
        else:
            actual_field = actual_fields[col_name]
            
            # Type mismatch check
            if actual_field.dataType != contract_field.dataType:
                errors.append(
                    f"  [TYPE MISMATCH] Column '{col_name}': "
                    f"contract={contract_field.dataType}, "
                    f"actual={actual_field.dataType}"
                )
            
            # Nullable mismatch check (contract says NOT NULL but data has nullable=True)
            if not contract_field.nullable and actual_field.nullable:
                errors.append(
                    f"  [NULLABLE] Column '{col_name}' is marked nullable=True in data "
                    f"but contract requires NOT NULL"
                )
    
    # Extra columns are non-breaking — just log them
    extra_cols = set(actual_fields) - set(contract_fields)
    if extra_cols:
        warnings.append(f"  [INFO] Extra columns (non-breaking, will be passed through): {extra_cols}")
    
    # Surface warnings
    for w in warnings:
        print(f"[{pipeline_name}] {w}")
    
    # Fail with all errors at once
    if errors:
        error_msg = (
            f"Schema drift detected in pipeline '{pipeline_name}':\n"
            + "\n".join(errors)
            + f"\n\nRun aborted. Fix upstream schema or update contract version."
        )
        raise ValueError(error_msg)
    
    print(f"[{pipeline_name}] Schema validation passed.")


# Usage in pipeline
incoming_df = spark.read.parquet("s3://upstream/users/date=2024-03-15/")
detect_schema_drift(incoming_df, EXPECTED_SCHEMA, pipeline_name="silver_users_daily")

# Only reaches here if schema is valid
incoming_df.write.format("delta").mode("append").save("/delta/silver/users")
```

**Design decisions:**
- Collect **all** errors before raising (fail-all), so operators see every problem in one run, not one per re-run.
- Extra columns are logged as INFO, not errors — additive changes should never block a pipeline.
- Missing optional columns are warnings (they'll become null), not errors.
- Missing required (non-nullable) columns are hard errors — the pipeline cannot safely produce correct output.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Design a Schema Evolution Strategy for a Delta Table with 5 Downstream Consumers

You own a Delta table `gold.orders` consumed by 5 downstream pipelines:
- 2 Spark batch jobs (read via `spark.table("gold.orders")`)
- 1 Databricks SQL dashboard
- 1 dbt model
- 1 ML feature pipeline

You need to handle:
- **Additive changes** (new columns from upstream) — should flow through automatically without breaking consumers
- **Breaking changes** (type changes, column renames, column removals) — must be versioned to avoid breaking consumers

Design a full schema evolution strategy including code patterns for both scenarios.

<details>
<summary>✅ Solution</summary>

### Strategy Overview

```
Additive changes:   mergeSchema=True + contract version bump (minor)
Breaking changes:   new table version (gold.orders_v2) + parallel write period
```

### Part 1: Handling Additive Changes Automatically

```python
# contracts/gold_orders.py
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType

CONTRACT_VERSION = "1.3.0"  # bump minor for new columns, major for breaking

GOLD_ORDERS_V1_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("total_amount", DecimalType(18, 2), nullable=False),
    StructField("status", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    # New additive columns appended here with nullable=True and defaults
    StructField("discount_pct", DecimalType(5, 2), nullable=True),   # added v1.1
    StructField("channel", StringType(), nullable=True),               # added v1.2
    StructField("promo_code", StringType(), nullable=True),            # added v1.3
])
```

```python
# pipeline: silver_to_gold_orders.py
from contracts.gold_orders import GOLD_ORDERS_V1_SCHEMA, CONTRACT_VERSION
from pyspark.sql.functions import lit

def write_gold_orders(df_silver):
    # Add new columns with sensible defaults when missing from silver
    for field in GOLD_ORDERS_V1_SCHEMA.fields:
        if field.name not in df_silver.columns:
            df_silver = df_silver.withColumn(field.name, lit(None).cast(field.dataType))
    
    # Validate no type mismatches (additive is OK; type change is not)
    for field in GOLD_ORDERS_V1_SCHEMA.fields:
        if field.name in df_silver.columns:
            actual_type = df_silver.schema[field.name].dataType
            if actual_type != field.dataType:
                raise ValueError(
                    f"Type change rejected for '{field.name}': "
                    f"contract={field.dataType}, incoming={actual_type}. "
                    f"Create gold.orders_v2 for breaking changes."
                )
    
    # Reorder to match contract (important for downstream Parquet readers)
    df_final = df_silver.select([f.name for f in GOLD_ORDERS_V1_SCHEMA.fields])
    
    (
        df_final.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")   # allows new columns added to contract
        .saveAsTable("gold.orders")
    )
    
    # Log contract version for lineage
    spark.sql(f"""
        COMMENT ON TABLE gold.orders IS 
        'Schema contract v{CONTRACT_VERSION}. Last written: {datetime.utcnow().isoformat()}'
    """)
```

### Part 2: Handling Breaking Changes with Versioning

```python
# When a breaking change is needed (e.g., rename total_amount → order_total):

# Step 1: Create v2 table with new schema — write in parallel
GOLD_ORDERS_V2_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("order_total", DecimalType(18, 2), nullable=False),  # renamed
    StructField("status", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("discount_pct", DecimalType(5, 2), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("promo_code", StringType(), nullable=True),
])

# Step 2: Write to BOTH tables during migration window (e.g., 2 sprints)
def write_dual(df_silver):
    # Write to v1 (backward compat)
    df_v1 = df_silver.withColumnRenamed("order_total", "total_amount") \
                     .select([f.name for f in GOLD_ORDERS_V1_SCHEMA.fields])
    df_v1.write.format("delta").mode("append").saveAsTable("gold.orders")
    
    # Write to v2 (new schema)
    df_v2 = df_silver.select([f.name for f in GOLD_ORDERS_V2_SCHEMA.fields])
    df_v2.write.format("delta").mode("append").saveAsTable("gold.orders_v2")

# Step 3: Migrate consumers one by one to gold.orders_v2
# Step 4: After all 5 consumers migrated, deprecate gold.orders (keep for 30 days)
# Step 5: Drop gold.orders, rename gold.orders_v2 → gold.orders (or keep versioned)
```

### Part 3: Consumer-Specific Compatibility Views

```sql
-- Create a compatibility view for consumers that haven't migrated yet
CREATE OR REPLACE VIEW gold.orders_v1_compat AS
SELECT
    order_id,
    customer_id,
    order_total AS total_amount,  -- alias breaking rename
    status,
    created_at,
    discount_pct,
    channel,
    promo_code
FROM gold.orders_v2;
```

### Summary of Decision Rules

| Change type | Action | Migration window |
|-------------|--------|-----------------|
| Add nullable column | `mergeSchema=True` + update contract | None needed |
| Add NOT NULL column | Add with default, then enforce | 1 sprint |
| Rename column | Create `orders_v2` + compatibility view | 2 sprints |
| Change column type | Create `orders_v2` | 2 sprints |
| Remove column | Create `orders_v2` | 2 sprints |

**Key principle:** The 5 consumers define your blast radius. More consumers = longer migration windows and more critical need for versioned tables and compatibility views.

</details>
</article>
