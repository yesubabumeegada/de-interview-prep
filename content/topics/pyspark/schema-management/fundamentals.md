---
title: "Schema Management - Fundamentals"
topic: pyspark
subtopic: schema-management
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, schema, structtype, structfield, datatypes, cast]
---

# Schema Management — Fundamentals

## Why Schema Management Matters

In production, reading data without an explicit schema is one of the most common sources of silent data corruption. Spark can infer schemas automatically, but inference is slow, unreliable on sparse data, and a reliability risk in production pipelines. Understanding how to define, inspect, and enforce schemas is a core PySpark skill.

---

## The DataType Hierarchy

PySpark has a rich type system. All types live under `pyspark.sql.types`:

| Type | Python equivalent | Notes |
|------|-------------------|-------|
| `StringType()` | `str` | Variable-length UTF-8 |
| `IntegerType()` | `int` (32-bit) | Max ~2.1 billion |
| `LongType()` | `int` (64-bit) | Use for IDs and counts |
| `DoubleType()` | `float` (64-bit) | Standard float; imprecise for money |
| `DecimalType(p, s)` | `decimal.Decimal` | Use for money; p=precision, s=scale |
| `TimestampType()` | `datetime` | UTC-anchored; timezone-aware |
| `BooleanType()` | `bool` | True/False |
| `ArrayType(elementType)` | `list` | Homogeneous list |
| `MapType(keyType, valueType)` | `dict` | Key-value pairs |
| `StructType([fields])` | nested object | Nested record / schema itself |

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, DecimalType,
    TimestampType, BooleanType, ArrayType, MapType
)
```

---

## StructType and StructField

A `StructType` is a list of `StructField` objects. Each `StructField` has:
- **name** — column name (case-sensitive in Spark)
- **dataType** — one of the types above
- **nullable** — whether `null` values are allowed (default `True`)

```python
schema = StructType([
    StructField("user_id", LongType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("revenue", DecimalType(18, 2), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("tags", ArrayType(StringType()), nullable=True),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True),
])
```

### Nested StructType (for objects / records inside records)

```python
address_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("zip_code", StringType(), nullable=True),
])

user_schema = StructType([
    StructField("user_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", address_schema, nullable=True),  # nested struct
])
```

---

## Schema Inference vs. Explicit Schema

### Schema Inference (`inferSchema=True`)

```python
# Spark reads the data to guess types — slow and risky
df = spark.read.option("inferSchema", True).csv("s3://bucket/users/")
```

**Problems with inference:**
1. **Slow** — Spark must scan a sample of the data before building a plan. For large files this adds minutes.
2. **Type instability** — If the first 100 rows of a column are all integers but row 101 has `"N/A"`, inference guesses `IntegerType` and the row 101 read fails or becomes null.
3. **Reproducibility** — Adding new files to the folder can change the inferred schema for a column, breaking downstream code silently.
4. **Loss of precision** — Inference promotes integers to `LongType` and decimals to `DoubleType`, which loses precision for financial data.

### Explicit Schema (production standard)

```python
schema = StructType([
    StructField("user_id", LongType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("signup_date", TimestampType(), nullable=True),
])

df = spark.read.schema(schema).csv("s3://bucket/users/", header=True)
```

The read is **instantaneous** (no data scan needed) and the types are **guaranteed**.

---

## Inspecting Schemas

```python
# Print the tree-formatted schema to the console
df.printSchema()
# root
#  |-- user_id: long (nullable = false)
#  |-- email: string (nullable = true)

# Get the StructType object programmatically
schema_obj = df.schema  # returns StructType

# Get field names as a list
df.columns  # ['user_id', 'email', 'signup_date']

# Get schema as a JSON string (useful for saving/comparing)
import json
schema_json = df.schema.json()
print(json.dumps(json.loads(schema_json), indent=2))
```

---

## Casting Columns

Spark uses `cast()` to convert one type to another. This is non-destructive — you get back a new Column expression.

```python
from pyspark.sql.functions import col

# Using .cast() inline
df = df.withColumn("revenue", col("revenue").cast("double"))

# Using DecimalType for precision
from pyspark.sql.types import DecimalType
df = df.withColumn("revenue", col("revenue").cast(DecimalType(18, 2)))

# Casting a string date to timestamp
df = df.withColumn("created_at", col("created_at").cast("timestamp"))

# Casting multiple columns at once
from functools import reduce
cols_to_cast = {"amount": "double", "quantity": "integer", "user_id": "long"}
for col_name, dtype in cols_to_cast.items():
    df = df.withColumn(col_name, col(col_name).cast(dtype))
```

**Important:** If the cast is impossible (e.g., casting `"hello"` to `integer`), Spark returns `null` — it does **not** raise an error by default.

---

## Reading JSON and CSV with Explicit Schema

### JSON with explicit schema

```python
json_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer_id", LongType(), nullable=True),
    StructField("total", DecimalType(10, 2), nullable=True),
])

df = spark.read.schema(json_schema).json("s3://bucket/orders/")
```

### CSV with explicit schema

```python
df = (
    spark.read
    .schema(json_schema)
    .option("header", True)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .option("mode", "PERMISSIVE")   # nullifies bad rows instead of failing
    .csv("s3://bucket/orders.csv")
)
```

### DDL string shorthand

For simple schemas you can use a DDL string instead of `StructType`:

```python
df = spark.read.schema("order_id LONG, customer_id LONG, total DECIMAL(10,2)").json(path)
```

---

## Key Takeaways

- Always define explicit schemas for production pipelines — never rely on `inferSchema=True`.
- Use `LongType` for IDs, `DecimalType` for money, `TimestampType` for time.
- `StructType` is recursive — you can nest `StructType` inside `StructType` for nested JSON.
- `cast()` silently returns `null` on failure; validate your data separately.
- `printSchema()` and `df.schema.json()` are your primary schema inspection tools.
