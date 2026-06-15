---
title: "Schema Management - Intermediate"
topic: pyspark
subtopic: schema-management
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, schema, delta-lake, schema-evolution, from_json, schema-drift]
---

# Schema Management — Intermediate

## Schema Evolution in Delta Lake

Delta Lake adds transactional schema management on top of Parquet. Two key options control evolution behavior:

### `mergeSchema` — opt-in schema widening per write

```python
(
    df_with_new_column
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save("/delta/users")
)
```

What `mergeSchema=True` allows:
- Adding new columns (widens the table schema)
- Changing `IntegerType` → `LongType` (safe widening)

What it **rejects** (even with `mergeSchema=True`):
- Changing `StringType` → `IntegerType` (incompatible types)
- Dropping existing columns from the incoming DataFrame — missing columns are written as `null`

### `overwriteSchema` — replace the schema entirely

```python
(
    df_completely_different
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/delta/users")
)
```

Use with care — this rewrites the schema definition and invalidates historical reads that relied on the old schema.

### Schema enforcement (Delta's default)

By default, Delta **rejects** any write whose schema doesn't match the table schema:

```python
# This will raise AnalysisException if df has extra/missing/wrong-typed columns
df.write.format("delta").mode("append").save("/delta/users")
```

This is a feature, not a bug — it prevents silent schema corruption.

---

## `from_json` and `to_json` for Nested JSON Columns

A common pattern: a column in your DataFrame contains a JSON string (e.g., from Kafka or a raw log table). Use `from_json` to parse it into a struct.

```python
from pyspark.sql.functions import from_json, to_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Define the schema of the JSON string
payload_schema = StructType([
    StructField("event_type", StringType()),
    StructField("item_id", LongType()),
    StructField("price", DoubleType()),
])

df = spark.read.parquet("/raw/events")

# Parse the JSON string column into a struct
df_parsed = df.withColumn("payload_struct", from_json(col("payload_json"), payload_schema))

# Access nested fields
df_parsed = df_parsed.withColumn("event_type", col("payload_struct.event_type"))
df_parsed = df_parsed.withColumn("price", col("payload_struct.price"))
```

Going the other direction — struct back to JSON string:

```python
df = df.withColumn("payload_json", to_json(col("payload_struct")))
```

---

## Handling Schema Drift

Schema drift occurs when upstream data sources add, rename, or reorder columns without notice. A robust pipeline must detect and handle this.

### Detecting new columns (drift detection)

```python
def detect_schema_drift(incoming_df, expected_schema: StructType):
    """
    Returns a dict with added, removed, and type-changed columns.
    """
    expected = {f.name: f.dataType for f in expected_schema.fields}
    actual = {f.name: f.dataType for f in incoming_df.schema.fields}

    added = set(actual) - set(expected)
    removed = set(expected) - set(actual)
    type_changed = {
        col for col in set(expected) & set(actual)
        if expected[col] != actual[col]
    }
    return {"added": added, "removed": removed, "type_changed": type_changed}


drift = detect_schema_drift(incoming_df, expected_schema)
if drift["removed"] or drift["type_changed"]:
    raise ValueError(f"Breaking schema drift detected: {drift}")
if drift["added"]:
    print(f"[WARN] New columns detected (non-breaking): {drift['added']}")
```

### Aligning incoming DataFrame to expected schema

When new columns appear, you may want to drop them or keep them. When expected columns are missing, add them as nulls:

```python
from pyspark.sql.functions import lit

def align_schema(df, expected_schema: StructType):
    """Add missing columns as null, drop unexpected columns."""
    # Add missing columns
    for field in expected_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    # Keep only expected columns in the right order
    return df.select([f.name for f in expected_schema.fields])
```

---

## `StructType.fromJson()` — Reconstructing Schemas

You can serialize and deserialize a schema as JSON, which is useful for storing schemas in a config file or database:

```python
import json
from pyspark.sql.types import StructType

# Save schema
schema_json_str = df.schema.json()

# Reload schema later (e.g., from a config store)
reloaded_schema = StructType.fromJson(json.loads(schema_json_str))

# Use it to read data with guaranteed types
df = spark.read.schema(reloaded_schema).parquet("/data/users")
```

This pattern is common in pipeline frameworks where schemas are stored in a schema registry or a YAML/JSON config file checked into version control.

---

## Schema Comparison Between DataFrames

```python
def schemas_equal(df1, df2) -> bool:
    """Check if two DataFrames have the same schema (names and types)."""
    s1 = {f.name: f.dataType for f in df1.schema.fields}
    s2 = {f.name: f.dataType for f in df2.schema.fields}
    return s1 == s2


def schema_diff(df_expected, df_actual):
    """Return columns where types differ."""
    expected = {f.name: str(f.dataType) for f in df_expected.schema.fields}
    actual = {f.name: str(f.dataType) for f in df_actual.schema.fields}
    
    mismatches = []
    for col_name, expected_type in expected.items():
        if col_name in actual and actual[col_name] != expected_type:
            mismatches.append({
                "column": col_name,
                "expected": expected_type,
                "actual": actual[col_name],
            })
    return mismatches
```

---

## Schema Validation Before Writes

Always validate schema before writing to critical tables:

```python
def validate_schema_before_write(df, expected_schema: StructType, table_name: str):
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    errors = []
    
    # Check for missing required columns
    missing = set(expected_fields) - set(actual_fields)
    if missing:
        errors.append(f"Missing columns: {missing}")
    
    # Check for type mismatches
    for col_name in set(expected_fields) & set(actual_fields):
        if expected_fields[col_name] != actual_fields[col_name]:
            errors.append(
                f"Column '{col_name}': expected {expected_fields[col_name]}, "
                f"got {actual_fields[col_name]}"
            )
    
    if errors:
        raise ValueError(f"Schema validation failed for '{table_name}':\n" + "\n".join(errors))
    
    print(f"[OK] Schema validation passed for '{table_name}'")


# Usage
validate_schema_before_write(transformed_df, EXPECTED_SCHEMA, "gold.users")
transformed_df.write.format("delta").mode("append").save("/delta/gold/users")
```

---

## Key Takeaways

- Delta's `mergeSchema` handles additive changes; `overwriteSchema` replaces everything — use both deliberately.
- `from_json` / `to_json` are essential for working with Kafka topics, event buses, and raw log tables that store JSON as strings.
- Schema drift detection should run before any write to a production table.
- Store schemas as JSON and reload with `StructType.fromJson()` for deterministic reads.
- Schema validation is a first-class quality gate — treat schema mismatches as pipeline failures.
