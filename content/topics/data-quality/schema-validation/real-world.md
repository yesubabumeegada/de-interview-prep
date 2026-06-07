---
title: "Schema Validation — Real World"
topic: data-quality
subtopic: schema-validation
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [schema-validation, production, pipeline, migration]
---

# Schema Validation — Real World Patterns

## Pattern 1: Schema Validation Gate in Spark ETL

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sys

EXPECTED_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("order_date", TimestampType(), nullable=False),
])

def run_etl(input_path: str, output_path: str, run_date: str):
    spark = SparkSession.builder.appName("orders_etl").getOrCreate()
    
    # Read with explicit schema — malformed rows go to _corrupt_record
    df = spark.read.schema(EXPECTED_SCHEMA).option(
        "columnNameOfCorruptRecord", "_corrupt_record"
    ).parquet(input_path)
    
    # Separate corrupt records
    corrupt = df.filter(df["_corrupt_record"].isNotNull())
    clean = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
    
    corrupt_count = corrupt.count()
    clean_count = clean.count()
    total = corrupt_count + clean_count
    
    print(f"Total: {total}, Clean: {clean_count}, Corrupt: {corrupt_count}")
    
    # Fail if too many corrupt records
    if total > 0 and corrupt_count / total > 0.01:  # >1% corrupt
        corrupt.write.mode("append").json(f"s3://quarantine/orders/dt={run_date}/")
        raise ValueError(f"Schema validation failed: {corrupt_count}/{total} corrupt records")
    
    # Write quarantine for non-fatal corrupt records
    if corrupt_count > 0:
        corrupt.write.mode("append").json(f"s3://quarantine/orders/dt={run_date}/")
    
    # Process clean records
    clean.write.mode("overwrite").partitionBy("order_date").parquet(output_path)
    
    return {"clean": clean_count, "corrupt": corrupt_count}
```

---

## Pattern 2: Schema Version Header in Batch Files

```python
import json
import pandas as pd
from pathlib import Path

SCHEMA_VERSIONS = {
    "1.0": {"required": ["order_id", "cust_id", "amount"], "optional": ["status"]},
    "2.0": {"required": ["order_id", "customer_id", "amount", "status"], "optional": []},
}

def read_with_schema_version(filepath: str) -> pd.DataFrame:
    """Read a file that embeds schema version in filename or header."""
    
    # Convention: filename includes version, e.g., orders_v2.0_20240115.parquet
    filename = Path(filepath).stem
    version = None
    for v in SCHEMA_VERSIONS:
        if f"v{v}" in filename:
            version = v
            break
    
    if version is None:
        version = "1.0"  # Default to oldest version
        print(f"Warning: No version in filename, assuming v{version}")
    
    df = pd.read_parquet(filepath)
    schema_def = SCHEMA_VERSIONS[version]
    
    # Validate required columns
    missing = set(schema_def["required"]) - set(df.columns)
    if missing:
        raise ValueError(f"v{version} schema: missing required columns {missing}")
    
    # Normalize to current schema (v2.0)
    if version == "1.0" and "cust_id" in df.columns:
        df = df.rename(columns={"cust_id": "customer_id"})
        if "status" not in df.columns:
            df["status"] = "unknown"  # Default for v1 records
    
    return df
```

---

## Pattern 3: Automated Schema Documentation

```python
import pandas as pd
from dataclasses import dataclass
from typing import Optional
import json

@dataclass
class ColumnDoc:
    name: str
    dtype: str
    nullable: bool
    unique_count: int
    null_count: int
    null_pct: float
    sample_values: list
    min_val: Optional[float] = None
    max_val: Optional[float] = None

def generate_schema_doc(df: pd.DataFrame, table_name: str) -> dict:
    """Generate schema documentation from a DataFrame."""
    columns = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        doc = ColumnDoc(
            name=col,
            dtype=str(df[col].dtype),
            nullable=null_count > 0,
            unique_count=int(df[col].nunique()),
            null_count=null_count,
            null_pct=round(null_count / len(df) * 100, 2),
            sample_values=df[col].dropna().sample(min(5, df[col].notna().sum())).tolist(),
        )
        if pd.api.types.is_numeric_dtype(df[col]):
            doc.min_val = float(df[col].min())
            doc.max_val = float(df[col].max())
        columns.append(doc.__dict__)
    
    return {
        "table_name": table_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": columns,
        "generated_at": pd.Timestamp.utcnow().isoformat(),
    }

# Save schema doc
doc = generate_schema_doc(orders_df, "orders")
with open("schema_docs/orders.json", "w") as f:
    json.dump(doc, f, indent=2, default=str)
```

---

## Common Schema Pitfalls

| Pitfall | Issue | Fix |
|---|---|---|
| Reading without schema in Spark | Types inferred wrong (int → bigint, date → string) | Always specify schema at read time |
| Nullable vs required not enforced | Nulls slip through | Add explicit null checks after reading |
| Schema stored only in code | Drift goes undetected | Register schema in catalog/registry |
| No schema versioning | Breaking changes break consumers silently | Semantic version all schemas |
| Assuming CSV column order | Column order changes break parsers | Always read by name, not position |
