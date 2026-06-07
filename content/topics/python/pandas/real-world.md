---
title: "Python Pandas - Real World"
topic: python
subtopic: pandas
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, pandas, production, data-quality, etl]
---

# Python Pandas — Real World Production Patterns

## Production Pandas for Data Engineering

These patterns represent how Pandas is actually used in production pipelines — with error handling, monitoring, and awareness of when to graduate to bigger tools.

---

## Pattern 1: Data Quality Profiling Framework

A reusable profiling framework that generates quality metrics for any DataFrame.

```python
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    total_count: int
    null_count: int
    null_pct: float
    unique_count: int
    unique_pct: float
    min_val: str | None = None
    max_val: str | None = None
    mean_val: float | None = None
    std_val: float | None = None
    top_values: dict | None = None

@dataclass
class DataQualityReport:
    table_name: str
    row_count: int
    column_count: int
    profiled_at: datetime
    columns: list[ColumnProfile]
    warnings: list[str] = field(default_factory=list)

class DataProfiler:
    """Production data quality profiler using Pandas."""
    
    def __init__(self, null_threshold: float = 0.1, cardinality_threshold: float = 0.95):
        self.null_threshold = null_threshold
        self.cardinality_threshold = cardinality_threshold
    
    def profile(self, df: pd.DataFrame, table_name: str) -> DataQualityReport:
        """Generate a full quality profile for a DataFrame."""
        columns = []
        warnings = []
        
        for col in df.columns:
            profile = self._profile_column(df[col])
            columns.append(profile)
            
            # Generate warnings
            if profile.null_pct > self.null_threshold:
                warnings.append(f"Column '{col}' has {profile.null_pct:.1%} nulls (threshold: {self.null_threshold:.0%})")
            
            if profile.unique_pct > self.cardinality_threshold and profile.dtype == "object":
                warnings.append(f"Column '{col}' may be a unique identifier ({profile.unique_pct:.1%} unique)")
            
            if profile.unique_count == 1:
                warnings.append(f"Column '{col}' is constant (single value)")
        
        return DataQualityReport(
            table_name=table_name,
            row_count=len(df),
            column_count=len(df.columns),
            profiled_at=datetime.utcnow(),
            columns=columns,
            warnings=warnings,
        )
    
    def _profile_column(self, series: pd.Series) -> ColumnProfile:
        """Profile a single column."""
        total = len(series)
        null_count = series.isna().sum()
        non_null = series.dropna()
        
        profile = ColumnProfile(
            name=series.name,
            dtype=str(series.dtype),
            total_count=total,
            null_count=int(null_count),
            null_pct=null_count / total if total > 0 else 0,
            unique_count=series.nunique(),
            unique_pct=series.nunique() / total if total > 0 else 0,
        )
        
        if pd.api.types.is_numeric_dtype(series):
            profile.min_val = str(non_null.min()) if len(non_null) > 0 else None
            profile.max_val = str(non_null.max()) if len(non_null) > 0 else None
            profile.mean_val = float(non_null.mean()) if len(non_null) > 0 else None
            profile.std_val = float(non_null.std()) if len(non_null) > 0 else None
        
        if series.nunique() <= 20:
            profile.top_values = series.value_counts().head(10).to_dict()
        
        return profile
    
    def compare_profiles(self, current: DataQualityReport, previous: DataQualityReport) -> list[str]:
        """Detect drift between two profiles."""
        drifts = []
        
        if abs(current.row_count - previous.row_count) / max(previous.row_count, 1) > 0.5:
            drifts.append(f"Row count changed significantly: {previous.row_count} → {current.row_count}")
        
        current_cols = {c.name for c in current.columns}
        previous_cols = {c.name for c in previous.columns}
        
        new_cols = current_cols - previous_cols
        dropped_cols = previous_cols - current_cols
        
        if new_cols:
            drifts.append(f"New columns: {new_cols}")
        if dropped_cols:
            drifts.append(f"Dropped columns: {dropped_cols}")
        
        return drifts

# Usage
profiler = DataProfiler(null_threshold=0.05)
df = pd.read_csv("daily_events.csv")
report = profiler.profile(df, "daily_events")

print(f"Rows: {report.row_count}, Columns: {report.column_count}")
print(f"Warnings: {len(report.warnings)}")
for w in report.warnings:
    print(f"  ⚠️ {w}")
```

---

## Pattern 2: Incremental File Processing (Chunks + Transform + Write)

Process files larger than memory by reading in chunks, transforming, and writing incrementally.

```python
import pandas as pd
from pathlib import Path
from typing import Callable
import logging

logger = logging.getLogger(__name__)

class IncrementalProcessor:
    """Process large files in chunks without loading everything into memory."""
    
    def __init__(self, chunk_size: int = 50_000):
        self.chunk_size = chunk_size
        self.total_input = 0
        self.total_output = 0
    
    def process_file(
        self,
        input_path: str,
        output_path: str,
        transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
        output_format: str = "parquet",
        read_kwargs: dict = None,
    ) -> dict:
        """Read → Transform → Write in chunks."""
        read_kwargs = read_kwargs or {}
        first_chunk = True
        
        for i, chunk in enumerate(pd.read_csv(input_path, chunksize=self.chunk_size, **read_kwargs)):
            self.total_input += len(chunk)
            
            # Apply transformation
            transformed = transform_fn(chunk)
            self.total_output += len(transformed)
            
            # Write incrementally
            if output_format == "parquet":
                # Parquet: append to a directory of files
                chunk_path = Path(output_path) / f"part_{i:05d}.parquet"
                chunk_path.parent.mkdir(parents=True, exist_ok=True)
                transformed.to_parquet(chunk_path, index=False)
            elif output_format == "csv":
                mode = "w" if first_chunk else "a"
                header = first_chunk
                transformed.to_csv(output_path, mode=mode, header=header, index=False)
            
            first_chunk = False
            
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {self.total_input:,} rows → {self.total_output:,} output rows")
        
        return {
            "input_rows": self.total_input,
            "output_rows": self.total_output,
            "filter_ratio": self.total_output / self.total_input if self.total_input > 0 else 0,
        }

# Define transformation
def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    """Transform applied to each chunk independently."""
    return (
        df
        .assign(
            event_date=pd.to_datetime(df["timestamp"], unit="s"),
            amount=pd.to_numeric(df["amount"], errors="coerce"),
        )
        .query("amount > 0")
        .dropna(subset=["user_id", "amount"])
        .assign(
            year_month=lambda d: d["event_date"].dt.to_period("M").astype(str),
        )
        [["user_id", "event_date", "year_month", "amount", "event_type"]]
    )

# Process a 50GB file using only ~200MB RAM
processor = IncrementalProcessor(chunk_size=100_000)
result = processor.process_file(
    input_path="raw_events_2024.csv",
    output_path="processed/events/",
    transform_fn=transform_events,
    output_format="parquet",
    read_kwargs={"dtype": {"user_id": "int32", "amount": "str"}},
)
print(f"Processed: {result}")
```

---

## Pattern 3: Multi-Source Data Reconciliation

Compare data across multiple sources to identify discrepancies — a common data quality task.

```python
import pandas as pd
from dataclasses import dataclass

@dataclass
class ReconciliationResult:
    source_a_name: str
    source_b_name: str
    matched: int
    only_in_a: int
    only_in_b: int
    value_mismatches: int
    mismatch_details: pd.DataFrame

class DataReconciler:
    """Reconcile records between two data sources."""
    
    def __init__(self, key_columns: list[str], compare_columns: list[str], tolerance: float = 0.01):
        self.key_columns = key_columns
        self.compare_columns = compare_columns
        self.tolerance = tolerance
    
    def reconcile(self, source_a: pd.DataFrame, source_b: pd.DataFrame,
                  name_a: str = "source_a", name_b: str = "source_b") -> ReconciliationResult:
        """Compare two DataFrames and find discrepancies."""
        
        # Outer join on key columns
        merged = pd.merge(
            source_a, source_b,
            on=self.key_columns,
            how="outer",
            suffixes=(f"_{name_a}", f"_{name_b}"),
            indicator=True,
        )
        
        # Categorize records
        only_a = merged[merged["_merge"] == "left_only"]
        only_b = merged[merged["_merge"] == "right_only"]
        both = merged[merged["_merge"] == "both"]
        
        # Check value mismatches for records in both
        mismatches = []
        for col in self.compare_columns:
            col_a = f"{col}_{name_a}"
            col_b = f"{col}_{name_b}"
            
            if col_a in both.columns and col_b in both.columns:
                if pd.api.types.is_numeric_dtype(source_a[col]):
                    # Numeric: use tolerance
                    diff = (both[col_a] - both[col_b]).abs()
                    mismatch_mask = diff > self.tolerance
                else:
                    # String: exact match
                    mismatch_mask = both[col_a] != both[col_b]
                
                mismatched_rows = both[mismatch_mask][self.key_columns + [col_a, col_b]].copy()
                mismatched_rows["mismatched_column"] = col
                mismatches.append(mismatched_rows)
        
        mismatch_df = pd.concat(mismatches, ignore_index=True) if mismatches else pd.DataFrame()
        
        return ReconciliationResult(
            source_a_name=name_a,
            source_b_name=name_b,
            matched=len(both) - len(mismatch_df),
            only_in_a=len(only_a),
            only_in_b=len(only_b),
            value_mismatches=len(mismatch_df),
            mismatch_details=mismatch_df,
        )

# Usage: Reconcile warehouse vs source system
reconciler = DataReconciler(
    key_columns=["order_id"],
    compare_columns=["amount", "status", "customer_id"],
    tolerance=0.01,
)

warehouse_df = pd.read_sql("SELECT * FROM orders WHERE date = '2024-01-15'", warehouse_conn)
source_df = pd.read_sql("SELECT * FROM orders WHERE date = '2024-01-15'", source_conn)

result = reconciler.reconcile(warehouse_df, source_df, "warehouse", "source_system")
print(f"Matched: {result.matched}, Only in warehouse: {result.only_in_a}, "
      f"Only in source: {result.only_in_b}, Mismatches: {result.value_mismatches}")
```

---

## Pattern 4: Automated Data Type Inference and Schema Validation

Infer optimal schema from data and validate incoming records against it.

```python
import pandas as pd
import numpy as np
from dataclasses import dataclass, field

@dataclass
class InferredSchema:
    columns: dict[str, dict] = field(default_factory=dict)

class SchemaInferrer:
    """Infer and validate schemas from Pandas DataFrames."""
    
    def infer(self, df: pd.DataFrame, sample_size: int = 10_000) -> InferredSchema:
        """Infer schema from a DataFrame sample."""
        sample = df.head(sample_size) if len(df) > sample_size else df
        schema = InferredSchema()
        
        for col in sample.columns:
            series = sample[col]
            col_schema = {
                "inferred_type": self._infer_type(series),
                "nullable": bool(series.isna().any()),
                "unique_ratio": series.nunique() / len(series),
            }
            
            if pd.api.types.is_numeric_dtype(series):
                col_schema["min"] = float(series.min())
                col_schema["max"] = float(series.max())
                col_schema["mean"] = float(series.mean())
            
            if col_schema["inferred_type"] == "datetime":
                non_null = series.dropna()
                col_schema["min_date"] = str(pd.to_datetime(non_null).min())
                col_schema["max_date"] = str(pd.to_datetime(non_null).max())
            
            schema.columns[col] = col_schema
        
        return schema
    
    def _infer_type(self, series: pd.Series) -> str:
        """Infer the semantic type of a column."""
        if pd.api.types.is_integer_dtype(series):
            return "integer"
        if pd.api.types.is_float_dtype(series):
            return "float"
        if pd.api.types.is_bool_dtype(series):
            return "boolean"
        
        # String analysis
        non_null = series.dropna().astype(str)
        if len(non_null) == 0:
            return "unknown"
        
        sample = non_null.head(100)
        
        # Try datetime
        try:
            pd.to_datetime(sample)
            return "datetime"
        except (ValueError, TypeError):
            pass
        
        # Try numeric string
        try:
            pd.to_numeric(sample)
            return "numeric_string"
        except (ValueError, TypeError):
            pass
        
        return "string"
    
    def validate(self, df: pd.DataFrame, schema: InferredSchema) -> list[str]:
        """Validate a DataFrame against an inferred schema."""
        violations = []
        
        # Check for missing columns
        expected_cols = set(schema.columns.keys())
        actual_cols = set(df.columns)
        
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        
        if missing:
            violations.append(f"Missing columns: {missing}")
        if extra:
            violations.append(f"Unexpected columns: {extra}")
        
        # Check constraints
        for col, rules in schema.columns.items():
            if col not in df.columns:
                continue
            
            if not rules["nullable"] and df[col].isna().any():
                null_count = df[col].isna().sum()
                violations.append(f"Column '{col}' has {null_count} nulls but should be non-nullable")
            
            if "min" in rules and "max" in rules:
                numeric_vals = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(numeric_vals) > 0:
                    actual_min = numeric_vals.min()
                    actual_max = numeric_vals.max()
                    if actual_min < rules["min"] * 0.5:
                        violations.append(f"Column '{col}' min {actual_min} below expected range")
                    if actual_max > rules["max"] * 2.0:
                        violations.append(f"Column '{col}' max {actual_max} above expected range")
        
        return violations

# Usage
inferrer = SchemaInferrer()

# Infer from historical data
historical_df = pd.read_parquet("historical_data.parquet")
schema = inferrer.infer(historical_df)

# Validate new incoming data
new_df = pd.read_csv("today_data.csv")
violations = inferrer.validate(new_df, schema)
if violations:
    for v in violations:
        print(f"  ❌ {v}")
else:
    print("  ✓ Schema validation passed")
```

---

## When to Move from Pandas to Spark

| Signal | What's Happening | Action |
|--------|-----------------|--------|
| Processing time > 30 min | Pandas is single-threaded | Consider Polars first, then Spark |
| OOM crashes | Data > 70% of RAM | Chunked processing or Spark |
| Multiple joins on large tables | Memory explosion | Spark broadcast joins |
| Daily data growth > 10% | Will outgrow machine soon | Plan Spark migration |
| Need distributed writes | S3/HDFS parallelism | Spark native |

**Migration path:** Pandas → Polars (same machine, 10× faster) → Spark (distributed)

---

## Interview Tips

> **Tip 1:** For data quality questions, show you have a systematic approach: "I profile every dataset on ingestion — null rates, cardinality, value distributions. Then I compare today's profile against yesterday's to detect drift. This catches silent schema changes that break downstream." This shows production thinking.

> **Tip 2:** The reconciliation pattern demonstrates you understand data pipeline testing. Say: "I reconcile source against warehouse daily. If mismatches exceed 0.1%, I alert the team. Common causes: late-arriving data, timezone issues, or float precision in aggregations."

> **Tip 3:** For the "when to leave Pandas" question, don't just say "when data is big." Give the decision framework: "If I can optimize dtypes and use chunking, I stay with Pandas. If I need parallelism on one machine, I try Polars. I only move to Spark when I need distributed processing or the data literally can't fit on one machine."
