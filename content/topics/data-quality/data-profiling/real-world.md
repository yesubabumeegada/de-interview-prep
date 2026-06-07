---
title: "Data Profiling — Real World"
topic: data-quality
subtopic: data-profiling
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-profiling, production, onboarding, migration, comparison]
---

# Data Profiling — Real World Patterns

## Pattern 1: New Table Onboarding Workflow

```python
from ydata_profiling import ProfileReport
import pandas as pd
import json
from pathlib import Path

def onboard_new_table(
    df: pd.DataFrame,
    table_name: str,
    output_dir: str = "profiles/",
) -> dict:
    """
    Complete onboarding workflow for a new data source.
    Returns profile summary and suggested DQ rules.
    """
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 1. Generate HTML report
    profile = ProfileReport(df, title=f"{table_name} Profile", minimal=False)
    profile.to_file(f"{output_dir}/{table_name}_profile.html")
    
    # 2. Extract machine-readable summary
    desc = profile.get_description()
    
    summary = {
        "table_name": table_name,
        "row_count": desc["table"]["n"],
        "column_count": desc["table"]["n_var"],
        "missing_cells_pct": round(desc["table"]["p_cells_missing"] * 100, 2),
        "duplicate_rows": desc["table"]["n_duplicates"],
        "suggested_rules": [],
    }
    
    # 3. Auto-generate suggested rules
    for col, var_stats in desc["variables"].items():
        if var_stats.get("p_missing", 0) == 0:
            summary["suggested_rules"].append(
                f"expect_column_values_to_not_be_null('{col}')"
            )
        
        if var_stats.get("p_unique", 0) > 0.99:
            summary["suggested_rules"].append(
                f"expect_column_values_to_be_unique('{col}')  # likely PK"
            )
        
        if var_stats.get("type") == "Categorical":
            n_cat = var_stats.get("n_distinct", 0)
            if n_cat <= 20:
                summary["suggested_rules"].append(
                    f"expect_column_values_to_be_in_set('{col}', [...])  # {n_cat} categories"
                )
    
    # 4. Save summary
    with open(f"{output_dir}/{table_name}_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"✓ Profile saved: {output_dir}/{table_name}_profile.html")
    print(f"✓ {len(summary['suggested_rules'])} DQ rules suggested")
    
    return summary
```

---

## Pattern 2: Pre/Post Migration Comparison

```python
import pandas as pd
import numpy as np
from typing import List

def compare_profiles(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    tolerance_pct: float = 1.0,
) -> List[dict]:
    """
    Compare profiles of source and target DataFrames.
    Used to validate data migrations.
    Returns list of discrepancies.
    """
    
    discrepancies = []
    
    # Row count
    row_delta_pct = abs(len(source_df) - len(target_df)) / max(len(source_df), 1) * 100
    if row_delta_pct > tolerance_pct:
        discrepancies.append({
            "check": "row_count",
            "source": len(source_df),
            "target": len(target_df),
            "delta_pct": round(row_delta_pct, 2),
            "status": "FAIL",
        })
    
    # Per-column checks
    for col in source_df.columns:
        if col not in target_df.columns:
            discrepancies.append({"check": f"{col}_exists", "status": "FAIL", "detail": "Missing in target"})
            continue
        
        # Null rate
        src_null = source_df[col].isna().mean() * 100
        tgt_null = target_df[col].isna().mean() * 100
        if abs(src_null - tgt_null) > tolerance_pct:
            discrepancies.append({
                "check": f"{col}_null_rate",
                "source_null_pct": round(src_null, 2),
                "target_null_pct": round(tgt_null, 2),
                "status": "FAIL",
            })
        
        # Numeric distributions
        if pd.api.types.is_numeric_dtype(source_df[col]):
            for metric, fn in [("mean", lambda s: s.mean()), ("std", lambda s: s.std())]:
                src_val = fn(source_df[col].dropna())
                tgt_val = fn(target_df[col].dropna())
                
                if src_val != 0:
                    delta = abs(src_val - tgt_val) / abs(src_val) * 100
                    if delta > tolerance_pct:
                        discrepancies.append({
                            "check": f"{col}_{metric}",
                            "source": round(src_val, 4),
                            "target": round(tgt_val, 4),
                            "delta_pct": round(delta, 2),
                            "status": "FAIL" if delta > 5 else "WARN",
                        })
    
    return discrepancies


# Usage
source = pd.read_parquet("s3://source/orders/")
target = pd.read_parquet("s3://target/orders_migrated/")

issues = compare_profiles(source, target, tolerance_pct=0.5)
if any(d["status"] == "FAIL" for d in issues):
    for d in issues:
        print(f"[{d['status']}] {d['check']}: {d}")
    raise ValueError("Migration validation failed — profile comparison found discrepancies")
```

---

## Pattern 3: Scheduled Weekly Profiling Report

```python
# Airflow DAG: profile key tables weekly and email the report
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ydata_profiling import ProfileReport
import pandas as pd

TABLES_TO_PROFILE = ["orders", "customers", "products", "payments"]

def profile_table(table_name: str, **context):
    import sqlalchemy as sa
    engine = sa.create_engine("postgresql://user:pass@host/db")
    
    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 500000", engine)
    
    profile = ProfileReport(df, minimal=True, title=f"{table_name} Weekly Profile")
    profile.to_file(f"/tmp/{table_name}_profile.html")
    
    desc = profile.get_description()
    return {
        "table": table_name,
        "rows": desc["table"]["n"],
        "missing_pct": desc["table"]["p_cells_missing"],
    }

with DAG("weekly_profiling", start_date=datetime(2024, 1, 1), schedule="@weekly") as dag:
    for table in TABLES_TO_PROFILE:
        PythonOperator(
            task_id=f"profile_{table}",
            python_callable=profile_table,
            op_kwargs={"table_name": table},
        )
```

---

## Profiling Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Profile on all data | Too slow, costly for large datasets | Sample 10-20%, use Spark for distributed |
| Profile once, never repeat | Drift goes undetected | Profile every batch, store metrics |
| Ignore profiling output | Rules based on assumptions | Rules derived from actual profile |
| Share raw profile reports with all teams | PII exposure in top-values | Mask PII columns in report |
| Profile without business context | Can't interpret findings | Profile with a domain expert present |
