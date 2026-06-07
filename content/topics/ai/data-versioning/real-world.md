---
title: "AI - Data Versioning"
topic: ai
subtopic: data-versioning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ai, data-versioning, audit, lineage, petabyte-scale, dataset-rollback]
---

# Data Versioning — Real World Patterns

## Training Data Lineage for Audit

In regulated industries, you must be able to answer: "What data was this model trained on, and who was in it?"

```python
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
import hashlib, json

@dataclass
class DatasetAuditRecord:
    """Immutable audit record for a training dataset."""
    
    dataset_id: str              # Unique identifier
    dataset_name: str
    version: str
    created_at: datetime
    created_by: str              # User or pipeline
    
    # Source provenance
    source_tables: List[dict]    # Each: {"table": "...", "database": "...", "row_count": N}
    sql_query: str               # Exact query used to extract data
    extraction_timestamp: str    # When data was extracted
    
    # Content fingerprinting
    row_count: int
    sha256_hash: str             # Hash of the full dataset
    column_hashes: Dict[str, str]  # Per-column hash for partial verification
    
    # Transformation lineage
    transformations_applied: List[str]  # e.g., ["StandardScaler", "OHE on plan_type"]
    
    # Data governance
    contains_pii: bool
    pii_handling: str            # "pseudonymized", "excluded", "raw"
    gdpr_region: str             # "EU", "US", "global"
    retention_expires_at: Optional[datetime] = None
    
    def to_audit_json(self) -> str:
        """Serialize to immutable JSON for audit storage."""
        record = {
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "row_count": self.row_count,
            "sha256_hash": self.sha256_hash,
            "source_tables": self.source_tables,
            "extraction_timestamp": self.extraction_timestamp,
            "transformations": self.transformations_applied,
            "pii_handling": self.pii_handling,
        }
        # Include hash of the record itself (tamper detection)
        record_str = json.dumps(record, sort_keys=True)
        record["record_hash"] = hashlib.sha256(record_str.encode()).hexdigest()
        return json.dumps(record, indent=2)


def create_audit_record(df, dataset_name: str, sql_query: str, source_tables: list) -> DatasetAuditRecord:
    """Create an audit record for a training dataset."""
    import pandas as pd
    
    # Compute dataset fingerprint
    # In practice: sample-based hashing for very large datasets
    dataset_bytes = df.to_parquet()
    sha256 = hashlib.sha256(dataset_bytes).hexdigest()
    
    # Per-column hashes (faster to verify individual columns)
    col_hashes = {}
    for col in df.columns:
        col_bytes = df[col].to_json().encode()
        col_hashes[col] = hashlib.md5(col_bytes).hexdigest()[:16]
    
    return DatasetAuditRecord(
        dataset_id=sha256[:16],
        dataset_name=dataset_name,
        version="1.0.0",
        created_at=datetime.utcnow(),
        created_by="training-pipeline-v3",
        source_tables=source_tables,
        sql_query=sql_query,
        extraction_timestamp=datetime.utcnow().isoformat(),
        row_count=len(df),
        sha256_hash=sha256,
        column_hashes=col_hashes,
        transformations_applied=[],
        contains_pii=False,
        pii_handling="pseudonymized",
        gdpr_region="US",
    )
```

---

## Rolling Back Datasets

```python
from delta import DeltaTable
from pyspark.sql import SparkSession
import mlflow

class DatasetRollbackManager:
    """
    Manages dataset rollback when a bad data batch is discovered.
    Uses Delta Lake time travel.
    """
    
    def __init__(self, delta_path: str, spark: SparkSession):
        self.path = delta_path
        self.spark = spark
        self.dt = DeltaTable.forPath(spark, delta_path)
    
    def get_version_history(self, n_versions: int = 20) -> "DataFrame":
        """Show recent version history."""
        history = self.dt.history(n_versions)
        return history.select(
            "version", "timestamp", "operation",
            "operationParameters.mode",
            "userMetadata",
            "numOutputRows"
        )
    
    def find_last_good_version(self, cutoff_timestamp: str) -> int:
        """Find the last version before a known-bad batch was written."""
        history = self.dt.history(100)
        
        good_versions = history.filter(
            history.timestamp < cutoff_timestamp
        ).orderBy("version", ascending=False)
        
        if good_versions.count() == 0:
            raise ValueError(f"No versions before {cutoff_timestamp}")
        
        return good_versions.select("version").collect()[0][0]
    
    def rollback_to_version(self, target_version: int, dry_run: bool = False) -> dict:
        """
        Rollback training data to a specific version.
        Deletes records added after target_version.
        """
        
        # Get data at target version
        good_data = self.spark.read.format("delta") \
            .option("versionAsOf", target_version) \
            .load(self.path)
        
        current_data = self.spark.read.format("delta").load(self.path)
        
        print(f"Current: {current_data.count():,} rows")
        print(f"Target (v{target_version}): {good_data.count():,} rows")
        print(f"Rows to remove: {current_data.count() - good_data.count():,}")
        
        if dry_run:
            print("DRY RUN — no changes made")
            return {"dry_run": True, "target_version": target_version}
        
        # Overwrite with good version
        (good_data.write
         .format("delta")
         .mode("overwrite")
         .option("userMetadata", f"Rollback to version {target_version}")
         .save(self.path))
        
        new_version = self.dt.history(1).select("version").collect()[0][0]
        print(f"Rollback complete. New version: {new_version}")
        
        return {
            "rolled_back_to": target_version,
            "new_version": new_version,
            "rows_after_rollback": good_data.count(),
        }
    
    def quarantine_bad_rows(
        self,
        bad_condition: str,  # SQL WHERE clause
        quarantine_path: str,
    ) -> int:
        """
        Move bad rows to quarantine instead of deleting.
        Preserves data for investigation.
        """
        
        current = self.spark.read.format("delta").load(self.path)
        
        bad_rows = current.filter(bad_condition)
        n_bad = bad_rows.count()
        
        print(f"Quarantining {n_bad:,} rows matching: {bad_condition}")
        
        # Save bad rows for investigation
        (bad_rows.write
         .format("delta")
         .mode("append")
         .option("userMetadata", f"Quarantined: {bad_condition}")
         .save(quarantine_path))
        
        # Remove from active training data
        self.dt.delete(condition=bad_condition)
        
        print(f"Quarantine complete. Active rows: {self.spark.read.format('delta').load(self.path).count():,}")
        return n_bad
```

---

## Large-Scale Data Versioning (Petabytes)

At petabyte scale, traditional approaches break. Here's how to handle it.

```python
# Approximate hashing for petabyte datasets
# Full SHA256 is infeasible — use sampling-based fingerprinting

import numpy as np
import hashlib
from pyspark.sql import functions as F

def approximate_dataset_fingerprint(spark_df, sample_fraction: float = 0.001) -> str:
    """
    Compute dataset fingerprint from a sample.
    At 1TB scale: 0.1% sample = 1GB — still fast.
    At 1PB scale: 0.001% sample = 10GB — acceptable.
    """
    # Deterministic sample (seed ensures reproducibility)
    sampled = spark_df.sample(fraction=sample_fraction, seed=42)
    
    # Convert to pandas for hashing
    sample_pd = sampled.toPandas()
    sample_bytes = sample_pd.to_json().encode()
    
    fingerprint = hashlib.sha256(sample_bytes).hexdigest()
    return f"approx-{fingerprint[:16]}-sample_{sample_fraction}"


class PetabyteDataVersionManager:
    """
    Data versioning strategy for petabyte-scale ML training data.
    Conventional approaches (DVC, full copies) don't work at this scale.
    """
    
    def __init__(self, delta_base_path: str, spark: SparkSession):
        self.base_path = delta_base_path
        self.spark = spark
    
    def create_logical_version(
        self,
        version_name: str,
        filter_condition: str,  # SQL WHERE clause defining this dataset version
        description: str,
    ) -> dict:
        """
        Create a logical dataset version WITHOUT copying data.
        At petabyte scale, copying is prohibitively expensive.
        Instead, store the filter predicate that defines the dataset.
        """
        
        # Get current Delta table version (for time travel reference)
        dt = DeltaTable.forPath(self.spark, self.base_path)
        current_version = dt.history(1).select("version").collect()[0][0]
        
        version_metadata = {
            "version_name": version_name,
            "description": description,
            "created_at": datetime.utcnow().isoformat(),
            "delta_table_path": self.base_path,
            "delta_version": current_version,   # Pin to current Delta version
            "filter_condition": filter_condition,  # What subset of data
            "fingerprint": self._compute_fingerprint(filter_condition, current_version),
        }
        
        # Store version metadata (tiny — not the data itself)
        version_path = f"{self.base_path}/.versions/{version_name}.json"
        with open(version_path, "w") as f:
            json.dump(version_metadata, f, indent=2)
        
        return version_metadata
    
    def materialize_version(self, version_name: str) -> "DataFrame":
        """
        Reconstruct dataset version on demand.
        Uses Delta time travel + stored filter to rebuild exact dataset.
        """
        
        version_path = f"{self.base_path}/.versions/{version_name}.json"
        with open(version_path) as f:
            metadata = json.load(f)
        
        return (
            self.spark.read.format("delta")
            .option("versionAsOf", metadata["delta_version"])
            .load(self.base_path)
            .filter(metadata["filter_condition"])
        )
    
    def _compute_fingerprint(self, filter_condition: str, delta_version: int) -> str:
        content = f"{self.base_path}:{delta_version}:{filter_condition}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
```

---

## Interview Tips

> **Tip 1:** "How would you implement GDPR right-to-erasure for training data?" — "This is one of the hardest ML compliance problems. Three approaches: (1) Pseudonymization — replace PII with deterministic hashes at ingest time, so there's no PII to erase from training data; (2) Influence-function-based forgetting — compute and subtract the influence of the deleted user's data from model weights (theoretically elegant but computationally expensive); (3) Scheduled retrain — don't erase retroactively, but exclude the user from the next training run and sunset the current model within SLA. Most companies use approach 3."

> **Tip 2:** "How do you version petabyte-scale training datasets?" — "You can't store full copies — even 0.1% overhead at 1PB is 1TB. Use logical versioning: store the definition (Delta table path + version number + filter predicate) rather than the data. Delta time travel lets you reconstruct any past version. The metadata record is a few kilobytes; the data stays in place. Use approximate fingerprinting (sampling) for integrity checks."

> **Tip 3:** "What's the difference between dataset rollback and quarantine?" — "Rollback reverts the entire dataset to a prior version — appropriate when an entire batch is bad. Quarantine moves specific bad rows to a separate location for investigation without losing them — appropriate when only a subset of rows is problematic (e.g., a specific merchant's data is corrupted). Quarantine is preferred because you preserve the problematic data for root cause analysis."

> **Tip 4:** "How do data contracts protect ML models from upstream changes?" — "Contracts are validated on every pipeline run before training starts. If the upstream team renames 'user_age' to 'age_years', the contract validator detects the missing column and fails the pipeline loudly — instead of silently defaulting to 0. This converts silent model degradation into a loud, attributable pipeline failure, which is much easier to debug and fix."
