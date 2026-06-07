---
title: "AI - Data Versioning"
topic: ai
subtopic: data-versioning
content_type: scenario_question
difficulty_level: junior
layer: scenarios
tags: [ai, data-versioning, scenarios, drift-tracing, reproducibility, audit]
---

# Data Versioning — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Dataset Drift Tracing

Your churn model's accuracy dropped 8% last week. You suspect the training data changed but no one on the team made any intentional changes. The data pipeline pulls from a transactional database using a daily ETL job. How do you investigate and what tools would help in the future?

<details>
<summary>💡 Hint</summary>

Even without formal data versioning, there may be clues: ETL logs, S3 object metadata (when was each file last modified?), database change logs. Think about what forensic evidence is available and what you would implement to make this investigation easy in the future.

</details>

<details>
<summary>✅ Solution</summary>

### Immediate Investigation

```python
import boto3
import pandas as pd
from datetime import datetime, timedelta

s3 = boto3.client("s3")

# Check when training data files were last modified
response = s3.list_objects_v2(
    Bucket="ml-training-data",
    Prefix="churn/processed/"
)

print("File modification history:")
for obj in sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True):
    print(f"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')}: {obj['Key']} ({obj['Size']:,} bytes)")

# Output might reveal: a file changed 7 days ago — coincides with accuracy drop
```

```python
# Compare feature distributions between old and new data
import pandas as pd
import numpy as np
from scipy.stats import ks_2samp

# Load current data and data from 2 weeks ago (if both exist in S3)
current = pd.read_parquet("s3://ml-data/churn/processed/latest/train.parquet")

# Check S3 versioning for old version
s3 = boto3.client("s3")
versions = s3.list_object_versions(
    Bucket="ml-data",
    Prefix="churn/processed/latest/train.parquet"
)

# Get version from 2 weeks ago
two_weeks_ago = datetime.now() - timedelta(weeks=2)
old_versions = [
    v for v in versions.get("Versions", [])
    if v["LastModified"].replace(tzinfo=None) < two_weeks_ago
]

if old_versions:
    newest_old = max(old_versions, key=lambda v: v["LastModified"])
    old_obj = s3.get_object(
        Bucket="ml-data",
        Key="churn/processed/latest/train.parquet",
        VersionId=newest_old["VersionId"]
    )
    old = pd.read_parquet(old_obj["Body"])
    
    print(f"Current: {len(current):,} rows, Old: {len(old):,} rows")
    
    # Distribution comparison
    for col in current.select_dtypes("number").columns[:10]:
        ks_stat, pval = ks_2samp(old[col].dropna(), current[col].dropna())
        if pval < 0.01:
            mean_shift = current[col].mean() - old[col].mean()
            print(f"SHIFT: {col} — mean shifted by {mean_shift:.4f}, KS p={pval:.4f}")
```

### Root Cause Analysis Template

```python
INVESTIGATION_CHECKLIST = """
Data Drift Investigation Checklist
===================================

1. DATA VOLUME
   [ ] Row count matches expected range?
   [ ] Number of files changed?
   [ ] ETL job duration changed?

2. SCHEMA CHANGES
   [ ] All expected columns present?
   [ ] Any new columns added?
   [ ] Column data types unchanged?

3. UPSTREAM CHANGES
   [ ] Any database migrations in last 30 days?
   [ ] Any new data sources added to ETL?
   [ ] Any business process changes (new product, pricing tier)?

4. DISTRIBUTION SHIFTS
   [ ] KS test or PSI for each feature
   [ ] Class balance (churn rate) stable?
   [ ] Any feature with >20% null rate increase?

5. TEMPORAL ISSUES
   [ ] Is the data fresh (not stale/duplicated)?
   [ ] Date range of training data correct?
   [ ] Any timezone handling changes?
"""
```

### Implementation: DVC for Future-Proofing

```bash
# Setup DVC to version training data going forward
dvc init
dvc remote add -d s3remote s3://ml-data/dvc-store

# Track existing processed data
dvc add data/processed/train.parquet
git add data/processed/train.parquet.dvc
git tag data-v1.0 -m "Pre-incident baseline dataset"
git push --tags
dvc push

# After ETL runs, automatically version new data
# In ETL script:
dvc add data/processed/train.parquet
git add data/processed/train.parquet.dvc
git commit -m "Update training data: $(date +%Y-%m-%d) batch"
git tag "data-$(date +%Y%m%d)"
dvc push
```

```python
# Automated distribution monitoring after each ETL run
def post_etl_validation(new_data_path: str, baseline_path: str):
    """Run after each ETL to detect dataset drift before training."""
    current = pd.read_parquet(new_data_path)
    baseline = pd.read_parquet(baseline_path)
    
    alerts = []
    
    # Row count check
    row_ratio = len(current) / len(baseline)
    if not 0.9 <= row_ratio <= 1.1:
        alerts.append(f"Unusual row count: {len(current):,} (baseline: {len(baseline):,})")
    
    # Feature drift
    for col in current.select_dtypes("number").columns:
        if col in baseline.columns:
            ks_stat, pval = ks_2samp(baseline[col].dropna(), current[col].dropna())
            if pval < 0.001:  # Strict threshold for automated alert
                alerts.append(f"{col}: significant distribution shift (KS p={pval:.4f})")
    
    if alerts:
        print("ALERT: Training data changed significantly:")
        for a in alerts:
            print(f"  - {a}")
        # In production: send to Slack, PagerDuty
        return False
    
    print("Data validation passed")
    return True
```

</details>
</article>

---

<article data-difficulty="mid-level">

## Scenario 2: Reproducing Exact Training Data

A compliance team requests: "Reproduce the exact training data used to train the fraud model v2.3 deployed in March 2023." This model is now generating regulatory scrutiny. Your current data setup: raw data in PostgreSQL, ETL runs daily and overwrites S3 files. No formal data versioning was in place in March 2023. Can you reproduce it?

<details>
<summary>💡 Hint</summary>

If data was overwritten without versioning, exact reproduction may be impossible. But there may be forensic paths: database audit logs, S3 versioning (was it enabled?), the model artifact itself (can you infer anything from it?), ETL job logs. Think about what's recoverable vs what's lost, and what to implement immediately.

</details>

<details>
<summary>✅ Solution</summary>

### Forensic Recovery Attempt

```python
import boto3
from datetime import datetime

class DataRecoveryAttempt:
    """Attempt to recover March 2023 training data from available sources."""
    
    def __init__(self):
        self.s3 = boto3.client("s3")
        self.rds_client = boto3.client("rds")
    
    def check_s3_versioning(self, bucket: str, prefix: str) -> dict:
        """Check if S3 versioning was enabled and if old versions exist."""
        try:
            versioning = self.s3.get_bucket_versioning(Bucket=bucket)
            status = versioning.get("Status", "Disabled")
            
            if status != "Enabled":
                return {"versioning": False, "can_recover": False, "reason": "S3 versioning was disabled"}
            
            # Check for March 2023 versions
            versions = self.s3.list_object_versions(Bucket=bucket, Prefix=prefix)
            
            target_month = "2023-03"
            march_versions = [
                v for v in versions.get("Versions", [])
                if v["LastModified"].strftime("%Y-%m") == target_month
            ]
            
            return {
                "versioning": True,
                "can_recover": len(march_versions) > 0,
                "march_versions": len(march_versions),
                "oldest_version": min(v["LastModified"] for v in versions.get("Versions", [])) if versions.get("Versions") else None,
            }
        except Exception as e:
            return {"versioning": False, "can_recover": False, "error": str(e)}
    
    def check_database_snapshots(self, db_instance_id: str) -> list:
        """Check for RDS automated snapshots from March 2023."""
        snapshots = self.rds_client.describe_db_snapshots(
            DBInstanceIdentifier=db_instance_id,
            SnapshotType="automated",
        )
        
        march_2023_start = datetime(2023, 3, 1).replace(tzinfo=None)
        march_2023_end = datetime(2023, 4, 1).replace(tzinfo=None)
        
        march_snapshots = [
            s for s in snapshots["DBSnapshots"]
            if march_2023_start <= s["SnapshotCreateTime"].replace(tzinfo=None) <= march_2023_end
        ]
        
        return march_snapshots
    
    def check_etl_logs(self, log_bucket: str) -> list:
        """Check ETL job logs to infer what data was processed."""
        # ETL logs may contain: row counts, date ranges, filter conditions
        response = self.s3.list_objects_v2(
            Bucket=log_bucket,
            Prefix="etl-logs/fraud/2023-03/",
        )
        return response.get("Contents", [])
    
    def generate_recovery_report(self, bucket: str, db_id: str, log_bucket: str) -> str:
        s3_check = self.check_s3_versioning(bucket, "fraud/training/")
        db_snapshots = self.check_database_snapshots(db_id)
        etl_logs = self.check_etl_logs(log_bucket)
        
        return f"""
Data Recovery Assessment for Fraud Model v2.3 (March 2023)
============================================================

S3 Versioning: {'ENABLED' if s3_check['versioning'] else 'DISABLED'}
Can recover training files: {'YES' if s3_check.get('can_recover') else 'NO'}
  {s3_check.get('reason', '')}

Database Snapshots: {len(db_snapshots)} snapshots found in March 2023
  {'CAN restore database to March state' if db_snapshots else 'NO SNAPSHOTS — cannot restore DB state'}

ETL Logs: {len(etl_logs)} log files found
  Can infer: date ranges, row counts, filter conditions

Recovery options:
{chr(10).join([
    '1. Restore from March 2023 DB snapshot + re-run ETL (if logs show exact parameters)' if db_snapshots else '1. NO DB SNAPSHOT — this path is unavailable',
    '2. Restore from S3 versioned files (if versioning was enabled)' if s3_check.get('can_recover') else '2. S3 VERSIONING DISABLED — this path is unavailable',
    '3. Approximate reconstruction: use MLflow model metadata + ETL logs to document what we know',
    '4. Accept that exact reproduction is not possible — document this for regulators',
])}
"""
```

### Immediate Implementation Plan

```python
# Never have this problem again — implement within 2 weeks

IMPLEMENTATION_PLAN = """
Immediate Actions (Week 1):
1. Enable S3 versioning on all training data buckets (retroactive)
   aws s3api put-bucket-versioning --bucket ml-training-data \\
     --versioning-configuration Status=Enabled
   
   Set lifecycle to keep versions for 7 years (regulatory requirement):
   aws s3api put-bucket-lifecycle-configuration --bucket ml-training-data \\
     --lifecycle-configuration file://7year_retention.json

2. Initialize DVC in training repo, track all processed data files
   dvc init && dvc add data/processed/ && git tag "data-retroactive-$(date +%Y%m%d)"

3. Log data version metadata in MLflow on every training run:
   mlflow.set_tags({
       's3_data_path': path,
       's3_version_id': version_id,
       'etl_job_id': job_id,
       'data_row_count': str(row_count),
       'data_date_range': f"{start} to {end}",
   })

Medium-term (Month 1):
4. Migrate to Delta Lake — free time-travel forever
5. Add data contract validation to ETL pipeline
6. Implement post-ETL distribution monitoring
"""
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Audit Requirements for 50+ Models

Your company is being audited by a financial regulator. They want: (1) for every model in production, show the training data used; (2) confirm no model was trained on data that included protected attributes (race, gender) directly; (3) verify that any user who invoked GDPR right-to-erasure was removed from all training datasets. You have 50+ models, 3 years of history, and inconsistent data versioning practices across teams. Design an audit response system.

<details>
<summary>💡 Hint</summary>

This is a governance problem as much as a technical one. Think about: (1) what information is currently available vs what you need to reconstruct, (2) how to create a unified audit view across inconsistent systems, (3) for GDPR erasure, how to verify absence from training data at scale without scanning petabytes.

</details>

<details>
<summary>✅ Solution</summary>

### Audit Response Architecture

```python
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
import mlflow, json

@dataclass
class ModelAuditRecord:
    """Complete audit record for regulatory compliance."""
    
    model_name: str
    model_version: str
    deployed_at: datetime
    
    # Data lineage
    training_data_path: str
    training_data_version: Optional[str]
    training_data_row_count: Optional[int]
    training_data_date_range: Optional[str]
    
    # Feature audit
    features_used: List[str]
    protected_attributes_in_features: List[str]  # Should be empty
    pii_features_used: List[str]
    
    # GDPR compliance
    erasure_requests_applied: List[str]  # User IDs removed
    last_erasure_check: Optional[datetime]
    
    # Evidence
    mlflow_run_id: Optional[str]
    data_hash: Optional[str]
    can_reproduce: bool
    reproduction_method: str  # "exact", "approximate", "cannot_reproduce"
    
    def meets_regulatory_requirements(self) -> dict:
        issues = []
        
        if self.protected_attributes_in_features:
            issues.append(f"Protected attributes in features: {self.protected_attributes_in_features}")
        
        if not self.can_reproduce:
            issues.append("Training data cannot be reproduced (missing versioning)")
        
        if self.last_erasure_check is None:
            issues.append("GDPR erasure compliance never verified")
        
        return {
            "compliant": len(issues) == 0,
            "issues": issues,
            "model": f"{self.model_name} v{self.model_version}",
        }


PROTECTED_ATTRIBUTES = {
    "race", "ethnicity", "race_ethnicity", "color", "national_origin",
    "gender", "sex", "religion", "age", "disability", "marital_status",
    "pregnancy_status",
}

def audit_model_features(model_name: str, model_version: str) -> dict:
    """Check if protected attributes were used in model training."""
    
    client = mlflow.tracking.MlflowClient()
    mv = client.get_model_version(model_name, model_version)
    run = client.get_run(mv.run_id)
    
    # Get feature names from logged artifacts
    feature_names = []
    
    # Try to load feature list from MLflow artifacts
    try:
        artifacts = client.list_artifacts(mv.run_id, "model")
        for artifact in artifacts:
            if "feature_names" in artifact.path:
                with open(client.download_artifacts(mv.run_id, artifact.path)) as f:
                    feature_names = json.load(f)
    except:
        feature_names = run.data.params.get("feature_names", "").split(",")
    
    # Check for protected attributes
    lower_features = {f.lower() for f in feature_names}
    protected_used = [f for f in feature_names if f.lower() in PROTECTED_ATTRIBUTES]
    
    return {
        "model": f"{model_name} v{model_version}",
        "features_checked": len(feature_names),
        "protected_attributes_found": protected_used,
        "compliant": len(protected_used) == 0,
    }


def verify_gdpr_erasure(
    model_name: str,
    model_version: str,
    erased_user_ids: List[str],
    training_data_path: str,
    spark,
) -> dict:
    """
    Verify that erased users are not in training data.
    Uses Delta time travel to check historical training data.
    """
    
    client = mlflow.tracking.MlflowClient()
    mv = client.get_model_version(model_name, model_version)
    run = client.get_run(mv.run_id)
    
    delta_version = int(run.data.tags.get("delta_data_version", -1))
    
    if delta_version < 0:
        return {
            "verified": False,
            "reason": "No Delta table version recorded — cannot verify",
            "recommendation": "Manual investigation required",
        }
    
    # Check training data snapshot for erased users
    training_data = spark.read.format("delta") \
        .option("versionAsOf", delta_version) \
        .load(training_data_path)
    
    erased_in_training = training_data.filter(
        training_data.user_id.isin(erased_user_ids)
    ).count()
    
    return {
        "verified": True,
        "delta_version_checked": delta_version,
        "erased_users_requested": len(erased_user_ids),
        "erased_users_found_in_training": erased_in_training,
        "compliant": erased_in_training == 0,
        "recommendation": "Model must be retrained without these users" if erased_in_training > 0 else "No action required",
    }


def generate_full_audit_report(models: List[tuple], spark) -> dict:
    """Generate comprehensive audit report for all production models."""
    
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_models": len(models),
        "models": [],
        "summary": {
            "compliant": 0,
            "issues": 0,
            "cannot_verify": 0,
        },
    }
    
    for model_name, model_version, data_path in models:
        feature_audit = audit_model_features(model_name, model_version)
        
        model_record = {
            "model": f"{model_name} v{model_version}",
            "feature_audit": feature_audit,
            "data_reproducibility": "exact" if data_path else "cannot_reproduce",
        }
        
        if feature_audit["compliant"]:
            report["summary"]["compliant"] += 1
        else:
            report["summary"]["issues"] += 1
        
        report["models"].append(model_record)
    
    return report
```

</details>
</article>
