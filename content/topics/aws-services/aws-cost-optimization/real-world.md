---
title: "AWS Cost Optimization Real-World Case Studies"
description: "Real-world case studies: Spot interruption strategy for EMR, S3 storage audit, Athena workgroup governance, and cross-region replication cost trap"
content_type: study_material
topic: aws-services
subtopic: aws-cost-optimization
layer: real-world
difficulty_level: senior
tags: [aws, cost-optimization, emr, spot-instances, s3, athena, workgroups, cross-region-replication, case-study, data-engineering]
---

# AWS Cost Optimization — Real-World Case Studies

## Case Study 1: Spot Interruption Strategy for Nightly EMR Jobs

### Context

A media analytics company runs nightly EMR Spark jobs to compute audience metrics from 2 TB of daily event data. The pipeline runs from midnight to 5 AM, processing logs through four stages:

1. Raw event validation and deduplication
2. Session reconstruction
3. Audience segment attribution
4. Aggregation and output to Redshift

Original configuration: 10x `m5.2xlarge` On-Demand core nodes, running $2,200/month.

### Problem

The team wanted to reduce costs but was afraid of Spot interruptions causing job failures in the middle of the night. Previous Spot attempts had caused complete job failures when multiple nodes were interrupted simultaneously.

### Root Cause Analysis

The original failures had three causes:
1. **Single instance type** — when that pool ran out, all nodes were interrupted simultaneously
2. **Large task sizes** — each Spark task took 45+ minutes, so any interruption wasted enormous compute
3. **No checkpointing** — pipeline restarts from scratch, losing all progress

### Solution Architecture

**Step 1: Redesign the cluster configuration**

```json
{
  "InstanceFleets": [
    {
      "InstanceFleetType": "MASTER",
      "TargetOnDemandCapacity": 1,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.xlarge"}
      ]
    },
    {
      "InstanceFleetType": "CORE",
      "TargetOnDemandCapacity": 2,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.xlarge"},
        {"InstanceType": "m5d.xlarge"}
      ]
    },
    {
      "InstanceFleetType": "TASK",
      "TargetSpotCapacity": 40,
      "TargetOnDemandCapacity": 0,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "m5.4xlarge", "WeightedCapacity": 4},
        {"InstanceType": "m5d.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "m4.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "r5.xlarge", "WeightedCapacity": 1}
      ],
      "LaunchSpecifications": {
        "SpotSpecification": {
          "TimeoutDurationMinutes": 5,
          "TimeoutAction": "SWITCH_TO_ON_DEMAND"
        }
      }
    }
  ]
}
```

**Step 2: Right-size Spark tasks**

```python
# Before: default parallelism → massive tasks
spark.conf.set("spark.default.parallelism", "2000")  # was 200
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # was 200

# Target: tasks complete in 1-3 minutes, not 45 minutes
```

**Step 3: Stage-level checkpointing**

```python
def run_pipeline():
    checkpoint_base = "s3://pipeline-bucket/checkpoints/"
    
    # Stage 1: Validation
    validated_path = f"{checkpoint_base}validated/"
    if not s3_path_exists(validated_path):
        validated = validate_and_deduplicate(raw_events)
        validated.write.parquet(validated_path)
    validated = spark.read.parquet(validated_path)
    
    # Stage 2: Session reconstruction
    sessions_path = f"{checkpoint_base}sessions/"
    if not s3_path_exists(sessions_path):
        sessions = reconstruct_sessions(validated)
        sessions.write.parquet(sessions_path)
    sessions = spark.read.parquet(sessions_path)
    
    # Stage 3: Attribution
    attributed_path = f"{checkpoint_base}attributed/"
    if not s3_path_exists(attributed_path):
        attributed = attribute_segments(sessions)
        attributed.write.parquet(attributed_path)
    attributed = spark.read.parquet(attributed_path)
    
    # Stage 4: Aggregation
    aggregate_and_write_to_redshift(attributed)
    
    # Cleanup checkpoints after success
    cleanup_s3_prefix(checkpoint_base)
```

**Step 4: Capacity Rebalancing**

Enable EMR's native Capacity Rebalancing, which proactively replaces at-risk Spot instances before they're interrupted:

```bash
aws emr modify-cluster \
    --cluster-id j-XXXXXXXXXXXX \
    --capacity-reservation-options '{"UsageStrategy": "use-capacity-reservations-first"}'
```

### Results

| Metric | Before | After |
|--------|--------|-------|
| Monthly cost | $2,200 | $680 |
| Job failure rate | 0% (On-Demand) | 1.2% (requiring restart) |
| Average restart cost | N/A | ~45 min extra compute |
| Net monthly savings | — | $1,520 (69%) |

The 1.2% interruption rate was deemed acceptable: checkpointing meant restarts picked up from the most recent stage rather than from scratch, adding ~45 minutes to affected runs.

---

## Case Study 2: S3 Storage Cost Audit — 60% Bill Reduction

### Context

A SaaS analytics company had grown their data lake to 850 TB over 4 years. Monthly S3 bill: $26,000. After an audit, they reduced it to $10,400 — a 60% reduction without losing any data.

### Audit Process

**Step 1: Inventory all buckets and storage class breakdown**

```bash
# Get storage class breakdown per bucket using S3 Storage Lens
aws s3control get-storage-lens-dashboard \
    --config-id my-storage-lens \
    --account-id 123456789012

# Alternatively, use S3 Inventory reports
aws s3api put-bucket-inventory-configuration \
    --bucket my-data-lake \
    --id full-inventory \
    --inventory-configuration '{
        "Destination": {
            "S3BucketDestination": {
                "Bucket": "arn:aws:s3:::inventory-output",
                "Format": "Parquet"
            }
        },
        "IsEnabled": true,
        "Filter": {},
        "Id": "full-inventory",
        "IncludedObjectVersions": "All",
        "OptionalFields": ["Size", "LastModifiedDate", "StorageClass"],
        "Schedule": {"Frequency": "Daily"}
    }'
```

**Step 2: Query inventory with Athena**

```sql
-- Identify objects by age and current storage class
SELECT
    storage_class,
    COUNT(*) AS object_count,
    SUM(size) / POWER(1024, 4) AS size_tb,
    DATE_DIFF('day', DATE(last_modified_date), CURRENT_DATE) AS age_days
FROM s3_inventory
GROUP BY 1, 2, 4
ORDER BY age_days, storage_class;
```

**Findings:**

| Age | Storage Class | Size | Monthly Cost | Should Be |
|-----|--------------|------|-------------|-----------|
| 0-30 days | Standard | 120 TB | $2,760 | Standard ✓ |
| 31-180 days | Standard | 380 TB | $8,740 | Standard-IA |
| 181-365 days | Standard | 250 TB | $5,750 | Glacier Instant |
| 365+ days | Standard | 100 TB | $2,300 | Deep Archive |

380 TB of 31-180 day data was still in Standard when it should have been in Standard-IA — they had simply never configured lifecycle policies.

**Step 3: Identify delete-eligible data**

```sql
-- Identify buckets with versioning enabled but no lifecycle for old versions
-- Old versions can silently accumulate to enormous sizes
SELECT
    bucket_name,
    SUM(CASE WHEN is_latest = 'false' THEN size ELSE 0 END) / POWER(1024, 4) AS old_versions_tb,
    SUM(CASE WHEN is_multipart_uploaded = 'true' AND is_latest = 'false' THEN size ELSE 0 END) 
        / POWER(1024, 3) AS incomplete_uploads_gb
FROM s3_inventory
GROUP BY 1
ORDER BY old_versions_tb DESC;
```

**Discovery:** 45 TB of old object versions accumulating from a versioning-enabled raw data bucket with no lifecycle rule for non-current versions.

**Step 4: Implement lifecycle policies**

```json
{
  "Rules": [
    {
      "ID": "DataLakeMainLifecycle",
      "Status": "Enabled",
      "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 180, "StorageClass": "GLACIER_IR"},
        {"Days": 730, "StorageClass": "DEEP_ARCHIVE"}
      ]
    },
    {
      "ID": "CleanupOldVersions",
      "Status": "Enabled",
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      }
    },
    {
      "ID": "CleanupIncompleteUploads",
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    }
  ]
}
```

### Results After 90-Day Transition Period

| Storage Class | Before | After | Savings |
|--------------|--------|-------|---------|
| Standard | 850 TB | 120 TB | $16,790/month |
| Standard-IA | 0 TB | 380 TB | -$4,750/month |
| Glacier Instant | 0 TB | 250 TB | -$1,000/month |
| Deep Archive | 0 TB | 145 TB | -$144/month |
| Deleted (old versions) | 45 TB | 0 TB | $1,035/month |
| **Total** | **$26,000** | **$10,400** | **$15,600/month** |

**Lessons learned:**
- Always configure lifecycle policies when creating buckets — not retroactively
- S3 Versioning without lifecycle for non-current versions is a cost time bomb
- S3 Inventory + Athena is more cost-effective than Storage Lens for ad-hoc analysis
- Incomplete multipart upload cleanup is free money (no business value lost)

---

## Case Study 3: Athena Query Cost Governance with Workgroup Quotas

### Context

A retail analytics company with 50 analysts using Athena for ad-hoc queries against a 200 TB data lake. Monthly Athena bill had grown to $35,000 — driven by full-table scans from analysts unfamiliar with query optimization.

### Problem Analysis

```sql
-- Most expensive queries (from CloudTrail + CUR join)
SELECT
    user_identity_arn AS analyst,
    query_string,
    bytes_scanned / 1e12 AS tb_scanned,
    bytes_scanned * 5e-12 AS cost_usd
FROM athena_queries
ORDER BY bytes_scanned DESC
LIMIT 10;
```

**Top finding:** Three analysts were running nightly scheduled queries with `SELECT *` on full tables, scanning 50+ TB per night collectively.

### Solution: Workgroup Architecture

Implemented a three-tier workgroup strategy:

**Tier 1: Production Workgroup (automated pipelines)**
```json
{
  "Name": "production",
  "Configuration": {
    "ResultConfiguration": {
      "OutputLocation": "s3://results/production/"
    },
    "EnforceWorkGroupConfiguration": true,
    "BytesScannedCutoffPerQuery": 107374182400,
    "PublishCloudWatchMetricsEnabled": true
  }
}
```
100 GB limit per query. Pipelines are pre-validated by data engineering team.

**Tier 2: Analytics Workgroup (trained analysts)**
```json
{
  "Name": "analytics-standard",
  "Configuration": {
    "BytesScannedCutoffPerQuery": 10737418240,
    "PublishCloudWatchMetricsEnabled": true
  }
}
```
10 GB limit per query (~$0.05 max per query). Must attend Athena optimization training to access.

**Tier 3: Sandbox Workgroup (all users, default)**
```json
{
  "Name": "sandbox",
  "Configuration": {
    "BytesScannedCutoffPerQuery": 1073741824,
    "PublishCloudWatchMetricsEnabled": true
  }
}
```
1 GB limit per query (~$0.005 max per query). Catch-all for new analysts.

**IAM policy to enforce workgroup assignment:**
```json
{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "athena:StartQueryExecution",
      "Resource": "arn:aws:athena:us-east-1:123:workgroup/sandbox"
    },
    {
      "Effect": "Deny",
      "Action": "athena:StartQueryExecution",
      "Resource": [
        "arn:aws:athena:us-east-1:123:workgroup/production",
        "arn:aws:athena:us-east-1:123:workgroup/analytics-standard"
      ]
    }
  ]
}
```

**Query cost dashboard Lambda (daily email to analysts):**
```python
def send_daily_cost_report():
    ce = boto3.client('ce')
    
    # Get Athena costs by workgroup tag for yesterday
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': yesterday.strftime('%Y-%m-%d'),
            'End': today.strftime('%Y-%m-%d')
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'TAG', 'Key': 'AthenaWorkgroup'},
            {'Type': 'TAG', 'Key': 'User'}
        ],
        Filter={
            'Dimensions': {
                'Key': 'SERVICE',
                'Values': ['Amazon Athena']
            }
        }
    )
    
    # Format and email via SES
    send_cost_email(response)
```

### Results

| Metric | Before | After 30 days |
|--------|--------|---------------|
| Monthly Athena bill | $35,000 | $8,200 |
| Full-table scans/day | ~45 | ~3 |
| Analyst query errors (scan limit) | N/A | ~12/day (declined quickly) |
| Training completion | 0% | 78% in first month |

**Key insight:** The scan limit errors were educational — analysts quickly learned to add partition filters when their queries were blocked. The cost reduction was 77% with no loss of analytical capability.

---

## Case Study 4: The Cross-Region Replication Cost Trap

### Context

A financial services company implemented S3 Cross-Region Replication (CRR) for disaster recovery. They replicated all data from `us-east-1` to `eu-west-1`. After 3 months, they noticed a $28,000/month line item they didn't anticipate.

### The Trap

**What they replicated:** All S3 data, including raw ingestion buckets

**Data breakdown:**
- Raw data (never needs DR): 150 TB/month ingested = 150 TB × $0.02 = **$3,000/month** in transfer
- Processed/curated data (actual DR requirement): 15 TB/month = 15 TB × $0.02 = **$300/month**

Plus: Storage costs in eu-west-1 for data that was never needed there.

**Total unnecessary cost:** ~$27,700/month

### Root Cause

The architect had replicated everything to "be safe" without analyzing:
1. Which data actually needs DR (only curated/production data)
2. Recovery Time Objective (RTO) — how quickly do they need to recover?
3. Recovery Point Objective (RPO) — how much data loss is acceptable?

### Solution

**Step 1: Classify data by DR requirement**

```
Bucket: raw-ingestion-*       → DR required? NO (can be re-ingested from source)
Bucket: processed-*           → DR required? NO (can be recomputed from raw)
Bucket: curated-gold-*        → DR required? YES (source of truth)
Bucket: ml-models-*           → DR required? YES (trained artifacts)
Bucket: config-*              → DR required? YES (operational data)
```

**Step 2: Restructure CRR to replicate only DR-required buckets**

```bash
# Remove CRR from raw and processed buckets
aws s3api delete-bucket-replication --bucket raw-ingestion-bucket
aws s3api delete-bucket-replication --bucket processed-bucket

# Keep CRR only on curated and critical buckets
# Curated data is ~10% of total volume
```

**Step 3: Add replication filters for curated bucket**

Even within curated data, not everything needs cross-region DR:

```json
{
  "ReplicationConfiguration": {
    "Rules": [
      {
        "ID": "CriticalDataOnly",
        "Status": "Enabled",
        "Filter": {
          "Prefix": "final/"
        },
        "Destination": {
          "Bucket": "arn:aws:s3:::curated-backup-eu-west-1",
          "StorageClass": "GLACIER_IR"
        }
      }
    ]
  }
}
```

Using Glacier Instant Retrieval in the destination region: data is available in milliseconds if needed for DR but costs much less than Standard.

### Final Cost Comparison

| Component | Before | After |
|-----------|--------|-------|
| CRR data transfer | $3,000/month | $300/month |
| DR bucket storage (eu-west-1) | $2,500/month | $250/month |
| Additional indirect costs | $22,200/month | $0 |
| **Total CRR cost** | **$28,000/month** | **$550/month** |

**98% cost reduction** by applying basic data classification before configuring replication.

### Lessons Learned

1. **Always define RTO/RPO before designing DR** — these determine what actually needs replication
2. **Raw data rarely needs cross-region replication** — it can be re-ingested from source systems
3. **Derived/computed data rarely needs cross-region replication** — it can be recomputed
4. **Use Glacier in the DR region** — DR data accessed only during actual disasters; Glacier retrieval latency is acceptable
5. **Replicate selectively with prefix filters** — even within "important" buckets, not all data is critical
6. **Audit CRR configurations quarterly** — data classification changes over time

---

## Common Cost Anti-Patterns in Data Engineering

Based on these case studies and broader industry experience:

| Anti-Pattern | Impact | Fix |
|-------------|--------|-----|
| On-Demand EMR with no auto-terminate | Running idle clusters overnight/weekends | Transient clusters + auto-terminate |
| S3 versioning without lifecycle rules | Unlimited version accumulation | NoncurrentVersionExpiration policy |
| SELECT * on Athena without partitions | Full-table scans on every query | Workgroup scan limits + training |
| Replicating all data cross-region "for safety" | 10x transfer costs | DR classification + selective CRR |
| Default Glue DPU count (10 DPUs) | Over-provisioned jobs | Profile + right-size |
| Single Spot instance type in EMR | Simultaneous interruptions | Instance Fleet with diversification |
| No cost allocation tags | Unable to attribute costs | Enforce tags via SCP/Config |
| Large Spark tasks (30+ min) | Huge waste on Spot interruption | Increase parallelism, smaller tasks |
