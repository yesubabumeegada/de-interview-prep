---
title: "AWS S3 - Scenario Questions"
topic: aws-services
subtopic: s3
content_type: scenario_question
tags: [aws, s3, interview, scenarios, data-lake]
---

# Scenario Questions — AWS S3

<article data-difficulty="junior">

## 🟢 Junior: Choose the Right Storage Class

**Scenario:** Your data lake has three types of data: (1) Daily transaction files queried multiple times per day by analysts, (2) Monthly compliance exports accessed once per quarter for audits, (3) Raw log files never accessed after initial processing but required to keep for 7 years by law. Assign a storage class to each.

<details>
<summary>✅ Solution</summary>

| Data Type | Storage Class | Reasoning | Cost/TB/month |
|-----------|--------------|-----------|--------------|
| Daily transaction files | **S3 Standard** | Frequent access, low latency needed | $23.00 |
| Monthly compliance exports | **S3 Standard-IA** or **Glacier Instant Retrieval** | Rare access, but need fast retrieval when audited | $12.50 or $4.00 |
| Raw logs (7-year retention) | **S3 Glacier Deep Archive** | Almost never accessed, cheapest storage | $0.99 |

**Cost savings vs all-Standard:**
- 100 TB compliance data: $23 → $4/TB/month = **$1,900/month saved**
- 500 TB logs: $23 → $0.99/TB/month = **$11,005/month saved**

**Implementation:**
```json
{
    "Rules": [
        {"Filter": {"Prefix": "raw/logs/"}, "Transitions": [{"Days": 1, "StorageClass": "DEEP_ARCHIVE"}]},
        {"Filter": {"Prefix": "compliance/"}, "Transitions": [{"Days": 30, "StorageClass": "GLACIER_IR"}]}
    ]
}
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design an Event-Driven Pipeline

**Scenario:** A partner uploads CSV files to your S3 bucket daily (unpredictable timing, between midnight and 6 AM). You need to: validate the file schema, convert to Parquet, load to your warehouse, and alert the team on success or failure. Design the architecture.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
Partner uploads to: s3://partner-drops/incoming/{date}/data.csv

S3 Event (ObjectCreated) → SQS Queue → Lambda: Validate
    ├── Schema OK → Move to s3://data-lake/raw/partner/
    │   └── Trigger Glue Job: CSV → Parquet → s3://data-lake/curated/partner/
    │       └── Success → SNS: notify team + trigger warehouse COPY
    │       └── Failure → SNS: alert team + move file to s3://quarantine/
    └── Schema BAD → Move to s3://quarantine/ + SNS alert
```

**Implementation:**

```python
# Lambda: validate_partner_file
def handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download and validate schema
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'], nrows=100)
    
    expected_columns = ['order_id', 'customer_id', 'amount', 'date']
    if list(df.columns) != expected_columns:
        # Move to quarantine
        s3.copy_object(Bucket='quarantine', Key=f'bad-schema/{key}', CopySource=f'{bucket}/{key}')
        sns.publish(TopicArn=ALERT_TOPIC, Message=f'Schema validation failed: {key}')
        return {'status': 'failed', 'reason': 'schema_mismatch'}
    
    # Schema OK: move to raw zone and trigger Glue
    s3.copy_object(Bucket='data-lake', Key=f'raw/partner/{key}', CopySource=f'{bucket}/{key}')
    glue.start_job_run(JobName='partner-csv-to-parquet', Arguments={'--input_key': key})
    return {'status': 'processing'}
```

**Key design decisions:**
- SQS between S3 and Lambda: handles retry, dead-letter queue for failed invocations
- Quarantine bucket: bad files stored for investigation, not deleted
- Glue (not Lambda) for transformation: handles large files better than Lambda's 15-min limit
- SNS for notifications: fan-out to email, Slack, PagerDuty

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Optimize a $50K/Month S3 Bill

**Scenario:** Your company's S3 bill is $50K/month across 3 PB of data. Leadership wants a 40% reduction. Using S3 Inventory data, you find: 60% of data hasn't been accessed in 6+ months, 15% of storage is incomplete multipart uploads, and 20% of objects are < 1 KB. Propose and quantify an optimization plan.

<details>
<summary>✅ Solution</summary>

**Current cost breakdown (3 PB in Standard at $23/TB/month):**
- 3,000 TB × $23 = $69,000/month (actual $50K means some data is already tiered)
- Let's assume: 2,000 TB Standard ($46K) + 500 TB IA ($6.25K) = ~$52K

**Optimization plan:**

| Action | Data Affected | Savings/Month |
|--------|--------------|--------------|
| Lifecycle: move 60% idle data to IA | 1,200 TB Standard → IA | $12,600 |
| Abort incomplete multipart uploads | 15% of total = 450 TB | $10,350 |
| Delete 1KB micro-objects or compact | 20% of objects (minimal storage) | $500 (API cost reduction) |
| Move 6+ month idle data to Glacier IR | 600 TB from IA → Glacier | $5,100 |
| **Total savings** | | **$28,550 (55% reduction!)** |

**Implementation timeline:**

Week 1: Immediate wins
```python
# Abort all incomplete multipart uploads (free 450 TB immediately)
lifecycle_rule = {
    'ID': 'abort-incomplete',
    'Status': 'Enabled',
    'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1}
}
# Apply to all buckets in the account

# Delete empty/micro objects (< 1 KB)
# Query S3 Inventory to find them, then batch delete
```

Week 2-4: Lifecycle policies
```python
# Transition cold data to cheaper tiers
lifecycle_rules = [
    # Data not accessed in 90 days → Standard-IA
    {'Days': 90, 'StorageClass': 'STANDARD_IA'},
    # Data not accessed in 180 days → Glacier Instant Retrieval
    {'Days': 180, 'StorageClass': 'GLACIER_IR'},
    # Data not accessed in 365 days → Glacier Flexible
    {'Days': 365, 'StorageClass': 'GLACIER'},
]
```

Month 2: Compaction
```python
# Compact small files in each prefix (reduce per-request costs)
# Run weekly Spark job:
for prefix in prefixes_with_small_files:
    spark.read.parquet(f"s3://lake/{prefix}/") \
        .coalesce(optimal_file_count) \
        .write.mode("overwrite").parquet(f"s3://lake/{prefix}/")
```

**Monitoring (ongoing):**
- S3 Storage Lens dashboard tracking tier distribution
- Weekly cost report by prefix/zone
- Alert if Standard storage grows > 5%/week without corresponding IA growth

</details>

</article>
