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

---

## ⚡ Quick-fire Q&A

**Q: What is Amazon S3 and why is it the foundation of modern data lakes?**
A: S3 is an object storage service with unlimited capacity, 11 nines of durability, and native integration with virtually every AWS analytics service. It's the data lake foundation because it decouples storage from compute — EMR, Athena, Glue, and Redshift Spectrum all read from S3 independently, enabling a cost-effective, pay-per-query analytics architecture.

**Q: What are S3 storage classes and how do you choose between them?**
A: S3 Standard is for frequently accessed data; Standard-IA and One Zone-IA for infrequent access with retrieval fees; Glacier Instant Retrieval, Glacier Flexible Retrieval, and Glacier Deep Archive for archival with increasing retrieval times and lower costs. Use S3 Intelligent-Tiering to automatically move objects between tiers based on access patterns when access is unpredictable.

**Q: What is S3 versioning and when should you enable it for data engineering?**
A: Versioning keeps all versions of an object, enabling recovery from accidental overwrites or deletions. Enable it on data lake raw and curated buckets to protect against ETL bugs that overwrite or corrupt data. Combine with S3 Lifecycle Policies to automatically transition older versions to cheaper storage classes.

**Q: What is S3 Select and Athena — how do they differ?**
A: S3 Select pushes down simple filter and projection queries into S3 itself, returning a subset of object data. It's useful for single-object queries in Lambda or Glue without loading the full file. Athena is a full SQL query engine that runs across many S3 objects using the Glue Data Catalog — use Athena for cross-object analytical queries.

**Q: What is the S3 consistency model?**
A: Since December 2020, S3 offers strong read-after-write consistency for all operations (PUT, DELETE, LIST). A successful write is immediately visible to all subsequent reads and list operations — there is no longer an eventual consistency window. This simplifies ETL pipelines that immediately read or list objects after writing.

**Q: How do you optimize S3 for high-throughput data pipelines?**
A: Use multi-part upload for objects over 100MB (required over 5GB), distribute object keys with high-cardinality prefixes to avoid request rate throttling (S3 scales to 3,500 PUT/s and 5,500 GET/s per prefix), use Transfer Acceleration for cross-region uploads, and use S3 Batch Operations for bulk object processing.

**Q: What is S3 Event Notifications and how is it used in pipelines?**
A: S3 Event Notifications publish events (ObjectCreated, ObjectRemoved, etc.) to SQS, SNS, Lambda, or EventBridge when objects are created or modified. This enables event-driven pipelines: a file landing in S3 automatically triggers a Lambda function, Glue job, or Step Functions workflow without polling.

**Q: What is S3 Lifecycle Policy?**
A: Lifecycle Policies automate object transitions between storage classes and expiration. For example: transition raw data to Standard-IA after 30 days, to Glacier after 90 days, and expire after 7 years — fully automating data retention and cost optimization without manual intervention.

---

## 💼 Interview Tips

- Always frame S3 in the context of data lake architecture: it separates storage from compute, enabling multiple analytics engines to query the same data without duplication — this is the foundational design principle interviewers want to hear.
- Senior interviewers probe partitioning strategy: describe organizing S3 data by `year=/month=/day=/hour=` prefixes so Athena and Glue can prune partitions efficiently. The partition structure determines query performance and cost.
- Mention the small files problem as a common production pain point: many small S3 files cause slow Athena queries and Glue job overhead. Describe compaction jobs (Glue or Spark) that merge small files into 128MB–1GB Parquet files.
- Demonstrate security depth: bucket policies restricting access to specific VPC endpoints, Block Public Access enabled, S3 Object Lock for compliance/WORM requirements, and SSE-KMS with customer-managed keys for encryption control.
- Know the cost implications of S3 request rates: frequent small GET requests on billions of objects can exceed storage costs. Mention caching (CloudFront, ElastiCache) for read-heavy access patterns on S3.
- Discuss S3 versioning + lifecycle policies together as the data durability and cost management duo — enabling versioning without lifecycle policies causes unbounded storage growth, a common operational mistake.
