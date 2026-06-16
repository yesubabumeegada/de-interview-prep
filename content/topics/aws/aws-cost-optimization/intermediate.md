---
title: "AWS Cost Optimization Intermediate — Spot Instances, Lifecycle Policies, and Athena Tuning"
description: "Spot instance strategies for EMR and Glue, S3 lifecycle automation, Savings Plans vs Reserved Instances, and Athena query cost control"
content_type: study_material
topic: aws
subtopic: aws-cost-optimization
layer: intermediate
difficulty_level: mid-level
tags: [aws, cost-optimization, spot-instances, emr, glue, s3-lifecycle, athena, savings-plans, reserved-instances, dpu-tuning]
---

# AWS Cost Optimization — Intermediate

## Spot Instances for Data Workloads

Spot instances offer up to 90% savings over On-Demand but require workloads designed to tolerate interruption. For data engineering, this means designing pipelines that can be safely interrupted and resumed.

### How Spot Interruption Works

When AWS needs capacity back, it sends a **Spot interruption notice** 2 minutes before terminating the instance. You can poll the EC2 metadata endpoint to detect this:

```bash
# Check for interruption notice from within the instance
curl -s http://169.254.169.254/latest/meta-data/spot/instance-action
```

If no interruption is pending, this returns a 404. When interruption is imminent, it returns:
```json
{"action": "terminate", "time": "2024-01-15T14:32:00Z"}
```

### Spot Instances in EMR

EMR has first-class Spot support. The key architectural principle: **never run HDFS data on Spot nodes**.

#### Recommended EMR Node Configuration

```
Master Node:    1x On-Demand   (cluster coordinator, always stable)
Core Nodes:     2x On-Demand   (HDFS storage, must be stable)
Task Nodes:     N x Spot       (compute only, safe to lose)
```

Task nodes have no HDFS responsibility, so their interruption only causes task rescheduling — no data loss.

#### EMR Instance Fleets (Recommended over Instance Groups)

Instance Fleets let you specify multiple instance types and capacity units. EMR allocates from whichever has available Spot capacity:

```json
{
  "InstanceFleetType": "TASK",
  "TargetSpotCapacity": 20,
  "InstanceTypeConfigs": [
    {"InstanceType": "m5.xlarge", "WeightedCapacity": 1},
    {"InstanceType": "m5.2xlarge", "WeightedCapacity": 2},
    {"InstanceType": "m4.xlarge", "WeightedCapacity": 1},
    {"InstanceType": "r5.xlarge", "WeightedCapacity": 1}
  ],
  "LaunchSpecifications": {
    "SpotSpecification": {
      "TimeoutDurationMinutes": 10,
      "TimeoutAction": "SWITCH_TO_ON_DEMAND"
    }
  }
}
```

**Key settings:**
- `TimeoutAction: SWITCH_TO_ON_DEMAND` — if no Spot capacity is available after the timeout, fall back to On-Demand rather than failing
- Multiple instance types diversify across Spot pools, reducing interruption risk
- `WeightedCapacity` normalizes different instance sizes to the same "unit"

#### Checkpointing Spark Jobs for Spot Resilience

If a Spot task node is interrupted mid-Spark-job, any in-progress tasks on that node must be retried. To minimize wasted work:

**1. Enable Spark checkpointing:**
```python
spark.sparkContext.setCheckpointDir("s3://my-bucket/checkpoints/")

# For streaming jobs
streaming_query = df \
    .writeStream \
    .option("checkpointLocation", "s3://my-bucket/checkpoints/my-job/") \
    .start("s3://my-bucket/output/")
```

**2. Use appropriate task sizes** — smaller tasks mean less work lost per interruption. Aim for tasks completing in 1-5 minutes.

**3. Enable speculative execution** — Spark reruns slow tasks on other nodes proactively:
```python
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.threshold", "0.75")
```

**4. Write intermediate results to S3** — Use S3 as the persistent store rather than HDFS so partial results survive cluster events.

### Spot Instances in Glue

AWS Glue workers are managed by AWS, but you can influence cost through DPU configuration and job bookmarks.

For Glue ETL jobs running long transforms, Glue automatically handles worker recovery if an underlying worker fails — you don't manage Spot interruptions directly in Glue. However, you can use **Glue Flex execution** (a lower-cost, lower-priority execution class):

```python
# Glue Job configuration
{
    "ExecutionClass": "FLEX",  # Uses spare capacity, ~34% cheaper than STANDARD
    "WorkerType": "G.1X",
    "NumberOfWorkers": 10
}
```

**FLEX execution** is appropriate for non-urgent batch jobs. It uses spare capacity similar to Spot and may take longer to start or run, but costs significantly less.

---

## S3 Lifecycle Policies

S3 Lifecycle policies automatically transition objects between storage classes or delete them after defined periods. This is one of the highest-ROI cost optimizations for data teams.

### Lifecycle Policy Structure

Lifecycle rules are JSON configurations applied to S3 buckets:

```json
{
  "Rules": [
    {
      "ID": "DataLakeLifecycle",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "raw/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER_IR"
        },
        {
          "Days": 365,
          "StorageClass": "DEEP_ARCHIVE"
        }
      ],
      "Expiration": {
        "Days": 2555
      }
    }
  ]
}
```

This rule transitions raw data:
- After 30 days → Standard-IA (~46% savings vs Standard)
- After 90 days → Glacier Instant Retrieval (~83% savings vs Standard)
- After 365 days → Deep Archive (~96% savings vs Standard)
- After 2555 days (7 years) → Delete

### Applying via AWS CLI

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket my-data-lake \
    --lifecycle-configuration file://lifecycle.json
```

### Lifecycle Patterns for Data Engineering

**Pattern 1: Raw data lake with compliance retention**
```
0-30 days:    Standard (active ingestion)
30-180 days:  Standard-IA (occasional re-processing)
180-365 days: Glacier Flexible (compliance, rare access)
365+ days:    Deep Archive (7-year legal hold)
```

**Pattern 2: Processed/curated data**
```
0-90 days:    Standard (active analytics)
90-365 days:  Standard-IA (historical queries)
365+ days:    Glacier Instant Retrieval (must access quickly)
```

**Pattern 3: Log data**
```
0-30 days:    Standard (monitoring, debugging)
30-90 days:   Standard-IA (incident investigation)
90+ days:     Delete (no retention requirement)
```

### Incomplete Multipart Upload Cleanup

A common source of hidden S3 costs: failed multipart uploads leave partial data that accrues charges indefinitely.

```json
{
  "Rules": [
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

Always include this rule on all buckets. Incomplete uploads can silently cost hundreds of dollars per month on high-throughput pipelines.

---

## Reserved Instances vs. Savings Plans for Data Workloads

### When to Use Reserved Instances

Reserved Instances (RIs) are ideal when you:
- Know exactly which instance type you'll use for 1-3 years
- Have steady-state 24/7 workloads (Redshift, always-on EMR, Kafka brokers)
- Want the maximum possible discount (up to 72%)

**Standard RI Example:**
- `r5.4xlarge` On-Demand: $1.008/hour
- 1-year Standard RI (All Upfront): ~$0.503/hour effective rate
- 3-year Standard RI (All Upfront): ~$0.319/hour effective rate
- **Savings: 50-68%**

**Convertible RI:**
- Can exchange for different instance family, OS, or tenancy
- Smaller discount (~45-54% off On-Demand for 3-year)
- Better if your instance needs might change

### When to Use Savings Plans

Savings Plans commit to a dollar-per-hour spend rather than a specific instance type:

```
"I will spend $5.00/hour on compute for the next year"
```

This covers:
- **Compute Savings Plans:** EC2 (any family, region, OS), Fargate, Lambda
- **EC2 Instance Savings Plans:** Specific instance family in a region, larger discount

**Savings Plans are generally preferred for data teams** because:
- Data workloads evolve — you might migrate from m5 to m6i or r5
- One commitment covers multiple services (EC2 + Lambda + Fargate)
- No capacity management of specific RI inventory

### Decision Framework

```
Is the workload steady-state 24/7? 
  └─ YES: Consider commitment
       Is the instance type fixed for 3 years?
         └─ YES: Standard RI (maximum discount)
         └─ NO:  Savings Plans (flexibility)
  └─ NO: Use On-Demand or Spot
```

### Right-Sizing Before Committing

**Never purchase RIs or Savings Plans without right-sizing first.** Use AWS Compute Optimizer to identify over-provisioned instances:

```bash
aws compute-optimizer get-ec2-instance-recommendations \
    --account-ids 123456789012
```

If your Redshift cluster is at 20% CPU, right-size to a smaller node type before committing to years of over-provisioned reserved capacity.

---

## Athena Cost Optimization

Amazon Athena charges $5 per TB of data scanned. On a large data lake, a single poorly written query can cost hundreds of dollars. Cost optimization here has a direct, measurable impact.

### Understanding Athena Pricing

- **$5.00 per TB scanned** (rounded up to the nearest 10 MB)
- **Minimum charge:** 10 MB per query
- **No charge for:** DDL statements, failed queries due to syntax errors, partition pruning that results in 0 bytes scanned

### Optimization 1: Partitioning

Partitioning is the single most impactful Athena optimization. Queries with a `WHERE` clause on the partition key scan only matching partitions.

**Without partitioning:**
```sql
-- Scans entire table (e.g., 1 TB = $5.00)
SELECT * FROM events WHERE event_date = '2024-01-15'
```

**With date partitioning:**
```sql
-- Scans only 2024/01/15 partition (e.g., 10 GB = $0.05)
SELECT * FROM events WHERE event_date = '2024-01-15'
```

**Partition design best practices:**
```
s3://bucket/events/year=2024/month=01/day=15/file.parquet
```

```sql
CREATE EXTERNAL TABLE events (
    event_id STRING,
    user_id  STRING,
    action   STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://bucket/events/'
```

Always partition by the columns most commonly used in `WHERE` clauses (typically date/time).

### Optimization 2: Columnar File Formats

CSV/JSON require scanning entire rows even if you only need a few columns. Columnar formats (Parquet, ORC) store data column-by-column, enabling column pruning.

**Impact example:**
```sql
SELECT user_id, revenue FROM orders WHERE date = '2024-01'
```

- **CSV (100 columns, 1 TB total):** Scans all 100 columns = $5.00
- **Parquet (100 columns, 1 TB total):** Scans only 2 columns ≈ $0.10

**Converting to Parquet using Glue:**
```python
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": "s3://bucket/parquet-output/"},
    format="glueparquet",
    format_options={"compression": "snappy"}
)
```

**Compression further reduces scan costs:**
- Snappy: ~2-4x compression (default, good balance)
- GZIP: ~4-6x compression (higher ratio, slower)
- ZSTD: ~4-5x with faster decompression (modern choice)

### Optimization 3: CTAS for Expensive Queries

Create Table As Select (CTAS) materializes query results as a new, optimized table. Use it to pre-aggregate expensive computations or convert CSV to Parquet.

```sql
-- Convert raw CSV to partitioned Parquet
CREATE TABLE events_parquet
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year', 'month', 'day'],
    external_location = 's3://bucket/events-parquet/'
)
AS SELECT
    event_id,
    user_id,
    action,
    YEAR(event_date) AS year,
    MONTH(event_date) AS month,
    DAY(event_date) AS day
FROM events_raw;
```

Subsequent queries on `events_parquet` will be dramatically cheaper than querying `events_raw`.

### Optimization 4: Athena Workgroups for Cost Control

Workgroups allow you to set query-level spending limits:

```json
{
  "WorkGroupConfiguration": {
    "ResultConfiguration": {
      "OutputLocation": "s3://bucket/query-results/"
    },
    "EnforceWorkGroupConfiguration": true,
    "BytesScannedCutoffPerQuery": 10737418240,
    "PublishCloudWatchMetricsEnabled": true
  }
}
```

`BytesScannedCutoffPerQuery: 10737418240` = 10 GB limit per query = max $0.05 per query.

Create separate workgroups for different teams/environments:
- `production` — higher limits, strict governance
- `analytics` — moderate limits, self-service
- `dev` — low limits, prevent accidental full-table scans

---

## Glue DPU Tuning

AWS Glue charges by Data Processing Units (DPU). Each DPU = 4 vCPU + 16 GB RAM. Default is often more than needed.

### DPU Pricing

- **Glue ETL Jobs:** $0.44/DPU-hour (Standard), $0.29/DPU-hour (Flex)
- **Glue Crawlers:** $0.44/DPU-hour
- **Glue DataBrew:** $1.00/DPU-hour

### Right-Sizing Glue Jobs

**Step 1: Profile the job**
Run with default settings (10 DPUs) and check:
- CloudWatch metrics: `glue.driver.BlockManager.disk.diskSpaceUsed_MB`
- `glue.driver.ExecutorAllocationManager.executors.numberAllMaxNeeded`
- `glue.ALL.jvm.heap.usage`

**Step 2: Identify bottleneck**

| Symptom | Root Cause | Solution |
|---------|-----------|---------|
| High heap usage | Memory pressure | Increase DPU size (G.2X) |
| Low executor utilization | Over-provisioned | Reduce NumberOfWorkers |
| Spill to disk | Insufficient memory | Increase DPU or repartition |
| Long shuffle time | Data skew | Salting, repartition |

**Step 3: Configure appropriate worker type**

```python
# For most ETL jobs
{
    "WorkerType": "G.1X",      # 1 DPU: 4 vCPU, 16 GB RAM
    "NumberOfWorkers": 5        # Adjusted from profiling
}

# For memory-intensive joins
{
    "WorkerType": "G.2X",      # 2 DPU: 8 vCPU, 32 GB RAM
    "NumberOfWorkers": 3
}

# For very large datasets
{
    "WorkerType": "G.4X",      # 4 DPU: 16 vCPU, 64 GB RAM
    "NumberOfWorkers": 2
}
```

### Glue Job Bookmarks

Enable bookmarks to prevent reprocessing data already processed. Without bookmarks, re-runs scan and charge for all historical data:

```python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)  # Bookmarks enabled in job config

# ... ETL logic ...

job.commit()  # Must call commit() for bookmark to advance
```

### Glue Crawler Optimization

Crawlers can be expensive if run too frequently or on large buckets:

**Best practices:**
1. Schedule crawlers only when new data arrives (event-driven via Lambda/SQS)
2. Use `Exclude patterns` to skip irrelevant paths
3. Use table-level locking to update only changed partitions
4. For high-frequency ingestion, register new partitions directly via API instead of crawling:

```python
glue_client.batch_create_partition(
    DatabaseName='my_database',
    TableName='my_table',
    PartitionInputList=[
        {
            'Values': ['2024', '01', '15'],
            'StorageDescriptor': { ... }
        }
    ]
)
```

---

## Key Takeaways

- Use EMR Instance Fleets with multiple Spot instance types and `SWITCH_TO_ON_DEMAND` fallback
- Implement S3 lifecycle policies on all buckets; always include incomplete multipart upload cleanup
- Prefer Savings Plans over Reserved Instances for data teams (flexibility as workloads evolve)
- Athena costs are dominated by data scanned — partitioning and columnar formats (Parquet) are mandatory optimizations
- Workgroup scan limits cap runaway Athena queries
- Profile Glue jobs before committing to DPU count; use Flex execution for non-urgent batch work
