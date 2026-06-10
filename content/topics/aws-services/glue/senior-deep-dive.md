---
title: "AWS Glue - Senior Deep Dive"
topic: aws-services
subtopic: glue
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [aws, glue, etl, performance, auto-scaling, iceberg, custom-connectors]
---

# AWS Glue — Senior-Level Deep Dive

## Glue Auto Scaling (Glue 3.0+)

Glue 3.0 introduced auto-scaling: dynamically adjusts worker count based on workload (and it remains available in Glue 4.0+):

```python
glue.create_job(
    Name='auto-scaling-etl',
    GlueVersion='4.0',
    WorkerType='G.1X',
    NumberOfWorkers=2,       # MINIMUM workers (starting point)
    MaxCapacity=50,          # MAXIMUM workers (auto-scales up to this)
    # Glue monitors workload and adds/removes workers automatically
)
```

**How it works:**
- Job starts with minimum workers
- If tasks are queuing (not enough compute), Glue adds workers
- If workers are idle, Glue removes them
- You pay only for DPU-time actually used

> **Cost benefit:** A job that needs 50 DPUs for the join stage but only 5 DPUs for the read/write stages now automatically scales — saving 60-70% vs fixed allocation.

---

## Glue + Apache Iceberg/Hudi Integration

Glue 4.0 natively supports table formats beyond plain Parquet:

```python
# Read an Iceberg table via Glue Catalog
iceberg_df = glueContext.create_dynamic_frame.from_catalog(
    database="lakehouse",
    table_name="fact_orders",  # Iceberg table registered in Glue Catalog
    additional_options={
        "useSparkDataSourceFormat": True  # Use native Spark Iceberg reader
    }
)

# Write to Iceberg table
spark.sql("""
    INSERT INTO lakehouse.fact_orders
    SELECT * FROM temp_new_orders
""")

# Time travel query (Iceberg)
spark.sql("""
    SELECT * FROM lakehouse.fact_orders
    FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00'
""")

# MERGE (Iceberg supports upsert natively)
spark.sql("""
    MERGE INTO lakehouse.fact_orders t
    USING staging.new_orders s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Glue Catalog + Iceberg benefits:**
- ACID transactions on S3
- Schema evolution without data rewrite
- Partition evolution (change partitioning without rewriting)
- Time travel for auditing
- Works with Athena, EMR, Redshift Spectrum (all read the same Iceberg table)

---

## Custom Connectors (Glue Marketplace)

For data sources not natively supported, use or build custom connectors:

```python
# Use a marketplace connector (e.g., Salesforce, SAP, Elasticsearch)
salesforce_data = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
        "className": "salesforce",
        "sfUrl": "https://mycompany.salesforce.com",
        "sfUser": "api_user",
        "sfPassword": "{{resolve:secretsmanager:sf-password}}",
        "sfObject": "Opportunity",
        "sfFilters": "CloseDate >= 2024-01-01"
    }
)

# Build your own connector (for internal APIs)
# Implement the Glue Spark connector interface:
# - getSchema()
# - read()
# - write()
```

---

## Advanced Job Bookmark Patterns

### Bookmark with Timestamp-Based Sources

```python
# For JDBC sources: bookmark tracks the max value of a timestamp column
orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="rds_source",
    table_name="orders",
    transformation_ctx="orders_source",  # Bookmark key
    additional_options={
        "jobBookmarkKeys": ["updated_at"],     # Column to track
        "jobBookmarkKeysSortOrder": "asc"      # Process oldest first
    }
)
# First run: processes all rows
# Next run: only rows where updated_at > last bookmark value
```

### Bookmark Reset and Management

```python
# Reset bookmark to reprocess from scratch
glue.reset_job_bookmark(JobName='daily-orders-etl')

# Get bookmark state (for debugging)
response = glue.get_job_bookmark(JobName='daily-orders-etl')
print(response['JobBookmarkEntry'])
```

### When Bookmarks Don't Work

| Scenario | Problem | Alternative |
|----------|---------|-------------|
| Data is overwritten (not appended) | Bookmark tracks files, not content changes | Use modification timestamp + custom watermark |
| Source has no monotonic key | Can't determine "new" vs "old" | Full reload with dedup |
| Need to reprocess specific dates | Bookmark is all-or-nothing | Custom high-water mark in DynamoDB |

---

## Glue Interactive Sessions and Notebooks

For development without deploying full jobs:

```python
# In Glue Studio Notebook (or locally with Glue Docker image):
%glue_version 4.0
%number_of_workers 5
%worker_type G.1X

# Interactive development with immediate feedback
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Test your transform logic interactively
df = spark.read.parquet("s3://lake/raw/orders/year=2024/month=01/")
df.show(5)
df.printSchema()
# Iterate on transforms before deploying as a production job
```

**Cost optimization:** Interactive sessions charge per-second. Use 2 workers for development, scale up only for the production job.

---

## Monitoring and Debugging

### CloudWatch Metrics for Glue Jobs

| Metric | What It Shows | Alert When |
|--------|--------------|-----------|
| `glue.driver.aggregate.bytesRead` | Data volume read | Unexpectedly high (reading too much) |
| `glue.driver.aggregate.elapsedTime` | Job duration | > 2x normal duration |
| `glue.ALL.system.cpuSystemLoad` | CPU utilization | Consistently < 20% (over-provisioned) |
| `glue.driver.jvm.heap.usage` | Memory pressure | > 80% (risk of OOM) |
| `glue.driver.aggregate.numCompletedTasks` | Task progress | Stalled (no progress for 10+ min) |

### Spark UI for Glue

```python
# Enable Spark UI logging
job_args = {
    '--enable-spark-ui': 'true',
    '--spark-event-logs-path': 's3://glue-logs/sparkui/'
}
# After job runs: view Spark UI in Glue console
# Look for: stage durations, task skew, shuffle sizes, spill metrics
```

### Common Glue Job Failures

| Error | Cause | Fix |
|-------|-------|-----|
| `OutOfMemoryError` | Data exceeds worker memory | Increase to G.2X or add workers |
| `Timeout` (job exceeds max runtime) | Too much data or bad partition pruning | Add pushdown predicates |
| `Connection refused` (JDBC) | VPC/security group misconfigured | Check Glue connection VPC settings |
| `S3 Access Denied` | IAM role missing permissions | Update Glue role policy |
| `Schema mismatch` | Source schema changed | Update catalog or use DynamicFrame resolveChoice |

---

## Glue vs Other AWS ETL Services

| Feature | AWS Glue | EMR | Lambda | Athena CTAS |
|---------|---------|-----|--------|-------------|
| Serverless | Yes | No (manage cluster) | Yes | Yes |
| Spark support | Yes (PySpark) | Yes (full ecosystem) | No | No |
| Max job duration | 48 hours | Unlimited | 15 min | Query timeout |
| Cost model | Per DPU-hour | Per EC2 hour | Per invocation | Per TB scanned |
| Catalog integration | Native | Manual | N/A | Native |
| Best for | Medium ETL (GB-TB) | Large ETL (TB+), custom tools | Small transforms, orchestration | SQL-based transforms |
| Auto-scaling | Yes (Glue 3.0+) | Yes (managed scaling) | Automatic | Automatic |

---

## Security Best Practices

```python
# Use IAM roles (not access keys)
# Glue role needs: S3 access, Catalog access, CloudWatch logs, VPC (if accessing private resources)

# Encrypt data at rest
glue.create_security_configuration(
    Name='glue-encryption',
    EncryptionConfiguration={
        'S3Encryption': [{'S3EncryptionMode': 'SSE-KMS', 'KmsKeyArn': 'arn:aws:kms:...'}],
        'CloudWatchEncryption': {'CloudWatchEncryptionMode': 'SSE-KMS', 'KmsKeyArn': '...'},
        'JobBookmarksEncryption': {'JobBookmarksEncryptionMode': 'CSE-KMS', 'KmsKeyArn': '...'}
    }
)

# Network isolation: run Glue in VPC for private data sources
# Glue connection specifies: VPC, subnet, security group
# Needs NAT Gateway for S3 access (or VPC endpoint)
```

---

## Interview Tips

> **Tip 1:** "How do you scale a Glue job for large datasets?" — "Three approaches: (1) Increase worker count (more DPUs = more parallelism). (2) Use Glue 4.0 auto-scaling (starts small, scales to workload automatically). (3) Add partition pushdown predicates to read less data. (4) Switch from G.1X to G.2X workers for memory-intensive transforms."

> **Tip 2:** "How do you handle schema evolution in Glue?" — "DynamicFrame handles mixed types gracefully (resolveChoice for ambiguous columns). For catalog schema updates: crawlers detect new columns automatically, or manually add columns with ALTER TABLE. For downstream compatibility: only add columns (backward compatible), never remove or rename without versioning."

> **Tip 3:** "Describe a production Glue pipeline" — "Schedule triggered by EventBridge (when upstream data is ready). Job reads from catalog with partition pushdown. Bookmarks ensure incremental processing. DynamicFrame reads, converts to DataFrame for transforms. Quality checks (DQDL rules) validate output. Write to Iceberg table in curated zone. Catalog auto-updated. CloudWatch alerts on failure."

## ⚡ Cheat Sheet

**Worker Types & Cost**
| Worker | vCPU | Memory | Price/DPU-hr |
|---|---|---|---|
| Standard | 4 | 16 GB | $0.44 |
| G.1X | 4 | 16 GB | $0.44 |
| G.2X | 8 | 32 GB | $0.88 |
| G.4X | 16 | 64 GB | $1.76 |
- Auto-scaling (Glue 3.0+): set `NumberOfWorkers=2` (min) + `MaxCapacity=50`; pay only for DPU-time used

**Bookmark Gotchas**
- Works on: S3 file modification time, JDBC monotonic timestamp column
- Does NOT work on: overwritten files, sources without monotonic keys
- Reset bookmark to reprocess: `glue.reset_job_bookmark(JobName='...')`
- Custom high-water mark in DynamoDB for per-partition or per-date control

**Glue vs Alternatives**
| | Glue | EMR | Lambda | Athena CTAS |
|---|---|---|---|---|
| Max duration | 48 hr | Unlimited | 15 min | Query timeout |
| Best for | Med ETL (GB–TB) | TB+, custom tools | Small transforms | SQL transforms |
| Cost model | DPU-hr | EC2-hr | Per invocation | Per TB scanned |

**Common Failures & Fixes**
- `OutOfMemoryError` → upgrade to G.2X or add workers
- `Connection refused` (JDBC) → Glue connection must have self-referencing SG rule (ALL TCP from itself)
- `S3 Access Denied` → check Glue IAM role (needs `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`)
- `Schema mismatch` → use `DynamicFrame.resolveChoice()` or update catalog with `ALTER TABLE`

**Iceberg Integration (Glue 4.0)**
- Read: `create_dynamic_frame.from_catalog(..., "useSparkDataSourceFormat": True)`
- Write: `spark.sql("MERGE INTO ... USING ... WHEN MATCHED ... WHEN NOT MATCHED ...")`
- Time travel: `SELECT * FROM table FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00'`

**Security Best Practices**
- Always use `SecurityConfiguration` with SSE-KMS for S3, CloudWatch, and bookmarks
- VPC connection for private data sources: subnet + SG with self-referencing rule + NAT GW or VPC endpoint for S3
- Triggered by EventBridge (data-ready event) > fixed schedule (avoids wasted runs)
