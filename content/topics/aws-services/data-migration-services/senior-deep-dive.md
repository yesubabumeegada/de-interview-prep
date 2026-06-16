---
title: "AWS Data Migration Services: Senior Deep Dive"
description: "Large-scale migration architecture patterns, cutover strategies, DMS tuning, and validation frameworks for senior engineers"
content_type: study_material
topic: aws-services
subtopic: data-migration-services
layer: senior-deep-dive
difficulty_level: senior
tags: [aws, dms, data-migration, snowball, sct, database-migration]
---

# AWS Data Migration Services: Senior Deep Dive

## Large-Scale Migration Architecture Patterns

### Oracle → Aurora PostgreSQL Migration

This is one of the most common enterprise migration paths. Oracle licensing costs and the desire for cloud-native databases drive most of these migrations.

#### SCT Assessment Phase

The AWS Schema Conversion Tool (SCT) performs a compatibility assessment before any data moves.

```
SCT Assessment Workflow:
1. Connect SCT to source Oracle database
2. Run full schema assessment
3. Review SCT report output:
   - Tables: typically 95%+ auto-converted
   - Views: 80-90% auto-converted
   - Stored procedures: 40-70% auto-converted (PL/SQL → PL/pgSQL)
   - Sequences: need manual adjustment (Oracle NEXTVAL vs PostgreSQL syntax)
   - Synonyms: no direct equivalent in Aurora PostgreSQL
4. Prioritize manual conversion items by business criticality
5. Export SCT conversion scripts
```

**Common SCT conversion challenges for Oracle → Aurora PostgreSQL:**

| Oracle Feature | PostgreSQL Equivalent | Notes |
|---|---|---|
| ROWNUM | ROW_NUMBER() OVER() | Different semantics |
| SYSDATE | CURRENT_TIMESTAMP | Minor syntax change |
| NVL() | COALESCE() | Drop-in replacement |
| CONNECT BY | Recursive CTEs (WITH RECURSIVE) | Significant rewrite |
| Outer join syntax (+) | Standard ANSI JOINs | SCT handles this |
| DECODE() | CASE WHEN | SCT handles this |
| DBMS_* packages | pg_* equivalents or custom functions | Manual rewrite required |
| Sequences (dual table) | Sequences (native) | Syntax differs |

#### Stored Procedure Conversion

SCT auto-converts a percentage of stored procedures, but the remainder requires manual effort.

```sql
-- Oracle PL/SQL example
CREATE OR REPLACE PROCEDURE update_order_status(
  p_order_id IN NUMBER,
  p_status IN VARCHAR2
) AS
BEGIN
  UPDATE orders SET status = p_status WHERE order_id = p_order_id;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;

-- Aurora PostgreSQL equivalent (SCT-converted)
CREATE OR REPLACE PROCEDURE update_order_status(
  p_order_id INTEGER,
  p_status VARCHAR
) LANGUAGE plpgsql AS $$
BEGIN
  UPDATE orders SET status = p_status WHERE order_id = p_order_id;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
$$;
```

**Sequence generator differences require special attention:**

```sql
-- Oracle: sequences are separate objects, referenced via DUAL
SELECT my_seq.NEXTVAL FROM DUAL;

-- PostgreSQL: sequences are integrated with SERIAL or GENERATED ALWAYS AS IDENTITY
CREATE TABLE orders (
  order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ...
);

-- Or use explicit sequence
CREATE SEQUENCE order_id_seq START WITH 1 INCREMENT BY 1;
SELECT nextval('order_id_seq');
```

After migration, sequences must be reset to MAX(id) + 1 to avoid primary key conflicts.

#### DMS Task Setup with LOB Handling

Large Object (LOB) handling is the most common source of DMS task failures and performance issues.

```json
{
  "TaskSettings": {
    "TargetMetadata": {
      "TargetSchema": "",
      "SupportLobs": true,
      "FullLobMode": false,
      "LobChunkSize": 64,
      "LimitedSizeLobMode": true,
      "LobMaxSize": 32768
    },
    "FullLoadSettings": {
      "TargetTablePrepMode": "DO_NOTHING",
      "CreatePkAfterFullLoad": false,
      "StopTaskCachedChangesApplied": false,
      "StopTaskCachedChangesNotApplied": false,
      "MaxFullLoadSubTasks": 8,
      "TransactionConsistencyTimeout": 600,
      "CommitRate": 50000
    },
    "Logging": {
      "EnableLogging": true
    }
  }
}
```

**LOB mode selection:**

| Mode | When to Use | Performance | Risk |
|---|---|---|---|
| Limited LOB mode | LOBs < known max size | Fastest | Truncation if LOB exceeds limit |
| Full LOB mode | LOBs of unknown/large size | Slowest | None |
| Inline LOB mode | LOBs < 64KB (common case) | Fast | None for small LOBs |

**Rule of thumb:** Use limited LOB mode with 64KB limit for most OLTP databases. Switch to full LOB mode only if you have large documents (BLOBs, CLOBs > 64KB).

#### Parallel Table Load Configuration

For large tables, parallel loading dramatically reduces full load time.

```json
{
  "rules": [
    {
      "rule-type": "table-settings",
      "rule-id": "1",
      "rule-name": "parallel-load-orders",
      "object-locator": {
        "schema-name": "myschema",
        "table-name": "orders"
      },
      "parallel-load": {
        "type": "ranges",
        "columns": ["order_id"],
        "boundaries": [
          ["1000000"],
          ["2000000"],
          ["3000000"],
          ["4000000"]
        ]
      }
    }
  ]
}
```

**Parallel load strategies:**

1. **Partition-based:** Use existing table partitions (best for partitioned tables)
2. **PK range segmentation:** Divide by primary key ranges (best for sequential integer PKs)
3. **Subquery segmentation:** Custom WHERE clause segments (most flexible)

```json
{
  "parallel-load": {
    "type": "partitions-auto",
    "number-of-partitions": 16,
    "collection-count-from-metadata": "true"
  }
}
```

#### Cutover Runbook

```
PRE-CUTOVER (T-24h):
□ Verify DMS task running with CDC latency < 30s
□ Run row count validation on all tables
□ Complete application smoke tests on Aurora
□ Notify stakeholders of maintenance window
□ Prepare rollback procedure

CUTOVER START (T-0, maintenance window begins):
□ Stop application writes to Oracle
□ Monitor DMS CDC latency drop to 0
□ Verify final row counts match

VALIDATION (T+30min):
□ Run automated validation suite
□ Spot-check critical business data
□ Test application connectivity to Aurora
□ Verify stored procedures execute correctly

GO LIVE (T+1h):
□ Update application connection strings to Aurora
□ Restart application services
□ Monitor error rates and latency for 30 minutes
□ Confirm business-critical transactions succeeding

ROLLBACK TRIGGER (if needed within T+2h):
□ Revert application connection strings to Oracle
□ Oracle was never written to during cutover, so no data loss
□ Investigate DMS issues before retry
```

---

### Teradata → Redshift Migration

Teradata to Redshift is a common data warehouse modernization path. This migration is different from OLTP migrations because DMS is generally not suitable for large Teradata environments.

#### Why DMS Falls Short for Teradata at Scale

DMS supports Teradata as a source but has significant limitations:
- No parallel unload capability
- Row-by-row extraction is slow for multi-TB tables
- BTEQ script conversion must go through SCT separately
- FastLoad/MultiLoad scripts need manual conversion

**Recommended approach: Teradata UNLOAD → S3 → Redshift COPY**

```sql
-- Teradata: export to file using FastExport
.EXPORT OUTFILE /tmp/orders.csv MODE RECORD FORMAT TEXT;
SELECT CAST(order_id AS CHAR(20))
     ||','|| CAST(customer_id AS CHAR(20))
     ||','|| CAST(order_date AS CHAR(26))
     ||','|| CAST(total_amount AS CHAR(20))
FROM orders;
.END EXPORT

-- Or use Teradata Parallel Transporter (TPT) for large tables
-- TPT EXPORT operator supports parallel streams
```

#### SCT for BTEQ/FastLoad Conversion

AWS SCT can analyze BTEQ scripts and convert them to AWS-compatible equivalents.

```sql
-- Original BTEQ script (Teradata)
.LOGON tdserver/user,password;
SEL * FROM database.orders WHERE order_date >= '2024-01-01';
.EXPORT FILE = orders_export.csv;
.LOGOFF;

-- SCT converts to Python/boto3 or Redshift COPY equivalent
-- Manual review required for complex BTEQ control flow
```

**FastLoad → Redshift COPY mapping:**

| Teradata | Redshift Equivalent |
|---|---|
| FastLoad | COPY command |
| MultiLoad (upsert) | MERGE statement |
| TPT Export | UNLOAD |
| BTEQ | Redshift stored procedures or scripts |

#### Schema Mapping and Distribution Key Selection

Distribution key selection is the most critical architectural decision for Redshift performance.

```sql
-- Analyze query patterns before choosing distribution keys
-- Common join pattern: orders join customers join products

-- Table: orders (largest fact table)
CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  order_date DATE,
  total_amount DECIMAL(18,2)
)
DISTKEY(customer_id)    -- distributes rows to nodes by customer_id
SORTKEY(order_date);    -- sorts rows within each node for range scans

-- Table: customers (dimension, smaller)
CREATE TABLE customers (
  customer_id BIGINT NOT NULL,
  customer_name VARCHAR(255),
  region VARCHAR(50)
)
DISTSTYLE ALL;          -- replicate to all nodes (small dimension table)

-- Use Redshift Advisor for recommendations:
SELECT * FROM svv_table_info WHERE "table" = 'orders';
```

**Distribution style decision tree:**

```
Table size < 1M rows → DISTSTYLE ALL (replicate everywhere)
Table is fact table joining on specific key → DISTKEY(join_key)
Table has no clear join key → DISTSTYLE EVEN (round-robin)
```

---

### On-Premises Hadoop → S3/Glue/Lake Formation

This migration replaces HDFS storage with S3 and the Hadoop ecosystem with AWS-native services.

#### AWS DataSync for HDFS Migration

DataSync supports HDFS as a source location, enabling direct migration without intermediate steps.

```
DataSync HDFS Configuration:
- Source: HDFS cluster (NameNode address, port 8020)
- Authentication: Simple (HDFS user) or Kerberos
- Agent: Deploy DataSync agent on-prem or near cluster
- Destination: S3 bucket with appropriate prefix structure
- Bandwidth limit: Configure to avoid impacting production Hadoop jobs
- Verification: DataSync performs end-to-end integrity checks
```

**Parallel copy strategies for large HDFS datasets:**

```bash
# Option 1: DataSync with multiple tasks (parallel by directory)
# Create separate DataSync tasks per HDFS directory

# Option 2: S3DistCp for Hadoop-native parallel transfer
hadoop jar /usr/lib/hadoop/tools/lib/emr-s3-dist-cp.jar \
  --src hdfs:///data/warehouse/ \
  --dest s3://my-datalake-bucket/warehouse/ \
  --outputCodec snappy \
  --deleteOnSuccess

# Option 3: AWS Transfer for SFTP (for smaller datasets)
```

#### Metadata Migration with Glue Crawlers

HDFS data has metadata in the Hive Metastore. Migrating this to AWS Glue Data Catalog is essential for query compatibility.

```python
# Export Hive Metastore schema
# Run on source cluster:
mysqldump -h hive-metastore-host -u hive -p hive_metastore > hive_metastore_dump.sql

# Option 1: Use AWS Glue Crawlers to re-discover schema from S3 data
import boto3

glue = boto3.client('glue', region_name='us-east-1')

response = glue.create_crawler(
    Name='warehouse-crawler',
    Role='arn:aws:iam::123456789:role/GlueCrawlerRole',
    DatabaseName='warehouse_db',
    Targets={
        'S3Targets': [
            {
                'Path': 's3://my-datalake-bucket/warehouse/',
                'Exclusions': ['**/_temporary/**']
            }
        ]
    },
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'LOG'
    },
    RecrawlPolicy={
        'RecrawlBehavior': 'CRAWL_EVERYTHING'
    }
)

# Option 2: Use AWS Glue Database Migration scripts for exact schema preservation
# Useful when column types, comments, and partition definitions must be preserved exactly
```

---

## Cutover Strategy Deep Dive

### Blue-Green Cutover

Blue-green cutover maintains two parallel environments until the moment of switch.

```
Blue (current production) → Oracle on-premises
Green (new environment)   → Aurora PostgreSQL on AWS

Timeline:
Week 1-8:   Migrate schema, data, stored procedures to Green
Week 9-12:  Parallel run — both environments receive updates via dual-write
            or DMS CDC keeps Green in sync from Blue
Week 13:    Cutover day
            - Blue traffic continues (users unaffected)
            - DMS lag drains to < 5 seconds
            - 30-minute maintenance window
            - Traffic DNS/load balancer switch: Blue → Green
            - Green becomes production
Week 13-15: Blue remains available (rollback capability)
Week 16:    Blue decommissioned
```

**Rollback plan:**
- Keep Blue environment running for 2 weeks post-cutover
- DNS TTL set to 60 seconds for fast rollback
- Rollback trigger: P1 incident affecting revenue or data integrity
- Rollback SLA: < 30 minutes

### Staged Migration: Read Replicas First

This approach migrates read traffic first to validate the new system before migrating writes.

```
Stage 1: Migrate read traffic
- DMS replicates from Oracle (source) to Aurora (target)
- Aurora is read-only replica from application perspective
- Route SELECT queries (reporting, analytics) to Aurora
- Validate: reports match between Oracle and Aurora

Stage 2: Validate write behavior
- Test insert/update/delete against Aurora in non-production environment
- Validate triggers, stored procedures, constraints

Stage 3: Migrate write traffic
- Short maintenance window (1-2 hours)
- Stop Oracle writes
- Drain DMS lag to 0
- Switch application writes to Aurora
- Resume operations
```

**Advantage:** Reduces risk by validating read paths weeks before write cutover. Issues are caught without impacting production writes.

### Zero-Downtime CDC-Based Cutover

The most sophisticated approach eliminates the maintenance window entirely.

```
Prerequisites:
- DMS CDC running with < 5s lag for 72+ consecutive hours
- All validation checks passing
- Application supports dual connection configuration

Cutover sequence (zero downtime):
T-5min:  Confirm CDC lag < 5s
T-0:     Enable dual-write mode in application
         (writes go to BOTH Oracle and Aurora)
T+10min: Validate writes landing correctly on Aurora
T+30min: Disable Oracle writes in application
         (Aurora is now sole write target)
T+60min: Disable DMS task (CDC no longer needed)
T+90min: Oracle goes read-only, migration complete

Result: Zero downtime, continuous write availability
```

**Limitation:** Dual-write mode requires application support and adds latency for the duration of the cutover window.

---

## DMS Performance Tuning

### Parallel Load Configuration

```json
{
  "FullLoadSettings": {
    "MaxFullLoadSubTasks": 8,
    "CommitRate": 50000,
    "TransactionConsistencyTimeout": 600
  },
  "ControlTablesSettings": {
    "historyTimeslotInMinutes": 5,
    "StatusTableEnabled": true,
    "CdcInsertsOnly": false
  }
}
```

**Segmentation strategies by table characteristics:**

```json
// For tables with sequential integer PKs:
{
  "parallel-load": {
    "type": "ranges",
    "columns": ["id"],
    "boundaries": [
      ["1000000"],
      ["2000000"],
      ["3000000"]
    ]
  }
}

// For partitioned tables (best performance):
{
  "parallel-load": {
    "type": "partitions-auto"
  }
}

// For tables without natural segmentation keys:
{
  "parallel-load": {
    "type": "subquery",
    "number-of-subqueries": 8
  }
}
```

### LOB Mode Deep Dive

**Limited LOB mode (default, fastest):**
- DMS reads LOBs up to the configured max size
- If LOB exceeds limit, it is truncated (data loss risk)
- Use when you know maximum LOB size from application analysis
- Typical setting: 64KB (covers most application-stored documents)

**Full LOB mode (slowest, safest):**
- DMS reads LOBs in chunks, regardless of size
- Requires two passes: one for regular columns, one for LOB columns
- 3-5x slower than limited LOB mode for LOB-heavy tables
- Use when LOB sizes are unknown or confirmed to be large

**Inline LOB mode (best of both worlds for small LOBs):**
- LOBs below threshold are loaded inline with the row (single pass)
- LOBs above threshold fall back to full LOB mode
- Threshold: configurable, default 64KB
- Best performance when most LOBs are small with occasional large ones

### Replication Instance Sizing

| Workload | Instance Class | Notes |
|---|---|---|
| < 100GB full load | dms.t3.medium | Development/testing |
| 100GB-1TB full load | dms.r5.xlarge | Standard production |
| 1TB-10TB full load | dms.r5.4xlarge | Large migrations |
| > 10TB or high CDC rate | dms.r5.8xlarge | Enterprise scale |

**Multi-AZ vs Single-AZ:**
- Single-AZ: 30% cheaper, acceptable for migrations (not long-running replication)
- Multi-AZ: automatic failover, use for ongoing replication exceeding 30 days
- DMS tasks must be restarted after failover (automatic in Multi-AZ)

**Network tuning:**
- Place replication instance in same VPC as target database
- Use VPC endpoints or Direct Connect for source connectivity
- Enable Enhanced VPC routing in Redshift for better COPY performance

### Connection Pooling and Network Tuning

```json
{
  "ConnectionAttributes": {
    "parallelLoadThreads": "8",
    "parallelLoadBufferSize": "100",
    "parallelLoadQueuesPerThread": "16"
  }
}
```

**Key network metrics to monitor:**

```
CloudWatch Metrics for DMS:
- NetworkTransmitThroughput: bytes/sec leaving replication instance
- NetworkReceiveThroughput: bytes/sec arriving at replication instance
- CDCLatencySource: lag between source changes and DMS reading them
- CDCLatencyTarget: lag between DMS reading and applying to target
- FullLoadRowsInserted: rows inserted per second during full load
```

---

## Migration Testing and Validation Framework

### Row Count Validation

The simplest and fastest validation — run this after every phase.

```sql
-- Source (Oracle)
SELECT table_name, num_rows
FROM user_tables
ORDER BY table_name;

-- Target (Aurora PostgreSQL)
SELECT
  schemaname,
  tablename,
  n_live_tup AS estimated_rows
FROM pg_stat_user_tables
ORDER BY tablename;

-- For exact counts (slow but accurate):
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM orders
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products;
```

### Checksum Validation with DMS Data Validation Task

DMS has built-in data validation that compares source and target row-by-row.

```json
{
  "ValidationSettings": {
    "EnableValidation": true,
    "ValidationMode": "ROW_LEVEL",
    "ThreadCount": 5,
    "FailureMaxCount": 10000,
    "HandleCollationDiff": false,
    "ValidationQueryCdcDelaySeconds": 0,
    "ValidationOnly": false,
    "TableFailureMaxCount": 1000
  }
}
```

DMS validation writes results to the `awsdms_validation_failures_v1` control table on the target.

```sql
-- Check validation results
SELECT
  table_owner,
  table_name,
  validation_status,
  validation_suspend_reason
FROM awsdms_validation_failures_v1
WHERE validation_status != 'Validated'
ORDER BY table_name;
```

### Sample Data Spot Checks

Automated checksums must be supplemented with business-logic validation.

```python
import psycopg2
import cx_Oracle

def validate_revenue_reconciliation(oracle_conn, aurora_conn, date_range):
    """
    Validate that revenue figures match between source and target.
    This catches transformation errors that row counts would miss.
    """
    query = """
    SELECT
      DATE_TRUNC('month', order_date) AS month,
      SUM(total_amount) AS total_revenue,
      COUNT(*) AS order_count,
      AVG(total_amount) AS avg_order_value
    FROM orders
    WHERE order_date BETWEEN %s AND %s
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY month
    """

    oracle_results = execute_query(oracle_conn, query, date_range)
    aurora_results = execute_query(aurora_conn, query, date_range)

    discrepancies = []
    for month, oracle_row in oracle_results.items():
        aurora_row = aurora_results.get(month)
        if not aurora_row:
            discrepancies.append(f"Month {month}: missing in target")
            continue
        if abs(oracle_row['total_revenue'] - aurora_row['total_revenue']) > 0.01:
            discrepancies.append(
                f"Month {month}: revenue mismatch "
                f"Oracle={oracle_row['total_revenue']} "
                f"Aurora={aurora_row['total_revenue']}"
            )

    return discrepancies
```

### Performance Benchmark: Pre vs Post Migration

```python
# Benchmark critical queries before and after migration
benchmark_queries = [
    {
        "name": "Monthly revenue report",
        "oracle_sql": "SELECT ...",  # Oracle syntax
        "aurora_sql": "SELECT ...",  # PostgreSQL syntax
        "sla_ms": 5000               # Must complete in 5 seconds
    },
    {
        "name": "Customer order history",
        "oracle_sql": "SELECT ...",
        "aurora_sql": "SELECT ...",
        "sla_ms": 1000
    }
]

# Run each query 10 times and compare P50/P95/P99 latencies
# Target: Aurora P95 <= Oracle P95 * 1.2 (20% tolerance)
```

---

## Post-Migration Reconciliation

### AWS Glue DataBrew for Data Quality Checks

DataBrew provides no-code data quality profiling on the migrated dataset.

```python
import boto3

databrew = boto3.client('databrew', region_name='us-east-1')

# Create DataBrew dataset pointing to Aurora (via JDBC)
dataset = databrew.create_dataset(
    Name='orders-post-migration',
    Format='CUSTOM',
    FormatOptions={},
    Input={
        'DatabaseInputDefinition': {
            'GlueConnectionName': 'aurora-postgresql-connection',
            'DatabaseTableName': 'orders',
            'TempDirectory': {
                'Bucket': 'my-databrew-temp',
                'Key': 'temp/'
            }
        }
    }
)

# Create profile job to analyze data quality
profile_job = databrew.create_profile_job(
    Name='orders-quality-profile',
    DatasetName='orders-post-migration',
    OutputLocation={
        'Bucket': 'my-databrew-output',
        'Key': 'profiles/'
    },
    RoleArn='arn:aws:iam::123456789:role/DataBrewRole'
)
```

DataBrew automatically detects: null percentages, duplicate rows, data type mismatches, value distributions, min/max ranges.

### DMS Validation Reports

```bash
# Pull DMS validation summary via CLI
aws dms describe-replication-task-assessment-results \
  --replication-task-arn arn:aws:dms:us-east-1:123456789:task:ABCDEF \
  --query 'ReplicationTaskAssessmentResults[0].AssessmentResultsFile'
```

---

## AWS MGN vs DMS

### AWS Application Migration Service (MGN)

MGN performs block-level replication of entire servers — it migrates the operating system, applications, and databases together.

**How MGN works:**
1. Install AWS Replication Agent on source server
2. Agent continuously replicates block-level changes to AWS staging area
3. At cutover: launch EC2 instance from replicated snapshot
4. Post-cutover: convert to native AWS instance (change instance type, enable features)

**Use cases for MGN:**
- Lift-and-shift entire application servers
- When database cannot be separated from its application layer
- Legacy applications where re-platforming is not feasible
- COTS (commercial off-the-shelf) software that must run on specific OS

### DMS vs MGN Decision Matrix

| Factor | DMS | MGN |
|---|---|---|
| Granularity | Database-level (logical) | Server-level (block) |
| Schema conversion | Yes (via SCT) | No |
| Target platform change | Yes (Oracle → Aurora) | No (same OS/DB) |
| Network bandwidth | Lower (logical changes only) | Higher (all block changes) |
| Migration complexity | Higher | Lower |
| Use case | Re-platforming | Lift-and-shift |

### Using MGN and DMS Together

A common pattern for complex migrations:
1. Use MGN to lift-and-shift the application server to EC2
2. Use DMS to migrate the database from on-prem to RDS/Aurora
3. Update application configuration on the EC2 instance to point to RDS
4. Result: application modernized (managed DB) without full re-architecture

```
On-premises:         AWS:
App Server ──MGN──→  EC2 Instance
Oracle DB  ──DMS──→  Aurora PostgreSQL
                     EC2 connects to Aurora
```

This approach decouples the application migration from the database migration, reducing risk and allowing parallel work streams.
