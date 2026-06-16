---
title: "AWS Data Migration Services: Real-World Case Studies"
description: "Production migration case studies covering Oracle to Aurora, Teradata to Redshift, and Snowball bulk transfers with lessons learned"
content_type: study_material
topic: aws-services
subtopic: data-migration-services
layer: real-world
difficulty_level: senior
tags: [aws, dms, data-migration, snowball, sct, database-migration]
---

# AWS Data Migration Services: Real-World Case Studies

## Case Study 1: Oracle → Aurora PostgreSQL (OLTP Migration)

### Background

A financial services company ran a 200GB Oracle 19c OLTP database supporting their core transaction processing platform. Oracle licensing costs exceeded $400K/year. The goal was to migrate to Aurora PostgreSQL to reduce costs and modernize the database layer.

**Source environment:**
- Oracle 19c Enterprise Edition (on-premises)
- 200GB total data size
- 150+ stored procedures (PL/SQL)
- 80 tables, 12 schemas
- 500 concurrent connections at peak
- 99.9% uptime SLA

### Phase 1: Assessment (2 weeks)

**SCT assessment results:**

```
Tables:           95% auto-converted (76 of 80)
Views:            88% auto-converted
Stored procedures: 68% auto-converted (102 of 150)
Triggers:         45% auto-converted (manual review required)
Functions:        72% auto-converted
Sequences:        100% identified, 100% manual adjustment needed
```

The 30% of stored procedures requiring manual conversion were triaged by business priority:
- 12 procedures: critical path (payment processing) → converted first
- 38 procedures: high priority (reporting) → converted weeks 3-4
- 10 procedures: low priority (batch jobs) → converted last

**Key complexity drivers:**
- CONNECT BY hierarchical queries (5 procedures) → rewritten as recursive CTEs
- DBMS_SCHEDULER jobs → migrated to Amazon EventBridge + Lambda
- Oracle UTL_FILE for file I/O → replaced with S3 SDK calls
- DBMS_CRYPTO encryption → replaced with pgcrypto extension

### Phase 2: Schema and Stored Procedure Migration (6 weeks)

**Sequence generator handling was the most time-consuming manual task:**

```sql
-- Oracle: each table uses a sequence + trigger for auto-increment
CREATE SEQUENCE transaction_id_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_transaction_id
BEFORE INSERT ON transactions
FOR EACH ROW
BEGIN
  IF :new.transaction_id IS NULL THEN
    SELECT transaction_id_seq.NEXTVAL INTO :new.transaction_id FROM DUAL;
  END IF;
END;

-- PostgreSQL: use GENERATED ALWAYS AS IDENTITY (modern approach)
CREATE TABLE transactions (
  transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ...
);

-- After migration, reset sequence to avoid conflicts:
SELECT setval(
  pg_get_serial_sequence('transactions', 'transaction_id'),
  (SELECT MAX(transaction_id) FROM transactions)
);
```

**TIMESTAMP WITH TIME ZONE handling required explicit mapping:**

```sql
-- Oracle stores timezone-aware timestamps
-- Field type: TIMESTAMP(6) WITH TIME ZONE

-- Aurora PostgreSQL equivalent:
-- TIMESTAMP WITH TIME ZONE (stores UTC, displays in session timezone)

-- DMS mapping rule in table-mappings JSON:
{
  "rule-type": "transformation",
  "rule-id": "10",
  "rule-name": "timestamp-conversion",
  "rule-action": "convert-uppercase",
  "rule-target": "column",
  "object-locator": {
    "schema-name": "%",
    "table-name": "%",
    "column-name": "%_DT"
  },
  "data-type": {
    "type": "datetime",
    "precision": 6,
    "timezone": "UTC"
  }
}
```

### Phase 3: DMS Migration Setup

**Replication instance:** dms.r5.2xlarge (Multi-AZ, 8 vCPU, 64GB RAM)

**Task configuration for 200GB OLTP database:**

```json
{
  "FullLoadSettings": {
    "MaxFullLoadSubTasks": 8,
    "CommitRate": 10000,
    "TransactionConsistencyTimeout": 600
  },
  "TargetMetadata": {
    "SupportLobs": true,
    "LimitedSizeLobMode": true,
    "LobMaxSize": 65536,
    "FullLobMode": false
  },
  "Logging": {
    "EnableLogging": true,
    "LogComponents": [
      {"Id": "SOURCE_UNLOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
      {"Id": "TARGET_LOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
      {"Id": "TASK_MANAGER", "Severity": "LOGGER_SEVERITY_DEFAULT"}
    ]
  }
}
```

**LOB handling decision:** The Oracle database contained BLOB columns storing scanned documents. Analysis showed 92% of BLOBs were under 32KB (check records, signature images). Used inline LOB mode with 64KB threshold — 92% loaded in single pass, 8% fell back to full LOB mode automatically.

**Full load performance:**
- 80 tables loaded in 6 hours 20 minutes
- Peak throughput: 45 MB/s
- Parallel load: 8 segments on the 3 largest tables (transactions: 120GB)

### Phase 4: Parallel Run and Validation

**Row count validation (automated, ran hourly during parallel run):**

```python
import psycopg2
import cx_Oracle
import boto3

def run_row_count_validation():
    tables_to_check = ['transactions', 'accounts', 'customers', 'payments']
    discrepancies = []

    for table in tables_to_check:
        oracle_count = get_count(oracle_conn, table)
        aurora_count = get_count(aurora_conn, table)

        delta = abs(oracle_count - aurora_count)
        delta_pct = delta / oracle_count * 100 if oracle_count > 0 else 0

        if delta_pct > 0.001:  # Tolerance: 0.001% (CDC lag acceptable)
            discrepancies.append({
                'table': table,
                'oracle': oracle_count,
                'aurora': aurora_count,
                'delta_pct': delta_pct
            })

    return discrepancies
```

**Business validation (ran daily):**
- Daily transaction totals matched within $0.01 (rounding difference in currency handling — fixed by adjusting NUMERIC precision)
- Account balance reconciliation: 100% match
- 30-day payment history spot check: 100% match

### Phase 5: Cutover

**Cutover window:** Saturday 2:00 AM - 6:00 AM EST (4-hour maintenance window)

```
02:00 - Application maintenance mode activated (no new transactions)
02:05 - DMS CDC latency: 8 seconds
02:15 - DMS CDC latency: 2 seconds
02:22 - DMS CDC latency: 0 seconds (fully caught up)
02:25 - Final row count validation: ALL TABLES MATCH
02:30 - Application connection strings updated to Aurora
02:35 - Application services restarted
02:45 - Smoke tests completed: payment processing, account lookup, reporting
02:50 - Operations team confirms: no errors, normal latency
02:55 - Maintenance mode deactivated
03:00 - Cutover complete (1 hour ahead of schedule)
```

**Post-cutover metrics (first 48 hours):**
- Transaction throughput: 103% of Oracle baseline (slight improvement)
- P99 query latency: 94% of Oracle baseline (improvement from query planner)
- Error rate: 0.002% (2 stored procedures with edge case failures — fixed same day)
- Zero data loss

### Results

| Metric | Before (Oracle) | After (Aurora PostgreSQL) |
|---|---|---|
| Annual DB licensing | $420,000 | $0 |
| Annual Aurora cost | $0 | $48,000 |
| Annual savings | — | **$372,000 (89% reduction)** |
| P99 query latency | 245ms | 198ms |
| Max connections | 500 | 5,000 (connection pooling) |
| Automated backups | Manual RMAN | Automated (35-day retention) |

---

## Case Study 2: On-Premises Data Warehouse → Redshift

### Background

A retail company ran a 10TB Teradata data warehouse supporting merchandising analytics, demand forecasting, and executive reporting. The warehouse was approaching end of life, and the vendor support cost was $600K/year. Migration target: Amazon Redshift.

**Source environment:**
- Teradata 16.20 (on-premises)
- 10TB compressed data (28TB uncompressed)
- 400 tables across 15 schemas
- 200+ BTEQ scripts (scheduled ETL)
- 50 FastLoad/MultiLoad jobs
- 30 BI reports (Tableau connecting to Teradata ODBC)

### Why DMS Was Not Used

Initial plan included DMS for data movement. This was abandoned after a proof-of-concept showed:
- DMS Teradata connector extracted at ~200 rows/second per table (sequential)
- At that rate, 10TB would take 3+ weeks for full load
- Teradata Parallel Transporter (TPT) extracted the same data in 18 hours
- DMS also had issues with Teradata-specific column types (BYTEINT, PERIOD)

**Final approach: Native Teradata UNLOAD → S3 → Redshift COPY**

### Phase 1: Schema Conversion (4 weeks)

**SCT for schema translation:**

```sql
-- Teradata DDL
CREATE TABLE sales_facts (
  sale_id INTEGER NOT NULL,
  sale_date DATE FORMAT 'YYYY-MM-DD',
  store_id SMALLINT,
  product_id INTEGER,
  quantity BYTEINT,        -- Teradata-specific: 1-byte integer
  unit_price DECIMAL(10,2),
  total_amount DECIMAL(12,2)
)
PRIMARY INDEX (store_id, sale_date);   -- Teradata distribution

-- SCT output: Redshift DDL
CREATE TABLE sales_facts (
  sale_id INTEGER NOT NULL,
  sale_date DATE,
  store_id SMALLINT,
  product_id INTEGER,
  quantity SMALLINT,       -- BYTEINT → SMALLINT
  unit_price DECIMAL(10,2),
  total_amount DECIMAL(12,2)
)
DISTKEY(store_id)          -- Primary index → distribution key
SORTKEY(sale_date);        -- Added based on query analysis
```

**Distribution key analysis using Redshift Advisor:**

```sql
-- After loading sample data, run Advisor recommendations
SELECT
  trim(n.nspname) AS schemaname,
  trim(c.relname) AS tablename,
  decode(c.reldiststyle,0,'EVEN',1,'KEY',8,'ALL') AS diststyle,
  trim(a.attname) AS distkey,
  svv_table_info.skew_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = c.relattnum
JOIN svv_table_info ON svv_table_info.table_id = c.oid
WHERE c.relkind = 'r'
ORDER BY svv_table_info.skew_rows DESC;

-- High skew_rows indicates poor distribution key choice
-- Target: skew_rows < 5%
```

### Phase 2: BTEQ Script Conversion

**200 BTEQ scripts converted to:**
- 120 scripts → SQL scripts running on Redshift
- 60 scripts → AWS Glue Python ETL jobs
- 20 scripts → Amazon EventBridge + Lambda (scheduling logic)

**Example BTEQ → Glue conversion:**

```sql
-- Original BTEQ (Teradata)
.LOGON tdserver/etl_user,password;

DELETE FROM stg_daily_sales WHERE sale_date = :sale_date;

INSERT INTO stg_daily_sales
SELECT * FROM src_pos_system.daily_sales
WHERE sale_date = :sale_date;

COLLECT STATISTICS ON stg_daily_sales COLUMN (sale_date);
.LOGOFF;
```

```python
# AWS Glue Python equivalent
import sys
from awsglue.utils import getResolvedOptions
import boto3

args = getResolvedOptions(sys.argv, ['sale_date'])
sale_date = args['sale_date']

redshift_data = boto3.client('redshift-data')

# Delete staging data
redshift_data.execute_statement(
    ClusterIdentifier='my-redshift-cluster',
    Database='analytics',
    DbUser='etl_user',
    Sql=f"DELETE FROM stg_daily_sales WHERE sale_date = '{sale_date}'"
)

# Load from S3 (data pre-staged by upstream job)
redshift_data.execute_statement(
    ClusterIdentifier='my-redshift-cluster',
    Database='analytics',
    DbUser='etl_user',
    Sql=f"""
    COPY stg_daily_sales
    FROM 's3://etl-bucket/daily-sales/{sale_date}/'
    IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftCopyRole'
    FORMAT AS PARQUET;
    """
)

# ANALYZE replaces COLLECT STATISTICS
redshift_data.execute_statement(
    ClusterIdentifier='my-redshift-cluster',
    Database='analytics',
    DbUser='etl_user',
    Sql="ANALYZE stg_daily_sales"
)
```

### Phase 3: Data Migration

**TPT Export from Teradata to S3 via staging server:**

```bash
# Run TPT Export for each schema (8 parallel streams per table)
tbuild -f export_sales_facts.tpt -v sale_date='2024-01-01'
```

```
-- export_sales_facts.tpt
DEFINE JOB EXPORT_SALES_FACTS
DESCRIPTION 'Export sales_facts to pipe-delimited files'
(
  DEFINE SCHEMA SALES_SCHEMA
  (
    sale_id INTEGER,
    sale_date CHAR(10),
    store_id SMALLINT,
    product_id INTEGER,
    quantity SMALLINT,
    unit_price CHAR(15),
    total_amount CHAR(15)
  );

  DEFINE OPERATOR EXPORT_OP
  TYPE EXPORT
  SCHEMA SALES_SCHEMA
  ATTRIBUTES
  (
    VARCHAR TdpId = 'tdserver',
    VARCHAR UserName = 'etl_user',
    VARCHAR UserPassword = 'password',
    VARCHAR SelectStmt =
      'SELECT sale_id, CAST(sale_date AS CHAR(10)),
              store_id, product_id, quantity,
              CAST(unit_price AS CHAR(15)),
              CAST(total_amount AS CHAR(15))
       FROM sales_facts
       WHERE sale_date >= ''2020-01-01''',
    INTEGER MaxSessions = 8,
    INTEGER MinSessions = 4
  );
  ...
);
```

**Parallel Redshift COPY strategy:**

```sql
-- Load 8 schemas simultaneously (one COPY per schema)
-- Each COPY points to a different S3 prefix
-- Redshift uses Massively Parallel Processing (MPP) to distribute load

-- Schema 1: Sales (largest, 4TB)
COPY sales_facts
FROM 's3://migration-bucket/teradata-export/sales_facts/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftCopyRole'
DELIMITER '|'
DATEFORMAT 'YYYY-MM-DD'
IGNOREHEADER 0
MAXERROR 0
COMPUPDATE ON
STATUPDATE ON;

-- Ran 8 COPY commands simultaneously across 8 Redshift connections
-- Total load time: 4 hours 15 minutes for 10TB
```

### Phase 4: Validation

**Row count validation:** 400 tables, all matched within 0.001% (acceptable — some tables had rows inserted during extraction window)

**Revenue spot check (the business-critical validation):**

```sql
-- Compare monthly revenue: Teradata vs Redshift
-- (Both queries run same day against frozen datasets)

-- Redshift validation query
SELECT
  DATE_TRUNC('month', sale_date) AS month,
  SUM(total_amount) AS monthly_revenue,
  COUNT(*) AS transaction_count
FROM sales_facts
WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month;

-- Compared against identical Teradata query output
-- Result: Revenue figures matched to the penny for all 12 months
-- One discrepancy found: 3 tables had decimal precision issue (Teradata DECIMAL(12,4) → Redshift DECIMAL(12,2))
-- Fix: Re-exported those 3 tables with 4 decimal precision
```

### Timeline and Results

**Project timeline:** 3 months total
- Month 1: Schema conversion, BTEQ migration, Tableau reconnection
- Month 2: Data migration, validation, parallel run
- Month 3 (2 weeks): Parallel run validation, cutover, hypercare

**Parallel run:** 2 weeks with both Teradata and Redshift receiving the same ETL loads nightly

**Cutover:** Single Saturday morning, completed in 3 hours

| Metric | Before (Teradata) | After (Redshift) |
|---|---|---|
| Annual cost | $600,000 | $72,000 |
| Query performance (complex joins) | 45 min avg | 8 min avg |
| Concurrency limit | 40 queries | 500 queries |
| Storage cost | Fixed (hardware) | $0.023/GB/month |
| Maintenance windows | Monthly (Teradata patches) | Zero (AWS managed) |

---

## Case Study 3: 500TB Bulk File Transfer with Snowball

### Background

A media company had 500TB of Hadoop HDFS data: video encoding outputs, metadata files, and analytics datasets in Parquet and ORC format. They needed to migrate to S3 as part of a move from on-premises Hadoop to AWS EMR.

**The network math that ruled out online transfer:**

```
Data size: 500TB = 500,000 GB
Available bandwidth: 1Gbps dedicated line
Effective throughput (accounting for overhead): ~800Mbps = 100 MB/s

Transfer time = 500,000 GB / (100 MB/s × 3600 s/hr × 24 hr/day)
             = 500,000,000 MB / 8,640,000 MB/day
             = 57.87 days

Even with 10Gbps Direct Connect (expensive): ~5.8 days
Business requirement: complete migration in 3 weeks
Network option: ruled out
```

### Snowball Edge Configuration

**Device selection:** Snowball Edge Storage Optimized (80TB usable per device)

```
Required devices = CEIL(500TB / 80TB) = 7 devices (plus 3 buffer for failures/overlap)
Total ordered: 10 × Snowball Edge Storage Optimized devices

Each device:
- 80TB usable storage
- 40 vCPUs
- 80GB RAM  
- 1Gbps and 10Gbps network interfaces
- Tamper-evident enclosure, 256-bit AES encryption
```

### Data Transfer Process

**Step 1: Prepare HDFS data for Snowball**

```bash
# Mount Snowball Edge device (after unlock with client)
snowballEdge unlock-device \
  --endpoint https://192.168.1.100 \
  --manifest-file /tmp/manifest.bin \
  --unlock-code UNLOCK-CODE-HERE

# Use S3 interface on Snowball Edge for transfer
# Install and configure AWS CLI for Snowball endpoint
aws configure set endpoint_url http://192.168.1.100:8080

# Copy from HDFS to Snowball using S3DistCp (runs on Hadoop cluster)
hadoop jar /usr/lib/hadoop/tools/lib/emr-s3-dist-cp.jar \
  --src hdfs:///data/analytics/ \
  --dest s3://snowball-bucket/analytics/ \
  --targetSize 536870912 \  # 512MB target file size for efficient transfer
  --groupBy '.*/(.*)/.*' \  # Group by partition
  --outputCodec none         # Keep existing compression (Parquet/ORC already compressed)
```

**Step 2: Parallel transfer across 10 devices**

```
Device assignment:
Snowball-01: /data/analytics/ (2020-2021) → 75TB
Snowball-02: /data/analytics/ (2022-2023) → 78TB
Snowball-03: /data/analytics/ (2024)      → 45TB
Snowball-04: /data/video-metadata/        → 60TB
Snowball-05: /data/video-output/ (part 1) → 80TB
Snowball-06: /data/video-output/ (part 2) → 80TB
Snowball-07: /data/video-output/ (part 3) → 80TB
Snowball-08: (overflow + buffer)
Snowball-09: (buffer)
Snowball-10: (buffer - unused)

Actual data: 498TB across 7 devices
```

**Step 3: Ship devices to AWS**

```
Shipping schedule:
Day 1-3:   Load Snowball-01 through Snowball-04
Day 3:     Ship first batch (4 devices) to AWS
Day 4-6:   Load Snowball-05 through Snowball-07
Day 6:     Ship second batch (3 devices) to AWS
Day 8-10:  AWS receives first batch, begins ingestion to S3
Day 10-12: AWS receives second batch, begins ingestion
Day 14:    All data in S3 (AWS confirmed)
```

**Step 4: Delta sync with AWS DataSync**

During the 2 weeks that devices were in transit, new data continued accumulating on HDFS. DataSync handled the delta.

```bash
# Deploy DataSync agent on-prem
# Create HDFS source location
aws datasync create-location-hdfs \
  --name-nodes '[{"Hostname":"namenode1.internal","Port":8020}]' \
  --authentication-type SIMPLE \
  --simple-user hdfs-user \
  --agents-arns arn:aws:datasync:us-east-1:123456789:agent/agent-ABC123

# Create S3 destination
aws datasync create-location-s3 \
  --s3-bucket-arn arn:aws:s3:::my-datalake-bucket \
  --s3-storage-class STANDARD \
  --s3-config '{"BucketAccessRoleArn":"arn:aws:iam::123456789:role/DataSyncS3Role"}'

# Create and start delta sync task
aws datasync create-task \
  --source-location-arn arn:aws:datasync:...:location/hdfs-location \
  --destination-location-arn arn:aws:datasync:...:location/s3-location \
  --name "hdfs-delta-sync" \
  --options '{"VerifyMode":"ONLY_FILES_TRANSFERRED","TransferMode":"CHANGED"}'

aws datasync start-task-execution \
  --task-arn arn:aws:datasync:...:task/task-ABC123
```

**Delta size:** 12TB of new/modified files accumulated during transit. DataSync transferred this in 28 hours at sustained 1.1 Gbps.

### Cost Comparison

| Option | Time | Cost |
|---|---|---|
| Direct network (1Gbps) | 57 days | $12,000 (Direct Connect fees) + 57 days of infrastructure |
| AWS Direct Connect (10Gbps, leased) | 6 days | $45,000 setup + $8,000/month |
| 10× Snowball Edge devices | 14 days total | $30,000 (device rental + shipping) |
| **Winner: Snowball** | **14 days** | **$30,000** |

**Rule of thumb:** If the transfer would take > 5 days at your available bandwidth, Snowball is likely faster and often cheaper.

### Total Migration Timeline

```
Week 1: HDFS data preparation and Snowball loading
Week 2: Snowball in transit + continued loading
Week 3: AWS S3 ingestion complete, DataSync delta running
         EMR cluster validation against S3 data
Week 4: Hadoop cluster decommissioned
         Cost savings begin: $180K/year saved
```

---

## Common Gotchas and Lessons Learned

These issues appeared across multiple case studies and are the most common sources of migration failures.

### Character Encoding: Latin1 vs UTF-8

**Problem:** Legacy Oracle and Teradata databases often use Latin1 (ISO-8859-1) character encoding. Aurora PostgreSQL defaults to UTF-8.

```sql
-- Check Oracle database character set
SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET';
-- Common output: WE8ISO8859P1 (Latin1), AL32UTF8 (UTF-8)

-- If source is Latin1, DMS may fail silently on non-ASCII characters
-- Characters like é, ñ, ü stored as single bytes in Latin1
-- UTF-8 requires 2 bytes for these characters
-- Result: garbled text or DMS row errors

-- Prevention: 
-- 1. Set Aurora PostgreSQL to LATIN1 encoding (then convert later)
-- 2. Or: force DMS to encode as UTF-8 and validate all non-ASCII data
-- 3. Best: run sample query before migration:
SELECT COUNT(*) FROM customers 
WHERE customer_name != CONVERT(customer_name USING utf8);
-- Non-zero result means you have encoding issues to resolve
```

### Timezone Handling

**Problem:** Oracle's TIMESTAMP WITH TIME ZONE stores timezone offset. PostgreSQL's TIMESTAMPTZ always stores in UTC and converts on display.

```sql
-- Oracle: stored as '2024-01-15 09:30:00 -05:00' (EST)
-- PostgreSQL: stored as '2024-01-15 14:30:00+00' (UTC)
-- Both represent the same moment, but application code may not handle conversion

-- Validation query (should return 0 rows if timestamps match):
SELECT COUNT(*) FROM (
  SELECT transaction_id,
         created_at AT TIME ZONE 'UTC' AS oracle_utc
  FROM oracle_transactions@dblink
) o
JOIN (
  SELECT transaction_id,
         created_at AS aurora_utc
  FROM transactions
) a ON o.transaction_id = a.transaction_id
WHERE o.oracle_utc != a.aurora_utc;
```

### NULL vs Empty String Semantics

**Problem:** Oracle treats empty string (`''`) as NULL. PostgreSQL treats them as distinct values.

```sql
-- Oracle
SELECT COUNT(*) FROM customers WHERE middle_name IS NULL;
-- Returns rows with NULL AND rows with '' (empty string)

-- PostgreSQL
SELECT COUNT(*) FROM customers WHERE middle_name IS NULL;
-- Returns ONLY rows with actual NULL

-- This causes row count mismatches in validation and application bugs
-- Fix: add COALESCE during migration transformation
-- Or: update application to use NULLIF(middle_name, '')
```

### Auto-Increment / Sequence Reset

**Problem:** After migration, sequences are reset to 1 while the table already contains data with higher IDs. The first INSERT fails with a primary key conflict.

```sql
-- Always run this immediately after migration for every table with a sequence:
DO $$
DECLARE
  t_name TEXT;
  seq_name TEXT;
  max_id BIGINT;
BEGIN
  FOR t_name IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
  LOOP
    BEGIN
      EXECUTE format('SELECT MAX(id) FROM %I', t_name) INTO max_id;
      IF max_id IS NOT NULL THEN
        seq_name := t_name || '_id_seq';
        EXECUTE format('SELECT setval(%L, %s)', seq_name, max_id + 1);
        RAISE NOTICE 'Reset sequence % to %', seq_name, max_id + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      NULL; -- Table may not have an id column
    END;
  END LOOP;
END $$;
```

### Foreign Key Constraints During Load

**Problem:** DMS loads tables in parallel. If a child table loads before its parent, foreign key violations cause task failure.

```sql
-- Disable all foreign key checks before DMS full load:
-- Aurora PostgreSQL
SET session_replication_role = 'replica';  -- Disables FK checks for this session

-- Or disable individual constraints:
ALTER TABLE orders DISABLE TRIGGER ALL;

-- After DMS full load completes:
SET session_replication_role = 'origin';

-- Validate referential integrity after re-enabling:
SELECT
  tc.table_name,
  kcu.column_name,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY';
```

### DMS Does Not Migrate Views, Procedures, and Triggers

**Problem:** Teams assume DMS handles the full schema migration. DMS only migrates table data (rows). All schema objects must be migrated separately via SCT or manual scripts.

```
DMS migrates:         Table data (INSERT equivalent for full load)
                      Ongoing changes via CDC (INSERT/UPDATE/DELETE)

DMS does NOT migrate: Views
                      Stored procedures and functions
                      Triggers
                      Sequences
                      Indexes (must be pre-created or created post-load)
                      Constraints (must be pre-created or created post-load)
                      Users and permissions
                      Database links (dblinks)

SCT migrates:         Schema objects (views, procedures, functions, triggers)
                      With conversion to target dialect

Manual migration:     Sequences (with reset)
                      Users and permissions
                      Database links → replaced with application-level connections
```

**Recommended sequence:**
1. SCT: convert and apply schema objects (tables, views, procedures) to target — no data yet
2. DMS: full load + CDC to migrate data
3. Manual: reset sequences, recreate indexes, validate constraints
4. SCT: apply remaining objects (triggers — enable after data load to avoid conflicts)
