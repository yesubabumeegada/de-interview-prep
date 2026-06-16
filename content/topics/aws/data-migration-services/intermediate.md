---
title: "AWS DMS Intermediate — Task Types, LOB Handling, Table Mapping, and Validation"
description: "DMS task types in depth, LOB column handling, table mapping rules, DMS validation, SCT assessment interpretation, and replication instance sizing"
content_type: study_material
topic: aws
subtopic: data-migration-services
layer: intermediate
difficulty_level: mid-level
tags: [aws, dms, cdc, lob-handling, table-mapping, dms-validation, sct, replication-instance, database-migration]
---

# AWS DMS Intermediate — Task Types, LOB Handling, and Validation

## DMS Task Types In Depth

### Full Load: How It Works Internally

During a full load, DMS:

1. **Selects tables** to migrate based on the table mapping rules
2. **Determines parallelism** — loads multiple tables concurrently (configurable `MaxFullLoadSubTasks`, default 8)
3. **Reads in batches** using SELECT statements with configurable batch sizes
4. **Writes to target** using bulk insert operations for efficiency

**Key configuration parameters:**

```json
{
  "FullLoadSettings": {
    "TargetTablePrepMode": "DROP_AND_CREATE",
    "CreatePKAfterFullLoad": false,
    "StopTaskCachedChangesApplied": false,
    "StopTaskCachedChangesNotApplied": false,
    "MaxFullLoadSubTasks": 8,
    "TransactionConsistencyTimeout": 600,
    "CommitRate": 50000
  }
}
```

**`TargetTablePrepMode` options:**
- `DROP_AND_CREATE` — drops and recreates target table (default, safe for fresh migrations)
- `TRUNCATE_BEFORE_LOAD` — truncates without dropping (preserves table structure/indexes)
- `DO_NOTHING` — assumes table exists and is empty (fastest, risky)

**`CommitRate`:** Number of rows per commit to target. Higher values = better throughput but larger transactions. Default 10,000. Increase to 50,000-100,000 for large tables.

### CDC: How It Works

CDC reads changes from the source database's transaction log — each database engine exposes this differently:

| Engine | Transaction Log | CDC Method |
|--------|----------------|-----------|
| Oracle | Redo logs | LogMiner or Binary Reader |
| SQL Server | Transaction log | MS-CDC or MS-Replication |
| MySQL | Binary log | binlog |
| PostgreSQL | WAL (Write-Ahead Log) | logical replication |

**DMS CDC pipeline:**
```
Source DB Transaction Log
    ↓ (DMS reads continuously)
DMS Replication Instance (parses and buffers changes)
    ↓ (applies in batches)
Target DB
```

**Latency:** Typical CDC lag is 1-30 seconds depending on write volume and replication instance size.

**CDC prerequisites by engine:**

For MySQL:
```sql
-- Check binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';           -- Must be ON
SHOW VARIABLES LIKE 'binlog_format';     -- Must be ROW
SHOW VARIABLES LIKE 'binlog_row_image';  -- Must be FULL

-- Grant DMS user CDC privileges
GRANT REPLICATION CLIENT ON *.* TO 'dms_user'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'dms_user'@'%';
```

For PostgreSQL:
```sql
-- Enable logical replication
-- postgresql.conf:
-- wal_level = logical
-- max_replication_slots = 5
-- max_wal_senders = 5

-- Grant DMS user replication permission
ALTER USER dms_user WITH REPLICATION;
```

For Oracle:
```sql
-- Enable supplemental logging (required for DMS)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Or table-level (less overhead):
ALTER TABLE schema.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### Full Load + CDC: The Standard Production Pattern

The recommended approach for live migrations with minimal downtime:

```
Phase 1: Full Load
  - DMS begins reading the source table data
  - CDC is simultaneously capturing changes (buffered in DMS)
  - Full load completes (may take hours for large tables)

Phase 2: CDC Catch-up
  - DMS applies buffered changes from during the full load
  - Ongoing changes continue to be applied
  - Monitor "CDCLatencySource" and "CDCLatencyTarget" CloudWatch metrics

Phase 3: Convergence
  - Target catches up to source
  - Latency approaches 0
  - Pre-cutover validation runs

Phase 4: Cutover
  - Quiesce source (pause writes)
  - Verify final latency = 0
  - Switch application to target
  - DMS task can be kept running for rollback capability
```

**CloudWatch metrics to monitor during CDC:**

```bash
# Key DMS CloudWatch metrics
CDCLatencySource:   Delay between source change and DMS reading it
CDCLatencyTarget:   Delay between DMS receiving change and applying to target
CDCIncomingChanges: Number of changes in the buffer
CDCThroughputRowsSource: Rows read from source per second
CDCThroughputRowsTarget: Rows applied to target per second
```

---

## LOB (Large Object) Handling

Large Object columns (BLOB, CLOB, TEXT, NTEXT, XML, JSON with large values) require special handling in DMS because they may be too large to read in-line with regular row data.

### Why LOBs Are Challenging

LOBs are stored separately from the main row data in most databases. Reading them requires additional database calls, which:
- Slows migration throughput significantly
- May not fit in DMS's standard row buffer
- Cannot always be captured via CDC in the same way as regular columns

### LOB Mode Options

**Inline LOB mode (default):**
```json
{
  "FullLobMode": false,
  "LobCheckColumns": false,
  "LimitedSizeLobMode": true,
  "LobMaxSize": 32,
  "InlineLobMaxSize": 0
}
```
- Reads LOBs inline with row data
- If LOB exceeds `LobMaxSize` (32 KB default), it is truncated
- Fastest mode, but data loss risk for large LOBs

**Limited LOB mode:**
```json
{
  "LimitedSizeLobMode": true,
  "LobMaxSize": 100
}
```
- Reads LOBs up to `LobMaxSize` KB
- Truncates anything larger
- Use when you know the maximum LOB size and can set appropriate limit

**Full LOB mode:**
```json
{
  "FullLobMode": true,
  "LobChunkSize": 64
}
```
- Reads entire LOB regardless of size
- Uses chunked reads (`LobChunkSize` KB per chunk)
- Much slower (2-10× slower for LOB-heavy tables)
- Never truncates data
- Use for tables where LOB completeness is required

**Inline LOB mode (v3.4.6+):**
```json
{
  "InlineLobMaxSize": 64
}
```
- For LOBs ≤ `InlineLobMaxSize` KB: reads inline (fast path)
- For LOBs > `InlineLobMaxSize` KB: uses full LOB mode (slow path)
- Best of both worlds for mixed-size LOB columns

### LOB Best Practice

1. Audit LOB column sizes in the source database:
```sql
-- Oracle: find max LOB sizes
SELECT
    table_name,
    column_name,
    ROUND(MAX(DBMS_LOB.GETLENGTH(column_name)) / 1024) AS max_kb
FROM your_table
GROUP BY table_name, column_name;
```

2. If max LOB size is known and bounded, use Limited LOB mode with appropriate `LobMaxSize`
3. If LOBs can be very large (>10 MB), use Full LOB mode but be aware of performance impact
4. For very large LOBs, consider migrating them separately via a custom script

---

## DMS Table Mapping Rules

Table mapping rules control which tables/schemas are migrated and how they're transformed.

### Selection Rules

```json
{
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": "1",
      "rule-name": "include-all-tables",
      "object-locator": {
        "schema-name": "salesdb",
        "table-name": "%"
      },
      "rule-action": "include"
    },
    {
      "rule-type": "selection",
      "rule-id": "2",
      "rule-name": "exclude-temp-tables",
      "object-locator": {
        "schema-name": "salesdb",
        "table-name": "temp_%"
      },
      "rule-action": "exclude"
    }
  ]
}
```

`%` is the wildcard. `exclude` takes precedence over `include`.

### Transformation Rules

Transformation rules modify schema names, table names, and column names during migration:

```json
{
  "rules": [
    {
      "rule-type": "transformation",
      "rule-id": "3",
      "rule-name": "lowercase-schema",
      "rule-action": "convert-lowercase",
      "rule-target": "schema",
      "object-locator": {
        "schema-name": "%"
      }
    },
    {
      "rule-type": "transformation",
      "rule-id": "4",
      "rule-name": "rename-table",
      "rule-action": "rename",
      "rule-target": "table",
      "object-locator": {
        "schema-name": "salesdb",
        "table-name": "CUSTOMER_MASTER"
      },
      "value": "customers"
    },
    {
      "rule-type": "transformation",
      "rule-id": "5",
      "rule-name": "add-prefix",
      "rule-action": "add-prefix",
      "rule-target": "table",
      "object-locator": {
        "schema-name": "%",
        "table-name": "%"
      },
      "value": "migrated_"
    }
  ]
}
```

**Available transformation actions:**
- `convert-lowercase` / `convert-uppercase`
- `rename` — rename to a specific name
- `add-prefix` / `remove-prefix`
- `add-suffix` / `remove-suffix`
- `add-column` — add a constant or computed column
- `remove-column` — exclude a column from migration

### Filtering Rows During Full Load

You can filter which rows to migrate using a `selection-rule` with a filter condition:

```json
{
  "rule-type": "selection",
  "rule-id": "10",
  "rule-name": "recent-orders-only",
  "object-locator": {
    "schema-name": "salesdb",
    "table-name": "orders"
  },
  "rule-action": "include",
  "filters": [
    {
      "filter-type": "source",
      "column-name": "order_date",
      "filter-conditions": [
        {
          "filter-operator": "between",
          "value": "2020-01-01",
          "end-value": "2024-12-31"
        }
      ]
    }
  ]
}
```

Useful for migrating only recent data when historical data is not needed.

---

## DMS Data Validation

DMS can automatically validate that data migrated to the target matches the source. This catches:
- Missing rows
- Rows that failed to migrate
- Data corruption during migration

### Enabling Validation

```json
{
  "ValidationSettings": {
    "EnableValidation": true,
    "ValidationMode": "ROW_LEVEL",
    "ThreadCount": 5,
    "PartitionSize": 10000,
    "FailureMaxCount": 10000,
    "RecordFailureDelayInMinutes": 5,
    "RecordSuspendDelayInMinutes": 30,
    "MaxKeyColumnSize": 8096,
    "TableFailureMaxCount": 1000,
    "ValidationOnly": false,
    "HandleCollectionNotAligned": "relocate-failures",
    "RecordFailureDelayLimitInMinutes": 0,
    "SkipLobColumns": false
  }
}
```

### Validation Metrics

DMS tracks validation state per row and per table:

**Row states:**
- `Validated` — row exists in both source and target with matching data
- `Mismatched` — row exists in both but data differs
- `Missing` — row exists in source but not in target
- `Extra` — row exists in target but not in source

**Table states:**
- `Validated` — all rows validated
- `Validation error` — mismatches or missing rows found
- `Suspended` — too many errors, validation paused

### Querying Validation Results

DMS writes validation failures to a replication instance table:

```sql
-- Query validation failures in DMS
SELECT
    table_owner,
    table_name,
    failure_time,
    key_type,
    key,
    failure_type
FROM awsdms_validation_failures_v1
WHERE failure_time > SYSDATE - 1
ORDER BY failure_time DESC;
```

### Validation Limitations

- Row-level validation compares primary key values and all column data
- LOB columns can be excluded from validation (`SkipLobColumns: true`) for performance
- Validation occurs after full load and during CDC — expect some validated → mismatched transitions if CDC changes are in flight
- Large tables with no primary key are harder to validate efficiently

---

## SCT Assessment Reports and Schema Incompatibilities

### Reading SCT Assessment Reports

SCT's assessment categorizes each database object by conversion complexity:

**Action Items by Category:**

| Category | Meaning | Typical Examples |
|----------|---------|-----------------|
| Automatic | 100% converted, no manual work | Standard SQL, DML, basic DDL |
| Simple action | Minor manual review needed | Slight syntax differences |
| Medium complexity | Some manual work | Unsupported functions, type mismatches |
| Complex action | Significant manual work | Proprietary PL/SQL, engine-specific features |
| Cannot convert | Must be manually rewritten | Oracle UTL packages, SQL Server CLR objects |

### Common Schema Incompatibilities

**Oracle → Aurora PostgreSQL:**

| Oracle Feature | PostgreSQL Equivalent | Action |
|---------------|----------------------|--------|
| `NUMBER(p,s)` | `NUMERIC(p,s)` | Automatic |
| `VARCHAR2(n)` | `VARCHAR(n)` | Automatic |
| `DATE` (includes time) | `TIMESTAMP` | Manual or SCT |
| `ROWNUM` | `ROW_NUMBER()` OVER () | Manual rewrite |
| `CONNECT BY` (hierarchical) | Recursive CTE | Manual rewrite |
| `PL/SQL` packages | `PL/pgSQL` functions | Partial automation |
| `DBMS_*` packages | No equivalent | Manual implementation |
| `MERGE` statement | `INSERT ... ON CONFLICT` | SCT with review |
| Sequences with NEXTVAL | PostgreSQL sequences | Automatic |

**SQL Server → Aurora MySQL:**

| SQL Server Feature | MySQL Equivalent | Action |
|-------------------|-----------------|--------|
| `IDENTITY` | `AUTO_INCREMENT` | Automatic |
| `NVARCHAR` | `VARCHAR(n) CHARACTER SET utf8mb4` | Automatic |
| `TOP n` | `LIMIT n` | Automatic |
| `GETDATE()` | `NOW()` | Automatic |
| `T-SQL stored procs` | MySQL stored procs | Partial |
| `CROSS APPLY` | Lateral join | Manual |
| `CLR objects` | No equivalent | Full manual |
| Full-text indexes | MySQL FULLTEXT | Manual |

### Handling Incompatibilities

**Strategy 1: Rewrite in application layer**
Move logic that cannot be converted (stored procedures, complex views) to application code. Often a better long-term architecture anyway.

**Strategy 2: Use AWS Lambda or Glue for transformation**
Replace database-side transformation with a serverless or managed ETL layer.

**Strategy 3: Accept functional equivalents**
Some incompatibilities are behavioral, not data. For example, Oracle's `DATE` type includes time; PostgreSQL's `DATE` does not. Map Oracle `DATE` to PostgreSQL `TIMESTAMP` and adjust application queries.

---

## DMS Replication Instance Sizing

The replication instance is the CPU and memory bottleneck for DMS. Sizing it correctly ensures the migration runs efficiently without being over-provisioned.

### Instance Classes

| Class | Use Case |
|-------|---------|
| `dms.t3.micro` / `t3.small` | Testing and development only |
| `dms.t3.medium` | Small migrations (< 100 GB, low CDC rate) |
| `dms.c5.large` | Medium migrations (100 GB – 1 TB) |
| `dms.c5.2xlarge` | Large migrations or high CDC throughput |
| `dms.c5.4xlarge` | Very large migrations, high-volume CDC |
| `dms.r5.2xlarge` | LOB-heavy tables, memory-intensive transformations |
| `dms.r5.4xlarge` | Multiple parallel tasks, large LOB workloads |

### Sizing Guidelines

**For full load:**
- Estimate: `1 vCPU per parallel table being loaded`
- Memory: `8 GB minimum`, more for LOB handling
- Starting recommendation: `dms.c5.2xlarge` (8 vCPU, 16 GB) for migrations > 100 GB

**For CDC:**
- CPU is the primary constraint (parsing and applying log changes)
- Rule of thumb: `1 vCPU per 5,000 transactions/second`
- Monitor `CPUUtilization` in CloudWatch; if sustained > 70%, scale up

**For both:**
- DMS uses disk for change buffering — allocate at least 50-100 GB storage on the replication instance
- Multi-AZ replication instances are available for high availability but add cost

### When to Scale Up

| Symptom | Metric | Action |
|---------|--------|--------|
| Full load too slow | `FullLoadThroughputRowsSource` | Increase `MaxFullLoadSubTasks`, scale up instance |
| CDC falling behind | `CDCLatencyTarget` > 30 seconds | Scale up replication instance |
| Memory pressure | `FreeableMemory` near 0 | Scale to r5 instance class |
| High CPU | `CPUUtilization` > 75% | Scale up instance class |
| Task restarting | DMS task state: `Recovering` | Check logs, may need more memory |

### Multi-Task Architecture for Large Migrations

For very large databases, distribute work across multiple DMS tasks:

```
Database: 500 tables, 10 TB total

Task 1: Large tables (5 tables, 8 TB)    → runs on c5.4xlarge
Task 2: Medium tables (50 tables, 1.5 TB) → runs on c5.2xlarge
Task 3: Small tables (445 tables, 0.5 TB) → runs on c5.large
```

This parallelizes the migration, reducing total migration time.

---

## Key Takeaways

- Full Load + CDC is the standard production migration pattern; monitor `CDCLatencyTarget` to know when the target is in sync
- LOB handling is the most common cause of DMS migration issues — audit LOB sizes and choose the appropriate LOB mode
- Table mapping rules control what gets migrated; transformation rules rename schemas, tables, and columns
- DMS validation automatically compares source and target rows — always enable it for production migrations
- SCT assessment reports classify schema objects by conversion complexity; plan manual effort based on "cannot convert" items
- Replication instance sizing is critical: under-sized instances cause full load to be slow or CDC to fall behind
