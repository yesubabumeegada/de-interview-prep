---
title: "Teradata Interview Scenarios - Senior Deep Dive"
topic: teradata
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [teradata, interview, scenarios, primary-index, bteq]
---

# Teradata Interview Scenarios — Senior Deep Dive

This guide targets senior DE and architect-level interview topics: Teradata Active System Management (TASM) workload management, advanced query optimization (join strategies, AMP parallelism), and migration patterns from Teradata to cloud platforms.

---

## 1. TASM — Workload Management

### What Is TASM?

Teradata Active System Management (TASM) is the workload management layer that classifies incoming requests and allocates system resources (CPU, I/O, AMP worker tasks) dynamically. It replaces the older Priority Scheduler and is managed through Viewpoint or DBC system tables.

### Core TASM Concepts

| Component | Description |
|---|---|
| **Workload** | A named classification bucket (e.g., `ETL_NIGHTLY`, `ADHOC_USERS`, `PRIORITY_EXEC`) |
| **Filter** | Rules that classify requests into workloads (by user, account, query band, resource usage) |
| **Throttle** | Limits on concurrent requests per workload |
| **Priority** | Relative CPU/IO allocation among workloads |
| **Tactical Response Time (TRT)** | Target latency for high-priority queries (typically <1 second) |
| **Exception** | Rules triggered when a query exceeds a threshold (CPU skew, spool usage, elapsed time) |
| **Planned Environment** | A named configuration set (e.g., "Batch Window", "Business Hours") — switched on schedule |

### Workload Classification Flow

```
Incoming SQL request
        │
        ▼
  Classification Rules
  (user, account string, query band, resource estimate)
        │
        ▼
   Assigned Workload
        │
        ├─ Throttle: max concurrent requests
        ├─ Priority: CPU/IO weight vs. other workloads
        └─ Exception thresholds: abort/demote if exceeded
```

### Setting Query Bands for Classification

Query bands allow applications to tag their sessions so TASM can classify them correctly without relying solely on the login username:

```sql
-- Set before executing ETL queries
SET QUERY_BAND = 'ApplicationName=ETL;JobName=DAILY_SALES_LOAD;' FOR SESSION;

-- TASM filter: IF QueryBand LIKE '%ApplicationName=ETL%' THEN classify as ETL_NIGHTLY
```

### TASM Exception Rules — Interview Scenario

**Q: A long-running adhoc query is consuming 80% of CPU on all AMPs and blocking priority business queries. How does TASM handle this, and how would you configure a preventive rule?**

> **A:** TASM can be configured with an Exception rule on the `ADHOC_USERS` workload:
>
> - **Trigger**: CPU time per AMP exceeds 60 seconds within 5 minutes.
> - **Action 1**: Demote the query to a lower-priority workload (`BACKGROUND`).
> - **Action 2**: If still exceeding threshold after 10 more minutes, abort the query and notify the user via Viewpoint.
>
> This prevents runaway adhoc queries from starving OLTP and ETL workloads without requiring manual DBA intervention.

**TASM configuration (via DBC or Viewpoint — shown as pseudo-code):**

```
Workload: ADHOC_USERS
  Throttle: max 20 concurrent requests
  Priority: Medium (weight 30/100)
  Exception:
    IF CPUSkew > 50% FOR 60 seconds
    THEN MOVE TO workload=BACKGROUND, ABORT IF repeated

Workload: PRIORITY_EXEC
  Throttle: max 5 concurrent requests
  Priority: High (weight 60/100)
  TRT: 1 second
```

### Planned Environments

```sql
-- Query current planned environment
SELECT * FROM DBC.WorkloadDefinition WHERE PlanName = 'BUSINESS_HOURS';

-- Switch environment manually (DBA action)
CALL SYSLIB.SwitchPlan('BATCH_WINDOW');
```

Planned environments allow automatic resource reallocation:
- **Business hours (08:00–18:00)**: High priority for OLTP and BI; throttle ETL.
- **Batch window (18:00–06:00)**: High priority for ETL/DW loads; throttle adhoc queries.

---

## 2. Advanced Query Optimization

### Join Strategies in Teradata

Teradata's optimizer chooses among three join strategies. Understanding when each is used — and how to influence the choice — is critical for senior interviews.

#### 1. Merge Join

Rows from both tables are sorted on the join key, then merged. Requires rows to be on the **same AMP** (co-located) or redistributed first.

```
When used:
- Both tables are large
- Join columns match the PI of at least one table
- Statistics exist for good cardinality estimates
```

```sql
-- Both tables have customer_id as PI — merge join, no redistribution
SELECT c.customer_name, SUM(s.amount)
FROM   customer_dim c          -- PI: customer_id (UPI)
JOIN   sales_fact  s           -- PI: customer_id (NUPI)
  ON   c.customer_id = s.customer_id
GROUP BY c.customer_name;

-- EXPLAIN shows: "We do an all-AMPs JOIN step ... merge join"
```

#### 2. Hash Join

Teradata builds a hash table from the smaller relation in memory on each AMP, then probes it with rows from the larger relation.

```
When used:
- One table is significantly smaller
- Join columns do not match PI (redistribution happens)
- Hash table fits in AMP memory (spool)
```

#### 3. Product Join (Nested Loop)

Every row of the outer table is compared with every row of the inner table — O(n×m). Teradata uses this only when both tables are tiny or a non-equi-join forces it.

```sql
-- Non-equi join forces product join
SELECT a.order_id, b.promo_id
FROM orders a
JOIN promotions b ON a.order_date BETWEEN b.start_date AND b.end_date;
-- EXPLAIN: "We do an all-AMPs product join step"
```

**Warning sign:** If EXPLAIN shows a product join on large tables, the optimizer likely lacks statistics or the query is missing a usable equi-join condition.

### Influencing Join Strategy

```sql
-- Force a row hash match join (hint — use sparingly)
SELECT /*+ ROWID */ ...

-- More reliable: set the join index or ensure statistics are current
COLLECT STATISTICS
    COLUMN (customer_id),
    COLUMN (sale_date, customer_id)
ON sales_fact;
```

### AMP Parallelism Deep Dive

Teradata parallelism has two dimensions:

1. **Inter-AMP parallelism**: All AMPs work simultaneously on their local data partitions.
2. **Intra-AMP parallelism**: Within a single AMP, multiple worker tasks can run concurrently (controlled by TASM).

**What limits parallelism:**
- **Skew**: One AMP with 60% of rows becomes the bottleneck — other AMPs idle.
- **Spool space exhaustion**: If an intermediate result set (spool) fills an AMP's vdisk, the query aborts with "spool space exceeded."
- **Lock contention**: A long-running write transaction holds an exclusive lock; all other requests on that table wait.
- **BYNET saturation**: Massive data redistribution (millions of rows moved between all AMPs) can saturate the internal network.

**Diagnosing spool exhaustion:**

```sql
-- Check current spool allocation
SELECT username, SpoolUsage / 1e9 AS spool_gb
FROM DBC.SessionInfo
ORDER BY spool_gb DESC;

-- Increase a user's spool limit (DBA action)
MODIFY USER etl_user AS SPOOL = 500000000000;  -- 500 GB
```

### Practical Optimization Checklist (Senior Interview)

1. **Run EXPLAIN before and after any change** — confirm the optimizer chose the expected strategy.
2. **Check statistics freshness** — `HELP STATISTICS <table>;`
3. **Identify skew** — `HASHAMP(HASHBUCKET(HASHROW(pi_col)))` distribution query.
4. **Verify PI co-location for frequent joins** — if two tables are always joined on the same column, both should have that column as their PI.
5. **Use PPI for range-filtered large tables** — eliminates non-relevant partitions before scanning.
6. **Review join index candidates** — pre-join or pre-aggregate materialized views for repeated costly joins.

```sql
-- Join index: pre-join customer_dim into sales_fact for fast rollup
CREATE JOIN INDEX sales_customer_ji AS
SELECT
    s.sale_date,
    c.region,
    c.segment,
    SUM(s.amount) AS total
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
GROUP BY s.sale_date, c.region, c.segment
PRIMARY INDEX (region, sale_date);
```

---

## 3. Teradata to Cloud Migration Patterns

### Migration Architecture Overview

A Teradata-to-cloud migration typically follows these phases:

```
Phase 1: Assessment
  ├─ Catalog all databases, tables, row counts, compression ratios
  ├─ Profile query workloads (top 50 queries by CPU, frequency)
  ├─ Identify Teradata-specific features: PI logic, BTEQ, FastLoad, TD-specific SQL extensions
  └─ Estimate cloud sizing (GCS/S3 storage, compute units)

Phase 2: Schema Translation
  ├─ Map Teradata data types to target types
  ├─ Replace PPI with partitioning equivalents (DATE partition in Snowflake/BigQuery)
  ├─ Translate NUPI/UPI → clustering keys (Snowflake) or partition + cluster (BigQuery)
  └─ Convert BTEQ/FastLoad scripts to cloud-native tools

Phase 3: Data Migration
  ├─ Extract via TPT (Teradata Parallel Transporter) to Parquet/CSV on S3/GCS
  ├─ Load into target (Snowflake COPY INTO, BigQuery bq load / Storage Write API)
  └─ Validate row counts, checksums, sample data

Phase 4: Query Validation
  ├─ Translate SQL (TD extensions → ANSI/Snowflake/BigQuery dialect)
  ├─ Run parallel execution: compare results row by row
  └─ Performance benchmark: SLA attainment on target

Phase 5: Cutover
  ├─ Freeze source, final incremental sync
  ├─ Redirect application connection strings
  └─ Monitor for 2–4 weeks before decommissioning Teradata
```

### Teradata → Snowflake

**Key mapping decisions:**

| Teradata | Snowflake Equivalent | Notes |
|---|---|---|
| Primary Index (UPI) | Clustering Key | Snowflake uses micro-partition pruning, not hashing |
| NUPI | No direct equivalent — use clustering | Consider sorting by PI column |
| PPI (date partition) | `PARTITION BY (RANGE(sale_date))` | Snowflake auto-clusters; manual clustering available |
| BTEQ scripts | SnowSQL + Snowpipe / Tasks | BTEQ directives translate to shell scripts |
| FastLoad | Snowflake `COPY INTO` from S3 | Equivalent throughput |
| MultiLoad | Snowflake `MERGE` statement | Full upsert support |
| `COLLECT STATISTICS` | `ANALYZE TABLE` (auto in Snowflake) | Snowflake auto-updates stats continuously |
| Volatile tables | Snowflake Transient tables or CTEs | Transient tables skip Fail-safe overhead |
| Derived tables / spool | CTEs, Snowflake Warehouse cache | No explicit spool management needed |

**Data type mapping:**

```sql
-- Teradata → Snowflake type mapping
BYTEINT        → NUMBER(3,0)
SMALLINT       → SMALLINT
INTEGER / INT  → INTEGER
BIGINT         → BIGINT
DECIMAL(p,s)   → NUMBER(p,s)
FLOAT          → FLOAT
CHAR(n)        → CHAR(n)
VARCHAR(n)     → VARCHAR(n)
DATE           → DATE
TIME           → TIME
TIMESTAMP      → TIMESTAMP_NTZ  -- no timezone conversion by default
INTERVAL       → (no direct equivalent — use DATEDIFF/DATEADD)
PERIOD(DATE)   → Store as start_date + end_date DATE columns
```

**SQL dialect translation examples:**

```sql
-- Teradata: QUALIFY (window function filter)
SELECT sale_id, customer_id, amount,
       RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
FROM sales_fact
QUALIFY rnk = 1;

-- Snowflake: Use subquery (Snowflake supports QUALIFY natively since 2022)
SELECT sale_id, customer_id, amount
FROM (
    SELECT sale_id, customer_id, amount,
           RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
    FROM sales_fact
)
WHERE rnk = 1;
-- OR: Snowflake also supports QUALIFY directly
```

```sql
-- Teradata: SAMPLE
SELECT * FROM customer_dim SAMPLE 1000;

-- Snowflake:
SELECT * FROM customer_dim SAMPLE (1000 ROWS);
```

```sql
-- Teradata: DATE arithmetic
WHERE order_date = CURRENT_DATE - 7

-- Snowflake:
WHERE order_date = CURRENT_DATE() - INTERVAL '7 days'
-- OR:
WHERE order_date = DATEADD('day', -7, CURRENT_DATE())
```

### Teradata → BigQuery

**Key differences from Snowflake migration:**

| Concern | Approach |
|---|---|
| No clustering key concept at write | Use `CLUSTER BY` on most-filtered columns |
| Columnar by default | Re-examine wide tables — BigQuery is heavily columnar |
| Nested/repeated fields | Flatten Teradata normalized schemas or use STRUCT/ARRAY |
| BTEQ → Dataform / dbt | Translate procedural BTEQ ETL to SQL-based transformation framework |
| Partitioned tables required for >1TB | Always partition large fact tables by `DATE(_PARTITIONTIME)` |

**BigQuery DDL equivalent of a Teradata PPI table:**

```sql
-- Teradata PPI
CREATE TABLE sales_fact (
    sale_id     INTEGER NOT NULL,
    customer_id INTEGER,
    sale_date   DATE,
    amount      DECIMAL(15,2)
)
PRIMARY INDEX (customer_id)
PARTITION BY RANGE_N(sale_date BETWEEN DATE '2020-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' MONTH);

-- BigQuery equivalent
CREATE TABLE `project.dataset.sales_fact`
(
    sale_id     INT64,
    customer_id INT64,
    sale_date   DATE,
    amount      NUMERIC
)
PARTITION BY sale_date
CLUSTER BY customer_id
OPTIONS (
    partition_expiration_days = 1825  -- 5 years
);
```

### TPT Export for Large-Scale Migration

```bash
# Teradata Parallel Transporter (TPT) export script
tbuild -f export_job.tpt

# export_job.tpt
DEFINE JOB EXPORT_SALES_FACT
DESCRIPTION 'Export sales_fact to S3'
(
    DEFINE OPERATOR EXPORT_OP
    TYPE EXPORT
    SCHEMA *
    ATTRIBUTES
    (
        VARCHAR TdpId = 'tdserver',
        VARCHAR UserName = 'etl_user',
        VARCHAR UserPassword = '${TD_PASSWORD}',
        VARCHAR SelectStmt = 'SELECT * FROM prod_db.sales_fact'
    );

    DEFINE OPERATOR S3_WRITE_OP
    TYPE DATACONNECTOR PRODUCER
    SCHEMA *
    ATTRIBUTES
    (
        VARCHAR DirectoryPath = 's3://my-bucket/teradata-exports/sales_fact/',
        VARCHAR FileWritingRule = 'Overwrite',
        VARCHAR Format = 'Parquet',
        INTEGER MaxSessions = 16
    );

    APPLY TO OPERATOR (S3_WRITE_OP) SELECT * FROM OPERATOR (EXPORT_OP);
);
```

### Migration Risk: Teradata-Specific SQL Features

These features have no direct cloud equivalent and require rewriting:

| Feature | Risk | Resolution |
|---|---|---|
| `COMPRESS` column-level compression | Not needed in cloud columnar stores | Remove — cloud handles natively |
| `FALLBACK` | No equivalent | Remove |
| `JOURNAL` tables | Implement change data capture via Debezium/Fivetran | Rearchitect |
| Derived periods / PERIOD data type | No native equivalent | Split into `start_date`, `end_date` columns |
| Procedural SQL (Teradata SP) | Snowflake JS procedures / BigQuery stored procedures | Rewrite |
| Macro objects | CTEs or stored procedures | Translate per use case |
| `USING` clause in UPDATE | Rewrite as MERGE | Standard SQL |
| `NORMALIZE` temporal operator | Custom window logic | Requires manual translation |
