---
title: "Teradata Interview Scenarios - Real World"
topic: teradata
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, interview, scenarios, primary-index, bteq]
---

# Teradata Interview Scenarios — Real World

This guide covers production incidents and real-world engineering decisions that senior Teradata engineers encounter: handling skew incidents in production, safely changing a Primary Index on a live table, and executing a large-scale EDW migration.

---

## 1. Production Skew Incidents

### Incident Anatomy: The "Black Friday" Skew Problem

**Background:** A retail EDW runs an `orders` fact table with NUPI on `customer_id`. During Black Friday, a single corporate client (a large wholesale buyer, `customer_id = 88001`) submits 4.2 million orders — 38% of the day's total. A single AMP is overwhelmed. Nightly ETL goes from 45 minutes to 6.5 hours. BI dashboards miss their 06:00 SLA.

**Detection:**

```sql
-- 1. Check which queries are running long
SELECT
    s.SessionNo,
    s.UserName,
    s.QueryBand,
    r.AmpCPUTime,
    r.ReqPhysIO,
    r.ElapsedTime,
    TRIM(r.SQLTextInfo) AS sql_text
FROM DBC.SessionInfo s
JOIN DBC.QryLogV r ON s.SessionNo = r.SessionNo
WHERE r.ElapsedTime > 3600  -- running > 1 hour
ORDER BY r.ElapsedTime DESC;

-- 2. Confirm skew on orders table
SELECT
    HASHAMP(HASHBUCKET(HASHROW(customer_id))) AS amp_num,
    COUNT(*) AS row_cnt
FROM orders
WHERE order_date = CURRENT_DATE
GROUP BY amp_num
ORDER BY row_cnt DESC;
-- Result: amp_num 47 has 4.2M rows; median AMP has ~110K rows
```

**Immediate mitigation (without schema change):**

```sql
-- Route the ETL query for customer 88001 through a separate spool distribution
-- by adding a synthetic distribution key column in the query
CREATE VOLATILE TABLE orders_tmp AS (
    SELECT
        o.*,
        (ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_id) MOD 32) AS dist_bucket
    FROM orders o
    WHERE order_date = CURRENT_DATE
) WITH DATA
PRIMARY INDEX (dist_bucket)  -- redistributes across 32 buckets → 32 AMPs
ON COMMIT PRESERVE ROWS;

-- ETL aggregation now runs against orders_tmp with even distribution
SELECT customer_id, SUM(amount) FROM orders_tmp GROUP BY 1;
```

This is a **query-time workaround**. The data is temporarily copied and redistributed in spool. It adds I/O overhead but rescues the SLA for that night.

**Permanent fix — evaluated after the incident:**

Option A: Change PI to `order_id` (UPI). Even distribution guaranteed. Requires PI change procedure (see Section 2).

Option B: Partition by `order_date` with NUPI on `customer_id`. Each partition is smaller; the hot-key skew is contained within a day's partition and mitigated by partition elimination.

```sql
CREATE TABLE orders_v2 AS orders WITH NO DATA
PRIMARY INDEX (customer_id)
PARTITION BY RANGE_N(
    order_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' DAY   -- 3,652 daily partitions; hot key only spans one day's partition
);

INSERT INTO orders_v2 SELECT * FROM orders;
```

Option C: Composite PI `(customer_id, order_date)`. The composite hash value has higher cardinality, distributing the wholesale buyer's rows across more AMPs.

**Post-mortem checklist:**

- [ ] Add proactive skew monitoring: scheduled job checking `skew_ratio > 1.5` daily.
- [ ] Add TASM exception rule for queries in `ETL_NIGHTLY` workload that exceed 4× average AMP CPU.
- [ ] Update data model review process to include cardinality analysis before PI selection.
- [ ] Implement a "hot key" registry — business team flags known high-volume accounts before seasonal peaks.

---

### Production Skew Monitoring Script

```sql
-- Run daily via BTEQ as part of DBA maintenance suite
CREATE VOLATILE TABLE skew_report AS (
    SELECT
        databasename,
        tablename,
        SUM(currentperm) / 1e9          AS total_gb,
        MAX(currentperm) / 1e6          AS max_amp_mb,
        AVG(currentperm) / 1e6          AS avg_amp_mb,
        MAX(currentperm) / AVG(currentperm) AS skew_ratio,
        (MAX(currentperm) - AVG(currentperm)) / NULLIFZERO(AVG(currentperm)) * 100 AS skew_pct
    FROM DBC.TableSizeV
    WHERE databasename IN ('PROD_DW', 'STAGING', 'MART')
    GROUP BY 1, 2
    HAVING skew_ratio > 1.3   -- flag tables with >30% skew
) WITH DATA
PRIMARY INDEX (tablename)
ON COMMIT PRESERVE ROWS;

-- Insert into alerting table for Viewpoint or external monitoring
INSERT INTO dba_monitoring.skew_alerts
SELECT
    CURRENT_TIMESTAMP AS detected_at,
    databasename,
    tablename,
    total_gb,
    skew_ratio,
    skew_pct
FROM skew_report
WHERE skew_pct > 50;  -- critical threshold
```

---

## 2. Primary Index Change Scenarios

### Why Changing a PI Is Risky

Changing a Primary Index on a large production table is one of the riskiest Teradata DDL operations:
- Rows must be physically redistributed across all AMPs.
- The operation is not instantaneous — for a 10TB table it can take hours.
- During the redistribution, the table may need to be locked (at minimum, read locks are held for validation).
- If the process is interrupted, you may be left with a partially migrated table.
- All dependent join indexes and secondary indexes are dropped and must be rebuilt.

### Safe PI Change Procedure

**Scenario:** Change `orders` table PI from `NUPI(customer_id)` to `UPI(order_id)` in production.

**Step 1: Assess impact**

```sql
-- Check table size and row count
SELECT SUM(CurrentPerm)/1e9 AS size_gb, SUM(RowCount) AS total_rows
FROM DBC.TableSizeV WHERE TableName = 'orders';

-- Check dependent objects
SELECT * FROM DBC.All_RI_ChildrenV WHERE ChildTable = 'orders';
SELECT * FROM DBC.All_RI_ParentsV WHERE ParentTable = 'orders';
SELECT * FROM DBC.IndicesV WHERE TableName = 'orders' AND IndexType IN ('J','K');  -- join/hash indexes
```

**Step 2: Create new table with target PI**

```sql
CREATE TABLE orders_new_pi
(
    order_id    INTEGER NOT NULL,
    customer_id INTEGER,
    order_date  DATE,
    amount      DECIMAL(15,2),
    status      CHAR(2)
)
UNIQUE PRIMARY INDEX (order_id)           -- new PI
FALLBACK                                   -- keep fallback if original had it
PARTITION BY RANGE_N(
    order_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);
```

**Step 3: Populate with parallelism**

```sql
-- Use INSERT...SELECT to populate; this redistributes all rows in parallel
INSERT INTO orders_new_pi
SELECT order_id, customer_id, order_date, amount, status
FROM orders;
```

For very large tables (>1TB), consider using TPT (Teradata Parallel Transporter) to export and reload, which provides better throughput and restart capability.

**Step 4: Validate**

```sql
-- Row count match
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM orders_new_pi;

-- Checksum on key columns (sample-based for huge tables)
SELECT SUM(HASHROW(order_id)) AS chk FROM orders SAMPLE 1000000;
SELECT SUM(HASHROW(order_id)) AS chk FROM orders_new_pi SAMPLE 1000000;

-- Spot-check high-risk rows (NULL PI values)
SELECT COUNT(*) FROM orders_new_pi WHERE order_id IS NULL;  -- should be 0 for UPI
```

**Step 5: Atomic rename (requires exclusive lock — plan for low-traffic window)**

```sql
-- Acquire exclusive access
LOCKING orders FOR EXCLUSIVE ACCESS;

RENAME TABLE orders         TO orders_old_pi;
RENAME TABLE orders_new_pi  TO orders;

-- Verify views and dependent queries still resolve
SELECT * FROM orders SAMPLE 10;
```

**Step 6: Rebuild indexes and collect statistics**

```sql
-- Rebuild any secondary indexes
CREATE INDEX (customer_id) ON orders;

-- Refresh stats
COLLECT STATISTICS
    INDEX (order_id),
    COLUMN (order_date),
    COLUMN (customer_id)
ON orders;
```

**Step 7: Keep old table for rollback window**

```sql
-- Retain orders_old_pi for 2 weeks; verify no regressions
-- After validation period:
DROP TABLE orders_old_pi;
```

### When ALTER TABLE PRIMARY INDEX Is Appropriate

Teradata supports `ALTER TABLE ... PRIMARY INDEX` but it performs in-place redistribution and drops all secondary indexes. For production tables:

- It is acceptable for small-to-medium tables (<100GB) where downtime is tolerable.
- It is **not recommended** for multi-TB tables — the create-and-rename approach gives better control, parallelism, and rollback options.

```sql
-- Simple ALTER (only for small tables or dev/staging)
ALTER TABLE orders PRIMARY INDEX (order_id);
-- Warning: this drops all secondary indexes; rebuild them after
```

---

## 3. Migration Case Studies

### Case Study 1: Retail EDW — Teradata 6500 to Snowflake

**Context:** 80TB active data, 500TB total with historical archives. 1,200 tables, 3,000 BTEQ/FastLoad scripts, 150 Cognos reports.

**Timeline:** 18-month project, 6-person data engineering team.

**Key decisions and lessons learned:**

**Decision 1: Incremental table migration by data domain**

Rather than lift-and-shift all 1,200 tables at once, the team migrated by business domain (Finance → HR → Sales → Operations) over 12 months. Each domain was validated by business analysts before the next domain started.

```
Month 1–2:  Finance (120 tables, 8TB) — most critical, most scrutinized
Month 3–4:  HR / Workforce (80 tables, 2TB)
Month 5–8:  Sales & Marketing (350 tables, 30TB) — largest domain
Month 9–12: Operations / Supply Chain (650 tables, 40TB)
```

**Decision 2: dbt for all ETL rewrites**

BTEQ scripts were rewritten as dbt models. This provided:
- Version control (all logic in Git)
- Automatic lineage documentation
- CI/CD testing for data quality (not possible with raw BTEQ)

**Decision 3: Dual-run validation for 3 months**

Both Teradata and Snowflake ran in parallel. Automated comparison jobs ran nightly:

```sql
-- Validation query run against both systems; results compared in Python
SELECT
    report_date,
    business_unit,
    SUM(revenue)   AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM sales_mart
WHERE report_date = CURRENT_DATE - 1
GROUP BY 1, 2;
```

**Lesson 1: PI → Clustering Key mapping requires performance testing**

The team initially mapped every Teradata NUPI column directly to a Snowflake clustering key. After performance testing, many clustering keys were revised because Snowflake's micro-partition pruning works differently from Teradata's AMP-routing. Tables with very low cardinality PI columns (e.g., `region_id` with 12 values) performed better with clustering on `(region_id, report_date)` in Snowflake.

**Lesson 2: BTEQ procedural logic is hard to map**

Approximately 20% of BTEQ scripts contained complex procedural logic (loops, conditional branching, dynamic SQL generation) that could not be directly translated to dbt SQL models. These were rewritten as Python + Snowflake stored procedures. Budget an extra 40% of time for procedural script migration.

**Lesson 3: Data type edge cases cause silent data corruption**

Teradata's `DATE` format is stored as an integer internally; Snowflake uses ISO 8601. Some legacy dates stored as `'YYMMDD'` format in CHAR columns caused silent corruption during load. The team implemented a pre-migration audit:

```python
# Python validation: scan for non-standard date formats
import teradatasql
conn = teradatasql.connect(host='tdserver', user='etl', password=TD_PWD)
cur = conn.cursor()
cur.execute("""
    SELECT TOP 100 order_date_char, COUNT(*)
    FROM legacy_orders
    WHERE CAST(order_date_char AS DATE FORMAT 'YYYY-MM-DD') IS NULL
    GROUP BY 1
    ORDER BY 2 DESC
""")
```

---

### Case Study 2: Financial Services — Teradata to BigQuery

**Context:** 25TB EDW with strict regulatory requirements (SOX, PCI-DSS). 400 tables, 95% SQL-based ETL (minimal BTEQ). 30-person analytics team using SAS and Tableau.

**Key differences from Retail case:**

**Regulatory data lineage:** BigQuery's `INFORMATION_SCHEMA` and Data Catalog were used to rebuild the audit trail that Teradata's journal tables previously provided.

**Row-level security:** Teradata used database-level security (separate databases per team). BigQuery's row-level access policies replaced this:

```sql
-- BigQuery row-level security (replaces Teradata separate databases)
CREATE ROW ACCESS POLICY finance_only
ON `project.dataset.accounts`
GRANT TO ('group:finance-team@company.com')
FILTER USING (department = 'FINANCE');
```

**Temporal table migration:** The bank used Teradata's PERIOD data type for slowly changing dimensions. BigQuery has no PERIOD type. The solution was a standard `valid_from` / `valid_to` pattern:

```sql
-- Teradata temporal SCD2
CREATE TABLE customer_scd (
    customer_id INTEGER,
    customer_name VARCHAR(100),
    valid_period PERIOD(DATE)  -- TD temporal type
) PRIMARY INDEX (customer_id);

-- BigQuery equivalent
CREATE TABLE `project.dataset.customer_scd`
(
    customer_id   INT64,
    customer_name STRING,
    valid_from    DATE,
    valid_to      DATE         -- NULL means current record
)
PARTITION BY valid_from
CLUSTER BY customer_id;

-- Query: get current record
SELECT * FROM `project.dataset.customer_scd`
WHERE valid_to IS NULL OR valid_to > CURRENT_DATE();
```

**Performance outcome:** After 6 months of tuning, 92% of queries met or beat their Teradata SLA. The remaining 8% were complex multi-terabyte joins that required query rewrites to leverage BigQuery's columnar strengths (denormalization, use of STRUCT/ARRAY for nested data).
